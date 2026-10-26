package org.openmoq.moq5

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow
import org.openmoq.moq5.engine.EndpointEngine
import org.openmoq.moq5.engine.EngineClosedException
import org.openmoq.moq5.engine.ObjectStream
import org.openmoq.moq5.engine.ReceiverModel
import org.openmoq.moq5.engine.RecordStream

/**
 * What the receiver does when the application stops draining and the
 * network keeps delivering (`moq_media_overflow_policy_t`). A REQUIRED
 * choice: the C tier refuses an unset policy, and plain drop-oldest is
 * deliberately absent because evicting a keyframe while keeping its deltas
 * corrupts the stream. Nothing here is a silent discard.
 */
public enum class OverflowPolicy {
    /** Live renderers: drop back to the next keyframe. */
    DropToKeyframe,

    /** Drop whole groups. */
    DropGroup,

    /** Pause the media subscriptions upstream when full; gaps are counted, never silent. */
    FlowControl,
}

/** How reported timestamps are presented (`moq_media_time_mode_t`); payload bytes are never modified. */
public enum class TimeMode {
    /** Wire timestamps pass through. */
    Raw,

    /** Reported timestamp fields are rebased against one shared epoch. */
    SharedEpoch,
}

/**
 * The receiver configuration (`moq_media_receiver_cfg_t`) for
 * [MoqEndpoint.attachReceiver].
 *
 * [namespace] is what to receive: required and NON-EMPTY here (the receiver
 * requires at least one part, each non-empty), unlike the generic
 * [Namespace] value, which may be empty in other uses. [overflow] is the
 * required policy. The bounds keep the C defaults (`0` on the wire means
 * the default, so the defaults are stated here as the values they resolve
 * to): [maxObjects] 256, [maxBytes] 32 MiB, [maxTrackEvents] 64 -- the
 * track-event bound is TERMINAL on overflow, because losing a discovery
 * event is unrecoverable miswiring. [autoSubscribe] subscribes every
 * eligible media track after discovery; false is discovery-only.
 * [catalogTrack] is the catalog's TRACK NAME (`catalog_track` is a
 * `moq_bytes_t` track name, not a namespace field): null means the C
 * default, the track named "catalog", and an explicit empty name is
 * refused rather than silently meaning the default. Every byte
 * identity crosses the bridge as bytes, never as text. [eventHistory] arms
 * the optional exact event stream ([MediaReceiver.trackEvents]); it is
 * [EventHistoryPolicy.Disabled] unless asked for, so an application that
 * only reads the inventory never stages history it does not consume.
 */
public data class ReceiverConfig(
    public val namespace: Namespace,
    public val overflow: OverflowPolicy,
    public val maxObjects: UInt = 256u,
    public val maxBytes: ULong = 32uL * 1024u * 1024u,
    public val maxTrackEvents: UInt = 64u,
    public val autoSubscribe: Boolean = true,
    public val catalogTrack: TrackName? = null,
    public val timeMode: TimeMode = TimeMode.Raw,
    public val eventHistory: EventHistoryPolicy = EventHistoryPolicy.Disabled,
) {
    init {
        require(namespace.partCount > 0) { "a receiver namespace names at least one part; the generic empty Namespace cannot be received" }
        for (i in 0 until namespace.partCount) {
            require(namespace.partAt(i).size > 0) { "every receiver namespace part is non-empty; part $i is empty" }
        }
        require(catalogTrack == null || catalogTrack.size > 0) { "catalogTrack is a non-empty track name, or null for the C default \"catalog\"" }
        require(maxObjects <= Int.MAX_VALUE.toUInt()) { "maxObjects must cross the bridge as a positive 32-bit value; got $maxObjects" }
        require(maxTrackEvents <= Int.MAX_VALUE.toUInt()) { "maxTrackEvents must cross the bridge as a positive 32-bit value; got $maxTrackEvents" }
        require(maxBytes <= Long.MAX_VALUE.toULong()) { "maxBytes must cross the bridge as a positive 64-bit value; got $maxBytes" }
    }
}

/** The C `moq_media_overflow_policy_t` ordinal; UNSET (0) is unrepresentable. */
internal fun OverflowPolicy.nativeOrdinal(): Int =
    when (this) {
        OverflowPolicy.DropToKeyframe -> 1
        OverflowPolicy.DropGroup -> 2
        OverflowPolicy.FlowControl -> 3
    }

/** The C `moq_media_start_mode_t` ordinal. */
internal fun StartMode.nativeOrdinal(): Int =
    when (this) {
        StartMode.Current -> 0
        StartMode.NextGroup -> 1
    }

/** The C `moq_media_time_mode_t` ordinal. */
internal fun TimeMode.nativeOrdinal(): Int =
    when (this) {
        TimeMode.Raw -> 0
        TimeMode.SharedEpoch -> 1
    }

/**
 * An immutable byte value from a catalog or an init segment: a track name,
 * a role, a codec string, an init segment, a key id. Compared by content,
 * never converted through text (see [ByteIdentity.utf8OrNull] for the
 * explicit, lossy human view). May be empty.
 */
public class MediaBytes(bytes: ByteArray) : ByteIdentity(bytes, "MediaBytes")

/** `moq_media_type_t`: exactly the cases the C header declares. */
public enum class MediaType { Video, Audio }

/** `moq_media_packaging_t`, the STRUCTURED packaging the media tier derived; the catalog's own `packaging` string stays on [TrackDescription.packaging]. */
public enum class PackagingKind { Raw, Cmaf }

/** `moq_cmaf_codec_kind_t`; the C `UNKNOWN` is represented as absent (`null`), not as a case. */
public enum class CmafCodecKind { Avc, Hevc, Av1, Aac, Opus }

/** Common-encryption parameters from an init segment (`moq_cmaf_init_info_t`). */
public data class CencInfo(
    /** The `schm` scheme type, four bytes (`cenc`, `cbcs`). */
    public val scheme: MediaBytes,
    /** The `tenc` default key id, sixteen bytes. */
    public val defaultKid: MediaBytes,
    public val defaultIsProtected: Boolean,
    public val perSampleIvSize: UInt,
)

/** What the init segment says (`moq_cmaf_init_info_t`), distinct from what the catalog says. */
public data class CmafInitInfo(
    public val codecKind: CmafCodecKind?,
    public val timescale: UInt,
    public val width: UInt,
    public val height: UInt,
    public val samplerate: UInt,
    public val channelCount: UInt,
    /** Decoder extradata; may be empty. */
    public val codecConfig: MediaBytes,
    /** `tkhd.track_ID`; zero when absent. */
    public val trackId: UInt,
    public val cenc: CencInfo?,
)

/**
 * An immutable snapshot of a track's description (`moq_media_track_desc_t`,
 * every field of its v0 layout except the media template, which is deferred
 * with the SAP/timeline surface).
 *
 * Byte fields: [name] is the track's identity and is always present, even
 * when empty. The other byte fields are optional catalog attributes with no
 * separate presence bit in C, so an EMPTY native span is `null` here.
 * Numeric catalog attributes carry their own presence bit in C and are
 * `null` exactly when it is clear. The catalog's passthrough dimensions
 * ([catalogWidth], [catalogHeight], [catalogSamplerate]) are distinct from
 * the init-derived ones in [init]: the catalog claims, the init segment
 * proves.
 */
public data class TrackDescription(
    public val name: MediaBytes,
    public val packaging: MediaBytes? = null,
    public val role: MediaBytes? = null,
    public val codec: MediaBytes? = null,
    public val lang: MediaBytes? = null,
    public val label: MediaBytes? = null,
    public val mimeType: MediaBytes? = null,
    public val eventType: MediaBytes? = null,
    public val channelConfig: MediaBytes? = null,
    public val mediaType: MediaType? = null,
    public val packagingKind: PackagingKind? = null,
    public val timescale: UInt = 0u,
    /** The MoQ draft the track's objects are read under (`moq_version_t`); zero when unknown. */
    public val transportVersion: UInt = 0u,
    public val init: CmafInitInfo? = null,
    /** The raw init segment, present when the init segment was resolved. */
    public val initData: MediaBytes? = null,
    public val catalogWidth: UInt? = null,
    public val catalogHeight: UInt? = null,
    public val catalogSamplerate: UInt? = null,
    /** Frames per second times one thousand. */
    public val framerateMillis: ULong? = null,
    public val bitrate: ULong? = null,
    public val maxGroupSap: UInt? = null,
    public val maxObjectSap: UInt? = null,
    public val contentProtectionRefIds: List<MediaBytes> = emptyList(),
    public val depends: List<MediaBytes> = emptyList(),
    public val trackDurationMs: ULong? = null,
    public val isLive: Boolean = true,
)

/**
 * A track's durable catalog state. `Present` is a track the catalog lists;
 * `Removed` was dropped by a later catalog generation; `Ended` was ended or
 * rejected by the peer. Neither terminal state discards the track: the C
 * handle stays valid, inert, until the receiver is destroyed, and so does
 * the [MediaTrack]. A live-to-VOD conversion is an EVENT ([TrackEvent.Updated])
 * that re-snapshots the description and leaves the track `Present`.
 */
public enum class TrackPresence { Present, Removed, Ended }

/**
 * One discovered track: an identity, not a value. The same native handle is
 * always the same instance, so a track may be used as a map key and compared
 * with `===`. Its [description] is an immutable snapshot, replaced only on
 * the one mutation the C tier sanctions (live to VOD); [presence] and
 * [parseDrops] are the durable state the receiver's events fold into it.
 * Every field is written on the endpoint's service worker and read from any
 * thread.
 *
 * [isRetired] is true once the receiver that discovered the track has been
 * closed: the snapshot stays readable, and nothing native is ever touched
 * through a retired track.
 */
public class MediaTrack internal constructor(
    internal val token: Long,
    description: TrackDescription,
) {
    @Volatile
    public var description: TrackDescription = description
        internal set

    @Volatile
    public var presence: TrackPresence = TrackPresence.Present
        internal set

    /** Cumulative media objects dropped at parse for this track, from the receiver's coalesced diagnostics. */
    @Volatile
    public var parseDrops: ULong = 0uL
        internal set

    @Volatile
    public var isRetired: Boolean = false
        internal set

    override fun toString(): String = "MediaTrack(${description.name}, $presence${if (isRetired) ", retired" else ""})"
}

/** Why the receiver stopped: a clean close, or its own fatal code. */
public sealed interface ReceiverTerminal {
    public data object Closed : ReceiverTerminal

    public data class Fatal(public val code: ReceiverFatalCode) : ReceiverTerminal
}

/**
 * ONE atomic snapshot of everything the receiver has discovered: the
 * [tracks] in discovery order (removed and ended ones retained with their
 * [TrackPresence]), whether the first catalog has been fully enumerated
 * ([catalogReady]), the receiver's [terminal] once it has one, and the
 * count of native events that could not be folded in ([anomalies]: an
 * ADDED for a handle already known, or a change for one never announced).
 *
 * [revision] advances by exactly one for every integrated change -- each
 * track event, each anomaly, the terminal -- so a collector on the
 * inventory wakes for a change that leaves the track list's contents
 * unchanged, such as an in-place description update or an end.
 */
public data class ReceiverInventory(
    public val tracks: List<MediaTrack>,
    public val catalogReady: Boolean,
    public val terminal: ReceiverTerminal?,
    public val anomalies: Int,
    public val revision: ULong,
)

/**
 * Whether the receiver keeps an exact history of its public discovery
 * events for one consumer. [Disabled] stages nothing; the inventory still
 * integrates everything. [Exact] keeps at most [Exact.capacity] undelivered
 * events; when the consumer falls further behind, the stream delivers what it
 * kept and then fails with [TrackEventHistoryOverflowException], STREAM-LOCAL:
 * the receiver, its inventory and the native queue are unaffected.
 */
public sealed interface EventHistoryPolicy {
    public data object Disabled : EventHistoryPolicy

    public data class Exact(public val capacity: Int) : EventHistoryPolicy {
        init {
            require(capacity >= 1) { "an exact history keeps at least one event; got $capacity" }
        }
    }
}

/**
 * The public discovery events, in the receiver's own order. Coalesced
 * parse-drop diagnostics and subscription acknowledgements are folded into
 * the [MediaTrack] and never appear here.
 */
public sealed interface TrackEvent {
    public data class Added(public val track: MediaTrack) : TrackEvent

    /** The description was re-snapshotted in place (live to VOD); the track stays present. */
    public data class Updated(public val track: MediaTrack) : TrackEvent

    public data class Removed(public val track: MediaTrack) : TrackEvent

    public data class Ended(public val track: MediaTrack) : TrackEvent

    public data object CatalogReady : TrackEvent
}

/**
 * A single-consumer pull stream. [next] suspends until the next element,
 * returns `null` once the stream has ended cleanly, and throws once it has
 * failed; what a particular stream delivers before an end or a failure, and
 * what can fail it, is that stream's own contract (see
 * [MediaReceiver.trackEvents] and [MediaReceiver.objects]). A second
 * concurrent [next] is refused with [IllegalStateException]: exactly one
 * consumer at a time.
 */
public interface ExactStream<T : Any> {
    public suspend fun next(): T?

    /** The same stream as a cold flow over [next]; the same single-consumer rule applies. */
    public fun asFlow(): Flow<T>
}

/** The declared history could not keep [dropped] events beyond its [capacity]; the kept prefix was delivered first. */
public class TrackEventHistoryOverflowException internal constructor(
    public val capacity: Int,
    public val dropped: ULong,
) : IllegalStateException("the exact track-event history of $capacity lost $dropped event(s)")

/** The receiver reached its fatal terminal; the events before it were delivered first. */
public class ReceiverFatalException internal constructor(
    public val code: ReceiverFatalCode,
) : IllegalStateException("the media receiver failed: $code")

/**
 * The endpoint's interrupt latch is set ([MoqEndpoint.interrupt]), so the
 * object poll was refused before the queue was read (`MOQ_ERR_INTERRUPTED`).
 * Not a terminal: nothing was consumed, and after [MoqEndpoint.resume] the
 * same objects are delivered.
 */
public class ReceiverInterruptedException internal constructor() : IllegalStateException("the endpoint is interrupted; the object poll was refused")

/** Where a subscription joins (`moq_media_start_mode_t`): the live edge, or the next group boundary. */
public enum class StartMode { Current, NextGroup }

/**
 * A track's delivery state (`moq_media_track_state_t`), a point-in-time
 * read. The two pauses are distinct on purpose: [PausedApp] is the
 * application's own [MediaReceiver.unsubscribe] and stays until the app
 * re-enables; [PausedFlow] is FLOW_CONTROL backpressure and resumes by
 * itself. [Ended] covers a peer-ended or rejected track and every track of a
 * terminal receiver.
 */
public enum class TrackState {
    Discovered,
    Pending,
    Active,
    PausedApp,
    PausedFlow,
    Ended,
    ;

    public companion object {
        /** The C value 0..5; anything else fails closed by field name. */
        public fun ofNative(raw: Int): TrackState =
            when (raw) {
                0 -> Discovered
                1 -> Pending
                2 -> Active
                3 -> PausedApp
                4 -> PausedFlow
                5 -> Ended
                else -> throw UnknownNativeValueException("track state", raw)
            }
    }
}

/** The track belongs to another receiver: an argument error, refused before anything native. */
public class ForeignTrackException internal constructor() : IllegalArgumentException("the track was discovered by another receiver")

/** This receiver is closed (its tracks read [MediaTrack.isRetired]), or it reached a clean terminal. */
public class ReceiverClosedException internal constructor() : IllegalStateException("the media receiver is closed")

/**
 * The track can no longer take this operation: [presence] is
 * [TrackPresence.Ended] (a subscribe on an ended track) or
 * [TrackPresence.Removed] (any command, or a state read, on a track a later
 * catalog dropped). Never [TrackPresence.Present].
 */
public class TrackUnavailableException internal constructor(
    public val presence: TrackPresence,
) : IllegalStateException("the track is $presence and cannot take this operation")

/** The track cannot be subscribed at all (`MOQ_ERR_UNSUPPORTED`: an MSF namespace-override track). */
public class TrackNotSubscribableException internal constructor() : IllegalStateException("the track declares its own namespace and cannot be subscribed")

/** A MoQ object Location: the group and object ids, all 64 bits of each (`rec.group` / `rec.object`). */
public data class ObjectLocation(public val groupId: ULong, public val objectId: ULong)

/** CMSF §3.6.1 stream access point type (`moq_sap_type_t`): exactly NONE, 1, 2, 3; `MOQ_SAP_UNKNOWN` is not a value here. */
public enum class SapType {
    None,
    Type1,
    Type2,
    Type3,
    ;

    public companion object {
        /** The C value 0..3; anything else, `MOQ_SAP_UNKNOWN` (0xFF) included, fails closed by field name. */
        public fun ofNative(raw: Int): SapType =
            when (raw) {
                0 -> None
                1 -> Type1
                2 -> Type2
                3 -> Type3
                else -> throw UnknownNativeValueException("sap type", raw)
            }
    }
}

/**
 * One SAP event-timeline record (`moq_media_sap_record_t`): the media
 * [location] of a stream access point on [track], its [sapType], and its
 * earliest presentation time `ept_ms`, in MILLISECONDS.
 */
public class SapRecord internal constructor(
    public val track: MediaTrack,
    public val location: ObjectLocation,
    public val sapType: SapType,
    public val earliestPresentationTime: MediaMillis,
) {
    override fun toString(): String = "SapRecord(${track.description.name}, $location, $sapType, $earliestPresentationTime)"
}

/**
 * One MSF §7.1.1 media-timeline record (`moq_media_timeline_record_t`):
 * the media presentation time `media_time_ms` at a [location] on [track],
 * and the encode [wallclock] -- absent when the native `wallclock_ms` is
 * zero (VOD/unknown), never the epoch.
 */
public class MediaTimelineRecord internal constructor(
    public val track: MediaTrack,
    public val mediaTime: MediaMillis,
    public val location: ObjectLocation,
    public val wallclock: UnixMillis?,
) {
    override fun toString(): String = "MediaTimelineRecord(${track.description.name}, $mediaTime, $location, $wallclock)"
}

/**
 * One element of a record stream ([MediaReceiver.sapRecords],
 * [MediaReceiver.mediaTimeline]): a delivered [Record], or a [Deferral]
 * WITNESS.
 *
 * The native record queues are bounded but never silently lossy: what was
 * admitted is retained until polled, and when a queue is full the rest of
 * an independent object is deferred and counted
 * (`stats.sap_record_deferrals` / `media_timeline_record_deferrals`), to be
 * delivered by the next independent object once capacity is drained. When
 * the stream's queue answers empty and that counter has advanced since the
 * stream last witnessed it, ONE [Deferral] is emitted carrying the counter
 * and what the stream had already witnessed. It is a value the collection
 * continues past, not a failure and not a loss.
 */
public sealed interface ReceiverRecordEvent<out T : Any> {
    public data class Record<T : Any>(public val value: T) : ReceiverRecordEvent<T>

    public data class Deferral(public val deferrals: ULong, public val previouslyWitnessed: ULong) : ReceiverRecordEvent<Nothing>
}

/**
 * One snapshot of the receiver's statistics (`moq_media_receiver_stats_t`),
 * every field in the struct's order. Counters keep all 64 bits. The drop
 * counters are distinct: [objectsDropped] (policy eviction, or an
 * unsubscribed track's purge), [parseDrops] (media-level parse failures),
 * [catalogDrops] (non-fatal catalog-update drops); [sapRecordDeferrals] and
 * [mediaTimelineRecordDeferrals] are the counters the record streams
 * witness. [objectsQueued] and [bytesQueued] are the NATIVE queue at the
 * snapshot: an object already dequeued into a stream's handoff slot is not
 * counted. [catalogComplete] is MSF §5.1.3 completion, latched by an
 * `isComplete` catalog -- not receiver close, not a terminal, not a track's
 * end.
 */
public data class ReceiverStats(
    public val objectsReceived: ULong,
    public val objectsQueued: ULong,
    public val bytesQueued: ULong,
    public val objectsDropped: ULong,
    public val groupsDropped: ULong,
    public val keyframesDropped: ULong,
    public val parseDrops: ULong,
    public val overflowEvents: ULong,
    public val pauseTransitions: ULong,
    public val paused: Boolean,
    public val catalogDrops: ULong,
    public val catalogComplete: Boolean,
    public val sapRecordDeferrals: ULong,
    public val mediaTimelineRecordDeferrals: ULong,
)

/** `moq_object_status_t`: exactly the three values `session.h` declares. */
public enum class ObjectStatus { Normal, EndOfGroup, EndOfTrack }

/** A byte span: [offset] and [length], both non-negative. Never an inclusive range. */
public data class ByteRange(public val offset: Int, public val length: Int) {
    init {
        require(offset >= 0 && length >= 0) { "a byte range is non-negative; got offset $offset length $length" }
    }
}

/** One CMAF sample (`moq_cmaf_sample_t`), exactly as the fragment's `trun` declared it, in the track's timescale ticks. */
public data class CmafSample(
    public val durationTicks: UInt,
    public val sizeBytes: UInt,
    public val flags: UInt,
    public val compositionOffsetTicks: Int,
)

/**
 * Where a received object's media bytes live, by the object's packaging
 * (`moq_media_object_t`): RAW/LOC media is the [Raw.payload]; a CMAF object
 * keeps its full [Cmaf.fragment] (container framing included) with the
 * media in the [Cmaf.mdat] span and the [Cmaf.samples] the fragment
 * declared. A CMAF object's native payload is empty and is never read as
 * media.
 *
 * A [Cmaf] object whose [MediaObject.status] is not [ObjectStatus.Normal]
 * carries NO media at all: its fragment is empty, its [Cmaf.mdat] span is
 * `0, 0`, and it has no samples. That is a legal object -- an end-of-group
 * marker on a CMAF track -- and not a malformed one. An end-of-TRACK status
 * does not reach this stream at all: the service converts it into a track
 * event, so what arrives here as a status object is an end-of-group.
 */
public sealed interface MediaPayload {
    public data class Raw(public val payload: MediaBytes) : MediaPayload

    public data class Cmaf(public val fragment: MediaBytes, public val mdat: ByteRange, public val samples: List<CmafSample>) : MediaPayload
}

/**
 * An object's identity on the wire, as the session delivered it. [group] and
 * [objectId] are the full unsigned 64-bit ids; [subgroup] is the subgroup id
 * when the wire carried one and `null` for a datagram, which has no subgroup
 * (an id of zero is a real subgroup, not an absent one); [publisherPriority]
 * is the effective priority the session delivered, 0..255, which is the
 * header or datagram byte when present and otherwise the session's default
 * for an inherited priority. Identity belongs to the object; its samples
 * have none of their own.
 */
public data class WireIdentity(
    public val group: ULong,
    public val subgroup: ULong?,
    public val objectId: ULong,
    public val publisherPriority: UByte,
)

/**
 * One received media object. It owns nothing native: its bytes were copied
 * on the service worker and the C object was released there, exactly once,
 * before this value existed. [track] is the inventory's own [MediaTrack]
 * instance. [captureTime] is wall clock and absent when the object carried no
 * LOC timestamp at all; [decodeTime] and [presentationTime] are media-timeline
 * positions; [compositionOffset] is signed. The types stay distinct
 * ([UnixMicros] versus [MediaMicros]): no unit conversion happens here.
 *
 * **On a RAW/LOC object the three come from ONE wire scalar.** Such an object's
 * property block carries a single LOC Capture Timestamp (LOC-01 2.3.1.1,
 * defined as an epoch capture instant), and when it is present the parser
 * COPIES that same scalar into [captureTime], [decodeTime] and
 * [presentationTime] -- reinterpreting it in each typed output rather than
 * converting it -- with [compositionOffset] zero. Two facts follow:
 *
 *  * a present [captureTime] is not proof that the publisher supplied a capture
 *    instant. LibMoQ's sender writes the publisher's media presentation scalar
 *    into that field when no capture time was supplied, and which source domain
 *    produced the scalar cannot be identified here;
 *  * the publisher's own media-timeline positions do not survive on this object
 *    alongside an application-supplied capture instant, because one scalar
 *    produced all three fields.
 *
 * A RAW/LOC object with NO timestamp is legal, not malformed: it arrives with
 * [captureTime] null and the timing fields zero, and a consumer that needs a
 * presentation time checks that rather than reading a zero as an instant.
 *
 * A CMAF object is different: its timing comes from the fragment it carries,
 * and none of the paragraph above applies to it.
 *
 * [identity] is the object's [WireIdentity] when the native record covered
 * it, and `null` when it did not (an older native library, or a bridge whose
 * buffer could not hold the tail): `null` is "not carried", which is distinct
 * from an all-zero identity and from an identity without a subgroup.
 */
public class MediaObject internal constructor(
    public val track: MediaTrack,
    public val payload: MediaPayload,
    public val status: ObjectStatus,
    public val endOfGroup: Boolean,
    public val viaDatagram: Boolean,
    public val keyframe: Boolean,
    public val captureTime: UnixMicros?,
    public val decodeTime: MediaMicros,
    public val compositionOffset: MediaOffsetMicros,
    public val presentationTime: MediaMicros,
    public val identity: WireIdentity?,
) {
    /**
     * The decodable media bytes: the RAW payload, or a copy of the CMAF mdat
     * slice. EMPTY for a status object, which carries no media at all.
     */
    public fun mediaBytes(): MediaBytes =
        when (val p = payload) {
            is MediaPayload.Raw -> p.payload
            is MediaPayload.Cmaf -> MediaBytes(p.fragment.bytes().copyOfRange(p.mdat.offset, p.mdat.offset + p.mdat.length))
        }

    override fun toString(): String = "MediaObject(${track.description.name}, $status, ${payload::class.simpleName})"
}

/**
 * A media receiver attached to an endpoint. Discovery runs on the endpoint's
 * service worker: on every service pass the native track-event queue is
 * drained to empty -- before the endpoint's own record is published -- and
 * folded into [inventory]. Nothing the application does or fails to do can
 * therefore fill the native queue, whose overflow would be terminal.
 *
 * The receiver is destroyed by [close], or by the endpoint's own teardown
 * BEFORE the endpoint is stopped (the C stop refuses while a receiver is
 * attached). Once closed, the endpoint's receiver slot is free again, the
 * inventory freezes with [ReceiverTerminal.Closed], and every track reads
 * [MediaTrack.isRetired].
 */
public class MediaReceiver internal constructor(
    private val engine: EndpointEngine,
    private val token: Long,
    model: ReceiverModel,
) {
    /** True once this receiver has been destroyed, by [close] or by the endpoint's teardown. */
    public val isClosed: Boolean
        get() = !engine.isReceiverAttached(token)

    /** The durable discovered state, one atomic snapshot per change. */
    public val inventory: StateFlow<ReceiverInventory> = model.inventory

    /**
     * The exact history of public discovery events, present only when
     * [ReceiverConfig.eventHistory] armed one at attach; `null` otherwise.
     * It delivers every event it kept, in order, and at the first event it
     * could not keep it fails with [TrackEventHistoryOverflowException] after
     * that kept prefix, permanently: nothing after the gap is ever delivered,
     * every later `next()` and fresh collection fails the same way, and the
     * exception counts what was not delivered (saturating, never wrapping).
     * The stream stays failed; the inventory and the receiver do not. The
     * receiver's terminal ends it only once the native track queue is
     * exhausted: `null` when clean, [ReceiverFatalException] when fatal,
     * after the kept events and never ahead of a latched failure.
     */
    public val trackEvents: ExactStream<TrackEvent>? = model.history

    /**
     * The received media objects, PULLED: one [ExactStream.next] is one
     * `moq_media_receiver_poll_object` attempt on the service worker,
     * preceded by the ordinary track-event drain, and followed by another
     * drain when the object's track is not yet in [inventory] (its ADDED is
     * queued before the object, but may not have been drained yet) -- so a
     * delivered object's [MediaTrack] is always the inventory's instance. Nothing polls objects on a service pass and
     * no JVM object queue exists: what the consumer has not pulled sits in the
     * C receiver under its own overflow policy. The one exception is the
     * handoff of an object whose demand was cancelled after the poll had
     * begun -- it is staged, never lost, and the next demand takes it without
     * polling. An empty answer is retained by the worker, which re-polls on
     * its own later passes and resumes the consumer when something can be
     * delivered; waiting publishes no wake. [ReceiverInterruptedException]
     * while the endpoint is interrupted (nothing consumed); `null` once THIS
     * queue answered its end (empty and terminal) or the receiver was
     * closed; [ReceiverFatalException] on its fatal terminal -- in each case
     * after the objects the C receiver still held, whatever another queue
     * reported first: a terminal is one reason, exhaustion is per queue.
     */
    public val objects: ExactStream<MediaObject> = ObjectStream(engine, token, model)

    /**
     * The SAP event-timeline records (`moq_media_receiver_poll_sap`), PULLED
     * exactly like [objects]: one [ExactStream.next] is one native poll on the
     * service worker behind the ordinary track drain (and another drain when
     * the record's track is not yet in [inventory]); an empty answer parks the
     * consumer until the worker's next pass. Each poll also reads
     * `stats.sap_record_deferrals` in the same bridge row, under the same
     * registry pin, after the poll -- so the counter is never older than the
     * queue it describes -- and a [ReceiverRecordEvent.Deferral] is emitted
     * when an empty answer finds it advanced. `null` once THIS queue answered
     * its end (empty and terminal) or the receiver was closed;
     * [ReceiverFatalException] on its fatal terminal -- after the records
     * still owed, whatever another queue reported first. The native poll
     * does not consult the endpoint's interrupt latch.
     */
    public val sapRecords: ExactStream<ReceiverRecordEvent<SapRecord>> = RecordStream(engine, token, model, RecordStream.SAP)

    /** The MSF media-timeline records (`moq_media_receiver_poll_media_timeline`), with the same contract as [sapRecords] and their own deferral counter. */
    public val mediaTimeline: ExactStream<ReceiverRecordEvent<MediaTimelineRecord>> = RecordStream(engine, token, model, RecordStream.TIMELINE)

    private val model: ReceiverModel = model

    /**
     * Ask for [track]'s delivery (`moq_media_receiver_subscribe_track`), on
     * the service worker. An ASYNCHRONOUS command: success means the intent
     * was validated and recorded, never that the peer accepted -- objects
     * arrive through [objects] once it has, and a rejection is the track's
     * ENDED event. Subscribing an already-wanted track is a no-op; after an
     * [unsubscribe], the next subscribe is a new command carrying THIS call's
     * [start] and [priority] (the C applies them only when the underlying
     * subscription is first issued; an existing one is not reconfigured).
     * [priority] `null` sends no priority.
     *
     * @throws ForeignTrackException the track is another receiver's
     * @throws ReceiverClosedException this receiver is closed, or reached a clean terminal
     * @throws ReceiverFatalException the receiver reached its fatal terminal
     * @throws TrackUnavailableException the track is Ended or Removed
     * @throws TrackNotSubscribableException the track cannot be subscribed
     */
    public suspend fun subscribe(track: MediaTrack, start: StartMode = StartMode.Current, priority: UByte? = null) {
        own(track)
        engine.subscribeTrack(token, model, track, start, priority)
    }

    /**
     * Stop [track]'s delivery (`moq_media_receiver_unsubscribe_track`): its
     * queued objects are purged and delivery is paused, not torn down, so a
     * later [subscribe] resumes it cheaply; the track and its catalog handle
     * stay valid. A no-op on a track that is not wanted, or that has ended.
     *
     * @throws ForeignTrackException the track is another receiver's
     * @throws ReceiverClosedException this receiver is closed, or reached a clean terminal
     * @throws ReceiverFatalException the receiver reached its fatal terminal
     * @throws TrackUnavailableException the track is Removed
     */
    public suspend fun unsubscribe(track: MediaTrack) {
        own(track)
        engine.unsubscribeTrack(token, model, track)
    }

    /**
     * [track]'s current delivery state (`moq_media_receiver_track_state`), a
     * point-in-time read on the service worker. Every track of a receiver
     * that reached its terminal reads [TrackState.Ended].
     *
     * @throws ForeignTrackException the track is another receiver's
     * @throws ReceiverClosedException this receiver is closed
     * @throws TrackUnavailableException the track is Removed
     */
    public suspend fun trackState(track: MediaTrack): TrackState {
        own(track)
        return engine.trackState(token, model, track)
    }

    /**
     * One snapshot of the receiver's statistics
     * (`moq_media_receiver_get_stats`), read on the service worker on demand.
     * There is no stats flow: the C read is itself a point-in-time snapshot
     * with no change signal, and the queue-depth fields move on every pull
     * and every intake, so a value refreshed on the engine's pass cadence
     * would only pretend to be current. A receiver at its terminal still
     * answers, as the C does.
     *
     * @throws ReceiverClosedException this receiver is closed
     */
    public suspend fun statsSnapshot(): ReceiverStats {
        if (isClosed) throw ReceiverClosedException()
        return engine.receiverStats(token)
    }

    private fun own(track: MediaTrack) {
        if (model.resolve(track.token) !== track) throw ForeignTrackException()
        if (isClosed) throw ReceiverClosedException()
    }

    /**
     * Destroys the native receiver on the endpoint's service worker and frees
     * the endpoint's receiver slot. Idempotent: a second call, or a call after
     * the endpoint tore the receiver down, does nothing. After the endpoint is
     * closed there is nothing left to close either.
     */
    public suspend fun close() {
        try {
            engine.closeReceiver(token)
        } catch (_: EngineClosedException) {
            // The endpoint's teardown already destroyed the receiver.
        }
    }
}

/** The endpoint refused the attach with its own `moq_result_t`, carried exactly (a second receiver: WRONG_STATE; terminal: CLOSED). */
public class ReceiverAttachFailedException internal constructor(
    public val code: MoqResultCode,
) : IllegalStateException("the endpoint refused to attach a receiver: $code")
