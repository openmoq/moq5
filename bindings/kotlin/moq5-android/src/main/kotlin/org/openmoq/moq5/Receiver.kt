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

/**
 * `moq_media_packaging_t`, the STRUCTURED packaging the media tier derived; the catalog's own `packaging` string stays on [TrackDescription.packaging].
 * [Locmaf] is a track's packaging only (draft-einarsson-moq-locmaf-01): the service rebuilds its Objects as canonical CMAF chunks, so they arrive as [MediaPayload.Cmaf].
 */
public enum class PackagingKind { Raw, Cmaf, Locmaf }

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
 * Where a track's decoder configuration comes from
 * (`moq_media_track_desc_t.init_source`, `MOQ_MEDIA_INIT_SOURCE_*`). An open
 * set: a value this binding does not know is carried as [Unknown], never
 * folded into a known case.
 */
public sealed interface InitSource {
    /** The catalog names no configuration source. */
    public data object None : InitSource

    /** From the catalog itself: [TrackDescription.initData] and the codec configuration. */
    public data object Inline : InitSource

    /**
     * In band, in the property [TrackDescription.initPropertyType] names, on
     * the track (MSF-02 5.1.7, 12.2): observable on each object's metadata
     * from the first object on, not at discovery.
     */
    public data object TrackProperty : InitSource

    /** In band, in the property [TrackDescription.initPropertyType] names, on each object. */
    public data object ObjectProperty : InitSource

    /** A source this binding does not know, by its native value. */
    public data class Unknown(public val raw: UInt) : InitSource

    public companion object {
        /** The C value: 0..3 are the named sources, anything else is [Unknown]. */
        public fun ofNative(raw: UInt): InitSource =
            when (raw) {
                0u -> None
                1u -> Inline
                2u -> TrackProperty
                3u -> ObjectProperty
                else -> Unknown(raw)
            }
    }
}

/**
 * A track's MSF encryption fields (MSF 5.2.38-5.2.41), verbatim from the
 * catalog; each is null when the catalog did not carry it.
 *
 * A present [scheme] means the object payload is encrypted and this library
 * does not decrypt it: a "loc" track is delivered as received (ciphertext,
 * with its LOC properties in plaintext), and a track of any other packaging
 * cannot be subscribed. [cipherSuite], [keyId] and [trackBaseKey] are what an
 * application needs to decrypt.
 *
 * [trackBaseKey] is KEY MATERIAL (MSF-02 13.2): handle it as a secret.
 * [toString] never prints it, only whether it is present; do not log it or
 * the [MediaBytes] it is.
 */
public class TrackEncryption(
    public val scheme: MediaBytes? = null,
    public val cipherSuite: MediaBytes? = null,
    public val keyId: MediaBytes? = null,
    public val trackBaseKey: MediaBytes? = null,
) {
    override fun equals(other: Any?): Boolean =
        other is TrackEncryption && scheme == other.scheme && cipherSuite == other.cipherSuite && keyId == other.keyId && trackBaseKey == other.trackBaseKey

    override fun hashCode(): Int {
        var h = scheme.hashCode()
        h = 31 * h + cipherSuite.hashCode()
        h = 31 * h + keyId.hashCode()
        return 31 * h + trackBaseKey.hashCode()
    }

    override fun toString(): String =
        "TrackEncryption(scheme=$scheme, cipherSuite=$cipherSuite, keyId=$keyId, trackBaseKey=${if (trackBaseKey == null) "null" else "<redacted>"})"
}

/**
 * An immutable snapshot of a track's description (`moq_media_track_desc_t`,
 * every field of its v0 layout except the media template, which is deferred
 * with the SAP/timeline surface, and the generation block appended after it).
 *
 * Byte fields: [name] is the track's identity and is always present, even
 * when empty. The other byte fields are optional catalog attributes with no
 * separate presence bit in C, so an EMPTY native span is `null` here.
 * Numeric catalog attributes carry their own presence bit in C and are
 * `null` exactly when it is clear. The catalog's passthrough dimensions
 * ([catalogWidth], [catalogHeight], [catalogSamplerate]) are distinct from
 * the init-derived ones in [init]: the catalog claims, the init segment
 * proves.
 *
 * [configGeneration] is the description's generation: 0 for the one the
 * track was discovered with, n for the one the n-th MSF-02 advisory change
 * published. A snapshot is the CURRENT generation when it was taken, so it may
 * be newer than the generation a [TrackEvent.Updated] announced. It,
 * [initSource] and [initPropertyType] are null when the native library did
 * not report them (a shorter description), never a zero. [encryption] is null
 * when the catalog carried none of the encryption fields.
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
    public val configGeneration: UInt? = null,
    public val initSource: InitSource? = null,
    /**
     * The property type that carries the configuration for
     * [InitSource.TrackProperty] and [InitSource.ObjectProperty] (0x0D
     * VIDEO_CONFIG, 0x0F AUDIO_CONFIG); null for every other source, and when
     * not reported.
     */
    public val initPropertyType: ULong? = null,
    public val encryption: TrackEncryption? = null,
)

/**
 * A track's durable catalog state. `Present` is a track the catalog lists;
 * `Removed` was dropped by a later catalog generation, or replaced by a new
 * track (see [MediaTrack.replacedBy]); `Ended` was ended or
 * rejected by the peer; `Failed` failed LOCALLY in this receiver (initial ACK
 * retention, LIVE delivery admission or UPDATE admission; a track whose state
 * read answers [TrackState.Failed] is folded in the same way). No terminal
 * state discards the track: the C handle stays valid, inert, until the
 * receiver is destroyed, and so does the [MediaTrack]. A failed track stays
 * `Failed` when it later ends, as the C keeps it FAILED; only a catalog
 * removal replaces it. A live-to-VOD conversion and an MSF-02 advisory change
 * are EVENTS ([TrackEvent.Updated]) that re-snapshot the description and leave
 * the track `Present`. A fill child's outcome never changes its parent's
 * presence.
 *
 * An MSF-02 catalog change that decides how objects are parsed or delivered
 * (packaging, role, codec, timescale, the resolved init, the content
 * protection, the stream mapping, `encryptionScheme`, ...) REPLACES a track: the
 * old track becomes `Removed` ([TrackEvent.Removed] with `replaced`), and its
 * successor is a new track, announced right after it ([TrackEvent.Added] with
 * `replaces`). The two are linked through [MediaTrack.replacedBy] and
 * [MediaTrack.replaces]. The old track's selection moves to its successor,
 * and a [MediaReceiver.subscribe] or [MediaReceiver.unsubscribe] issued on
 * the old track acts on the newest track of its replacement chain.
 */
public enum class TrackPresence { Present, Removed, Ended, Failed }

/**
 * One discovered track: an identity, not a value. The same native handle is
 * always the same instance, so a track may be used as a map key and compared
 * with `===`. Its [description] is an immutable snapshot, replaced on the
 * changes the C tier sanctions (live to VOD, and an MSF-02 advisory
 * generation); [presence] and [parseDrops] are the durable state the
 * receiver's events fold into it. Every field is written on the endpoint's
 * service worker and read from any thread.
 *
 * [replaces] and [replacedBy] link the tracks of a replacement chain (see
 * [TrackPresence]): [replaces] is set when the track is announced as a
 * successor, [replacedBy] once that successor has been announced. A replaced
 * track whose successor was never announced (the receiver was closed first)
 * keeps a null [replacedBy].
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

    /** The track this one replaced, or null when it was not announced as a successor. */
    @Volatile
    public var replaces: MediaTrack? = null
        internal set

    /** The track that replaced this one, once its successor was announced; null otherwise. */
    @Volatile
    public var replacedBy: MediaTrack? = null
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
 *
 * A field the native event did not carry (a library that stamped a shorter
 * event) is null: unobserved, never a zero. A local failure is a result of
 * this receiver ([MoqResultCode]), never a peer error or an END_OF_TRACK.
 */
public sealed interface TrackEvent {
    /**
     * A new track. [replaces] is the track it replaced (an MSF-02 critical
     * change), announced immediately before as [Removed] with `replaced`; null
     * for any other new track.
     */
    public data class Added(public val track: MediaTrack, public val replaces: MediaTrack? = null) : TrackEvent

    /**
     * The track's description changed and was re-snapshotted; the track stays
     * present. Either a live-to-VOD conversion, written on the current
     * generation, or an MSF-02 advisory change, which publishes generation
     * [configGeneration]. The [MediaTrack.description] snapshot is the current
     * generation when the event was drained, so its own
     * [TrackDescription.configGeneration] may be newer than [configGeneration]
     * (a later change was already adopted).
     */
    public data class Updated(public val track: MediaTrack, public val configGeneration: UInt) : TrackEvent

    /**
     * The track left the catalog. [replaced] is true when a new track replaced
     * it (an MSF-02 critical change): its successor's [Added] follows, and
     * links the two ([MediaTrack.replacedBy]).
     */
    public data class Removed(public val track: MediaTrack, public val replaced: Boolean = false) : TrackEvent

    public data class Ended(public val track: MediaTrack) : TrackEvent

    public data object CatalogReady : TrackEvent

    /**
     * The track's initial ACK metadata could not be retained locally
     * (`MOQ_MEDIA_TRACK_LOCAL_FAILURE`; [result] is the exact `MOQ_ERR_NOMEM`
     * or `MOQ_ERR_BUFFER`). The track is [TrackPresence.Failed]; other
     * tracks continue.
     */
    public data class LocalFailure(public val track: MediaTrack, public val result: MoqResultCode?) : TrackEvent

    /**
     * Local delivery capacity refused an object (`MOQ_MEDIA_TRACK_DELIVERY_LOCAL_FAILURE`;
     * [result] is `MOQ_ERR_BUFFER`). [DeliveryOrigin.Live] fails the track
     * ([TrackPresence.Failed]); [DeliveryOrigin.Fill] fails only that fill
     * child and the track carries on. Objects already accepted stay deliverable.
     * A null [origin] was not carried, so the track's presence is left as it is.
     */
    public data class DeliveryLocalFailure(
        public val track: MediaTrack,
        public val result: MoqResultCode?,
        public val origin: DeliveryOrigin?,
    ) : TrackEvent

    /**
     * A local UPDATE admission failed permanently (`MOQ_MEDIA_TRACK_UPDATE_LOCAL_FAILURE`;
     * [result] is the exact local error, not a peer response). The track is
     * [TrackPresence.Failed].
     */
    public data class UpdateLocalFailure(public val track: MediaTrack, public val result: MoqResultCode?) : TrackEvent

    /**
     * One fill child of [track] ended (`MOQ_MEDIA_TRACK_FILL_TERMINAL`); the
     * track's own state is unchanged. [initiatingRequestId] is the SUBSCRIBE
     * or UPDATE that asked for the fill (zero is a valid ID). Only
     * [FillEndReason.Complete] is a success: a [FillErrorDomain.Stream] code
     * of zero is still a failure. [unreportedLoss] is the fill's loss that no
     * delivered object reported (MPEG-TS media fills only; [FillLoss.None]
     * otherwise).
     */
    public data class FillTerminal(
        public val track: MediaTrack,
        public val initiatingRequestId: RequestId?,
        public val reason: FillEndReason?,
        public val errorDomain: FillErrorDomain?,
        public val errorCode: MoqErrorCode?,
        public val unreportedLoss: FillLoss?,
    ) : TrackEvent

    /**
     * A fill of [track] was admitted locally (`MOQ_MEDIA_TRACK_FILL_CREATED`):
     * its UPDATE was sent. Not the peer's acceptance. [initiatingRequestId]
     * is that UPDATE's ID; zero is valid.
     */
    public data class FillCreated(public val track: MediaTrack, public val initiatingRequestId: RequestId?) : TrackEvent
}

/** Whose delivery a [TrackEvent.DeliveryLocalFailure] refused (`MOQ_MEDIA_DELIVERY_*`). */
public sealed interface DeliveryOrigin {
    /** The track's own live delivery: the track has failed. */
    public data object Live : DeliveryOrigin

    /** One fill child, by the request that asked for it (null when not carried; zero is valid). Its parent continues. */
    public data class Fill(public val initiatingRequestId: RequestId?) : DeliveryOrigin
}

/** Why a fill ended (`MOQ_FILL_*`, `session.h`). Only [Complete] is a success. */
public enum class FillEndReason(internal val native: Long) {
    Complete(1),
    NoStream(2),
    LocalCancelled(3),
    PeerCancelledOrReset(4),
    LocalAborted(5),
    ParentRejected(6),
    ParentTerminated(7),
    ;

    internal companion object {
        fun ofNative(raw: Long): FillEndReason? = entries.firstOrNull { it.native == raw }
    }
}

/**
 * The domain a fill's [TrackEvent.FillTerminal.errorCode] belongs to
 * (`MOQ_FILL_ERROR_*`): a stream reset code, the parent request's error, or
 * the parent's done status. [None] carries no code.
 */
public enum class FillErrorDomain(internal val native: Long) {
    None(0),
    Stream(1),
    ParentRequest(2),
    ParentDone(3),
    ;

    internal companion object {
        fun ofNative(raw: Long): FillErrorDomain? = entries.firstOrNull { it.native == raw }
    }
}

/** One cause of a fill's unreported loss (`MOQ_MEDIA_FILL_LOSS_*`). */
public enum class FillLossCause(internal val bit: Long) {
    /** This receiver dropped objects of the fill. */
    LocalDrop(0x1),

    /** The fill ended with an End of Unknown Range. */
    UnknownRange(0x2),

    /** The fill ended with an End of Timed-Out Range. */
    TimedOutRange(0x4),
}

/**
 * A fill's coalesced loss that no delivered object reported. It is not a
 * list of gaps: [Lost.from] is where the contribution earliest in the fill's
 * delivery order starts.
 */
public sealed interface FillLoss {
    public data object None : FillLoss

    public data class Lost(public val causes: Set<FillLossCause>, public val from: ObjectLocation) : FillLoss
}

/** The C fill-loss block: no causes is [FillLoss.None]; a bit outside the C set is refused (null). */
internal fun fillLossOfNative(causes: Long, group: Long, objectId: Long): FillLoss? {
    if (causes == 0L) return FillLoss.None
    if (causes and FillLossCause.entries.fold(0L) { m, c -> m or c.bit }.inv() != 0L) return null
    return FillLoss.Lost(FillLossCause.entries.filter { causes and it.bit != 0L }.toSet(), ObjectLocation(group.toULong(), objectId.toULong()))
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
 * itself. [Ended] covers a peer-ended or rejected track and every other track
 * of a terminal receiver. [Failed] is a local failure of this receiver (ACK
 * retention, LIVE delivery, UPDATE admission or credentials); it stays
 * failed, also once the receiver is terminal, and is never rearmed.
 */
public enum class TrackState {
    Discovered,
    Pending,
    Active,
    PausedApp,
    PausedFlow,
    Ended,
    Failed,
    ;

    public companion object {
        /** The C value 0..6; anything else fails closed by field name. */
        public fun ofNative(raw: Int): TrackState =
            when (raw) {
                0 -> Discovered
                1 -> Pending
                2 -> Active
                3 -> PausedApp
                4 -> PausedFlow
                5 -> Ended
                6 -> Failed
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
 * [TrackPresence.Ended] or [TrackPresence.Failed] (a subscribe on an ended or
 * locally failed track) or [TrackPresence.Removed] (any command, or a state
 * read, on a track a later catalog dropped; a command on a replaced track acts
 * on its successor instead, and a state read on it is refused). For a
 * command on a replaced track, [presence] is that of the track it acted on.
 * Never [TrackPresence.Present].
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
 * One snapshot of the receiver's statistics (`moq_media_receiver_stats_t`),
 * every field through `loc_order_forced` in the struct's order; the LOCMAF
 * counters after it are not carried. Counters keep all 64 bits. The drop
 * counters are distinct: [objectsDropped] (policy eviction, or an
 * unsubscribed track's purge), [parseDrops] (media-level parse failures),
 * [catalogDrops] (non-fatal catalog-update drops). [gapsReported] counts
 * the loss reports [MediaReceiver.objects] handed out, [gapTailsUnknown] the
 * subset whose tail is unknown, [reorderRefused] the objects a reorder
 * window refused as late or duplicated, and [tracksMalformed] the tracks
 * whose subscription ended because the track was malformed. [objectsQueued] and [bytesQueued] are the NATIVE queue at the
 * snapshot: an object already dequeued into a stream's handoff slot is not
 * counted. [catalogComplete] is MSF §5.1.3 completion, latched by an
 * `isComplete` catalog -- not receiver close, not a terminal, not a track's
 * end.
 *
 * The fields from [catalogRevision] on are one block: each is null when the
 * native library did not report it (a shorter stats record), never a zero.
 * [catalogRevision] is the effective catalog's MSF revision, meaningful once
 * the inventory is [ReceiverInventory.catalogReady] (before that the native
 * zero reads as [CatalogRevision.Msf01]). The compression counters (MSF-02
 * 12.1) count catalog and media-timeline objects: [objectsDecompressed],
 * [decompressFailures] (malformed compressed data) and [decompressLimited]
 * (over the decompressed-size cap); [compressionRefused] (an algorithm this
 * build does not decompress, a repeated or malformed property, or a
 * compressed MSF-01 catalog), [compressionConflicts] (an object property
 * where the track property is set) and [compressionIgnored] (a timeline's
 * property under an MSF-01 catalog). [eventTimelineHeadersIgnored] counts
 * SAP-timeline objects whose origin carried an event-timeline header.
 * [locOrderGaps] counts objects the in-group ordering stage of an
 * "object"-mapped LOC track gave up on (a policy, not proof the network lost
 * them), and [locOrderForced] the advances its capacity forced.
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
    public val gapsReported: ULong,
    public val gapTailsUnknown: ULong,
    public val reorderRefused: ULong,
    public val tracksMalformed: ULong,
    public val catalogRevision: CatalogRevision? = null,
    public val objectsDecompressed: ULong? = null,
    public val decompressFailures: ULong? = null,
    public val decompressLimited: ULong? = null,
    public val compressionRefused: ULong? = null,
    public val compressionConflicts: ULong? = null,
    public val compressionIgnored: ULong? = null,
    public val eventTimelineHeadersIgnored: ULong? = null,
    public val locOrderGaps: ULong? = null,
    public val locOrderForced: ULong? = null,
)

/**
 * The MSF revision a catalog follows (`moq_msf_revision_t`,
 * `MOQ_MSF_REVISION_*`). An open set: a value this binding does not know is
 * carried as [Unknown].
 */
public sealed interface CatalogRevision {
    /** draft-ietf-moq-msf-01 (the native zero). */
    public data object Msf01 : CatalogRevision

    /** draft-ietf-moq-msf-02. */
    public data object Msf02 : CatalogRevision

    /** A revision this binding does not know, by its native value. */
    public data class Unknown(public val raw: UInt) : CatalogRevision

    public companion object {
        /** The C value: 0 and 1 are the named revisions, anything else is [Unknown]. */
        public fun ofNative(raw: UInt): CatalogRevision =
            when (raw) {
                0u -> Msf01
                1u -> Msf02
                else -> Unknown(raw)
            }
    }
}

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
     * consumer until the worker's next pass. The native record queue is
     * bounded and drops its oldest record under sustained non-polling; a
     * record is delivered at most once. `null` once THIS queue answered
     * its end (empty and terminal) or the receiver was closed;
     * [ReceiverFatalException] on its fatal terminal -- after the records
     * still owed, whatever another queue reported first. The native poll
     * does not consult the endpoint's interrupt latch.
     */
    public val sapRecords: ExactStream<SapRecord> = RecordStream(engine, token, model, RecordStream.SAP)

    /** The MSF media-timeline records (`moq_media_receiver_poll_media_timeline`), with the same contract as [sapRecords]. */
    public val mediaTimeline: ExactStream<MediaTimelineRecord> = RecordStream(engine, token, model, RecordStream.TIMELINE)

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
     * On a track a replacement removed ([MediaTrack.replacedBy]) the command
     * acts on the newest track of its replacement chain, and whether that
     * track is already wanted decides whether a command is sent; the
     * replacement already moved the old track's selection there.
     *
     * @throws ForeignTrackException the track is another receiver's
     * @throws ReceiverClosedException this receiver is closed, or reached a clean terminal
     * @throws ReceiverFatalException the receiver reached its fatal terminal
     * @throws TrackUnavailableException the track (or the newest track of its replacement chain) is Ended, Failed or Removed
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
     * stay valid. A no-op on a track that is not wanted, or that has ended;
     * a locally failed track is still disabled. On a replaced track it acts
     * on the newest track of the replacement chain, as [subscribe] does.
     *
     * @throws ForeignTrackException the track is another receiver's
     * @throws ReceiverClosedException this receiver is closed, or reached a clean terminal
     * @throws ReceiverFatalException the receiver reached its fatal terminal
     * @throws TrackUnavailableException the track is Removed and not replaced, or the newest track of its replacement chain is Removed
     */
    public suspend fun unsubscribe(track: MediaTrack) {
        own(track)
        engine.unsubscribeTrack(token, model, track)
    }

    /**
     * [track]'s current delivery state (`moq_media_receiver_track_state`), a
     * point-in-time read on the service worker. Every track of a receiver
     * that reached its terminal reads [TrackState.Ended], except a locally
     * failed one, which keeps [TrackState.Failed]: that is still read from the
     * native receiver while it is attached, so a failure no event reported is
     * not lost. A [TrackState.Failed] read is also folded into the track's
     * [MediaTrack.presence]. A replaced track has no state of its own: read
     * its successor's.
     *
     * @throws ForeignTrackException the track is another receiver's
     * @throws ReceiverClosedException this receiver is closed
     * @throws TrackUnavailableException the track is Removed, replaced included
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
