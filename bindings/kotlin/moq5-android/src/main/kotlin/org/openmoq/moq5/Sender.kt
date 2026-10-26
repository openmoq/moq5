package org.openmoq.moq5

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeoutOrNull
import org.openmoq.moq5.engine.Demanded
import org.openmoq.moq5.engine.EndpointEngine
import org.openmoq.moq5.engine.EngineClosedException
import org.openmoq.moq5.engine.SenderModel
import org.openmoq.moq5.engine.WriteAttempt
import kotlin.time.Duration
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.nanoseconds

/**
 * What the sender does when its bounded send queue is full
 * (`moq_media_send_backpressure_t`). A REQUIRED choice: the C refuses an
 * unset policy. The two drop policies evict whole groups wire-correctly at
 * the C and never block a writer; [ReturnWouldBlock] never drops and answers
 * [WriteOutcome.WouldBlock] at once, which [MediaSender.write] turns into a
 * pass-driven retry bounded by the caller's timeout -- the drop decision is
 * then the application's, never the service's.
 *
 * The C `BLOCK_TIMEOUT` policy has no spelling here on purpose: it parks the
 * calling thread inside C, and on Android every native call runs on the
 * endpoint's service worker, so one parked write would stall every other
 * operation on that endpoint and could not be cancelled.
 */
public enum class SendBackpressure {
    /** Evict whole groups from the oldest end, always retaining the most recent sync point. */
    DropToKeyframe,

    /** Evict whole groups. */
    DropGroup,

    /** Never drop: a full queue answers [WriteOutcome.WouldBlock] immediately. */
    ReturnWouldBlock,
}

/**
 * The automatic independent catalog refresh cadence
 * (`catalog_refresh_interval_us`): while the catalog track has demand the
 * sender periodically republishes the complete catalog as a new independent
 * group, so a late joiner behind a relay still bootstraps. [Default] is the
 * library's one second; [Every] a positive, finite, whole-microsecond
 * interval; [Disabled] switches it off (with the documented late-join
 * caveat).
 */
public sealed interface CatalogRefresh {
    /** The library default interval (one second). */
    public data object Default : CatalogRefresh

    /** No automatic refresh. */
    public data object Disabled : CatalogRefresh

    /** A custom interval: positive, finite, and a whole number of microseconds. */
    public data class Every(public val interval: Duration) : CatalogRefresh {
        init {
            require(interval.isFinite() && interval.isPositive()) { "a catalog refresh interval is positive and finite: $interval" }
            require(interval == interval.inWholeMicroseconds.microseconds) { "a catalog refresh interval is a whole number of microseconds: $interval" }
        }
    }
}

/** The lowering to `catalog_refresh_interval_us` through the signed carrier: 0 = default, `UINT64_MAX` (-1) = disabled, else whole microseconds. */
internal fun CatalogRefresh.toNativeMicros(): Long =
    when (this) {
        CatalogRefresh.Default -> 0L
        CatalogRefresh.Disabled -> -1L
        is CatalogRefresh.Every -> interval.inWholeMicroseconds
    }

/**
 * The sender configuration (`moq_media_sender_cfg_t`) for
 * [MoqEndpoint.attachSender].
 *
 * [namespace] is what to announce: at least one part, each non-empty (the
 * part rule is [NamespacePart]'s own). [backpressure] is the required
 * policy. [catalogTrack] is the catalog's TRACK NAME (`catalog_track` is a
 * `moq_bytes_t` track name, not a namespace field): null means the C
 * default, the track named "catalog"; an explicit empty name is refused.
 * The queue bounds are stated as the values the C resolves `0` to (the send
 * queue and the pre-ready queue: 256 objects, 32 MiB each; whole-sender
 * budgets shared by every track, not per-track). [validateCmaf] is strict
 * by default, as every C initializer sets it. [publishTracks] selects push
 * mode. [dropWithoutDemand] false holds media queued until a subscriber
 * appears; true drops it to stay at the live edge. [catalogRefresh] is the
 * automatic republish cadence.
 */
public data class SenderConfig(
    public val namespace: Namespace,
    public val backpressure: SendBackpressure,
    public val catalogTrack: TrackName? = null,
    public val queueMaxObjects: UInt = 256u,
    public val queueMaxBytes: UInt = 32u * 1024u * 1024u,
    public val preReadyMaxObjects: UInt = 256u,
    public val preReadyMaxBytes: UInt = 32u * 1024u * 1024u,
    public val validateCmaf: Boolean = true,
    public val publishTracks: Boolean = false,
    public val dropWithoutDemand: Boolean = false,
    public val catalogRefresh: CatalogRefresh = CatalogRefresh.Default,
) {
    init {
        require(namespace.partCount > 0) { "a sender namespace names at least one part; the generic empty Namespace cannot be announced" }
        require(catalogTrack == null || catalogTrack.size > 0) { "catalogTrack is a non-empty track name, or null for the C default \"catalog\"" }
        require(queueMaxObjects > 0u) { "queueMaxObjects is positive (the C default is 256)" }
        require(queueMaxBytes > 0u) { "queueMaxBytes is positive (the C default is 32 MiB)" }
        require(preReadyMaxObjects > 0u) { "preReadyMaxObjects is positive (the C default is 256)" }
        require(preReadyMaxBytes > 0u) { "preReadyMaxBytes is positive (the C default is 32 MiB)" }
    }
}

/** The C `moq_media_send_backpressure_t` ordinal; UNSET (0) and BLOCK_TIMEOUT (3) are unrepresentable. */
internal fun SendBackpressure.nativeOrdinal(): Int =
    when (this) {
        SendBackpressure.DropToKeyframe -> 1
        SendBackpressure.DropGroup -> 2
        SendBackpressure.ReturnWouldBlock -> 4
    }

/** The C `moq_media_type_t` ordinal. */
internal fun MediaType.nativeOrdinal(): Int =
    when (this) {
        MediaType.Video -> 1
        MediaType.Audio -> 2
    }

/** The C `moq_media_packaging_t` ordinal. */
internal fun PackagingKind.nativeOrdinal(): Int =
    when (this) {
        PackagingKind.Raw -> 1
        PackagingKind.Cmaf -> 2
    }

/**
 * One media track to add (`moq_media_track_cfg_t`): enough to derive its
 * catalog entry and package its media. The service builds and publishes the
 * catalog; the application never hand-builds catalog JSON.
 *
 * The MSF-required fields are refused at construction as the C would refuse
 * them (`INVAL`): a non-empty [name] and [codec], a nonzero [bitrate], and
 * for audio a nonzero [samplerate] and non-empty [channelConfig]. A
 * [trackDuration] (MSF §5.2.35, VOD-from-start) is refused together with
 * [isLive]; a non-live track without one is accepted, as the C accepts it.
 * Defaults are stated as the values the C resolves `0` to: [timescale]
 * microseconds, eight history groups for either generated timeline. The
 * `has_*` fields of the C are the nullability of [maxGroupSap],
 * [maxObjectSap], [trackDuration] and [altGroup]. Content-protection
 * reference ids (CMSF §4.1.2) are a later slice.
 */
public data class SendTrackConfig(
    public val name: TrackName,
    public val mediaType: MediaType,
    public val packaging: PackagingKind,
    public val codec: MediaBytes,
    public val bitrate: ULong,
    public val timescale: UInt = 1_000_000u,
    public val initData: MediaBytes? = null,
    public val role: MediaBytes? = null,
    public val lang: MediaBytes? = null,
    public val isLive: Boolean = true,
    public val width: UInt = 0u,
    public val height: UInt = 0u,
    public val framerateMillis: ULong = 0uL,
    public val samplerate: UInt = 0u,
    public val channelConfig: MediaBytes? = null,
    public val maxGroupSap: UInt? = null,
    public val maxObjectSap: UInt? = null,
    public val emitSapTimeline: Boolean = false,
    public val sapTimelineHistoryGroups: UInt = 8u,
    public val trackDuration: MediaMillis? = null,
    public val emitMediaTimeline: Boolean = false,
    public val mediaTimelineHistoryGroups: UInt = 8u,
    public val altGroup: Int? = null,
) {
    init {
        require(name.size > 0) { "a track name is non-empty (moq_media_track_cfg_t.name is required)" }
        require(codec.size > 0) { "a codec string is required for audio and video tracks (MSF-01 5.2.18)" }
        require(bitrate > 0uL) { "a nonzero bitrate is required for audio and video tracks (MSF-01 5.2.22)" }
        require(timescale > 0u) { "timescale is positive (the C default is 1000000, microseconds)" }
        require(sapTimelineHistoryGroups > 0u) { "sapTimelineHistoryGroups is positive (the C default is 8)" }
        require(mediaTimelineHistoryGroups > 0u) { "mediaTimelineHistoryGroups is positive (the C default is 8)" }
        if (mediaType == MediaType.Audio) {
            require(samplerate > 0u) { "an audio track requires a nonzero samplerate (MSF-01 5.2.28)" }
            require(channelConfig != null && channelConfig.size > 0) { "an audio track requires a non-empty channelConfig (MSF-01 5.2.29)" }
        }
        require(!(isLive && trackDuration != null)) { "a trackDuration is for a non-live track: it cannot be set together with isLive = true" }
    }
}

/**
 * A track this sender added: the sender-owned handle `add_track` returned,
 * stable for the sender's life. [isRemoved], [isEnded] and [isVod] are the
 * binding's own record of the operations THIS sender accepted on it, so a
 * write the C would refuse with `WRONG_STATE` is refused here before any
 * native call.
 */
public class SendTrack internal constructor(
    internal val token: Long,
    public val config: SendTrackConfig,
) {
    /** The catalog track name. */
    public val name: TrackName get() = config.name

    /** True once [MediaSender.removeTrack] was accepted. */
    @Volatile
    public var isRemoved: Boolean = false
        internal set

    /** True once an end marker was accepted ([MediaSender.tryEndTrack] / [MediaSender.endTrack]). */
    @Volatile
    public var isEnded: Boolean = false
        internal set

    /** True once a [MediaSender.convertToVod] batch carrying this track was accepted. */
    @Volatile
    public var isVod: Boolean = false
        internal set

    override fun toString(): String = "SendTrack($name)"
}

/**
 * One media object to submit (`moq_media_send_object_t`), an immutable
 * value: [payload] is copied on the way in and on the way out, so nothing
 * the caller does later changes what is written, and a [WriteOutcome.WouldBlock]
 * retry resends the identical value. [isSync] marks a random-access point
 * (a new group must open with one); [startsGroup] / [endsGroup] are the
 * group boundaries -- group and object ids are the service's.
 *
 * **A RAW/LOC object's property block carries ONE timestamp scalar**, so that
 * object cannot preserve both a capture instant and a media-timeline position.
 * Two layers are worth keeping apart:
 *
 *  * the PROTOCOL field is the LOC Capture Timestamp (LOC-01 2.3.1.1:
 *    wall-clock microseconds since the Unix epoch, a varint);
 *  * LibMoQ's COMPATIBILITY FALLBACK writes [presentationTime] -- a media
 *    scalar -- into that field when [captureTime] is null, so unless the
 *    caller's presentation values are themselves epoch-relative, the field then
 *    holds something that is not a capture instant.
 *
 * What the peer sees, either way:
 *
 *  * [captureTime] non-null sets `has_capture_time` and that epoch value is
 *    emitted verbatim -- no clamp, no scaling. The parser then copies the same
 *    scalar into its separately typed capture, decode and presentation outputs,
 *    with no unit or domain conversion, so the media-timeline positions
 *    submitted here do not reach it on this object.
 *  * [captureTime] null emits [presentationTime] in that field instead. The
 *    object is NOT untimed on the wire: the peer observes a capture timestamp
 *    equal to the presentation time submitted here, and cannot identify which
 *    source domain produced the scalar.
 *
 * So the choice of what this OBJECT's single scalar means is made here.
 * [presentationTime] separately drives the generated SAP/media-timeline tracks,
 * which are unaffected by that choice; [decodeTime] is advisory and absent
 * sends the presentation time. The emitted timestamp must fit the QUIC varint
 * range, which the C checks. None of this applies to CMAF: a CMAF object's
 * timing travels in its own property block (see [cmafProperties]), and these
 * RAW/LOC fields are ignored for it.
 *
 * [sapType] non-null declares the CMSF SAP type (`has_sap_type`).
 * [cmafProperties] is a CMAF object's complete property block, passed
 * through; whether a block is legal for the track's packaging is the C's
 * decision (`INVAL`), never re-judged here.
 */
public data class SendObject(
    public val payload: MediaBytes,
    public val isSync: Boolean,
    public val startsGroup: Boolean,
    public val presentationTime: MediaMicros,
    public val endsGroup: Boolean = false,
    public val decodeTime: MediaMicros? = null,
    public val captureTime: UnixMicros? = null,
    public val sapType: SapType? = null,
    public val cmafProperties: MediaBytes? = null,
) {
    /** The `decode_time_us` sent: the declared decode time, or the presentation time when none was declared. */
    public fun decodeTimeOrPresentation(): MediaMicros = decodeTime ?: presentationTime
}

/**
 * What one write or end-track attempt did. [Accepted] means the C took the
 * object (its bytes are the service's now, queued in decode order);
 * [WouldBlock] means flow control refused it and NOTHING was transferred --
 * retry the same value. It is a value, never an exception: under
 * [SendBackpressure.ReturnWouldBlock] it is the ordinary full-queue answer
 * and under a drop policy it is the C's "a group larger than the bound" or
 * "no anchor for this delta", neither of which a retry loop should hide.
 */
public sealed interface WriteOutcome {
    public data object Accepted : WriteOutcome

    public data object WouldBlock : WriteOutcome
}

/**
 * A media sender's fatal code (`moq_media_sender_fatal_code()`): the
 * sender's own `MOQ_MEDIA_SENDER_FATAL_*` when the sender itself failed,
 * otherwise the endpoint's code underneath it. Full width, its own domain:
 * never a [MoqErrorCode], never a [ReceiverFatalCode].
 */
@JvmInline
public value class SenderFatalCode(public val value: ULong) {
    override fun toString(): String = "SenderFatalCode($value)"

    public companion object {
        /** MSF encode failed. */
        public val CATALOG_ENCODE: SenderFatalCode = SenderFatalCode(0x1uL)

        /** Publisher, track or retained setup failed. */
        public val SETUP_FAILED: SenderFatalCode = SenderFatalCode(0x2uL)

        /** The peer refused the namespace. */
        public val NAMESPACE_REJECTED: SenderFatalCode = SenderFatalCode(0x3uL)

        /** The peer withdrew a previously accepted namespace. */
        public val NAMESPACE_CANCELLED: SenderFatalCode = SenderFatalCode(0x4uL)

        /** The peer rejected a catalog or media-track PUBLISH. */
        public val PUBLISH_REJECTED: SenderFatalCode = SenderFatalCode(0x5uL)

        /** All 64 raw bits of a native `uint64_t`, reinterpreted. */
        public fun fromNative(raw: Long): SenderFatalCode = SenderFatalCode(raw.toULong())
    }
}

/**
 * The sender's terminal: [Closed] when the endpoint underneath closed (or
 * the application closed the sender), [Fatal] when the sender or the
 * endpoint failed (`is_fatal`, which wins over `is_closed`).
 */
public sealed interface SenderTerminal {
    public data object Closed : SenderTerminal

    public data class Fatal(public val code: SenderFatalCode) : SenderTerminal
}

/**
 * One observation of the sender: [ready] is `is_ready` -- the catalog is
 * published and the peer acknowledged the announcement, so media may be
 * written -- NOT that anyone subscribed; it is false whenever [terminal] is
 * present, because a write refuses in that state and "ready" would be a
 * false "write now". [mediaDemand] is `has_media_subscriber`: some
 * app-visible media track has a subscriber (a catalog-only subscription
 * does not count).
 */
public data class SenderStatus(
    public val ready: Boolean,
    public val terminal: SenderTerminal?,
    public val mediaDemand: Boolean,
)

/**
 * One snapshot of the sender's statistics (`moq_media_sender_stats_t`),
 * every field in the struct's order, counters full width. Five fields form
 * one population of MEDIA objects accepted by a write: `objectsWritten ==
 * objectsSent + objectsQueued + objectsDropped`, with [bytesQueued] their
 * bytes; the private END_OF_TRACK marker is in none of them. [lastError] is
 * the last non-OK write result the C latched, as the `moq_result_t` it was.
 */
public data class SenderStats(
    public val objectsWritten: ULong,
    public val objectsSent: ULong,
    public val objectsQueued: ULong,
    public val bytesQueued: ULong,
    public val objectsDropped: ULong,
    public val groupsDropped: ULong,
    public val keyframesDropped: ULong,
    public val groupsAbandoned: ULong,
    public val backpressureStalls: ULong,
    public val lastError: MoqResultCode,
    public val sapRecordsEvicted: ULong,
)

/** One item of a [MediaSender.convertToVod] batch: the sender's own handle and its MSF §5.2.35 duration. */
public data class VodConversion(public val track: SendTrack, public val duration: MediaMillis)

/**
 * A media sender attached to an endpoint (`moq_media_sender_attach`). Every
 * native call runs on the endpoint's service worker, only on the caller's
 * demand; the C `moq_media_sender_wait` is never used (it parks a thread
 * inside C on the endpoint's single activity level, which the engine's own
 * loop owns).
 *
 * The sender is destroyed by [close], or by the endpoint's own teardown
 * before the endpoint is stopped (the C stop refuses while a sender is
 * attached). Once closed, every operation refuses with
 * [SenderClosedException] and makes no native call.
 */
public class MediaSender internal constructor(
    private val engine: EndpointEngine,
    private val token: Long,
    private val model: SenderModel,
    /** The attached configuration: the policy decides whether a WouldBlock is flow control. */
    public val config: SenderConfig,
) {
    /** True once this sender has been destroyed, by [close] or by the endpoint's teardown. */
    public val isClosed: Boolean
        get() = !engine.isSenderAttached(token)

    /**
     * The sender's state, refreshed from ONE native probe on every service
     * pass (readiness flips, demand changes and terminals all happen on pump
     * cycles, which mark the endpoint's activity; the C demand callbacks are
     * installed by the bridge solely to wake the endpoint so a change
     * produces a pass promptly). The value is always the C queries' answer,
     * never composed from callback arguments.
     */
    public val status: StateFlow<SenderStatus> = model.status

    /**
     * Suspend until [status] reports ready. A terminal beats readiness:
     * throws [SenderClosedException] with the terminal once one is reported,
     * or once the sender is closed.
     */
    public suspend fun awaitReady() {
        val s = status.first { it.ready || it.terminal != null }
        if (s.terminal != null) throw SenderClosedException(s.terminal)
        if (isClosed) throw SenderClosedException(model.status.value.terminal)
    }

    /**
     * Add a track (`moq_media_sender_add_track`): legal before and after
     * readiness. The catalog republish it records is emitted by a later
     * service pass. The returned handle belongs to this sender.
     *
     * Ownership boundary: the call is commit-grade. Cancellation BEFORE the
     * submission was accepted creates nothing; once accepted, the native add
     * runs to completion and its handle (or its typed refusal) is delivered
     * to this caller even if it was cancelled meanwhile -- a track the C
     * registered is never left without the handle that removes it. The
     * handle is retired once, by [removeTrack] or by the sender's close.
     *
     * @throws SenderClosedException this sender is closed or terminal
     * @throws SenderCompletedException the broadcast was completed
     * @throws TrackNameBusyException the name, or a generated sibling, is still tearing down: RETRY later
     * @throws TrackConfigRejectedException the C refused the configuration, with its exact code
     * @throws SenderInterruptedException the endpoint's interrupt latch is set
     */
    public suspend fun addTrack(config: SendTrackConfig): SendTrack {
        if (isClosed) throw SenderClosedException(model.status.value.terminal)
        return engine.senderAddTrack(token, model, config)
    }

    /**
     * Remove [track] from the catalog (`moq_media_sender_remove_track`): its
     * queued objects are discarded on a later pass and the handle is
     * retired here -- later operations on it refuse without a native call.
     *
     * @throws ForeignSendTrackException the track is another sender's
     * @throws SenderClosedException this sender is closed or terminal
     * @throws SendTrackUnavailableException the track is already removed (or the C's WRONG_STATE)
     * @throws SenderInterruptedException the endpoint's interrupt latch is set
     */
    public suspend fun removeTrack(track: SendTrack) {
        own(track)
        if (track.isRemoved) throw SendTrackUnavailableException()
        engine.senderRemoveTrack(token, model, track)
    }

    /**
     * ONE `moq_media_sender_write` attempt on the service worker.
     * [WriteOutcome.Accepted] transferred the object; [WriteOutcome.WouldBlock]
     * transferred nothing. The attempt is bounded (the admitted policies
     * never park) and its outcome is ALWAYS delivered: a caller cancelled
     * while the attempt is in flight still receives the outcome, and
     * cancellation takes effect at its next suspension, so the caller always
     * knows whether transfer happened.
     *
     * @throws ForeignSendTrackException the track is another sender's
     * @throws SenderClosedException this sender is closed or terminal
     * @throws SendTrackUnavailableException the track is removed, ended, VOD, or the C's WRONG_STATE
     * @throws ObjectRejectedException the C refused the object (`INVAL`, or `PROTO` for a CMAF validation failure)
     * @throws SenderInterruptedException the endpoint's interrupt latch is set
     */
    public suspend fun tryWrite(track: SendTrack, obj: SendObject): WriteOutcome = resolvedOutcome(writeAttempt(track, obj, retain = false))

    private suspend fun writeAttempt(track: SendTrack, obj: SendObject, retain: Boolean): Demanded<WriteAttempt> {
        own(track)
        unavailable(track)
        return engine.senderWrite(token, model, track, obj, retain)
    }

    private fun resolvedOutcome(d: Demanded<WriteAttempt>): WriteOutcome =
        when (d) {
            is Demanded.Resolved -> d.value.outcome
            is Demanded.Pending -> error("an unretained attempt is always resolved")
        }

    /**
     * [tryWrite], re-attempted once per service pass on [WriteOutcome.WouldBlock]
     * until accepted or [timeout] elapses, answering [WriteOutcome.WouldBlock]
     * then with nothing transferred. No timer and no C wait: the attempt is
     * retained on the endpoint's service worker, which re-attempts it on its
     * own passes -- queue drain and the readiness flip both happen on pump
     * cycles, which mark the endpoint's activity, so the next pass IS the
     * C's own wait signal; waiting publishes no wake of its own. Under a drop
     * policy the C never blocks on a full queue, so this returns on the
     * first attempt. Cancellable between attempts, where nothing is in
     * flight: the retained attempt is retired and never crosses again.
     * [Duration.ZERO] is a single attempt.
     */
    public suspend fun write(track: SendTrack, obj: SendObject, timeout: Duration): WriteOutcome =
        retryOnPasses(timeout) { retain -> writeAttempt(track, obj, retain) }

    /**
     * ONE `moq_media_sender_end_track` attempt: queues the reliable
     * END_OF_TRACK marker, emitted after the track's queued objects drain.
     * Idempotent at the C (a second call answers OK and does nothing; the
     * attempt still crosses). Ends nothing else. [WriteOutcome.WouldBlock]
     * latched nothing: retry. Once accepted the track reads [SendTrack.isEnded]
     * and a later write refuses here.
     *
     * @throws ForeignSendTrackException the track is another sender's
     * @throws SenderClosedException this sender is closed or terminal
     * @throws SendTrackUnavailableException the track is removed
     * @throws SenderInterruptedException the endpoint's interrupt latch is set
     */
    public suspend fun tryEndTrack(track: SendTrack): WriteOutcome = resolvedOutcome(endAttempt(track, retain = false))

    private suspend fun endAttempt(track: SendTrack, retain: Boolean): Demanded<WriteAttempt> {
        own(track)
        if (track.isRemoved) throw SendTrackUnavailableException()
        return engine.senderEndTrack(token, model, track, retain)
    }

    /** [tryEndTrack] with [write]'s pass-driven retry and timeout. */
    public suspend fun endTrack(track: SendTrack, timeout: Duration): WriteOutcome =
        retryOnPasses(timeout) { retain -> endAttempt(track, retain) }

    /**
     * Convert live tracks to VOD in place (`moq_media_sender_convert_to_vod`):
     * ONE atomic native call over the whole batch -- either every item is
     * converted or none is. The batch is snapshotted from [items] before
     * validation and submission: the snapshot is what the C converts and
     * what reads [SendTrack.isVod] afterwards, whatever the caller does to
     * its list meanwhile. A converted track refuses writes here;
     * [tryEndTrack] and [removeTrack] stay available. Legal only after
     * readiness and before [complete]. Ordinary cancellation: cancelled while
     * queued, nothing is converted; once claimed, a successful batch updates
     * the snapshotted handles even if the caller no longer awaits the result.
     *
     * @throws IllegalArgumentException an empty batch
     * @throws ForeignSendTrackException an item's track is another sender's
     * @throws SenderClosedException this sender is closed or terminal
     * @throws SenderNotReadyException not ready yet
     * @throws SenderCompletedException the broadcast was completed
     * @throws VodConversionRejectedException the C refused the batch (nothing converted), with its exact code
     * @throws SenderInterruptedException the endpoint's interrupt latch is set
     */
    public suspend fun convertToVod(items: List<VodConversion>) {
        // ONE owned shallow snapshot, taken before validation and submission
        // and used for the native batch and the successful local updates
        // alike: what the caller does to its own list afterwards changes
        // neither what the C converts nor which handles read VOD.
        val batch = items.toList()
        require(batch.isNotEmpty()) { "a VOD conversion converts at least one track" }
        for (item in batch) own(item.track)
        engine.senderConvertToVod(token, model, batch)
    }

    /**
     * Permanently terminate the broadcast (`moq_media_sender_complete`, MSF
     * §11.3): every track is ended reliably and a terminal `isComplete`
     * catalog generation is published -- by LATER service passes. This call
     * only RECORDS the completion: it performs no drain and implies none.
     *
     * Completion REMOVES every track, and a removed track's still-queued
     * media and lifecycle work is discarded (the discarded media objects are
     * counted in [SenderStats.objectsDropped]; lifecycle markers are not). A
     * drain after [complete] flushes
     * the completion work; it cannot bring back a media tail that had not
     * drained before [complete] was called. To preserve accepted finite
     * output: stop writing and mutating, end any track you intend to end
     * explicitly, [MoqEndpoint.drain] and require `Flushed`, then [complete],
     * then [MoqEndpoint.drain] and require `Flushed` again (the completion
     * work reached the core), then [close]. A drain can stay pending, and so
     * not answer `Flushed`, while protocol or demand gates hold queued work.
     * A `Flushed` answer is local: it is not proof that a peer received or
     * rendered anything, and does not undo what a drop policy already
     * discarded. Idempotent. Afterwards every mutation refuses.
     *
     * @throws SenderClosedException this sender is closed or terminal
     * @throws SenderNotReadyException not ready yet
     * @throws SenderInterruptedException the endpoint's interrupt latch is set
     */
    public suspend fun complete() {
        if (isClosed) throw SenderClosedException(model.status.value.terminal)
        engine.senderComplete(token, model)
    }

    /**
     * [track]'s active subscription count (`moq_media_sender_track_subscriptions`),
     * a point-in-time read on the service worker.
     *
     * @throws ForeignSendTrackException the track is another sender's
     * @throws SenderClosedException this sender is closed
     */
    public suspend fun trackDemand(track: SendTrack): UInt {
        own(track)
        return engine.senderTrackDemand(token, track)
    }

    /**
     * One snapshot of the sender's statistics (`moq_media_sender_get_stats`),
     * read on the service worker on demand; no stats flow, for the same
     * reason as the receiver's. A sender at its terminal still answers.
     *
     * @throws SenderClosedException this sender is closed
     */
    public suspend fun statsSnapshot(): SenderStats {
        if (isClosed) throw SenderClosedException(model.status.value.terminal)
        return engine.senderStats(token)
    }

    /**
     * Destroys the native sender on the endpoint's service worker and frees
     * the endpoint's sender slot. Idempotent. Accepted-but-unemitted work is
     * discarded unless [MoqEndpoint.drain] flushed it first.
     */
    public suspend fun close() {
        try {
            engine.closeSender(token)
        } catch (_: EngineClosedException) {
            // The endpoint's teardown already destroyed the sender.
        }
    }

    private fun own(track: SendTrack) {
        if (model.resolve(track.token) !== track) throw ForeignSendTrackException()
        if (isClosed) throw SenderClosedException(model.status.value.terminal)
    }

    private fun unavailable(track: SendTrack) {
        if (track.isRemoved || track.isEnded || track.isVod) throw SendTrackUnavailableException()
    }

    /**
     * The retry: only under [SendBackpressure.ReturnWouldBlock] is a
     * WouldBlock flow control worth waiting out; under a drop policy the C
     * never blocks on a full queue, so the first attempt's answer is final,
     * and [Duration.ZERO] is a single attempt. Otherwise the first attempt is
     * made with RETENTION: a WouldBlock answer leaves the attempt on the
     * worker, which re-attempts it once per pass -- on the endpoint's own
     * activity, never on a wake of its own -- and the caller awaits the
     * result. The timeout or a cancellation retires the retained attempt so
     * no transfer can happen after the caller gave up; a transfer the last
     * attempt had already committed is still reported.
     */
    private suspend inline fun retryOnPasses(timeout: Duration, attempt: (retain: Boolean) -> Demanded<WriteAttempt>): WriteOutcome {
        require(!timeout.isNegative()) { "a write timeout is non-negative: $timeout" }
        val retain = config.backpressure == SendBackpressure.ReturnWouldBlock && timeout > Duration.ZERO
        val pending =
            when (val d = attempt(retain)) {
                is Demanded.Resolved -> return d.value.outcome
                is Demanded.Pending -> d.retained
            }
        val committed: WriteAttempt? =
            try {
                if (timeout.isFinite()) withTimeoutOrNull(timeout) { pending.result.await() } else pending.result.await()
            } catch (e: CancellationException) {
                // The retirement waits for an attempt already in its native
                // call and answers with its complete outcome: a transfer that
                // commits there is reported, a refusal there is thrown typed,
                // and only "nothing happened" lets this cancellation win.
                engine.retireRetained(pending) ?: throw e
            }
        return (committed ?: engine.retireRetained(pending))?.outcome ?: WriteOutcome.WouldBlock
    }
}

/** The endpoint refused the attach with its own `moq_result_t`, carried exactly (a second sender: WRONG_STATE; terminal: CLOSED). */
public class SenderAttachFailedException internal constructor(
    public val code: MoqResultCode,
) : IllegalStateException("the endpoint refused the sender attach: $code")

/** The sender is closed or terminal; [terminal] is the last reported terminal, or null after a local close before any was reported. */
public class SenderClosedException internal constructor(
    public val terminal: SenderTerminal?,
) : IllegalStateException("the media sender is closed: ${terminal ?: "closed by the application"}")

/** The endpoint's sticky interrupt latch is set: the operation was refused and nothing changed; non-terminal. */
public class SenderInterruptedException internal constructor() : IllegalStateException("the endpoint is interrupted; the sender operation was refused")

/** The track was added by another sender. */
public class ForeignSendTrackException internal constructor() : IllegalArgumentException("the track belongs to another sender")

/** The track is removed, ended or VOD-converted, or the C answered WRONG_STATE for it: no new content can be written. */
public class SendTrackUnavailableException internal constructor() : IllegalStateException("the track accepts no further content")

/** The C refused the object with [code]: `INVAL` (malformed, out-of-range timestamp, illegal property block), or `PROTO` (CMAF validation). Nothing was transferred. */
public class ObjectRejectedException internal constructor(
    public val code: MoqResultCode,
) : IllegalArgumentException("the sender refused the object: $code")

/** `add_track` answered WOULD_BLOCK: [name] (or its generated `.sap` / `.timeline` sibling) belongs to a just-removed track whose teardown has not completed. RETRY later; nothing was registered. */
public class TrackNameBusyException internal constructor(
    public val name: TrackName,
) : IllegalStateException("the track name $name is still tearing down; retry the add later")

/** The C refused the track configuration with [code]. */
public class TrackConfigRejectedException internal constructor(
    public val code: MoqResultCode,
) : IllegalArgumentException("the sender refused the track configuration: $code")

/** The operation is legal only after readiness (the C's WRONG_STATE before it). */
public class SenderNotReadyException internal constructor() : IllegalStateException("the sender is not ready yet")

/** The broadcast was completed: no further mutation is accepted. */
public class SenderCompletedException internal constructor() : IllegalStateException("the broadcast was completed")

/** The C refused the VOD conversion batch with [code]: NOTHING was converted. */
public class VodConversionRejectedException internal constructor(
    public val code: MoqResultCode,
) : IllegalArgumentException("the sender refused the VOD conversion: $code")
