package org.openmoq.moq5.engine

import org.openmoq.moq5.EndpointSnapshot
import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.MediaMillis
import org.openmoq.moq5.MediaOffsetMicros
import org.openmoq.moq5.ObjectLocation
import org.openmoq.moq5.SapType
import org.openmoq.moq5.MediaPayload
import org.openmoq.moq5.MoqResultCode
import org.openmoq.moq5.ObjectStatus
import org.openmoq.moq5.ReceiverConfig
import org.openmoq.moq5.ReceiverStats
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.SendTrackConfig
import org.openmoq.moq5.SenderConfig
import org.openmoq.moq5.SenderStats
import org.openmoq.moq5.SenderStatus
import org.openmoq.moq5.StartMode
import org.openmoq.moq5.TrackDescription
import org.openmoq.moq5.UnixMicros
import org.openmoq.moq5.WireIdentity
import org.openmoq.moq5.UnixMillis

/**
 * The result of one blocking backend wait, mirroring `moq_endpoint_wait()`
 * (`service/include/moq/endpoint.h`).
 *
 * The C contract is the load-bearing part: the activity flag behind the wait is
 * COALESCED (several nudges before a cycle collapse into one) and
 * LEVEL-RETAINED (a wake landing before the next wait is not lost -- that wait
 * returns immediately), and the latch beats the level.
 */
internal sealed interface BackendWaitResult {
    /** `MOQ_OK` -- activity was observed and the level has been consumed. */
    data object ACTIVITY : BackendWaitResult

    /** `MOQ_DONE` -- the timeout elapsed with no activity. */
    data object TIMEOUT : BackendWaitResult

    /** `MOQ_ERR_INTERRUPTED` -- the interrupt latch is set. */
    data object INTERRUPTED : BackendWaitResult

    /** `MOQ_ERR_CLOSED` -- terminal. */
    data object CLOSED : BackendWaitResult

    /**
     * Anything else the bridge answered.
     *
     * `moq_endpoint_wait()`'s public contract is exactly the four outcomes
     * above, so a fifth value means the BRIDGE's contract was broken, not that
     * the endpoint reported a new state. It is carried exactly and never
     * normalised into [CLOSED] or [TIMEOUT] -- either would invent a state the
     * endpoint never reported.
     */
    data class Bridge(val failure: BridgeFailure) : BackendWaitResult
}

/**
 * A BRIDGE-CONTRACT failure: an exact native or private-registry code that the
 * signed protocol says cannot happen.
 *
 * It is deliberately NOT a [MoqResultCode]. A private registry refusal --
 * `STALE`, `KIND`, `THREAD`, `STATE` -- is the bridge's own vocabulary, and
 * letting one enter a `MoqResultCode` would claim the C endpoint returned it.
 * The two domains stay separate all the way to the engine.
 */
internal data class BridgeFailure(val op: String, val code: Int)

/** The result of an operation whose ordinary answer carries no value. */
internal sealed interface BackendOpResult {
    data object Ok : BackendOpResult

    data class Bridge(val failure: BridgeFailure) : BackendOpResult
}

/** One coherent snapshot, or the bridge's refusal to produce one. */
internal sealed interface BackendSnapshotResult {
    data class Ok(val snapshot: EndpointSnapshot) : BackendSnapshotResult

    data class Bridge(val failure: BridgeFailure) : BackendSnapshotResult
}

/**
 * The answers of `moq_endpoint_drain()`, one value each, plus the bridge
 * refusing the token or answering outside the contract.
 */
internal sealed interface BackendDrainResult {
    data object Flushed : BackendDrainResult
    data object TimedOut : BackendDrainResult
    data object Interrupted : BackendDrainResult
    data object Closed : BackendDrainResult
    data object Unsupported : BackendDrainResult
    data class Bridge(val failure: BridgeFailure) : BackendDrainResult
}

/**
 * What `moq_media_receiver_attach()` answered through the bridge: an OWNED
 * child token, the endpoint's own refusal carried exactly, or the bridge
 * refusing the token.
 */
internal sealed interface BackendAttachResult {
    data class Attached(val token: Long) : BackendAttachResult
    data class Refused(val code: MoqResultCode) : BackendAttachResult
    data class Bridge(val failure: BridgeFailure) : BackendAttachResult
}

/** `moq_media_track_event_kind_t`, one case per native kind, by its native value. */
internal enum class BackendTrackEventKind(val native: Int) {
    Added(1),
    Updated(2),
    Removed(3),
    Ended(4),
    CatalogReady(5),
    UpdateOk(6),
    ParseDrop(7),
    ;

    companion object {
        fun ofNative(value: Int): BackendTrackEventKind? = entries.firstOrNull { it.native == value }
    }
}

/**
 * One drained `moq_media_track_event_t`: the kind, the track's BORROWED
 * child token (zero for CATALOG_READY), the description snapshot taken at
 * the poll for ADDED and UPDATED, and the coalesced parse-drop counters.
 */
internal data class BackendTrackEvent(
    val kind: BackendTrackEventKind,
    val track: Long,
    val description: TrackDescription?,
    val parseDropsTotal: ULong = 0uL,
    val parseDropsDelta: ULong = 0uL,
)

/**
 * What `moq_media_receiver_poll_track()` answered through the bridge: an
 * event, the empty queue of a live receiver, the receiver's terminal (empty
 * AND terminal: clean, or fatal with the receiver's own code), or the bridge
 * refusing a token or answering outside the contract.
 */
internal sealed interface BackendTrackEventResult {
    data object Empty : BackendTrackEventResult

    data class Event(val event: BackendTrackEvent) : BackendTrackEventResult

    data class Closed(val fatal: Boolean, val code: ULong) : BackendTrackEventResult

    data class Bridge(val failure: BridgeFailure) : BackendTrackEventResult
}

/** One dequeued `moq_media_object_t`, its bytes already copied and the C object already released. */
internal data class BackendObject(
    val track: Long,
    val payload: MediaPayload,
    val status: ObjectStatus,
    val endOfGroup: Boolean,
    val viaDatagram: Boolean,
    val keyframe: Boolean,
    val captureTime: UnixMicros?,
    val decodeTime: MediaMicros,
    val compositionOffset: MediaOffsetMicros,
    val presentationTime: MediaMicros,
    val identity: WireIdentity? = null,   // null: the native record did not carry the identity tail
)

/**
 * What `moq_media_receiver_poll_object()` answered through the bridge: an
 * object, the empty queue of a live receiver, the endpoint's interrupt
 * latch (before the queue was read; nothing consumed), the receiver's
 * terminal (empty AND terminal), or the bridge refusing a token or
 * answering outside the contract.
 */
internal sealed interface BackendObjectResult {
    data object Empty : BackendObjectResult

    data class Object(val value: BackendObject) : BackendObjectResult

    data object Interrupted : BackendObjectResult

    data class Closed(val fatal: Boolean, val code: ULong) : BackendObjectResult

    data class Bridge(val failure: BridgeFailure) : BackendObjectResult
}

/** What a receiver COMMAND (`subscribe_track` / `unsubscribe_track`) answered: the endpoint's own result exactly, or the bridge's refusal. */
internal sealed interface BackendCommandResult {
    data object Ok : BackendCommandResult

    data class Refused(val code: MoqResultCode) : BackendCommandResult

    data class Bridge(val failure: BridgeFailure) : BackendCommandResult
}

/** What `moq_media_receiver_track_state` answered: the raw state value, the endpoint's refusal, or the bridge's. */
internal sealed interface BackendTrackStateResult {
    data class State(val raw: Int) : BackendTrackStateResult

    data class Refused(val code: MoqResultCode) : BackendTrackStateResult

    data class Bridge(val failure: BridgeFailure) : BackendTrackStateResult
}

/** One dequeued `moq_media_sap_record_t`, its track as the borrowed token. */
internal data class BackendSapRecord(val track: Long, val location: ObjectLocation, val sapType: SapType, val earliestPresentationTime: MediaMillis)

/** One dequeued `moq_media_timeline_record_t`; [wallclock] is `wallclock_ms` through `UnixMillis.fromNativeOrNull` (native 0 = absent). */
internal data class BackendTimelineRecord(val track: Long, val mediaTime: MediaMillis, val location: ObjectLocation, val wallclock: UnixMillis?)

/**
 * What a record poll answered, with the family's deferral counter as the
 * same row read it after the poll: a record, the empty queue of a live
 * receiver, the receiver's terminal, or the bridge's refusal.
 */
internal sealed interface BackendRecordResult<out R : Any> {
    data class Record<R : Any>(val value: R, val deferrals: ULong) : BackendRecordResult<R>

    data class Empty(val deferrals: ULong) : BackendRecordResult<Nothing>

    data class Closed(val fatal: Boolean, val code: ULong) : BackendRecordResult<Nothing>

    data class Bridge(val failure: BridgeFailure) : BackendRecordResult<Nothing>
}

/** What `moq_media_receiver_get_stats` answered: the snapshot, or the bridge's refusal (the C refuses only a NULL/short struct, which the row refuses before acquiring). */
internal sealed interface BackendStatsResult {
    data class Stats(val value: ReceiverStats) : BackendStatsResult

    data class Bridge(val failure: BridgeFailure) : BackendStatsResult
}

/** What `moq_media_sender_add_track` answered: the track's BORROWED child token, the retryable WOULD_BLOCK, the endpoint's own refusal exactly, or the bridge's. */
internal sealed interface BackendSenderTrackResult {
    data class Added(val track: Long) : BackendSenderTrackResult

    data object WouldBlock : BackendSenderTrackResult

    data class Refused(val code: MoqResultCode) : BackendSenderTrackResult

    data class Bridge(val failure: BridgeFailure) : BackendSenderTrackResult
}

/** What ONE `moq_media_sender_write` / `moq_media_sender_end_track` attempt answered: transfer on Accepted only; WouldBlock transferred nothing; a refusal exactly; or the bridge's. */
internal sealed interface BackendWriteResult {
    data object Accepted : BackendWriteResult

    data object WouldBlock : BackendWriteResult

    data class Refused(val code: MoqResultCode) : BackendWriteResult

    data class Bridge(val failure: BridgeFailure) : BackendWriteResult
}

/** What the sender probe row answered: the decoded status, or the bridge's refusal. */
internal sealed interface BackendSenderProbeResult {
    data class Status(val value: SenderStatus) : BackendSenderProbeResult

    data class Bridge(val failure: BridgeFailure) : BackendSenderProbeResult
}

/** What `moq_media_sender_track_subscriptions` answered. */
internal sealed interface BackendDemandResult {
    data class Count(val value: UInt) : BackendDemandResult

    data class Bridge(val failure: BridgeFailure) : BackendDemandResult
}

/** What `moq_media_sender_get_stats` answered. */
internal sealed interface BackendSenderStatsResult {
    data class Stats(val value: SenderStats) : BackendSenderStatsResult

    data class Bridge(val failure: BridgeFailure) : BackendSenderStatsResult
}

/**
 * What `moq_endpoint_stop()` answered.
 *
 * The native call returns `moq_result_t` and is not always `MOQ_OK`: it reports
 * `MOQ_ERR_WRONG_STATE` for an endpoint with live attachments
 * (`service/src/endpoint.c:1871`) and for a call made from inside `on_pump`
 * (`:1877`). A refused stop changed NOTHING -- the endpoint is still live and
 * still owned by the service tier, so it must not be destroyed.
 *
 * The JNI backend maps `MOQ_OK` to [Stopped] and every non-OK ENDPOINT result
 * EXACTLY to [Refused]; it never normalises a code. A private registry refusal
 * is not an endpoint result and takes [Bridge] instead. There is deliberately
 * no `AlreadyStopped` case: the C call folds that into `MOQ_OK`, so a binding
 * cannot observe it and must not invent it.
 */
internal sealed interface BackendStopResult {
    /** `MOQ_OK`. The endpoint is stopped and may be destroyed. */
    data object Stopped : BackendStopResult

    /** Any non-OK `moq_result_t` from the ENDPOINT, carried exactly. */
    data class Refused(val code: MoqResultCode) : BackendStopResult

    /**
     * The bridge refused before the endpoint was reached -- a private registry
     * category, say. This is NOT an ordinary stop refusal and must never be
     * reported as one: the endpoint did not answer.
     */
    data class Bridge(val failure: BridgeFailure) : BackendStopResult
}

/**
 * The engine's view of one C endpoint. Deliberately narrow for this tranche:
 * only the wake/wait/stop/destroy mechanics the engine loop is built around.
 *
 * Call-site contract, taken from the C tier rather than invented here:
 *
 *  * [wake] is infallible, allocation-free and callable from ANY thread. It
 *    queues no task, so it cannot fail; it is best-effort and idempotent, and a
 *    no-op after stop.
 *  * [waitForActivity] BLOCKS and has exactly ONE consumer. It is the only
 *    method here that may park, and the engine must own the thread that calls
 *    it.
 *  * [stop] then [destroy] is the canonical teardown order, each exactly once.
 *  * After [destroy] the C endpoint is freed, so ANY later call on this
 *    interface is a use-after-free.
 *
 * Throwing: no method the SERVICE LOOP calls may throw. The engine's worker treats
 * [waitForActivity], [stop] and [destroy] as returning normally -- an exception
 * from one of them would escape the service loop and strand every waiter on the
 * endpoint -- and [wake] is the C tier's infallible nudge. A backend that can
 * fail must report it through its result, not by throwing.
 *
 * [stop] therefore reports its outcome as a VALUE, not by throwing: a refused
 * native stop is an ordinary result the engine must act on, not an exception.
 *
 * [snapshot] is the one exception, and deliberately so. It is invoked only from
 * a job body, never from the service loop, so a throw is delivered to the
 * snapshot caller and strands nobody. That matters because mapping a native
 * record FAILS CLOSED on a discriminator this binding does not know -- inventing
 * a state or a terminal cause would be worse than refusing -- and the refusal
 * has no code to report as a bridge failure, because the value is not a result.
 */
internal interface EndpointBackend {
    /**
     * `moq_endpoint_wake()`. Any thread; coalesced and level-retained.
     *
     * It returns a RESULT rather than nothing: the C wake cannot fail, but the
     * bridge in front of it can refuse the token, and a method that may not
     * throw has nowhere else to put that. Under the signed protocol such a
     * refusal is unreachable; if one ever arrives the engine quarantines the
     * native ownership rather than continuing on a token the bridge rejected.
     */
    fun wake(): BackendOpResult

    /**
     * The sticky interrupt latch: any-thread, idempotent, non-terminal. Its
     * only failure is the bridge refusing the token.
     */
    fun setInterrupted(interrupted: Boolean): BackendOpResult

    /**
     * The application-thread graceful flush: blocks for up to [timeoutMicros]
     * (validated non-negative by the caller). Any-thread, but it WAITS ON THE
     * ENDPOINT, so the engine keeps its worker out of [waitForActivity] while
     * one is in flight.
     */
    fun drain(timeoutMicros: Long): BackendDrainResult

    /**
     * `moq_endpoint_wait()`. Blocking, single-consumer; consumes the retained
     * activity level when it returns [BackendWaitResult.ACTIVITY].
     */
    fun waitForActivity(timeoutMicros: Long): BackendWaitResult

    /**
     * `moq_endpoint_stop()`. Attempted exactly once, before [destroy].
     *
     * Returns the native result rather than discarding it. On
     * [BackendStopResult.Refused] the endpoint is still LIVE and the engine
     * must not call [destroy] or make any later call on this backend.
     */
    fun stop(): BackendStopResult

    /**
     * One coherent snapshot record, never four independent reads.
     *
     * Authority-thread confined like [waitForActivity] and [stop], which is why
     * it belongs here rather than on a separate reader: the token has ONE owner.
     */
    fun snapshot(): BackendSnapshotResult

    /** Retire the root. Exactly once, after a successful [stop]. */
    fun destroy(): BackendOpResult

    /** Attach a receiver under this root, on the service thread; the token identifies the owned child. */
    fun attachReceiver(config: ReceiverConfig): BackendAttachResult

    /** Retire one receiver child under this root, on the service thread; the root stays live. */
    fun closeReceiver(token: Long): BackendOpResult

    /**
     * Drain ONE track event from the receiver child [token], on the service
     * thread. Non-blocking; the engine calls it until [BackendTrackEventResult.Empty]
     * or [BackendTrackEventResult.Closed].
     */
    fun pollTrack(token: Long): BackendTrackEventResult

    /**
     * ONE `moq_media_receiver_poll_object` attempt on the receiver child
     * [token], on the service thread, only ever on a consumer's demand. A
     * returned object is already copied and its C object released.
     */
    fun pollObject(token: Long): BackendObjectResult

    /** `moq_media_receiver_subscribe_track` for [track] under the receiver [token], on the service thread; a null [priority] sends none. */
    fun subscribeTrack(token: Long, track: Long, start: StartMode, priority: UByte?): BackendCommandResult

    /** `moq_media_receiver_unsubscribe_track`, on the service thread. */
    fun unsubscribeTrack(token: Long, track: Long): BackendCommandResult

    /** `moq_media_receiver_track_state`, a point-in-time read on the service thread. */
    fun trackState(token: Long, track: Long): BackendTrackStateResult

    /** ONE `moq_media_receiver_poll_sap` attempt plus the SAP deferral counter, on the service thread, only on a consumer's demand. */
    fun pollSap(token: Long): BackendRecordResult<BackendSapRecord>

    /** ONE `moq_media_receiver_poll_media_timeline` attempt plus the timeline deferral counter. */
    fun pollMediaTimeline(token: Long): BackendRecordResult<BackendTimelineRecord>

    /** ONE `moq_media_receiver_get_stats` read on the receiver child [token], on the service thread, on demand. */
    fun receiverStats(token: Long): BackendStatsResult

    /** Attach a sender under this root, on the service thread; the token identifies the owned child. */
    fun attachSender(config: SenderConfig): BackendAttachResult

    /** Retire one sender child under this root, on the service thread; the root stays live. */
    fun closeSender(token: Long): BackendOpResult

    /** `moq_media_sender_add_track` on the sender child [token]; an added track is a BORROWED child of it. */
    fun senderAddTrack(token: Long, config: SendTrackConfig): BackendSenderTrackResult

    /** `moq_media_sender_remove_track`. */
    fun senderRemoveTrack(token: Long, track: Long): BackendCommandResult

    /** ONE `moq_media_sender_write` attempt: the object's bytes are transferred on Accepted only. */
    fun senderWrite(token: Long, track: Long, obj: SendObject): BackendWriteResult

    /** ONE `moq_media_sender_end_track` attempt. */
    fun senderEndTrack(token: Long, track: Long): BackendWriteResult

    /** ONE `moq_media_sender_convert_to_vod` over the whole batch, `(track token, duration ms)` in order. */
    fun senderConvertToVod(token: Long, items: List<Pair<Long, ULong>>): BackendCommandResult

    /** `moq_media_sender_complete`. */
    fun senderComplete(token: Long): BackendCommandResult

    /** ONE probe: is_ready, is_closed, is_fatal, fatal_code, has_media_subscriber, under one pin, decoded. */
    fun senderProbe(token: Long): BackendSenderProbeResult

    /** `moq_media_sender_track_subscriptions`. */
    fun senderTrackDemand(token: Long, track: Long): BackendDemandResult

    /** ONE `moq_media_sender_get_stats` read on the sender child [token], on demand. */
    fun senderStats(token: Long): BackendSenderStatsResult
}
