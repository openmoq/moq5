package org.openmoq.moq5.engine

import org.openmoq.moq5.EndpointSnapshot
import org.openmoq.moq5.EndpointState
import org.openmoq.moq5.MoqResultCode
import org.openmoq.moq5.ReceiverConfig
import org.openmoq.moq5.ReceiverStats
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.SendTrackConfig
import org.openmoq.moq5.SenderConfig
import org.openmoq.moq5.SenderFatalCode
import org.openmoq.moq5.SenderStats
import org.openmoq.moq5.SenderStatus
import org.openmoq.moq5.SenderTerminal
import org.openmoq.moq5.StartMode
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * A deterministic stand-in for the C endpoint that implements the REAL
 * mechanics rather than an idealised version of them, because the engine tests
 * are only worth anything if they exercise the contracts in `endpoint.h`:
 *
 *  * the activity flag is coalesced and LEVEL-RETAINED, and exactly one
 *    consumer may take it;
 *  * the interrupt latch beats the level and does NOT consume it;
 *  * `timeoutMicros == 0` elapses immediately -- it is not an indefinite park;
 *  * a SUCCESSFUL `stop()` is TERMINAL: `moq_endpoint_stop()` sets the
 *    endpoint's closed state (`service/src/endpoint.c:1877-1880`), so a later
 *    wait reports CLOSED, not a timeout. A REFUSED stop changes nothing and
 *    leaves the endpoint live;
 *  * a wake after `stop()` or once terminal is a harmless no-op and cannot
 *    resurrect activity.
 *
 * It is also the tripwire: every entry point records the calling thread by ID,
 * refuses re-entrancy, and flags any call made after [destroy] -- a real C
 * backend frees the endpoint there, so a post-destroy call is a use-after-free
 * in production, not a style problem.
 *
 * No sleeps and no wall-clock dependence anywhere. A positive wait is released
 * by an explicit scripted event ([expireWait], [markActivity], [setInterrupted],
 * [setClosed]); the ceilings in [awaitCondition] exist only so a broken engine
 * fails by name instead of hanging.
 */
internal class ScriptedEndpointBackend(private val failures: TestFailures) : EndpointBackend {

    /** An atomic copy of every recorder, taken under the lock. */
    internal data class Snapshot(
        val wakeCount: Int = 0,
        /** Wakes ignored because THIS engine had already called stop(). */
        val postStopWakeCount: Int = 0,
        /** Wakes ignored because the endpoint went terminal externally. */
        val externalTerminalWakeCount: Int = 0,
        /** Wakes attempted after destroy(); also recorded as a violation. */
        val postDestroyWakeCount: Int = 0,
        val waitEntries: Int = 0,
        val waitReturns: List<BackendWaitResult> = emptyList(),
        val waitTimeouts: List<Long> = emptyList(),
        val stopCount: Int = 0,
        val destroyCount: Int = 0,
        val violations: List<String> = emptyList(),
        val waitParked: Boolean = false,
        val activityLevel: Boolean = false,
        val serviceThreadId: Long? = null,
        val serviceThreadName: String? = null,
        /** The exact worker, retained so cleanup can JOIN it rather than scan. */
        val serviceThread: Thread? = null,
        val callOrder: List<String> = emptyList(),
        val expiries: Int = 0,
        /** True while a computed wait result is being held by the test. */
        val waitReturnHeld: Boolean = false,
        /**
         * The native result a REFUSED `moq_endpoint_stop()` reported, or null
         * if stop was never refused.
         *
         * `stop()` returns this to the engine as a `BackendStopResult`; this
         * field is the backend's own record of what it answered, so a test can
         * assert the code independently of what the engine did with it.
         * `moq_endpoint_stop()` answers `MOQ_ERR_WRONG_STATE` for an endpoint
         * with live attachments and for a call made from inside `on_pump`
         * (`service/src/endpoint.c:1871,1877`), and a refused stop leaves the
         * endpoint ALIVE -- not closed, and not safe to destroy.
         */
        val stopRefusedCode: MoqResultCode? = null,
        /** Every latch store the engine made, in order, and the thread id of each. */
        val latchSets: List<Boolean> = emptyList(),
        val latchThreadIds: List<Long> = emptyList(),
        /** Every drain call: count, timeouts, thread ids, whether one is in flight. */
        val drainCalls: Int = 0,
        val drainTimeouts: List<Long> = emptyList(),
        val drainThreadIds: List<Long> = emptyList(),
        val drainInFlight: Boolean = false,
        /** Latched true if any drain was ENTERED while a wait was parked. */
        val drainEnteredWhileWaitParked: Boolean = false,
        /** Activity levels the drain itself consumed, as the C drain's inner `moq_endpoint_wait` slice does while the flush is pending. */
        val drainConsumedLevels: Int = 0,
        /** True while a drain call is held BEFORE entry: the engine has handed off, the backend has not been reached. */
        val drainEntryHeld: Boolean = false,
        /** Every receiver attach the engine made, with the thread it ran on. */
        val receiverAttaches: List<ReceiverAttach> = emptyList(),
        /** Every receiver close, by child token, in order. */
        val receiverCloses: List<Long> = emptyList(),
        /** Every track poll, in order, with the thread it ran on and what it answered. */
        val trackPolls: List<TrackPoll> = emptyList(),
        /** Scripted events not yet drained: zero after every pass, or the engine left the native queue to fill. */
        val trackPollsLeftQueued: Int = 0,
        /** Every object poll, in order, with its thread and answer: only ever on a demand. */
        val objectPolls: List<ObjectPoll> = emptyList(),
        /** Scripted objects not yet polled: the native queue is the only queue. */
        val objectsLeftQueued: Int = 0,
        /** True while an object poll is held inside the call by the test. */
        val objectPollHeld: Boolean = false,
        /** Every record poll, in order, with its thread and answer: only ever on a demand. */
        val sapPolls: List<RecordPoll> = emptyList(),
        val timelinePolls: List<RecordPoll> = emptyList(),
        /** True while a sap poll is held inside the call by the test. */
        val sapPollHeld: Boolean = false,
        /** Every stats read, in order, with its thread: only ever on a demand. */
        val statsReads: List<TrackCall> = emptyList(),
        /** Every control command and state read, in order, with its thread. */
        val subscribeCalls: List<SubscribeCall> = emptyList(),
        val unsubscribeCalls: List<TrackCall> = emptyList(),
        val trackStateCalls: List<TrackCall> = emptyList(),
        /** Queued objects whose track is wanted AND accepted: what a poll could deliver now. */
        val objectsDeliverable: Int = 0,
        /* The sender seam's recorders. */
        val senderAttaches: List<SenderAttach> = emptyList(),
        val senderCloses: List<Long> = emptyList(),
        val senderAddTracks: List<SenderAddTrack> = emptyList(),
        val senderRemoves: List<TrackCall> = emptyList(),
        /** Every write attempt, in order, with its thread and answer. */
        val senderWrites: List<SenderWrite> = emptyList(),
        /** The payloads the seam ACCEPTED (transfer on success only). */
        val acceptedPayloads: List<ByteArray> = emptyList(),
        val senderEndTracks: List<TrackCall> = emptyList(),
        val senderVodConversions: List<VodCall> = emptyList(),
        val senderCompletes: List<Long> = emptyList(),
        val senderProbes: Int = 0,
        val senderDemandReads: List<TrackCall> = emptyList(),
        val senderStatsReads: List<TrackCall> = emptyList(),
        /** True while a write is held inside the call by the test. */
        val senderWriteHeld: Boolean = false,
        /** True while an end-track attempt is held inside the call by the test. */
        val senderEndTrackHeld: Boolean = false,
        /** A sender probe has begun and its answer has not reached the engine. */
        val senderProbeHeld: Boolean = false,
        /** The seam has NO wait method (`moq_media_sender_wait` is never bound); the field exists so a row can state that absence. */
        val senderWaits: Int = 0,
    )

    internal data class SenderAttach(val config: SenderConfig, val threadId: Long, val token: Long)

    internal data class SenderAddTrack(val token: Long, val config: SendTrackConfig, val threadId: Long, val track: Long)

    internal data class SenderWrite(val token: Long, val track: Long, val value: SendObject, val threadId: Long, val result: BackendWriteResult)

    internal data class VodCall(val items: List<Pair<Long, ULong>>, val threadId: Long)

    internal data class RecordPoll(val token: Long, val threadId: Long, val result: Any)

    internal data class SubscribeCall(val track: Long, val start: StartMode, val priority: UByte?, val threadId: Long)

    internal data class TrackCall(val track: Long, val threadId: Long)

    internal data class ObjectPoll(val token: Long, val threadId: Long, val result: BackendObjectResult)

    internal data class ReceiverAttach(val config: ReceiverConfig, val threadId: Long, val token: Long)

    internal data class TrackPoll(val token: Long, val threadId: Long, val result: BackendTrackEventResult)

    private val lock = ReentrantLock()
    private val changed = lock.newCondition()

    /* lock-guarded */
    private var activityLevel = false
    private var interrupted = false
    private var closed = false
    private var stopped = false
    private var destroyed = false
    private var expireRequested = false
    private var wakeBarrier = false
    private var holdReturn = false
    private var releaseReturn = false
    private var inCall = false
    private var refuseStopCode: MoqResultCode? = null
    private var r = Snapshot()

    /** Invoked inside [wake], while the call is still in progress. */
    internal var onWake: (() -> Unit)? = null

    /** Invoked inside [stop], while the call is still in progress. */
    internal var onStop: (() -> Unit)? = null

    /** Invoked inside [destroy], while the call is still in progress. */
    internal var onDestroy: (() -> Unit)? = null

    /**
     * The exact result each private-refusal branch answers. Defaults are the
     * ordinary success answers, so a test opts INTO a bridge refusal rather
     * than inheriting one.
     */
    internal var wakeResult: BackendOpResult = BackendOpResult.Ok
    internal var setInterruptedResult: BackendOpResult = BackendOpResult.Ok
    internal var drainResult: BackendDrainResult = BackendDrainResult.Flushed

    /** The next attach's answer; null = a fresh owned child token, or the C WRONG_STATE while one is attached. */
    internal var attachResult: BackendAttachResult? = null
    internal var closeReceiverResult: BackendOpResult = BackendOpResult.Ok
    private var nextChild = 0L
    private val liveChildren = mutableSetOf<Long>()

    /** The scripted native track-event queue, drained by [pollTrack] in order. */
    private val trackQueue = ArrayDeque<BackendTrackEvent>()

    /**
     * What [pollTrack] answers once the queue is empty instead of
     * [BackendTrackEventResult.Empty]: the receiver's terminal, or a bridge
     * refusal. Null is the ordinary live, empty answer.
     */
    internal var pollTrackTerminal: BackendTrackEventResult? = null

    /* The scripted subscription model: the C's desire, the peer's acceptance
     * (scripted), the tracks the polled events ended, and the FLOW_CONTROL
     * pause; the state derives exactly as the C derives it. */
    private val desiredTracks = mutableSetOf<Long>()
    private val issuedTracks = mutableSetOf<Long>()
    private val acceptedTracks = mutableSetOf<Long>()
    private val endedTracks = mutableSetOf<Long>()

    /** Overrides for one command's answer (null = the scripted semantics), and hooks run INSIDE the command after its answer is decided. */
    internal var subscribeResult: BackendCommandResult? = null
    internal var unsubscribeResult: BackendCommandResult? = null
    internal var trackStateResult: BackendCommandResult? = null
    internal var afterSubscribe: (() -> Unit)? = null
    internal var afterUnsubscribe: (() -> Unit)? = null
    internal var afterTrackState: (() -> Unit)? = null

    /** FLOW_CONTROL backpressure, as the C's `paused` flag. */
    @Volatile internal var flowPaused: Boolean = false

    /* The scripted record queues, the counters each poll reports (the family's
     * `*_record_deferrals` as the bridge row reads it after the poll), and what
     * an empty poll answers instead of Empty (a terminal or a bridge refusal). */
    private val sapQueue = ArrayDeque<BackendSapRecord>()
    private val timelineQueue = ArrayDeque<BackendTimelineRecord>()
    @Volatile internal var sapDeferrals: ULong = 0uL
    @Volatile internal var timelineDeferrals: ULong = 0uL
    internal var pollSapTerminal: BackendRecordResult<Nothing>? = null
    internal var pollTimelineTerminal: BackendRecordResult<Nothing>? = null
    private var sapHolds = 0
    private var sapReleases = 0

    /* -- the sender seam ------------------------------------------------------- */

    /** The next sender attach's answer; null = a fresh owned child token, or the C WRONG_STATE while one is attached. */
    internal var senderAttachResult: BackendAttachResult? = null
    private var senderLive: Long? = null
    private var nextSenderTrack = 0L
    private val senderTracks = mutableSetOf<Long>()

    /* The scripted probe state, read by every probe. */
    internal var senderReady: Boolean = false
    internal var senderFatal: ULong? = null
    internal var senderEndpointClosed: Boolean = false
    internal var senderMediaDemand: Boolean = false
    private val trackDemands = HashMap<Long, UInt>()

    private val addTrackQueue = ArrayDeque<BackendSenderTrackResult>()
    private val writeQueue = ArrayDeque<BackendWriteResult>()
    private val endTrackQueue = ArrayDeque<BackendWriteResult>()

    /** What a write answers once the scripted queue is empty. */
    internal var senderWriteDefault: BackendWriteResult = BackendWriteResult.Accepted

    /** What an end-track attempt answers once its scripted queue is empty. */
    internal var senderEndTrackDefault: BackendWriteResult = BackendWriteResult.Accepted

    /** What remove / convert / complete answer; null = Ok. */
    internal var senderCommandResult: BackendCommandResult? = null
    internal var scriptedSenderStats: SenderStats = SenderStats(0uL, 0uL, 0uL, 0uL, 0uL, 0uL, 0uL, 0uL, 0uL, MoqResultCode(0), 0uL)
    private var writeHolds = 0
    private var writeReleases = 0

    internal fun scriptSenderAddTrack(vararg results: BackendSenderTrackResult) = lock.withLock { addTrackQueue.addAll(results) }

    internal fun scriptSenderWrite(vararg results: BackendWriteResult) = lock.withLock { writeQueue.addAll(results) }

    internal fun scriptSenderEndTrack(vararg results: BackendWriteResult) = lock.withLock { endTrackQueue.addAll(results) }

    internal fun trackDemand(track: Long, count: UInt) = lock.withLock { trackDemands[track] = count }

    internal fun holdNextSenderWrite() = lock.withLock { writeHolds++ }

    private var endTrackHolds = 0
    private var endTrackReleases = 0

    internal fun holdNextSenderEndTrack() = lock.withLock { endTrackHolds++ }

    internal fun releaseSenderEndTrack() = lock.withLock {
        endTrackReleases++
        changed.signalAll()
    }

    internal fun releaseSenderWrite() = lock.withLock {
        writeReleases++
        changed.signalAll()
    }

    private var probeHolds = 0
    private var probeReleases = 0

    /**
     * Hold the next sender probe INSIDE the call, before it is recorded.
     *
     * The probe is the first backend call of the worker's post-drain
     * observation, so holding it opens a window between "the post-drain branch
     * has been entered" and any record it produces -- which is what lets a row
     * arm a fault for that observation and no earlier one.
     */
    internal fun holdNextSenderProbe() = lock.withLock { probeHolds++ }

    internal fun releaseSenderProbe() = lock.withLock {
        probeReleases++
        changed.signalAll()
    }

    override fun attachSender(config: SenderConfig): BackendAttachResult {
        lock.withLock {
            enterLocked("attachSender")
            noteServiceThreadLocked("attachSender")
            if (stopped || closed || destroyed) r = r.copy(violations = r.violations + "attachSender on a stopped, closed or destroyed root")
            val scripted = senderAttachResult
            val result =
                when {
                    scripted != null -> scripted
                    senderLive != null -> BackendAttachResult.Refused(MoqResultCode(-5))
                    else -> {
                        nextChild += 1
                        BackendAttachResult.Attached(SENDER_BASE + nextChild)
                    }
                }
            if (result is BackendAttachResult.Attached) senderLive = result.token
            r = r.copy(senderAttaches = r.senderAttaches + SenderAttach(config, Thread.currentThread().threadId(), (result as? BackendAttachResult.Attached)?.token ?: 0L), callOrder = r.callOrder + "sender-attach")
            changed.signalAll()
            exitLocked()
            return result
        }
    }

    override fun closeSender(token: Long): BackendOpResult {
        lock.withLock {
            enterLocked("closeSender")
            noteServiceThreadLocked("closeSender")
            if (senderLive != token) r = r.copy(violations = r.violations + "closeSender of a token that is not the live sender: $token")
            if (stopped) r = r.copy(violations = r.violations + "closeSender after stop: the C stop would have refused while attached")
            senderLive = null
            senderTracks.clear()
            r = r.copy(senderCloses = r.senderCloses + token, callOrder = r.callOrder + "sender-destroy")
            changed.signalAll()
            exitLocked()
            return BackendOpResult.Ok
        }
    }

    private fun senderCheckLocked(name: String, token: Long) {
        if (senderLive != token) r = r.copy(violations = r.violations + "$name of a token that is not the live sender: $token")
    }

    override fun senderAddTrack(token: Long, config: SendTrackConfig): BackendSenderTrackResult {
        lock.withLock {
            enterLocked("senderAddTrack")
            noteServiceThreadLocked("senderAddTrack")
            senderCheckLocked("senderAddTrack", token)
            val result =
                addTrackQueue.removeFirstOrNull() ?: run {
                    nextSenderTrack += 1
                    BackendSenderTrackResult.Added(TRACK_BASE + nextSenderTrack)
                }
            if (result is BackendSenderTrackResult.Added) senderTracks += result.track
            r = r.copy(senderAddTracks = r.senderAddTracks + SenderAddTrack(token, config, Thread.currentThread().threadId(), (result as? BackendSenderTrackResult.Added)?.track ?: 0L), callOrder = r.callOrder + "sender-add-track")
            changed.signalAll()
            exitLocked()
            return result
        }
    }

    private fun senderTrackCheckLocked(name: String, track: Long) {
        if (track !in senderTracks) r = r.copy(violations = r.violations + "$name of a track the seam never added: $track")
    }

    override fun senderRemoveTrack(token: Long, track: Long): BackendCommandResult {
        lock.withLock {
            enterLocked("senderRemoveTrack")
            noteServiceThreadLocked("senderRemoveTrack")
            senderCheckLocked("senderRemoveTrack", token)
            senderTrackCheckLocked("senderRemoveTrack", track)
            val result = senderCommandResult ?: BackendCommandResult.Ok
            r = r.copy(senderRemoves = r.senderRemoves + TrackCall(track, Thread.currentThread().threadId()), callOrder = r.callOrder + "sender-remove-track")
            changed.signalAll()
            exitLocked()
            return result
        }
    }

    override fun senderWrite(token: Long, track: Long, obj: SendObject): BackendWriteResult {
        lock.withLock {
            enterLocked("senderWrite")
            noteServiceThreadLocked("senderWrite")
            senderCheckLocked("senderWrite", token)
            senderTrackCheckLocked("senderWrite", track)
            if (writeHolds > 0) {
                // Held INSIDE the call: the attempt has begun, the answer has not reached the engine.
                writeHolds--
                r = r.copy(senderWriteHeld = true)
                changed.signalAll()
                inCall = false
                while (writeReleases == 0) changed.await()
                writeReleases--
                inCall = true
                r = r.copy(senderWriteHeld = false)
            }
            val result = writeQueue.removeFirstOrNull() ?: senderWriteDefault
            r =
                r.copy(
                    senderWrites = r.senderWrites + SenderWrite(token, track, obj, Thread.currentThread().threadId(), result),
                    acceptedPayloads = if (result is BackendWriteResult.Accepted) r.acceptedPayloads + obj.payload.bytes() else r.acceptedPayloads,
                    callOrder = r.callOrder + "sender-write",
                )
            changed.signalAll()
            exitLocked()
            return result
        }
    }

    override fun senderEndTrack(token: Long, track: Long): BackendWriteResult {
        lock.withLock {
            enterLocked("senderEndTrack")
            noteServiceThreadLocked("senderEndTrack")
            senderCheckLocked("senderEndTrack", token)
            senderTrackCheckLocked("senderEndTrack", track)
            if (endTrackHolds > 0) {
                // Held INSIDE the call: the attempt has begun, the answer has not reached the engine.
                endTrackHolds--
                r = r.copy(senderEndTrackHeld = true)
                changed.signalAll()
                inCall = false
                while (endTrackReleases == 0) changed.await()
                endTrackReleases--
                inCall = true
                r = r.copy(senderEndTrackHeld = false)
            }
            val result = endTrackQueue.removeFirstOrNull() ?: senderEndTrackDefault
            r = r.copy(senderEndTracks = r.senderEndTracks + TrackCall(track, Thread.currentThread().threadId()), callOrder = r.callOrder + "sender-end-track")
            changed.signalAll()
            exitLocked()
            return result
        }
    }

    override fun senderConvertToVod(token: Long, items: List<Pair<Long, ULong>>): BackendCommandResult {
        lock.withLock {
            enterLocked("senderConvertToVod")
            noteServiceThreadLocked("senderConvertToVod")
            senderCheckLocked("senderConvertToVod", token)
            for ((t, _) in items) senderTrackCheckLocked("senderConvertToVod", t)
            val result = senderCommandResult ?: BackendCommandResult.Ok
            r = r.copy(senderVodConversions = r.senderVodConversions + VodCall(items, Thread.currentThread().threadId()), callOrder = r.callOrder + "sender-convert-to-vod")
            changed.signalAll()
            exitLocked()
            return result
        }
    }

    override fun senderComplete(token: Long): BackendCommandResult {
        lock.withLock {
            enterLocked("senderComplete")
            noteServiceThreadLocked("senderComplete")
            senderCheckLocked("senderComplete", token)
            val result = senderCommandResult ?: BackendCommandResult.Ok
            r = r.copy(senderCompletes = r.senderCompletes + token, callOrder = r.callOrder + "sender-complete")
            changed.signalAll()
            exitLocked()
            return result
        }
    }

    override fun senderProbe(token: Long): BackendSenderProbeResult {
        lock.withLock {
            enterLocked("senderProbe")
            noteServiceThreadLocked("senderProbe")
            senderCheckLocked("senderProbe", token)
            if (probeHolds > 0) {
                // Held INSIDE the call, before the record: the observation has
                // begun and nothing of it has reached the engine.
                probeHolds--
                r = r.copy(senderProbeHeld = true)
                changed.signalAll()
                inCall = false
                while (probeReleases == 0) changed.await()
                probeReleases--
                inCall = true
                r = r.copy(senderProbeHeld = false)
            }
            val fatal = senderFatal
            val terminal: SenderTerminal? =
                when {
                    fatal != null -> SenderTerminal.Fatal(SenderFatalCode(fatal))
                    senderEndpointClosed -> SenderTerminal.Closed
                    else -> null
                }
            val status = SenderStatus(ready = senderReady && terminal == null, terminal = terminal, mediaDemand = senderMediaDemand)
            r = r.copy(senderProbes = r.senderProbes + 1, callOrder = r.callOrder + "sender-probe")
            changed.signalAll()
            exitLocked()
            return BackendSenderProbeResult.Status(status)
        }
    }

    override fun senderTrackDemand(token: Long, track: Long): BackendDemandResult {
        lock.withLock {
            enterLocked("senderTrackDemand")
            noteServiceThreadLocked("senderTrackDemand")
            senderCheckLocked("senderTrackDemand", token)
            senderTrackCheckLocked("senderTrackDemand", track)
            r = r.copy(senderDemandReads = r.senderDemandReads + TrackCall(track, Thread.currentThread().threadId()), callOrder = r.callOrder + "sender-track-demand")
            changed.signalAll()
            exitLocked()
            return BackendDemandResult.Count(trackDemands[track] ?: 0u)
        }
    }

    override fun senderStats(token: Long): BackendSenderStatsResult {
        lock.withLock {
            enterLocked("senderStats")
            noteServiceThreadLocked("senderStats")
            senderCheckLocked("senderStats", token)
            r = r.copy(senderStatsReads = r.senderStatsReads + TrackCall(token, Thread.currentThread().threadId()), callOrder = r.callOrder + "sender-stats")
            changed.signalAll()
            exitLocked()
            return BackendSenderStatsResult.Stats(scriptedSenderStats)
        }
    }

    /** The scripted stats base: the live scripted object queue depth and deferral counters are added to its `objectsQueued` and deferral fields. */
    internal var scriptedStats: ReceiverStats = ReceiverStats(0uL, 0uL, 0uL, 0uL, 0uL, 0uL, 0uL, 0uL, 0uL, false, 0uL, false, 0uL, 0uL)
    internal var statsResult: BackendStatsResult? = null

    /** The scripted native object queue, and what [pollObject] answers once it is empty (null = the live, empty answer). */
    private val objectQueue = ArrayDeque<BackendObject>()
    internal var pollObjectTerminal: BackendObjectResult? = null
    private var objectHolds = 0
    private var objectReleases = 0

    /**
     * When set, the drain answers this BRIDGE refusal on return whatever the
     * latch or closed state says: the token was refused at the row, which no
     * endpoint state masks. Narrow, for the quarantine-during-retirement row.
     */
    internal var drainBridgeOnReturn: BridgeFailure? = null

    /**
     * Model ONE source-grounded path of the C drain's pending loop
     * (`endpoint.c`, `moq_endpoint_drain`): while the flush is pending it
     * wakes the facade and blocks in its own `moq_endpoint_wait` slice, which
     * CONSUMES the retained activity level, and then completes successfully.
     * When set, a drain that was held takes the level on its way out of that
     * successful completion and records it, so a row can prove that activity
     * serviced by the drain is not lost to the engine. It says nothing about
     * the interrupt, close or bridge-refusal returns, which leave the level
     * as the C would on those paths (not modelled here).
     */
    @Volatile internal var drainConsumesActivity: Boolean = false
    private var drainHolds = 0
    private var drainReleases = 0
    private var drainEntryHolds = 0
    private var drainEntryReleases = 0

    /** When set, `stop()` answers a BRIDGE refusal instead of a native one. */
    internal var stopBridgeFailure: BridgeFailure? = null

    /** When set, every wait answers a BRIDGE refusal, latched and sticky. */
    private var waitBridgeFailureField: BridgeFailure? = null

    /**
     * When set, every wait answers a BRIDGE refusal, latched and sticky.
     *
     * Written under the backend lock and signalled, so arming it releases a
     * wait that is ALREADY parked -- otherwise the refusal would not be seen
     * until some unrelated wake ended a sixty-second park.
     */
    internal var waitBridgeFailure: BridgeFailure?
        get() = lock.withLock { waitBridgeFailureField }
        set(value) {
            lock.withLock {
                waitBridgeFailureField = value
                changed.signalAll()
            }
        }
    internal var destroyResult: BackendOpResult = BackendOpResult.Ok
    internal var snapshotResult: BackendSnapshotResult =
        BackendSnapshotResult.Ok(
            EndpointSnapshot(
                state = EndpointState.Connecting,
                negotiatedVersion = null,
                terminal = null,
            ),
        )

    // -- EndpointBackend ------------------------------------------------------

    override fun wake(): BackendOpResult {
        lock.withLock {
            enterLocked("wake")

            // `endpoint.h`: wake is best-effort and idempotent, and harmless --
            // a no-op -- after stop or once terminal. It must not resurrect
            // activity on an endpoint that is already going away.
            //
            // The three causes are recorded SEPARATELY because they are not
            // equally significant. A wake after the engine's own stop is an
            // engine lifecycle error; a wake ignored because the transport
            // terminalised the endpoint asynchronously is contractually benign
            // and races every legitimate submission; a wake after destroy is a
            // use-after-free and is additionally a violation.
            if (stopped || closed || destroyed) {
                r =
                    when {
                        destroyed -> r.copy(postDestroyWakeCount = r.postDestroyWakeCount + 1)
                        stopped -> r.copy(postStopWakeCount = r.postStopWakeCount + 1)
                        else -> r.copy(externalTerminalWakeCount = r.externalTerminalWakeCount + 1)
                    }
                onWake?.invoke()
                exitLocked()
                return wakeResult
            }

            activityLevel = true // coalesced: repeated wakes collapse into one
            r = r.copy(wakeCount = r.wakeCount + 1, activityLevel = true)
            changed.signalAll()
            onWake?.invoke()

            // Ordering barrier: when armed, the WAKER does not proceed until the
            // single consumer has taken the level. That makes "enqueue before
            // wake" externally observable without any production seam -- a wake
            // published before its job is enqueued is consumed while the queue
            // is still empty, and the job is stranded.
            if (wakeBarrier) {
                inCall = false
                var waited = 0L
                while (activityLevel && waited < BARRIER_CEILING_NANOS) {
                    val start = System.nanoTime()
                    changed.awaitNanos(BARRIER_CEILING_NANOS - waited)
                    waited += System.nanoTime() - start
                }
                if (activityLevel) {
                    failures.record("wake barrier: the published wake was never consumed")
                }
                inCall = true
            }
            exitLocked()
            return wakeResult
        }
    }

    override fun setInterrupted(interrupted: Boolean): BackendOpResult {
        lock.withLock {
            enterLocked("setInterrupted")
            val result = setInterruptedResult
            r =
                r.copy(
                    latchSets = r.latchSets + interrupted,
                    latchThreadIds = r.latchThreadIds + Thread.currentThread().threadId(),
                )
            if (result is BackendOpResult.Ok) this.interrupted = interrupted
            changed.signalAll()
            exitLocked()
            return result
        }
    }

    /**
     * Models the C loop: the latch answers first on every check, a closed
     * endpoint answers CLOSED, and while a hold is armed the call blocks until
     * released -- or until the latch or close lands, which end it at once.
     */
    override fun drain(timeoutMicros: Long): BackendDrainResult {
        lock.withLock {
            // The PRE-ENTRY window: the engine has granted the native turn but
            // this thread has not reached the row yet. Nothing is recorded and
            // nothing shortcuts it -- it models a thread that has not arrived.
            if (drainEntryHolds > 0) {
                drainEntryHolds--
                r = r.copy(drainEntryHeld = true)
                changed.signalAll()
                while (drainEntryReleases == 0) changed.await()
                drainEntryReleases--
                r = r.copy(drainEntryHeld = false)
            }
            enterLocked("drain")
            r =
                r.copy(
                    drainCalls = r.drainCalls + 1,
                    drainTimeouts = r.drainTimeouts + timeoutMicros,
                    drainThreadIds = r.drainThreadIds + Thread.currentThread().threadId(),
                    drainInFlight = true,
                    drainEnteredWhileWaitParked = r.drainEnteredWhileWaitParked || r.waitParked,
                )
            changed.signalAll()
            if (drainHolds > 0) {
                drainHolds--
                inCall = false // a held drain is not "in progress" for reentrancy
                while (drainReleases == 0 && !interrupted && !closed) changed.await()
                if (drainReleases > 0 && !interrupted && !closed) drainReleases--
                inCall = true
            }
            val bridge = drainBridgeOnReturn
            if (drainConsumesActivity && activityLevel && bridge == null && !interrupted && !closed) {
                activityLevel = false
                r = r.copy(activityLevel = false, drainConsumedLevels = r.drainConsumedLevels + 1, callOrder = r.callOrder + "drain-consumed-level")
            }
            val result =
                when {
                    bridge != null -> BackendDrainResult.Bridge(bridge)
                    interrupted -> BackendDrainResult.Interrupted
                    closed -> BackendDrainResult.Closed
                    else -> drainResult
                }
            r = r.copy(drainInFlight = false, callOrder = r.callOrder + "drain") // recorded on RETURN
            changed.signalAll()
            exitLocked()
            return result
        }
    }

    override fun waitForActivity(timeoutMicros: Long): BackendWaitResult {
        lock.withLock {
            enterLocked("waitForActivity")
            noteServiceThreadLocked("waitForActivity")
            r =
                r.copy(
                    waitEntries = r.waitEntries + 1,
                    waitTimeouts = r.waitTimeouts + timeoutMicros,
                )

            // A zero timeout elapses immediately: evaluate and return, never park.
            if (timeoutMicros > 0) {
                r = r.copy(waitParked = true)
                changed.signalAll()
                inCall = false // a parked wait is not "in progress" for reentrancy
                while (!activityLevel && !interrupted && !closed && !expireRequested &&
                    waitBridgeFailureField == null
                ) {
                    changed.await()
                }
                inCall = true
                r = r.copy(waitParked = false)
            }

            val result =
                when {
                    // A bridge refusal outranks every endpoint answer: the wait
                    // never reached the endpoint, so no endpoint state applies.
                    waitBridgeFailureField != null ->
                        BackendWaitResult.Bridge(waitBridgeFailureField!!)
                    interrupted -> BackendWaitResult.INTERRUPTED // latch beats level
                    closed -> BackendWaitResult.CLOSED
                    activityLevel -> {
                        activityLevel = false // the single consumer takes the level
                        BackendWaitResult.ACTIVITY
                    }
                    else -> {
                        if (expireRequested) {
                            expireRequested = false
                            r = r.copy(expiries = r.expiries + 1)
                        }
                        BackendWaitResult.TIMEOUT
                    }
                }
            r = r.copy(activityLevel = activityLevel, waitReturns = r.waitReturns + result)
            changed.signalAll()

            // Hold the computed result inside the call, so a test can act while
            // the engine is provably between "the endpoint answered" and "the
            // engine saw the answer". `inCall` is dropped for the hold: calls
            // arriving during it are ordinary concurrent calls, not reentrancy.
            if (holdReturn) {
                holdReturn = false
                r = r.copy(waitReturnHeld = true)
                inCall = false
                changed.signalAll()
                while (!releaseReturn) changed.await()
                releaseReturn = false
                inCall = true
                r = r.copy(waitReturnHeld = false)
                changed.signalAll()
            }
            exitLocked()
            return result
        }
    }

    override fun stop(): BackendStopResult {
        lock.withLock {
            enterLocked("stop")
            noteServiceThreadLocked("stop")
            if (liveChildren.isNotEmpty()) {
                // The C tier: moq_endpoint_stop refuses WRONG_STATE while a
                // receiver is attached. Recorded as a violation AND answered
                // as the refusal, so a wrong teardown order fails by name.
                r =
                    r.copy(
                        violations = r.violations + "stop while a receiver is attached: ${liveChildren.toList()}",
                        stopCount = r.stopCount + 1,
                        callOrder = r.callOrder + "stop-refused",
                    )
                changed.signalAll()
                exitLocked()
                return BackendStopResult.Refused(MoqResultCode(-5))
            }

            // A REFUSED native stop. The endpoint is NOT stopped and NOT
            // closed: the C call returned an error and changed nothing, so the
            // native object is still live and destroying it would free an
            // endpoint the service tier still owns.
            val bridge = stopBridgeFailure
            if (bridge != null) {
                r = r.copy(stopCount = r.stopCount + 1, callOrder = r.callOrder + "stop-bridge")
                changed.signalAll()
                onStop?.invoke()
                exitLocked()
                return BackendStopResult.Bridge(bridge)
            }

            val refusal = refuseStopCode
            if (refusal != null) {
                r =
                    r.copy(
                        stopCount = r.stopCount + 1,
                        stopRefusedCode = refusal,
                        callOrder = r.callOrder + "stop-refused",
                    )
                changed.signalAll()
                onStop?.invoke()
                exitLocked()
                return BackendStopResult.Refused(refusal)
            }

            stopped = true
            closed = true // endpoint.c:1877-1880 -- stop sets the closed state
            r = r.copy(stopCount = r.stopCount + 1, callOrder = r.callOrder + "stop")
            changed.signalAll()
            // Invoked while the call is still in progress: a hook that calls
            // back into the backend is a REAL reentrancy, and the tripwire must
            // see it rather than a tidied-up serialised version.
            onStop?.invoke()
            exitLocked()
            return BackendStopResult.Stopped
        }
    }

    override fun snapshot(): BackendSnapshotResult {
        lock.withLock {
            enterLocked("snapshot")
            noteServiceThreadLocked("snapshot")
            r = r.copy(callOrder = r.callOrder + "snapshot")
            exitLocked()
            return snapshotResult
        }
    }

    override fun attachReceiver(config: ReceiverConfig): BackendAttachResult {
        lock.withLock {
            enterLocked("attachReceiver")
            noteServiceThreadLocked("attachReceiver")
            if (stopped || closed || destroyed) {
                r = r.copy(violations = r.violations + "attachReceiver on a stopped, closed or destroyed root")
            }
            val scripted = attachResult
            val result =
                when {
                    scripted != null -> scripted
                    // One receiver per endpoint: the C tier's WRONG_STATE.
                    liveChildren.isNotEmpty() -> BackendAttachResult.Refused(MoqResultCode(-5))
                    else -> {
                        nextChild += 1
                        BackendAttachResult.Attached(CHILD_BASE + nextChild)
                    }
                }
            if (result is BackendAttachResult.Attached) liveChildren += result.token
            r =
                r.copy(
                    receiverAttaches = r.receiverAttaches +
                        ReceiverAttach(config, Thread.currentThread().threadId(), (result as? BackendAttachResult.Attached)?.token ?: 0L),
                    callOrder = r.callOrder + "receiver-attach",
                )
            changed.signalAll()
            exitLocked()
            return result
        }
    }

    override fun closeReceiver(token: Long): BackendOpResult {
        lock.withLock {
            enterLocked("closeReceiver")
            noteServiceThreadLocked("closeReceiver")
            if (token !in liveChildren) {
                r = r.copy(violations = r.violations + "closeReceiver of a token that is not a live child: $token")
            }
            if (stopped) {
                r = r.copy(violations = r.violations + "closeReceiver after stop: the C stop would have refused while attached")
            }
            liveChildren -= token
            r = r.copy(receiverCloses = r.receiverCloses + token, callOrder = r.callOrder + "receiver-destroy")
            changed.signalAll()
            exitLocked()
            return closeReceiverResult
        }
    }

    override fun pollTrack(token: Long): BackendTrackEventResult {
        lock.withLock {
            enterLocked("pollTrack")
            noteServiceThreadLocked("pollTrack")
            if (token !in liveChildren) {
                r = r.copy(violations = r.violations + "pollTrack of a token that is not a live child: $token")
            }
            val next = trackQueue.removeFirstOrNull()
            if (next != null && next.kind == BackendTrackEventKind.Ended) endedTracks += next.track
            val result =
                when {
                    next != null -> BackendTrackEventResult.Event(next)
                    else -> pollTrackTerminal ?: BackendTrackEventResult.Empty
                }
            r =
                r.copy(
                    trackPolls = r.trackPolls + TrackPoll(token, Thread.currentThread().threadId(), result),
                    trackPollsLeftQueued = trackQueue.size,
                    callOrder = r.callOrder + "receiver-poll-track",
                )
            changed.signalAll()
            exitLocked()
            return result
        }
    }

    override fun pollObject(token: Long): BackendObjectResult {
        lock.withLock {
            enterLocked("pollObject")
            noteServiceThreadLocked("pollObject")
            if (token !in liveChildren) {
                r = r.copy(violations = r.violations + "pollObject of a token that is not a live child: $token")
            }
            if (objectHolds > 0) {
                // Held INSIDE the call: the poll has begun, the answer has not
                // reached the engine; a test acts in exactly that window.
                objectHolds--
                r = r.copy(objectPollHeld = true)
                changed.signalAll()
                inCall = false
                while (objectReleases == 0) changed.await()
                objectReleases--
                inCall = true
                r = r.copy(objectPollHeld = false)
            }
            // Only an object whose track is wanted AND accepted is deliverable
            // (the C admits objects for subscribed tracks only); the rest stay.
            val next = if (interrupted) null else objectQueue.firstOrNull { deliverableLocked(it.track) }?.also { objectQueue.remove(it) }
            val result =
                when {
                    // The C gate: the latch answers before the queue is read.
                    interrupted -> BackendObjectResult.Interrupted
                    next != null -> BackendObjectResult.Object(next)
                    else -> pollObjectTerminal ?: BackendObjectResult.Empty
                }
            r =
                r.copy(
                    objectPolls = r.objectPolls + ObjectPoll(token, Thread.currentThread().threadId(), result),
                    objectsLeftQueued = objectQueue.size,
                    objectsDeliverable = objectQueue.count { deliverableLocked(it.track) },
                    callOrder = r.callOrder + "receiver-poll-object",
                )
            changed.signalAll()
            exitLocked()
            return result
        }
    }

    /** A track with no subscription control ever scripted is "accepted" by default, so the object rows before the controls slice keep their meaning. */
    private fun deliverableLocked(track: Long): Boolean =
        (track !in desiredTracks && track !in issuedTracks && track !in acceptedTracks) || (track in desiredTracks && track in acceptedTracks)

    override fun subscribeTrack(token: Long, track: Long, start: StartMode, priority: UByte?): BackendCommandResult {
        lock.withLock {
            enterLocked("subscribeTrack")
            noteServiceThreadLocked("subscribeTrack")
            if (token !in liveChildren) r = r.copy(violations = r.violations + "subscribeTrack of a token that is not a live child: $token")
            val result =
                subscribeResult ?: when {
                    track in endedTracks -> BackendCommandResult.Refused(MoqResultCode(-5))
                    else -> {
                        desiredTracks += track
                        issuedTracks += track
                        BackendCommandResult.Ok
                    }
                }
            r = r.copy(subscribeCalls = r.subscribeCalls + SubscribeCall(track, start, priority, Thread.currentThread().threadId()), callOrder = r.callOrder + "track-subscribe")
            afterSubscribe?.invoke()
            recomputeDeliverableLocked()
            changed.signalAll()
            exitLocked()
            return result
        }
    }

    override fun unsubscribeTrack(token: Long, track: Long): BackendCommandResult {
        lock.withLock {
            enterLocked("unsubscribeTrack")
            noteServiceThreadLocked("unsubscribeTrack")
            if (token !in liveChildren) r = r.copy(violations = r.violations + "unsubscribeTrack of a token that is not a live child: $token")
            // The answer is decided BEFORE any side effect; only an Ok clears
            // the desire and purges (the C purges under the queue lock).
            val result = unsubscribeResult ?: BackendCommandResult.Ok
            if (result is BackendCommandResult.Ok) {
                desiredTracks -= track
                objectQueue.removeAll { it.track == track }
            }
            r = r.copy(unsubscribeCalls = r.unsubscribeCalls + TrackCall(track, Thread.currentThread().threadId()), objectsLeftQueued = objectQueue.size, callOrder = r.callOrder + "track-unsubscribe")
            afterUnsubscribe?.invoke()
            recomputeDeliverableLocked()
            changed.signalAll()
            exitLocked()
            return result
        }
    }

    override fun trackState(token: Long, track: Long): BackendTrackStateResult {
        lock.withLock {
            enterLocked("trackState")
            noteServiceThreadLocked("trackState")
            if (token !in liveChildren) r = r.copy(violations = r.violations + "trackState of a token that is not a live child: $token")
            val override = trackStateResult
            val result: BackendTrackStateResult =
                when {
                    override is BackendCommandResult.Refused -> BackendTrackStateResult.Refused(override.code)
                    override is BackendCommandResult.Bridge -> BackendTrackStateResult.Bridge(override.failure)
                    track in endedTracks -> BackendTrackStateResult.State(5)
                    track !in desiredTracks -> BackendTrackStateResult.State(if (track in issuedTracks) 3 else 0)
                    track !in acceptedTracks -> BackendTrackStateResult.State(1)
                    flowPaused -> BackendTrackStateResult.State(4)
                    else -> BackendTrackStateResult.State(2)
                }
            r = r.copy(trackStateCalls = r.trackStateCalls + TrackCall(track, Thread.currentThread().threadId()), callOrder = r.callOrder + "track-state")
            afterTrackState?.invoke()
            changed.signalAll()
            exitLocked()
            return result
        }
    }

    override fun pollSap(token: Long): BackendRecordResult<BackendSapRecord> {
        lock.withLock {
            enterLocked("pollSap")
            noteServiceThreadLocked("pollSap")
            if (token !in liveChildren) r = r.copy(violations = r.violations + "pollSap of a token that is not a live child: $token")
            if (sapHolds > 0) {
                sapHolds--
                r = r.copy(sapPollHeld = true)
                changed.signalAll()
                inCall = false
                while (sapReleases == 0) changed.await()
                sapReleases--
                inCall = true
                r = r.copy(sapPollHeld = false)
            }
            val next = sapQueue.removeFirstOrNull()
            val result: BackendRecordResult<BackendSapRecord> =
                when {
                    next != null -> BackendRecordResult.Record(next, sapDeferrals)
                    else -> pollSapTerminal ?: BackendRecordResult.Empty(sapDeferrals)
                }
            r = r.copy(sapPolls = r.sapPolls + RecordPoll(token, Thread.currentThread().threadId(), result), callOrder = r.callOrder + "receiver-poll-sap")
            changed.signalAll()
            exitLocked()
            return result
        }
    }

    override fun pollMediaTimeline(token: Long): BackendRecordResult<BackendTimelineRecord> {
        lock.withLock {
            enterLocked("pollMediaTimeline")
            noteServiceThreadLocked("pollMediaTimeline")
            if (token !in liveChildren) r = r.copy(violations = r.violations + "pollMediaTimeline of a token that is not a live child: $token")
            val next = timelineQueue.removeFirstOrNull()
            val result: BackendRecordResult<BackendTimelineRecord> =
                when {
                    next != null -> BackendRecordResult.Record(next, timelineDeferrals)
                    else -> pollTimelineTerminal ?: BackendRecordResult.Empty(timelineDeferrals)
                }
            r = r.copy(timelinePolls = r.timelinePolls + RecordPoll(token, Thread.currentThread().threadId(), result), callOrder = r.callOrder + "receiver-poll-timeline")
            changed.signalAll()
            exitLocked()
            return result
        }
    }

    override fun receiverStats(token: Long): BackendStatsResult {
        lock.withLock {
            enterLocked("receiverStats")
            noteServiceThreadLocked("receiverStats")
            if (token !in liveChildren) r = r.copy(violations = r.violations + "receiverStats of a token that is not a live child: $token")
            val result =
                statsResult ?: BackendStatsResult.Stats(
                    scriptedStats.copy(
                        objectsQueued = scriptedStats.objectsQueued + objectQueue.size.toULong(),
                        sapRecordDeferrals = scriptedStats.sapRecordDeferrals + sapDeferrals,
                        mediaTimelineRecordDeferrals = scriptedStats.mediaTimelineRecordDeferrals + timelineDeferrals,
                    ),
                )
            r = r.copy(statsReads = r.statsReads + TrackCall(token, Thread.currentThread().threadId()), callOrder = r.callOrder + "receiver-stats")
            changed.signalAll()
            exitLocked()
            return result
        }
    }

    internal fun scriptSapRecords(vararg records: BackendSapRecord) = lock.withLock {
        sapQueue.addAll(records)
        changed.signalAll()
    }

    internal fun scriptTimelineRecords(vararg records: BackendTimelineRecord) = lock.withLock {
        timelineQueue.addAll(records)
        changed.signalAll()
    }

    /** The next sap poll is held inside the call until [releaseSapPoll]. */
    internal fun holdNextSapPoll() = lock.withLock {
        sapHolds++
        changed.signalAll()
    }

    internal fun releaseSapPoll() = lock.withLock {
        sapReleases++
        changed.signalAll()
    }

    private fun recomputeDeliverableLocked() {
        r = r.copy(objectsDeliverable = objectQueue.count { deliverableLocked(it.track) })
    }

    /** The peer accepted the subscription for [track]: its queued objects become deliverable. */
    internal fun acceptSubscription(track: Long) = lock.withLock {
        acceptedTracks += track
        recomputeDeliverableLocked()
        changed.signalAll()
    }

    override fun destroy(): BackendOpResult {
        lock.withLock {
            enterLocked("destroy") // a second destroy is itself a violation
            noteServiceThreadLocked("destroy")
            destroyed = true
            r = r.copy(destroyCount = r.destroyCount + 1, callOrder = r.callOrder + "destroy")
            changed.signalAll()
            onDestroy?.invoke()
            exitLocked()
            return destroyResult
        }
    }

    // -- tripwires ------------------------------------------------------------

    private fun enterLocked(name: String) {
        if (destroyed) {
            r = r.copy(violations = r.violations + "post-destroy call: $name")
        }
        if (inCall) {
            r = r.copy(violations = r.violations + "reentrant backend call: $name")
        }
        inCall = true
    }

    private fun exitLocked() {
        inCall = false
    }

    /**
     * Record the thread that owns the blocking/mutating surface BY ID. The
     * first such call defines the service thread; every later call from a
     * different thread is a confinement violation. Names are diagnostics only:
     * two live threads may share a name, and identity may not rest on that.
     */
    private fun noteServiceThreadLocked(name: String) {
        val t = Thread.currentThread()
        val known = r.serviceThreadId
        if (known == null) {
            r = r.copy(serviceThreadId = t.threadId(), serviceThreadName = t.name, serviceThread = t)
        } else if (known != t.threadId()) {
            val message =
                "$name ran on thread id ${t.threadId()} ('${t.name}') but the service " +
                    "thread is id $known ('${r.serviceThreadName}')"
            r = r.copy(violations = r.violations + message)
        }
    }

    // -- test control ---------------------------------------------------------

    /** Make the next parked wait return [BackendWaitResult.TIMEOUT]. */
    internal fun expireWait() = lock.withLock {
        expireRequested = true
        changed.signalAll()
    }

    /** Hold the NEXT wait's computed result until [releaseHeldWaitReturn]. */
    internal fun holdNextWaitReturn() = lock.withLock {
        holdReturn = true
        changed.signalAll()
    }

    internal fun releaseHeldWaitReturn() = lock.withLock {
        releaseReturn = true
        changed.signalAll()
    }

    /** Hold each waker until the single consumer has taken its level. */
    internal fun armWakeBarrier() = lock.withLock {
        wakeBarrier = true
        changed.signalAll()
    }

    /**
     * Make the next `stop()` report a native refusal carrying [code].
     *
     * This is a WITNESS, not a policy: it records what the C tier answered and
     * leaves the endpoint live. It cannot make the engine behave correctly --
     * the engine reads the same answer from `stop()`'s return value, and this
     * record is what lets a test check the code independently of it.
     */
    internal fun refuseStopWith(code: MoqResultCode) = lock.withLock {
        refuseStopCode = code
        changed.signalAll()
    }

    /** The next drain entered blocks until [releaseDrain], the latch, or close. */
    internal fun holdDrain() = lock.withLock {
        drainHolds++
        changed.signalAll()
    }

    internal fun releaseDrain() = lock.withLock {
        drainReleases++
        changed.signalAll()
    }

    /** The next drain call is held BEFORE it is recorded or entered, until [releaseDrainEntry]. */
    internal fun holdNextDrainEntry() = lock.withLock {
        drainEntryHolds++
        changed.signalAll()
    }

    internal fun releaseDrainEntry() = lock.withLock {
        drainEntryReleases++
        changed.signalAll()
    }

    /** Fixture arm: the latch as the C tier would hold it, without an engine call. */
    internal fun armInterrupted(value: Boolean) = lock.withLock {
        interrupted = value
        changed.signalAll()
    }

    internal fun setClosed(value: Boolean) = lock.withLock {
        closed = value
        changed.signalAll()
    }

    /** Queue track events for the next polls, as the receiver's pump would. */
    internal fun scriptTrackEvents(vararg events: BackendTrackEvent) = lock.withLock {
        trackQueue.addAll(events)
        r = r.copy(trackPollsLeftQueued = trackQueue.size)
        changed.signalAll()
    }

    /** Queue objects for the next demands, as the receiver's intake would. */
    internal fun scriptObjects(vararg objects: BackendObject) = lock.withLock {
        objectQueue.addAll(objects)
        r = r.copy(objectsLeftQueued = objectQueue.size)
        recomputeDeliverableLocked()
        changed.signalAll()
    }

    /** The next object poll is held inside the call until [releaseObjectPoll]. */
    internal fun holdNextObjectPoll() = lock.withLock {
        objectHolds++
        changed.signalAll()
    }

    internal fun releaseObjectPoll() = lock.withLock {
        objectReleases++
        changed.signalAll()
    }

    /** Raise the activity level directly, as a pump cycle would. */
    internal fun markActivity() = lock.withLock {
        activityLevel = true
        r = r.copy(activityLevel = true)
        changed.signalAll()
    }

    internal fun observed(): Snapshot = lock.withLock { r }

    /**
     * Block the calling thread until [predicate] holds for a snapshot taken
     * under the lock.
     *
     * The result is always consumed by a named assertion at the call site, so
     * an expiry here cannot release anything silently. The sticky registry is
     * reserved for the wake barrier and for [Gate], whose returns a job body
     * genuinely ignores.
     */
    internal fun awaitCondition(
        what: String,
        ceilingMillis: Long = CEILING_MILLIS,
        predicate: (Snapshot) -> Boolean,
    ): Boolean {
        val deadline = System.nanoTime() + ceilingMillis * 1_000_000
        lock.withLock {
            while (!predicate(r)) {
                val remaining = deadline - System.nanoTime()
                if (remaining <= 0) return false
                changed.awaitNanos(remaining)
            }
            return true
        }
    }

    internal companion object {
        /**
         * Deadlock ceiling. Not a timing oracle: no assertion depends on how
         * long anything takes, only on a hung engine failing by name.
         */
        const val CEILING_MILLIS: Long = 5_000

        /** Child tokens as the registry would mint them: bit 63 set, kind 2 in the low byte. */
        const val CHILD_BASE: Long = Long.MIN_VALUE + 0x0102L

        /** The scripted sender child tokens and its track tokens: distinct from the receiver's, full width. */
        const val SENDER_BASE: Long = Long.MIN_VALUE + 0x0202L
        const val TRACK_BASE: Long = Long.MIN_VALUE + 0x0302L

        private const val BARRIER_CEILING_NANOS: Long = 5_000L * 1_000_000

        /**
         * The defensive wait slice engine tests pass. Positive by contract: a
         * zero timeout would elapse immediately and busy-spin the loop.
         */
        const val WAIT_SLICE_MICROS: Long = 60_000_000
    }
}
