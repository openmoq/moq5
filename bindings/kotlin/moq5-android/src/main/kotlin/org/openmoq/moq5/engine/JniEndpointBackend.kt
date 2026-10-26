package org.openmoq.moq5.engine

import org.openmoq.moq5.EndpointSnapshotMapper
import org.openmoq.moq5.MoqResultCode
import org.openmoq.moq5.NativeEndpointOffer
import org.openmoq.moq5.ReceiverConfig
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.SendTrackConfig
import org.openmoq.moq5.SenderConfig
import org.openmoq.moq5.StartMode
import org.openmoq.moq5.toNativeMicros
import org.openmoq.moq5.TrackDescription
import org.openmoq.moq5.nativeOrdinal
import org.openmoq.moq5.NativeBridgeInitializationException
import org.openmoq.moq5.jni.BridgeInit
import org.openmoq.moq5.jni.NativeBridge
import org.openmoq.moq5.jni.NativeBridgeOwner
import org.openmoq.moq5.toInitializationFailure
import org.openmoq.moq5.jni.ReceiverEventScalars
import org.openmoq.moq5.jni.ReceiverObjectScalars
import org.openmoq.moq5.jni.ReceiverSapScalars
import org.openmoq.moq5.jni.ReceiverStatsScalars
import org.openmoq.moq5.jni.ReceiverTimelineScalars
import org.openmoq.moq5.jni.SendTrackScalars
import org.openmoq.moq5.jni.SenderAttachScalars
import org.openmoq.moq5.jni.SenderProbeScalars
import org.openmoq.moq5.jni.SenderStatsScalars
import org.openmoq.moq5.jni.TrackDescribeScalars

/**
 * The real [EndpointBackend]: one private registry token, and nothing else.
 *
 * It holds no pointer. The token is opaque, full-width and generation-tagged,
 * so a stale one is REFUSED by the registry rather than dereferenced -- which
 * is why this class can be a plain value over a `Long` and still be safe.
 *
 * Two mapping rules run through every method here, and both exist to stop one
 * domain masquerading as another:
 *
 *  * a PRIVATE REGISTRY refusal (`MOQ5_ERR_REGISTRY_BASE` plus the negative
 *    `moq5_reg_result_t`, and the host-only lifecycle space below it) is the
 *    bridge's own vocabulary. It becomes a [BridgeFailure] carrying the exact
 *    code, and is NEVER wrapped in a `MoqResultCode` -- doing so would claim
 *    the C endpoint answered something it never saw;
 *  * an ordinary `moq_result_t` from the ENDPOINT is carried exactly, without
 *    normalisation, into whichever result its operation defines.
 *
 * Construction is deliberately not a constructor call: [create] runs on the
 * engine worker through [factory], so the same exact thread that creates the
 * root also waits, snapshots, stops and destroys it. The registry enforces that
 * confinement itself and refuses a stateful call from any other thread with its
 * `THREAD` category.
 */
internal class JniEndpointBackend private constructor(
    /** The full-width registry token. Never a pointer; never truncated. */
    internal val token: Long,
) : EndpointBackend {
    override fun wake(): BackendOpResult {
        // `moq_endpoint_wake()` cannot fail, and the JNI row leases rather than
        // acquires the root, so it is not authority-confined. The only non-OK
        // answer is the registry refusing the token itself.
        val rc = NativeBridge.nativeEndpointWake(token)
        return if (rc == MOQ_OK) BackendOpResult.Ok else BackendOpResult.Bridge(failure("wake", rc))
    }

    override fun setInterrupted(interrupted: Boolean): BackendOpResult {
        val rc = NativeBridge.nativeEndpointSetInterrupted(token, interrupted)
        return if (rc == MOQ_OK) BackendOpResult.Ok else BackendOpResult.Bridge(failure("set_interrupted", rc))
    }

    override fun drain(timeoutMicros: Long): BackendDrainResult =
        drainResultOf(NativeBridge.nativeEndpointDrain(token, timeoutMicros))

    override fun waitForActivity(timeoutMicros: Long): BackendWaitResult =
        when (val rc = NativeBridge.nativeEndpointWait(token, timeoutMicros)) {
            MOQ_OK -> BackendWaitResult.ACTIVITY
            MOQ_DONE -> BackendWaitResult.TIMEOUT
            MOQ_ERR_INTERRUPTED -> BackendWaitResult.INTERRUPTED
            MOQ_ERR_CLOSED -> BackendWaitResult.CLOSED
            // `moq_endpoint_wait()`'s contract is exactly those four. Anything
            // else -- a registry refusal, or an endpoint result outside the
            // documented set -- means the BRIDGE's contract was broken, so it is
            // carried whole rather than folded into CLOSED or TIMEOUT.
            else -> BackendWaitResult.Bridge(failure("wait", rc))
        }

    override fun snapshot(): BackendSnapshotResult {
        // ONE native call for the whole record: state, negotiated version,
        // terminal reason and terminal detail are never re-read separately, so
        // the four fields cannot come from different instants.
        val out = LongArray(SNAPSHOT_FIELDS)
        val rc = NativeBridge.nativeEndpointSnapshot(token, out)
        if (rc != MOQ_OK) return BackendSnapshotResult.Bridge(failure("snapshot", rc))
        return BackendSnapshotResult.Ok(
            EndpointSnapshotMapper.map(
                state = out[0].toInt(),
                // Zero means absent; every nonzero value, known draft or not,
                // crosses unchanged rather than being clamped to a known one.
                negotiatedVersion = out[1].toUInt(),
                terminalReason = out[2].toInt(),
                // All 64 bits, reinterpreted rather than widened or narrowed.
                terminalDetail = out[3].toULong(),
            ),
        )
    }

    override fun stop(): BackendStopResult {
        val rc = NativeBridge.nativeEndpointStop(token)
        return when {
            rc == MOQ_OK -> BackendStopResult.Stopped
            // The bridge refused before the endpoint was reached, so there is
            // no endpoint answer to report.
            isBridgeRefusal(rc) -> BackendStopResult.Bridge(failure("stop", rc))
            // The ENDPOINT refused. Carried exactly: the engine decides what a
            // refusal means, and a normalised code would take that away.
            else -> BackendStopResult.Refused(MoqResultCode(rc))
        }
    }

    override fun attachReceiver(config: ReceiverConfig): BackendAttachResult {
        val parts = Array(config.namespace.partCount) { config.namespace.partAt(it).bytes() }
        val out = LongArray(1)
        val rc = NativeBridge.nativeReceiverAttach(
            token, parts, config.overflow.nativeOrdinal(), config.maxObjects.toInt(), config.maxBytes.toLong(),
            config.maxTrackEvents.toInt(), config.autoSubscribe, config.catalogTrack?.bytes(),
            config.timeMode.nativeOrdinal(), out,
        )
        return when {
            rc == MOQ_OK -> BackendAttachResult.Attached(out[0])
            isBridgeRefusal(rc) -> BackendAttachResult.Bridge(failure("receiver-attach", rc))
            else -> BackendAttachResult.Refused(MoqResultCode(rc))
        }
    }

    override fun closeReceiver(token: Long): BackendOpResult {
        val rc = NativeBridge.nativeReceiverClose(this.token, token)
        return if (rc == MOQ_OK) BackendOpResult.Ok else BackendOpResult.Bridge(failure("receiver-close", rc))
    }

    override fun pollTrack(token: Long): BackendTrackEventResult {
        val sc = LongArray(ReceiverEventScalars.COUNT)
        return when (val rc = NativeBridge.nativeReceiverPollTrack(this.token, token, sc)) {
            MOQ_OK -> {
                val raw = sc[ReceiverEventScalars.KIND]
                // A kind outside the header's enum is the bridge's contract
                // broken, not a new event: carried as the bridge's failure.
                val kind =
                    BackendTrackEventKind.ofNative(raw.toInt())?.takeIf { raw in 1L..7L }
                        ?: return BackendTrackEventResult.Bridge(failure("receiver-poll-track-kind", raw.toInt()))
                val track = sc[ReceiverEventScalars.TRACK]
                val description =
                    when (kind) {
                        BackendTrackEventKind.Added, BackendTrackEventKind.Updated ->
                            when (val d = describe(token, track)) {
                                is Described.Ok -> d.description
                                is Described.Bridge -> return BackendTrackEventResult.Bridge(d.failure)
                            }
                        else -> null
                    }
                BackendTrackEventResult.Event(
                    BackendTrackEvent(
                        kind = kind,
                        track = track,
                        description = description,
                        parseDropsTotal = sc[ReceiverEventScalars.PARSE_DROPS_TOTAL].toULong(),
                        parseDropsDelta = sc[ReceiverEventScalars.PARSE_DROPS_DELTA].toULong(),
                    ),
                )
            }
            MOQ_DONE -> BackendTrackEventResult.Empty
            MOQ_ERR_CLOSED ->
                BackendTrackEventResult.Closed(
                    fatal = sc[ReceiverEventScalars.TERMINAL_FATAL] != 0L,
                    // All 64 bits of the receiver's own code, reinterpreted.
                    code = sc[ReceiverEventScalars.TERMINAL_CODE].toULong(),
                )
            else -> BackendTrackEventResult.Bridge(failure("receiver-poll-track", rc))
        }
    }

    override fun pollObject(token: Long): BackendObjectResult {
        val sc = LongArray(ReceiverObjectScalars.COUNT)
        val bytes = NativeBridge.nativeReceiverPollObject(this.token, token, sc)
        if (bytes != null) return BackendObjectResult.Object(MediaObjectCodec.decode(bytes, sc))
        return when (val rc = sc[ReceiverObjectScalars.RESULT].toInt()) {
            MOQ_DONE -> BackendObjectResult.Empty
            MOQ_ERR_INTERRUPTED -> BackendObjectResult.Interrupted
            MOQ_ERR_CLOSED ->
                BackendObjectResult.Closed(
                    fatal = sc[ReceiverObjectScalars.TERMINAL_FATAL] != 0L,
                    code = sc[ReceiverObjectScalars.TERMINAL_CODE].toULong(),
                )
            // A registry refusal (a refused token mint included), or an answer
            // outside the contract (MOQ_OK with no array included).
            else -> BackendObjectResult.Bridge(failure("receiver-poll-object", rc))
        }
    }

    override fun subscribeTrack(token: Long, track: Long, start: StartMode, priority: UByte?): BackendCommandResult =
        commandResultOf("track-subscribe", NativeBridge.nativeTrackSubscribe(this.token, token, track, start.nativeOrdinal(), priority != null, priority?.toInt() ?: 0))

    override fun unsubscribeTrack(token: Long, track: Long): BackendCommandResult =
        commandResultOf("track-unsubscribe", NativeBridge.nativeTrackUnsubscribe(this.token, token, track))

    override fun trackState(token: Long, track: Long): BackendTrackStateResult {
        val out = LongArray(1)
        val rc = NativeBridge.nativeTrackState(this.token, token, track, out)
        return when {
            rc == MOQ_OK -> BackendTrackStateResult.State(out[0].toInt())
            isBridgeRefusal(rc) -> BackendTrackStateResult.Bridge(failure("track-state", rc))
            else -> BackendTrackStateResult.Refused(MoqResultCode(rc))
        }
    }

    override fun pollSap(token: Long): BackendRecordResult<BackendSapRecord> {
        val sc = LongArray(ReceiverSapScalars.COUNT)
        return recordResultOf("receiver-poll-sap", NativeBridge.nativeReceiverPollSap(this.token, token, sc), sc[ReceiverSapScalars.DEFERRALS], sc[ReceiverSapScalars.TERMINAL_FATAL], sc[ReceiverSapScalars.TERMINAL_CODE]) { TimelineRecordCodec.decodeSap(sc) }
    }

    override fun pollMediaTimeline(token: Long): BackendRecordResult<BackendTimelineRecord> {
        val sc = LongArray(ReceiverTimelineScalars.COUNT)
        return recordResultOf("receiver-poll-timeline", NativeBridge.nativeReceiverPollMediaTimeline(this.token, token, sc), sc[ReceiverTimelineScalars.DEFERRALS], sc[ReceiverTimelineScalars.TERMINAL_FATAL], sc[ReceiverTimelineScalars.TERMINAL_CODE]) { TimelineRecordCodec.decodeTimeline(sc) }
    }

    override fun receiverStats(token: Long): BackendStatsResult {
        val sc = LongArray(ReceiverStatsScalars.COUNT)
        val rc = NativeBridge.nativeReceiverStats(this.token, token, sc)
        return if (rc == MOQ_OK) BackendStatsResult.Stats(ReceiverStatsCodec.decode(sc)) else BackendStatsResult.Bridge(failure("receiver-stats", rc))
    }

    override fun attachSender(config: SenderConfig): BackendAttachResult {
        val parts = Array(config.namespace.partCount) { config.namespace.partAt(it).bytes() }
        val sc = LongArray(SenderAttachScalars.COUNT)
        sc[SenderAttachScalars.BACKPRESSURE] = config.backpressure.nativeOrdinal().toLong()
        sc[SenderAttachScalars.QUEUE_MAX_OBJECTS] = config.queueMaxObjects.toLong()
        sc[SenderAttachScalars.QUEUE_MAX_BYTES] = config.queueMaxBytes.toLong()
        sc[SenderAttachScalars.PRE_READY_MAX_OBJECTS] = config.preReadyMaxObjects.toLong()
        sc[SenderAttachScalars.PRE_READY_MAX_BYTES] = config.preReadyMaxBytes.toLong()
        sc[SenderAttachScalars.VALIDATE_CMAF] = if (config.validateCmaf) 1L else 0L
        sc[SenderAttachScalars.PUBLISH_TRACKS] = if (config.publishTracks) 1L else 0L
        sc[SenderAttachScalars.DROP_WITHOUT_DEMAND] = if (config.dropWithoutDemand) 1L else 0L
        sc[SenderAttachScalars.CATALOG_REFRESH_INTERVAL_US] = config.catalogRefresh.toNativeMicros()
        val out = LongArray(SenderAttachScalars.OUT_COUNT)
        val rc = NativeBridge.nativeSenderAttach(token, parts, config.catalogTrack?.bytes(), sc, out)
        return when {
            rc == MOQ_OK -> BackendAttachResult.Attached(out[0])
            isBridgeRefusal(rc) -> BackendAttachResult.Bridge(failure("sender-attach", rc))
            else -> BackendAttachResult.Refused(MoqResultCode(rc))
        }
    }

    override fun closeSender(token: Long): BackendOpResult {
        val rc = NativeBridge.nativeSenderClose(this.token, token)
        return if (rc == MOQ_OK) BackendOpResult.Ok else BackendOpResult.Bridge(failure("sender-close", rc))
    }

    override fun senderAddTrack(token: Long, config: SendTrackConfig): BackendSenderTrackResult {
        val out = LongArray(SendTrackScalars.OUT_COUNT)
        val rc = NativeBridge.nativeSenderAddTrack(this.token, token, SendTrackConfigCodec.bytes(config), SendTrackConfigCodec.scalars(config), out)
        return when {
            rc == MOQ_OK -> BackendSenderTrackResult.Added(out[0])
            rc == MOQ_ERR_WOULD_BLOCK -> BackendSenderTrackResult.WouldBlock
            isBridgeRefusal(rc) -> BackendSenderTrackResult.Bridge(failure("sender-add-track", rc))
            else -> BackendSenderTrackResult.Refused(MoqResultCode(rc))
        }
    }

    override fun senderRemoveTrack(token: Long, track: Long): BackendCommandResult =
        commandResultOf("sender-remove-track", NativeBridge.nativeSenderRemoveTrack(this.token, token, track))

    override fun senderWrite(token: Long, track: Long, obj: SendObject): BackendWriteResult =
        writeResultOf("sender-write", NativeBridge.nativeSenderWrite(this.token, token, track, obj.payload.bytes(), obj.cmafProperties?.bytes(), SendObjectCodec.encode(obj)))

    override fun senderEndTrack(token: Long, track: Long): BackendWriteResult =
        writeResultOf("sender-end-track", NativeBridge.nativeSenderEndTrack(this.token, token, track))

    override fun senderConvertToVod(token: Long, items: List<Pair<Long, ULong>>): BackendCommandResult =
        commandResultOf("sender-convert-to-vod", NativeBridge.nativeSenderConvertToVod(this.token, token, LongArray(items.size) { items[it].first }, LongArray(items.size) { items[it].second.toLong() }))

    override fun senderComplete(token: Long): BackendCommandResult =
        commandResultOf("sender-complete", NativeBridge.nativeSenderComplete(this.token, token))

    override fun senderProbe(token: Long): BackendSenderProbeResult {
        val sc = LongArray(SenderProbeScalars.COUNT)
        val rc = NativeBridge.nativeSenderProbe(this.token, token, sc)
        return if (rc == MOQ_OK) BackendSenderProbeResult.Status(SenderProbeCodec.decode(sc)) else BackendSenderProbeResult.Bridge(failure("sender-probe", rc))
    }

    override fun senderTrackDemand(token: Long, track: Long): BackendDemandResult {
        val out = LongArray(1)
        val rc = NativeBridge.nativeSenderTrackDemand(this.token, token, track, out)
        // A `size_t` count; a value above UInt is the bridge's contract broken, not a demand.
        if (rc != MOQ_OK) return BackendDemandResult.Bridge(failure("sender-track-demand", rc))
        if (out[0] < 0L || out[0] > UInt.MAX_VALUE.toLong()) return BackendDemandResult.Bridge(failure("sender-track-demand-range", out[0].toInt()))
        return BackendDemandResult.Count(out[0].toUInt())
    }

    override fun senderStats(token: Long): BackendSenderStatsResult {
        val sc = LongArray(SenderStatsScalars.COUNT)
        val rc = NativeBridge.nativeSenderStats(this.token, token, sc)
        return if (rc == MOQ_OK) BackendSenderStatsResult.Stats(SenderStatsCodec.decode(sc)) else BackendSenderStatsResult.Bridge(failure("sender-stats", rc))
    }

    private fun writeResultOf(op: String, rc: Int): BackendWriteResult =
        when {
            rc == MOQ_OK -> BackendWriteResult.Accepted
            rc == MOQ_ERR_WOULD_BLOCK -> BackendWriteResult.WouldBlock
            isBridgeRefusal(rc) -> BackendWriteResult.Bridge(failure(op, rc))
            else -> BackendWriteResult.Refused(MoqResultCode(rc))
        }

    private inline fun <R : Any> recordResultOf(op: String, rc: Int, deferrals: Long, fatal: Long, code: Long, decode: () -> R): BackendRecordResult<R> =
        when (rc) {
            MOQ_OK -> BackendRecordResult.Record(decode(), deferrals.toULong())
            MOQ_DONE -> BackendRecordResult.Empty(deferrals.toULong())
            MOQ_ERR_CLOSED -> BackendRecordResult.Closed(fatal = fatal != 0L, code = code.toULong())
            else -> BackendRecordResult.Bridge(failure(op, rc))
        }

    private fun commandResultOf(op: String, rc: Int): BackendCommandResult =
        when {
            rc == MOQ_OK -> BackendCommandResult.Ok
            isBridgeRefusal(rc) -> BackendCommandResult.Bridge(failure(op, rc))
            // The endpoint's own answer, carried exactly; the engine decides what it means.
            else -> BackendCommandResult.Refused(MoqResultCode(rc))
        }

    private sealed interface Described {
        data class Ok(val description: TrackDescription) : Described

        data class Bridge(val failure: BridgeFailure) : Described
    }

    /** The description snapshot for [track] under [receiver]: the row's refusal is the bridge's, whatever its category. */
    private fun describe(receiver: Long, track: Long): Described {
        val sc = LongArray(TrackDescribeScalars.COUNT)
        val bytes =
            NativeBridge.nativeTrackDescribe(this.token, receiver, track, sc)
                ?: return Described.Bridge(failure("track-describe", sc[TrackDescribeScalars.RESULT].toInt()))
        if (bytes.size != TrackDescriptionCodec.expectedByteFields(sc)) {
            return Described.Bridge(failure("track-describe-shape", bytes.size))
        }
        return Described.Ok(TrackDescriptionCodec.decode(bytes, sc))
    }

    override fun destroy(): BackendOpResult {
        // Retirement eligibility is the REGISTRY's, not this class's: a root
        // that was never stopped is refused by the `STATE` category and stays
        // valid and uncleaned. This method never stops on the caller's behalf.
        val rc = NativeBridge.nativeEndpointDestroy(token)
        return if (rc == MOQ_OK) {
            BackendOpResult.Ok
        } else {
            BackendOpResult.Bridge(failure("destroy", rc))
        }
    }

    internal companion object {
        const val MOQ_OK: Int = 0
        const val MOQ_DONE: Int = 1
        const val MOQ_ERR_CLOSED: Int = -4
        const val MOQ_ERR_WOULD_BLOCK: Int = -8
        const val MOQ_ERR_INTERRUPTED: Int = -13
        const val MOQ_ERR_UNSUPPORTED: Int = -14

        /**
         * `MOQ5_ERR_REGISTRY_BASE`. Every refusal at or below it belongs to the
         * bridge's private space -- the registry categories, and the host-only
         * lifecycle space beneath them -- and none of them is an endpoint
         * result.
         */
        const val REGISTRY_BASE: Int = -1000

        /** state, negotiated version, terminal reason, terminal detail. */
        const val SNAPSHOT_FIELDS: Int = 4

        fun isBridgeRefusal(rc: Int): Boolean = rc < REGISTRY_BASE

        /**
         * Total over the drain row's answers: each documented C result to its
         * value; anything else -- a registry refusal, or MOQ_ERR_WRONG_STATE,
         * which no Kotlin thread can earn -- is a bridge failure carrying the
         * code verbatim.
         */
        fun drainResultOf(rc: Int): BackendDrainResult =
            when (rc) {
                MOQ_OK -> BackendDrainResult.Flushed
                MOQ_DONE -> BackendDrainResult.TimedOut
                MOQ_ERR_INTERRUPTED -> BackendDrainResult.Interrupted
                MOQ_ERR_CLOSED -> BackendDrainResult.Closed
                MOQ_ERR_UNSUPPORTED -> BackendDrainResult.Unsupported
                else -> BackendDrainResult.Bridge(failure("drain", rc))
            }

        private fun failure(op: String, code: Int) = BridgeFailure(op, code)

        /**
         * The ONE readiness gate, passed before the first endpoint native call
         * of either creation row.
         *
         * [decide] is the process-wide owner's decision in production -- the
         * packaged-name load and the ABI/feature handshake, made once and
         * sticky. A ready bridge proceeds; anything else becomes the public
         * [NativeBridgeInitializationException], thrown here in the engine
         * factory, before any reservation, so nothing is published and
         * nothing is owed. Two creation rows share this one function so
         * there cannot be two translations that drift apart.
         *
         * Without it, a fresh process reaches an unbound native method and
         * the JVM reports `UnsatisfiedLinkError` from the worker thread --
         * the library then works only when something else happened to load
         * it first, which is not a product.
         */
        private fun requireBridge(decide: () -> BridgeInit) {
            val decision = decide()
            if (decision !is BridgeInit.Ready) {
                throw NativeBridgeInitializationException(decision.toInitializationFailure())
            }
        }

        /**
         * Create the endpoint root and take ownership of its token.
         *
         * MUST run on the engine worker -- see [factory]. A failure publishes
         * NOTHING: no token, no registry slot and no generation are consumed,
         * so there is nothing to tear down and the engine reports it as a
         * construction failure rather than as a quarantine.
         *
         * [decide] defaults to the process-wide owner and is a parameter only
         * so a host test can drive the readiness gate with a controlled
         * initializer; it never selects a library path.
         */
        fun create(
            url: String,
            decide: () -> BridgeInit = NativeBridgeOwner::initialize,
        ): JniEndpointBackend {
            requireBridge(decide)
            val out = LongArray(1)
            val rc = NativeBridge.nativeEndpointCreate(url, out)
            if (rc != MOQ_OK) throw EndpointCreateException(rc)
            return JniEndpointBackend(out[0])
        }

        /**
         * Create the endpoint root through the TRUSTED row -- the same
         * transaction as [create], with the library-owned platform delegate
         * selected by [ordinal] -- and take ownership of its token. This is the
         * only production route the public endpoint uses; the same failure
         * contract as [create] applies.
         */
        fun createTrusted(
            url: String,
            ordinal: Int,
            offer: NativeEndpointOffer,
            handshakeMicros: Long,
            protocol: Int,
            webTransportPath: String?,
            decide: () -> BridgeInit = NativeBridgeOwner::initialize,
        ): JniEndpointBackend {
            requireBridge(decide)
            val out = LongArray(1)
            val rc = NativeBridge.nativeEndpointCreateTrusted(
                url, ordinal, offer.policy, offer.versions.toIntArray(), handshakeMicros, protocol, webTransportPath, out,
            )
            if (rc != MOQ_OK) throw EndpointCreateException(rc)
            return JniEndpointBackend(out[0])
        }

        /**
         * The engine factory. Invoked on the worker, before that worker
         * performs any endpoint operation, so creation and every later
         * stateful call share one authority thread.
         */
        fun factory(url: String): () -> EndpointBackend = { create(url) }

        /** For rows that must drive a token the registry will refuse. */
        fun adopt(token: Long): JniEndpointBackend = JniEndpointBackend(token)
    }
}

/**
 * `moq5_nativeEndpointCreate` refused. The [code] is the native result verbatim
 * -- an endpoint `moq_result_t`, or a private registry refusal if the failure
 * was the bridge's own. It is NOT narrowed into either domain here, because the
 * caller that reports it needs to know which one it came from.
 */
internal class EndpointCreateException(
    val code: Int,
) : IllegalStateException("the endpoint root could not be created: native result $code")
