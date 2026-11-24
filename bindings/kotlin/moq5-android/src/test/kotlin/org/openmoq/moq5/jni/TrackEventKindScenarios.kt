package org.openmoq.moq5.jni

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.EndpointConfig
import org.openmoq.moq5.EventHistoryPolicy
import org.openmoq.moq5.FillEndReason
import org.openmoq.moq5.FillErrorDomain
import org.openmoq.moq5.FillLoss
import org.openmoq.moq5.FillLossCause
import org.openmoq.moq5.MediaReceiver
import org.openmoq.moq5.MoqEndpoint
import org.openmoq.moq5.MoqErrorCode
import org.openmoq.moq5.MoqResultCode
import org.openmoq.moq5.Namespace
import org.openmoq.moq5.NamespacePart
import org.openmoq.moq5.ObjectLocation
import org.openmoq.moq5.OverflowPolicy
import org.openmoq.moq5.ReceiverConfig
import org.openmoq.moq5.RequestId
import org.openmoq.moq5.TrackEvent
import org.openmoq.moq5.TrackPresence
import org.openmoq.moq5.TrackState
import org.openmoq.moq5.engine.BackendAttachResult
import org.openmoq.moq5.engine.BackendDeliveryOrigin
import org.openmoq.moq5.engine.BackendOpResult
import org.openmoq.moq5.engine.BackendStopResult
import org.openmoq.moq5.engine.BackendTrackEvent
import org.openmoq.moq5.engine.BackendTrackEventKind
import org.openmoq.moq5.engine.BackendTrackEventResult
import org.openmoq.moq5.engine.BackendTrackStateResult
import org.openmoq.moq5.engine.BridgeFailure
import org.openmoq.moq5.engine.EndpointBackend
import org.openmoq.moq5.engine.JniEndpointBackend

/**
 * Track-event kinds 8-12 and the FAILED state through the product's actual
 * conversion: the JNI poll's slots and coverage, then `JniEndpointBackend`'s
 * decoding. The fixture scripts ONE answer at the `poll_track` / `track_state`
 * boundary on a real track handle (their real producers need a live peer);
 * everything after that boundary is the shipped code. Run by [EndpointChildMain].
 */
internal object TrackEventKindScenarios {
    /** The child's failure convention: the reason on stdout, then the throw. */
    private fun check(cond: Boolean, what: String) {
        if (!cond) {
            println("RESULT failed: $what")
            throw IllegalStateException(what)
        }
    }

    private const val CATALOG = """{"version":"1","tracks":[{"name":"a","packaging":"loc","isLive":true,"role":"video","codec":"avc1.42e01e"},{"name":"b","packaging":"loc","isLive":true,"role":"audio","codec":"opus"}]}"""
    private val NS = Namespace(listOf(NamespacePart("live".toByteArray())))
    private const val NOMEM = -1
    private const val WRONG_STATE = -5
    private const val BUFFER = -9

    /** The ten scripted values in header order; every field defaults to zero. */
    private fun values(
        local: Long = 0, initiating: Long = 0, reason: Long = 0, domain: Long = 0, code: Long = 0,
        origin: Long = 0, delivery: Long = 0, causes: Long = 0, group: Long = 0, objectId: Long = 0,
    ) = longArrayOf(local, initiating, reason, domain, code, origin, delivery, causes, group, objectId)

    private class Rig {
        val root: Long
        val b: JniEndpointBackend
        val rx: Long
        val a: Long
        val layout: LongArray

        init {
            System.load(BridgeDsos.path("moq5_jni"))
            TrustRouteProbe.arm(emptyList())
            layout = TrustRouteProbe.nativeTrackEventLayout()
            val t = longArrayOf(0L)
            check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK, "setup: a live root")
            root = t[0]
            b = JniEndpointBackend.adopt(root)
            val attached = b.attachReceiver(ReceiverConfig(NS, OverflowPolicy.DropToKeyframe, autoSubscribe = false))
            check(attached is BackendAttachResult.Attached, "setup: attach")
            rx = (attached as BackendAttachResult.Attached).token
            check(TrustRouteProbe.nativeIngestCatalog(root, rx, CATALOG.toByteArray()) == EndpointAbi.OK, "setup: ingest")
            val added = b.pollTrack(rx)
            check(added is BackendTrackEventResult.Event && added.event.kind == BackendTrackEventKind.Added, "setup: the real ADDED(a); got $added")
            a = (added as BackendTrackEventResult.Event).event.track
        }

        fun scripted(kind: Int, v: LongArray, stamp: Long = -1): BackendTrackEventResult {
            TrustRouteProbe.nativeArmTrackEvent(kind, stamp, v)
            return b.pollTrack(rx)
        }

        fun event(kind: BackendTrackEventKind) = BackendTrackEvent(kind, a, null)

        fun at(field: Int): Long = layout[field]

        fun finish(where: String, scripted: Long) {
            check(TrustRouteProbe.nativeScriptedTrackEvents() == scripted, "$where: $scripted scripted answers delivered; got ${TrustRouteProbe.nativeScriptedTrackEvents()}")
            check(b.closeReceiver(rx) == BackendOpResult.Ok && b.stop() == BackendStopResult.Stopped && b.destroy() == BackendOpResult.Ok, "$where: cleanup")
            val f = TrustRouteProbe.fault()
            check(f == null, "$where: the recorder latched an instrumentation fault: $f")
        }
    }

    private fun expect(got: BackendTrackEventResult, want: BackendTrackEvent, what: String) =
        check(got == BackendTrackEventResult.Event(want), "$what: want $want; got $got")

    /** Every new kind at a whole stamp, with exact payloads; out-of-set values stay the bridge's refusal. */
    fun kinds() {
        val r = Rig()
        expect(r.scripted(8, values(local = NOMEM.toLong())), r.event(BackendTrackEventKind.LocalFailure).copy(localResult = MoqResultCode(NOMEM)), "LOCAL_FAILURE")
        expect(r.scripted(11, values(local = WRONG_STATE.toLong())), r.event(BackendTrackEventKind.UpdateLocalFailure).copy(localResult = MoqResultCode(WRONG_STATE)), "UPDATE_LOCAL_FAILURE")
        expect(
            r.scripted(10, values(local = BUFFER.toLong(), origin = 1)),
            r.event(BackendTrackEventKind.DeliveryLocalFailure).copy(localResult = MoqResultCode(BUFFER), deliveryOrigin = BackendDeliveryOrigin.Live),
            "DELIVERY_LOCAL_FAILURE, LIVE: no child ID",
        )
        expect(
            r.scripted(10, values(local = BUFFER.toLong(), origin = 2, delivery = 0)),
            r.event(BackendTrackEventKind.DeliveryLocalFailure).copy(
                localResult = MoqResultCode(BUFFER), deliveryOrigin = BackendDeliveryOrigin.Fill, deliveryInitiatingRequestId = RequestId(0uL),
            ),
            "DELIVERY_LOCAL_FAILURE, FILL: a zero child ID is a present ID",
        )
        expect(
            r.scripted(9, values(initiating = 0, reason = 4, domain = 1, code = 0)),
            r.event(BackendTrackEventKind.FillTerminal).copy(
                initiatingRequestId = RequestId(0uL), fillReason = FillEndReason.PeerCancelledOrReset,
                fillErrorDomain = FillErrorDomain.Stream, fillErrorCode = MoqErrorCode(0uL), fillLoss = FillLoss.None,
            ),
            "FILL_TERMINAL: zero ID, a STREAM code of zero kept as a code",
        )
        expect(
            r.scripted(9, values(initiating = -1, reason = 1, domain = 0, code = 0, causes = 0x5, group = -2, objectId = 3)),
            r.event(BackendTrackEventKind.FillTerminal).copy(
                initiatingRequestId = RequestId(ULong.MAX_VALUE), fillReason = FillEndReason.Complete,
                fillErrorDomain = FillErrorDomain.None, fillErrorCode = null,
                fillLoss = FillLoss.Lost(setOf(FillLossCause.LocalDrop, FillLossCause.TimedOutRange), ObjectLocation(ULong.MAX_VALUE - 1uL, 3uL)),
            ),
            "FILL_TERMINAL: full-width ID and location, no code in domain NONE, the loss summary",
        )
        expect(r.scripted(12, values(initiating = 42)), r.event(BackendTrackEventKind.FillCreated).copy(initiatingRequestId = RequestId(42uL)), "FILL_CREATED")
        // A closed C set: values outside it are the bridge's contract broken.
        check(r.scripted(13, values()) == BackendTrackEventResult.Bridge(BridgeFailure("receiver-poll-track-kind", 13)), "an unknown kind is still refused")
        check(r.scripted(9, values(reason = 9)) == BackendTrackEventResult.Bridge(BridgeFailure("receiver-poll-track-fill-reason", 9)), "an unknown fill reason")
        check(r.scripted(9, values(reason = 1, domain = 4)) == BackendTrackEventResult.Bridge(BridgeFailure("receiver-poll-track-fill-domain", 4)), "an unknown fill domain")
        check(r.scripted(9, values(reason = 1, causes = 0x8)) == BackendTrackEventResult.Bridge(BridgeFailure("receiver-poll-track-fill-loss", 8)), "an unknown loss cause")
        check(r.scripted(10, values(origin = 3)) == BackendTrackEventResult.Bridge(BridgeFailure("receiver-poll-track-delivery-origin", 3)), "an unknown delivery origin")
        // The scripted answers consumed nothing real: discovery continues in order.
        val next = r.b.pollTrack(r.rx)
        check(next is BackendTrackEventResult.Event && next.event.kind == BackendTrackEventKind.Added && next.event.track != r.a, "the real ADDED(b) is next; got $next")
        val ready = r.b.pollTrack(r.rx)
        check(ready is BackendTrackEventResult.Event && ready.event.kind == BackendTrackEventKind.CatalogReady, "then CATALOG_READY; got $ready")
        r.finish("receiverlocalkinds", 12)
        println("RESULT ok")
    }

    /** A field is published only when the library's stamp wholly covers it; absent is never zero. */
    fun stamps() {
        val r = Rig()
        val raw = LongArray(ReceiverEventScalars.COUNT)
        fun rawPoll(kind: Int, v: LongArray, stamp: Long): LongArray {
            TrustRouteProbe.nativeArmTrackEvent(kind, stamp, v)
            raw.fill(EndpointAbi.SENTINEL)
            check(NativeBridge.nativeReceiverPollTrack(r.root, r.rx, raw) == EndpointAbi.OK, "raw poll of kind $kind")
            return raw
        }
        val local = TrustRouteProbe.EventLayout.LOCAL_RESULT
        // local_result split by the stamp: absent; then present.
        rawPoll(8, values(local = NOMEM.toLong()), r.at(local) + 2).let {
            check(it[ReceiverEventScalars.HAS] == 0L && it[ReceiverEventScalars.LOCAL_RESULT] == 0L, "a stamp inside local_result: no HAS bit, slot zeroed; got ${it.toList()}")
        }
        rawPoll(8, values(local = NOMEM.toLong()), r.at(local) + 4).let {
            check(it[ReceiverEventScalars.HAS] == ReceiverEventScalars.HAS_LOCAL_RESULT && it[ReceiverEventScalars.LOCAL_RESULT] == NOMEM.toLong(), "covered: signed result; got ${it.toList()}")
        }
        expect(r.scripted(8, values(local = NOMEM.toLong()), r.at(local) + 2), r.event(BackendTrackEventKind.LocalFailure), "LOCAL_FAILURE without its result")
        // FILL_TERMINAL stamped short of each field in turn.
        val v = values(initiating = 0, reason = 4, domain = 1, code = 0, causes = 0x2, group = 9, objectId = 1)
        expect(r.scripted(9, v, r.at(TrustRouteProbe.EventLayout.INITIATING_ID)), r.event(BackendTrackEventKind.FillTerminal), "nothing covered: every field unobserved")
        expect(
            r.scripted(9, v, r.at(TrustRouteProbe.EventLayout.FILL_REASON) + 4),
            r.event(BackendTrackEventKind.FillTerminal).copy(initiatingRequestId = RequestId(0uL), fillReason = FillEndReason.PeerCancelledOrReset),
            "ID and reason covered; domain and code not",
        )
        expect(
            r.scripted(9, v, r.at(TrustRouteProbe.EventLayout.FILL_LOSS_OBJECT) + 4),
            r.event(BackendTrackEventKind.FillTerminal).copy(
                initiatingRequestId = RequestId(0uL), fillReason = FillEndReason.PeerCancelledOrReset,
                fillErrorDomain = FillErrorDomain.Stream, fillErrorCode = MoqErrorCode(0uL),
            ),
            "the loss block is all or nothing: a stamp inside its last field leaves it unobserved",
        )
        expect(
            r.scripted(9, v, r.at(TrustRouteProbe.EventLayout.FILL_LOSS_OBJECT) + 8),
            r.event(BackendTrackEventKind.FillTerminal).copy(
                initiatingRequestId = RequestId(0uL), fillReason = FillEndReason.PeerCancelledOrReset,
                fillErrorDomain = FillErrorDomain.Stream, fillErrorCode = MoqErrorCode(0uL),
                fillLoss = FillLoss.Lost(setOf(FillLossCause.UnknownRange), ObjectLocation(9uL, 1uL)),
            ),
            "the whole loss block covered",
        )
        // DELIVERY_LOCAL_FAILURE: the origin without its child ID, then without the origin.
        val d = values(local = BUFFER.toLong(), origin = 2, delivery = 7)
        expect(
            r.scripted(10, d, r.at(TrustRouteProbe.EventLayout.DELIVERY_ID)),
            r.event(BackendTrackEventKind.DeliveryLocalFailure).copy(localResult = MoqResultCode(BUFFER), deliveryOrigin = BackendDeliveryOrigin.Fill),
            "FILL origin covered, its child ID not",
        )
        expect(
            r.scripted(10, d, r.at(TrustRouteProbe.EventLayout.DELIVERY_ORIGIN)),
            r.event(BackendTrackEventKind.DeliveryLocalFailure).copy(localResult = MoqResultCode(BUFFER)),
            "origin unobserved",
        )
        r.finish("receiverkindstamps", 9)
        println("RESULT ok")
    }

    /**
     * The replacement links are one block: published only when the stamp
     * covers `replaced_by`, as the borrowed tokens of real handles, and
     * decoded on the kinds that carry them (the successor on a REMOVED, the
     * predecessor on an ADDED); an unobserved block is null, never a zero
     * token.
     */
    fun replacementStamps() {
        val r = Rig()
        val second = r.b.pollTrack(r.rx)
        check(second is BackendTrackEventResult.Event && second.event.kind == BackendTrackEventKind.Added && second.event.track != r.a, "setup: the real ADDED(b); got $second")
        val tb = (second as BackendTrackEventResult.Event).event.track
        fun links(predecessor: Long, successor: Long) = values() + longArrayOf(predecessor, successor)
        val raw = LongArray(ReceiverEventScalars.COUNT)
        fun rawPoll(kind: Int, v: LongArray, stamp: Long): LongArray {
            TrustRouteProbe.nativeArmTrackEvent(kind, stamp, v)
            raw.fill(EndpointAbi.SENTINEL)
            check(NativeBridge.nativeReceiverPollTrack(r.root, r.rx, raw) == EndpointAbi.OK, "raw poll of kind $kind")
            return raw
        }
        val replacedBy = r.at(TrustRouteProbe.EventLayout.REPLACED_BY)
        check(r.at(TrustRouteProbe.EventLayout.REPLACES) < replacedBy && replacedBy + 8 <= r.at(TrustRouteProbe.EventLayout.SIZE), "setup: the block is the event's tail")
        rawPoll(3, links(0, 1), -1).let {
            check(it[ReceiverEventScalars.HAS] and ReceiverEventScalars.HAS_REPLACEMENT != 0L && it[ReceiverEventScalars.TRACK] == tb, "a whole stamp publishes the block; got ${it.toList()}")
            check(it[ReceiverEventScalars.REPLACED_BY] == r.a && it[ReceiverEventScalars.REPLACES] == 0L, "the successor's token, and a NULL predecessor as zero")
        }
        rawPoll(3, links(0, 1), replacedBy + 7).let {
            check(it[ReceiverEventScalars.HAS] and ReceiverEventScalars.HAS_REPLACEMENT == 0L, "a stamp inside replaced_by: no HAS bit; got ${it.toList()}")
            check(it[ReceiverEventScalars.REPLACED_BY] == 0L && it[ReceiverEventScalars.REPLACES] == 0L, "both slots zeroed")
        }
        rawPoll(1, links(1, 0), replacedBy).let {
            check(it[ReceiverEventScalars.HAS] and ReceiverEventScalars.HAS_REPLACEMENT == 0L && it[ReceiverEventScalars.REPLACES] == 0L, "replaces covered, replaced_by not: the block is all or nothing; got ${it.toList()}")
        }
        expect(r.scripted(3, links(0, 1)), BackendTrackEvent(BackendTrackEventKind.Removed, tb, null, replacedBy = r.a), "REMOVED names its successor")
        expect(r.scripted(3, links(0, 1), replacedBy + 7), BackendTrackEvent(BackendTrackEventKind.Removed, tb, null), "an unobserved successor is null")
        expect(r.scripted(3, links(1, 0)), BackendTrackEvent(BackendTrackEventKind.Removed, tb, null), "a REMOVED carries no predecessor")
        val added = r.scripted(1, links(1, 0))
        check(added is BackendTrackEventResult.Event && added.event.kind == BackendTrackEventKind.Added && added.event.replaces == r.a && added.event.replacedBy == null, "ADDED names its predecessor; got $added")
        val plain = r.scripted(1, links(0, 0))
        check(plain is BackendTrackEventResult.Event && plain.event.replaces == null, "a NULL predecessor is null, not token zero; got $plain")
        // The scripted answers consumed nothing real: discovery continues.
        check(r.b.pollTrack(r.rx).let { it is BackendTrackEventResult.Event && it.event.kind == BackendTrackEventKind.CatalogReady }, "then the real CATALOG_READY")
        r.finish("receiverreplacementstamps", 8)
        println("RESULT ok")
    }

    /** The FAILED state crosses as 6 and reads as [TrackState.Failed]; the next read is the real one again. */
    fun failedState() {
        val r = Rig()
        TrustRouteProbe.nativeArmTrackState(6)
        val failed = r.b.trackState(r.rx, r.a)
        check(failed == BackendTrackStateResult.State(6) && TrackState.ofNative(6) == TrackState.Failed, "FAILED crosses as 6; got $failed")
        check(r.b.trackState(r.rx, r.a) == BackendTrackStateResult.State(0), "unarmed: the real DISCOVERED")
        r.finish("receiverfailedstate", 0)
        println("RESULT ok")
    }

    /**
     * The device's route, on the host: a d22 receiver whose fill child ends
     * between two of the parent's objects. Through the PUBLIC API over the
     * real JNI backend, the fill terminal is an event with its payload, the
     * parent stays present, the endpoint is not quarantined and the parent's
     * objects on either side of it are both delivered.
     */
    fun publicFillTerminalD22() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        var fixture: FillFixture? = null
        val failures = mutableListOf<String>()
        runBlocking {
            val endpoint =
                MoqEndpoint.open(EndpointConfig(EndpointAbi.UNSERVABLE)) {
                    val t = longArrayOf(0L)
                    check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK) { "fixture: the root, on the worker" }
                    FillFixture(JniEndpointBackend.adopt(t[0]), t[0]).also { fixture = it }
                }
            var receiver: MediaReceiver? = null
            val body =
                runCatching {
                    val rx = endpoint.attachReceiver(ReceiverConfig(NS, OverflowPolicy.DropToKeyframe, autoSubscribe = false, eventHistory = EventHistoryPolicy.Exact(16)))
                    receiver = rx
                    val first = withTimeout(30_000L) { rx.objects.next() }
                    val second = withTimeout(30_000L) { rx.objects.next() }
                    check(first != null && second != null, "both of the parent's objects are delivered: $first, $second")
                    check(first!!.location() == ObjectLocation(0uL, 0uL) && second!!.location() == ObjectLocation(1uL, 0uL), "in order around the fill terminal")
                    val events = checkNotNull(rx.trackEvents) { "an exact history was declared" }
                    val seen = mutableListOf<TrackEvent>()
                    withTimeout(30_000L) { while (seen.size < 4) seen += checkNotNull(events.next()) }
                    val a = rx.inventory.value.tracks.first()
                    check(seen[0] == TrackEvent.Added(a), "Added(a) first; got ${seen[0]}")
                    check(
                        seen[1] == TrackEvent.FillTerminal(a, RequestId(0uL), FillEndReason.PeerCancelledOrReset, FillErrorDomain.Stream, MoqErrorCode(0uL), FillLoss.None),
                        "the fill terminal with its exact payload; got ${seen[1]}",
                    )
                    check(seen[2] is TrackEvent.Added && seen[3] == TrackEvent.CatalogReady, "discovery continues; got ${seen.drop(2)}")
                    check(a.presence == TrackPresence.Present, "a fill child's end leaves its parent present; got ${a.presence}")
                    check(rx.trackState(a) != TrackState.Failed, "and the parent is not failed")
                }
            body.exceptionOrNull()?.let { failures += "body: $it" }
            runCatching { receiver?.close() }.exceptionOrNull()?.let { failures += "receiver close: $it" }
            runCatching { endpoint.closeAndJoin() }.exceptionOrNull()?.let { failures += "endpoint close: $it" }
        }
        val f = fixture
        if (f == null || f.injections != 2) failures += "the fixture injected ${f?.injections} of 2 objects"
        if (TrustRouteProbe.nativeScriptedTrackEvents() != 1L) failures += "scripted answers: ${TrustRouteProbe.nativeScriptedTrackEvents()} of 1"
        TrustRouteProbe.fault()?.let { failures += "recorder fault: $it" }
        failures.forEach { println("PROBLEM|publicfilltermd22|$it") }
        check(failures.isEmpty(), "publicfilltermd22: ${failures.size} problem(s)")
        println("RESULT ok")
    }

    private fun org.openmoq.moq5.MediaObject.location(): ObjectLocation? = identity?.let { ObjectLocation(it.group, it.objectId) }

    /**
     * On the engine's worker: latches draft 22 and the catalog at attach; on the
     * real ADDED(a), wants the track, injects its first object and scripts the
     * fill terminal the next poll answers; once that terminal has been polled,
     * injects the second object.
     */
    private class FillFixture(private val inner: JniEndpointBackend, private val root: Long) : EndpointBackend by inner {
        var injections = 0
            private set
        private var track = 0L

        override fun attachReceiver(config: ReceiverConfig): BackendAttachResult {
            val r = inner.attachReceiver(config)
            if (r is BackendAttachResult.Attached) {
                check(TrustRouteProbe.nativeSetTransportVersion(root, r.token, 22) == EndpointAbi.OK) { "fixture: draft 22" }
                check(TrustRouteProbe.nativeIngestCatalog(root, r.token, CATALOG.toByteArray()) == EndpointAbi.OK) { "fixture: the catalog" }
            }
            return r
        }

        override fun pollTrack(token: Long): BackendTrackEventResult {
            val ev = inner.pollTrack(token)
            if (ev is BackendTrackEventResult.Event && ev.event.kind == BackendTrackEventKind.Added && track == 0L) {
                track = ev.event.track
                check(TrustRouteProbe.nativeMarkTrackDesired(root, token, track) == EndpointAbi.OK) { "fixture: want a" }
                inject(token, 0L)
                TrustRouteProbe.nativeArmTrackEvent(9, -1, values(initiating = 0, reason = 4, domain = 1, code = 0))
            } else if (ev is BackendTrackEventResult.Event && ev.event.kind == BackendTrackEventKind.FillTerminal) {
                inject(token, 1L)
            }
            return ev
        }

        private fun inject(token: Long, group: Long) {
            check(TrustRouteProbe.nativeIngestObjectIdentity(root, token, track, group, 0L, 0L, 128, false, "g$group".toByteArray()) == EndpointAbi.OK) {
                "fixture: inject the parent's object in group $group"
            }
            injections++
        }
    }
}
