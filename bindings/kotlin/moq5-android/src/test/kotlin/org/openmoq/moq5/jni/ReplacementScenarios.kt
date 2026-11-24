package org.openmoq.moq5.jni

import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.CatalogRevision
import org.openmoq.moq5.EndpointConfig
import org.openmoq.moq5.EventHistoryPolicy
import org.openmoq.moq5.InitSource
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaReceiver
import org.openmoq.moq5.MoqEndpoint
import org.openmoq.moq5.Namespace
import org.openmoq.moq5.NamespacePart
import org.openmoq.moq5.OverflowPolicy
import org.openmoq.moq5.ReceiverConfig
import org.openmoq.moq5.TrackEncryption
import org.openmoq.moq5.TrackEvent
import org.openmoq.moq5.TrackPresence
import org.openmoq.moq5.TrackState
import org.openmoq.moq5.TrackUnavailableException
import org.openmoq.moq5.engine.BackendAttachResult
import org.openmoq.moq5.engine.BackendOpResult
import org.openmoq.moq5.engine.BackendStopResult
import org.openmoq.moq5.engine.BackendTrackEvent
import org.openmoq.moq5.engine.BackendTrackEventKind
import org.openmoq.moq5.engine.BackendTrackEventResult
import org.openmoq.moq5.engine.BridgeFailure
import org.openmoq.moq5.engine.EndpointBackend
import org.openmoq.moq5.engine.JniEndpointBackend
import org.openmoq.moq5.engine.ReceiverStatsCodec
import org.openmoq.moq5.engine.TrackDescriptionCodec

/**
 * MSF-02 adoption through the real one-DSO bridge: real catalogs fed through
 * the service's own intake seam, the REAL `moq_media_receiver_poll_track`,
 * `_track_desc_copy`, `_get_stats` and track commands, then the product's own
 * slots, coverage and decoding. A critical change replaces a track (REMOVED
 * naming its successor, then the successor's ADDED naming it), an advisory
 * one publishes a description generation, and the description carries its
 * init source and its encryption fields. Run by [EndpointChildMain].
 */
internal object ReplacementScenarios {
    /** The child's failure convention: the reason on stdout, then the throw. */
    private fun check(cond: Boolean, what: String) {
        if (!cond) {
            println("RESULT failed: $what")
            throw IllegalStateException(what)
        }
    }

    private const val DRAFT_16 = 16
    private val NS = arrayOf("live".toByteArray(), "cam1".toByteArray())
    private val NAMESPACE = Namespace(NS.map { NamespacePart(it) })

    private fun video(name: String, codec: String, bitrate: Int, extra: String = "") =
        """{"name":"$name","packaging":"loc","isLive":true,"role":"video","codec":"$codec","bitrate":$bitrate,"framerate":30$extra}"""

    /** Two MSF-02 video tracks, then a critical change of the first (its codec) and an advisory change of the second (its bitrate). */
    private val BASE = """{"version":"draft-02","generatedAt":1,"tracks":[${video("video", "av01", 1000)},${video("other", "av01", 500)}]}"""
    private const val CRITICAL = """{"generatedAt":2,"deltaUpdate":[{"op":"update","tracks":[{"parentName":"video","codec":"avc1.64001f"}]}]}"""
    private const val ADVISORY = """{"generatedAt":3,"deltaUpdate":[{"op":"update","tracks":[{"parentName":"other","bitrate":2000}]}]}"""

    /** One track per init source the catalog can name, and one with all four encryption fields. */
    private val DESCRIBED =
        """{"version":"draft-02","generatedAt":1,"initDataList":[{"id":"vc","type":"track-property","data":"0x0D"}],"tracks":[""" +
            video("v", "av01", 1000, ""","initRef":"vc"""") + "," +
            video("i", "av01", 1000, ""","initData":"AQID"""") + "," +
            video("e", "av01", 1000, ""","encryptionScheme":"moq-secure-objects","cipherSuite":"aes-128-gcm-sha256","keyId":"a2V5LTE=","trackBaseKey":"c2VjcmV0LWtleQ=="""") +
            "]}"

    private fun liveRoot(): Long {
        val t = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK, "setup: construct a live root")
        return t[0]
    }

    /** A discovery-only receiver on the draft-16 transport precondition, before any catalog. */
    private fun attach(root: Long): Long {
        val out = longArrayOf(EndpointAbi.SENTINEL)
        check(NativeBridge.nativeReceiverAttach(root, NS, 1, 0, 0L, 0, false, null, 0, out) == EndpointAbi.OK, "setup: attach")
        check(TrustRouteProbe.nativeSetTransportVersion(root, out[0], DRAFT_16) == EndpointAbi.OK, "setup: the transport-draft precondition")
        return out[0]
    }

    private fun ingest(root: Long, rx: Long, obj: Long, json: String) =
        check(TrustRouteProbe.nativeIngestCatalogAt(root, rx, 0L, obj, json.toByteArray()) == EndpointAbi.OK, "setup: ingest catalog object $obj")

    private fun poll(root: Long, rx: Long, sc: LongArray): Int {
        sc.fill(EndpointAbi.SENTINEL)
        return NativeBridge.nativeReceiverPollTrack(root, rx, sc)
    }

    private fun describe(root: Long, rx: Long, track: Long): Pair<Array<ByteArray>, LongArray> {
        val ds = LongArray(TrackDescribeScalars.COUNT) { EndpointAbi.SENTINEL }
        val bytes = NativeBridge.nativeTrackDescribe(root, rx, track, ds)
        check(bytes != null && ds[TrackDescribeScalars.RESULT] == 0L, "describe answers; result ${ds[TrackDescribeScalars.RESULT]}")
        check(ds.none { it == EndpointAbi.SENTINEL }, "every describe slot written: ${ds.toList()}")
        check(bytes!!.size == TrackDescriptionCodec.expectedByteFields(ds), "the byte fields the scalars announce: ${bytes.size}")
        return bytes to ds
    }

    private fun trackState(root: Long, rx: Long, track: Long): Pair<Int, Long> {
        val out = longArrayOf(EndpointAbi.SENTINEL)
        return NativeBridge.nativeTrackState(root, rx, track, out) to out[0]
    }

    private fun cleanup(where: String, root: Long, rx: Long) {
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "$where: cleanup")
        val f = TrustRouteProbe.fault()
        check(f == null, "$where: the recorder latched an instrumentation fault: $f")
    }

    /** Polls the base catalog's discovery: ADDED(video), ADDED(other), CATALOG_READY; returns both tokens. */
    private fun discoverBase(root: Long, rx: Long, sc: LongArray): Pair<Long, Long> {
        ingest(root, rx, 0L, BASE)
        check(poll(root, rx, sc) == EndpointAbi.OK && sc[ReceiverEventScalars.KIND] == 1L, "ADDED(video)")
        val ta = sc[ReceiverEventScalars.TRACK]
        check(sc[ReceiverEventScalars.CONFIG_GENERATION] == 0L, "discovered at generation 0")
        check(sc[ReceiverEventScalars.HAS] and ReceiverEventScalars.HAS_REPLACEMENT != 0L, "the library's stamp covers the replacement block on every event")
        check(sc[ReceiverEventScalars.REPLACES] == 0L && sc[ReceiverEventScalars.REPLACED_BY] == 0L, "an ordinary ADDED names no other track")
        check(poll(root, rx, sc) == EndpointAbi.OK && sc[ReceiverEventScalars.KIND] == 1L, "ADDED(other)")
        val to = sc[ReceiverEventScalars.TRACK]
        check(poll(root, rx, sc) == EndpointAbi.OK && sc[ReceiverEventScalars.KIND] == 5L, "CATALOG_READY")
        check(poll(root, rx, sc) == EndpointAbi.DONE, "then empty")
        return ta to to
    }

    /**
     * The raw rows: a critical change is REMOVED(a) naming b, then ADDED(b)
     * naming a, with b's token minted at the REMOVED; an advisory change is
     * UPDATED at generation 1 and the describe row reports the generation; a
     * command on a reaches b natively, and a's state is the C's refusal.
     */
    fun replacement() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attach(root)
        val sc = LongArray(ReceiverEventScalars.COUNT)
        val (ta, to) = discoverBase(root, rx, sc)
        ingest(root, rx, 1L, CRITICAL)
        check(poll(root, rx, sc) == EndpointAbi.OK && sc[ReceiverEventScalars.KIND] == 3L && sc[ReceiverEventScalars.TRACK] == ta, "TRACK_REMOVED(a) first; got ${sc.toList()}")
        check(sc[ReceiverEventScalars.HAS] and ReceiverEventScalars.HAS_REPLACEMENT != 0L, "HAS_REPLACEMENT is set")
        val tb = sc[ReceiverEventScalars.REPLACED_BY]
        check(tb != 0L && tb != ta && tb != to && EndpointAbi.kindOf(tb) == 3L && tb < 0L, "REPLACED_BY is a fresh borrowed token (kind 3, full width), minted before b's ADDED; got $tb")
        check(sc[ReceiverEventScalars.REPLACES] == 0L, "the REMOVED names no predecessor")
        check(poll(root, rx, sc) == EndpointAbi.OK && sc[ReceiverEventScalars.KIND] == 1L, "TRACK_ADDED(b) immediately after; got ${sc.toList()}")
        check(sc[ReceiverEventScalars.TRACK] == tb, "the ADDED's TRACK is the token the REMOVED named")
        check(sc[ReceiverEventScalars.REPLACES] == ta, "the ADDED's REPLACES is the replaced track's token")
        check(sc[ReceiverEventScalars.REPLACED_BY] == 0L && sc[ReceiverEventScalars.CONFIG_GENERATION] == 0L, "a successor starts at generation 0 and is not replaced")
        val (bytesB, dsB) = describe(root, rx, tb)
        check(String(bytesB[TrackDescribeBytes.CODEC]) == "avc1.64001f" && dsB[TrackDescribeScalars.DESC_GENERATION] == 0L, "b's description is the new configuration, generation 0")
        check(dsB[TrackDescribeScalars.HAS] and TrackDescribeScalars.HAS_GENERATION != 0L, "the copy's stamp covers the generation block")
        val (bytesA, _) = describe(root, rx, ta)
        check(String(bytesA[TrackDescribeBytes.CODEC]) == "av01", "a's description is untouched")
        check(poll(root, rx, sc) == EndpointAbi.DONE, "exactly two events for the replacement")
        ingest(root, rx, 2L, ADVISORY)
        check(poll(root, rx, sc) == EndpointAbi.OK && sc[ReceiverEventScalars.KIND] == 2L && sc[ReceiverEventScalars.TRACK] == to, "TRACK_UPDATED(other); got ${sc.toList()}")
        check(sc[ReceiverEventScalars.CONFIG_GENERATION] == 1L, "CONFIG_GENERATION is 1 on the first advisory UPDATED")
        check(sc[ReceiverEventScalars.REPLACES] == 0L && sc[ReceiverEventScalars.REPLACED_BY] == 0L, "an advisory change replaces nothing")
        val (bytesO, dsO) = describe(root, rx, to)
        check(dsO[TrackDescribeScalars.DESC_GENERATION] == 1L && dsO[TrackDescribeScalars.HAS_BITRATE] == 1L && dsO[TrackDescribeScalars.BITRATE] == 2000L && String(bytesO[TrackDescribeBytes.NAME]) == "other",
            "the current generation is 1, with the new bitrate; got ${dsO.toList()}")
        check(poll(root, rx, sc) == EndpointAbi.DONE, "then empty")
        // A command on the replaced track reaches its successor natively.
        check(NativeBridge.nativeTrackSubscribe(root, rx, ta, 0, false, 0) == EndpointAbi.OK, "subscribe on the replaced track is the successor's result")
        check(trackState(root, rx, tb) == (EndpointAbi.OK to 1L), "b is PENDING: the command reached it")
        check(trackState(root, rx, ta).first == EndpointAbi.WRONG_STATE, "a replaced track has no state of its own: WRONG_STATE")
        check(NativeBridge.nativeTrackUnsubscribe(root, rx, ta) == EndpointAbi.OK && trackState(root, rx, tb) == (EndpointAbi.OK to 0L), "unsubscribe on a reaches b too")
        cleanup("receiverreplacement", root, rx)
        println("RESULT ok")
    }

    /** A refused token mint for `replaced_by` after the REMOVED was consumed is the registry's code; the successor's ADDED still mints and names its predecessor. */
    fun replacementMintFault() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attach(root)
        val sc = LongArray(ReceiverEventScalars.COUNT)
        val (ta, _) = discoverBase(root, rx, sc)
        ingest(root, rx, 1L, CRITICAL)
        TrustRouteProbe.nativeArmCtorFault(TrustRouteProbe.CHILD_INSERT_REFUSE_ONCE)
        val rc = poll(root, rx, sc)
        check(rc == EndpointAbi.REG_CAPACITY, "the REMOVED's successor could not be minted: the registry's exact code; got $rc")
        check(sc.all { it == EndpointAbi.SENTINEL }, "nothing published: ${sc.toList()}")
        check(poll(root, rx, sc) == EndpointAbi.OK && sc[ReceiverEventScalars.KIND] == 1L && sc[ReceiverEventScalars.REPLACES] == ta, "the successor's ADDED follows, minted now, naming a; got ${sc.toList()}")
        check(EndpointAbi.kindOf(sc[ReceiverEventScalars.TRACK]) == 3L, "b's token")
        cleanup("receiverreplacementmintfault", root, rx)

        // Through the backend: the same refusal is a bridge failure, never a track event.
        val root2 = liveRoot()
        val b = JniEndpointBackend.adopt(root2)
        val attached = b.attachReceiver(ReceiverConfig(NAMESPACE, OverflowPolicy.DropToKeyframe, autoSubscribe = false))
        check(attached is BackendAttachResult.Attached, "attach")
        val rx2 = (attached as BackendAttachResult.Attached).token
        check(TrustRouteProbe.nativeSetTransportVersion(root2, rx2, DRAFT_16) == EndpointAbi.OK, "setup: draft")
        ingest(root2, rx2, 0L, BASE)
        repeat(3) { check(b.pollTrack(rx2) is BackendTrackEventResult.Event, "setup: discovery") }
        ingest(root2, rx2, 1L, CRITICAL)
        TrustRouteProbe.nativeArmCtorFault(TrustRouteProbe.CHILD_INSERT_REFUSE_ONCE)
        val refused = b.pollTrack(rx2)
        check(refused == BackendTrackEventResult.Bridge(BridgeFailure("receiver-poll-track", EndpointAbi.REG_CAPACITY)), "a refused replaced_by mint is a bridge failure; got $refused")
        check(b.closeReceiver(rx2) == BackendOpResult.Ok && b.stop() == BackendStopResult.Stopped && b.destroy() == BackendOpResult.Ok, "cleanup")
        check(TrustRouteProbe.fault() == null, "no recorder fault")
        println("RESULT ok")
    }

    /**
     * The backend's decoding of the replacement pair and of a generation:
     * the links as tokens on the kinds that carry them, the event's
     * generation, and the description's generation block.
     */
    fun backendReplacement() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val b = JniEndpointBackend.adopt(root)
        val attached = b.attachReceiver(ReceiverConfig(NAMESPACE, OverflowPolicy.DropToKeyframe, autoSubscribe = false))
        check(attached is BackendAttachResult.Attached, "attach")
        val rx = (attached as BackendAttachResult.Attached).token
        check(TrustRouteProbe.nativeSetTransportVersion(root, rx, DRAFT_16) == EndpointAbi.OK, "setup: draft")
        ingest(root, rx, 0L, BASE)
        fun event(): BackendTrackEvent {
            val r = b.pollTrack(rx)
            check(r is BackendTrackEventResult.Event, "an event; got $r")
            return (r as BackendTrackEventResult.Event).event
        }
        val addA = event()
        check(addA.kind == BackendTrackEventKind.Added && addA.replaces == null && addA.configGeneration == 0u, "ADDED(a) names nothing; got $addA")
        check(addA.description?.configGeneration == 0u && addA.description?.initSource == InitSource.None && addA.description?.encryption == null, "a's description: generation 0, no init, no encryption; got ${addA.description}")
        val addO = event()
        check(event().kind == BackendTrackEventKind.CatalogReady, "CATALOG_READY")
        ingest(root, rx, 1L, CRITICAL)
        val removed = event()
        check(removed.kind == BackendTrackEventKind.Removed && removed.track == addA.track && removed.replacedBy != null && removed.replaces == null, "REMOVED(a) names its successor; got $removed")
        val addB = event()
        check(addB.kind == BackendTrackEventKind.Added && addB.track == removed.replacedBy && addB.replaces == addA.track && addB.replacedBy == null, "ADDED(b) names a; got $addB")
        check(addB.description?.codec == MediaBytes("avc1.64001f".toByteArray()) && addB.description?.configGeneration == 0u, "b's new description at generation 0")
        ingest(root, rx, 2L, ADVISORY)
        val upd = event()
        check(upd.kind == BackendTrackEventKind.Updated && upd.track == addO.track && upd.configGeneration == 1u, "UPDATED(other) at generation 1; got $upd")
        check(upd.description?.configGeneration == 1u && upd.description?.bitrate == 2000uL, "its snapshot is generation 1; got ${upd.description}")
        check(b.pollTrack(rx) == BackendTrackEventResult.Empty, "then empty")
        check(b.closeReceiver(rx) == BackendOpResult.Ok && b.stop() == BackendStopResult.Stopped && b.destroy() == BackendOpResult.Ok, "cleanup")
        check(TrustRouteProbe.fault() == null, "no recorder fault")
        println("RESULT ok")
    }

    /**
     * The describe row's appended fields from a real MSF-02 catalog: the
     * generation, each init source the catalog can name with its property
     * type, and the four encryption fields verbatim; then a copy whose stamp
     * stops at the v0 size publishes none of them although its bytes are all
     * there.
     */
    fun describeMsf02() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attach(root)
        ingest(root, rx, 0L, DESCRIBED)
        val sc = LongArray(ReceiverEventScalars.COUNT)
        val tokens = ArrayList<Long>()
        while (poll(root, rx, sc) == EndpointAbi.OK) if (sc[ReceiverEventScalars.KIND] == 1L) tokens += sc[ReceiverEventScalars.TRACK]
        check(tokens.size == 3, "three tracks discovered; got ${tokens.size}")
        val (tv, ti, te) = tokens
        val gen = TrackDescribeScalars.HAS_GENERATION
        val enc = TrackDescribeScalars.HAS_ENCRYPTION_SCHEME or TrackDescribeScalars.HAS_CIPHER_SUITE or TrackDescribeScalars.HAS_KEY_ID or TrackDescribeScalars.HAS_TRACK_BASE_KEY
        val (bv, dv) = describe(root, rx, tv)
        check(dv[TrackDescribeScalars.HAS] == gen && dv[TrackDescribeScalars.DESC_GENERATION] == 0L, "v: the generation block, no encryption; got ${dv.toList()}")
        check(dv[TrackDescribeScalars.INIT_SOURCE] == 2L && dv[TrackDescribeScalars.INIT_PROPERTY_TYPE] == 0x0DL, "v: TRACK_PROPERTY in VIDEO_CONFIG (0x0D)")
        check((TrackDescribeBytes.ENCRYPTION_SCHEME..TrackDescribeBytes.TRACK_BASE_KEY).all { bv[it].isEmpty() }, "v: the encryption byte fields are empty")
        val (_, di) = describe(root, rx, ti)
        check(di[TrackDescribeScalars.HAS] == gen && di[TrackDescribeScalars.INIT_SOURCE] == 1L && di[TrackDescribeScalars.INIT_PROPERTY_TYPE] == 0L && di[TrackDescribeScalars.HAS_INIT] == 1L,
            "i: INLINE, no property type, the init data resolved; got ${di.toList()}")
        val (be, de) = describe(root, rx, te)
        check(de[TrackDescribeScalars.HAS] == gen or enc && de[TrackDescribeScalars.INIT_SOURCE] == 0L, "e: every encryption field carried, no init source; got ${de.toList()}")
        check(
            String(be[TrackDescribeBytes.ENCRYPTION_SCHEME]) == "moq-secure-objects" && String(be[TrackDescribeBytes.CIPHER_SUITE]) == "aes-128-gcm-sha256" &&
                String(be[TrackDescribeBytes.KEY_ID]) == "a2V5LTE=" && String(be[TrackDescribeBytes.TRACK_BASE_KEY]) == "c2VjcmV0LWtleQ==",
            "the four fields cross verbatim",
        )
        // The shipped decoder over the same carrier.
        val v = TrackDescriptionCodec.decode(bv, dv)
        check(v.initSource == InitSource.TrackProperty && v.initPropertyType == 0x0DuL && v.configGeneration == 0u && v.encryption == null, "decoded v: $v")
        val e = TrackDescriptionCodec.decode(be, de)
        check(
            e.encryption == TrackEncryption(MediaBytes("moq-secure-objects".toByteArray()), MediaBytes("aes-128-gcm-sha256".toByteArray()), MediaBytes("a2V5LTE=".toByteArray()), MediaBytes("c2VjcmV0LWtleQ==".toByteArray())),
            "decoded e's encryption: ${e.encryption}",
        )
        check(e.initSource == InitSource.None && e.initPropertyType == null, "decoded e's init source")
        check(!e.toString().contains(MediaBytes("c2VjcmV0LWtleQ==".toByteArray()).toString().substringAfter("hex:").substringBefore(")")), "the decoded description never renders the key")
        // A copy stamped at the v0 size: every appended byte is there, none is published.
        TrustRouteProbe.nativeArmCtorFault(TrustRouteProbe.DESC_COPY_RESTAMP_V0_ONCE)
        val (bs, ds) = describe(root, rx, te)
        check(ds[TrackDescribeScalars.HAS] == 0L, "nothing appended is published outside the stamp; got ${ds[TrackDescribeScalars.HAS]}")
        check(ds[TrackDescribeScalars.DESC_GENERATION] == 0L && ds[TrackDescribeScalars.INIT_SOURCE] == 0L && ds[TrackDescribeScalars.INIT_PROPERTY_TYPE] == 0L, "the generation slots stay zero")
        check((TrackDescribeBytes.ENCRYPTION_SCHEME..TrackDescribeBytes.TRACK_BASE_KEY).all { bs[it].isEmpty() }, "and the encryption fields empty")
        val short = TrackDescriptionCodec.decode(bs, ds)
        check(short.configGeneration == null && short.initSource == null && short.initPropertyType == null && short.encryption == null, "decoded: unreported, never zero; got $short")
        check(String(bs[TrackDescribeBytes.NAME]) == "e" && String(bs[TrackDescribeBytes.CODEC]) == "av01", "the v0 fields still cross")
        cleanup("receiverdescribemsf02", root, rx)
        println("RESULT ok")
    }

    /** The stats row's appended block from the real receiver: covered, with an MSF-02 catalog's revision and every counter zero. */
    fun statsRevision() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attach(root)
        fun stats(): LongArray {
            val sc = LongArray(ReceiverStatsScalars.COUNT) { EndpointAbi.SENTINEL }
            check(NativeBridge.nativeReceiverStats(root, rx, sc) == EndpointAbi.OK, "stats answers OK")
            check(sc.none { it == EndpointAbi.SENTINEL }, "every slot written exactly once: ${sc.toList()}")
            return sc
        }
        val before = stats()
        check(before[ReceiverStatsScalars.HAS_REVISION_BLOCK] == 1L, "the library's stamp covers the block")
        val sc = LongArray(ReceiverEventScalars.COUNT)
        discoverBase(root, rx, sc)
        val s = stats()
        check(s[ReceiverStatsScalars.HAS_REVISION_BLOCK] == 1L && s[ReceiverStatsScalars.CATALOG_REVISION] == 1L, "an MSF-02 catalog's revision is MOQ_MSF_REVISION_02 (1); got ${s.toList()}")
        check((ReceiverStatsScalars.OBJECTS_DECOMPRESSED..ReceiverStatsScalars.LOC_ORDER_FORCED).all { s[it] == 0L }, "no compression, header or ordering counts: ${s.toList()}")
        val decoded = ReceiverStatsCodec.decode(s)
        check(decoded.catalogRevision == CatalogRevision.Msf02 && decoded.compressionRefused == 0uL && decoded.locOrderForced == 0uL, "decoded: $decoded")
        cleanup("receiverstatsrevision", root, rx)
        println("RESULT ok")
    }

    /**
     * The device's route, on the host: the PUBLIC API over the real JNI
     * backend. The fixture feeds the base catalog at attach and the critical
     * change once discovery has been drained; the public history and the
     * links follow, and a command on the replaced track reaches its successor
     * natively.
     */
    fun publicReplacement() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        var fixture: Fixture? = null
        val failures = mutableListOf<String>()
        runBlocking {
            val endpoint =
                MoqEndpoint.open(EndpointConfig(EndpointAbi.UNSERVABLE)) {
                    val t = longArrayOf(0L)
                    check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK, "fixture: the root, on the worker")
                    Fixture(JniEndpointBackend.adopt(t[0]), t[0]).also { fixture = it }
                }
            var receiver: MediaReceiver? = null
            val body =
                runCatching {
                    val rx = endpoint.attachReceiver(ReceiverConfig(NAMESPACE, OverflowPolicy.DropToKeyframe, autoSubscribe = false, eventHistory = EventHistoryPolicy.Exact(16)))
                    receiver = rx
                    val inv = withTimeout(30_000L) { rx.inventory.first { it.tracks.size == 3 } }
                    val (a, other, b) = inv.tracks
                    val events = checkNotNull(rx.trackEvents) { "an exact history was declared" }
                    val seen = withTimeout(30_000L) { List(5) { checkNotNull(events.next()) } }
                    check(
                        seen == listOf(TrackEvent.Added(a), TrackEvent.Added(other), TrackEvent.CatalogReady, TrackEvent.Removed(a, replaced = true), TrackEvent.Added(b, replaces = a)),
                        "the public replacement pair; got $seen",
                    )
                    check(a.presence == TrackPresence.Removed && a.replacedBy === b && b.replaces === a, "linked both ways")
                    check(b.description.codec == MediaBytes("avc1.64001f".toByteArray()) && b.description.configGeneration == 0u, "b's description")
                    rx.subscribe(a)
                    check(rx.trackState(b) == TrackState.Pending, "subscribe on the replaced track reached b natively")
                    check(runCatching { rx.trackState(a) }.exceptionOrNull().let { it is TrackUnavailableException && it.presence == TrackPresence.Removed }, "a has no state of its own")
                    rx.unsubscribe(a)
                    check(rx.trackState(b) == TrackState.Discovered, "unsubscribe on a reached b too")
                }
            body.exceptionOrNull()?.let { failures += "body: $it" }
            runCatching { receiver?.close() }.exceptionOrNull()?.let { failures += "receiver close: $it" }
            runCatching { endpoint.closeAndJoin() }.exceptionOrNull()?.let { failures += "endpoint close: $it" }
        }
        val f = fixture
        if (f == null || !f.replaced) failures += "the fixture never fed the critical change"
        TrustRouteProbe.fault()?.let { failures += "recorder fault: $it" }
        failures.forEach { println("PROBLEM|publicreplacement|$it") }
        check(failures.isEmpty(), "publicreplacement: ${failures.size} problem(s)")
        println("RESULT ok")
    }

    /** On the engine's worker: the base catalog at attach; the critical change once CATALOG_READY has been polled. */
    private class Fixture(private val inner: JniEndpointBackend, private val root: Long) : EndpointBackend by inner {
        var replaced = false
            private set

        override fun attachReceiver(config: ReceiverConfig): BackendAttachResult {
            val r = inner.attachReceiver(config)
            if (r is BackendAttachResult.Attached) {
                check(TrustRouteProbe.nativeSetTransportVersion(root, r.token, DRAFT_16) == EndpointAbi.OK) { "fixture: draft 16" }
                check(TrustRouteProbe.nativeIngestCatalogAt(root, r.token, 0L, 0L, BASE.toByteArray()) == EndpointAbi.OK) { "fixture: the base catalog" }
            }
            return r
        }

        override fun pollTrack(token: Long): BackendTrackEventResult {
            val ev = inner.pollTrack(token)
            if (!replaced && ev is BackendTrackEventResult.Event && ev.event.kind == BackendTrackEventKind.CatalogReady) {
                check(TrustRouteProbe.nativeIngestCatalogAt(root, token, 0L, 1L, CRITICAL.toByteArray()) == EndpointAbi.OK) { "fixture: the critical change" }
                replaced = true
            }
            return ev
        }
    }
}
