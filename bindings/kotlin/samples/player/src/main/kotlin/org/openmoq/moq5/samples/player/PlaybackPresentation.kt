package org.openmoq.moq5.samples.player

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.Duration.Companion.nanoseconds

/**
 * An optional acceptance observer for the sample's own presentation. It reads
 * the frame-index marker drawn into the test input from each displayed frame
 * and freezes one verdict after decoder output EOS. It is sample-only and
 * never part of playback: the ordinary player neither waits on it nor
 * changes behaviour because of it.
 */

/** The launch extra that arms the acceptance observer; absent means no observer at all. */
internal const val OBSERVE_PRESENTATION_EXTRA = "org.openmoq.moq5.samples.player.OBSERVE_PRESENTATION"

/** The platform-log tags: the acceptance receipt and the later observer-closed receipt. */
internal const val PRESENTATION_LOG_TAG = "MoqPlayer"
internal const val OBSERVER_LOG_TAG = "MoqPlayerObserver"

/**
 * Where receipts go: the platform log under a tag, which is the direct
 * diagnostic channel, and the session report, which reaches the screen only
 * while that session is still the one on display (the controller discards a
 * stale session's lines at delivery). Each receipt is offered to both, once,
 * even when the other throws.
 */
internal class PresentationSinks(val report: (String) -> Unit, val log: (tag: String, line: String) -> Unit)

/**
 * The Activity's seat when the launch asked for observation, else null: an
 * ordinary launch captures nothing, classifies nothing and never waits.
 */
internal fun <T : Any, S : Any> presentationSeatFor(
    observe: Boolean, registry: PlaybackDisplayRegistry<T, S>, nowNs: () -> Long,
): PlaybackPresentationSeat<T, S>? = if (observe) PlaybackPresentationSeat(registry, nowNs) else null

/** Marker outcomes, named exactly as the test-owned recipe's vectors name them. */
internal enum class PlaybackMarkerOutcome(val wire: String) {
    Ok("ok"),
    Blank("blank"),
    Unreadable("unreadable"),
    SyncInvalid("sync-invalid"),
    ComplementInvalid("complement-invalid"),
    CaptureNull("capture-null"),
    CaptureSize("capture-size"),
    CaptureEmpty("capture-empty"),
    /** The optional capture itself threw; never produced by [PlaybackMarker]. */
    CaptureFailed("capture-failed"),
}

internal data class PlaybackMarkerReading(val outcome: PlaybackMarkerOutcome, val index: Int?)

/** Reads the marker from a whole-frame 8-bit luma capture by fractional cell sampling. */
internal object PlaybackMarker {
    private const val SOURCE_WIDTH = 320.0
    private const val SOURCE_HEIGHT = 240.0
    private const val CELL = 40
    private const val THRESHOLD = 128
    private const val MARGIN = 64
    private const val BITS = 6
    private val ROW_Y = intArrayOf(20, 80)
    private val SYNC_WHITE = arrayOf(mapOf(6 to true, 7 to false), mapOf(6 to false, 7 to true))
    private const val CELLS = 8

    fun classify(luma: ByteArray?, width: Int, height: Int): PlaybackMarkerReading {
        if (width <= 0 || height <= 0) return PlaybackMarkerReading(PlaybackMarkerOutcome.CaptureEmpty, null)
        if (luma == null) return PlaybackMarkerReading(PlaybackMarkerOutcome.CaptureNull, null)
        if (luma.size.toLong() != width.toLong() * height) return PlaybackMarkerReading(PlaybackMarkerOutcome.CaptureSize, null)
        val rows = ROW_Y.map { y ->
            IntArray(CELLS) { cell ->
                val fx = (cell * CELL + CELL / 2.0) / SOURCE_WIDTH
                val fy = (y + CELL / 2.0) / SOURCE_HEIGHT
                val x = minOf(width - 1, (fx * width).toInt())
                val yy = minOf(height - 1, (fy * height).toInt())
                luma[yy * width + x].toInt() and 0xFF
            }
        }
        if (rows.any { r -> r.any { kotlin.math.abs(it - THRESHOLD) < MARGIN } }) return PlaybackMarkerReading(PlaybackMarkerOutcome.Unreadable, null)
        val bits = rows.map { r -> BooleanArray(CELLS) { r[it] > THRESHOLD } }
        val all = bits.flatMap { it.toList() }
        if (all.all { it } || all.none { it }) return PlaybackMarkerReading(PlaybackMarkerOutcome.Blank, null)
        for ((row, sync) in SYNC_WHITE.withIndex()) {
            if (sync.any { (cell, white) -> bits[row][cell] != white }) return PlaybackMarkerReading(PlaybackMarkerOutcome.SyncInvalid, null)
        }
        if ((0 until BITS).any { bits[0][it] == bits[1][it] }) return PlaybackMarkerReading(PlaybackMarkerOutcome.ComplementInvalid, null)
        return PlaybackMarkerReading(PlaybackMarkerOutcome.Ok, (0 until BITS).sumOf { if (bits[0][it]) 1 shl it else 0 })
    }
}


/**
 * One per Activity. Only the first display lease this Activity ever offers,
 * and only while that lease is its texture's first, may arm an acceptance
 * observer; every later lease is refused, even on a re-announced texture.
 */
internal class PresentationObserverGate {
    private var observed = false

    @Synchronized fun admit(lease: PlaybackDisplayLease<*, *>): Boolean {
        if (observed) return false
        observed = true
        return lease.ordinal == 1
    }
}


/** One classified capture, stamped on the UI thread when it was observed. */
internal data class PresentationFact(
    val reading: PlaybackMarkerReading,
    val textureTimestampNs: Long,
    val observedNs: Long,
)

/**
 * The frozen acceptance verdict. A value: nothing observed after freeze
 * reaches it. [preVideo] counts blank and unreadable captures before the first
 * valid index; the four outcome counts cover every accepted capture.
 * Refusal codes contain no spaces, so [line] is a space-separated key=value
 * grammar a parser can compare field by field.
 */
internal data class PresentationSnapshot(
    val pass: Boolean,
    val released: Int,
    val latched: Int,
    val distinctTs: Int,
    val indices: List<Int>,
    val preVideo: Int,
    val unreadable: Int,
    val syncInvalid: Int,
    val complementInvalid: Int,
    val captureRefusals: Int,
    val ordered: Boolean,
    val settled: Boolean,
    val ordinal: Int,
    val leaseId: Long,
    val refusal: String?,
    val freezeReason: String,
) {
    /** `presented: pass=… released=… … refusal=<code|none> freezeReason=…`, indices as `[a,b]` or `[]`. */
    fun line(): String =
        "presented: pass=$pass released=$released latched=$latched distinctTs=$distinctTs " +
            "indices=[${indices.joinToString(",")}] preVideo=$preVideo unreadable=$unreadable syncInvalid=$syncInvalid " +
            "complementInvalid=$complementInvalid captureRefusals=$captureRefusals ordered=$ordered settled=$settled " +
            "ordinal=$ordinal leaseId=$leaseId refusal=${refusal ?: "none"} freezeReason=$freezeReason"
}

/** Diagnostics counted between freeze and close; never part of acceptance. */
internal data class PresentationClosedReceipt(
    val lateAfterFreeze: Int,
    val ignoredForeign: Int,
    val retiredAfterFreeze: Int,
) {
    /** `observer-closed: lateAfterFreeze=… ignoredForeign=… retiredAfterFreeze=…`. */
    fun line(): String =
        "observer-closed: lateAfterFreeze=$lateAfterFreeze ignoredForeign=$ignoredForeign retiredAfterFreeze=$retiredAfterFreeze"
}

/** An 8-bit luma plane for [PlaybackMarker], converted from a whole-frame ARGB capture. */
internal class PlaybackCapture(val luma: ByteArray, val width: Int, val height: Int) {
    companion object {
        /** Rec. 601 integer luma of each ARGB pixel; alpha is ignored. Grey (r = g = b = v) maps to exactly v. */
        fun fromArgb(argb: IntArray, width: Int, height: Int): PlaybackCapture =
            PlaybackCapture(ByteArray(argb.size) { i ->
                val p = argb[i]
                ((77 * ((p shr 16) and 0xFF) + 150 * ((p shr 8) and 0xFF) + 29 * (p and 0xFF) + 128) shr 8).toByte()
            }, width, height)
    }
}

/**
 * The UI-to-renderer handoff for one lease. [offer] and [retire] are called on
 * the UI thread and never wait; [freeze] is called once by the renderer after
 * decoder output EOS and returns the same snapshot to every later caller.
 */
internal class PresentationRecorder(
    private val nowNs: () -> Long,
    private val finalIndex: Int,
    private val capacity: Int = 512,
) {
    private val lock = Any()
    private val signal = CompletableDeferred<String>()
    private var opened = false
    private var texture: Any? = null
    private var leaseId = 0L
    private var ordinal = 0
    private var failure: String? = null
    private var facts = 0
    private val okIndices = ArrayList<Int>()
    private val okTs = ArrayList<Long>()
    private var preVideo = 0
    private var unreadable = 0
    private var syncInvalid = 0
    private var complementInvalid = 0
    private var captureRefusals = 0
    private var finalObservedNs: Long? = null
    private var frozen: PresentationSnapshot? = null
    private var closed: PresentationClosedReceipt? = null
    private var late = 0
    private var foreign = 0
    private var retiredAfterFreeze = 0

    val wake: Deferred<String> get() = signal

    private fun fail(reason: String) {
        if (failure == null) failure = reason
        signal.complete(reason)
    }

    fun open(lease: PlaybackDisplayLease<*, *>, admitted: Boolean): Unit = synchronized(lock) {
        check(!opened) { "a recorder observes one lease" }
        opened = true
        texture = lease.texture
        leaseId = lease.leaseId
        ordinal = lease.ordinal
        if (!admitted || lease.ordinal != 1) fail("observer-refused:reused-surface")
    }

    fun offer(texture: Any, leaseId: Long?, fact: PresentationFact): Boolean = synchronized(lock) {
        if (closed != null) return false
        if (!opened || texture !== this.texture || leaseId != this.leaseId) { foreign++; return false }
        if (frozen != null) { late++; return false }
        if (failure != null) return false
        if (facts == capacity) { fail("overflowed"); return false }
        facts++
        val r = fact.reading
        when (r.outcome) {
            PlaybackMarkerOutcome.Unreadable -> unreadable++
            PlaybackMarkerOutcome.SyncInvalid -> syncInvalid++
            PlaybackMarkerOutcome.ComplementInvalid -> complementInvalid++
            PlaybackMarkerOutcome.CaptureNull, PlaybackMarkerOutcome.CaptureSize, PlaybackMarkerOutcome.CaptureEmpty,
            PlaybackMarkerOutcome.CaptureFailed -> captureRefusals++
            else -> Unit
        }
        when (r.outcome) {
            PlaybackMarkerOutcome.CaptureNull, PlaybackMarkerOutcome.CaptureSize, PlaybackMarkerOutcome.CaptureEmpty,
            PlaybackMarkerOutcome.CaptureFailed -> fail("capture-refused")
            PlaybackMarkerOutcome.SyncInvalid, PlaybackMarkerOutcome.ComplementInvalid -> fail("geometry-refused")
            PlaybackMarkerOutcome.Blank, PlaybackMarkerOutcome.Unreadable -> if (okIndices.isEmpty()) preVideo++ else fail("post-valid-invalid")
            PlaybackMarkerOutcome.Ok -> {
                val index = checkNotNull(r.index)
                okIndices += index
                okTs += fact.textureTimestampNs
                if (index == finalIndex && finalObservedNs == null) {
                    finalObservedNs = fact.observedNs
                    signal.complete("final")
                }
            }
        }
        true
    }

    fun retire(): Unit = synchronized(lock) {
        if (closed != null) return
        if (frozen != null) retiredAfterFreeze++ else fail("display-retired")
    }

    fun freeze(reason: String, decoderEosNs: Long, released: Int): PresentationSnapshot = synchronized(lock) {
        frozen?.let { return it }
        if (reason == "cancelled") fail("cancelled")
        val final = finalObservedNs
        val settled = final != null && final <= decoderEosNs + SETTLE_WINDOW_NS
        val distinct = okIndices.toSortedSet()
        val distinctTs = okTs.toSet().size
        val ordered = okTs.zipWithNext().all { (a, b) -> a <= b } && okIndices.zipWithNext().all { (a, b) -> a <= b }
        val refusal = failure ?: when {
            okIndices.isEmpty() -> "no-valid-marker"
            final == null -> "final-frame-missing"
            !settled -> "final-frame-late"
            distinct.size < 2 -> "too-few-indices"
            distinctTs < 2 -> "too-few-timestamps"
            !ordered -> "unordered"
            released != finalIndex + 1 -> "released-mismatch"
            else -> null
        }
        PresentationSnapshot(
            pass = refusal == null, released = released, latched = facts, distinctTs = distinctTs,
            indices = if (failure == "geometry-refused") emptyList() else distinct.toList(),
            preVideo = preVideo, unreadable = unreadable, syncInvalid = syncInvalid, complementInvalid = complementInvalid,
            captureRefusals = captureRefusals, ordered = ordered, settled = settled, ordinal = ordinal, leaseId = leaseId,
            refusal = refusal, freezeReason = reason,
        ).also { frozen = it }
    }

    fun close(): PresentationClosedReceipt = synchronized(lock) {
        closed ?: PresentationClosedReceipt(late, foreign, retiredAfterFreeze).also { closed = it }
    }

    /**
     * On the renderer, after decoder output EOS observed at [decoderEosNs]:
     * wait for [wake] no later than that instant plus the settlement window,
     * then freeze. Cancellation freezes with reason `cancelled` first.
     */
    suspend fun settle(decoderEosNs: Long, released: Int): PresentationSnapshot {
        var normal = false
        try {
            val left = decoderEosNs + SETTLE_WINDOW_NS - nowNs()
            if (left > 0) withTimeoutOrNull(left.nanoseconds) { wake.await() }
            normal = true
            return freeze("settled", decoderEosNs, released)
        } finally {
            if (!normal) withContext(NonCancellable) { freeze("cancelled", decoderEosNs, released) }
        }
    }

    private companion object { const val SETTLE_WINDOW_NS = 3_000_000_000L }
}

/**
 * The Activity's one acceptance-observer seat: its display registry, its one
 * irreversible first-lease guard and the recorder of the attempt in flight.
 * The Activity's TextureView callbacks call [onFrame] and [onDestroyed] on the
 * UI thread; each Start takes an [attempt] and passes it to the host and the
 * session, so every lifecycle edge crosses this one object.
 */
internal class PlaybackPresentationSeat<T : Any, S : Any>(
    val registry: PlaybackDisplayRegistry<T, S>,
    private val nowNs: () -> Long,
) {
    private val gate = PresentationObserverGate()
    @Volatile private var current: Seated? = null

    private val emission = Any()

    fun attempt(objectBudget: Int, sinks: PresentationSinks): PlaybackPresentationAttempt = Seated(objectBudget - 1, sinks)

    /** onSurfaceTextureUpdated: stamp with the registry's current lease id, capture, classify and offer. */
    fun onFrame(texture: T, textureTimestampNs: Long, capture: () -> PlaybackCapture?) {
        val seated = current ?: return
        val leaseId = registry.leaseIdFor(texture)
        // The capture is optional diagnostics: its failure is a named refusal, never the UI callback's.
        val reading = try {
            val c = capture()
            if (c == null) PlaybackMarkerReading(PlaybackMarkerOutcome.CaptureNull, null)
            else PlaybackMarker.classify(c.luma, c.width, c.height)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (failed: Exception) {
            PlaybackMarkerReading(PlaybackMarkerOutcome.CaptureFailed, null)
        }
        seated.recorder.offer(texture, leaseId, PresentationFact(reading, textureTimestampNs, nowNs()))
    }

    /**
     * onSurfaceTextureDestroyed: retire the observer bound to [texture], then
     * the registry's answer, which the Activity returns to the framework.
     */
    fun onDestroyed(texture: T): Boolean {
        current?.takeIf { it.texture === texture }?.recorder?.retire()
        return registry.destroyed(texture)
    }

    private inner class Seated(finalIndex: Int, private val sinks: PresentationSinks) : PlaybackPresentationAttempt {
        val recorder = PresentationRecorder(nowNs, finalIndex)
        @Volatile var texture: Any? = null
        private var reported = false
        private var finished = false
        @Volatile private var displayClosed = false

        override fun displayReleased() {
            displayClosed = true
        }

        /** One line to both channels, adjacent, attempted on each even if the other throws; the first failure surfaces. */
        private fun emit(tag: String, line: String) = synchronized(emission) {
            val reportFailure = runCatching { sinks.report(line) }.exceptionOrNull()
            val logFailure = runCatching { sinks.log(tag, line) }.exceptionOrNull()
            val first = reportFailure ?: logFailure
            if (first != null && logFailure != null && logFailure !== first) first.addSuppressed(logFailure)
            first?.let { throw it }
        }

        override fun onDisplayOwned(display: PlaybackDisplay<*>) {
            val lease = display as? PlaybackDisplayLease<*, *> ?: throw IllegalStateException("the display is not a registry lease")
            texture = lease.texture
            recorder.open(lease, gate.admit(lease))
            current = this
        }

        private fun accept(snapshot: PresentationSnapshot) {
            if (reported) return
            reported = true
            emit(PRESENTATION_LOG_TAG, snapshot.line())
        }

        override suspend fun settle(decoderEosNs: Long, released: Int) = accept(recorder.settle(decoderEosNs, released))

        override suspend fun abandon(released: Int) =
            withContext(NonCancellable) { accept(recorder.freeze("cancelled", nowNs(), released)) }

        override fun finish() {
            if (finished) return
            finished = true
            // A failure before the renderer existed never reached abandon: nothing was released.
            val unreported = runCatching { accept(recorder.freeze("cancelled", nowNs(), 0)) }.exceptionOrNull()
            if (!displayClosed) {
                // The host retained the display: the observer stays bound to its lease and
                // nothing claims a closed boundary. The host's retained failure is terminal.
                unreported?.let { throw it }
                return
            }
            if (current === this) current = null
            val closed = runCatching { emit(OBSERVER_LOG_TAG, recorder.close().line()) }.exceptionOrNull()
            val first = unreported ?: closed
            if (first != null && closed != null && closed !== first) first.addSuppressed(closed)
            first?.let { throw it }
        }
    }
}

/**
 * One Start's observer. [onDisplayOwned] is the host's `onDisplayOwned` hook;
 * [settle] runs on the renderer at decoder output EOS with the renderer's
 * released-output count; [abandon] freezes `cancelled` with the count read
 * while the renderer is still owned, if no settle froze; [displayReleased] is
 * the host's fact that the attempt was released after proved display closure;
 * [finish] runs after the host returned. The acceptance receipt is emitted
 * when the snapshot freezes; the observer-closed receipt is emitted at
 * [finish] only when the display release is proved, each once on both
 * [PresentationSinks] channels. A retained display keeps the observer bound
 * and emits no closed receipt. An observer that never saw a lease reports
 * `ordinal=0 leaseId=0`.
 */
internal interface PlaybackPresentationAttempt {
    fun onDisplayOwned(display: PlaybackDisplay<*>)
    fun displayReleased()
    suspend fun settle(decoderEosNs: Long, released: Int)
    suspend fun abandon(released: Int)
    fun finish()
}
