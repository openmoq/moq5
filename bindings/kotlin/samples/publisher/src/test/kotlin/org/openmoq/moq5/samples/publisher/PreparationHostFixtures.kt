package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import org.openmoq.moq5.CodecConfigFormat
import org.openmoq.moq5.CodecConfigSource
import org.openmoq.moq5.DrainOutcome
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.MediaType
import org.openmoq.moq5.PackagingKind
import org.openmoq.moq5.SampleEntry
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.SendTrackConfig
import org.openmoq.moq5.TrackName
import org.openmoq.moq5.UnixMicros
import org.openmoq.moq5.WriteOutcome

/*
 * The host side of a live preparation: opaque track handles, the scripted
 * sender boundary and its exact effect inventory, the real capture session
 * over host lanes, and the declared configuration fixtures. Shared by the
 * ownership rows and the format-wait rows, which drive the SAME owner,
 * publication and coordinator; only the sender boundary is scripted.
 */

internal class HostTrack(val name: String) {
    override fun toString(): String = "HostTrack($name)"
}

internal class HostRemovalRefused(message: String) : IllegalStateException(message)

internal class HostRemovalError(message: String) : Error(message)

internal class HostWriteFailed(message: String) : IllegalStateException(message)

internal class HostStopError(message: String) : Error(message)

/** A child's OWN typed cancellation that wraps what it observed: it shares an ancestor with the caller's cancellation without being a reconstruction of it. */
internal class HostChildStop(message: String, cause: Throwable) : CancellationException(message) {
    init {
        initCause(cause)
    }
}

/** One recorded sender effect: the handle by IDENTITY, the whole SendObject, and the declared timeout. */
internal sealed interface Effect {
    class Add(val config: SendTrackConfig) : Effect

    class Write(val handle: HostTrack, val obj: SendObject, val timeout: Duration) : Effect

    class End(val handle: HostTrack, val timeout: Duration) : Effect

    class Remove(val handle: HostTrack) : Effect

    /** Same kind, SAME handle instance, equal SendObject (every field, value bytes) and equal timeout. */
    fun matches(other: Effect): Boolean =
        when (this) {
            is Add -> other is Add && config == other.config
            is Write -> other is Write && handle === other.handle && obj == other.obj && timeout == other.timeout
            is End -> other is End && handle === other.handle && timeout == other.timeout
            is Remove -> other is Remove && handle === other.handle
        }

    /** Diagnostics only; never the oracle. */
    fun label(): String =
        when (this) {
            is Add -> "add:" + config.name.bytes().decodeToString()
            is Write -> "write:${handle.name}[${obj.payload.size}b@${obj.presentationTime}]"
            is End -> "end:${handle.name}"
            is Remove -> "remove:${handle.name}"
        }
}

internal sealed interface AddScript {
    class Answers(val handle: HostTrack, val hold: CompletableDeferred<Unit>? = null) : AddScript

    class Raises(val raise: Throwable, val hold: CompletableDeferred<Unit>? = null) : AddScript
}

/**
 * The scripted sender boundary. A held add signals its entry, then
 * completes under `NonCancellable` once released, the way a commit-grade
 * add delivers to a cancelled caller. State is scripted, never inferred.
 */
internal class ScriptedTracks : TrackOperations<HostTrack> {
    private val lock = Any()
    val adds = ArrayDeque<AddScript>()
    val entries = ArrayDeque<CompletableDeferred<Unit>>()
    val removeRaises = mutableMapOf<HostTrack, Throwable>()
    val writeRaises = mutableMapOf<HostTrack, Throwable>()
    val removedState = mutableSetOf<HostTrack>()
    private val effects = mutableListOf<Effect>()
    private val writeHolds = mutableMapOf<HostTrack, ArrayDeque<WriteHold>>()

    /** One scripted write that signals [entered] once recorded, then waits for [release] before answering Accepted. */
    class WriteHold {
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
    }

    override suspend fun addTrack(config: SendTrackConfig): HostTrack {
        val script =
            synchronized(lock) {
                effects.add(Effect.Add(config))
                adds.removeFirstOrNull() ?: throw IllegalStateException("no add was scripted")
            }
        val hold = if (script is AddScript.Answers) script.hold else (script as AddScript.Raises).hold
        if (hold != null) {
            synchronized(lock) { entries.removeFirstOrNull() }?.complete(Unit)
            withContext(NonCancellable) { hold.await() }
        }
        return when (script) {
            is AddScript.Answers -> script.handle
            is AddScript.Raises -> throw script.raise
        }
    }

    override suspend fun removeTrack(handle: HostTrack) {
        synchronized(lock) { effects.add(Effect.Remove(handle)) }
        removeRaises[handle]?.let { throw it }
        synchronized(lock) { removedState.add(handle) }
    }

    override suspend fun write(handle: HostTrack, obj: SendObject, timeout: Duration): WriteOutcome {
        val hold =
            synchronized(lock) {
                effects.add(Effect.Write(handle, obj, timeout))
                writeHolds[handle]?.removeFirstOrNull()
            }
        writeRaises[handle]?.let { throw it }
        writeObserver?.invoke("write ${handle.name} entered")
        if (hold != null) {
            hold.entered.complete(Unit)
            hold.release.await()
        }
        writeObserver?.invoke("write ${handle.name} returned")
        return WriteOutcome.Accepted
    }

    /** Test-only: told when each write is entered and when it returns. */
    @Volatile
    var writeObserver: ((String) -> Unit)? = null

    /** Holds the next write to [handle]: it is recorded and entered, and answers only once released. */
    fun holdNextWrite(handle: HostTrack): WriteHold = WriteHold().also { synchronized(lock) { writeHolds.getOrPut(handle) { ArrayDeque() }.addLast(it) } }

    override suspend fun endTrack(handle: HostTrack, timeout: Duration): WriteOutcome {
        synchronized(lock) { effects.add(Effect.End(handle, timeout)) }
        return WriteOutcome.Accepted
    }

    override fun isRemoved(handle: HostTrack): Boolean = synchronized(lock) { handle in removedState }

    fun all(): List<Effect> = synchronized(lock) { effects.toList() }

    fun labels(): List<String> = all().map { it.label() }

    fun answers(handle: HostTrack, hold: CompletableDeferred<Unit>? = null) = apply { adds.addLast(AddScript.Answers(handle, hold)) }

    fun refuses(reason: String) = apply { adds.addLast(AddScript.Raises(TrackRefusedException(reason))) }

    fun raises(raise: Throwable, hold: CompletableDeferred<Unit>? = null) = apply { adds.addLast(AddScript.Raises(raise, hold)) }

    fun entering(): CompletableDeferred<Unit> = CompletableDeferred<Unit>().also { synchronized(lock) { entries.addLast(it) } }
}

/** The exact inventory: same length, and each effect matches by kind, handle identity and content. */
internal fun expectEffects(expected: List<Effect>, actual: List<Effect>) {
    val ok = expected.size == actual.size && expected.indices.all { expected[it].matches(actual[it]) }
    assertTrue(ok, "expected ${expected.map { it.label() }} but was ${actual.map { it.label() }}")
}

/** The recorded primary's suppressed roots must be EXACTLY [expected], by identity: no extras, no duplicates, and zero where none is owed. */
internal fun expectRoots(primary: Throwable, expected: List<Throwable>) {
    val actual = primary.suppressed.toList()
    val remaining = actual.toMutableList()
    val missing = expected.filter { e -> val i = remaining.indexOfFirst { it === e }; if (i >= 0) { remaining.removeAt(i); false } else true }
    assertTrue(missing.isEmpty() && remaining.isEmpty(), "expected roots $expected but was $actual (missing $missing, extra $remaining)")
}

/** The handoff the SESSION hands the coordinator: the first thing a lane does is register its wake signal, so entry is observable here. */
internal class EntryObserving(private val inner: CaptureHandoff) : CaptureHandoff by inner {
    @Volatile
    var entered = false

    /** Test-only: told of every dequeue, before it happens, then of what it returned. Never substitutes a result. */
    @Volatile
    var onNext: ((CapturedOutput?) -> Unit)? = null

    @Volatile
    var beforeNext: (() -> Unit)? = null

    override fun signal(sink: OutputSignal) {
        entered = true
        inner.signal(sink)
    }

    override fun next(): CapturedOutput? {
        beforeNext?.invoke()
        return inner.next().also { item -> onNext?.invoke(item) }
    }

    /** Test-only: runs before the session closes this handoff, on the closing thread; the close itself is the real one. */
    @Volatile
    var beforeClose: (() -> Unit)? = null

    override fun close(mode: CloseMode) {
        beforeClose?.invoke()
        inner.close(mode)
    }
}

internal class Completion : CompletionSink {
    val calls = mutableListOf<String>()

    /** Answers for the drains, first to last; a flush once they run out. */
    val answers = ArrayDeque<DrainOutcome>()

    override suspend fun complete() {
        calls.add("complete")
    }

    override suspend fun drain(timeout: Duration): DrainOutcome {
        calls.add("drain")
        return answers.removeFirstOrNull() ?: DrainOutcome.Flushed
    }
}

/** One discard witness exactly as a handoff reported it: generation, kind and count. */
internal data class Discard(val generation: Long, val kind: MediaKind, val outputs: Int)

internal class Forward : CaptureFaultSink {
    @Volatile
    var to: CaptureFaultSink? = null

    private val witnessed = mutableListOf<Discard>()

    override fun onFault(generation: Long, fault: CaptureFault) {
        to?.onFault(generation, fault)
    }

    override fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) {
        synchronized(witnessed) { witnessed.add(Discard(generation, kind, outputs)) }
        to?.onDiscarded(generation, kind, outputs)
    }

    /** Every discard witness the handoffs reported, in order, with exact multiplicity. */
    fun discards(): List<Discard> = synchronized(witnessed) { witnessed.toList() }
}

/** The real session, counting how many times the coordinator stopped it. */
internal class StopCounting(private val inner: CaptureSession) : CaptureSession by inner {
    /** The wrapped session's own decision. */
    override fun admitPublication(generation: Long): Boolean = inner.admitPublication(generation)

    /** The wrapped session's own receipt. */
    override suspend fun retirement(): RetirementReceipt = inner.retirement()

    @Volatile
    var stops = 0
        private set

    override fun stop(primary: Exception?): Exception? {
        stops++
        return inner.stop(primary)
    }
}

internal class NoAcquire : CaptureAcquirer {
    override val order: List<CaptureResource> = emptyList()

    override fun begin(ownership: CaptureOwnership, steps: AcquisitionSteps): AcquireOutcome.Failed? = null
}

internal class Granted(override val generation: Long) : PermissionFlow {
    override fun stateOf(permission: CapturePermission) = PermissionState.Granted

    override fun request(permissions: Set<CapturePermission>) = emptySet<CapturePermission>()

    override fun onResult(generation: Long, permission: CapturePermission, facts: PermissionFacts) = PermissionOutcome.Ignored("granted")

    override fun retire(reason: String) = Unit

    override fun readiness(): CaptureReadiness = CaptureReadiness.Ready
}

/** The real session whose `stop` ALSO raises, for the capture-cleanup-failure variant. */
internal class RaisingStopSession(private val inner: CaptureSession, private val raise: Throwable) : CaptureSession by inner {
    /** The wrapped session's own decision. */
    override fun admitPublication(generation: Long): Boolean = inner.admitPublication(generation)

    /** The wrapped session's own receipt. */
    override suspend fun retirement(): RetirementReceipt = inner.retirement()

    override fun stop(primary: Exception?): Exception? {
        inner.stop(primary)
        throw raise
    }
}

internal class Lane(val kind: MediaKind, faults: CaptureFaultSink, maxOutputBytes: Int = HOST_MAX_OUTPUT_BYTES) {
    val buffers = HostBuffers()
    val releases = HostReleases()
    val ledger = BoundedCaptureLedger(kind, GENERATION, capacity = 8)
    val handoff = BoundedCaptureHandoff(kind, GENERATION, buffers, releases, ledger, faults, CAPACITY, maxOutputBytes)
}

/**
 * The real session, the real owner, the production live publication; only the sender boundary is scripted.
 *
 * [acquirer], [listen] and [schedule] default to an immediate start, the
 * coordinator's own events as the session's listener, and no held boundary;
 * a row that needs a startup settled elsewhere, a listener around those
 * events, or a boundary held, supplies them.
 */
internal class Rig(
    val tracks: ScriptedTracks = ScriptedTracks(),
    stopRaises: Throwable? = null,
    maxOutputBytes: Int = HOST_MAX_OUTPUT_BYTES,
    acquirer: CaptureAcquirer = NoAcquire(),
    listen: (BroadcastEvents) -> CaptureSessionListener = { it },
    schedule: StartupSchedule = StartupSchedule.NONE,
) {
    val forward = Forward()
    val video = Lane(MediaKind.Video, forward, maxOutputBytes)
    val audio = Lane(MediaKind.Audio, forward, maxOutputBytes)
    val videoEntry = EntryObserving(video.handoff)
    val audioEntry = EntryObserving(audio.handoff)
    val events = BroadcastEvents()
    val ownership = OrderedCaptureOwnership(GENERATION, HostDispatcher(), LateLog())
    private val real = PlatformCaptureSession(GENERATION, ownership, acquirer, videoEntry, audioEntry, listen(events), schedule)

    /** The real session's format-callback side: the boundary holders will use. */
    val gate: FormatCallbackGate get() = real

    /** The same session's codec-callback side: the boundary encoders use. */
    val codecGate: CodecCallbackGate get() = real
    val session = StopCounting(if (stopRaises == null) real else RaisingStopSession(real, stopRaises))
    val owner = TrackOwner(tracks)
    val completion = Completion()
    val clock = PlatformCaptureClock(ClockCalibration(elapsedRealtimeNanos = BASE_NANOS, epoch = UnixMicros(EPOCH), uncertaintyMicros = 10L))
    /** The converter's bound: the same numeric output cap these lanes' handoffs enforce. */
    val limits = AvcPayloadLimits(HOST_MAX_NAL_UNITS, maxOutputBytes)
    val publication = livePublication(owner, tracks, events, session.publicationAdmission(), validatedVideo(), validatedAudio(), limits, clock, ORIGIN)

    init {
        forward.to = real
    }

    /** The production format-wait publication over the SAME owner, sender boundary and events. */
    fun formatPublication(formats: FormatSources, budget: Duration) =
        // These lanes key no PCM, so there is no audio keying to agree with.
        livePublication(owner, tracks, events, session.publicationAdmission(), formats, budget, limits, clock, ORIGIN, keying = null)

    fun broadcast(through: Publication = publication) = LiveBroadcast(session, events, through, completion, WRITE_TIMEOUT, DRAIN_TIMEOUT)

    fun terminate() = video.handoff.onCodecError(GENERATION, "the encoder failed")

    suspend fun run(through: Publication = publication): BroadcastOutcome =
        broadcast(through).run(Granted(GENERATION), CaptureCapability.Available, CaptureCapability.Available)
}

/**
 * The object the accepted pipeline publishes for one delivered key frame,
 * DECLARED from the contract: the exact bytes, the presentation time over
 * origin 0, the capture instant the ledger claimed (epoch + pts), sync,
 * grouping by kind, and the defaults this sample declares nothing for.
 */
internal fun sendObject(payload: ByteArray, pts: Long, startsGroup: Boolean) =
    SendObject(
        payload = MediaBytes(payload),
        isSync = true,
        startsGroup = startsGroup,
        presentationTime = MediaMicros(pts.toULong()),
        endsGroup = false,
        decodeTime = null,
        captureTime = UnixMicros(EPOCH + pts.toULong()),
        sapType = null,
        cmafProperties = null,
    )

internal fun deliver(lane: Lane, index: Int, payload: ByteArray, pts: Long) {
    lane.buffers.put(index, ByteBuffer.wrap(payload))
    assertEquals(ObserveOutcome.Recorded, lane.ledger.observe(GENERATION, lane.kind, CaptureObservation(pts, BASE_NANOS + pts * 1_000L)))
    assertEquals(HandoffOutcome.Queued, lane.handoff.onOutput(GENERATION, CodecOutput(index, 0, payload.size, pts, CaptureContract.FLAG_KEY_FRAME)))
}

internal fun deliverEnd(lane: Lane, index: Int) {
    lane.buffers.put(index, ByteBuffer.allocate(0))
    assertEquals(HandoffOutcome.Queued, lane.handoff.onOutput(GENERATION, CodecOutput(index, 0, 0, 0L, CaptureContract.FLAG_END_OF_STREAM)))
}

/** The run must still be live here; if it already settled, its recorded cause is what fails the row. */
internal fun stillLive(owner: RunOwner) {
    if (owner.reached) owner.observed().exceptionOrNull()?.let { throw it }
}

/**
 * The run is EXPECTED to have settled here through a cancellation this
 * row issued; anything else it settled with -- a TODO, an Error, a
 * refusal -- is what fails the row, as itself. Not a liveness premise.
 */
internal fun onlyIssuedSettlement(owner: RunOwner) {
    if (owner.reached) owner.observed().exceptionOrNull()?.takeUnless { owner.issued(it) }?.let { throw it }
}

internal suspend fun settled(owner: RunOwner): Result<BroadcastOutcome> {
    withTimeoutOrNull(BOUNDED) { owner.job.join() } ?: throw AssertionError("the broadcast did not settle")
    return owner.observed()
}

    const val GENERATION = 1L
    const val CAPACITY = 4
    const val BASE_NANOS = 1_000_000_000L
    const val ORIGIN = 0L
    const val EPOCH = 1_700_000_000_000_000uL
    val WRITE_TIMEOUT = 1.seconds
    val DRAIN_TIMEOUT = 5.seconds
    val BOUNDED = 30.seconds

    /** DECLARED fixtures: they certify nothing about an encoder. */
    val VIDEO_CONFIG =
        SendTrackConfig(
            name = TrackName("video".encodeToByteArray()),
            mediaType = MediaType.Video,
            packaging = PackagingKind.Raw,
            codec = MediaBytes("avc1.64001f".encodeToByteArray()),
            bitrate = 1_500_000uL,
            width = 640u,
            height = 480u,
        )
    val AUDIO_CONFIG =
        SendTrackConfig(
            name = TrackName("audio".encodeToByteArray()),
            mediaType = MediaType.Audio,
            packaging = PackagingKind.Raw,
            codec = MediaBytes("mp4a.40.2".encodeToByteArray()),
            bitrate = 128_000uL,
            samplerate = 48_000u,
            channelConfig = MediaBytes("2".encodeToByteArray()),
        )

    /** The record the scripted AVC utility answers for [validatedVideo]: a declared literal, not a built avcC. */
    val SCRIPTED_VIDEO_RECORD = byteArrayOf(0xa1.toByte(), 0xb2.toByte(), 0xc3.toByte(), 0xd4.toByte(), 0xe5.toByte())

    /** AudioSpecificConfig for AAC LC, 48 kHz (index 3), two channels: 00010 0011 0010 000. */
    val FIXTURE_ASC = byteArrayOf(0x11, 0x90.toByte())

    /** What the owner is expected to add for the validated fixtures: the declared configurations plus their initData. */
    val VIDEO_FORMAT_CONFIG = VIDEO_CONFIG.copy(initData = MediaBytes(SCRIPTED_VIDEO_RECORD))
    val AUDIO_FORMAT_CONFIG = AUDIO_CONFIG.copy(initData = MediaBytes(FIXTURE_ASC))

/**
 * SCRIPTED validator fixtures: synthetic parameter-set and
 * AudioSpecificConfig literals through the REAL [TrackFormats], with utilities
 * that answer declared responses (the declared fixture codec strings and a
 * literal record). A validated format is the only kind a slot or the format
 * wait can carry; these certify nothing about an encoder, and the media the
 * rows publish stays explicitly synthetic.
 */
internal object ScriptedVideoUtility : AvcRecordUtility {
    override fun buildInitData(source: CodecConfigSource, bytes: MediaBytes, nalLengthSize: Int): UtilityOutcome<MediaBytes> =
        UtilityOutcome.Done(MediaBytes(SCRIPTED_VIDEO_RECORD))

    override fun formatCodecString(format: CodecConfigFormat, sampleEntry: SampleEntry, decoderConfig: MediaBytes): UtilityOutcome<String> =
        UtilityOutcome.Done(VIDEO_CONFIG.codec.bytes().decodeToString())
}

internal object ScriptedAudioUtility : AacStringUtility {
    override fun formatCodecString(audioSpecificConfig: MediaBytes): UtilityOutcome<String> =
        UtilityOutcome.Done(AUDIO_CONFIG.codec.bytes().decodeToString())
}

internal val FIXTURE_BOUNDS = ConfigBounds(maxUnits = 4, maxUnitBytes = 4096, maxTotalBytes = 8192)

/** The fixture's video output-format observation: fresh buffers over the synthetic parameter sets, at [width] x [height]. */
internal fun videoObservation(width: Int? = 640, height: Int? = 480): VideoFormatObservation {
    val sps = byteArrayOf(0, 0, 0, 1, 0x67, 0x42, 0x00, 0x1e, 0xab.toByte(), 0xcd.toByte())
    val pps = byteArrayOf(0, 0, 0, 1, 0x68, 0xce.toByte(), 0x3c, 0x80.toByte())
    return VideoFormatObservation(width, height, null, VIDEO_CONFIG.bitrate, listOf(ByteBuffer.wrap(sps), ByteBuffer.wrap(pps)))
}

/** The fixture's audio output-format observation over [asc], declared at [sampleRate] and [channels]. */
internal fun audioObservation(asc: ByteArray = FIXTURE_ASC, sampleRate: Int? = 48_000, channels: Int? = 2): AudioFormatObservation =
    AudioFormatObservation(sampleRate, channels, null, AUDIO_CONFIG.bitrate, listOf(ByteBuffer.wrap(asc.copyOf())))

internal fun validatedVideo(): CompleteTrackFormat {
    val observed = videoObservation()
    val verdict = TrackFormats.video(VIDEO_CONFIG.name, observed, FIXTURE_BOUNDS, ScriptedVideoUtility)
    return (verdict as? TrackFormatVerdict.Complete ?: throw AssertionError("setup: the video fixture validates")).format
}

internal fun validatedAudio(): CompleteTrackFormat = validatedAudio(FIXTURE_ASC, 48_000, 2)

/** AudioSpecificConfig for AAC LC, 44.1 kHz (index 4), two channels: 00010 0100 0010 000. */
internal val FIXTURE_ASC_44100 = byteArrayOf(0x12, 0x10)

/** A validated audio format over [asc], declared at [sampleRate] and [channels], through the real validator and the scripted string utility. */
internal fun validatedAudio(asc: ByteArray, sampleRate: Int, channels: Int): CompleteTrackFormat {
    val observed = AudioFormatObservation(sampleRate, channels, null, AUDIO_CONFIG.bitrate, listOf(ByteBuffer.wrap(asc.copyOf())))
    val verdict = TrackFormats.audio(AUDIO_CONFIG.name, observed, FIXTURE_BOUNDS, ScriptedAudioUtility)
    return (verdict as? TrackFormatVerdict.Complete ?: throw AssertionError("setup: the audio fixture validates")).format
}

/** An explicit NAL-unit bound for host pipelines; each host frame holds far fewer. */
internal const val HOST_MAX_NAL_UNITS = 16

/** The validated fixture format for [kind], through the real validator over scripted utilities. */
internal fun validatedFormat(kind: MediaKind): CompleteTrackFormat = if (kind == MediaKind.Video) validatedVideo() else validatedAudio()

/** The host lanes' converter bound: [HOST_MAX_NAL_UNITS] and the host handoff output cap. */
internal val HOST_LIMITS = AvcPayloadLimits(HOST_MAX_NAL_UNITS, HOST_MAX_OUTPUT_BYTES)

/**
 * One declared Annex-B key frame for the validated fixture video format:
 * `00 00 00 01`, an IDR header (nal_ref_idc 3, type 5), then six body bytes.
 * Synthetic framing, not a decodable picture.
 */
internal val VIDEO_ANNEXB = byteArrayOf(0, 0, 0, 1, 0x65, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15)

/** Its declared avc1 payload: the same seven-byte NAL unit after its big-endian length. */
internal val VIDEO_AVC1 = byteArrayOf(0, 0, 0, 7, 0x65, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15)

/**
 * The admitted format callbacks one row holds, released on every exit from
 * the row's body.
 *
 * A row admits through [admit] -- covered the instant it is admitted -- and
 * reports through [release], the protocol under test. [owning] runs the
 * row's body and, on EVERY exit, releases whatever is still admitted, with no
 * cause, so it claims nothing -- before the run wrapper joins an owner that
 * is waiting for those callbacks. Every release is attempted even when one
 * raises; what they raised is attached to the row's own failure, never put
 * in its place, and leaves by itself only when the row had none.
 */
internal class RowReservations(private val gate: FormatCallbackGate) {
    private val outstanding = mutableListOf<FormatReservation>()

    fun admit(key: FormatHolderKey): FormatReservation {
        val admission = gate.reserveFormat(key)
        if (admission is FormatAdmission.Admitted) outstanding.add(admission.reservation)
        check(admission is FormatAdmission.Admitted) { "the ${key.kind} callback was not admitted: $admission" }
        return admission.reservation
    }

    /** The protocol call. An accepted release -- including one whose claimant cleanup then raises -- is no longer outstanding. */
    fun release(reservation: FormatReservation, cause: Throwable?): FormatRelease {
        var accepted = true
        try {
            val released = gate.releaseFormat(reservation, cause)
            accepted = released is FormatRelease.Released
            return released
        } finally {
            if (accepted) outstanding.removeAll { it === reservation }
        }
    }

    /** Releases everything still admitted, attempting each; returns what they raised. */
    fun releaseAll(): List<Throwable> {
        val raised = mutableListOf<Throwable>()
        for (reservation in outstanding.toList()) {
            runCatching { gate.releaseFormat(reservation, null) }.exceptionOrNull()?.let { raised.add(it) }
            outstanding.removeAll { it === reservation }
        }
        return raised
    }

    suspend fun <T> owning(block: suspend () -> T): T {
        var primary: Throwable? = null
        try {
            return block()
        } catch (cancelled: CancellationException) {
            primary = cancelled
            throw cancelled
        } catch (failure: Exception) {
            primary = failure
            throw failure
        } catch (error: Error) {
            primary = error
            throw error
        } finally {
            val found = releaseAll()
            val leaving = primary
            if (found.isNotEmpty()) {
                if (leaving == null) {
                    val first = found.first()
                    found.drop(1).forEach { if (it !== first) first.addSuppressed(it) }
                    throw first
                }
                found.forEach { if (it !== leaving) leaving.addSuppressed(it) }
            }
        }
    }
}
