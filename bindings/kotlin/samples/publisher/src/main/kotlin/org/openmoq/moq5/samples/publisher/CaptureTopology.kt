package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlin.coroutines.coroutineContext
import kotlin.time.Duration
import kotlinx.coroutines.ensureActive
import org.openmoq.moq5.MediaSender
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.SendTrackConfig
import org.openmoq.moq5.TrackName
import org.openmoq.moq5.WriteOutcome

/**
 * The smallest shape that lets the publisher sample publish real media.
 *
 * ONE capture session, owned by the broadcast that already exists, yielding two
 * [CaptureHandoff]s -- video and audio -- behind the already accepted
 * [EncodedOutputSource] seam. `PublishPipeline`, `PublishSequence`,
 * `PublishTimeline` and `PlatformCaptureClock` are unchanged; the session keeps
 * writing through the same `MediaSender`. There is no second publication path,
 * and nothing new is added to the public MOQ5 surface.
 *
 * **Integration with the frozen caller, named before it is edited.**
 * `BroadcastSession.publish` today reads `encoder.next() ?: break`
 * (`BroadcastSession.kt:108`), and `EncodedOutputSource` documents `null` as
 * "none left" (`PublishPipeline.kt:32-34`). That is correct for a finite
 * fixture and wrong for a live track, which is empty between callbacks.
 * Substituting a live source there would complete the broadcast before the
 * first encoder output. The narrow change GREEN needs in `BroadcastSession`
 * only -- `PublishPipeline` untouched -- is:
 *
 *  1. one drain loop PER TRACK, each over its own `PublishPipeline` and track
 *     handle, both writing through the ONE sender;
 *  2. on `next() == null`: if `handoff.ended`, the track's terminal has been
 *     delivered -- `endTrack` it; otherwise SUSPEND on the [OutputSignal] the
 *     handoff raises. No polling, no sleep, no cross-track blocking;
 *  3. `complete()` only after BOTH tracks have ended or the session terminated;
 *     `EndTrack` from the pipeline is per track, never the broadcast's end.
 *
 * **Acquisition is driven, not fabricated.** The session does not open anything
 * itself; it runs an injected [CaptureAcquirer] against its ownership in a
 * declared order, and asynchronous steps settle through the ownership's
 * `adopt`/`fail`. [CaptureStart.Pending] means the asynchronous steps are in
 * flight; [CaptureReady] arrives -- through the sample-internal route, not a
 * test call -- when every step has settled. Start is refused before anything
 * is acquired if a permission or either clock fact does not allow it. The
 * steps are not independent -- the capture session depends on the camera
 * having opened -- so acquisition is counted through [AcquisitionSteps] as it
 * is submitted, never by reading a count after the driver has returned.
 *
 * **One ending.** Every fault reaches the session through [CaptureFaultSink]
 * from the track that raised it, outside that track's monitor; the session
 * decides termination under ITS lock, releases it, and only then closes both
 * tracks and the ownership. Terminating twice terminates nothing twice.
 *
 * What this file does NOT claim: that any of it acquires a camera. Real
 * acquisition, decodable encoder output, hardware timestamps and A/V
 * synchronisation are device evidence.
 */
internal sealed interface CaptureStart {
    /** Every step settled synchronously; both tracks are live. */
    data object Started : CaptureStart

    /** Asynchronous steps are in flight; [CaptureReady] follows through the session's route. */
    data object Pending : CaptureStart

    /**
     * Named. A permission or clock refusal happens before anything is
     * acquired; a submission that failed part-way through the declared order
     * leaves whatever had been acquired already unwound, and any step already
     * submitted still settles late into the closing ownership.
     */
    data class Refused(val reason: String) : CaptureStart
}

/** Capture readiness, delivered once, after asynchronous acquisition settles. */
internal sealed interface CaptureReady {
    data object Ready : CaptureReady

    /** An asynchronous step failed; what was acquired has already unwound. */
    data class Refused(val reason: String) : CaptureReady
}

/** How a capture session ended. */
internal sealed interface CaptureTermination {
    /** Both tracks delivered their terminal, or the user stopped a healthy session. */
    data object Completed : CaptureTermination

    /** The first fault that ended it; later faults do not replace it. */
    data class Failed(val fault: CaptureFault) : CaptureTermination

    data class Cancelled(val reason: String) : CaptureTermination
}

/** What the session tells its owner, in order, through one route. */
internal interface CaptureSessionListener {
    fun onReady(generation: Long, ready: CaptureReady)

    fun onTerminated(generation: Long, termination: CaptureTermination)

    /**
     * A track's handoff threw away [outputs] admitted outputs when it closed
     * failed, and said so once.
     *
     * It is defaulted, so a listener that does not care about discards need
     * not implement it.
     *
     * It carries ONE observed count per [MediaKind] for one generation, and
     * makes no ordering promise against the terminal: an
     * immediate failed-close witness can precede it, and an admitted callback
     * that settles afterwards produces one behind it. A terminal never waits
     * for a witness that may never come, because a track that discarded
     * nothing produces none at all.
     */
    fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) {}
}

/**
 * The session-owned record of what one generation's tracks threw away.
 *
 * It is a VALUE holder, bounded by construction: at most one observed count
 * per [MediaKind], for the single generation it was built for. It holds no
 * Activity, sender, endpoint, payload, coroutine scope or report callback, so
 * it stays valid after the broadcast's jobs and its UI have retired, and a
 * later integration can take an immutable snapshot from it without a reference
 * back to either.
 *
 * PRESENCE IS NOT VALUE. A kind absent from [snapshot] is UNOBSERVED -- not
 * zero, and not proof that its producers have settled or that anything was
 * physically disposed. A track that discarded nothing never witnesses, and
 * these counts describe handoff discards only.
 */
internal class CaptureDiscardRecord(val generation: Long) {
    private val lock = Any()
    private val observed = mutableMapOf<MediaKind, Int>()

    /**
     * Records one witness, and answers whether it was taken.
     *
     * Three things are refused, each WITHOUT changing anything: a foreign
     * generation; a second witness for a kind already observed, because the
     * handoff promises at most one and a duplicate is a caller defect rather
     * than new information; and a count that is not strictly positive, because
     * a handoff only witnesses when it actually threw output away -- zero is
     * not a fact it can produce, and a later legitimate first witness for that
     * kind is still accepted.
     */
    fun record(generation: Long, kind: MediaKind, outputs: Int): Boolean =
        synchronized(lock) {
            if (generation != this.generation) return false
            if (outputs <= 0) return false
            if (observed.containsKey(kind)) return false
            observed[kind] = outputs
            true
        }

    /**
     * A DETACHED read-only copy: a later record cannot change a snapshot
     * already taken. An absent kind is unobserved, never zero. This is a
     * Kotlin `Map` that the caller cannot mutate through, not a runtime
     * unmodifiable wrapper, and nothing here claims the latter.
     */
    fun snapshot(): Map<MediaKind, Int> = synchronized(lock) { observed.toMap() }
}

/**
 * The platform-facing driver: acquires resources against an ownership in a
 * DECLARED order, using `acquire` for created resources and `request` for
 * callback-settled ones. The real one talks to camera2 and the codecs; the host
 * one is scripted. Either way the session only sees the ownership's outcomes.
 */
internal interface CaptureAcquirer {
    /** The order every step will be taken in, so a partial failure has an expected prefix. */
    val order: List<CaptureResource>

    /**
     * Runs the steps. Returns the first synchronous failure, or `null` when
     * every synchronous step succeeded and any asynchronous steps are in
     * flight. Each asynchronous step settles through [ownership] on its
     * dispatcher, and the driver reports it through [steps] -- the production
     * route by which the session learns that `onOpened`/`onConfigured` (or
     * their failures) have arrived.
     *
     * A synchronous failure is the driver's own submission failing where it
     * stands: `CameraManager.openCamera` and `CameraDevice.createCaptureSession`
     * are declared to throw `CameraAccessException`, and the latter also
     * `IllegalStateException` once the device is closed. Steps already
     * SUBMITTED when that happens are not cancelled by it -- their callbacks
     * still arrive, and settle into an ownership that is by then closing.
     */
    fun begin(
        ownership: CaptureOwnership,
        steps: AcquisitionSteps,
    ): AcquireOutcome.Failed?
}

/**
 * An acquirer that also reports what happens to the devices it installed
 * after their acquisition settled -- a camera lost once it was open. It is
 * bound to the session's own fault route by assembly, before anything starts.
 */
internal interface CaptureFaultRouting {
    fun bindFaults(sink: CaptureFaultSink)
}

/**
 * The session's acquisition accounting, handed to the driver.
 *
 * The platform decides the shape. `CameraDevice.StateCallback.onOpened` is
 * where the camera "is ready to use, and CameraDevice#createCaptureSession can
 * be called to set up the first capture session" -- so the capture session
 * cannot be SUBMITTED until the camera has opened, and a submission therefore
 * appears from INSIDE the settlement of the step it depends on. Counting
 * outstanding steps after the driver returns cannot see that, and a count read
 * after `begin` has already raced with every callback delivered during it.
 *
 * Two rules make the accounting exact:
 *
 *  * every asynchronous step is [submitted] BEFORE anything can settle it --
 *    a dependent step is submitted first, and only then is the settlement that
 *    triggered it reported, so the outstanding count never dips through zero;
 *  * [settled] is reported exactly once per submitted step, whatever the
 *    outcome, so the count reaches zero exactly once and never goes negative.
 *
 * The session holds a STARTUP GUARD of its own across [CaptureAcquirer.begin].
 * A step that settles inside `begin`, or between `begin` returning and the
 * session publishing its state, therefore cannot drive the count to zero:
 * readiness follows the last real prerequisite, never an accidental zero, and
 * is never reported after a terminal decision. Both calls are made outside the
 * session's lock.
 */
internal interface AcquisitionSteps {
    /** An asynchronous step has been submitted; its callback has not arrived yet. */
    fun submitted(resource: CaptureResource)

    /** A submitted step settled, exactly once, with what the ownership decided. */
    fun settled(resource: CaptureResource, outcome: AdoptOutcome)
}

/**
 * The boundaries a test may hold a START at. All of them are OUTSIDE the
 * session's lock, and the defaults do nothing.
 *
 * [afterBegin] is the one window a caller cannot otherwise reach: the driver
 * has returned, every synchronous step is done, and the session has not yet
 * published its state. A settlement delivered there is exactly what the
 * startup guard exists for, so it has to be forceable rather than raced.
 *
 * [beforeReadiness] is the other: the last prerequisite has settled and the
 * readiness has not been decided yet. An ending that lands there must leave no
 * readiness behind it, and that too has to be forceable rather than raced.
 *
 * [afterStartupDecision] is where the startup's disposition has been taken and
 * the caller has not answered yet: a callback that settles the last
 * prerequisite there is a valid ordering, and what it decides must not turn
 * this start into a refusal.
 *
 * [afterFailureRecorded] is where a failure has been RECORDED and its cleanup
 * and announcement have not run yet. Nothing reaching a readiness decision
 * there may find a state that says the failed prerequisite succeeded.
 *
 * [afterDeliveryReleased] is scheduling instrumentation for the notification
 * route, not a startup boundary: a caller that owned the delivery has given
 * it up and has not yet let its delivery failure escape. Another thread may
 * retire the ending there, so whatever that caller throws must already be
 * final.
 *
 * [afterDrainReleased] and [afterDiscardQueued] are scheduling
 * instrumentation of the same kind: the first is where a delivery owner has
 * just given up the drain, the second where a discard notification has been
 * admitted and queued and its caller has not yet tried to deliver it.
 */
internal interface StartupSchedule {
    fun afterBegin() {}

    fun beforeReadiness() {}

    fun afterStartupDecision() {}

    fun afterFailureRecorded() {}

    fun afterDeliveryReleased() {}

    fun afterDrainReleased() {}

    fun afterDiscardQueued() {}

    companion object {
        val NONE: StartupSchedule = object : StartupSchedule {}
    }
}

/**
 * One capture session: two tracks, one ownership, one generation.
 */
internal interface CaptureSession : CaptureFaultSink {
    val generation: Long

    /** The two tracks, present from a successful [start] on. */
    val video: CaptureHandoff?

    val audio: CaptureHandoff?

    /** How this session ended, or `null` while it is running. */
    val termination: CaptureTermination?

    fun start(
        permissions: PermissionFlow,
        cameraFacts: CaptureCapability,
        audioFacts: CaptureCapability,
    ): CaptureStart

    /** A track drained its terminal; when both have, the session completes. */
    fun onTrackEnded(generation: Long, kind: MediaKind)

    /** The single idempotent ending; capture children close before the broadcast lifetime. */
    fun stop(primary: Exception?): Exception?

    /**
     * Whether ONE new publication operation -- an add, or starting the
     * drains -- may begin now: true only for this session's [generation] and
     * only while no ending has been claimed. It is decided atomically with the
     * ending's claim, not read from the later terminal delivery. A true answer
     * admits that one operation even if it executes or commits after a claim
     * that follows; it is not reused for the next one, and it is no promise
     * that no sender call physically runs after the claim.
     */
    fun admitPublication(generation: Long): Boolean

    /**
     * The receipt of the ending this session claimed, frozen once the claimant
     * has finished its closes, every admitted callback -- format or codec --
     * has released,
     * and every notification that ending queued has been attempted --
     * whichever thread claimed it, reported a cause, or owned the delivery.
     * Suspends until then, and until an ending is claimed at all: call it
     * only once one has been. Every call returns the same receipt.
     */
    suspend fun retirement(): RetirementReceipt
}

/**
 * What one claimed ending left behind, decided once and never changed: the
 * [termination]; the [causes] admitted callbacks reported -- the one that
 * claimed the ending first when one did, then any late one, at most one per
 * kind and role, each exactly as raised, and never a failure this ending
 * already holds as cleanup; and, separately, every distinct
 * [cleanup] failure -- the claimant's child and ownership closes, then the
 * ending's notification deliveries -- in the order they were found,
 * whichever thread found them.
 *
 * Each inventory is identity-unique WITHIN itself; the two describe different
 * facts, so one Throwable can be in both. A cause raised by a callback and
 * then raised again by the ending's own cleanup -- the claimant's close
 * notifying the same failing collaborator -- is once a cause and once a
 * cleanup fact. What is never done is the reverse: a cleanup failure the
 * ending already holds, rethrown into a callback boundary, is not recorded
 * again as a new cause. This is not a promise that the receipt as a whole
 * holds each Throwable once.
 *
 * It does not cover what has its own contract: a discard witnessed after this
 * was frozen is delivered and recorded as before, and resources an admitted
 * transfer still holds are disposed of later through the ownership's late
 * cleanup.
 */
internal class RetirementReceipt(val termination: CaptureTermination, val causes: List<Throwable>, val cleanup: List<Throwable>)

/**
 * Which platform callback boundary a key admits: a track's output-format
 * observations, or its codec's asynchronous callbacks (buffers, input, error).
 * Both are admitted by the SAME session authority and retired by the same
 * receipt; they differ only in the fault an overlap reports.
 */
internal enum class CallbackRole { Format, Codec }

/**
 * Minted once per (kind, [role]) by the session it was bound to; only this
 * exact object reserves for that kind and role there. The format-era name is
 * kept because format holders were its first role.
 */
internal class FormatHolderKey internal constructor(val kind: MediaKind, val role: CallbackRole = CallbackRole.Format)

/** One admitted callback of [key]'s holder, released exactly once and only by this identity. */
internal class FormatReservation internal constructor(val key: FormatHolderKey)

/** What a format callback's admission decided. */
internal sealed interface FormatAdmission {
    class Admitted(val reservation: FormatReservation) : FormatAdmission

    /** Not a key this session minted -- another session's, or one it never bound. Nothing changed. */
    data object NotBound : FormatAdmission

    /** The ending is claimed: no new callback is admitted. */
    data object Ended : FormatAdmission

    /** This kind already has an admitted callback; nothing was counted. */
    data object Overlapping : FormatAdmission
}

/** What releasing a reservation did. */
internal sealed interface FormatRelease {
    /** Recorded and balanced; [claimed] is the ending this release claimed, if it did. */
    data class Released(val claimed: CaptureTermination?) : FormatRelease

    /**
     * Not this session's active reservation -- a duplicate, a stale one, or
     * another session's. Nothing was recorded or removed, and [cause] is
     * still the caller's own.
     */
    data class Refused(val cause: Throwable?) : FormatRelease
}

/** The format-callback state this session holds right now, read under its lock. */
internal data class FormatInventory(val active: Set<MediaKind>, val causes: List<Throwable>, val termination: CaptureTermination?)

/**
 * The session side of format callbacks, decided under the session's own lock.
 *
 * **Holders.** [bindFormatHolder] mints ONE key per kind for the life of the
 * session; a second bind for a kind fails and changes nothing. Only that
 * exact key reserves for its kind here -- another session's key, or one this
 * session never minted, is [FormatAdmission.NotBound].
 *
 * **Admission.** [reserveFormat] admits at most one callback per kind, so at
 * most two at once; a same-kind overlap counts nothing, and the other kind is
 * independent. Nothing is admitted once the ending is claimed.
 *
 * **Release.** [releaseFormat] matches the exact active reservation. Its
 * [cause] -- anything the callback raised that is not a typed verdict -- is
 * recorded, identity-deduplicated, BEFORE the reservation is removed, in the
 * same critical section. A cause recorded before any ending claims one,
 * `Failed(Unexpected)`, and that release then does the claimant's work; a
 * callback admitted before another claim still reports its own late cause.
 * Structurally at most one cause per kind is ever recorded: the first one
 * claims the ending, after which nothing new is admitted, and at most one
 * callback per kind was admitted when it did.
 *
 * **Retirement.** The ending retires only once every admitted callback has
 * released; its receipt carries the recorded causes.
 */
internal interface FormatCallbackGate {
    fun bindFormatHolder(kind: MediaKind): FormatHolderKey

    fun reserveFormat(key: FormatHolderKey): FormatAdmission

    fun releaseFormat(reservation: FormatReservation, cause: Throwable?): FormatRelease

    fun formatInventory(): FormatInventory

    /**
     * Reports [key]'s track format fault, `CaptureFault.Format(kind, reason)`,
     * through this session's own fault route, outside every lock. A key this
     * session did not mint is a programming error and changes nothing.
     */
    fun reportFormat(key: FormatHolderKey, reason: String)
}

/** Every admitted callback, of every role, and the causes they reported, read under the session's lock. */
internal data class CallbackInventory(
    val active: Set<Pair<MediaKind, CallbackRole>>,
    val causes: List<Throwable>,
    val termination: CaptureTermination?,
)

/**
 * The same session authority as [FormatCallbackGate], for a track's codec
 * callbacks: one key per kind in the [CallbackRole.Codec] role, one active
 * reservation per key, identity-checked release, the cause recorded before
 * the reservation is removed, and the ending retired only once every admitted
 * callback -- of either role -- has released. A key of one role never
 * reserves or reports in the other.
 */
internal interface CodecCallbackGate {
    fun bindCodecCallbacks(kind: MediaKind): FormatHolderKey

    fun reserveCodec(key: FormatHolderKey): FormatAdmission

    fun releaseCodec(reservation: FormatReservation, cause: Throwable?): FormatRelease

    /**
     * Reports [key]'s track codec fault, `CaptureFault.Codec(kind, reason)`,
     * through this session's own fault route, outside every lock. A key this
     * session did not mint in this role is a programming error and changes
     * nothing.
     */
    fun reportCodec(key: FormatHolderKey, reason: String)

    fun callbackInventory(): CallbackInventory

    /**
     * Whether this session's ending already holds [failure] -- as one of its
     * causes or as one of its cleanup facts. A callback boundary asks this
     * of what a release or a report raised: such a failure is the receipt's
     * already, and is neither a second cause nor the callback's to rethrow.
     */
    fun owns(failure: Throwable): Boolean
}

/** One bounded decision whether a new publication operation may begin: see [CaptureSession.admitPublication]. */
internal fun interface PublicationAdmission {
    fun admit(): Boolean
}

/** The admission authority of THIS session, for its own generation. */
internal fun CaptureSession.publicationAdmission(): PublicationAdmission = PublicationAdmission { admitPublication(generation) }

/**
 * The listener a session is BUILT WITH when its discards are to be recorded.
 *
 * It is the owning edge between the session's notification route and the
 * session-owned [CaptureDiscardRecord]: readiness and termination are passed
 * to whatever observer the owner supplied, and every discard fact the session
 * routes is WRITTEN INTO the record first, whether or not an observer is still
 * listening.
 *
 * **Lifetimes.** `PlatformCaptureSession` retains this object for as long as it
 * lives, and this object retains the record -- which holds no Activity, sender,
 * endpoint, payload, scope or report callback. The OBSERVER is the only thing
 * that can hold those, and it is the only thing [retire] drops. A snapshot is
 * read from the record itself, never through this listener.
 *
 * **What [retire] is, exactly.** It is a small non-blocking DETACHMENT: after
 * its locked clear, no NEW observer selection can happen. A callback already
 * selected before that moment may still start, still run and still hold the
 * observer until it returns, so `retire` returning is NOT a join, not a
 * quiescence barrier, and not proof that no observer call is in flight. It
 * disposes nothing. It is safe to call reentrantly and more than once, it
 * never holds the lock across a user callback, and it CANNOT stop the record
 * from being written -- persistent recording is independent of it.
 *
 * **Downstream validation.** A fact the record refuses -- a foreign
 * generation, a duplicate for a kind already observed, a non-positive count --
 * is dropped here too, and nothing is queued for a retry. This is DEFENSIVE
 * only: it runs after the session has already dequeued the notification, so it
 * is not what bounds that queue. The session's own admission state is.
 */
internal class RecordingCaptureListener(
    private val record: CaptureDiscardRecord,
    observer: CaptureSessionListener,
) : CaptureSessionListener {
    private val lock = Any()
    private var observer: CaptureSessionListener? = observer

    /** Drops the observer. The record keeps recording. */
    fun retire() {
        synchronized(lock) { observer = null }
    }

    private fun observer(): CaptureSessionListener? = synchronized(lock) { observer }

    override fun onReady(generation: Long, ready: CaptureReady) {
        observer()?.onReady(generation, ready)
    }

    override fun onTerminated(generation: Long, termination: CaptureTermination) {
        observer()?.onTerminated(generation, termination)
    }

    override fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) {
        // The record first, and only a fact it ACCEPTED is passed on.
        if (!record.record(generation, kind, outputs)) return
        observer()?.onDiscarded(generation, kind, outputs)
    }
}

/**
 * The session this sample runs: two tracks, one ownership, one generation.
 *
 * **Starting.** Permission readiness and BOTH capability facts are read before
 * anything is acquired, so a refusal owns nothing. A start is claimed exactly
 * once: a second start, a concurrent one, and a start after the session has
 * ended are all [CaptureStart.Refused] by name, and none of them re-enters the
 * driver.
 *
 * **Accounting.** Submitted steps are tracked BY RESOURCE, not as a total: a
 * settlement reported for a step that was never submitted -- a duplicate, or
 * one resource reported for another -- cannot spend a different step's
 * outstanding entry, and is refused where it is reported. The startup guard is
 * a flag of its own rather than a phantom entry, so no settlement can consume
 * it, and readiness follows the last real prerequisite.
 *
 * **Saying it.** Readiness and the ending are DECIDED under this session's
 * lock and DELIVERED outside it, in decision order, by one delivery owner at a
 * time. A listener that calls back in -- or another thread that decides
 * something while a callback is running -- queues its event and returns, so
 * nothing is delivered recursively and no lock is held across a callback. A
 * notification that throws is attempted once, never re-queued, and does not
 * stop the remaining deliveries or the ownership of the drain.
 *
 * **Ending.** The terminal decision happens under the lock; both tracks and the
 * ownership are closed after it is released. Every mandatory close is attempted
 * even when an earlier one throws: the primary keeps its identity and carries
 * what the cleanup found, and with no primary the first cleanup failure escapes
 * rather than disappearing. Outstanding acquisitions and admitted transfers
 * settle through the ownership; this session never waits for one.
 */
internal class PlatformCaptureSession(
    override val generation: Long,
    val ownership: CaptureOwnership,
    val acquirer: CaptureAcquirer,
    val videoTrack: CaptureHandoff,
    val audioTrack: CaptureHandoff,
    val listener: CaptureSessionListener,
    val schedule: StartupSchedule = StartupSchedule.NONE,
) : CaptureSession, FormatCallbackGate, CodecCallbackGate {
    private val lock = Any()

    /** Submitted steps that have not settled, BY RESOURCE. The guard is not one of these. */
    private val outstandingSteps = mutableMapOf<CaptureResource, Int>()

    /** Which tracks delivered their terminal. A kind, not a tally: the same track twice is once. */
    private val endedTracks = mutableSetOf<MediaKind>()

    /**
     * Which tracks' discard witnesses have been ADMITTED. At most one per
     * kind, for the life of this session: it is never cleared, so a duplicate
     * can never be queued, whenever it arrives.
     */
    private val admittedDiscards = mutableSetOf<MediaKind>()

    /** Decided events, in decision order, delivered outside the lock in that order. */
    private val outbox = ArrayDeque<() -> Unit>()
    private var startClaimed = false
    private var started = false
    private var guarded = false
    private var readyReported = false

    /** Whether a SUCCESSFUL readiness was decided; [readyReported] also covers a refusal. */
    private var readySucceeded = false
    private var decided: CaptureTermination? = null
    private var delivering = false

    /** The ending's cleanup failures, recorded as they are found; frozen into [receipt]. */
    private val endingFacts = mutableListOf<Throwable>()

    /** Whether the claimant has recorded its closes and queued the terminal. */
    private var claimantDone = false
    private var receipt: RetirementReceipt? = null

    /** The one key minted per (kind, role), and the one admitted callback per (kind, role): at most four. */
    private val boundKeys = mutableMapOf<Pair<MediaKind, CallbackRole>, FormatHolderKey>()
    private val active = mutableMapOf<Pair<MediaKind, CallbackRole>, FormatReservation>()

    /**
     * The unexpected causes admitted callbacks reported, in order, each with
     * the role that reported it; at most one per (kind, role).
     */
    private val causes = mutableListOf<Pair<CallbackRole, Throwable>>()
    private val retired = CompletableDeferred<RetirementReceipt>()

    override val video: CaptureHandoff? get() = synchronized(lock) { if (started) videoTrack else null }

    override val audio: CaptureHandoff? get() = synchronized(lock) { if (started) audioTrack else null }

    override val termination: CaptureTermination? get() = synchronized(lock) { decided }

    override fun start(
        permissions: PermissionFlow,
        cameraFacts: CaptureCapability,
        audioFacts: CaptureCapability,
    ): CaptureStart {
        when (val readiness = permissions.readiness()) {
            is CaptureReadiness.Refused -> return CaptureStart.Refused(readiness.reason)
            CaptureReadiness.Waiting -> return CaptureStart.Refused("a permission is still outstanding")
            CaptureReadiness.Ready -> Unit
        }
        if (cameraFacts is CaptureCapability.Unavailable) return CaptureStart.Refused(cameraFacts.reason)
        if (audioFacts is CaptureCapability.Unavailable) return CaptureStart.Refused(audioFacts.reason)
        val refusal =
            synchronized(lock) {
                when {
                    decided != null -> "the session has already ended"
                    startClaimed -> "this session has already been started"
                    else -> {
                        startClaimed = true
                        guarded = true
                        null
                    }
                }
            }
        if (refusal != null) return CaptureStart.Refused(refusal)
        // Whatever the driver's own submission raises -- a checked
        // CameraAccessException, an unchecked one, an Error, a cancellation --
        // the startup unwinds completely and that failure is what leaves.
        var raised: Throwable? = null
        val failed =
            try {
                acquirer.begin(ownership, steps)
            } catch (cancelled: CancellationException) {
                raised = cancelled
                throw cancelled
            } catch (failure: Exception) {
                raised = failure
                throw failure
            } catch (error: Error) {
                raised = error
                throw error
            } finally {
                raised?.let { unwindStartup(it) }
            }
        if (failed != null) {
            // Steps already SUBMITTED are not cancelled by this: their
            // callbacks still arrive and settle into the closing ownership,
            // which is what unwinds whatever they carry. This is a REPORTED
            // failure, not a thrown one: it leaves as the named refusal.
            endStartup(failed.reason)
            return CaptureStart.Refused(failed.reason)
        }
        schedule.afterBegin()
        // ONE protected decision, taken with the guard released in the same
        // critical section: an ending that already won ends this startup, a
        // live startup with prerequisites left is pending and publishes its
        // tracks, and only an otherwise complete one goes on to decide the
        // readiness. Nothing here is sampled twice.
        val disposition =
            synchronized(lock) {
                guarded = false
                when {
                    decided != null -> Disposition.Ended
                    outstandingLocked() > 0 -> {
                        started = true
                        Disposition.Pending
                    }
                    else -> Disposition.Decide
                }
            }
        schedule.afterStartupDecision()
        return when (disposition) {
            Disposition.Ended -> CaptureStart.Refused(ENDED_BEFORE_READY)
            // A callback that settles the last prerequisite right here has
            // decided a real readiness; this start was pending when it was
            // decided, and that is what it says.
            Disposition.Pending -> CaptureStart.Pending
            Disposition.Decide -> {
                reportReady()
                // The disposition is the STATE -- whether a SUCCESSFUL
                // readiness was decided, by whoever decided it -- never
                // whether this call was the one that queued the notification.
                // A readiness whose listener then stopped the session still
                // succeeded; a refused readiness is not a successful one.
                if (synchronized(lock) { readySucceeded }) CaptureStart.Started else CaptureStart.Refused(ENDED_BEFORE_READY)
            }
        }
    }

    /** What the one protected startup decision decided. */
    private enum class Disposition { Ended, Pending, Decide }

    /** The accounting the driver reports through. Neither call runs under this session's lock. */
    private val steps =
        object : AcquisitionSteps {
            override fun submitted(resource: CaptureResource) {
                synchronized(lock) { outstandingSteps[resource] = (outstandingSteps[resource] ?: 0) + 1 }
            }

            override fun settled(resource: CaptureResource, outcome: AdoptOutcome) =
                this@PlatformCaptureSession.settled(resource, outcome)
        }

    private fun outstandingLocked(): Int = outstandingSteps.values.sum()

    /**
     * A submitted step settled. The last successful one makes the session
     * ready; a failure refuses the startup. A settlement for a step this
     * session never submitted is refused where it is reported, and spends
     * neither another step's entry nor the guard.
     */
    private fun settled(resource: CaptureResource, outcome: AdoptOutcome) {
        val reason = if (outcome is AdoptOutcome.Failed) "$resource: ${outcome.reason}" else null
        var mine: CaptureTermination? = null
        val ready =
            synchronized(lock) {
                val open = outstandingSteps[resource] ?: 0
                check(open > 0) { "$resource settled a step that was never submitted" }
                if (open == 1) outstandingSteps.remove(resource) else outstandingSteps[resource] = open - 1
                if (reason != null) {
                    // The failure is RECORDED in the same step that consumes
                    // the prerequisite. There is no instant in which the
                    // accounting says this step is done and the session does
                    // not know it failed, so nothing can read that zero as a
                    // readiness.
                    val termination = CaptureTermination.Cancelled(reason)
                    if (claimLocked(termination)) mine = termination
                    false
                } else {
                    outcome == AdoptOutcome.Adopted && outstandingLocked() == 0 && !guarded && decided == null
                }
            }
        if (reason != null) schedule.afterFailureRecorded()
        when (outcome) {
            AdoptOutcome.Adopted -> if (ready) reportReady()
            // Recorded under the lock; the cleanup and the announcement happen
            // here, outside it, by the thread that recorded it.
            is AdoptOutcome.Failed ->
                mine?.let { complete(it, CloseMode.Failed, null, CaptureReady.Refused(checkNotNull(reason))) }
            is AdoptOutcome.Retired, is AdoptOutcome.Unknown -> Unit
        }
    }

    /** The startup ended before it was ready: unwind, then say so, once, in that order. */
    private fun endStartup(reason: String) = failStartup(CaptureTermination.Cancelled(reason), null)

    /**
     * The driver threw. The unwind is the same transaction, with that failure
     * as the primary: it keeps its identity, carries every distinct cleanup
     * failure the unwind finds, and is rethrown by the caller unchanged.
     */
    private fun unwindStartup(primary: Throwable) =
        failStartup(CaptureTermination.Cancelled(primary.message ?: "the acquisition threw where it stood"), primary)

    /**
     * A startup failure the session ACCEPTED: the guard is released and the
     * terminal recorded in one critical section, so the callback that settles
     * the last prerequisite in the gap cannot find a live startup with nothing
     * outstanding and announce a readiness. The cleanup and the announcement
     * are then done, outside the lock, by the thread that recorded it.
     */
    private fun failStartup(termination: CaptureTermination, primary: Throwable?) {
        val mine =
            synchronized(lock) {
                guarded = false
                claimLocked(termination)
            }
        schedule.afterFailureRecorded()
        if (mine) complete(termination, CloseMode.Failed, primary, refusal = null)
    }

    /**
     * Readiness is a decision about prerequisites: never after a terminal one,
     * never twice. The decision and its place in the notification order are
     * taken in ONE critical section -- marking it first and queueing it
     * afterwards would let an ending decide, queue and deliver in between, and
     * the readiness would then arrive behind the ending it preceded.
     */
    private fun reportReady(): Boolean {
        schedule.beforeReadiness()
        val queued =
            synchronized(lock) {
                if (readyReported || decided != null) {
                    false
                } else {
                    readyReported = true
                    // The tracks become visible with the readiness that
                    // decided they are live, not before it.
                    started = true
                    readySucceeded = true
                    outbox.addLast { listener.onReady(generation, CaptureReady.Ready) }
                    true
                }
            }
        if (queued) deliver()
        return queued
    }

    override fun onTrackEnded(generation: Long, kind: MediaKind) {
        if (generation != this.generation) return
        // BOTH kinds, so the same track twice completes nothing.
        val complete = synchronized(lock) { endedTracks.add(kind); endedTracks.size == 2 && decided == null }
        if (complete) finish(CaptureTermination.Completed, CloseMode.Completed, null, refusal = null)
    }

    /**
     * A track's typed fault. [CaptureFault.Unexpected] is not one: it is what
     * an accepted callback release claims with, carrying a cause the
     * session records. Submitted here it is refused before anything is read
     * or changed -- for any generation, before or after a claim -- and its
     * Throwable goes back to the caller as the refusal's cause, never
     * recorded or dropped.
     */
    override fun onFault(generation: Long, fault: CaptureFault) {
        if (fault is CaptureFault.Unexpected) {
            throw IllegalArgumentException("an unexpected ${fault.kind} cause is reported by releasing its admitted callback, not as a fault", fault.cause)
        }
        if (generation != this.generation) return
        finish(CaptureTermination.Failed(fault), CloseMode.Failed, null, refusal = null)
    }

    /**
     * The track witnessed the outputs a failed close threw away, and this is
     * where that fact becomes the owner's.
     *
     * **Admission.** It happens HERE, under this session's own lock, and
     * decides in ONE step whether the fact may be queued at all: the
     * generation must be this one, the count must be positive -- a handoff
     * only witnesses when it really threw output away -- and it must be the
     * FIRST for that kind. Marking the kind and enqueuing the notification are
     * the same critical section, so a burst of duplicates arriving while the
     * first is still being delivered queues nothing: the bound is two facts
     * per session, held in the state this lock already guards.
     *
     * **Delivery.** The notification is attempted through the SAME outbox and
     * single drain owner as readiness and termination: outside the lock, in
     * order, once, with failures collected rather than swallowed and no retry.
     * A listener is never called while the admission lock is held.
     *
     * **Lateness.** This route is not gated on the terminal, on readiness, on
     * a coordinator or on any observer being present. A genuine witness that
     * settles after the first terminal is still admitted, and the admitted
     * kinds are never reset -- not on terminal, not on retirement.
     *
     * Admission is not observation: the fact becomes OBSERVED when the
     * delivered callback is accepted by the recipient's record. Nothing here
     * invents a zero, and none of it says anything about physical cleanup or
     * about producers having settled.
     */
    override fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) {
        val admitted =
            synchronized(lock) {
                if (generation != this.generation) return
                if (outputs <= 0) return
                if (!admittedDiscards.add(kind)) return
                outbox.addLast { listener.onDiscarded(generation, kind, outputs) }
                true
            }
        if (admitted) {
            schedule.afterDiscardQueued()
            deliver()
        }
    }

    override fun admitPublication(generation: Long): Boolean = synchronized(lock) { generation == this.generation && decided == null }

    override suspend fun retirement(): RetirementReceipt = retired.await()

    override fun bindFormatHolder(kind: MediaKind): FormatHolderKey = bindKey(kind, CallbackRole.Format)

    override fun bindCodecCallbacks(kind: MediaKind): FormatHolderKey = bindKey(kind, CallbackRole.Codec)

    private fun bindKey(kind: MediaKind, role: CallbackRole): FormatHolderKey =
        synchronized(lock) {
            val slot = kind to role
            check(boundKeys[slot] == null) { "a $kind ${role.name.lowercase()} callback holder is already bound to this session" }
            FormatHolderKey(kind, role).also { boundKeys[slot] = it }
        }

    override fun reserveFormat(key: FormatHolderKey): FormatAdmission = reserve(key, CallbackRole.Format)

    override fun reserveCodec(key: FormatHolderKey): FormatAdmission = reserve(key, CallbackRole.Codec)

    private fun reserve(key: FormatHolderKey, role: CallbackRole): FormatAdmission =
        synchronized(lock) {
            val slot = key.kind to role
            when {
                key.role != role || boundKeys[slot] !== key -> FormatAdmission.NotBound
                decided != null -> FormatAdmission.Ended
                active[slot] != null -> FormatAdmission.Overlapping
                else -> FormatAdmission.Admitted(FormatReservation(key).also { active[slot] = it })
            }
        }

    /**
     * Records [cause] and releases [reservation] in ONE critical section, then
     * -- outside the lock -- does the claimant's work if this release claimed
     * the ending, or lets the receipt freeze if it was the last admitted
     * callback. A cleanup failure that claimant work raises escapes to this
     * caller after the reservation is balanced and every fact is recorded.
     */
    override fun releaseFormat(reservation: FormatReservation, cause: Throwable?): FormatRelease = release(reservation, cause, CallbackRole.Format)

    override fun releaseCodec(reservation: FormatReservation, cause: Throwable?): FormatRelease = release(reservation, cause, CallbackRole.Codec)

    private fun release(reservation: FormatReservation, cause: Throwable?, role: CallbackRole): FormatRelease {
        val kind = reservation.key.kind
        val slot = kind to role
        val claimed =
            synchronized(lock) {
                if (reservation.key.role != role || boundKeys[slot] !== reservation.key || active[slot] !== reservation) return FormatRelease.Refused(cause)
                var mine: CaptureTermination? = null
                // A cause is recorded once, whichever role reports it, and a
                // cleanup failure the ending already holds is not a cause.
                if (cause != null && causes.none { it.second === cause } && endingFacts.none { it === cause }) {
                    causes.add(role to cause)
                    val ending = CaptureTermination.Failed(CaptureFault.Unexpected(kind, cause))
                    if (claimLocked(ending)) mine = ending
                }
                active.remove(slot)
                mine
            }
        if (claimed != null) complete(claimed, CloseMode.Failed, null, refusal = null) else retireIfDue()
        return FormatRelease.Released(claimed)
    }

    /** The format role's own view, as documented: its admitted kinds and the causes format callbacks reported. */
    override fun formatInventory(): FormatInventory =
        synchronized(lock) {
            FormatInventory(
                active.keys.filter { it.second == CallbackRole.Format }.map { it.first }.toSet(),
                causes.filter { it.first == CallbackRole.Format }.map { it.second },
                decided,
            )
        }

    override fun callbackInventory(): CallbackInventory = synchronized(lock) { CallbackInventory(active.keys.toSet(), causes.map { it.second }, decided) }

    override fun owns(failure: Throwable): Boolean = synchronized(lock) { causes.any { it.second === failure } || endingFacts.any { it === failure } }

    override fun reportFormat(key: FormatHolderKey, reason: String) {
        require(synchronized(lock) { key.role == CallbackRole.Format && boundKeys[key.kind to CallbackRole.Format] === key }) { "a ${key.kind} format key this session did not mint" }
        onFault(generation, CaptureFault.Format(key.kind, reason))
    }

    override fun reportCodec(key: FormatHolderKey, reason: String) {
        require(synchronized(lock) { key.role == CallbackRole.Codec && boundKeys[key.kind to CallbackRole.Codec] === key }) { "a ${key.kind} codec key this session did not mint" }
        onFault(generation, CaptureFault.Codec(key.kind, reason))
    }

    override fun stop(primary: Exception?): Exception? {
        val termination =
            when (primary) {
                null -> CaptureTermination.Completed
                is CancellationException -> CaptureTermination.Cancelled(primary.message ?: "cancelled")
                else -> CaptureTermination.Cancelled(primary.message ?: "failed")
            }
        val mode = if (primary == null) CloseMode.Completed else CloseMode.Failed
        return finish(termination, mode, primary, refusal = null)
    }

    /**
     * The one ending: decided under the lock, closed outside it, announced
     * after the children have reached their boundary. A readiness decided
     * before this point was queued while [decided] was still null, so it is
     * already ahead of this in the order.
     */
    private fun finish(
        termination: CaptureTermination,
        mode: CloseMode,
        primary: Throwable?,
        refusal: CaptureReady.Refused?,
    ): Exception? {
        val first = synchronized(lock) { claimLocked(termination) }
        if (!first) return primary as? Exception
        return complete(termination, mode, primary, refusal)
    }

    /** Records the one terminal decision. The lock is held; no work is done here. */
    private fun claimLocked(termination: CaptureTermination): Boolean {
        if (decided != null) return false
        decided = termination
        return true
    }

    /**
     * The work the recorded terminal owes: close the children, announce it
     * once, carry what the cleanup found. Only the thread that recorded the
     * decision runs this, and it always does.
     */
    private fun complete(
        termination: CaptureTermination,
        mode: CloseMode,
        primary: Throwable?,
        refusal: CaptureReady.Refused?,
    ): Exception? {
        val cleanup = mutableListOf<Throwable>()
        val ending = closeChildren(mode, primary, cleanup)
        synchronized(lock) {
            if (refusal != null && !readyReported) {
                readyReported = true
                outbox.addLast { listener.onReady(generation, refusal) }
            }
            outbox.addLast { listener.onTerminated(generation, termination) }
            // What the closes found is the ending's, recorded in the same step
            // that queues its terminal -- including the ownership's report,
            // which a claimant whose caller ignores the return would lose.
            cleanup.forEach { recordLocked(it) }
            if (ending != null && ending !== primary) recordLocked(ending)
        }
        cleanup.addAll(drain())
        // A failure the ownership REPORTED is returned when nothing else
        // escapes. When something does, the return value is unreachable, so it
        // is carried rather than lost.
        if (ending != null && ending !== primary && cleanup.isNotEmpty() && cleanup.none { it === ending }) {
            cleanup.add(ending)
        }
        // The claimant's own failure graph is complete before the receipt can
        // be frozen; the receipt is signalled outside the lock, and a waiter
        // resumed inline there finds every fact already in place.
        val escaping = carrier(primary, cleanup)
        synchronized(lock) { claimantDone = true }
        retireIfDue()
        escaping?.let { throw it }
        return ending
    }

    /** Records one of the ending's failures, once, while its receipt is not yet frozen. The lock is held. */
    private fun recordLocked(failure: Throwable) {
        if (receipt == null && endingFacts.none { it === failure }) endingFacts.add(failure)
    }

    /**
     * Freezes the receipt once nothing more can reach it -- the ending is
     * claimed, the claimant has finished, every admitted callback of either
     * role has released, and no notification is queued or being delivered -- and
     * signals it outside the lock. Called by the claimant, by every drain
     * owner as it finishes and by every released callback, so whichever of
     * them is last freezes it, once. No caller waits here.
     */
    private fun retireIfDue() {
        val frozen =
            synchronized(lock) {
                val ending = decided
                if (receipt != null || ending == null || !claimantDone || active.isNotEmpty() || delivering || outbox.isNotEmpty()) return
                RetirementReceipt(ending, causes.map { it.second }, endingFacts.toList()).also { receipt = it }
            }
        retired.complete(frozen)
    }

    /**
     * Delivers what is queued, then lets the receipt freeze, then lets the
     * first delivery failure escape. That failure's graph was finished while
     * this caller still owned the delivery, so nothing is attached to it once
     * the ending can retire.
     */
    private fun deliver() {
        val failures = drain(carrying = true)
        schedule.afterDeliveryReleased()
        retireIfDue()
        failures.firstOrNull()?.let { throw it }
    }

    /**
     * The children, in order, outside the lock, EVERY one attempted. The codec
     * is not disposed here: it is one of the ownership's resources, and closing
     * that handle asks its lifetime for a disposal that happens when the last
     * transfer settles.
     */
    private fun closeChildren(mode: CloseMode, primary: Throwable?, cleanup: MutableList<Throwable>): Exception? {
        attempt { videoTrack.close(mode) }?.let { cleanup.add(it) }
        attempt { audioTrack.close(mode) }?.let { cleanup.add(it) }
        // The ownership's close carries an Exception; an Error primary is
        // carried here instead. What that close REPORTS is then a failure with
        // nowhere to be returned, so it is carried too rather than dropped.
        val carried = primary as? Exception
        var ending = carried
        attempt { ending = ownership.close(carried) }?.let { cleanup.add(it) }
        val reported = ending
        if (primary != null && carried == null && reported != null) cleanup.add(reported)
        return ending
    }

    /**
     * Delivers what has been decided, in decision order, OUTSIDE the lock.
     *
     * ONE drain owner at a time; anyone else queues and returns. Each
     * notification is taken off the queue before it is attempted, so one that
     * threw is never delivered a second time, and its failure neither ends the
     * drain nor keeps the ownership of it.
     *
     * Taking the next notification and giving up the drain are ONE decision:
     * the owner that finds the queue empty releases the drain in that same
     * step and returns, and never takes it back. A notification queued while
     * this owner holds the drain is therefore delivered by it; one queued
     * after the release is delivered by the caller that queued it, which
     * finds the drain free. Nothing an owner collected is ever extended once
     * it has let go.
     *
     * [carrying] is for a caller that will throw the first failure itself:
     * each later failure is attached to that first one here, while this owner
     * still holds the drain, rather than after it has let go.
     */
    private fun drain(carrying: Boolean = false): List<Throwable> {
        val failures = mutableListOf<Throwable>()
        if (!synchronized(lock) { if (delivering) false else { delivering = true; true } }) return failures
        var owned = true
        try {
            while (true) {
                val next =
                    synchronized(lock) {
                        outbox.removeFirstOrNull().also {
                            if (it == null) {
                                delivering = false
                                owned = false
                            }
                        }
                    } ?: break
                attempt { next() }?.let { failure ->
                    // Carried and recorded BEFORE this owner gives up the
                    // drain, so no one can find the ending retired without
                    // this failure, or with its caller's graph unfinished.
                    if (carrying) failures.firstOrNull()?.let { first -> if (first !== failure && first.suppressed.none { it === failure }) first.addSuppressed(failure) }
                    failures.add(failure)
                    synchronized(lock) { if (decided != null) recordLocked(failure) }
                }
            }
        } finally {
            // Only a failure escaping the attempt itself gets here still owning.
            if (owned) synchronized(lock) { delivering = false }
        }
        schedule.afterDrainReleased()
        return failures
    }

    /**
     * One cleanup attempt whose failure is COLLECTED rather than allowed to end
     * the cleanup around it, or to replace what is already leaving.
     */
    private fun attempt(action: () -> Unit): Throwable? =
        try {
            try {
                action()
                null
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (failure: Exception) {
                failure
            } catch (error: Error) {
                error
            }
        } catch (cancelled: CancellationException) {
            cancelled
        }

    /**
     * The primary keeps its identity and carries what the cleanup found. With
     * no primary the first cleanup failure carries the rest and is returned,
     * for the caller to throw once it has finished its own bookkeeping, rather
     * than disappearing.
     */
    private fun carrier(primary: Throwable?, cleanup: List<Throwable>): Throwable? {
        if (primary != null) {
            val attached = mutableListOf<Throwable>()
            for (other in cleanup) {
                if (other === primary || attached.any { it === other }) continue
                attached.add(other)
                primary.addSuppressed(other)
            }
            return null
        }
        val carrier = cleanup.firstOrNull() ?: return null
        for (other in cleanup) if (other !== carrier) carrier.addSuppressed(other)
        return carrier
    }

    private companion object {
        /** The named refusal for a startup that a terminal decision ended first. */
        const val ENDED_BEFORE_READY = "the session ended before the startup was ready"
    }
}

/**
 * One codec's platform edges, owned together because they are one codec.
 *
 * [lifetime] is the SAME object the track's handoff leases and the resource
 * handle the acquirer installs in the ownership closes: a handoff left on
 * `CodecLifetime.RETAINED` would grant every lease and never defer a disposal,
 * and two independent lifetime owners for one codec would each think they were
 * the last.
 */
internal class TrackCodec(
    val lifetime: CodecLifetime,
    val buffers: OutputBufferAccess,
    val indices: CodecBufferOwner,
    /**
     * The codec's callback boundary, when this codec delivers its own
     * callbacks: the deferred composition binds it to the session, the
     * track's handoff and its format holder BEFORE anything starts.
     */
    val binding: EncoderBinding? = null,
)

/**
 * Everything one live broadcast needs that this sample does not own: the
 * platform's acquisition driver, the handler its callbacks arrive on, the sink
 * for cleanup facts that arrive after a close has returned, and one
 * [TrackCodec] per kind.
 *
 * Every member is an ALREADY ACCEPTED seam. On the platform they are
 * `PlatformCapture.of`'s: the [SequencedAcquirer] over [CameraPart] and
 * [AudioPart], the owned [HandlerDispatcher], and one [TrackEncoder] per kind;
 * host tests supply scripted ones.
 */
internal class CaptureEnvironment(
    val acquirer: CaptureAcquirer,
    val dispatcher: CallbackDispatcher,
    val late: LateCleanupSink,
    val video: TrackCodec,
    val audio: TrackCodec,
)

/** The two narrow forwarding boundaries to the sender, plus the completion half. */
internal class PublishSinks(val video: TrackSink, val audio: TrackSink, val completion: CompletionSink)

/**
 * Everything one track's platform driver needs, typed, with nothing invented.
 *
 * A driver reports a completed access unit by OBSERVING it on [ledger] -- the
 * accepted correlation route, which is what supplies the capture instant a
 * published object carries -- and then delivering the codec callback to
 * [handoff]. Video observes under its own presentation time; audio feeds its
 * PCM reads to [keying] and observes each completed unit under the key that
 * route returns, so a unit's timestamp comes from the frame count rather than
 * from the clock it was noticed on. [keying] is null for video, which has no
 * frame-count step. Nothing here derives a timestamp or a flag on the driver's
 * behalf.
 *
 * For audio this is the EXACT instance the caller supplied: a broadcast that
 * replaced it would place its units on a second, independently advancing unit
 * counter.
 */
internal class TrackInputs(
    val kind: MediaKind,
    val generation: Long,
    val ledger: CaptureLedger,
    val keying: AudioKeying?,
    val handoff: CaptureHandoff,
)

/**
 * What assembly produced: the typed inputs the platform driver needs, the
 * session-owned record, and the ONE run.
 *
 * **Assembly acquires nothing.** It builds bounded in-memory state only; the
 * platform is first touched inside [run], by the accepted session's `start`.
 * So if assembly itself fails, or if [run] is never called, there is no
 * acquired resource to own -- only objects the caller drops. If [run] is
 * called twice the accepted session refuses the second by name and that call
 * returns an `Unfinished` outcome having touched nothing; it is not a second
 * broadcast.
 */
internal class LiveCapture(
    val video: TrackInputs,
    val audio: TrackInputs,
    val record: CaptureDiscardRecord,
    val run: suspend () -> BroadcastOutcome,
)

/**
 * Assembles one live broadcast from the accepted pieces, in the one order
 * their contracts force, and hands back what the platform needs.
 *
 * ORDER, and why: the record and the coordinator's [BroadcastEvents] first,
 * because [RecordingCaptureListener] must be the listener the session is
 * CONSTRUCTED with; the fault relay next, bound to the session the moment the
 * session exists and BEFORE anything is started, so no callback can arrive at
 * an unbound sink; the ownership and both handoffs over their own ledgers and
 * their own [TrackCodec.lifetime]; the session; then one [PublishTimeline] per
 * track over the SAME immutable origin and the one calibration, because that
 * component is not safe to share; then the lanes and the accepted
 * [LiveBroadcast].
 *
 * **References it keeps.** The persistent listener retains the record and the
 * events, and neither holds an Activity, a reporter, a sender, an endpoint, a
 * scope or a payload. Scoped progress reporting stays with the broadcast job
 * through [report], which the caller owns and retires.
 *
 * **Generations.** [generation], [permissions] and [record] must agree; that
 * is checked here, so a record built for another generation cannot silently
 * refuse every fact it is later given.
 *
 * **The audio keying.** [audioKeying] is supplied, not inferred: its sample
 * rate and unit geometry are facts about the audio the caller configured, and
 * neither a permission nor a requested codec preset carries them. That exact
 * instance is retained in the audio [TrackInputs], it belongs to this one
 * broadcast and is not reused across generations, and assembly does not
 * advance its read state -- the first `onRead` a driver makes is the first one
 * it has seen. Its [AudioKeying.originUs] must equal [origin], because the two
 * place the same track on one timeline; a disagreement is refused here with
 * `the audio keying is keyed to origin <its>, not the broadcast origin <this>`
 * rather than silently publishing audio a fixed distance from its video. Its
 * [AudioKeying.sampleRate] and [AudioKeying.framesPerUnit] must equal the
 * validated AAC facts [audio] carries -- the rate and the access-unit length
 * the configuration itself declares -- and a disagreement is refused by name
 * too. These are three field comparisons: the supplied keying is not asked
 * for a key or fed a read, and it is retained as the SAME object with its
 * state, never replaced by one derived from the facts.
 *
 * **The formats.** [video] and [audio] are the tracks' VALIDATED formats;
 * each lane's pipeline converts and compares against its own. The converted
 * payload is bounded by [maxNalUnits] and by the same numeric [maxOutputBytes]
 * the handoffs enforce on what the encoder hands over.
 *
 * It creates no resource of its own and adds no lifecycle: refusal,
 * cancellation, ending and cleanup are the accepted session's and
 * coordinator's, unchanged.
 */
internal fun composeLiveBroadcast(
    generation: Long,
    permissions: PermissionFlow,
    cameraFacts: CaptureCapability,
    audioFacts: CaptureCapability,
    calibration: ClockCalibration,
    origin: Long,
    audioKeying: AudioKeying,
    capacity: Int,
    maxOutputBytes: Int,
    maxNalUnits: Int,
    video: CompleteTrackFormat,
    audio: CompleteTrackFormat,
    environment: CaptureEnvironment,
    sinks: PublishSinks,
    record: CaptureDiscardRecord,
    writeTimeout: Duration,
    drainTimeout: Duration,
    report: (String) -> Unit = {},
): LiveCapture {
    require(video.kind == MediaKind.Video) { "the video format is a ${video.kind} format" }
    require(audio.kind == MediaKind.Audio) { "the audio format is a ${audio.kind} format" }
    checkComposition(generation, permissions, record, maxOutputBytes, audioKeying, origin)
    audioKeyingDisagreement(audioKeying, (audio.authority as FormatAuthority.Aac).facts)?.let { throw IllegalArgumentException(it) }

    val parts = assembleCapture(generation, calibration, capacity, maxOutputBytes, maxNalUnits, environment, record)
    // The audio track's own ledger and keying, for the microphone's pump.
    (environment.acquirer as? CaptureInputRouting)?.bindAudioInputs(parts.audioLedger, audioKeying, generation)
    // One timeline each over the SAME immutable origin: that state is per
    // track and not shared.
    val broadcast =
        LiveBroadcast(
            session = parts.session,
            events = parts.events,
            video = TrackLane(MediaKind.Video, PublishPipeline(MediaKind.Video, PublishTimeline(origin), parts.clock, video, parts.limits), sinks.video),
            audio = TrackLane(MediaKind.Audio, PublishPipeline(MediaKind.Audio, PublishTimeline(origin), parts.clock, audio, parts.limits), sinks.audio),
            completion = sinks.completion,
            writeTimeout = writeTimeout,
            drainTimeout = drainTimeout,
            report = report,
        )

    return LiveCapture(
        video = TrackInputs(MediaKind.Video, generation, parts.videoLedger, null, parts.videoHandoff),
        audio = TrackInputs(MediaKind.Audio, generation, parts.audioLedger, audioKeying, parts.audioHandoff),
        record = record,
        run = { broadcast.run(permissions, cameraFacts, audioFacts) },
    )
}

/**
 * Why [keying] disagrees with the validated AAC [facts] -- its sample rate,
 * then its access-unit length in frames -- or null when both agree. Two field
 * comparisons: the keying is not asked for a key, fed a read or replaced.
 * Every valid AAC configuration is not assumed to use 1024 frames; the facts
 * carry the unit the configuration itself declares.
 */
internal fun audioKeyingDisagreement(keying: AudioKeying, facts: AacLcConfigFacts): String? =
    when {
        keying.sampleRate != facts.sampleRate ->
            "the audio keying's sample rate ${keying.sampleRate} is not the validated AAC format's ${facts.sampleRate}"
        keying.framesPerUnit != facts.framesPerAccessUnit ->
            "the audio keying's unit of ${keying.framesPerUnit} frames is not the validated AAC format's ${facts.framesPerAccessUnit}"
        else -> null
    }

/** The composition checks both roots make BEFORE anything is built: generations, the byte bound, and the keying's origin. */
private fun checkComposition(
    generation: Long,
    permissions: PermissionFlow,
    record: CaptureDiscardRecord,
    maxOutputBytes: Int,
    audioKeying: AudioKeying,
    origin: Long,
) {
    require(permissions.generation == generation) {
        "the permission flow is for generation ${permissions.generation}, not $generation"
    }
    require(record.generation == generation) {
        "the discard record is for generation ${record.generation}, not $generation"
    }
    require(maxOutputBytes > 0) { "the output byte limit is positive, not $maxOutputBytes" }
    require(audioKeying.originUs == origin) {
        "the audio keying is keyed to origin ${audioKeying.originUs}, not the broadcast origin $origin"
    }
}

/** What both roots assemble identically: one session, its events, its tracks' ledgers and handoffs, one clock and one bound. */
private class CaptureAssembly(
    val events: BroadcastEvents,
    val session: PlatformCaptureSession,
    val videoLedger: CaptureLedger,
    val audioLedger: CaptureLedger,
    val videoHandoff: CaptureHandoff,
    val audioHandoff: CaptureHandoff,
    val clock: PlatformCaptureClock,
    val limits: AvcPayloadLimits,
)

/**
 * The shared assembly, in the one order the contracts force; see
 * [composeLiveBroadcast]. It acquires nothing: the platform is first touched
 * inside the run, by the session's `start`.
 */
private fun assembleCapture(
    generation: Long,
    calibration: ClockCalibration,
    capacity: Int,
    maxOutputBytes: Int,
    maxNalUnits: Int,
    environment: CaptureEnvironment,
    record: CaptureDiscardRecord,
): CaptureAssembly {
    // The listener the session is CONSTRUCTED with, so a readiness decided
    // inside start is already recorded when start returns. It retains the
    // record and the coordinator's events, and nothing else.
    val events = BroadcastEvents()
    val listener = RecordingCaptureListener(record, events)

    val ownership = OrderedCaptureOwnership(generation, environment.dispatcher, environment.late)
    val faults = SessionFaults()
    val videoLedger = BoundedCaptureLedger(MediaKind.Video, generation, capacity)
    val audioLedger = BoundedCaptureLedger(MediaKind.Audio, generation, capacity)
    val videoHandoff =
        BoundedCaptureHandoff(
            kind = MediaKind.Video,
            generation = generation,
            buffers = environment.video.buffers,
            owner = environment.video.indices,
            ledger = videoLedger,
            faults = faults,
            capacity = capacity,
            maxOutputBytes = maxOutputBytes,
            lifetime = environment.video.lifetime,
        )
    val audioHandoff =
        BoundedCaptureHandoff(
            kind = MediaKind.Audio,
            generation = generation,
            buffers = environment.audio.buffers,
            owner = environment.audio.indices,
            ledger = audioLedger,
            faults = faults,
            capacity = capacity,
            maxOutputBytes = maxOutputBytes,
            lifetime = environment.audio.lifetime,
        )

    val session = PlatformCaptureSession(generation, ownership, environment.acquirer, videoHandoff, audioHandoff, listener)
    // BEFORE anything is started: the driver's own `begin` can report a fault,
    // and there is no window in which one has nowhere to go.
    faults.bind(session)
    (environment.acquirer as? CaptureFaultRouting)?.bindFaults(faults)

    // ONE clock for the broadcast -- a recalibration applies to both tracks.
    val clock = PlatformCaptureClock(calibration)
    // The converted payload's bound: the same numeric cap the handoffs enforce.
    val limits = AvcPayloadLimits(maxNalUnits, maxOutputBytes)
    return CaptureAssembly(events, session, videoLedger, audioLedger, videoHandoff, audioHandoff, clock, limits)
}

/**
 * What the deferred assembly produced: the platform driver's typed inputs,
 * the two format holders its output-format callbacks report to, the
 * session-owned record, and the ONE run.
 */
internal class DeferredLiveCapture(
    val video: TrackInputs,
    val audio: TrackInputs,
    val videoFormat: VideoFormatHolder,
    val audioFormat: AudioFormatHolder,
    val record: CaptureDiscardRecord,
    val run: suspend () -> BroadcastOutcome,
)

/**
 * The live broadcast whose track formats are still to be observed: the same
 * assembly as [composeLiveBroadcast], with the two [FormatHolders] bound to
 * its session BEFORE anything starts, and the live publication awaiting
 * those holders' own [FormatSources] -- both validated formats -- before it
 * admits video, then audio, through [tracks] and the session's admission.
 *
 * **The audio keying.** [audioKeying] is supplied and retained as the SAME
 * object with its state, exactly as in [composeLiveBroadcast]; its origin is
 * checked here. Its rate and unit geometry cannot be checked until the AAC
 * facts arrive, so the publication checks them then, BEFORE either add: a
 * disagreement is a named preparation refusal, never a replacement keying,
 * a converted timestamp or a reset of the reads it has already taken.
 *
 * Holders, formats, ending, cleanup and retirement are the accepted ones; this
 * adds no lifecycle and acquires nothing until [DeferredLiveCapture.run].
 */
internal fun <H : Any> composeDeferredBroadcast(
    generation: Long,
    permissions: PermissionFlow,
    cameraFacts: CaptureCapability,
    audioFacts: CaptureCapability,
    calibration: ClockCalibration,
    origin: Long,
    audioKeying: AudioKeying,
    capacity: Int,
    maxOutputBytes: Int,
    maxNalUnits: Int,
    videoName: TrackName,
    audioName: TrackName,
    formatBounds: ConfigBounds,
    formatBudget: Duration,
    environment: CaptureEnvironment,
    tracks: TrackOperations<H>,
    completion: CompletionSink,
    record: CaptureDiscardRecord,
    writeTimeout: Duration,
    drainTimeout: Duration,
    report: (String) -> Unit = {},
    videoUtility: AvcRecordUtility = PublicCodecSignaling,
    audioUtility: AacStringUtility = PublicAacSignaling,
): DeferredLiveCapture {
    checkComposition(generation, permissions, record, maxOutputBytes, audioKeying, origin)
    (environment.audio.binding?.configuration as? AudioEncoderConfiguration)?.let { pcm ->
        pcm.keyingDisagreement(audioKeying)?.let { throw IllegalArgumentException(it) }
    }
    val parts = assembleCapture(generation, calibration, capacity, maxOutputBytes, maxNalUnits, environment, record)
    // Bound to THIS session before anything starts: a format observed during
    // the startup is admitted by the same authority as any other.
    val holders = FormatHolders(parts.session, videoName, audioName, formatBounds, videoUtility, audioUtility)
    // The same session, each codec's own track and holder: every callback a
    // codec delivers is admitted by the authority that admits its formats.
    environment.video.binding?.bind(
        EncoderRoute(parts.session, parts.session.bindCodecCallbacks(MediaKind.Video), generation, parts.videoHandoff) { fields, maximum ->
            holders.video.onOutputFormat(fields, maximum)
        },
    )
    // The audio track's own ledger and keying, for the microphone's pump.
    (environment.acquirer as? CaptureInputRouting)?.bindAudioInputs(parts.audioLedger, audioKeying, generation)
    environment.audio.binding?.bind(
        EncoderRoute(parts.session, parts.session.bindCodecCallbacks(MediaKind.Audio), generation, parts.audioHandoff) { fields, maximum ->
            holders.audio.onOutputFormat(fields, maximum)
        },
    )
    val publication =
        livePublication(TrackOwner(tracks), tracks, parts.events, parts.session.publicationAdmission(), holders.sources, formatBudget, parts.limits, parts.clock, origin, audioKeying)
    val broadcast = LiveBroadcast(parts.session, parts.events, publication, completion, writeTimeout, drainTimeout, report)
    return DeferredLiveCapture(
        video = TrackInputs(MediaKind.Video, generation, parts.videoLedger, null, parts.videoHandoff),
        audio = TrackInputs(MediaKind.Audio, generation, parts.audioLedger, audioKeying, parts.audioHandoff),
        videoFormat = holders.video,
        audioFormat = holders.audio,
        record = record,
        run = { broadcast.run(permissions, cameraFacts, audioFacts) },
    )
}

/** The production specialization: the same deferred assembly over a real, BORROWED [MediaSender]. It is never closed here. */
internal fun composeSenderDeferredBroadcast(
    generation: Long,
    permissions: PermissionFlow,
    cameraFacts: CaptureCapability,
    audioFacts: CaptureCapability,
    calibration: ClockCalibration,
    origin: Long,
    audioKeying: AudioKeying,
    capacity: Int,
    maxOutputBytes: Int,
    maxNalUnits: Int,
    videoName: TrackName,
    audioName: TrackName,
    formatBounds: ConfigBounds,
    formatBudget: Duration,
    environment: CaptureEnvironment,
    sender: MediaSender,
    completion: CompletionSink,
    record: CaptureDiscardRecord,
    writeTimeout: Duration,
    drainTimeout: Duration,
    report: (String) -> Unit = {},
): DeferredLiveCapture =
    composeDeferredBroadcast(
        generation, permissions, cameraFacts, audioFacts, calibration, origin, audioKeying, capacity, maxOutputBytes, maxNalUnits,
        videoName, audioName, formatBounds, formatBudget, environment, sender.trackOperations(), completion, record, writeTimeout, drainTimeout, report,
    )

/**
 * The live publication: a preparation that admits both tracks through
 * [owner] -- video, then audio -- and binds each lane to the handle it was
 * admitted with. No lane exists before its handle does. Between the two adds
 * the preparation stops new work if the session already ended or this
 * coroutine was cancelled; an add already accepted still commits to its
 * slot. The owner's own rejection and the sender's declared refusals are the
 * only outcomes that become a reason; everything else propagates as itself.
 *
 * The closure captures the concrete owner, so the handle type never reaches
 * the coordinator, which sees only [TrackOwnership].
 *
 * Each track is added with its validated format's exact configuration, and
 * its lane converts and compares against that same format within [limits].
 * A format of the wrong kind is refused here, before any add. Each add is
 * admitted afresh by [admission] immediately before it: once the session has
 * claimed its ending, no new add begins, while an add admitted before that
 * claim still commits to its slot and is retired by the owner.
 */
internal fun <H : Any> livePublication(
    owner: TrackOwner<H>,
    tracks: TrackOperations<H>,
    events: BroadcastEvents,
    admission: PublicationAdmission,
    video: CompleteTrackFormat,
    audio: CompleteTrackFormat,
    limits: AvcPayloadLimits,
    clock: PlatformCaptureClock,
    origin: Long,
): Publication.Live {
    require(video.kind == MediaKind.Video) { "the video format is a ${video.kind} format" }
    require(audio.kind == MediaKind.Audio) { "the audio format is a ${audio.kind} format" }
    fun lane(kind: MediaKind, handle: H, format: CompleteTrackFormat): TrackLane =
        TrackLane(
            kind,
            PublishPipeline(kind, PublishTimeline(origin), clock, format, limits),
            object : TrackSink {
                override suspend fun write(obj: SendObject, timeout: Duration): WriteOutcome = tracks.write(handle, obj, timeout)

                override suspend fun endTrack(timeout: Duration): WriteOutcome = tracks.endTrack(handle, timeout)
            },
        )
    suspend fun prepare(): Preparation {
        if (!admission.admit()) return Preparation.Refused("the capture session ended before the video track was admitted")
        val first =
            when (val added = owner.add(MediaKind.Video, video.config)) {
                is AddOutcome.Added -> added.handle
                is AddOutcome.Rejected -> return Preparation.Refused(added.reason)
                is AddOutcome.Refused -> return Preparation.Refused(added.reason)
            }
        // New work stops here; the add above, if it was accepted, is already the owner's.
        coroutineContext.ensureActive()
        if (events.terminal.isCompleted) return Preparation.Refused("the capture session ended before the audio track was admitted")
        if (!admission.admit()) return Preparation.Refused("the capture session ended before the audio track was admitted")
        val second =
            when (val added = owner.add(MediaKind.Audio, audio.config)) {
                is AddOutcome.Added -> added.handle
                is AddOutcome.Rejected -> return Preparation.Refused(added.reason)
                is AddOutcome.Refused -> return Preparation.Refused(added.reason)
            }
        return Preparation.Ready(lane(MediaKind.Video, first, video), lane(MediaKind.Audio, second, audio))
    }
    return Publication.Live(owner, ::prepare)
}

/** The production specialization: the same live publication over a real, BORROWED [MediaSender]. It is never closed here. */
internal fun senderPublication(
    sender: MediaSender,
    events: BroadcastEvents,
    admission: PublicationAdmission,
    video: CompleteTrackFormat,
    audio: CompleteTrackFormat,
    limits: AvcPayloadLimits,
    clock: PlatformCaptureClock,
    origin: Long,
): Publication.Live {
    val tracks = sender.trackOperations()
    return livePublication(TrackOwner(tracks), tracks, events, admission, video, audio, limits, clock, origin)
}

/**
 * The tracks' fault route to the session they belong to.
 *
 * The session cannot be built before its handoffs and the handoffs cannot be
 * built without somewhere to report, so exactly one of the two is bound late
 * -- once, before anything starts. It is not a sink that may be absent: a
 * fault arriving with nothing bound is a wiring mistake, and it says so rather
 * than losing the fact.
 */
private class SessionFaults : CaptureFaultSink {
    private val lock = Any()
    private var to: CaptureFaultSink? = null

    fun bind(sink: CaptureFaultSink) {
        synchronized(lock) {
            check(to == null) { "this fault route is already bound to a session" }
            to = sink
        }
    }

    private fun bound(): CaptureFaultSink =
        synchronized(lock) { checkNotNull(to) { "a capture fault arrived before its session was bound" } }

    override fun onFault(generation: Long, fault: CaptureFault) = bound().onFault(generation, fault)

    override fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) =
        bound().onDiscarded(generation, kind, outputs)
}
