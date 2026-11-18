package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlin.time.Duration
import org.openmoq.moq5.MediaSender
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.SendTrack
import org.openmoq.moq5.SendTrackConfig
import org.openmoq.moq5.SenderCompletedException
import org.openmoq.moq5.TrackConfigRejectedException
import org.openmoq.moq5.TrackNameBusyException
import org.openmoq.moq5.WriteOutcome

/**
 * One of the sender's DECLARED business refusals of an add, as a value a
 * host row can raise: the library's own exception constructors are internal,
 * so the three mapped refusals are re-raised as this sample-local type on the
 * FAILURE path only. Anything else -- cancellation, an unexpected failure, an
 * `Error` -- keeps its identity.
 */
internal class TrackRefusedException(message: String) : IllegalStateException(message)

/**
 * The sender's external effect boundary for one live publication, generic
 * over the handle type so a host row can supply distinct opaque handles and
 * script their state explicitly. Production specializes [H] to [SendTrack]
 * and forwards every operation to the public [MediaSender] surface
 * ([trackOperations]); no host row can construct a [SendTrack], and none
 * needs to.
 */
internal interface TrackOperations<H : Any> {
    /**
     * `MediaSender.addTrack`: commit-grade -- once accepted, the handle is
     * delivered even to a cancelled caller -- and returned RAW, with nothing
     * allocated on the success path between the sender's answer and the
     * caller's store. A declared refusal raises [TrackRefusedException].
     */
    suspend fun addTrack(config: SendTrackConfig): H

    /** `MediaSender.removeTrack`: fallible; it throws its typed refusals. */
    suspend fun removeTrack(handle: H)

    suspend fun write(handle: H, obj: SendObject, timeout: Duration): WriteOutcome

    suspend fun endTrack(handle: H, timeout: Duration): WriteOutcome

    /** `SendTrack.isRemoved`: a removal THIS sender accepted, read without an operation. */
    fun isRemoved(handle: H): Boolean
}

/** The production specialization over the public library surface. */
internal fun MediaSender.trackOperations(): TrackOperations<SendTrack> = SenderTrackOperations(this)

private class SenderTrackOperations(private val sender: MediaSender) : TrackOperations<SendTrack> {
    override suspend fun addTrack(config: SendTrackConfig): SendTrack =
        try {
            sender.addTrack(config)
        } catch (busy: TrackNameBusyException) {
            throw TrackRefusedException("the track name is busy: ${busy.message}")
        } catch (rejected: TrackConfigRejectedException) {
            throw TrackRefusedException("the track configuration was rejected: ${rejected.message}")
        } catch (completed: SenderCompletedException) {
            throw TrackRefusedException("the broadcast was already completed: ${completed.message}")
        }

    override suspend fun removeTrack(handle: SendTrack) = sender.removeTrack(handle)

    override suspend fun write(handle: SendTrack, obj: SendObject, timeout: Duration): WriteOutcome = sender.write(handle, obj, timeout)

    override suspend fun endTrack(handle: SendTrack, timeout: Duration): WriteOutcome = sender.endTrack(handle, timeout)

    override fun isRemoved(handle: SendTrack): Boolean = handle.isRemoved
}

/** What the owner did with one slot when it settled. */
internal enum class Retired {
    /** Nothing was ever stored in the slot. */
    NotHeld,

    /** Released: the lanes ended the track and the sender's own close retires the handle. */
    Left,

    /** Already removed by this sender before the attempt (the one accepted proof); no attempt was made. */
    Skipped,

    /** The removal was attempted and accepted. */
    Removed,

    /** The removal was attempted and raised; the handle stays referenced and the cause is the slot's root. */
    Failed,
}

/** The settlement of both slots, built only after BOTH attempts have run. */
internal class OwnerSettlement(
    val video: Retired,
    val videoRoot: Throwable?,
    val audio: Retired,
    val audioRoot: Throwable?,
) {
    val roots: List<Throwable> get() = listOfNotNull(videoRoot, audioRoot)
}

/**
 * What the coordinator needs from the owner: ONE settlement decision, never a
 * handle. Keeps the handle type out of [LiveBroadcast].
 */
internal interface TrackOwnership {
    val isDecided: Boolean

    /** Normal completion: nothing is removed. Decided once. */
    suspend fun release(): OwnerSettlement

    /** Every other exit: both slots attempted, under `NonCancellable`. Decided once. */
    suspend fun retire(): OwnerSettlement
}

/** What one add attempt did at the OWNER's boundary, before or after the sender was asked. */
internal sealed interface AddOutcome<out H : Any> {
    class Added<H : Any>(val handle: H) : AddOutcome<H>

    /** The owner refused before any sender effect: a duplicate kind, or a request after the decision. */
    class Rejected(val reason: String) : AddOutcome<Nothing>

    /** The sender's own declared business refusal. */
    class Refused(val reason: String) : AddOutcome<Nothing>
}

/**
 * The durable owner of at most two commit-grade handles, one per kind.
 *
 * It is allocated before the preparation child exists, so a result the child
 * never delivers still has an owner. [add] RESERVES the kind's slot before the
 * sender is asked -- a duplicate request or a request after the decision is
 * rejected with no sender effect -- and stores the returned handle as the
 * very next statement: no allocation, no suspension, no callback, no wrapper,
 * no new coroutine boundary in between, so a cancellation that arrives while
 * the add was in flight still finds the handle in its slot. The owner never
 * removes a handle on the child's behalf, holds no lock across a suspending
 * removal, and keeps each slot's failure root in a fixed field preallocated
 * before the attempts, so nothing is allocated between them. A failed removal
 * keeps its handle referenced; the borrowed sender's own close is its eventual
 * retirement. Bounded sample-internal bookkeeping, not a registry.
 */
internal class TrackOwner<H : Any>(private val tracks: TrackOperations<H>) : TrackOwnership {
    private enum class Decision { Released, Retired }

    private val lock = Any()
    private var video: H? = null
    private var audio: H? = null
    private var videoReserved = false
    private var audioReserved = false
    private var decision: Decision? = null
    private var settlement: OwnerSettlement? = null

    /** One root per slot, in a list with room for exactly that root, allocated before any attempt runs. */
    private val videoRoots = ArrayList<Throwable>(1)
    private val audioRoots = ArrayList<Throwable>(1)

    override val isDecided: Boolean get() = synchronized(lock) { decision != null }

    fun handle(kind: MediaKind): H? = synchronized(lock) { if (kind == MediaKind.Video) video else audio }

    suspend fun add(kind: MediaKind, config: SendTrackConfig): AddOutcome<H> {
        val rejected =
            synchronized(lock) {
                when {
                    decision != null -> "the track ownership is already decided"
                    kind == MediaKind.Video && videoReserved -> "a video track was already requested"
                    kind == MediaKind.Audio && audioReserved -> "an audio track was already requested"
                    else -> {
                        if (kind == MediaKind.Video) videoReserved = true else audioReserved = true
                        null
                    }
                }
            }
        if (rejected != null) return AddOutcome.Rejected(rejected)
        val handle =
            try {
                tracks.addTrack(config)
            } catch (refused: TrackRefusedException) {
                return AddOutcome.Refused(refused.message ?: "the track was refused")
            }
        // COMMIT. The next statement after the sender answered, and the first
        // thing done with the raw handle: a monitor, not a suspension and not
        // an allocation, so neither a cancellation nor an allocation failure
        // can come between the answer and its owner.
        synchronized(lock) {
            if (kind == MediaKind.Video) video = handle else audio = handle
        }
        return AddOutcome.Added(handle)
    }

    override suspend fun release(): OwnerSettlement {
        synchronized(lock) {
            settlement?.let { return it }
            decision = Decision.Released
            val s =
                OwnerSettlement(
                    if (video != null) Retired.Left else Retired.NotHeld,
                    null,
                    if (audio != null) Retired.Left else Retired.NotHeld,
                    null,
                )
            settlement = s
            return s
        }
    }

    override suspend fun retire(): OwnerSettlement {
        val (v, a) =
            synchronized(lock) {
                settlement?.let { return it }
                decision = Decision.Retired
                video to audio
            }
        var videoVerdict = Retired.NotHeld
        var audioVerdict = Retired.NotHeld
        withContext(NonCancellable) {
            if (v != null) {
                if (tracks.isRemoved(v)) {
                    videoVerdict = Retired.Skipped
                } else {
                    try {
                        tracks.removeTrack(v)
                        videoVerdict = Retired.Removed
                    } catch (cancelled: CancellationException) {
                        videoRoots.add(cancelled)
                    } catch (failure: Exception) {
                        videoRoots.add(failure)
                    } catch (error: Error) {
                        videoRoots.add(error)
                    }
                    if (videoRoots.isNotEmpty()) videoVerdict = Retired.Failed
                }
            }
            if (a != null) {
                if (tracks.isRemoved(a)) {
                    audioVerdict = Retired.Skipped
                } else {
                    try {
                        tracks.removeTrack(a)
                        audioVerdict = Retired.Removed
                    } catch (cancelled: CancellationException) {
                        audioRoots.add(cancelled)
                    } catch (failure: Exception) {
                        audioRoots.add(failure)
                    } catch (error: Error) {
                        audioRoots.add(error)
                    }
                    if (audioRoots.isNotEmpty()) audioVerdict = Retired.Failed
                }
            }
        }
        val s = OwnerSettlement(videoVerdict, videoRoots.firstOrNull(), audioVerdict, audioRoots.firstOrNull())
        synchronized(lock) { settlement = s }
        return s
    }
}

/** What the preparation child produced. */
internal sealed interface Preparation {
    /** Both tracks admitted; the lanes are bound to their handles. */
    class Ready(val video: TrackLane, val audio: TrackLane) : Preparation

    /** A declared business refusal, or the owner's own rejection. What was already admitted is the owner's. */
    class Refused(val reason: String) : Preparation
}

/** The closed choice of what a broadcast publishes through. */
internal sealed interface Publication {
    /** Two real prebuilt lanes; nothing is adopted or retired. */
    class Preconfigured(val video: TrackLane, val audio: TrackLane) : Publication

    /** The durable owner and the preparation that fills it; no lane exists until its handle does. */
    class Live(val ownership: TrackOwnership, val prepare: suspend () -> Preparation) : Publication
}
