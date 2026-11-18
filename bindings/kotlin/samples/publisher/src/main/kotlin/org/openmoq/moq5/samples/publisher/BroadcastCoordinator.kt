package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withContext
import kotlin.coroutines.coroutineContext
import kotlin.time.Duration
import org.openmoq.moq5.DrainOutcome
import org.openmoq.moq5.MediaSender
import org.openmoq.moq5.MoqEndpoint

/**
 * The two-track live broadcast: one capture session, one drain per track, one
 * completion.
 *
 * It owns no sender and no capture resource. It composes the accepted
 * [drainTrack] for both tracks, the accepted [CaptureSession] lifecycle, and a
 * completion seam that forwards to the sender and the endpoint.
 *
 * **Why the listener is a constructor argument.** [CaptureSession] takes its
 * listener when it is built and has no later registration, so the only way to
 * observe readiness and the one terminal is to install this object's events
 * BEFORE the session is started. The caller therefore builds the events, then
 * the session, then this.
 *
 * **What completion requires.** Two successful drains are not a completed
 * broadcast. The session decides its terminal under its own lock and announces
 * it afterwards, so a Failed decision can already have won while its
 * notification is still in flight: this coordinator waits for the session's
 * explicit Completed notification IN ADDITION to both successful drains, and
 * an absent failure is never read as success.
 */
internal interface CompletionSink {
    /**
     * `MediaSender.complete()`: it only RECORDS the completion. Completing
     * REMOVES every track, and a removed track's still-queued media and
     * terminal are discarded, so it comes only after they have drained.
     */
    suspend fun complete()

    /** `MoqEndpoint.drain(timeout)`: the answer is reported as it is, never as a flush. */
    suspend fun drain(timeout: Duration): DrainOutcome
}

/** The sender's completion and its endpoint's drain, as one [CompletionSink]. */
internal fun endpointCompletion(sender: MediaSender, endpoint: MoqEndpoint): CompletionSink =
    object : CompletionSink {
        override suspend fun complete() = sender.complete()

        override suspend fun drain(timeout: Duration): DrainOutcome = endpoint.drain(timeout)
    }

/** How a finite publication's ending went: both barriers flushed, or the one that did not. */
internal sealed interface Finish {
    /** The accepted media and track terminals, then the terminal catalog, both reached the core. */
    data object Flushed : Finish

    /** The accepted media and track terminals did not drain; completion was NOT requested. */
    data class MediaNotDrained(val outcome: DrainOutcome) : Finish

    /**
     * The completion was requested after the media drained, and its terminal
     * catalog did not drain. The completion was accepted and cannot be undone.
     */
    data class CompletionNotDrained(val outcome: DrainOutcome) : Finish
}

/**
 * Ends a finite publication whose writes have stopped and whose track ends
 * were accepted: drain the accepted media and track terminals, and only if
 * that FLUSHED request completion, then drain the terminal catalog it
 * records. Completion removes the tracks and discards what they still hold,
 * so completing first would lose that tail.
 *
 * Each answer is reported as itself; nothing short of [DrainOutcome.Flushed]
 * passes a barrier. A local flush is not proof that a peer received or
 * rendered anything, and does not undo an earlier policy drop or refusal.
 * Cancellation is checked before each new operation, and neither drain is
 * made non-cancellable.
 */
internal suspend fun CompletionSink.finishPublication(timeout: Duration, report: (String) -> Unit): Finish {
    coroutineContext.ensureActive()
    val media = drain(timeout)
    report("drain (accepted media and track terminals): $media")
    if (media != DrainOutcome.Flushed) return Finish.MediaNotDrained(media)
    coroutineContext.ensureActive()
    complete()
    coroutineContext.ensureActive()
    val catalog = drain(timeout)
    report("drain (terminal catalog): $catalog")
    if (catalog != DrainOutcome.Flushed) return Finish.CompletionNotDrained(catalog)
    return Finish.Flushed
}

/** One track's own publishing state: its pipeline and the sender it writes to. */
internal class TrackLane(
    val kind: MediaKind,
    val pipeline: PublishPipeline,
    val sink: TrackSink,
)

/** What one broadcast produced. */
internal sealed interface BroadcastOutcome {
    /**
     * Both tracks reached their own terminal and their ends were accepted, the
     * session said [CaptureTermination.Completed] and its ending retired, and
     * BOTH barriers of [finishPublication] flushed: the accepted media and
     * track terminals before completion, the terminal catalog after it. Any
     * other drain answer is [Unfinished], never this. It is a local flush,
     * not proof of a peer's receipt.
     */
    data class Published(
        val video: TrackDrain.Ended,
        val audio: TrackDrain.Ended,
    ) : BroadcastOutcome

    /**
     * A named outcome that is not a completed broadcast: a refused start, a
     * track that ended without a terminal, a refused `endTrack`, or a session
     * that ended any way other than Completed. Cancellation and failures are
     * NOT outcomes -- they propagate.
     */
    data class Unfinished(
        val reason: String,
        val video: TrackDrain?,
        val audio: TrackDrain?,
    ) : BroadcastOutcome
}

/**
 * The coordinator's view of the session's lifecycle. Installed in the
 * session's constructor; nothing else observes those callbacks.
 *
 * Readiness has its OWN bounded one-shot state: it is not one of the terminal
 * receipts, and a second delivery of either fact changes nothing.
 */
internal class BroadcastEvents : CaptureSessionListener {
    internal val readiness = CompletableDeferred<CaptureReady>()

    internal val terminal = CompletableDeferred<CaptureTermination>()

    override fun onReady(generation: Long, ready: CaptureReady) {
        readiness.complete(ready)
    }

    override fun onTerminated(generation: Long, termination: CaptureTermination) {
        terminal.complete(termination)
    }
}

/**
 * The reason a failed or cancelled broadcast gives the capture session.
 *
 * It carries NO cause: the coordinator already retains the primary, and a
 * back-reference is what would let a cleanup carrier that already suppresses
 * this reason close a cycle. It is an adapter, never an outcome -- it is never
 * attached to the primary as an ordinary cleanup root.
 */
internal class BroadcastStopped(reason: String) : IllegalStateException(reason)

/**
 * Registers the ONE receipt a child owes, whether or not it has already
 * completed: `invokeOnCompletion` fires immediately for a job that is already
 * finished, and reports that job's actual cause. Production registers through
 * here, so the behaviour is exercised where it is used.
 */
internal fun <T> kotlinx.coroutines.Deferred<T>.reportCompletion(emit: (Throwable?) -> Unit): kotlinx.coroutines.Deferred<T> =
    also { child -> child.invokeOnCompletion { cause -> emit(cause) } }

/** One child's settled outcome, written once by that child and read once by its handler. */
private class OutcomeSlot {
    private var value: TrackDrain? = null

    @Synchronized
    fun set(outcome: TrackDrain) {
        value = outcome
    }

    @Synchronized
    fun get(): TrackDrain? = value
}

/** Exactly one of these per producer: two children and the session's terminal. */
private sealed interface Receipt {
    class Drained(val kind: MediaKind, val outcome: TrackDrain?, val cause: Throwable?) : Receipt

    class Terminal(val termination: CaptureTermination) : Receipt
}

/**
 * One live broadcast, from start to completion.
 *
 * [run] starts the session, drains both tracks independently, and completes
 * only when BOTH tracks ended with an accepted `endTrack` AND the session
 * itself announced [CaptureTermination.Completed]. A refused start creates no
 * drain at all; a failure or cancellation in one track cancels and joins the
 * other; and a session that says Completed does not erase a failure a child
 * already reported.
 */
internal class LiveBroadcast(
    val session: CaptureSession,
    val events: BroadcastEvents,
    val publication: Publication,
    val completion: CompletionSink,
    val writeTimeout: Duration,
    val drainTimeout: Duration,
    val report: (String) -> Unit = {},
) {
    /** Two prebuilt lanes: the preconfigured publication. */
    constructor(
        session: CaptureSession,
        events: BroadcastEvents,
        video: TrackLane,
        audio: TrackLane,
        completion: CompletionSink,
        writeTimeout: Duration,
        drainTimeout: Duration,
        report: (String) -> Unit = {},
    ) : this(session, events, Publication.Preconfigured(video, audio), completion, writeTimeout, drainTimeout, report)

    /** True once this broadcast has stopped the capture it started. */
    private var stopped = false

    /** The live publication's ownership, settled exactly once by THIS coordinator; null when preconfigured. */
    private val ownership: TrackOwnership? = (publication as? Publication.Live)?.ownership

    /** True once this coordinator has retired that ownership, so its roots are carried exactly once. */
    private var retired = false

    /** True once this coordinator has taken the capture's retirement receipt, so its facts are carried exactly once. */
    private var receiptTaken = false

    suspend fun run(
        permissions: PermissionFlow,
        cameraFacts: CaptureCapability,
        audioFacts: CaptureCapability,
    ): BroadcastOutcome {
        // The listener is already installed: it was given to the session when
        // the session was built, so a readiness delivered INSIDE start is
        // already recorded when start returns.
        // A start that throws may still have an ending claimed -- by its own
        // unwinding, or by another thread before it -- whose cleanup is still
        // running elsewhere, or whose notification another thread still
        // delivers. That ending owes its receipt, and what its cleanup raised
        // leaves with the start's own failure. A start that claimed nothing
        // owes none and waits for nothing. This coordinator does not stop a
        // capture it never started.
        var startRaised: Throwable? = null
        val start =
            try {
                session.start(permissions, cameraFacts, audioFacts)
            } catch (cancelled: CancellationException) {
                startRaised = cancelled
                throw cancelled
            } catch (failure: Exception) {
                startRaised = failure
                throw failure
            } catch (error: Error) {
                startRaised = error
                throw error
            } finally {
                startRaised?.let { carry(it, retirementRoots()) }
            }
        if (start is CaptureStart.Refused) {
            // A refusal is the SESSION's own decision, and it owns what it did
            // about it: a gate refusal acquired nothing, and a refusal after
            // acquisition has already unwound through the session's own
            // terminal. This coordinator never started that capture, so it does
            // not stop it -- and it does not read "refused" as "nothing was
            // acquired" either.
            // An ending the session claimed while refusing owes its receipt,
            // and what that cleanup raised escapes rather than hiding behind
            // the refusal; a refusal that claimed nothing owes none, and
            // nothing is awaited for it.
            retirementRoots().let { roots -> roots.firstOrNull()?.let { first -> carry(first, roots.drop(1)); throw first } }
            return BroadcastOutcome.Unfinished("the capture session refused to start: ${start.reason}", null, null)
        }
        // From here the started session is THIS coordinator's to stop, on every
        // exit: readiness, source selection and the whole publication.
        var raised: Throwable? = null
        try {
            return started()
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
            raised?.let { settle(it, "the broadcast failed") }
        }
    }

    private suspend fun started(): BroadcastOutcome {
        // Either the prerequisites settle, or the session ends first. Both are
        // lifecycle callbacks; neither is polled.
        val ready =
            select {
                events.readiness.onAwait { it }
                events.terminal.onAwait { CaptureReady.Refused("the session ended before it was ready: $it") }
            }
        if (ready is CaptureReady.Refused) {
            return unfinished(ready.reason, null, null)
        }
        val videoSource = session.video
        val audioSource = session.audio
        if (videoSource == null || audioSource == null) {
            return unfinished("the capture session published no tracks", null, null)
        }
        return when (val through = publication) {
            is Publication.Preconfigured -> publish(videoSource, audioSource, through.video, through.audio)
            is Publication.Live -> prepared(through, videoSource, audioSource)
        }
    }

    /**
     * The ONE preparation child's durable outcome: its result recorded on
     * ITS OWN frame -- identity intact, never a copy `await` would recover --
     * and the actual cause its completion boundary reported.
     */
    private class PreparationReceipt {
        @Volatile
        var result: Result<Preparation>? = null

        @Volatile
        var cause: Throwable? = null
    }

    private sealed interface Selected {
        data object Child : Selected

        class Terminal(val termination: CaptureTermination) : Selected
    }

    /**
     * The live path: ONE supervised preparation child, settled before it is
     * interpreted.
     *
     * The terminal is checked BEFORE the child exists, so an already-ended
     * session admits nothing. The child adds and adopts through the real
     * owner and builds the lanes; it never removes a handle. The select
     * watches the child AND the terminal; on EVERY exit the child is
     * cancelled and its join FINISHED under `NonCancellable` before ownership
     * is decided, so a commit-grade add still in flight reaches its slot and
     * a Ready whose delivery is lost is still owned and retired. What the
     * child died of is read from its receipt, never from `join` or a
     * recovered `await`; a Throwable already leaving this frame stays the
     * primary and carries a distinct child failure as a root.
     */
    private suspend fun prepared(
        through: Publication.Live,
        videoSource: CaptureHandoff,
        audioSource: CaptureHandoff,
    ): BroadcastOutcome {
        if (events.terminal.isCompleted) {
            return unfinished("the capture session ended before the tracks were prepared: ${events.terminal.await()}", null, null)
        }
        val receipt = PreparationReceipt()
        val why = CancellationException("the preparation is settling")
        var selected: Selected? = null
        var leaving: Throwable? = null
        try {
            supervisorScope {
                val child =
                    async {
                        val result = runCatching { through.prepare() }
                        receipt.result = result
                        result.getOrThrow()
                    }
                child.reportCompletion { cause -> receipt.cause = cause }
                try {
                    selected =
                        select {
                            child.onJoin { Selected.Child }
                            events.terminal.onAwait { Selected.Terminal(it) }
                        }
                } finally {
                    withContext(NonCancellable) {
                        child.cancel(why)
                        child.join()
                    }
                }
            }
        } catch (cancelled: CancellationException) {
            leaving = cancelled
            throw cancelled
        } catch (failure: Exception) {
            leaving = failure
            throw failure
        } catch (error: Error) {
            leaving = error
            throw error
        } finally {
            leaving?.let { outgoing ->
                // What is leaving may be the runtime's reconstruction of the
                // cause the child finished with -- the same class, carrying
                // that instance as its direct cause -- rather than the instance
                // itself. The child's recorded failure is compared against the
                // stopping that cause names: a copy of the same cancellation
                // the child caught at its own suspension is the same stopping,
                // not a root, whichever frame the runtime reconstructed it on.
                // A child cancellation with no causal link to it stays a root.
                val cause = receipt.cause
                val stopping = if (cause != null && reconstructionOf(outgoing, cause)) cause else outgoing
                val found = childFailure(receipt, why)?.takeIf { it !== outgoing && it !== stopping && !propagatedFrom(it, stopping) }
                if (found != null) carry(outgoing, listOf(found))
            }
        }
        // Settled. Everything below is interpreted on THIS frame.
        val failure = childFailure(receipt, why)
        if (failure != null) {
            settle(failure, "the preparation failed")
            throw failure
        }
        val ended = (selected as? Selected.Terminal)?.termination ?: if (events.terminal.isCompleted) events.terminal.await() else null
        if (ended != null) {
            return unfinished("the capture session ended before the tracks were prepared: $ended", null, null)
        }
        return when (val result = receipt.result?.getOrNull()) {
            null -> unfinished("the preparation produced no result", null, null)
            is Preparation.Refused -> unfinished(result.reason, null, null)
            is Preparation.Ready -> {
                // A Ready built while this broadcast was being cancelled is
                // owned and retired, never published; the session's end was
                // read above, after the child settled.
                coroutineContext.ensureActive()
                publish(videoSource, audioSource, result.video, result.audio)
            }
        }
    }

    /** What the child actually died of, if anything: its own recorded failure, else its completion cause -- excluding this coordinator's own settling cancellation. */
    private fun childFailure(receipt: PreparationReceipt, why: CancellationException): Throwable? {
        val recorded = receipt.result?.exceptionOrNull() ?: receipt.cause ?: return null
        return recorded.takeUnless { ownCancellation(it, why) }
    }

    /** Whether [copy] is the runtime's reconstruction of the cancellation [original]: the same class, with [original] itself as the direct cause. */
    private fun reconstructionOf(copy: Throwable, original: Throwable): Boolean =
        copy is CancellationException && original is CancellationException && copy !== original && copy::class == original::class && copy.cause === original

    /** What the supervised section observed. Nothing is thrown out of it. */
    private class Settled(
        val video: TrackDrain?,
        val audio: TrackDrain?,
        val terminal: CaptureTermination?,
        val failure: Throwable?,
        val unfinished: String?,
        val lost: List<Throwable>,
    )

    private suspend fun publish(
        videoSource: CaptureHandoff,
        audioSource: CaptureHandoff,
        videoLane: TrackLane,
        audioLane: TrackLane,
    ): BroadcastOutcome {
        // Starting the drains is a new publication operation: admitted afresh,
        // atomically with the session's claim of its ending. An ending already
        // claimed -- even one not yet delivered -- starts no drain.
        if (!session.admitPublication(session.generation)) {
            return unfinished("the capture session ended before publication started", null, null)
        }
        // THREE one-shot producers, and the channel is sized for exactly them.
        // A receive cancelled mid-delivery would otherwise DROP the element it
        // had already taken, so an undelivered receipt is kept and accounted
        // for with the rest.
        val dropped = mutableListOf<Receipt>()
        val receipts =
            Channel<Receipt>(capacity = 3, onUndeliveredElement = { receipt -> synchronized(dropped) { dropped.add(receipt) } })
        // Failures that could not be delivered, and real failures found while
        // the children were settling after a primary was already chosen.
        val undelivered = mutableListOf<Throwable>()
        var video: TrackDrain? = null
        var audio: TrackDrain? = null
        var terminal: CaptureTermination? = null
        var failure: Throwable? = null
        var unfinished: String? = null
        var retiring = false
        var settledChildren = 0

        // A cancelled parent makes this scope THROW rather than return the
        // value that carries what the children actually produced, so the
        // accounting is transferred onto whatever is leaving.
        var leaving: Throwable? = null
        val settled =
            try {
                kotlinx.coroutines.supervisorScope {
                    val children =
                        listOf(
                            drainChild(videoSource, videoLane, receipts, undelivered),
                            drainChild(audioSource, audioLane, receipts, undelivered),
                        )
                    val watcher = launch { receipts.send(Receipt.Terminal(events.terminal.await())) }
                    try {
                        while (failure == null && unfinished == null && !retiring && (settledChildren < 2 || terminal == null)) {
                            when (val receipt = receipts.receive()) {
                                is Receipt.Drained -> {
                                    settledChildren++
                                    val cause = receipt.cause
                                    val drained = receipt.outcome
                                    when {
                                        cause != null -> failure = cause
                                        drained is TrackDrain.Ended -> {
                                            if (receipt.kind == MediaKind.Video) video = drained else audio = drained
                                            // The session learns a track ended only
                                            // once its end was ACCEPTED. Its own
                                            // cleanup can fail there, and that
                                            // failure is COLLECTED so it leaves on
                                            // this coordinator's frame.
                                            endTrackOnSession(receipt.kind)?.let { failure = it }
                                        }
                                        drained is TrackDrain.Unfinished -> {
                                            if (receipt.kind == MediaKind.Video) video = drained else audio = drained
                                            unfinished = drained.reason
                                        }
                                        else -> unfinished = "the ${receipt.kind} track settled without an outcome"
                                    }
                                }
                                is Receipt.Terminal -> {
                                    terminal = receipt.termination
                                    // A session that FAILED or was cancelled retires
                                    // this publication itself: a drain suspended
                                    // inside the sender has no reason to be woken
                                    // again, and nothing further can complete.
                                    if (receipt.termination !is CaptureTermination.Completed) retiring = true
                                }
                            }
                        }
                    } finally {
                        // EVERY child is cancelled and joined, and the channel is
                        // retired -- even when this context is already cancelled,
                        // which would otherwise abort the sequence part-way.
                        withContext(NonCancellable) {
                            val why = CancellationException("the broadcast is settling")
                            children.forEach { it.cancel(why) }
                            watcher.cancel(why)
                            children.forEach { it.join() }
                            watcher.join()
                            // What the children actually produced while they
                            // settled is evidence, not permission to publish: the
                            // receipts still in the channel are accounted for, and
                            // no write or end is made here.
                            val remaining = ArrayDeque<Receipt>()
                            synchronized(dropped) {
                                remaining.addAll(dropped)
                                dropped.clear()
                            }
                            while (true) {
                                val pending = remaining.removeFirstOrNull() ?: receipts.tryReceive().getOrNull() ?: break
                                when (pending) {
                                    is Receipt.Drained -> {
                                        val cause = pending.cause
                                        when {
                                            cause == null -> Unit
                                            ownCancellation(cause, why) -> Unit
                                            failure == null -> failure = cause
                                            else -> synchronized(undelivered) { undelivered.add(cause) }
                                        }
                                    }
                                    is Receipt.Terminal -> if (terminal == null) terminal = pending.termination
                                }
                            }
                            receipts.close()
                        }
                    }
                        Settled(video, audio, terminal, failure, unfinished, synchronized(undelivered) { undelivered.toList() })
                    }
            } catch (cancelled: CancellationException) {
                leaving = cancelled
                throw cancelled
            } catch (raised: Exception) {
                leaving = raised
                throw raised
            } catch (error: Error) {
                leaving = error
                throw error
            } finally {
                leaving?.let { outgoing ->
                    // Real failures the children produced are transferred onto
                    // whatever is leaving. Only a cancellation that is the SAME
                    // stopping -- one propagated from what is leaving -- is
                    // redundant; a child that cancelled itself earlier, for its
                    // own reason, is evidence of its own.
                    val found =
                        (listOfNotNull(failure) + synchronized(undelivered) { undelivered.toList() })
                            .filter { it !== outgoing }
                            .filterNot { propagatedFrom(it, outgoing) }
                    carry(outgoing, found)
                }
            }
        // Everything below runs on THIS frame. A failure thrown from inside a
        // scope crosses a coroutine boundary, where kotlinx recovers -- that
        // is, COPIES -- it; the primary a caller sees has to be the instance
        // the child actually raised.
        return conclude(settled)
    }

    private suspend fun conclude(settled: Settled): BroadcastOutcome {
        val primary = settled.failure ?: settled.lost.firstOrNull()
        if (primary != null) {
            carry(primary, settled.lost.filter { it !== primary })
            settle(primary, "the broadcast failed")
            throw primary
        }
        val video = settled.video
        val audio = settled.audio
        val ended = settled.terminal
        if (settled.unfinished != null) {
            return unfinished(settled.unfinished, video, audio)
        }
        if (video !is TrackDrain.Ended || audio !is TrackDrain.Ended || ended !is CaptureTermination.Completed) {
            return unfinished(
                when {
                    ended == null -> "the capture session never announced its terminal"
                    ended !is CaptureTermination.Completed -> "the capture session ended as $ended"
                    else -> "a track did not end cleanly"
                },
                video,
                audio,
            )
        }
        // The session said Completed; its ending is accepted only once that
        // ending has retired. An unexpected cause an admitted callback
        // reported, or a failure its cleanup or its notification delivery
        // raised -- on whatever thread -- is this broadcast's failure, found
        // BEFORE the sender is told anything completed.
        val receipt = retired()
        val late = receipt?.let { it.causes + it.cleanup }.orEmpty()
        late.firstOrNull()?.let { first ->
            carry(first, late.drop(1))
            settle(first, "the broadcast failed")
            throw first
        }
        // The media drain, the completion and the catalog drain, each checked
        // for cancellation first. What already resolved is not undone by it,
        // and an accepted completion is not undone by a catalog drain that
        // did not flush: that is reported, and the ownership is settled.
        when (val finish = completion.finishPublication(drainTimeout, report)) {
            Finish.Flushed -> Unit
            is Finish.MediaNotDrained ->
                return unfinished("the accepted media did not drain before completion: ${finish.outcome}", video, audio)
            is Finish.CompletionNotDrained ->
                return unfinished("the completion was accepted but its terminal catalog did not drain: ${finish.outcome}", video, audio)
        }
        // The accepted publication releases its ownership: the lanes ended the
        // tracks, and the borrowed sender's own close retires the handles.
        ownership?.release()
        return BroadcastOutcome.Published(video, audio)
    }

    /**
     * A named outcome that is not a completion: the capture this coordinator
     * started is stopped, and a REAL cleanup failure found there escapes
     * rather than being swallowed by the outcome.
     */
    private suspend fun unfinished(reason: String, video: TrackDrain?, audio: TrackDrain?): BroadcastOutcome {
        settle(null, "the broadcast did not finish")?.let { throw it }
        return BroadcastOutcome.Unfinished(reason, video, audio)
    }

    /**
     * ONE settlement policy for every exit that is not a completion.
     *
     * The capture this coordinator started is stopped exactly once. An
     * existing [primary] -- a cancellation included -- keeps its identity and
     * carries the selected cleanup roots. With no primary, the FIRST selected
     * root escapes carrying the rest, so a real cleanup failure is never lost
     * to an outcome value.
     */
    private suspend fun settle(primary: Throwable?, why: String): Throwable? {
        // The stop comes first -- it may be the claim -- then the ending's
        // receipt, whose unexpected causes lead the roots, then the ownership.
        val stopped = stopFailedCapture(why)
        val roots = retirementRoots() + stopped + retireOwnership()
        if (primary != null) {
            carry(primary, roots)
            return primary
        }
        val first = roots.firstOrNull() ?: return null
        carry(first, roots.drop(1))
        return first
    }

    /**
     * The failures the capture's claimed ending left behind, taken from its
     * retirement receipt exactly once: the unexpected causes admitted format
     * callbacks reported, then the cleanup failures. Awaited under
     * `NonCancellable`: the claimant may be another thread still closing,
     * another thread may still own the ending's notification delivery, and an
     * admitted callback may not have reported yet, and none of them is joined
     * any other way. The session only completes the receipt, so this never waits
     * on its own work. Nothing is owed, and nothing is awaited, when no ending
     * was claimed.
     */
    private suspend fun retirementRoots(): List<Throwable> = retired()?.let { it.causes + it.cleanup }.orEmpty()

    /** The receipt itself, taken exactly once, under `NonCancellable`; null when none is owed or it was already taken. */
    private suspend fun retired(): RetirementReceipt? {
        if (receiptTaken || session.termination == null) return null
        receiptTaken = true
        return withContext(NonCancellable) { session.retirement() }
    }

    /**
     * Retires the live ownership exactly once, AFTER the capture cleanup has
     * been attempted and whatever it raised has been collected: the owner
     * attempts both handles under `NonCancellable`, so neither a capture
     * cleanup failure nor the first removal's failure bypasses the second
     * attempt. Its typed roots are carried once; a later call carries nothing.
     * The sender is borrowed and is never closed here.
     */
    private suspend fun retireOwnership(): List<Throwable> {
        val owned = ownership ?: return emptyList()
        if (retired) return emptyList()
        retired = true
        return owned.retire().roots
    }

    /**
     * One drain, owned by [this] scope, whose completion handler emits EXACTLY
     * one receipt -- normal return, failure, `Error` or cancellation alike --
     * carrying the outcome that child wrote for itself and the actual cause
     * the completion boundary reports.
     */
    private fun CoroutineScope.drainChild(
        source: CaptureHandoff,
        lane: TrackLane,
        receipts: Channel<Receipt>,
        undelivered: MutableList<Throwable>,
    ): Job {
        val slot = OutcomeSlot()
        val child =
            async {
                val drained = drainTrack(lane.kind, source, lane.pipeline, lane.sink, writeTimeout, report)
                slot.set(drained)
                drained
            }
        child.reportCompletion { cause ->
            val sent = receipts.trySend(Receipt.Drained(lane.kind, slot.get(), cause))
            if (sent.isFailure && !sent.isClosed) {
                // Impossible by construction -- three one-shot producers, three
                // slots -- so it is recorded rather than dropped.
                synchronized(undelivered) {
                    undelivered.add(IllegalStateException("the ${lane.kind} receipt could not be delivered"))
                }
            }
        }
        return child
    }

    /**
     * Tells the session a track ended, collecting whatever its own cleanup
     * raised so that failure leaves on THIS coordinator's frame with its
     * identity intact. A cancellation is not collected: it is this broadcast
     * being stopped, and it propagates.
     */
    private fun endTrackOnSession(kind: MediaKind): Throwable? =
        try {
            session.onTrackEnded(session.generation, kind)
            null
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (raised: Exception) {
            raised
        } catch (error: Error) {
            error
        }

    /**
     * Stops failed or cancelled capture with a NON-NULL reason -- `stop(null)`
     * would decide a Completed the broadcast never had -- and returns the
     * cleanup ROOTS that stop produced.
     *
     * Root selection: if what comes back IS this adapter reason, its DIRECT
     * suppressed entries are the roots; if it is a distinct real failure, that
     * carrier is itself a root and its inherited structure is left exactly as
     * the callee built it -- cycles included. Nothing is flattened, nothing is
     * rewritten, and the reason is never a root.
     */
    private suspend fun stopFailedCapture(why: String): List<Throwable> {
        if (stopped) return emptyList()
        stopped = true
        val reason = BroadcastStopped(why)
        var returned: Exception? = null
        val raised = mutableListOf<Throwable>()
        withContext(NonCancellable) {
            try {
                returned = session.stop(reason)
            } catch (cancelled: CancellationException) {
                raised.add(cancelled)
            } catch (failure: Exception) {
                raised.add(failure)
            } catch (error: Error) {
                raised.add(error)
            }
        }
        val carrier = raised.firstOrNull() ?: returned
        if (carrier == null || carrier === reason) {
            return reason.suppressed.filter { it !== reason }.toList()
        }
        return listOf(carrier)
    }

    /**
     * Whether [cause] is THIS coordinator's own cancellation of a child.
     *
     * Both halves matter. The identity half is [why] itself -- never a message
     * -- and the CATEGORY half is that only a cancellation can be one: an
     * ordinary failure or an `Error` raised while a child handled that
     * cancellation is a REAL failure that merely carries it as its cause, and
     * ignoring it would lose that failure entirely.
     */
    private fun ownCancellation(cause: Throwable, why: CancellationException): Boolean =
        cause is CancellationException && (cause === why || cause.cause === why)

    /**
     * Whether [candidate] is the SAME stopping as [outgoing] rather than
     * separate evidence.
     *
     * A child cancelled BY what is leaving carries it in its own causal chain;
     * a child that cancelled itself earlier, for its own reason, does not. The
     * walk is identity-visited and terminates on a cycle, and it only reads the
     * graph the callee built.
     */
    private fun propagatedFrom(candidate: Throwable, outgoing: Throwable): Boolean {
        if (candidate !is CancellationException) return false
        return reaches(candidate, outgoing) || reaches(outgoing, candidate)
    }

    /** Walks [from]'s causal chain for [target], visiting by identity and stopping on a cycle. */
    private fun reaches(from: Throwable, target: Throwable): Boolean {
        val seen = mutableListOf<Throwable>()
        var at: Throwable? = from
        while (at != null && seen.none { it === at }) {
            if (at === target) return true
            seen.add(at)
            at = at.cause
        }
        return false
    }

    /** Attaches each distinct root once, by identity, and never the primary itself. */
    private fun carry(primary: Throwable, roots: List<Throwable>) {
        val attached = primary.suppressed.toMutableList()
        for (root in roots) {
            if (root === primary || root is BroadcastStopped) continue
            if (attached.any { it === root }) continue
            attached.add(root)
            primary.addSuppressed(root)
        }
    }
}
