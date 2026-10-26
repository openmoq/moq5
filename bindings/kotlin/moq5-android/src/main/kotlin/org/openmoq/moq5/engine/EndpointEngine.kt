package org.openmoq.moq5.engine

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.openmoq.moq5.EndpointClosedException
import org.openmoq.moq5.EndpointSnapshot
import org.openmoq.moq5.ReceiverConfig
import org.openmoq.moq5.EndpointState
import org.openmoq.moq5.MoqResultCode
import org.openmoq.moq5.MediaObject
import org.openmoq.moq5.MediaTrack
import org.openmoq.moq5.ReceiverClosedException
import org.openmoq.moq5.ReceiverFatalCode
import org.openmoq.moq5.ReceiverFatalException
import org.openmoq.moq5.ReceiverRecordEvent
import org.openmoq.moq5.ReceiverStats
import org.openmoq.moq5.ReceiverTerminal
import org.openmoq.moq5.ObjectRejectedException
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.SendTrack
import org.openmoq.moq5.SendTrackConfig
import org.openmoq.moq5.SendTrackUnavailableException
import org.openmoq.moq5.SenderClosedException
import org.openmoq.moq5.SenderCompletedException
import org.openmoq.moq5.SenderConfig
import org.openmoq.moq5.SenderInterruptedException
import org.openmoq.moq5.SenderNotReadyException
import org.openmoq.moq5.SenderStats
import org.openmoq.moq5.TrackConfigRejectedException
import org.openmoq.moq5.TrackNameBusyException
import org.openmoq.moq5.VodConversion
import org.openmoq.moq5.VodConversionRejectedException
import org.openmoq.moq5.WriteOutcome
import org.openmoq.moq5.StartMode
import org.openmoq.moq5.TrackNotSubscribableException
import org.openmoq.moq5.TrackPresence
import org.openmoq.moq5.TrackState
import org.openmoq.moq5.TrackUnavailableException

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.coroutines.Continuation
import kotlin.coroutines.coroutineContext
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withContext
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.suspendCancellableCoroutine

/** How the engine answered a submission. */
internal enum class SubmitOutcome {
    /** Queued; the body WILL run exactly once. */
    ACCEPTED,

    /** Shutdown had already begun; the body will never run. */
    REFUSED_SHUTDOWN,
}

/**
 * The per-endpoint confinement engine: one dedicated OS thread that is "the app
 * thread" as far as the C service tier is concerned. Coroutine surfaces enqueue
 * work onto it and suspend.
 *
 * Confinement covers stateful endpoint work and the blocking calls:
 * `waitForActivity()`, `stop()` and `destroy()` are made only from the engine
 * thread. `wake()` is the deliberate exception and is called from the
 * SUBMITTING thread: `moq_endpoint_wake()` is documented as callable from any
 * thread, non-allocating and infallible, precisely so a caller can record its
 * command and then nudge without marshalling. A future backend must keep that
 * property -- it must not marshal, queue or poll the wake onto another thread.
 *
 * ## One lock
 *
 * A single [ReentrantLock] guards the phase, the FIFO queue and each job's
 * queued/claimed/cancelled state, the close waiters, the retirement latch, and
 * the engine-side wake level. Two orderings are load-bearing:
 *
 *  * **every accepted commit publishes its wake under that same lock**, on the
 *    submitting thread, so an accepted submission can never wake an endpoint
 *    that has since been retired -- retirement takes the same lock;
 *  * **retirement is latched, and `stop()`/`destroy()` are called, while the
 *    lock is held**, so no any-thread path can pass a stale liveness check.
 *
 * The worker never holds the lock while running a body or while blocked in
 * `waitForActivity()`.
 *
 * ## Cancellation
 *
 * Kotlin's prompt cancellation, not a transliteration of the Swift precedent:
 *
 *  * [perform] cancelled BEFORE the job is claimed enqueues nothing, wakes
 *    nothing and prevents the body;
 *  * [perform] cancelled AFTER the claim never abandons the body -- it still
 *    completes exactly once -- but the caller resolves exactly once as
 *    cancellation, and neither a value nor a later exception reaches it;
 *  * [postAndWait] and [close] are teardown-grade and immune to caller
 *    cancellation in both windows;
 *  * [performCommitted] is commit-grade: cancellation BEFORE submission
 *    enqueues nothing; once accepted the body runs exactly once and its
 *    result -- value or exception -- is delivered to the caller whether or
 *    not the caller was cancelled meanwhile (queued or in flight). It exists
 *    for bodies whose native call decides ownership of what they carry
 *    (a media write) or creates a native resource whose only handle is
 *    the result (a receiver or sender attach, a catalog track).
 *
 * ## Retained work
 *
 * A demand that finds its native queue empty, or a write refused with
 * WOULD_BLOCK under the return-would-block policy, is not answered to
 * its caller and resubmitted later: the worker RETAINS it and re-attempts
 * it on its own passes, resolving the caller's deferred on progress. A
 * retry never publishes a wake, so an empty pass cannot feed the next
 * one; only the initial submission and the endpoint's genuine activity
 * wake the worker.
 *
 *
 * ## Body contract, by shape
 *
 * [post] and [postAndWait] bodies are LIBRARY-OWNED and must not throw: they
 * have no caller to receive an exception -- `post` has no waiter at all, and
 * `postAndWait` reports only an acceptance outcome -- so a throwing body would
 * have nowhere to go but the service thread, where it would strand every other
 * waiter on this endpoint. [perform] is the throwing shape: it delivers the
 * body's exception to its caller and the worker survives.
 *
 * [perform] is a general-purpose command primitive and MUST NOT be used to hand
 * back transfer-owned native state: a cancelled caller drops the value, so
 * ownership would leak or a media object would be consumed with nothing to show
 * for it. Object transfer belongs to the later `ExactStream` reservation/commit
 * slot, where the JVM-owned item is committed before resume and survives a
 * cancelled collector.
 */
internal class EndpointEngine(
    private val factory: () -> EndpointBackend,
    /**
     * Publish one coherent snapshot record per endpoint activity. When set, the
     * worker takes ONE backend snapshot right after the root is published (the
     * seed) and one after every wait that returns ACTIVITY -- never on a
     * timeout, never on a timer -- and publishes it to [snapshots]. A bridge
     * refusal on that background read quarantines exactly as an explicit
     * [snapshot] refusal does. Off by default so the engine's own contract
     * rows keep their exact backend call order.
     */
    private val publishSnapshots: Boolean = false,
) {
    /**
     * Convenience for a backend that already exists. The factory contract is
     * unchanged -- it is still invoked on the worker, before the worker makes
     * any endpoint call -- so both forms exercise the same construction order.
     */
    constructor(backend: EndpointBackend) : this({ backend })

    private enum class Phase { RUNNING, STOPPING, RETIRED }

    /** All state is guarded by the engine lock; [body] runs outside it. */
    private class Job(val body: () -> Any?) {
        var claimed = false
        var cancelled = false
        var onDone: ((Result<Any?>) -> Unit)? = null
    }

    private val lock = ReentrantLock()

    /**
     * The engine's own wake. It exists because the backend's wake cannot be
     * relied on once the endpoint is terminal -- `moq_endpoint_wake()` is a
     * documented no-op there -- while an accepted engine command must still
     * run, and `close()` must still be able to reach a parked worker.
     */
    private val idle = lock.newCondition()

    private val jobs = ArrayDeque<Job>()
    private var phase = Phase.RUNNING
    private var retired = false

    /**
     * True once the first retirement pass has SETTLED: stop and destroy (or
     * the quarantine that replaced them) are known. [retired] latches earlier
     * -- before any teardown call -- and while a drain turn granted before
     * retirement is still completing, the two differ: no new backend work is
     * admitted, but a close arriving then must join the wait, not return.
     */
    private var retirementSettled = false
    private var level = false

    /** The last backend wait reported CLOSED or the sticky INTERRUPTED latch. */
    private var backendUnusable = false

    /**
     * Drain callers active or queued. While nonzero the worker enters no
     * backend wait: the C drain waits on the endpoint itself and would consume
     * the activity the record stream is built from.
     */
    private var drainDemand = 0

    /** Exactly one native drain at a time; later callers wait their turn. */
    private var nativeDrainActive = false

    /** True whenever the worker is not inside a backend wait. */
    private var workerOffWait = true

    /**
     * The worker owes one observation when it resumes after a native drain
     * that consumed the endpoint's activity: retained work is re-attempted
     * (mechanics, always) and, when publication is on, a record is taken.
     */
    private var postDrainWorkDue = false

    /** True once a native drain has run during the current quiesce: it consumed the endpoint's wait. */
    private var nativeDrainConsumedWait = false

    /**
     * The one receiver attached under this root, by its child token, or null.
     * The C tier admits one receiver per endpoint and REFUSES to stop while
     * it is attached, so retirement closes it before the stop.
     */
    private var receiverToken: Long? = null

    /** The durable state of the attached receiver, alive exactly as long as [receiverToken]. */
    private var receiverModel: ReceiverModel? = null

    /** The one sender attached under this root, by its child token, or null; closed before the stop like the receiver. */
    private var senderToken: Long? = null

    /** The durable state of the attached sender, alive exactly as long as [senderToken]. */
    private var senderModel: SenderModel? = null

    /**
     * Work the worker RETAINS on the application's behalf: a demand whose
     * native queue was empty, or a write refused with WOULD_BLOCK under the
     * return-would-block policy. The worker re-attempts every retained entry
     * on each of its own passes -- after an activity wait returns, and after
     * a native drain's observation -- and resolves the caller's deferred once
     * progress is possible. A retry therefore never submits a job and never
     * publishes a wake: the only wakes are the initial submission's and the
     * endpoint's genuine activity, which is what ends the self-fed cycle of
     * retry, wake, empty pass, retry. Retirement (the caller's cancellation
     * or timeout, the owner's close, quarantine, engine close) removes an
     * entry; a value its attempt had already committed is kept for the
     * retiring caller. Guarded by the engine lock.
     */
    private val retained = mutableListOf<Retained<*>>()

    /**
     * Resolutions a job BODY owes to retained callers (an owner closed under
     * it). They run on the worker outside any body, so a caller resumed
     * inline is an ordinary caller again and may call the engine.
     */
    private val pendingResolutions = mutableListOf<() -> Unit>()

    /**
     * Where the blocking native drain runs: off the worker, off the caller's
     * dispatcher, and detached from the caller's cancellation -- a cancelled
     * caller stops waiting for the result; the bounded native call completes
     * under this engine's accounting.
     */
    private val drainScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    private class DrainTicket {
        var cancelled = false
    }

    private val closeWaiters = mutableListOf<(Throwable?) -> Unit>()

    /** The published records; created at the seed, on the worker. */
    @Volatile private var snapshotFlow: MutableStateFlow<EndpointSnapshot>? = null

    /**
     * The activity-driven record stream. Available once construction has
     * succeeded with [publishSnapshots] set; reading it earlier is a caller
     * error.
     */
    internal val snapshots: StateFlow<EndpointSnapshot>
        get() = snapshotFlow ?: error("no snapshot stream: the root is not published or publication is off")

    /**
     * Waiters for ESTABLISHED, resolved from the worker on each published
     * record, on quarantine, and on close. Guarded by the engine lock.
     */
    private val establishWaiters = mutableListOf<EstablishWaiter>()

    private class EstablishWaiter(val resume: (Result<EndpointSnapshot>) -> Unit) {
        var done = false
    }

    /**
     * Construction, DECIDED once: the root was published, the factory failed
     * with a cause, or the engine was closed before creation. Every
     * [awaitConstructed] caller learns that one decision, and a job accepted
     * before a failed construction is resolved with the same cause rather
     * than left in the queue when the worker retires.
     */
    private val constructWaiters = mutableListOf<(Throwable?) -> Unit>()
    private var constructDecided = false
    private var constructOutcome: Throwable? = null

    /**
     * The endpoint, created BY THE WORKER before it performs any endpoint
     * operation, so the same exact thread creates, waits, snapshots, stops and
     * destroys the root. Null until then, and null forever if construction
     * failed -- in which case there is nothing to tear down.
     */
    private var backendOrNull: EndpointBackend? = null

    /**
     * Set when the factory itself failed or was cancelled before it produced a
     * root. There is then no native ownership at all: no teardown call is made,
     * and every waiter learns this exact cause.
     */
    private var constructionFailure: Exception? = null

    /** A wake raised before the root existed, owed to it once it does. */
    private var wakePending = false

    /**
     * Set when the native stop was REFUSED. The endpoint is still live and was
     * deliberately NOT destroyed, so no backend call may follow -- this is a
     * quarantine, not a reclamation. Latched under the lock before any
     * concurrent path can observe retirement.
     */
    private var stopRefusal: Exception? = null

    /**
     * Set when the BRIDGE answered something its contract forbids -- a private
     * refusal, a result outside the wait domain, or a refused wake, snapshot or
     * post-stop destroy. Written and read only on the engine worker, before
     * retirement latches the same failure under the lock for every waiter.
     */
    private var bridgeFailure: Exception? = null

    /**
     * True once NO NEW BACKEND CALL MAY BE ADMITTED. One carve-out: a native
     * drain turn granted before retirement may still be completing, and
     * destroy waits for it.
     *
     * Deliberately not "destroy was called": on a refused stop the endpoint is
     * quarantined rather than destroyed, and that state is equally final from
     * the backend's point of view. The name is kept because that is exactly
     * what every caller uses it for.
     */
    internal val isBackendRetired: Boolean
        get() = lock.withLock { retired }

    /** The refusal this engine is quarantined by, or null. */
    internal val quarantineFailure: Exception?
        get() = lock.withLock { stopRefusal ?: bridgeFailure }

    /** The one worker, retained so reentry is decided by exact identity. */
    private val worker: Thread =
        Thread({ run() }, "moq.endpoint.service").apply { isDaemon = true }

    /**
     * True only while [worker] is inside a job body. It brackets body execution
     * and NOT the later result delivery, so a continuation resumed inline on the
     * worker may still call the engine.
     */
    @Volatile private var executingBody: Boolean = false

    init {
        // Eagerly, so an idle engine parks in the backend wait with no
        // submission needed to get it started.
        worker.start()
    }

    /**
     * Refuse a suspending engine call made synchronously from an executing
     * body. Such a call waits for the very worker that is inside the body, so
     * it would deadlock the endpoint permanently; failing at the point of
     * misuse is the only outcome that stays diagnosable.
     *
     * `post` is deliberately not guarded: it is non-blocking FIFO tail work.
     */
    private fun rejectBodyReentry(call: String) {
        if (Thread.currentThread() === worker && executingBody) {
            throw EngineReentryException(call)
        }
    }

    // -- submission -----------------------------------------------------------

    /**
     * Fire-and-forget; accepted while running, refused once shutdown starts OR
     * once the engine is quarantined.
     *
     * A quarantine means the bridge will not honour this engine's token again,
     * so a body accepted after it would be work the engine has already been
     * told it cannot perform. Bodies accepted BEFORE the quarantine still drain
     * under the shutdown rule -- that is a separate policy and is not decided
     * here.
     */
    fun post(body: () -> Unit): SubmitOutcome =
        lock.withLock {
            if (phase != Phase.RUNNING || bridgeFailure != null) {
                return SubmitOutcome.REFUSED_SHUTDOWN
            }
            jobs.addLast(Job(body))
            wakeLocked()
            SubmitOutcome.ACCEPTED
        }

    /** Run [body] on the service thread and return its result. */
    suspend fun <T> perform(body: () -> T): T {
        rejectBodyReentry("perform()")
        // A caller already cancelled must leave no job and publish no wake.
        coroutineContext.ensureActive()
        val job = Job { body() }
        @Suppress("UNCHECKED_CAST")
        return suspendCancellableCoroutine { cont ->
            var refusal: Exception? = null
            var refused = false
            lock.withLock {
                if (phase != Phase.RUNNING || bridgeFailure != null) {
                    refused = true
                    // A quarantine is reported as the RETAINED bridge failure,
                    // never as EngineClosedException: the engine did not shut
                    // down normally, and saying so would misdescribe why the
                    // body will not run.
                    refusal = bridgeFailure
                } else {
                    job.onDone = { result ->
                        result.fold({ cont.resume(it as T) }, { cont.resumeWithException(it) })
                    }
                    jobs.addLast(job)
                    wakeLocked()
                }
            }
            if (refused) {
                cont.resumeWithException(refusal ?: EngineClosedException())
                return@suspendCancellableCoroutine
            }
            cont.invokeOnCancellation {
                // Arbitrate queued-versus-claimed under the lock, and nothing
                // more. The handler runs BECAUSE the continuation is already
                // cancelled, so Kotlin owns that resolution and the caller's own
                // cancellation cause: a second resume with a fabricated
                // exception would be dead at best, and cause-replacing at worst.
                lock.withLock {
                    if (!job.claimed && !job.cancelled) {
                        job.cancelled = true
                        jobs.remove(job) // never executes, never reorders survivors
                        job.onDone = null
                    }
                }
            }
        }
    }

    /**
     * Commit-grade submission for a body whose native call decides ownership
     * of what it carries (a media write: transfer on success). Cancellation
     * is honoured only BEFORE submission; once the job is accepted the caller
     * is immune until the body's result -- value or exception -- is delivered,
     * so a caller cancelled while its job is queued or in flight still learns
     * exactly what the body did. Refused with the retained bridge failure or
     * [EngineClosedException] when the engine is not running.
     */
    suspend fun <T> performCommitted(body: () -> T): T {
        rejectBodyReentry("performCommitted()")
        coroutineContext.ensureActive()
        val job = Job { body() }
        @Suppress("UNCHECKED_CAST")
        return suspendCoroutine { cont: Continuation<T> ->
            var refusal: Exception? = null
            var refused = false
            lock.withLock {
                if (phase != Phase.RUNNING || bridgeFailure != null) {
                    refused = true
                    refusal = bridgeFailure
                } else {
                    job.onDone = { result ->
                        result.fold({ cont.resume(it as T) }, { cont.resumeWithException(it) })
                    }
                    jobs.addLast(job)
                    wakeLocked()
                }
            }
            if (refused) cont.resumeWithException(refusal ?: EngineClosedException())
        }
    }

    /**
     * One coherent snapshot, taken ON THE WORKER through [perform] so the same
     * thread that created the root also reads it -- no any-thread read races a
     * concurrent teardown.
     *
     * A bridge refusal QUARANTINES the native ownership exactly as a refused
     * wake or wait does, and is then reported to this caller and to every close
     * waiter as the same stable failure. It is never mapped onto a snapshot
     * value, which would invent a state the endpoint never reported.
     */
    suspend fun snapshot(): EndpointSnapshot =
        perform {
            val b = backendOrNull ?: error("the endpoint root was never created")
            when (val r = b.snapshot()) {
                is BackendSnapshotResult.Ok -> r.snapshot
                is BackendSnapshotResult.Bridge -> throw quarantine(r.failure)
            }
        }

    /**
     * Latches the quarantine and returns the failure to throw.
     *
     * Callable from the worker or a submitting thread; the latch is taken under
     * the lock so no concurrent path can slip another backend call past it.
     */
    private fun quarantine(failure: BridgeFailure): Exception {
        var resumptions: List<() -> Unit> = emptyList()
        val result =
            lock.withLock {
                val existing = bridgeFailure
                val latched =
                    existing ?: BridgeContractException(failure).also {
                        bridgeFailure = it
                        idle.signalAll() // the worker must leave any backend park
                    }
                resumptions = judgeEstablishLocked(null, latched) + failRetainedLocked(latched)
                latched
            }
        resumptions.forEach { it() }
        return result
    }

    /** Teardown-grade submission: immune to caller cancellation. */
    suspend fun postAndWait(body: () -> Unit): SubmitOutcome {
        rejectBodyReentry("postAndWait()")
        val job = Job(body)
        return suspendCoroutine { cont: Continuation<SubmitOutcome> ->
            var accepted = false
            lock.withLock {
                if (phase == Phase.RUNNING && bridgeFailure == null) {
                    accepted = true
                    job.onDone = { cont.resume(SubmitOutcome.ACCEPTED) }
                    jobs.addLast(job)
                    wakeLocked()
                }
            }
            if (!accepted) cont.resume(SubmitOutcome.REFUSED_SHUTDOWN)
        }
    }

    /**
     * Suspend until the root exists. Throws the construction cause when the
     * factory failed, or [EngineClosedException] when the engine was closed
     * before creation. Resolves exactly once per caller, from the decision the
     * worker made under the lock, and never after a partial construction.
     */
    suspend fun awaitConstructed() {
        rejectBodyReentry("awaitConstructed()")
        suspendCoroutine { cont: Continuation<Unit> ->
            var decided = false
            var outcome: Throwable? = null
            lock.withLock {
                if (constructDecided) {
                    decided = true
                    outcome = constructOutcome
                } else {
                    constructWaiters += { t ->
                        if (t != null) cont.resumeWithException(t) else cont.resume(Unit)
                    }
                }
            }
            if (decided) {
                val o = outcome
                if (o != null) cont.resumeWithException(o) else cont.resume(Unit)
            }
        }
    }

    /**
     * Suspend until a published record reports ESTABLISHED and return it.
     * Throws [EndpointClosedException] with the exact terminal when a record is
     * terminal, [EngineClosedException] when the engine was closed (locally,
     * with no terminal record), and the retained bridge failure when the
     * background read quarantined the endpoint. No native read is taken here:
     * the answer comes from the worker's records. Cancellation removes this
     * waiter only.
     */
    suspend fun awaitEstablished(): EndpointSnapshot {
        rejectBodyReentry("awaitEstablished()")
        coroutineContext.ensureActive()
        return suspendCancellableCoroutine { cont ->
            var immediate: Result<EndpointSnapshot>? = null
            val waiter = EstablishWaiter { r -> r.fold({ cont.resume(it) }, { cont.resumeWithException(it) }) }
            lock.withLock {
                val failure = bridgeFailure
                val current = snapshotFlow?.value
                immediate =
                    when {
                        failure != null -> Result.failure(failure)
                        phase != Phase.RUNNING -> Result.failure(EngineClosedException())
                        current != null && current.state == EndpointState.Established -> Result.success(current)
                        current?.terminal != null -> Result.failure(EndpointClosedException(current.terminal))
                        else -> null
                    }
                if (immediate == null) establishWaiters += waiter
            }
            immediate?.let { waiter.resume(it) }
            cont.invokeOnCancellation {
                lock.withLock {
                    waiter.done = true
                    establishWaiters.remove(waiter)
                }
            }
        }
    }

    /**
     * Caller holds the lock. Judges the latest record for the establishment
     * waiters and returns the resumptions to run OUTSIDE the lock.
     */
    private fun judgeEstablishLocked(record: EndpointSnapshot?, failure: Throwable?): List<() -> Unit> {
        val outcome: Result<EndpointSnapshot>? =
            when {
                failure != null -> Result.failure(failure)
                record == null -> null
                record.state == EndpointState.Established -> Result.success(record)
                record.terminal != null -> Result.failure(EndpointClosedException(record.terminal))
                else -> null
            }
        if (outcome == null) return emptyList()
        val taken = establishWaiters.toList()
        establishWaiters.clear()
        return taken.map { w -> { if (!w.done) { w.done = true; w.resume(outcome) } } }
    }

    /**
     * Attach a receiver on the service worker and record its child token.
     * The endpoint's own refusal is thrown as [ReceiverRefusedException]; a
     * bridge refusal quarantines exactly as a refused snapshot does.
     *
     * Commit-grade ([performCommitted]): the body creates a native child
     * whose only handle is its result, so an accepted submission finishes and
     * delivers that handle -- or its error -- to the caller whether or not
     * the caller was cancelled while queued, claimed, after the native attach
     * succeeded, or while the result was already dispatched. Cancellation
     * before acceptance creates nothing.
     */
    suspend fun attachReceiver(config: ReceiverConfig): AttachedReceiver =
        performCommitted {
            val b = backendOrNull ?: error("the endpoint root was never created")
            when (val r = b.attachReceiver(config)) {
                is BackendAttachResult.Attached -> {
                    val model = ReceiverModel(config.eventHistory)
                    lock.withLock {
                        // The C tier admits one receiver per endpoint; a second
                        // attach is its WRONG_STATE, never a second token here.
                        check(receiverToken == null) { "the bridge attached a second receiver under one root" }
                        receiverToken = r.token
                        receiverModel = model
                    }
                    AttachedReceiver(r.token, model)
                }
                is BackendAttachResult.Refused -> throw ReceiverRefusedException(r.code)
                is BackendAttachResult.Bridge -> throw quarantine(r.failure)
            }
        }

    /** Whether [token] is the receiver currently attached under this root. */
    fun isReceiverAttached(token: Long): Boolean = lock.withLock { receiverToken == token }

    /**
     * Close the receiver [token] on the service worker: the child is retired,
     * the root stays live and its receiver slot is free. Idempotent -- a
     * token that is not the attached receiver is a no-op.
     */
    suspend fun closeReceiver(token: Long) {
        perform {
            val mine = lock.withLock { receiverToken == token }
            if (!mine) return@perform
            val b = backendOrNull ?: error("the endpoint root was never created")
            when (val r = b.closeReceiver(token)) {
                is BackendOpResult.Ok -> {
                    val model = lock.withLock { receiverModel.also { receiverToken = null; receiverModel = null } }
                    model?.retire()
                    if (model != null) lock.withLock { retireOwnerLocked(model) }
                }
                is BackendOpResult.Bridge -> throw quarantine(r.failure)
            }
        }
    }

    /**
     * One object attempt on the service worker: the ordinary track drain,
     * then ONE poll, with the lock released around the poll. A dequeued
     * object is staged in the receiver's handoff slot (never returned, since
     * a cancelled caller would drop it). The C queues a track's ADDED before
     * any object for it, but that event may have arrived after this attempt's
     * drain, so an object whose track the inventory does not know yet is
     * answered by draining AGAIN and resolving again; only a handle still
     * unknown after that is the bridge-contract failure that quarantines.
     * CLOSED is THIS queue's exhaustion: the terminal is recorded, the track
     * ring's final tail is integrated first, and the queue is marked so it is
     * never polled again. Returns null when the queue was empty: no progress.
     */
    private fun attemptObject(b: EndpointBackend, token: Long, model: ReceiverModel): ObjectDemand? {
        var failure: BridgeFailure? = null
        val before: ObjectDemand? =
            lock.withLock {
                when {
                    receiverToken != token || model.isRetired -> ObjectDemand.Closed
                    model.hasHandoff() -> ObjectDemand.Delivered
                    drainReceiverLocked(b) -> {
                        failure = (bridgeFailure as? BridgeContractException)?.failure
                        null
                    }
                    model.isExhausted(ReceiverModel.NativeQueue.Objects) -> ObjectDemand.Closed
                    else -> ObjectDemand.Poll
                }
            }
        if (before == null) throw quarantine(failure ?: BridgeFailure("receiver-poll-object", 0))
        if (before !is ObjectDemand.Poll) return before
        // The poll itself runs with the lock RELEASED: the backend may block
        // inside it (a real poll never does, a scripted one may be held), and
        // only this worker makes stateful backend calls, so nothing races the
        // receiver's state meanwhile.
        val polled = b.pollObject(token)
        var progress = false
        val outcome: ObjectDemand? =
            lock.withLock {
                when (polled) {
                    is BackendObjectResult.Object -> {
                        progress = true
                        val o = polled.value
                        var track = model.resolve(o.track)
                        if (track == null && !drainReceiverLocked(b)) track = model.resolve(o.track)
                        if (track == null) {
                            failure = (bridgeFailure as? BridgeContractException)?.failure ?: BridgeFailure("receiver-object-track", 0)
                            backendUnusable = true
                            null
                        } else {
                            model.stageHandoff(
                                MediaObject(track, o.payload, o.status, o.endOfGroup, o.viaDatagram, o.keyframe, o.captureTime, o.decodeTime, o.compositionOffset, o.presentationTime, o.identity),
                            )
                            ObjectDemand.Delivered
                        }
                    }
                    is BackendObjectResult.Empty -> null
                    is BackendObjectResult.Interrupted -> {
                        progress = true
                        ObjectDemand.Interrupted
                    }
                    is BackendObjectResult.Closed -> {
                        progress = true
                        model.terminal(if (polled.fatal) ReceiverTerminal.Fatal(ReceiverFatalCode(polled.code)) else ReceiverTerminal.Closed)
                        // The final track tail before this stream may see the end.
                        if (drainReceiverLocked(b)) {
                            failure = (bridgeFailure as? BridgeContractException)?.failure
                            null
                        } else {
                            model.markExhausted(ReceiverModel.NativeQueue.Objects)
                            ObjectDemand.Closed
                        }
                    }
                    is BackendObjectResult.Bridge -> {
                        progress = true
                        failure = polled.failure
                        backendUnusable = true
                        null
                    }
                }
            }
        if (progress && outcome == null) throw quarantine(failure ?: BridgeFailure("receiver-poll-object", 0))
        return outcome
    }

    /**
     * One object demand: an attempt on the worker now; when the queue is
     * empty the demand is RETAINED and re-attempted on the worker's later
     * passes, and this call suspends until an object is staged, the latch
     * answers, or this queue's own CLOSED arrives. Cancelling the caller
     * retires the demand; an object already staged stays in the handoff slot
     * for the next demand.
     */
    suspend fun demandObject(token: Long, model: ReceiverModel): ObjectDemand {
        val entry =
            Retained<ObjectDemand>(
                owner = model,
                attempt = { b, _ -> attemptObject(b, token, model) },
                onOwnerRetired = { Result.success(ObjectDemand.Closed) },
            )
        val demanded: Demanded<ObjectDemand> =
            try {
                perform {
                    val b = backendOrNull ?: error("the endpoint root was never created")
                    attemptObject(b, token, model)?.let { Demanded.Resolved(it) } ?: lock.withLock { retainLocked(entry) }
                }
            } catch (e: CancellationException) {
                retireRetainedNow(entry)
                throw e
            }
        return when (demanded) {
            is Demanded.Resolved -> demanded.value
            is Demanded.Pending -> awaitRetained(demanded.retained)
        }
    }

    // -- the receiver's manual controls ------------------------------------------
    //
    // Commands (subscribe, unsubscribe) and the state read run on the worker
    // behind the ordinary track drain, so the model's presence and terminal
    // are current before anything is decided. A refusal code from the C
    // carries no payload: CLOSED and WRONG_STATE answered while the model
    // shows the receiver live / the track Present make the engine drain AGAIN
    // and decide from what it learned; a code the drain cannot explain is a
    // bridge-contract failure -- never an invented terminal or presence.

    /** What the drain-then-check prelude found: null means proceed to the backend. */
    private fun controlPreludeLocked(b: EndpointBackend, token: Long, model: ReceiverModel, track: MediaTrack, mutating: Boolean): Throwable? {
        if (receiverToken != token) return ReceiverClosedException()
        if (drainReceiverLocked(b)) return bridgeFailure
        when (val t = model.recordedTerminal) {
            null -> Unit
            is ReceiverTerminal.Fatal -> return if (mutating) ReceiverFatalException(t.code) else null
            is ReceiverTerminal.Closed -> return if (mutating) ReceiverClosedException() else null
        }
        if (track.presence == TrackPresence.Removed) return TrackUnavailableException(TrackPresence.Removed)
        return null
    }

    /** Caller holds the lock. Explain a refused command by draining again; null when the code cannot be explained. */
    private fun explainRefusalLocked(b: EndpointBackend, op: String, code: Int, model: ReceiverModel, track: MediaTrack): Throwable {
        when (code) {
            MOQ_ERR_WRONG_STATE -> {
                if (drainReceiverLocked(b)) return bridgeFailure!!
                return when (track.presence) {
                    TrackPresence.Removed, TrackPresence.Ended -> TrackUnavailableException(track.presence)
                    TrackPresence.Present -> quarantineLocked(BridgeFailure("$op-wrong-state", code))
                }
            }
            MOQ_ERR_CLOSED -> {
                if (drainReceiverLocked(b)) return bridgeFailure!!
                return when (val t = model.recordedTerminal) {
                    is ReceiverTerminal.Fatal -> ReceiverFatalException(t.code)
                    is ReceiverTerminal.Closed -> ReceiverClosedException()
                    null -> quarantineLocked(BridgeFailure("$op-closed", code))
                }
            }
            MOQ_ERR_UNSUPPORTED -> return TrackNotSubscribableException()
            else -> return quarantineLocked(BridgeFailure(op, code))
        }
    }

    /** Caller holds the lock. Latch the quarantine and hand back its stable exception (establishment waiters are judged by the caller's unlock path). */
    private fun quarantineLocked(failure: BridgeFailure): Throwable {
        val latched = bridgeFailure ?: BridgeContractException(failure).also { bridgeFailure = it }
        backendUnusable = true
        idle.signalAll()
        return latched
    }

    suspend fun subscribeTrack(token: Long, model: ReceiverModel, track: MediaTrack, start: StartMode, priority: UByte?) {
        perform {
            val b = backendOrNull ?: error("the endpoint root was never created")
            val outcome: Throwable? =
                lock.withLock {
                    controlPreludeLocked(b, token, model, track, mutating = true)?.let { return@withLock it }
                    if (track.presence == TrackPresence.Ended) return@withLock TrackUnavailableException(TrackPresence.Ended)
                    if (model.isDesired(track.token)) return@withLock null
                    when (val r = b.subscribeTrack(token, track.token, start, priority)) {
                        is BackendCommandResult.Ok -> {
                            model.setDesired(track.token, true)
                            null
                        }
                        is BackendCommandResult.Refused -> explainRefusalLocked(b, "track-subscribe", r.code.value, model, track)
                        is BackendCommandResult.Bridge -> quarantineLocked(r.failure)
                    }
                }
            outcome?.let { throw settleQuarantine(it) }
        }
    }

    suspend fun unsubscribeTrack(token: Long, model: ReceiverModel, track: MediaTrack) {
        perform {
            val b = backendOrNull ?: error("the endpoint root was never created")
            val outcome: Throwable? =
                lock.withLock {
                    controlPreludeLocked(b, token, model, track, mutating = true)?.let { return@withLock it }
                    if (!model.isDesired(track.token)) return@withLock null
                    when (val r = b.unsubscribeTrack(token, track.token)) {
                        is BackendCommandResult.Ok -> {
                            model.setDesired(track.token, false)
                            null
                        }
                        is BackendCommandResult.Refused -> explainRefusalLocked(b, "track-unsubscribe", r.code.value, model, track)
                        is BackendCommandResult.Bridge -> quarantineLocked(r.failure)
                    }
                }
            outcome?.let { throw settleQuarantine(it) }
        }
    }

    suspend fun trackState(token: Long, model: ReceiverModel, track: MediaTrack): TrackState =
        perform {
            val b = backendOrNull ?: error("the endpoint root was never created")
            var failure: Throwable? = null
            val state: TrackState? =
                lock.withLock {
                    controlPreludeLocked(b, token, model, track, mutating = false)?.let { failure = it; return@withLock null }
                    // The C's rule for a terminal receiver, answered locally: every track reads ENDED.
                    if (model.recordedTerminal != null) return@withLock TrackState.Ended
                    when (val r = b.trackState(token, track.token)) {
                        is BackendTrackStateResult.State -> TrackState.ofNative(r.raw)
                        is BackendTrackStateResult.Refused -> {
                            failure = explainRefusalLocked(b, "track-state", r.code.value, model, track)
                            null
                        }
                        is BackendTrackStateResult.Bridge -> {
                            failure = quarantineLocked(r.failure)
                            null
                        }
                    }
                }
            state ?: throw settleQuarantine(failure!!)
        }

    /** A quarantine latched under the lock still owes its establishment waiters their answer; other failures pass through. */
    private fun settleQuarantine(t: Throwable): Throwable {
        if (t is BridgeContractException) {
            val resumptions = lock.withLock { judgeEstablishLocked(null, t) + failRetainedLocked(t) }
            resumptions.forEach { it() }
        }
        return t
    }

    /** The record twin of [attemptObject]: the family's poll, its deferral witness, and ITS queue's exhaustion. Null when empty. */
    private fun <R : Any, T : Any> attemptRecord(b: EndpointBackend, token: Long, model: ReceiverModel, stream: RecordStream<R, T>, family: RecordStream.Family<R, T>): ObjectDemand? {
        var failure: BridgeFailure? = null
        val before: ObjectDemand? =
            lock.withLock {
                when {
                    receiverToken != token || model.isRetired -> ObjectDemand.Closed
                    stream.hasHandoff() -> ObjectDemand.Delivered
                    drainReceiverLocked(b) -> {
                        failure = (bridgeFailure as? BridgeContractException)?.failure
                        null
                    }
                    model.isExhausted(family.queue) -> ObjectDemand.Closed
                    else -> ObjectDemand.Poll
                }
            }
        if (before == null) throw quarantine(failure ?: BridgeFailure(family.trackOp, 0))
        if (before !is ObjectDemand.Poll) return before
        val polled = family.poll(b, token)
        var progress = false
        val outcome: ObjectDemand? =
            lock.withLock {
                when (polled) {
                    is BackendRecordResult.Record -> {
                        progress = true
                        val handle = family.trackOf(polled.value)
                        var track = model.resolve(handle)
                        if (track == null && !drainReceiverLocked(b)) track = model.resolve(handle)
                        if (track == null) {
                            failure = (bridgeFailure as? BridgeContractException)?.failure ?: BridgeFailure(family.trackOp, 0)
                            backendUnusable = true
                            null
                        } else {
                            stream.stage(ReceiverRecordEvent.Record(family.toPublic(polled.value, track)))
                            ObjectDemand.Delivered
                        }
                    }
                    is BackendRecordResult.Empty -> {
                        val witness = stream.witnessIfAdvanced(polled.deferrals)
                        if (witness != null) {
                            progress = true
                            stream.stage(witness)
                            ObjectDemand.Delivered
                        } else {
                            null
                        }
                    }
                    is BackendRecordResult.Closed -> {
                        progress = true
                        model.terminal(if (polled.fatal) ReceiverTerminal.Fatal(ReceiverFatalCode(polled.code)) else ReceiverTerminal.Closed)
                        if (drainReceiverLocked(b)) {
                            failure = (bridgeFailure as? BridgeContractException)?.failure
                            null
                        } else {
                            model.markExhausted(family.queue)
                            ObjectDemand.Closed
                        }
                    }
                    is BackendRecordResult.Bridge -> {
                        progress = true
                        failure = polled.failure
                        backendUnusable = true
                        null
                    }
                }
            }
        if (progress && outcome == null) throw quarantine(failure ?: BridgeFailure(family.trackOp, 0))
        return outcome
    }

    /** One record demand, retained on the worker while its queue is empty, exactly like [demandObject]. */
    suspend fun <R : Any, T : Any> demandRecord(token: Long, model: ReceiverModel, stream: RecordStream<R, T>, family: RecordStream.Family<R, T>): ObjectDemand {
        val entry =
            Retained<ObjectDemand>(
                owner = model,
                attempt = { b, _ -> attemptRecord(b, token, model, stream, family) },
                onOwnerRetired = { Result.success(ObjectDemand.Closed) },
            )
        val demanded: Demanded<ObjectDemand> =
            try {
                perform {
                    val b = backendOrNull ?: error("the endpoint root was never created")
                    attemptRecord(b, token, model, stream, family)?.let { Demanded.Resolved(it) } ?: lock.withLock { retainLocked(entry) }
                }
            } catch (e: CancellationException) {
                retireRetainedNow(entry)
                throw e
            }
        return when (demanded) {
            is Demanded.Resolved -> demanded.value
            is Demanded.Pending -> awaitRetained(demanded.retained)
        }
    }

    /** One stats snapshot on the service worker: the backend read, a bridge refusal quarantining. */
    suspend fun receiverStats(token: Long): ReceiverStats =
        perform {
            val b = backendOrNull ?: error("the endpoint root was never created")
            if (lock.withLock { receiverToken != token }) throw ReceiverClosedException()
            when (val r = b.receiverStats(token)) {
                is BackendStatsResult.Stats -> r.value
                is BackendStatsResult.Bridge -> throw quarantine(r.failure)
            }
        }

    // -- the sender ------------------------------------------------------------

    /** The sender attach, commit-grade for the same ownership reason as [attachReceiver]. */
    suspend fun attachSender(config: SenderConfig): AttachedSender =
        performCommitted {
            val b = backendOrNull ?: error("the endpoint root was never created")
            when (val r = b.attachSender(config)) {
                is BackendAttachResult.Attached -> {
                    val model = SenderModel()
                    lock.withLock {
                        // The C tier admits one sender per endpoint; a second attach is its WRONG_STATE, never a second token here.
                        check(senderToken == null) { "the bridge attached a second sender under one root" }
                        senderToken = r.token
                        senderModel = model
                    }
                    AttachedSender(r.token, model)
                }
                is BackendAttachResult.Refused -> throw SenderRefusedException(r.code)
                is BackendAttachResult.Bridge -> throw quarantine(r.failure)
            }
        }

    /** Whether [token] is the sender currently attached under this root. */
    fun isSenderAttached(token: Long): Boolean = lock.withLock { senderToken == token }

    /** Close the sender [token] on the service worker: the child is retired, the root stays live and its sender slot is free. Idempotent. */
    suspend fun closeSender(token: Long) {
        perform {
            val mine = lock.withLock { senderToken == token }
            if (!mine) return@perform
            val b = backendOrNull ?: error("the endpoint root was never created")
            when (val r = b.closeSender(token)) {
                is BackendOpResult.Ok -> {
                    val model = lock.withLock { senderModel.also { senderToken = null; senderModel = null } }
                    model?.retire()
                    if (model != null) lock.withLock { retireOwnerLocked(model) }
                }
                is BackendOpResult.Bridge -> throw quarantine(r.failure)
            }
        }
    }

    /**
     * Caller holds the lock. ONE probe of the attached sender on this worker
     * -- readiness, terminal, media demand -- published to its status flow,
     * on every service pass. Returns true when the bridge refused: the token
     * is quarantined exactly as a refused snapshot quarantines it.
     */
    private fun probeSenderLocked(b: EndpointBackend): Boolean {
        val token = senderToken ?: return false
        val model = senderModel ?: return false
        return when (val r = b.senderProbe(token)) {
            is BackendSenderProbeResult.Status -> {
                model.publish(r.value)
                false
            }
            is BackendSenderProbeResult.Bridge -> {
                bridgeFailure = bridgeFailure ?: BridgeContractException(r.failure)
                backendUnusable = true
                true
            }
        }
    }

    /** Caller holds the lock. The sender's CLOSED refusal explained by a fresh probe: the terminal it reports, or the quarantine. */
    private fun senderClosedLocked(b: EndpointBackend, model: SenderModel): Throwable {
        if (probeSenderLocked(b)) return bridgeFailure!!
        return SenderClosedException(model.status.value.terminal)
    }

    /** Caller holds the lock. The refusals every sender mutation shares; null when [code] is the operation's own to explain. */
    private fun senderCommonRefusalLocked(b: EndpointBackend, model: SenderModel, code: Int): Throwable? =
        when (code) {
            MOQ_ERR_CLOSED -> senderClosedLocked(b, model)
            MOQ_ERR_INTERRUPTED -> SenderInterruptedException()
            else -> null
        }

    private inline fun <T> senderOp(token: Long, model: SenderModel, crossinline body: (EndpointBackend) -> T): T {
        val b = backendOrNull ?: error("the endpoint root was never created")
        var failure: Throwable? = null
        val value: T? =
            lock.withLock {
                if (senderToken != token) {
                    failure = SenderClosedException(model.status.value.terminal)
                    null
                } else {
                    try {
                        body(b)
                    } catch (t: Throwable) {
                        failure = t
                        null
                    }
                }
            }
        failure?.let { throw settleQuarantine(it) }
        @Suppress("UNCHECKED_CAST")
        return value as T
    }

    /** The catalog add, commit-grade: a track the C registered is never left without the handle that removes it. */
    suspend fun senderAddTrack(token: Long, model: SenderModel, config: SendTrackConfig): SendTrack =
        performCommitted {
            senderOp(token, model) { b ->
                when (val r = b.senderAddTrack(token, config)) {
                    is BackendSenderTrackResult.Added -> SendTrack(r.track, config).also { model.add(it) }
                    is BackendSenderTrackResult.WouldBlock -> throw TrackNameBusyException(config.name)
                    is BackendSenderTrackResult.Refused ->
                        throw senderCommonRefusalLocked(b, model, r.code.value)
                            ?: if (r.code.value == MOQ_ERR_WRONG_STATE) SenderCompletedException() else TrackConfigRejectedException(r.code)
                    is BackendSenderTrackResult.Bridge -> throw quarantineLocked(r.failure)
                }
            }
        }

    suspend fun senderRemoveTrack(token: Long, model: SenderModel, track: SendTrack) {
        perform {
            senderOp(token, model) { b ->
                when (val r = b.senderRemoveTrack(token, track.token)) {
                    is BackendCommandResult.Ok -> track.isRemoved = true
                    is BackendCommandResult.Refused ->
                        throw senderCommonRefusalLocked(b, model, r.code.value)
                            ?: if (r.code.value == MOQ_ERR_WRONG_STATE) SendTrackUnavailableException() else quarantineLocked(BridgeFailure("sender-remove-track", r.code.value))
                    is BackendCommandResult.Bridge -> throw quarantineLocked(r.failure)
                }
            }
        }
    }

    /** ONE write attempt under the engine lock; null on WOULD_BLOCK. A retained [entry] that was retired meanwhile makes no native call. */
    private fun attemptWrite(token: Long, model: SenderModel, track: SendTrack, obj: SendObject, entry: Retained<WriteAttempt>?): WriteAttempt? =
        senderOp(token, model) { b ->
            if (entry != null && entry.retired) return@senderOp null
            when (val r = b.senderWrite(token, track.token, obj)) {
                is BackendWriteResult.Accepted -> WriteAttempt(WriteOutcome.Accepted).also { entry?.commit(it) }
                is BackendWriteResult.WouldBlock -> null
                is BackendWriteResult.Refused ->
                    throw senderCommonRefusalLocked(b, model, r.code.value)
                        ?: when (r.code.value) {
                            MOQ_ERR_WRONG_STATE -> SendTrackUnavailableException()
                            MOQ_ERR_INVAL, MOQ_ERR_PROTO -> ObjectRejectedException(r.code)
                            else -> quarantineLocked(BridgeFailure("sender-write", r.code.value))
                        }
                is BackendWriteResult.Bridge -> throw quarantineLocked(r.failure)
            }
        }

    /** ONE end-track attempt under the engine lock; null on WOULD_BLOCK; an accepted marker is remembered on the handle. */
    private fun attemptEndTrack(token: Long, model: SenderModel, track: SendTrack, entry: Retained<WriteAttempt>?): WriteAttempt? =
        senderOp(token, model) { b ->
            if (entry != null && entry.retired) return@senderOp null
            when (val r = b.senderEndTrack(token, track.token)) {
                is BackendWriteResult.Accepted -> {
                    track.isEnded = true
                    WriteAttempt(WriteOutcome.Accepted).also { entry?.commit(it) }
                }
                is BackendWriteResult.WouldBlock -> null
                is BackendWriteResult.Refused ->
                    throw senderCommonRefusalLocked(b, model, r.code.value)
                        ?: if (r.code.value == MOQ_ERR_WRONG_STATE) SendTrackUnavailableException() else quarantineLocked(BridgeFailure("sender-end-track", r.code.value))
                is BackendWriteResult.Bridge -> throw quarantineLocked(r.failure)
            }
        }

    /**
     * ONE write attempt, commit-grade: the outcome is delivered to the caller
     * whatever its cancellation state. With [retain], a WOULD_BLOCK answer is
     * not returned: the attempt is RETAINED on the worker, re-attempted on
     * each later pass, and the returned [Demanded.Pending] resolves with the
     * accepted attempt or the typed refusal. A retained attempt that was
     * retired (timeout, cancellation, the sender's close) makes no further
     * native call, so a transfer never happens after the caller gave up.
     */
    suspend fun senderWrite(token: Long, model: SenderModel, track: SendTrack, obj: SendObject, retain: Boolean): Demanded<WriteAttempt> {
        val entry =
            if (!retain) {
                null
            } else {
                Retained<WriteAttempt>(
                    owner = model,
                    attempt = { _, self -> attemptWrite(token, model, track, obj, self) },
                    onOwnerRetired = { Result.failure(SenderClosedException(model.status.value.terminal)) },
                )
            }
        return performCommitted {
            attemptWrite(token, model, track, obj, null)?.let { Demanded.Resolved(it) }
                ?: if (entry == null) Demanded.Resolved(WriteAttempt(WriteOutcome.WouldBlock)) else lock.withLock { retainLocked(entry) }
        }
    }

    /** ONE end-track attempt, commit-grade and retained like [senderWrite]. */
    suspend fun senderEndTrack(token: Long, model: SenderModel, track: SendTrack, retain: Boolean): Demanded<WriteAttempt> {
        val entry =
            if (!retain) {
                null
            } else {
                Retained<WriteAttempt>(
                    owner = model,
                    attempt = { _, self -> attemptEndTrack(token, model, track, self) },
                    onOwnerRetired = { Result.failure(SenderClosedException(model.status.value.terminal)) },
                )
            }
        return performCommitted {
            attemptEndTrack(token, model, track, null)?.let { Demanded.Resolved(it) }
                ?: if (entry == null) Demanded.Resolved(WriteAttempt(WriteOutcome.WouldBlock)) else lock.withLock { retainLocked(entry) }
        }
    }

    suspend fun senderConvertToVod(token: Long, model: SenderModel, items: List<VodConversion>) {
        perform {
            senderOp(token, model) { b ->
                when (val r = b.senderConvertToVod(token, items.map { it.track.token to it.duration.value })) {
                    is BackendCommandResult.Ok -> for (item in items) item.track.isVod = true
                    is BackendCommandResult.Refused ->
                        throw senderCommonRefusalLocked(b, model, r.code.value)
                            ?: when (r.code.value) {
                                MOQ_ERR_WRONG_STATE -> if (model.isCompleted) SenderCompletedException() else SenderNotReadyException()
                                MOQ_ERR_INVAL -> VodConversionRejectedException(r.code)
                                else -> quarantineLocked(BridgeFailure("sender-convert-to-vod", r.code.value))
                            }
                    is BackendCommandResult.Bridge -> throw quarantineLocked(r.failure)
                }
            }
        }
    }

    suspend fun senderComplete(token: Long, model: SenderModel) {
        perform {
            senderOp(token, model) { b ->
                when (val r = b.senderComplete(token)) {
                    is BackendCommandResult.Ok -> model.markCompleted()
                    is BackendCommandResult.Refused ->
                        throw senderCommonRefusalLocked(b, model, r.code.value)
                            ?: if (r.code.value == MOQ_ERR_WRONG_STATE) SenderNotReadyException() else quarantineLocked(BridgeFailure("sender-complete", r.code.value))
                    is BackendCommandResult.Bridge -> throw quarantineLocked(r.failure)
                }
            }
        }
    }

    suspend fun senderTrackDemand(token: Long, track: SendTrack): UInt =
        perform {
            val b = backendOrNull ?: error("the endpoint root was never created")
            if (lock.withLock { senderToken != token }) throw SenderClosedException(senderModel?.status?.value?.terminal)
            when (val r = b.senderTrackDemand(token, track.token)) {
                is BackendDemandResult.Count -> r.value
                is BackendDemandResult.Bridge -> throw quarantine(r.failure)
            }
        }

    /** One stats snapshot on the service worker: the backend read, a bridge refusal quarantining. */
    suspend fun senderStats(token: Long): SenderStats =
        perform {
            val b = backendOrNull ?: error("the endpoint root was never created")
            if (lock.withLock { senderToken != token }) throw SenderClosedException(senderModel?.status?.value?.terminal)
            when (val r = b.senderStats(token)) {
                is BackendSenderStatsResult.Stats -> r.value
                is BackendSenderStatsResult.Bridge -> throw quarantine(r.failure)
            }
        }

    // -- retained work --------------------------------------------------------

    /** Caller holds the lock. Register [entry] unless it was retired before the body reached this point (its caller is gone). */
    private fun <T : Any> retainLocked(entry: Retained<T>): Demanded<T> {
        if (!entry.retired) retained += entry
        return Demanded.Pending(entry)
    }

    /**
     * Retire [entry]: it is never attempted again. Returns the value its
     * attempt committed, if any -- a transfer that happened is reported,
     * never dropped -- null when nothing happened natively, and THROWS the
     * attempt's typed failure when it was refused. When an attempt is in
     * flight the answer is deferred until that attempt settles (its native
     * call is bounded), immune to the caller's own cancellation.
     */
    internal suspend fun <T : Any> retireRetained(entry: Retained<T>): T? {
        // Registered under the entry's OWN lock first: the worker may hold the
        // engine lock around this very entry's native call, and a caller must
        // be able to record its retirement -- and suspend until the attempt
        // settles -- without waiting for that lock.
        val settled =
            when (val r = entry.retire()) {
                is Retained.Retirement.Now -> r.settled
                is Retained.Retirement.AfterSettle -> withContext(NonCancellable) { r.settled.await() }
            }
        lock.withLock { retained.remove(entry) }
        return when (settled) {
            is Retained.Settled.Committed -> settled.value
            // The attempt happened and was refused: its typed outcome is the
            // caller's answer, not the cancellation that contested it.
            is Retained.Settled.Failed -> throw settled.failure
            is Retained.Settled.NoProgress -> null
        }
    }

    /** Retire a DEMAND entry without waiting: an object a racing poll still dequeues stays in the handoff slot for the next demand. */
    internal fun <T : Any> retireRetainedNow(entry: Retained<T>) {
        entry.retire()
        lock.withLock { retained.remove(entry) }
    }

    /** Await a retained demand; a cancelled caller retires it (the demand's value, if any, stays staged) and resolves as cancellation. */
    internal suspend fun <T : Any> awaitRetained(entry: Retained<T>): T =
        try {
            entry.result.await()
        } catch (e: CancellationException) {
            retireRetainedNow(entry)
            throw e
        }

    /** Caller holds the lock. Every retained entry of [owner] is resolved with its owner-retired answer, on the worker outside any body. */
    private fun retireOwnerLocked(owner: Any) {
        val mine = retained.filter { it.owner === owner }
        retained.removeAll(mine)
        for (e in mine) pendingResolutions += { e.resolveOwnerRetired() }
    }

    /**
     * Caller holds the lock. Shutdown: every retained entry is resolved with
     * its owner-retired answer -- the sender's close outcome, the stream's
     * end -- on the worker outside any body. Never the engine's own closed
     * exception, which is not a public outcome.
     */
    private fun retireAllForShutdownLocked() {
        val taken = retained.toList()
        retained.clear()
        for (e in taken) pendingResolutions += { e.resolveOwnerRetired() }
    }

    /**
     * Caller holds the lock. Quarantine: every retained entry learns the
     * stable bridge [failure] -- unless its own attempt already settled with
     * a transfer or a typed refusal, which stands. Returns the resumptions
     * to run outside the lock.
     */
    private fun failRetainedLocked(failure: Throwable): List<() -> Unit> {
        val taken = retained.toList()
        retained.clear()
        for (e in taken) e.retire()
        return taken.map { e -> { e.resolveQuarantined(failure) } }
    }

    /** Caller holds the lock. Hand back the body-owed resolutions to run outside the lock, outside any body. */
    private fun takePendingResolutionsLocked(): List<() -> Unit> {
        val taken = pendingResolutions.toList()
        pendingResolutions.clear()
        return taken
    }

    /**
     * Caller holds the engine lock. The ONE claim decision for a retained
     * attempt: shutdown and quarantine are read under the lock they are
     * written under, and the entry is claimed in that same step -- so a close
     * that returns before the claim leaves the entry unclaimed (it gets its
     * owner-retired answer), a claim that precedes close settles under the
     * in-flight rule, and a latched bridge failure -- whoever latched it, a
     * drain, a probe, a record, or a retry -- permits no further native
     * call. Nothing native is called here.
     */
    private fun <T : Any> claimForServiceLocked(e: Retained<T>): Boolean =
        phase == Phase.RUNNING && bridgeFailure == null && e.beginAttempt()

    /**
     * On the worker, lock NOT held for the attempt itself: claim each
     * retained entry through [claimForServiceLocked] and re-attempt it once
     * -- one pass, one attempt each -- resolving those that settled. An
     * entry retired since the snapshot is dropped; shutdown or quarantine
     * ends the pass.
     */
    private fun serviceRetained(b: EndpointBackend) {
        val entries = lock.withLock { retained.toList() }
        for (e in entries) {
            val claimed = lock.withLock { claimForServiceLocked(e) }
            if (!claimed) {
                // Shutdown or quarantine ends the pass: the remaining entries
                // get their owner-retired answers, or the stable failure, at
                // the top of the worker loop. An entry retired since the
                // snapshot is simply dropped.
                if (lock.withLock { phase != Phase.RUNNING || bridgeFailure != null }) return
                lock.withLock { retained.remove(e) }
                continue
            }
            serviceOne(e, b)
        }
    }

    /** The claimed attempt: run it, settle it with its complete outcome, deliver progress or failure. */
    private fun <T : Any> serviceOne(e: Retained<T>, b: EndpointBackend) {
        val outcome =
            try {
                e.attempt(b, e)
            } catch (t: Throwable) {
                lock.withLock { retained.remove(e) }
                e.retire()
                e.endAttempt(t)
                e.result.completeExceptionally(t)
                return
            }
        // A sender attempt committed under the engine lock, inside its native
        // call; a demand commits here, before the attempt is marked ended.
        if (outcome != null) e.commit(outcome)
        val settled = e.endAttempt(null)
        if (settled != null) {
            lock.withLock { retained.remove(e) }
            e.result.complete(settled)
        }
    }

    /**
     * Caller holds the lock. Drains the attached receiver's native track-event
     * queue TO EMPTY on this worker and folds every event into its model --
     * on every service pass, before the endpoint's record is taken, so the
     * inventory a collector sees at a record is already integrated, and so
     * nothing the application does can let the native queue overflow. The
     * ring's own CLOSED records the receiver's terminal, marks the ring
     * exhausted and closes the history; a terminal another queue reported
     * first leaves this ring's tail to be drained. Returns true when the
     * bridge refused: the token is quarantined exactly as a refused snapshot
     * quarantines it.
     */
    private fun drainReceiverLocked(b: EndpointBackend): Boolean {
        val token = receiverToken ?: return false
        val model = receiverModel ?: return false
        // A recorded terminal is not this ring's exhaustion: the tail is
        // still drained until the ring itself answers CLOSED.
        if (model.isRetired || model.isTrackExhausted) return false
        while (true) {
            when (val r = b.pollTrack(token)) {
                is BackendTrackEventResult.Event -> model.integrate(r.event)
                is BackendTrackEventResult.Empty -> return false
                is BackendTrackEventResult.Closed -> {
                    model.terminal(
                        if (r.fatal) ReceiverTerminal.Fatal(ReceiverFatalCode(r.code)) else ReceiverTerminal.Closed,
                    )
                    model.trackRingExhausted()
                    return false
                }
                is BackendTrackEventResult.Bridge -> {
                    bridgeFailure = bridgeFailure ?: BridgeContractException(r.failure)
                    backendUnusable = true
                    return true
                }
            }
        }
    }

    /**
     * The sticky interrupt latch, set or cleared ON THE CALLING THREAD, at
     * once. Never marshalled to the worker: the worker is the thread the latch
     * exists to unblock, and the C call wakes the facade itself, so no engine
     * wake is sent for it. Not a state change, so no record is published.
     *
     * Clearing also raises the engine level and signals the worker's own
     * condition: after an INTERRUPTED wait the worker parks there rather than
     * re-entering a wait that would answer immediately, and it must retry the
     * backend wait now, not when unrelated work happens to arrive.
     *
     * After a logical close the latch is refused with [EngineClosedException]
     * and no backend call is made; after a bridge quarantine the stable
     * failure is thrown and no backend call is made. A bridge refusal of the
     * latch itself quarantines exactly as a refused wake does.
     */
    fun setInterrupted(interrupted: Boolean) {
        var resumptions: List<() -> Unit> = emptyList()
        val failure: Throwable? =
            lock.withLock {
                bridgeFailure?.let { return@withLock it }
                if (phase != Phase.RUNNING) return@withLock EngineClosedException()
                val b = backendOrNull ?: error("the endpoint root was never created")
                when (val r = b.setInterrupted(interrupted)) {
                    is BackendOpResult.Ok -> {
                        if (!interrupted) {
                            level = true
                            idle.signalAll()
                        }
                        null
                    }
                    is BackendOpResult.Bridge -> {
                        val latched = BridgeContractException(r.failure)
                        bridgeFailure = latched
                        idle.signalAll() // the worker must leave any backend park
                        resumptions = judgeEstablishLocked(null, latched)
                        latched
                    }
                }
            }
        resumptions.forEach { it() }
        if (failure != null) throw failure
    }

    /**
     * The graceful flush, [timeoutMicros] already validated non-negative.
     *
     * Refused on the caller's thread, before anything else, with the public
     * discipline: the stable quarantine failure, or [EngineClosedException]
     * after a logical close. Then, on a blocking thread: the quiesce is armed
     * (the worker leaves its current backend wait and parks on the engine's
     * condition), this caller waits for its turn (one native drain at a time)
     * and for the worker to be off the wait, and only then leases the row. A
     * caller cancelled while queued makes no native call. The worker keeps
     * servicing jobs and close throughout, takes one observation when the
     * last drain has released, and only then re-enters the backend wait.
     *
     * A bridge failure from the row quarantines exactly as a refused wake does.
     */
    suspend fun drain(timeoutMicros: Long): BackendDrainResult {
        rejectBodyReentry("drain()")
        require(timeoutMicros >= 0) { "drain timeout must be non-negative" }
        lock.withLock {
            bridgeFailure?.let { throw it }
            if (phase != Phase.RUNNING) throw EngineClosedException()
        }
        val ticket = DrainTicket()
        val native = drainScope.async { runDrain(ticket, timeoutMicros) }
        try {
            return native.await()
        } catch (e: CancellationException) {
            // Detach: the caller stops waiting. A queued drain never starts;
            // an active one completes under the engine's accounting.
            lock.withLock {
                ticket.cancelled = true
                idle.signalAll()
            }
            throw e
        }
    }

    private fun runDrain(ticket: DrainTicket, timeoutMicros: Long): BackendDrainResult {
        val b: EndpointBackend =
            lock.withLock {
                drainDemand++
                // Leave the current backend wait: the ordinary engine wake. The
                // worker then parks on the engine's condition instead of
                // re-entering the wait while any drain is in flight.
                wakeLocked()
                try {
                    while (true) {
                        if (ticket.cancelled) throw CancellationException("the drain caller detached before the native drain began")
                        bridgeFailure?.let { throw it }
                        if (phase != Phase.RUNNING) throw EngineClosedException()
                        if (!nativeDrainActive && workerOffWait) break
                        idle.await()
                    }
                    nativeDrainActive = true
                    backendOrNull ?: error("the endpoint root was never created")
                } catch (t: Throwable) {
                    releaseDrainDemandLocked() // no native call was made
                    throw t
                }
            }
        val result = b.drain(timeoutMicros)
        var resumptions: List<() -> Unit> = emptyList()
        val failure =
            lock.withLock {
                nativeDrainActive = false
                nativeDrainConsumedWait = true
                releaseDrainDemandLocked()
                if (result is BackendDrainResult.Bridge) {
                    val latched = bridgeFailure ?: BridgeContractException(result.failure).also { bridgeFailure = it }
                    resumptions = judgeEstablishLocked(null, latched)
                    latched
                } else {
                    null
                }
            }
        resumptions.forEach { it() }
        if (failure != null) throw failure
        return result
    }

    /**
     * Caller holds the lock. One fewer drain caller; the last one releases the
     * worker. The post-drain observation is owed only when a native drain
     * actually ran during this quiesce -- a caller cancelled or refused before
     * its native call consumed nothing, so the worker simply resumes.
     */
    private fun releaseDrainDemandLocked() {
        drainDemand--
        if (drainDemand == 0) {
            if (nativeDrainConsumedWait) {
                postDrainWorkDue = true
                level = true
            }
            nativeDrainConsumedWait = false
        }
        idle.signalAll() // the worker, and any queued drain, re-evaluate
    }

    /**
     * The non-suspending logical close: exactly [close]'s pre-suspension part.
     * Future submissions refuse, the worker is woken to begin the existing
     * teardown, and pending establishment waiters learn the engine closed. No
     * record is published: a local close is not a terminal the endpoint
     * reported. Idempotent.
     */
    fun requestClose() {
        val resumptions =
            lock.withLock {
                if (phase == Phase.RUNNING) {
                    phase = Phase.STOPPING
                    wakeLocked()
                } else {
                    idle.signalAll()
                }
                judgeEstablishLocked(null, EngineClosedException())
            }
        resumptions.forEach { it() }
    }

    /**
     * Caller holds the lock. Latches the construction decision and returns the
     * waiters to resume OUTSIDE the lock; on a failed or cancelled
     * construction the queued jobs are returned too, each to be resolved with
     * [outcome], because the worker will retire without claiming them.
     */
    private fun decideConstructionLocked(outcome: Throwable?): List<(Throwable?) -> Unit> {
        constructDecided = true
        constructOutcome = outcome
        val resumers = constructWaiters.toMutableList()
        constructWaiters.clear()
        if (outcome != null) {
            val stranded = jobs.toList()
            jobs.clear()
            for (job in stranded) {
                job.claimed = true
                val done = job.onDone
                job.onDone = null
                if (done != null) resumers += { t -> done(Result.failure(t ?: outcome)) }
            }
        }
        return resumers
    }

    /**
     * Drain jobs accepted before shutdown in FIFO order, then retire the
     * backend. Idempotent, and teardown-grade like [postAndWait].
     */
    suspend fun close() {
        rejectBodyReentry("close()")
        suspendCoroutine { cont: Continuation<Unit> ->
            var alreadyRetired = false
            var lateFailure: Exception? = null
            lock.withLock {
                val first = phase == Phase.RUNNING
                if (first) phase = Phase.STOPPING
                if (retired && retirementSettled) {
                    // Teardown SETTLED: the stable result, at once, with no
                    // backend call.
                    alreadyRetired = true
                    lateFailure = stopRefusal
                } else if (retired) {
                    // Teardown IN PROGRESS (destroy is waiting for a drain turn
                    // granted before retirement): join the close-waiter set,
                    // with no backend call, and learn the same outcome.
                    closeWaiters += { failure ->
                        if (failure != null) cont.resumeWithException(failure)
                        else cont.resume(Unit)
                    }
                    idle.signalAll()
                } else {
                    closeWaiters += { failure ->
                        if (failure != null) cont.resumeWithException(failure)
                        else cont.resume(Unit)
                    }
                    if (first) {
                        // Marked STOPPING, waiter registered and the wake
                        // published, all under the lock, before this call's
                        // first suspension.
                        wakeLocked()
                    } else {
                        idle.signalAll() // a concurrent close adds no backend call
                    }
                }
            }
            // A close after quarantine resolves with the SAME stable failure and
            // makes no backend call.
            if (alreadyRetired) {
                val f = lateFailure
                if (f != null) cont.resumeWithException(f) else cont.resume(Unit)
            }
        }
    }

    /**
     * Caller holds the lock. Raises the engine level and the backend's.
     *
     * Runs on the SUBMITTING thread by design: the backend wake is the C tier's
     * any-thread, non-allocating nudge, not confined work.
     */
    private fun wakeLocked() {
        level = true
        idle.signalAll()
        if (bridgeFailure != null) return // quarantined: no backend call, ever
        // Null before the worker has constructed the root: the engine level was
        // already raised, so the worker sees the work without a native nudge.
        val b = backendOrNull
        if (b == null) {
            // The worker has not published the root yet. The wake is RETAINED,
            // not dropped: construction happens before any endpoint operation,
            // so delivering it once the root exists preserves the C tier's
            // coalesced level exactly -- a dropped wake would be a lost nudge.
            wakePending = true
            return
        }
        when (val r = b.wake()) {
            is BackendOpResult.Ok -> Unit
            is BackendOpResult.Bridge -> {
                // A refused wake means the bridge no longer honours this token.
                // Quarantine here, under the lock the caller already holds, so
                // the worker cannot race ahead into another backend call. The
                // engine level was raised above, so the worker still runs every
                // accepted command and close() without the backend's help.
                bridgeFailure = bridgeFailure ?: BridgeContractException(r.failure)
            }
        }
    }

    // -- the service thread ---------------------------------------------------

    private fun run() {
        if (!construct()) {
            retire()
            return
        }
        while (true) {
            val claimed = claimNext() ?: break
            // Bodies run OUTSIDE the lock; a throwing body must not kill the
            // worker, so its outcome is captured and delivered to its owner.
            //
            // The reentry bracket covers the body ONLY. Result delivery below
            // may resume a continuation inline on this thread, and that
            // continuation is an ordinary caller.
            executingBody = true
            val outcome =
                try {
                    runCatching { claimed.body() }
                } finally {
                    executingBody = false
                }
            (lock.withLock { claimed.onDone })?.invoke(outcome)
        }
        retire()
    }

    /**
     * Creates the root ON THIS WORKER, before any endpoint operation runs.
     *
     * Returns false when there is no root to serve -- the factory failed, or
     * the engine was already closed. Both cases leave nothing to tear down,
     * which is what makes a cancellation before native creation leak-free.
     */
    private fun construct(): Boolean {
        // Closed before creation: the ONE recorded outcome is what every
        // pre-registered waiter and queued job is resumed with.
        var closedBefore: EngineClosedException? = null
        val cancelledBefore =
            lock.withLock {
                if (phase != Phase.RUNNING) {
                    closedBefore = EngineClosedException()
                    decideConstructionLocked(closedBefore)
                } else {
                    null
                }
            }
        if (cancelledBefore != null) {
            cancelledBefore.forEach { it(closedBefore) }
            return false
        }
        val created =
            try {
                factory()
            } catch (t: Throwable) {
                val cause = t as? Exception ?: IllegalStateException(t)
                val waiters =
                    lock.withLock {
                        constructionFailure = cause
                        decideConstructionLocked(cause)
                    }
                waiters.forEach { it(cause) }
                return false
            }
        // The SEED record, taken on this worker before the root is published
        // and before construction is decided: the stream's first value is a
        // native record, and open() returns only after it exists. A bridge
        // refusal here quarantines exactly as an explicit snapshot would.
        var seed: EndpointSnapshot? = null
        var seedFailure: BridgeFailure? = null
        if (publishSnapshots) {
            when (val r = created.snapshot()) {
                is BackendSnapshotResult.Ok -> seed = r.snapshot
                is BackendSnapshotResult.Bridge -> seedFailure = r.failure
            }
        }
        // Publication window: the root exists but nothing owns it yet. Closing
        // in this window must still destroy it, so it is published first and
        // the phase re-checked afterwards -- never the reverse.
        lock.withLock {
            backendOrNull = created
            if (seed != null) snapshotFlow = MutableStateFlow(seed)
            if (seedFailure != null) {
                // The bridge refused the token on the very first read: the root
                // is registry-owned and QUARANTINED -- no stop, no destroy --
                // and construction is decided with that exact failure, so no
                // caller ever receives an endpoint without its stream. The
                // worker retires through the ordinary path below.
                val quarantined = BridgeContractException(seedFailure)
                bridgeFailure = bridgeFailure ?: quarantined
                val waiters = decideConstructionLocked(quarantined)
                lock.unlock()
                try {
                    waiters.forEach { it(quarantined) }
                } finally {
                    lock.lock()
                }
                return false
            }
            if (wakePending) {
                // Deliver the retained wake exactly once, before the worker
                // performs any other endpoint operation.
                wakePending = false
                // The retained wake carries the same contract as any other:
                // a refusal here quarantines rather than being discarded.
                when (val r = created.wake()) {
                    is BackendOpResult.Ok -> Unit
                    is BackendOpResult.Bridge ->
                        bridgeFailure = bridgeFailure ?: BridgeContractException(r.failure)
                }
            }
            val waiters = decideConstructionLocked(null)
            val serving = phase == Phase.RUNNING
            // Resumed outside the lock: a waiter may call straight back in.
            lock.unlock()
            try {
                waiters.forEach { it(null) }
            } finally {
                lock.lock()
            }
            return serving
        }
    }

    /**
     * Caller holds the lock. One coherent record on this worker, published to
     * the stream and judged; a bridge refusal is the same quarantine an
     * explicit snapshot refusal is. Returns the resumptions to run outside
     * the lock.
     */
    private fun takeRecordLocked(b: EndpointBackend): List<() -> Unit> =
        when (val r = b.snapshot()) {
            is BackendSnapshotResult.Ok -> {
                snapshotFlow?.value = r.snapshot
                judgeEstablishLocked(r.snapshot, null)
            }
            is BackendSnapshotResult.Bridge -> {
                bridgeFailure = bridgeFailure ?: BridgeContractException(r.failure)
                backendUnusable = true
                judgeEstablishLocked(null, bridgeFailure)
            }
        }

    /** Returns the next job, or null when the engine must retire. */
    private fun claimNext(): Job? {
        lock.withLock {
            while (true) {
                // Shutdown retires every retained entry with its owner-retired
                // answer: close's own wake is never another native attempt.
                // A pre-existing bridge failure outranks close: those entries
                // learn the stable failure in the quarantined branch below.
                if (phase != Phase.RUNNING && retained.isNotEmpty() && bridgeFailure == null) retireAllForShutdownLocked()
                // Resolutions a body owed to retained callers run first, here,
                // outside any body: a caller resumed inline is an ordinary
                // caller again.
                val owed = takePendingResolutionsLocked()
                if (owed.isNotEmpty()) {
                    lock.unlock()
                    try {
                        owed.forEach { it() }
                    } finally {
                        lock.lock()
                    }
                    continue
                }
                if (jobs.isNotEmpty()) {
                    return jobs.removeFirst().also { it.claimed = true }
                }
                if (phase != Phase.RUNNING) return null

                if (bridgeFailure != null) {
                    // QUARANTINED. Unlike the backendUnusable park below this
                    // is permanent: the bridge refused the token, so no later
                    // backend call is attempted no matter what wakes us.
                    val failed = failRetainedLocked(bridgeFailure!!)
                    if (failed.isNotEmpty()) {
                        lock.unlock()
                        try {
                            failed.forEach { it() }
                        } finally {
                            lock.lock()
                        }
                    }
                    while (jobs.isEmpty() && phase == Phase.RUNNING) idle.await()
                    continue
                }

                if (drainDemand > 0) {
                    // A native drain is in flight or about to be: it waits on
                    // the endpoint itself, so this worker stays out of the
                    // backend wait and parks here, where jobs and close()
                    // still reach it. Off the wait is what the drain waits for.
                    workerOffWait = true
                    idle.signalAll()
                    while (drainDemand > 0 && jobs.isEmpty() && phase == Phase.RUNNING) {
                        idle.await()
                    }
                    continue
                }

                if (postDrainWorkDue) {
                    // ONE observation on resuming: the drain consumed the
                    // endpoint's activity itself, so the level cannot be relied
                    // on to republish what changed meanwhile -- nor to
                    // re-attempt retained work, which is done here regardless
                    // of whether records are published.
                    postDrainWorkDue = false
                    if (phase != Phase.RUNNING) continue
                    val b = backendOrNull ?: return null
                    val quarantined = drainReceiverLocked(b) || probeSenderLocked(b)
                    val resumptions =
                        if (quarantined) {
                            judgeEstablishLocked(null, bridgeFailure) + failRetainedLocked(bridgeFailure!!)
                        } else if (publishSnapshots) {
                            takeRecordLocked(b)
                        } else {
                            emptyList()
                        }
                    lock.unlock()
                    try {
                        resumptions.forEach { it() }
                        // The drain consumed the endpoint's activity itself: this
                        // observation is the pass on which retained work is
                        // re-attempted. The record above may itself have
                        // quarantined the root; the claim reads the CURRENT
                        // failure, never a decision taken before the record.
                        serviceRetained(b)
                    } finally {
                        lock.lock()
                    }
                    continue
                }

                if (backendUnusable) {
                    // A wait that reports CLOSED or the sticky INTERRUPTED
                    // latch returns immediately forever, so re-entering it
                    // would hot-loop. Park on the engine's own condition
                    // instead; accepted commands and close() still get through.
                    while (!level && jobs.isEmpty() && phase == Phase.RUNNING) {
                        idle.await()
                    }
                    level = false
                    // Give the backend wait another chance: the latch may have
                    // been cleared. If it has not, the next wait returns at once
                    // and parks here again -- one wait per wake, not a spin.
                    backendUnusable = false
                    continue
                }

                level = false
                val b = backendOrNull ?: return null
                workerOffWait = false
                val result =
                    run {
                        lock.unlock() // never block in the backend holding the lock
                        try {
                            b.waitForActivity(WAIT_SLICE_MICROS)
                        } finally {
                            lock.lock()
                        }
                    }
                workerOffWait = true
                idle.signalAll() // a drain waiting for the worker to be off the wait
                if (result !is BackendWaitResult.Bridge && (drainReceiverLocked(b) || probeSenderLocked(b))) {
                    val resumptions = judgeEstablishLocked(null, bridgeFailure) + failRetainedLocked(bridgeFailure!!)
                    if (resumptions.isNotEmpty()) {
                        lock.unlock()
                        try {
                            resumptions.forEach { it() }
                        } finally {
                            lock.lock()
                        }
                    }
                    continue
                }
                if (result !is BackendWaitResult.Bridge) {
                    // The pass: every retained demand and retry is attempted
                    // once, with the lock released, and resolved on progress.
                    // No wake is published for it -- the wait this pass came
                    // from is the only activity a retry ever consumes.
                    lock.unlock()
                    try {
                        serviceRetained(b)
                    } finally {
                        lock.lock()
                    }
                    // A retry that quarantined the root ends the pass here: the
                    // record that would follow is a backend call on a token the
                    // bridge refused. The quarantined branch at the top of the
                    // loop delivers the stable failure to every waiter.
                    if (bridgeFailure != null) continue
                }
                when (result) {
                    BackendWaitResult.INTERRUPTED -> backendUnusable = true
                    BackendWaitResult.CLOSED -> {
                        // The documented terminal answer: the endpoint has a
                        // terminal record now, and a pending establishment
                        // waiter must learn it rather than time out. Exactly
                        // one record, on this worker, the same rule as an
                        // activity -- including the same quarantine on refusal.
                        backendUnusable = true
                        if (publishSnapshots) {
                            val resumptions: List<() -> Unit>
                            when (val r = b.snapshot()) {
                                is BackendSnapshotResult.Ok -> {
                                    snapshotFlow?.value = r.snapshot
                                    resumptions = judgeEstablishLocked(r.snapshot, null)
                                }
                                is BackendSnapshotResult.Bridge -> {
                                    bridgeFailure = bridgeFailure ?: BridgeContractException(r.failure)
                                    resumptions = judgeEstablishLocked(null, bridgeFailure)
                                }
                            }
                            if (resumptions.isNotEmpty()) {
                                lock.unlock()
                                try {
                                    resumptions.forEach { it() }
                                } finally {
                                    lock.lock()
                                }
                            }
                        }
                    }
                    // TIMEOUT simply causes another service pass: no record.
                    BackendWaitResult.TIMEOUT -> Unit
                    BackendWaitResult.ACTIVITY -> {
                        // ONE coherent record per activity, on this worker,
                        // published to the stream; a bridge refusal here is the
                        // same quarantine an explicit snapshot refusal is.
                        if (publishSnapshots) {
                            val resumptions: List<() -> Unit>
                            when (val r = b.snapshot()) {
                                is BackendSnapshotResult.Ok -> {
                                    snapshotFlow?.value = r.snapshot
                                    resumptions = judgeEstablishLocked(r.snapshot, null)
                                }
                                is BackendSnapshotResult.Bridge -> {
                                    bridgeFailure = bridgeFailure ?: BridgeContractException(r.failure)
                                    backendUnusable = true
                                    resumptions = judgeEstablishLocked(null, bridgeFailure)
                                }
                            }
                            if (resumptions.isNotEmpty()) {
                                lock.unlock()
                                try {
                                    resumptions.forEach { it() }
                                } finally {
                                    lock.lock()
                                }
                            }
                        }
                    }
                    is BackendWaitResult.Bridge -> {
                        // The bridge answered something its contract forbids.
                        // The token can no longer be trusted, so the native
                        // ownership is QUARANTINED exactly as a refused stop is:
                        // no further backend call, no retry, and every waiter
                        // learns the same stable failure.
                        bridgeFailure =
                            bridgeFailure ?: BridgeContractException(result.failure)
                        backendUnusable = true
                        val resumptions = judgeEstablishLocked(null, bridgeFailure)
                        if (resumptions.isNotEmpty()) {
                            lock.unlock()
                            try {
                                resumptions.forEach { it() }
                            } finally {
                                lock.lock()
                            }
                        }
                    }
                }
            }
        }
        return null // unreachable; the loop returns from inside the lock
    }

    /**
     * Caller holds the lock. Marks the retirement pass settled and hands back
     * every close waiter registered so far -- the only way close waiters
     * leave [retire], so no path can release them unsettled.
     */
    private fun settleRetirementLocked(): List<(Throwable?) -> Unit> {
        retirementSettled = true
        return closeWaiters.toList().also { closeWaiters.clear() }
    }

    private fun retire() {
        var failure: Exception?
        var establish: List<() -> Unit> = emptyList()
        val waiters =
            lock.withLock {
                val quarantinedBy = bridgeFailure
                if (quarantinedBy == null) retireAllForShutdownLocked()
                establish = judgeEstablishLocked(null, quarantinedBy ?: EngineClosedException()) +
                    (if (quarantinedBy != null) failRetainedLocked(quarantinedBy) else emptyList()) +
                    takePendingResolutionsLocked()
                retired = true // latched BEFORE either teardown call
                phase = Phase.RETIRED
                failure = constructionFailure ?: bridgeFailure
                val b = backendOrNull
                if (b == null) {
                    // No root was ever published: nothing to stop or destroy.
                    stopRefusal = failure
                    return@withLock settleRetirementLocked()
                }
                if (failure != null) {
                    // A bridge-contract failure already quarantined the native
                    // ownership: no teardown call is made at all, because the
                    // bridge refused the token we would be tearing down.
                    stopRefusal = failure
                    return@withLock settleRetirementLocked()
                }
                // The lock spans the retirement decision and the teardown --
                // except while waiting, below, for a drain turn that was
                // granted before retirement, when `idle.await()` releases it
                // -- so no any-thread path can pass a stale liveness check and
                // then call into a destroyed endpoint: retirement is latched
                // first, and destroy waits for that turn.
                //
                // Children FIRST: the C stop refuses while a receiver is
                // attached, so an attached receiver is closed (its child
                // retired, exactly once) before the endpoint is stopped. A
                // bridge refusal here quarantines like any other.
                val rx = receiverToken
                if (rx != null) {
                    receiverToken = null
                    val model = receiverModel
                    receiverModel = null
                    when (val c = b.closeReceiver(rx)) {
                        is BackendOpResult.Ok -> model?.retire()
                        is BackendOpResult.Bridge -> {
                            failure = BridgeContractException(c.failure)
                            bridgeFailure = bridgeFailure ?: failure
                            stopRefusal = failure
                            return@withLock settleRetirementLocked()
                        }
                    }
                }
                // The sender likewise: the C stop refuses while EITHER attachment is live.
                val tx = senderToken
                if (tx != null) {
                    senderToken = null
                    val model = senderModel
                    senderModel = null
                    when (val c = b.closeSender(tx)) {
                        is BackendOpResult.Ok -> model?.retire()
                        is BackendOpResult.Bridge -> {
                            failure = BridgeContractException(c.failure)
                            bridgeFailure = bridgeFailure ?: failure
                            stopRefusal = failure
                            return@withLock settleRetirementLocked()
                        }
                    }
                }
                when (val result = b.stop()) {
                    is BackendStopResult.Stopped -> {
                        // A drain granted the native turn may not have reached
                        // its registry lease yet, and one that has holds a pin
                        // destroy would wait on. The stop above is what makes
                        // the C drain answer Closed; destroy is deferred, on
                        // the engine's own condition, until that turn releases.
                        // Queued callers never took the turn and are refused.
                        while (nativeDrainActive) idle.await()
                        // The turn may have ended in the bridge refusing the
                        // row: the token is then quarantined and the stop that
                        // already succeeded stands, but nothing is destroyed.
                        val quarantined = bridgeFailure
                        if (quarantined != null) {
                            failure = quarantined
                            stopRefusal = quarantined
                            return@withLock settleRetirementLocked()
                        }
                        when (val d = b.destroy()) {
                            is BackendOpResult.Ok -> Unit
                            is BackendOpResult.Bridge -> {
                                // A stop succeeded and the RETIREMENT was
                                // refused by the bridge. That is not an ordinary
                                // stop refusal and is not reported as one.
                                failure = BridgeContractException(d.failure)
                                stopRefusal = failure
                            }
                        }
                    }
                    is BackendStopResult.Bridge -> {
                        failure = BridgeContractException(result.failure)
                        stopRefusal = failure
                    }
                    is BackendStopResult.Refused -> {
                        // The native endpoint is STILL LIVE and still owned by
                        // the service tier. Destroying it would free an object
                        // this engine does not own, and retrying stop would be
                        // a second call on a backend that just refused. So the
                        // endpoint is quarantined: no destroy, no retry, no
                        // later backend call at all. The quarantine is latched
                        // under this same lock, before any concurrent path can
                        // observe retirement.
                        failure = StopRefusedException(result.code)
                        stopRefusal = failure
                    }
                }
                settleRetirementLocked()
            }
        // Resumed only after teardown completed, and outside the lock. Every
        // already-registered waiter learns the refusal; none is told that
        // reclamation succeeded.
        establish.forEach { it() }
        waiters.forEach { it(failure) }
    }

    private companion object {
        /**
         * Defensive slice for one backend wait. Strictly positive: a zero
         * timeout elapses immediately and would spin the service loop.
         */
        const val WAIT_SLICE_MICROS: Long = 60_000_000

        /** The C results the control commands may answer besides OK. */
        const val MOQ_ERR_INVAL: Int = -2
        const val MOQ_ERR_PROTO: Int = -3
        const val MOQ_ERR_CLOSED: Int = -4
        const val MOQ_ERR_WRONG_STATE: Int = -5
        const val MOQ_ERR_INTERRUPTED: Int = -13
        const val MOQ_ERR_UNSUPPORTED: Int = -14
    }
}

/** A receiver attached under the root: its child token and the durable state the worker folds its events into. */
internal class AttachedReceiver(val token: Long, val model: ReceiverModel)

/** The endpoint's own refusal of a receiver attach, carried exactly. */
internal class ReceiverRefusedException(val code: MoqResultCode) : IllegalStateException("the endpoint refused the receiver: $code")

/** One write or end-track attempt's outcome. */
internal class WriteAttempt(val outcome: WriteOutcome)

/**
 * Work the worker retains for a caller, as a small state machine under ITS
 * OWN lock: idle-retained, attempt in flight, and settled.
 *
 * An attempt that RAN has a terminal outcome -- a committed value, or a
 * typed failure -- and that outcome is STORED here, under this lock, before
 * anyone can observe the attempt as ended. Every later reader sees the same
 * one: a retirement that arrives while the attempt is in flight, one that
 * arrives after it settled but before the worker published the caller's
 * result, a repeated retirement, the owner's close, and a quarantine. Only
 * [Settled.NoProgress] -- nothing happened natively -- leaves the entry
 * retryable and lets the caller's own cancellation or timeout stand.
 *
 *  * The worker claims an attempt with [beginAttempt] (inside the engine's
 *    own claim decision) and ends it with [endAttempt]; the attempt itself
 *    calls [commit] the moment its native call accepted -- under whatever
 *    lock that call was made.
 *  * [retire] answers immediately with the stored outcome, or, when an
 *    attempt is still in flight and nothing is stored yet, with the
 *    settlement [Settled] that attempt will publish.
 *  * [resolveOwnerRetired] and [resolveQuarantined] never overwrite a stored
 *    outcome: a transfer that happened, or the refusal the native call
 *    answered -- the local service tier's own refusal as much as the peer's
 *    -- is the caller's answer even when the owner closed or the token was
 *    refused meanwhile. They apply only when no attempt won.
 *  * [retired] is read by a sender attempt under the engine lock before its
 *    native call, so a retired entry never makes another one.
 *
 * Nothing here resumes caller code under the engine lock: the deferreds are
 * completed after this lock is released, by the worker or the retiring
 * caller.
 */
internal class Retained<T : Any>(
    val owner: Any,
    val attempt: (EndpointBackend, Retained<T>) -> T?,
    private val onOwnerRetired: () -> Result<T>,
) {
    private enum class State { IDLE, IN_FLIGHT, SETTLED }

    /** The complete outcome of an attempt that ran. */
    sealed interface Settled<out T : Any> {
        /** The native call accepted: ownership transferred. */
        class Committed<T : Any>(val value: T) : Settled<T>

        /** The native call was refused, or the attempt threw: the typed outcome. */
        class Failed(val failure: Throwable) : Settled<Nothing>

        /** Nothing happened natively; the entry stays retryable. */
        data object NoProgress : Settled<Nothing>
    }

    private val lock = ReentrantLock()

    /* guarded by [lock] */
    private var state = State.IDLE

    /** The stored terminal outcome: Committed or Failed, never NoProgress. */
    private var terminal: Settled<T>? = null
    private var settlement: CompletableDeferred<Settled<T>>? = null
    private var retiredFlag = false

    /** The caller's result: the attempt's outcome, its typed failure, or the owner-retired answer. */
    val result = CompletableDeferred<T>()

    /** True once retired: no further attempt may start or make a native call. */
    val retired: Boolean
        get() = lock.withLock { retiredFlag }

    /** How a retirement request was answered. */
    sealed interface Retirement<out T : Any> {
        /** Nothing in flight: the attempt's stored outcome, or no progress. */
        class Now<T : Any>(val settled: Settled<T>) : Retirement<T>

        /** An attempt is in flight and has stored nothing yet: the answer is what it settles with. */
        class AfterSettle<T : Any>(val settled: CompletableDeferred<Settled<T>>) : Retirement<T>
    }

    /** Worker, inside the engine's claim decision: start an attempt; false when the entry is retired, already in flight, or terminally settled. */
    fun beginAttempt(): Boolean =
        lock.withLock {
            if (retiredFlag || state != State.IDLE) return false
            state = State.IN_FLIGHT
            true
        }

    /** The attempt, the moment its native call accepted: store the committed value. */
    fun commit(value: T) =
        lock.withLock {
            if (terminal == null) terminal = Settled.Committed(value)
        }

    /**
     * Worker: the attempt ended, with [failure] when its native call was
     * refused or it threw. Stores the terminal outcome, settles a retirement
     * that arrived in flight, and returns the committed value or null. A
     * terminal outcome leaves the entry SETTLED -- never retryable again;
     * no progress returns it to IDLE.
     */
    fun endAttempt(failure: Throwable?): T? {
        val (settled, waiter) =
            lock.withLock {
                val s: Settled<T> =
                    terminal
                        ?: if (failure != null) Settled.Failed(failure).also { terminal = it } else Settled.NoProgress
                state = if (s is Settled.NoProgress) State.IDLE else State.SETTLED
                val w = settlement
                settlement = null
                s to w
            }
        waiter?.complete(settled)
        return (settled as? Settled.Committed)?.value
    }

    /** Caller: retire the entry. */
    fun retire(): Retirement<T> =
        lock.withLock {
            retiredFlag = true
            val stored = terminal
            if (state == State.IN_FLIGHT && stored == null) {
                Retirement.AfterSettle(settlement ?: CompletableDeferred<Settled<T>>().also { settlement = it })
            } else {
                Retirement.Now(stored ?: Settled.NoProgress)
            }
        }

    /** The committed value, if any; for a caller that already retired. */
    fun committed(): T? = lock.withLock { (terminal as? Settled.Committed)?.value }

    /** The owner closed: its answer, unless an attempt already settled with its own outcome. */
    fun resolveOwnerRetired() {
        val stored = lock.withLock { retiredFlag = true; terminal }
        if (stored != null) {
            publish(stored)
            return
        }
        val answer = onOwnerRetired()
        lock.withLock {
            if (terminal == null) answer.getOrNull()?.let { terminal = Settled.Committed(it) }
        }
        answer.fold({ v -> result.complete(v) }, { t -> result.completeExceptionally(t) })
    }

    /** The bridge refused the token: the stable [failure], unless an attempt already settled with its own outcome. */
    fun resolveQuarantined(failure: Throwable) {
        val stored = lock.withLock { retiredFlag = true; terminal }
        if (stored != null) publish(stored) else result.completeExceptionally(failure)
    }

    private fun publish(settled: Settled<T>) {
        when (settled) {
            is Settled.Committed -> result.complete(settled.value)
            is Settled.Failed -> result.completeExceptionally(settled.failure)
            is Settled.NoProgress -> Unit
        }
    }
}

/** How a demand or attempt answered: resolved now, or retained by the worker until progress is possible. */
internal sealed interface Demanded<out T : Any> {
    class Resolved<T : Any>(val value: T) : Demanded<T>

    class Pending<T : Any>(val retained: Retained<T>) : Demanded<T>
}

/** A sender attached under the root: its child token and the durable state the worker's probes publish into. */
internal class AttachedSender(val token: Long, val model: SenderModel)

/** The endpoint's own refusal of a sender attach, carried exactly. */
internal class SenderRefusedException(val code: MoqResultCode) : IllegalStateException("the endpoint refused the sender: $code")

/**
 * Thrown when a suspending engine call is made from inside an engine body, on
 * the endpoint's own service thread. Internal: it reports an implementation
 * mistake, and is deliberately not part of any public surface.
 */
internal class EngineReentryException(call: String) :
    IllegalStateException(
        "$call was called from inside an engine body on the endpoint's own service " +
            "thread; it would wait for the very worker executing that body. Use post() " +
            "for follow-up work on the engine.",
    )

/** Thrown when a submission is refused because the engine is shutting down. */
internal class EngineClosedException : IllegalStateException("endpoint engine is closed")

/**
 * The stable failure every waiter receives when the bridge answered outside
 * its own contract. It is deliberately NOT a [StopRefusedException]: a private
 * registry refusal is not a native stop refusal, and conflating the two would
 * report a bridge defect as an ordinary lifecycle outcome.
 */
internal class BridgeContractException(
    val failure: BridgeFailure,
) : IllegalStateException(
    "the native bridge refused ${failure.op} with code ${failure.code}; the " +
        "endpoint is quarantined, not destroyed",
)

/**
 * The stable failure every close waiter receives when the native stop was
 * refused and the endpoint was therefore quarantined rather than reclaimed.
 *
 * INTERNAL on purpose: the public failure mapping is a later API ruling. What
 * is settled here is that a waiter is never told reclamation succeeded, and
 * that it learns the exact native [code].
 */
internal class StopRefusedException(
    val code: MoqResultCode,
) : IllegalStateException(
    "moq_endpoint_stop() was refused with $code; the endpoint is quarantined, " +
        "not destroyed",
)
