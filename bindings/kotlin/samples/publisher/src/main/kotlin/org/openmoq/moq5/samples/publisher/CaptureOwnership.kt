package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.CancellationException

/**
 * Who owns what during capture, and what closing means.
 *
 * A real capture session acquires several platform resources in order, and any
 * one of them can fail: the camera can be taken by another app between the
 * permission grant and the open, the encoder can refuse the format, the
 * microphone can be busy. What must never happen is a partial acquisition that
 * leaks the resources that DID open, or a close path that stops at the first
 * failure and abandons everything after it.
 *
 * This is the discipline the accepted `broadcastLifetime` already applies to
 * the endpoint and its sender, stated for an ordered set of resources:
 *
 *  * ownership starts the instant an acquisition RETURNS, or the instant a
 *    delivered resource is ADOPTED;
 *  * a partial acquisition unwinds exactly the resources acquired, each once;
 *  * children close before what they were opened from;
 *  * every close is ATTEMPTED even after an earlier close failed, and the
 *    failures are collected, in order, onto the primary failure;
 *  * closing twice closes nothing twice;
 *  * once a generation retires, a late callback settles nothing and cannot
 *    reopen anything.
 *
 * **Two platform shapes.** A codec, an input surface and an audio input are
 * CREATED and returned. A camera and a capture session are not:
 * `CameraManager.openCamera(...)` returns `void`, and what arrives later on the
 * callback handler is ONE of `CameraDevice.StateCallback.onOpened`,
 * `onDisconnected` or `onError` -- the last two "may be called instead of
 * onOpened", CARRY the `CameraDevice`, and the platform says to "clean up the
 * camera with close" after them (android-36.1 `CameraDevice.java:1640-1693`).
 * `createCaptureSession(...)` settles through
 * `CameraCaptureSession.StateCallback.onConfigured` or `onConfigureFailed`; a
 * failed session "is considered to be closed" and every later method on it
 * throws, so it is NOT closed again. A failure is therefore not "nothing was
 * acquired": it can carry a resource that must be closed exactly once, or one
 * that must not be touched.
 *
 * **Callback delivery outlives the capture worker only if we let it.** Those
 * callbacks are posted to the handler this session supplies. Android documents
 * that posting to a `HandlerThread` after `quitSafely()` FAILS
 * (`HandlerThread.java:240-241`): a quit looper does not drain future
 * callbacks, it drops them. So an ownership that has `close()`d but still has
 * an acquisition outstanding must keep the [CallbackDispatcher] alive until
 * that acquisition settles -- otherwise the `CameraDevice` the platform is
 * about to hand us is delivered to nobody and leaks until the process dies.
 * Quitting the dispatcher before settlement is the defect, and it is modelled
 * as one.
 *
 * **What that wait is NOT.** The installed sources do not say that exactly one
 * of those callbacks arrives, and they do not bound when one does.
 * `onDisconnected` and `onError` "may be called instead of onOpened" when the
 * OPEN fails -- that is a terminal answer to this acquisition -- but the same
 * two are also how a camera that DID open is later lost, and `onClosed`
 * follows a `CameraDevice.close()`. So a callback arriving is not by itself a
 * settlement: an acquisition settles when the TICKET it was submitted under
 * settles, exactly once, and a later callback about an INSTALLED resource is a
 * device-loss fact -- [CaptureOwnership.lost] -- not a second settlement.
 *
 * This sample therefore does not promise finite settlement, and it does not
 * manufacture one. No timer here abandons an outstanding ticket: dropping the
 * dispatcher or the retained worker while the platform still holds our
 * callback is precisely the leak this file exists to prevent, and a deadline
 * would only move the leak to where nobody is watching. A ticket that never
 * settles keeps its dispatcher and its retained worker alive for the life of
 * the process, and that is a STATED cost of refusing to invent a deadline.
 * Bounding it needs a platform fact this sample does not have; if a bound is
 * wanted it belongs in the public capture contract, not in this sample.
 *
 * **Identity is the platform resource.** Two wrappers around one
 * `CameraDevice` are the same resource; a duplicate `onOpened` for the device
 * this session is using must close nothing. A delivery of a DIFFERENT resource
 * on a ticket nobody holds is unowned, and the ownership that received it is
 * the only party that can dispose of it -- so it does, once, and reports it.
 *
 * The resources are named by ROLE, not by platform class: this file is
 * host-testable and holds no platform type at all.
 */
internal enum class CaptureResource {
    /** The thread/handler platform callbacks are delivered on. Acquired first, closed last. */
    Worker,

    /** The opened camera, from which everything else on the video side hangs. */
    Camera,

    /** The configured capture stream that feeds the encoder's input surface. */
    CameraStream,

    /** The encoder input surface the stream draws into. */
    CameraSurface,

    /** The audio input. */
    Microphone,

    /** The video encoder. */
    VideoEncoder,

    /** The audio encoder. */
    AudioEncoder,
}

/**
 * One acquired resource, closed exactly once by its owner.
 *
 * [identity] is the PLATFORM object -- the `CameraDevice`, the `MediaCodec` --
 * not this wrapper, so two deliveries of one device compare equal here.
 */
internal interface ResourceHandle {
    val identity: Any

    /** Closes the platform resource. May throw; is never called twice. */
    fun close()
}

/**
 * The handler platform callbacks are posted to: `Handler.post` semantics.
 * Returns `false`, delivering nothing, once the looper has quit.
 */
internal interface CallbackDispatcher {
    val isAlive: Boolean

    fun deliver(callback: () -> Unit): Boolean

    /** `quitSafely()`: already-due callbacks run; later posts fail. */
    fun quit()
}

/**
 * One outstanding asynchronous acquisition. It belongs to one generation and one
 * resource, and settles exactly once -- by adoption or by failure.
 */
internal class AcquisitionTicket(val generation: Long, val resource: CaptureResource)

/** What one synchronous acquisition did. */
internal sealed interface AcquireOutcome {
    data object Acquired : AcquireOutcome

    /**
     * The acquisition itself threw an ordinary failure; nothing was acquired
     * and nothing is owned. A failure that carries failures of its own partial
     * cleanup is never this: it leaves [CaptureOwnership.acquire] as itself.
     */
    data class Failed(val resource: CaptureResource, val reason: String) : AcquireOutcome

    /** The generation retired first; what was returned is closed here, once. */
    data class Retired(val resource: CaptureResource, val reason: String) : AcquireOutcome
}

/**
 * An opening that failed AND whose disposal of what it had already opened
 * failed too: the explicit fact that nothing was proved disposed.
 *
 * It is thrown by an `open` given to [CaptureOwnership.acquire] and unwrapped
 * there, never beyond it: [primary] -- the opening failure, by identity, of
 * whatever class -- leaves carrying each [failures] entry that is not itself.
 * The fact is this object, not anything inferred from [primary]'s suppressed
 * list, which cannot hold [primary] even when the disposal raised it.
 */
internal class UnprovedDisposal(val primary: Throwable, val failures: List<Throwable>) :
    Exception("an opening failed and its disposal was not proved: ${primary.message}") {
    init {
        require(failures.isNotEmpty()) { "an unproved disposal names at least one failure" }
    }

    /** [primary], carrying each distinct disposal failure once. */
    fun unwrap(): Throwable {
        for (failure in failures) {
            if (failure !== primary && primary.suppressed.none { it === failure }) primary.addSuppressed(failure)
        }
        return primary
    }
}

/** What settling an outstanding acquisition did. */
internal sealed interface AdoptOutcome {
    /** Installed and owned; closes with everything else, in acquisition order. */
    data object Adopted : AdoptOutcome

    /**
     * The generation retired or closed while the request was in flight. The
     * delivered handle is closed HERE, exactly once, and never installed.
     */
    data class Retired(val reason: String) : AdoptOutcome

    /**
     * The ticket is not outstanding here: already settled, or never issued by
     * this ownership. The LIVE resource is never touched. If the delivered
     * handle IS the live resource (by identity), nothing is closed -- it is a
     * duplicate delivery. If it is a different, unowned resource, it is closed
     * here exactly once and reported through [LateCleanupSink], because this
     * ownership is the only party holding it.
     */
    data class Unknown(val reason: String) : AdoptOutcome

    /**
     * A failure callback settled the request. A handle it carried -- the
     * `CameraDevice` that `onDisconnected`/`onError` deliver instead of
     * `onOpened` -- is closed here exactly once and never installed; a failed
     * capture session carries nothing to close.
     */
    data class Failed(val reason: String) : AdoptOutcome
}

/** What losing an installed resource did. */
internal sealed interface LostOutcome {
    /** The installed handle was closed here, once; a later close skips it. */
    data object Released : LostOutcome

    data class Ignored(val reason: String) : LostOutcome
}

/**
 * Cleanup facts that arrive AFTER `close()` has returned -- a late adoption's
 * disposal, a foreign delivery's disposal, a close that threw on a late handle.
 * They cannot ride a return value that has already been consumed.
 */
internal interface LateCleanupSink {
    fun onLateClose(generation: Long, resource: CaptureResource, failure: Exception?)

    /** An unowned resource was delivered and disposed of here. */
    fun onForeignDisposal(generation: Long, resource: CaptureResource, failure: Exception?)
}

/**
 * Something that still needs the callback worker -- and therefore the
 * dispatcher its callbacks arrive on -- to exist.
 *
 * An outstanding acquisition ticket is one such dependent; so is an ADMITTED
 * encoder transfer, which is still inside codec-owned memory and will deliver
 * its result on that same dispatcher. Both must hold the worker, through the
 * ownership that owns it, rather than through a second object that happens to
 * quit a dispatcher of its own: a parallel owner cannot see the ownership's
 * close, and the ownership cannot see the parallel owner's users.
 *
 * [retainWorker] returns false once the worker has actually been released; a
 * dependent refused a retention must not expect a callback to be delivered.
 */
internal interface WorkerRetention {
    /**
     * Takes a retention on the callback worker. FALSE when it is already gone,
     * and a caller refused one must not proceed: it has no dispatcher to
     * deliver on, and it must not release what it never took.
     *
     * A dependent that needs a second thing -- a codec lease, say -- acquires
     * BOTH or NEITHER: the retention is taken first, and if the other
     * acquisition is refused the retention is given straight back. Nothing is
     * acquired while another object's lock is held.
     */
    fun retainWorker(): Boolean

    /** Releases one retention taken by [retainWorker]; never one that was refused. */
    fun releaseWorker()
}

/**
 * The ordered ownership of one capture generation.
 *
 * Implementations are judged by ORDER and IDENTITY, never by how many handles
 * they closed.
 */
internal interface CaptureOwnership : WorkerRetention {
    val generation: Long

    /**
     * The dispatcher platform callbacks are delivered on. It stays alive while
     * any ticket is outstanding -- which is a requirement on this ownership,
     * not a claim that the platform will settle every ticket.
     */
    val dispatcher: CallbackDispatcher

    /** True once [close] has run; a second call closes nothing again. */
    val isClosed: Boolean

    /** Outstanding asynchronous acquisitions that have not settled. */
    val pending: Int

    /**
     * Acquires one resource the platform creates and returns. Ownership begins
     * when [open] returns.
     *
     * Every resource but the [CaptureResource.Worker] runs on the worker, so
     * the worker is protected from before [open] is called until the handle is
     * installed or, once this generation has retired, closed -- whatever [open]
     * does, a throw included. If that protection cannot be taken because the
     * worker is already gone, [open] is not called and the outcome is
     * [AcquireOutcome.Retired].
     *
     * An [open] that throws an ordinary failure is [AcquireOutcome.Failed]. One
     * that throws [UnprovedDisposal] -- it failed, and so did its own disposal
     * of what it had opened -- is not reduced to a reason: its primary is
     * rethrown as itself, carrying the disposal failures and anything this
     * acquisition's own cleanup then found.
     */
    fun acquire(resource: CaptureResource, open: () -> ResourceHandle): AcquireOutcome

    /** Begins an acquisition that settles through a platform callback. */
    fun request(resource: CaptureResource): AcquisitionTicket

    /** The platform delivered the resource. */
    fun adopt(ticket: AcquisitionTicket, handle: ResourceHandle): AdoptOutcome

    /**
     * The platform reported the acquisition failed. [carried] is the resource
     * the failure delivered, if any; it is closed here and never installed.
     */
    fun fail(ticket: AcquisitionTicket, reason: String, carried: ResourceHandle?): AdoptOutcome

    /**
     * An INSTALLED resource was lost -- `onDisconnected`/`onError` after
     * `onOpened`. It is closed here, once, and skipped by the later close.
     */
    fun lost(resource: CaptureResource, reason: String): LostOutcome

    /** Retires this generation without closing anything yet. */
    fun retire(reason: String)

    /**
     * Closes every installed resource, in reverse acquisition order, exactly
     * once each, continuing past a failure -- EXCEPT the worker while a ticket
     * OR a [WorkerRetention] is outstanding, which is retained until the last
     * of them settles and then closed late.
     *
     * Deciding to close is not the same as having closed. The worker is what
     * every other resource's own close runs ON, so it is retired only once BOTH
     * are true: every dependent has settled, AND this close has finished with
     * the resources that depend on it -- including a codec whose disposal its
     * handle only ASKS for. A dependent that settles while those closes are
     * still running does not retire the worker early.
     *
     * **How a failure leaves.** A close that throws never stops the ones behind
     * it: every handle is attempted, then the boundary and the retained worker
     * are settled, and only then is anything propagated.
     *
     *  * Ordinary failures keep this method's RETURN contract, above.
     *  * A CANCELLATION or an ERROR raised by any close cannot be returned, so
     *    the first one raised leaves by THROW, by identity, carrying [primary]
     *    and every other collected failure as suppressed.
     *  * If [primary] is itself a cancellation it is the ORIGINAL one: it is
     *    what leaves, carrying the later cleanup failures, and a cancellation
     *    raised by a close does not replace it. Returns what should end the session: [primary] with every
     * cleanup failure attached in close order; otherwise the first cleanup
     * failure; otherwise `null`.
     */
    fun close(primary: Exception?): Exception?
}

/** The one boundary a test may hold an adoption at: before settlement, outside the monitor. */
internal fun interface AdoptionSchedule {
    fun beforeSettle()

    companion object {
        val NONE: AdoptionSchedule = AdoptionSchedule { }
    }
}

/**
 * The ownership this sample runs.
 *
 * One monitor guards the bookkeeping -- what is installed, what is outstanding,
 * and who still needs the callback worker. Nothing else happens under it: every
 * platform close and every notification runs outside, and close never waits for
 * a callback of its own.
 *
 * The worker is the resource every other one's close runs on, so it is retired
 * only when nothing needs it any more: no outstanding ticket, no admitted
 * transfer holding a [WorkerRetention], no cleanup in progress, and this
 * ownership's own close finished with the resources that depend on it. A
 * cleanup takes that protection in the same locked step that stops the ticket
 * or the installed entry from providing it, and gives it back when its close
 * attempt has ended.
 */
internal class OrderedCaptureOwnership(
    override val generation: Long,
    override val dispatcher: CallbackDispatcher,
    val late: LateCleanupSink,
    val schedule: AdoptionSchedule = AdoptionSchedule.NONE,
) : CaptureOwnership {
    private val lock = Any()
    private val installed = mutableListOf<Pair<CaptureResource, ResourceHandle>>()
    private val tickets = mutableSetOf<AcquisitionTicket>()
    private var closed = false
    private var retired = false
    private var closeBoundaryReached = false
    private var retainedWorker: ResourceHandle? = null
    private var workerRetentions = 0
    private var workerGone = false

    override val isClosed: Boolean get() = synchronized(lock) { closed }

    override val pending: Int get() = synchronized(lock) { tickets.size }

    override fun acquire(resource: CaptureResource, open: () -> ResourceHandle): AcquireOutcome {
        // Every resource but the worker is opened ON the worker, and its
        // callbacks can arrive while it opens. The worker is therefore
        // protected from BEFORE the first platform effect until the handle is
        // installed or disposed of -- on every exit -- and a resource whose
        // worker is already gone is not opened at all.
        val protected = resource != CaptureResource.Worker
        if (protected && !retainWorker()) {
            return AcquireOutcome.Retired(resource, "the callback worker has already been released; nothing was opened")
        }
        val cleanup = Cleanup()
        var primary: Throwable? = null
        val outcome: AcquireOutcome
        try {
            outcome = openAndSettle(resource, open, cleanup)
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
            if (protected) releaseProtection()?.let { cleanup.closeAndReport(CaptureResource.Worker, it) }
            primary?.let { leaving -> attachOthers(leaving, cleanup.collected()) }
        }
        cleanup.settle()
        return outcome
    }

    /**
     * Opens [resource] and settles the handle: installed, or -- once this
     * generation has retired -- closed here, once.
     *
     * An open that throws an ordinary failure acquired nothing, and is the
     * named [AcquireOutcome.Failed]. An [UnprovedDisposal] is not: what it
     * opened was not proved disposed, so its primary leaves as itself, with
     * that whole graph, to the caller that owns the startup.
     */
    private fun openAndSettle(resource: CaptureResource, open: () -> ResourceHandle, cleanup: Cleanup): AcquireOutcome {
        val handle =
            try {
                open()
            } catch (unproved: UnprovedDisposal) {
                throw unproved.unwrap()
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (failure: Exception) {
                return AcquireOutcome.Failed(resource, failure.message ?: failure::class.simpleName.orEmpty())
            }
        val install = synchronized(lock) { if (closed || retired) false else installed.add(resource to handle) }
        if (install) return AcquireOutcome.Acquired
        cleanup.closeAndReport(resource, handle)
        return AcquireOutcome.Retired(resource, "retired before this acquisition returned")
    }

    override fun request(resource: CaptureResource): AcquisitionTicket =
        AcquisitionTicket(generation, resource).also { synchronized(lock) { tickets.add(it) } }

    override fun retainWorker(): Boolean = synchronized(lock) { reserveProtectionLocked() }

    override fun releaseWorker() {
        releaseProtection()?.let {
            val cleanup = Cleanup()
            cleanup.closeAndReport(CaptureResource.Worker, it)
            cleanup.settle()
        }
    }

    override fun adopt(ticket: AcquisitionTicket, handle: ResourceHandle): AdoptOutcome {
        schedule.beforeSettle()
        var action: (() -> Unit)? = null
        val outcome =
            synchronized(lock) {
                when {
                    ticket !in tickets -> {
                        // A resource nobody is waiting for. It is disposed only
                        // if its IDENTITY is not one this generation still owns.
                        val live = installed.any { it.second.identity === handle.identity } ||
                            retainedWorker?.identity === handle.identity
                        if (!live) action = disposal(ticket.resource, handle, reserveProtectionLocked())
                        AdoptOutcome.Unknown("no outstanding ticket for ${ticket.resource} in generation $generation")
                    }
                    closed || retired -> {
                        tickets.remove(ticket)
                        action = lateClose(ticket.resource, handle, reserveProtectionLocked())
                        AdoptOutcome.Retired("this generation ended while ${ticket.resource} was opening")
                    }
                    else -> {
                        tickets.remove(ticket)
                        installed.add(ticket.resource to handle)
                        AdoptOutcome.Adopted
                    }
                }
            }
        action?.invoke()
        return outcome
    }

    override fun fail(ticket: AcquisitionTicket, reason: String, carried: ResourceHandle?): AdoptOutcome {
        var action: (() -> Unit)? = null
        val outcome =
            synchronized(lock) {
                if (ticket !in tickets) {
                    if (carried != null && installed.none { it.second.identity === carried.identity }) {
                        action = disposal(ticket.resource, carried, reserveProtectionLocked())
                    }
                    AdoptOutcome.Unknown("no outstanding ticket for ${ticket.resource}")
                } else {
                    tickets.remove(ticket)
                    action = lateClose(ticket.resource, carried, reserveProtectionLocked())
                    AdoptOutcome.Failed(reason)
                }
            }
        action?.invoke()
        return outcome
    }

    override fun lost(resource: CaptureResource, reason: String): LostOutcome {
        var protection = false
        val handle =
            synchronized(lock) {
                val at = installed.indexOfFirst { it.first == resource }
                if (at < 0) {
                    null
                } else {
                    protection = reserveProtectionLocked()
                    installed.removeAt(at).second
                }
            } ?: return LostOutcome.Ignored("$resource is not installed")
        val cleanup = Cleanup()
        try {
            cleanup.closeAndReport(resource, handle, reportClean = false)
        } finally {
            if (protection) releaseProtection()?.let { cleanup.closeAndReport(CaptureResource.Worker, it) }
        }
        cleanup.settle()
        return LostOutcome.Released
    }

    override fun retire(reason: String) {
        synchronized(lock) { retired = true }
    }

    override fun close(primary: Exception?): Exception? {
        val toClose =
            synchronized(lock) {
                if (closed) return primary
                closed = true
                val order = installed.reversed().toMutableList()
                installed.clear()
                if (tickets.isNotEmpty() || workerRetentions > 0) {
                    val worker = order.firstOrNull { it.first == CaptureResource.Worker }
                    if (worker != null) {
                        order.remove(worker)
                        retainedWorker = worker.second
                    }
                }
                if (retainedWorker == null) workerGone = true
                order
            }
        val cleanup = Cleanup()
        for ((_, handle) in toClose) cleanup.add(attempt { handle.close() })
        // Deciding to close is not having closed: only now is this close
        // finished with the resources that run ON the worker.
        val lateWorker = synchronized(lock) { closeBoundaryReached = true; workerToReleaseLocked() }
        lateWorker?.let { cleanup.closeAndReport(CaptureResource.Worker, it) }

        val collected = cleanup.collected()
        val terminal = collected.firstOrNull { it is CancellationException || it is Error } ?: cleanup.firstReportFailure()
        if (terminal != null) {
            val carrier = if (primary is CancellationException) primary else terminal
            attachOthers(carrier, collected + listOfNotNull(primary))
            throw carrier
        }
        val failures = collected.filterIsInstance<Exception>()
        val ending = primary ?: failures.firstOrNull()
        if (ending != null) failures.filter { it !== ending }.forEach { ending.addSuppressed(it) }
        return ending
    }

    /** The disposal of a handle this generation does not own. */
    private fun disposal(resource: CaptureResource, handle: ResourceHandle, protection: Boolean): () -> Unit = {
        val cleanup = Cleanup()
        try {
            cleanup.closeAndDisown(resource, handle)
        } finally {
            if (protection) releaseProtection()?.let { cleanup.closeAndReport(CaptureResource.Worker, it) }
        }
        cleanup.settle()
    }

    /** A settlement that arrived after this generation ended. */
    private fun lateClose(resource: CaptureResource, handle: ResourceHandle?, protection: Boolean): () -> Unit = {
        val cleanup = Cleanup()
        try {
            handle?.let { cleanup.closeAndReport(resource, it) }
        } finally {
            if (protection) releaseProtection()?.let { cleanup.closeAndReport(CaptureResource.Worker, it) }
        }
        cleanup.settle()
    }

    /**
     * Takes worker protection while there is a live worker to protect (lock
     * held). A FALSE result is not a reservation: the caller must carry it and
     * release nothing, or the count goes negative on a path where the worker
     * has already retired.
     */
    private fun reserveProtectionLocked(): Boolean =
        if (workerGone) {
            false
        } else {
            workerRetentions++
            true
        }

    /** Gives protection back; returns the worker to close when nothing else needs it. */
    private fun releaseProtection(): ResourceHandle? =
        synchronized(lock) {
            workerRetentions--
            workerToReleaseLocked()
        }

    private fun workerToReleaseLocked(): ResourceHandle? =
        if (closed && closeBoundaryReached && tickets.isEmpty() && workerRetentions == 0) {
            retainedWorker.also { if (it != null) { retainedWorker = null; workerGone = true } }
        } else {
            null
        }

    /**
     * One attempt -- a close, or a notification -- whose failure is COLLECTED
     * rather than allowed to end the cleanup around it. The inner rethrow is
     * the cancellation rule for ordinary bodies; this is a cleanup collector,
     * so it is caught again here and decided by [Cleanup.settle].
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

    /** Attaches every other collected failure to [carrier], once each, never itself. */
    private fun attachOthers(carrier: Throwable, collected: List<Throwable>) {
        val attached = mutableListOf<Throwable>()
        for (other in collected) {
            if (other === carrier || attached.any { it === other }) continue
            attached.add(other)
            carrier.addSuppressed(other)
        }
    }

    /**
     * One cleanup transaction: every close and every report in it is collected,
     * in occurrence order, and the decision is made once, at the end.
     */
    private inner class Cleanup {
        private val collected = mutableListOf<Throwable>()
        private val fromReports = mutableListOf<Throwable>()

        fun closeAndReport(resource: CaptureResource, handle: ResourceHandle, reportClean: Boolean = true) {
            val closeFailure = attempt { handle.close() }
            closeFailure?.let { collected.add(it) }
            if (closeFailure == null && !reportClean) return
            attempt { late.onLateClose(generation, resource, closeFailure as? Exception) }?.let {
                collected.add(it)
                fromReports.add(it)
            }
        }

        fun closeAndDisown(resource: CaptureResource, handle: ResourceHandle) {
            val closeFailure = attempt { handle.close() }
            closeFailure?.let { collected.add(it) }
            attempt { late.onForeignDisposal(generation, resource, closeFailure as? Exception) }?.let {
                collected.add(it)
                fromReports.add(it)
            }
        }

        fun add(raised: Throwable?) {
            raised?.let { collected.add(it) }
        }

        fun collected(): List<Throwable> = collected.toList()

        fun firstReportFailure(): Throwable? = fromReports.firstOrNull()

        /**
         * The first cancellation or Error by occurrence leaves, by identity,
         * carrying the rest. Otherwise a REPORT failure leaves -- the sink is
         * what would have carried it. An ordinary close failure is already
         * carried by the report it was given to.
         */
        fun settle() {
            val carrier = collected.firstOrNull { it is CancellationException || it is Error }
                ?: fromReports.firstOrNull()
                ?: return
            attachOthers(carrier, collected)
            throw carrier
        }
    }
}
