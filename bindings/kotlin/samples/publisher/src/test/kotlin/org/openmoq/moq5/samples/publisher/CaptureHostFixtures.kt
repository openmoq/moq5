package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CancellationException

/**
 * Conforming HOST fixtures for the capture contracts.
 *
 * These exist for one reason: a contract is only worth filling in if it can
 * be satisfied at all. Each fixture implements its interface under exactly the
 * exclusion rule the product KDoc states, so every RED row can also be run
 * against it and shown to PASS. They prove satisfiability and drive the
 * accepted pipeline in the topology rows; they prove nothing about a platform.
 *
 * They are deliberately plain: one monitor each, decisions under it, platform
 * calls and owner notification outside it.
 */

/** `Handler.post` semantics on the host: inline while alive, refused after quit. */
/** The per-output byte limit host rows declare unless a row is about the limit itself: above every fixture payload, so accepted behaviors are unchanged. */
internal const val HOST_MAX_OUTPUT_BYTES = 65_536

internal class HostDispatcher : CallbackDispatcher {
    @Volatile
    override var isAlive: Boolean = true
        private set

    override fun deliver(callback: () -> Unit): Boolean {
        if (!isAlive) return false
        callback()
        return true
    }

    override fun quit() {
        isAlive = false
    }
}

/** A platform resource with an identity distinct from any wrapper around it. */
internal class HostResource(val name: String) {
    override fun toString(): String = name
}

/** One wrapper around a platform resource; closes are logged by identity. */
internal class HostHandle(
    override val identity: Any,
    private val log: MutableList<Any>,
    private val fail: Exception? = null,
) : ResourceHandle {
    override fun close() {
        synchronized(log) { log.add(identity) }
        if (fail != null) throw fail
    }
}

/** Every late cleanup fact, in order. */
internal class LateLog : LateCleanupSink {
    private val events = mutableListOf<String>()

    @Synchronized
    override fun onLateClose(generation: Long, resource: CaptureResource, failure: Exception?) {
        events.add("late-close:$resource" + (failure?.let { ":" + it.message } ?: ""))
    }

    @Synchronized
    override fun onForeignDisposal(generation: Long, resource: CaptureResource, failure: Exception?) {
        events.add("foreign:$resource" + (failure?.let { ":" + it.message } ?: ""))
    }

    @Synchronized
    fun all(): List<String> = events.toList()
}

/**
 * A codec the session owns, with the lifetime a transfer depends on.
 *
 * The owner asks for disposal whenever it likes; a transfer holding a lease
 * defers it. The LAST lease released disposes -- outside the lock, exactly
 * once -- and only then is the callback dispatcher quit. Every disposal and
 * every index release is appended to one shared order log, so "the codec was
 * still alive when the index went back" is an assertion about a list.
 */
internal class HostCodec(
    private val order: MutableList<String>,
    private val dispatcher: HostDispatcher? = null,
    private val name: String = "codec",
    private val worker: WorkerRetention? = null,
    /** Raised ONCE by the lease RETURN, as a real disposal path may; null means not this time. */
    private val raiseOnReturn: (() -> Throwable?)? = null,
    /** One shared log of the cleanup operations, in the order they actually happen. */
    private val trace: MutableList<String>? = null,
) : HostCodecOwner {
    private val lock = Any()
    private var leases = 0
    private var requested = false
    private var committed = false
    private var destroyed = false
    private var disposals = 0
    private var peakLeases = 0
    private var returns = 0
    private var returnRaiseArmed = true

    override val isDisposed: Boolean get() = synchronized(lock) { destroyed }

    override val disposalCount: Int get() = synchronized(lock) { disposals }

    val outstandingLeases: Int get() = synchronized(lock) { leases }

    val everLeased: Boolean get() = synchronized(lock) { peakLeases > 0 }

    /** How many times a lease was GIVEN BACK. Balanced bookkeeping, for a row's assertions. */
    val leaseReturns: Int get() = synchronized(lock) { returns }

    override fun retainForTransfer(): Boolean {
        // BOTH or NEITHER. The transfer comes back on the worker's dispatcher,
        // so the worker is taken FIRST -- outside this codec's lock, and its
        // refusal is respected: a transfer with nowhere to deliver never
        // touches the codec at all. If the lease is then refused, the retention
        // is given straight back, so nothing is left holding either side.
        if (worker != null && !worker.retainWorker()) return false
        val granted =
            synchronized(lock) {
                // COMMITTED, not destroyed, is what closes the door: the
                // decision and the refusal of new leases are the same instant,
                // so no lease can be granted in the gap before the destruction
                // itself runs.
                if (committed) {
                    false
                } else {
                    leases++
                    if (leases > peakLeases) peakLeases = leases
                    true
                }
            }
        if (!granted) worker?.releaseWorker()
        if (granted) trace?.let { synchronized(it) { it.add("retain") } }
        return granted
    }

    override fun releaseAfterTransfer() {
        trace?.let { synchronized(it) { it.add("lease") } }
        val commit = synchronized(lock) { leases--; returns++; commitLocked() }
        if (commit) destroyOutsideTheLock()
        // The codec goes before the worker it was driven from.
        worker?.releaseWorker()
        // Scripted ONCE, and only when the script says so: a row that checks
        // the cleanup after a failed return still needs the codec to behave
        // before and afterwards.
        val script = synchronized(lock) { if (returnRaiseArmed) raiseOnReturn else null }
        val raise = script?.invoke()
        if (raise != null) {
            synchronized(lock) { returnRaiseArmed = false }
            throw raise
        }
    }

    override fun requestDisposal() {
        val commit = synchronized(lock) { requested = true; commitLocked() }
        if (commit) destroyOutsideTheLock()
    }

    /** The whole decision, under the lock, exactly once. The destruction is not done here. */
    private fun commitLocked(): Boolean {
        if (!requested || committed || leases > 0) return false
        committed = true
        disposals++
        return true
    }

    /** The platform call. Outside every lock, by the thread that won the commit. */
    private fun destroyOutsideTheLock() {
        synchronized(order) { order.add("dispose:$name") }
        synchronized(lock) { destroyed = true }
        dispatcher?.quit()
        if (dispatcher != null) synchronized(order) { order.add("quit:dispatcher") }
    }
}

/**
 * The callback worker as an OWNED resource: closing it really does quit the
 * dispatcher, the way `HandlerThread.quitSafely()` does, so a close that
 * happens too early is observable rather than cosmetic.
 */
internal class HostWorkerHandle(
    private val dispatcher: HostDispatcher,
    private val order: MutableList<String>,
    override val identity: Any = "worker",
    /** Raised after the dispatcher is actually quit, as a real close may. */
    private val raise: (() -> Throwable)? = null,
) : ResourceHandle {
    override fun close() {
        synchronized(order) { order.add("close:$identity") }
        dispatcher.quit()
        synchronized(order) { order.add("quit:dispatcher") }
        raise?.let { throw it() }
    }
}

/** The codec as an OWNED resource: closing the handle ASKS its lifetime to dispose. */
internal class HostCodecHandle(
    private val codec: HostCodecOwner,
    override val identity: Any,
    private val order: MutableList<String>,
    /** Runs between this handle being closed and the disposal it asks for. */
    private val between: () -> Unit = {},
) : ResourceHandle {
    override fun close() {
        synchronized(order) { order.add("close:$identity") }
        between()
        codec.requestDisposal()
    }
}

/**
 * The codec destroyed the moment its owner says close, as an unprotected
 * handoff would leave it. Leases are granted and ignored, so a transfer in
 * flight keeps reading and releasing a codec that is already gone.
 */
internal class EagerDisposalCodec(
    private val order: MutableList<String>,
    private val dispatcher: HostDispatcher? = null,
    private val name: String = "codec",
) : HostCodecOwner {
    private val lock = Any()
    private var disposed = false
    private var disposals = 0

    override val isDisposed: Boolean get() = synchronized(lock) { disposed }

    override val disposalCount: Int get() = synchronized(lock) { disposals }

    override fun retainForTransfer(): Boolean = true

    override fun releaseAfterTransfer() = Unit

    override fun requestDisposal() {
        val first = synchronized(lock) { if (disposed) false else { disposed = true; disposals++; true } }
        if (!first) return
        synchronized(order) { order.add("dispose:$name") }
        dispatcher?.quit()
        if (dispatcher != null) synchronized(order) { order.add("quit:dispatcher") }
    }
}

/** A codec whose disposal its owner drives; the lifetime decides when it happens. */
internal interface HostCodecOwner : CodecLifetime {
    /** The owner is finished with the codec. Disposal may still be deferred by a lease. */
    fun requestDisposal()

    /** Whether the codec has actually been destroyed. */
    val isDisposed: Boolean

    /** How many times it was destroyed; one is the only correct answer after a close. */
    val disposalCount: Int
}

/**
 * A one-shot rendezvous with a FAIL-ONLY deadline. Expiry is an assertion
 * failure, never evidence that the other side got there.
 */
internal class HostGate(private val what: String, count: Int = 1) {
    private val latch = java.util.concurrent.CountDownLatch(count)

    fun open() = latch.countDown()

    /** Opens the gate completely, however many waiters it was built for. */
    fun openFully() {
        while (latch.count > 0L) latch.countDown()
    }

    val isOpen: Boolean get() = latch.count == 0L

    /**
     * Waits for the gate. Expiry is a FAILURE. When [sources] are named and one
     * of them has already died, the wait ends immediately with that cause
     * instead of burning the whole deadline on something that will never open.
     */
    fun await(vararg sources: HostWorker, millis: Long = DEADLINE_MS) {
        val deadline = System.nanoTime() + millis * 1_000_000L
        while (true) {
            if (latch.await(SLICE_MS, java.util.concurrent.TimeUnit.MILLISECONDS)) return
            sources.firstNotNullOfOrNull { it.failureNow() }?.let {
                throw AssertionError("$what never happened: a worker failed first with $it")
            }
            sources.firstOrNull { it.isSettled }?.let {
                throw AssertionError("$what never happened: ${it.name} finished without reaching it")
            }
            if (System.nanoTime() >= deadline) {
                throw AssertionError("$what did not happen within ${millis}ms: the schedule is not satisfiable")
            }
        }
    }

    private companion object {
        const val DEADLINE_MS = 10_000L
        const val SLICE_MS = 20L
    }
}

/**
 * A MECHANISM check, not an observable one.
 *
 * It reads the ownership's private reservation counter by reflection, because
 * once the worker has already retired an unbalanced count changes nothing a
 * caller can see -- no leak, no resurrection, no missing close -- so
 * dispatcher retirement cannot prove it. Nothing in production exposes this;
 * only the balance rows look.
 */
internal fun assertReservationsBalanced(ownership: CaptureOwnership, where: String) {
    val field = ownership.javaClass.getDeclaredField("workerRetentions")
    field.isAccessible = true
    kotlin.test.assertEquals(0, field.getInt(ownership), "the reservation bookkeeping must balance $where")
}

/** Releases every gate and settles every worker; the first failure wins, the rest are suppressed. */
internal fun settleAll(gates: List<HostGate>, workers: List<HostWorker>) = withSettlement(gates, workers) {}

/**
 * Runs [body], then ALWAYS opens every gate it owns and attempts to settle
 * EVERY worker before propagating. The body's failure is the primary one; a
 * cleanup failure is preserved -- suppressed under it, or raised when the body
 * succeeded.
 */
internal fun withSettlement(gates: List<HostGate>, workers: List<HostWorker>, body: () -> Unit) {
    var primary: Throwable? = null
    var finished = false
    var released = false

    fun release() {
        if (released) return
        released = true
        for (gate in gates) gate.openFully()
    }

    fun record(raised: Throwable) {
        val current = primary
        when {
            current == null -> primary = raised
            current === raised -> Unit
            else -> current.addSuppressed(raised)
        }
    }

    /**
     * One settlement attempt. It returns what it raised rather than raising it,
     * so one worker cannot skip the rest -- an `Error` from a join included.
     * Nothing is CONVERTED: the same instance is rethrown at the end.
     */
    fun settle(worker: HostWorker): Throwable? =
        try {
            worker.join()
            null
        } catch (failure: RuntimeException) {
            failure
        } catch (interrupted: InterruptedException) {
            interrupted
        } catch (error: Error) {
            error
        }
    try {
        try {
            body()
        } catch (failure: RuntimeException) {
            primary = failure
        } catch (interrupted: InterruptedException) {
            primary = interrupted
        } catch (error: Error) {
            // CAPTURED, not converted: the same instance is rethrown below,
            // after the cleanup, so an Error can carry what the cleanup found
            // instead of losing it. Nothing recovers from it and nothing else
            // replaces it as the primary.
            primary = error
        }
        finished = true
    } finally {
        // ALWAYS. An `Error` -- a declaration's NotImplementedError, say -- is
        // captured above and rethrown below unchanged, and anything else that
        // escapes still leaves through here: either way the gates and workers
        // this row owns are settled on the way out, and what the cleanup found
        // is attached to the failure that is leaving.
        //
        // An interrupted controlling wait leaves this thread's interrupt flag
        // set, which would make every cleanup join throw at once. The flag is
        // cleared here so the cleanup can actually run; the interruption itself
        // is the primary failure and is still propagated below.
        Thread.interrupted()

        // On the way out of a FAILED body the barriers go first: whoever is
        // parked on one is not going to finish otherwise. After a successful
        // body the row's own protocol releases them, and the joins are the
        // evidence -- so they are opened only if a join shows someone is stuck.
        if (primary != null || !finished) release()
        for (worker in workers) {
            // RECORDED FIRST. A worker that had to be released by the cleanup
            // did not settle on the row's own schedule, and a retry that then
            // succeeds does not unmake that: settling a worker is cleanup, not
            // evidence, and it can never turn a timeout, an interruption or a
            // worker failure into a pass.
            val failure = settle(worker)
            if (failure != null) {
                record(failure)
                if (!released) {
                    release()
                    settle(worker)?.let { record(it) }
                }
            }
        }
    }
    primary?.let { throw it }
}

/** Ownership under the stated rule: decide under the monitor, close and notify outside it. */
internal class ConformingOwnership(
    override val generation: Long,
    override val dispatcher: CallbackDispatcher,
    private val late: LateCleanupSink,
    private val schedule: AdoptionSchedule = AdoptionSchedule.NONE,
) : CaptureOwnership {
    private val lock = Any()
    private val installed = mutableListOf<Pair<CaptureResource, ResourceHandle>>()
    private val tickets = mutableSetOf<AcquisitionTicket>()
    private var closed = false
    private var retired = false
    private var retainedWorker: ResourceHandle? = null
    private var workerRetentions = 0
    private var workerGone = false

    /** Close has been DECIDED. The resources that run on the worker may still be closing. */
    private var closing = false

    /** Close has finished with every resource that depends on the worker. */
    private var closeBoundaryReached = false

    override val isClosed: Boolean get() = synchronized(lock) { closed }

    override val pending: Int get() = synchronized(lock) { tickets.size }

    override fun acquire(resource: CaptureResource, open: () -> ResourceHandle): AcquireOutcome {
        // Every resource but the worker opens ON the worker: it is protected
        // from before the open until the handle is installed or closed, and a
        // resource whose worker is already gone is not opened.
        val protected = resource != CaptureResource.Worker
        if (protected && !retainWorker()) {
            return AcquireOutcome.Retired(resource, "the callback worker has already been released; nothing was opened")
        }
        val cleanup = CleanupTransaction()
        var outcome: AcquireOutcome? = null
        try {
            val handle =
                try {
                    open()
                } catch (unproved: UnprovedDisposal) {
                    // Not proved disposed: the primary leaves as itself.
                    throw unproved.unwrap()
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (failure: Exception) {
                    null.also { outcome = AcquireOutcome.Failed(resource, failure.message ?: failure::class.simpleName.orEmpty()) }
                }
            if (handle != null) {
                val install = synchronized(lock) { if (closed || retired) false else installed.add(resource to handle) }
                if (install) {
                    outcome = AcquireOutcome.Acquired
                } else {
                    cleanup.closeAndReport(resource, handle)
                    outcome = AcquireOutcome.Retired(resource, "retired before this acquisition returned")
                }
            }
        } finally {
            if (protected) releaseCleanupProtection()?.let { cleanup.closeAndReport(CaptureResource.Worker, it) }
        }
        cleanup.settle()
        return checkNotNull(outcome)
    }

    override fun request(resource: CaptureResource): AcquisitionTicket =
        AcquisitionTicket(generation, resource).also { synchronized(lock) { tickets.add(it) } }

    override fun retainWorker(): Boolean = synchronized(lock) { if (workerGone) false else { workerRetentions++; true } }

    override fun releaseWorker() {
        val worker = synchronized(lock) { workerRetentions--; workerToReleaseLocked() }
        worker?.let {
            val cleanup = CleanupTransaction()
            cleanup.closeAndReport(CaptureResource.Worker, it)
            cleanup.settle()
        }
    }

    /** Dependents still holding the worker. For a row's assertions, not for the contract. */
    val workerDependents: Int get() = synchronized(lock) { workerRetentions }

    override fun adopt(ticket: AcquisitionTicket, handle: ResourceHandle): AdoptOutcome {
        schedule.beforeSettle()
        var action: (() -> Unit)? = null
        val outcome =
            synchronized(lock) {
                if (ticket !in tickets) {
                    val live = installed.any { it.second.identity === handle.identity } ||
                        retainedWorker?.identity === handle.identity
                    if (!live) {
                        // Disposing an unowned handle is still cleanup work,
                        // and it must not run against a worker that retires
                        // underneath it.
                        val protection = reserveCleanupProtectionLocked()
                        action = {
                            val cleanup = CleanupTransaction()
                            try {
                                cleanup.closeAndDisown(ticket.resource, handle)
                            } finally {
                                if (protection) {
                                    releaseCleanupProtection()?.let { cleanup.closeAndReport(CaptureResource.Worker, it) }
                                }
                            }
                            cleanup.settle()
                        }
                    }
                    AdoptOutcome.Unknown("no outstanding ticket for ${ticket.resource} in generation $generation")
                } else {
                    tickets.remove(ticket)
                    if (closed || retired) {
                        // The ticket stops protecting the worker HERE, and the
                        // cleanup that is about to run starts protecting it in
                        // the same breath: the handover is one step under this
                        // monitor, so nothing in between can retire the worker
                        // the carried close still runs against.
                        workerRetentions++
                        action = {
                            // The carried device's close cannot skip a worker
                            // release this settlement already owes, and an
                            // ordinary failure from the first close cannot hide
                            // a terminal one from the second.
                            val cleanup = CleanupTransaction()
                            try {
                                cleanup.closeAndReport(ticket.resource, handle)
                            } finally {
                                releaseCleanupProtection()?.let { cleanup.closeAndReport(CaptureResource.Worker, it) }
                            }
                            // The decision is made HERE, not in the finally: a
                            // retirement failure must not overwrite what the
                            // carried path already raised.
                            cleanup.settle()
                        }
                        AdoptOutcome.Retired("this generation ended while ${ticket.resource} was opening")
                    } else {
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
                        val protection = reserveCleanupProtectionLocked()
                        action = {
                            val cleanup = CleanupTransaction()
                            try {
                                cleanup.closeAndDisown(ticket.resource, carried)
                            } finally {
                                if (protection) {
                                    releaseCleanupProtection()?.let { cleanup.closeAndReport(CaptureResource.Worker, it) }
                                }
                            }
                            cleanup.settle()
                        }
                    }
                    AdoptOutcome.Unknown("no outstanding ticket for ${ticket.resource}")
                } else {
                    tickets.remove(ticket)
                    // Same handover as the adopted-late path: the cleanup takes
                    // over the worker protection the ticket is giving up, under
                    // this monitor, before either close runs outside it.
                    workerRetentions++
                    action = {
                        val cleanup = CleanupTransaction()
                        try {
                            carried?.let { handle -> cleanup.closeAndReport(ticket.resource, handle) }
                        } finally {
                            releaseCleanupProtection()?.let { cleanup.closeAndReport(CaptureResource.Worker, it) }
                        }
                        cleanup.settle()
                    }
                    AdoptOutcome.Failed(reason)
                }
            }
        action?.invoke()
        return outcome
    }

    override fun lost(resource: CaptureResource, reason: String): LostOutcome {
        val handle =
            synchronized(lock) {
                val at = installed.indexOfFirst { it.first == resource }
                if (at < 0) {
                    null
                } else {
                    // The entry stops protecting the worker the moment it
                    // leaves `installed`, and the close that follows runs
                    // OUTSIDE this monitor. The protection is taken here, in
                    // the same step, so an overlapping ownership close cannot
                    // retire the worker while the lost resource is still
                    // closing.
                    workerRetentions++
                    installed.removeAt(at).second
                }
            } ?: return LostOutcome.Ignored("$resource is not installed")
        val cleanup = CleanupTransaction()
        try {
            // A clean loss is reported by its outcome, not by the late sink.
            cleanup.closeAndReport(resource, handle, reportClean = false)
        } finally {
            releaseCleanupProtection()?.let { cleanup.closeAndReport(CaptureResource.Worker, it) }
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
                closing = true
                val order = installed.reversed().toMutableList()
                installed.clear()
                // The worker outlives this close while ANY dependent still
                // needs it: an acquisition the platform has not settled, or an
                // admitted transfer still inside the codec.
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
        val cleanup = CleanupTransaction()
        // EVERY handle is attempted, in order, exactly once: one that throws --
        // a cancellation included -- cannot abandon the ones behind it.
        for ((_, handle) in toClose) cleanup.add(closeCollecting(handle))
        // Only now has this close finished with the resources that RUN ON the
        // worker -- a codec whose handle merely asked for its disposal
        // included. A dependent that settled while those closes were still
        // running left the worker to this boundary rather than retiring it.
        val lateWorker = synchronized(lock) { closeBoundaryReached = true; workerToReleaseLocked() }
        lateWorker?.let { cleanup.closeAndReport(CaptureResource.Worker, it) }

        // Only now is anything propagated, and the precedence is explicit.
        // A cancellation or an Error raised by a close leaves by IDENTITY --
        // EXCEPT when the caller's own failure is itself a cancellation, which
        // is the ORIGINAL one and is never replaced by a later cleanup
        // cancellation. A failure raised by a REPORT has no sink left to carry
        // it, so it escapes too. An ordinary result keeps the return contract.
        val collected = cleanup.collected()
        val terminal = terminalOf(collected) ?: cleanup.firstReportFailure()
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

    /**
     * Reserves cleanup protection when there is still a live owned worker to
     * protect (lock held). Returns false when the worker is already gone: the
     * cleanup then runs on the caller's context under the existing contract,
     * and nothing is resurrected.
     */
    private fun reserveCleanupProtectionLocked(): Boolean =
        if (workerGone) {
            false
        } else {
            workerRetentions++
            true
        }

    /**
     * Gives back the protection a late cleanup took from its ticket, once that
     * cleanup's own close attempt has ended -- on every path, exceptions
     * included. Returns the worker to close when this was the last dependent.
     */
    private fun releaseCleanupProtection(): ResourceHandle? =
        synchronized(lock) {
            workerRetentions--
            workerToReleaseLocked()
        }

    /**
     * The retained worker, once the last dependent has settled AND this close
     * has finished with what runs on the worker (lock held).
     */
    private fun workerToReleaseLocked(): ResourceHandle? =
        if (closed && closeBoundaryReached && tickets.isEmpty() && workerRetentions == 0) {
            retainedWorker.also { if (it != null) { retainedWorker = null; workerGone = true } }
        } else {
            null
        }

    /**
     * One close attempt that COLLECTS whatever it raised instead of letting it
     * skip the rest of the cleanup.
     *
     * A close that throws is allowed -- [ResourceHandle.close] says so, and it
     * says nothing about which exceptions -- and it must not be able to abandon
     * the handles behind it, the close boundary, or a worker release that is
     * already owed. So NOTHING escapes this attempt: every Exception, checked
     * or not, a cancellation, and every Error are returned, collected into the
     * caller's [CleanupTransaction], and decided there by `settle()` once that
     * transaction is complete. Nothing is swallowed and nothing is translated.
     */
    private fun closeCollecting(handle: ResourceHandle): Throwable? = attempt { handle.close() }

    /**
     * One attempt -- a close, or a notification -- whose failure is COLLECTED
     * rather than allowed to end the cleanup around it. Reporting is an attempt
     * like any other: a sink that raises must not stop the transaction from
     * reaching its decision, or the failure it was being told about is lost.
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
            // The sample's rule -- rethrow a cancellation immediately -- is a
            // rule about ORDINARY bodies. This is a cleanup collector: the
            // rethrow above is caught right here and collected like everything
            // else, so the cancellation is neither swallowed nor allowed to
            // abandon the cleanup. `propagate` gives it back, by identity,
            // once that cleanup has finished.
            cancelled
        }

    /** The part of a collected failure the late-cleanup sink can carry. */
    private fun reportable(raised: Throwable?): Exception? = raised as? Exception

    /**
     * ONE cleanup transaction: every close attempt and every report attempt in
     * it is collected, in occurrence order, and the decision is made once, at
     * the end, by [settle].
     *
     * A close failure is normally CARRIED by the report it is given to, so an
     * ordinary one is not raised again. A REPORT failure has nowhere else to
     * go -- the sink is what would have carried it -- so it escapes. A
     * cancellation or an Error from either kind outranks both.
     */
    private inner class CleanupTransaction {
        private val collected = mutableListOf<Throwable>()
        private val fromReports = mutableListOf<Throwable>()

        /** Closes one handle and tells the sink, whatever either of them raises. */
        fun closeAndReport(resource: CaptureResource, handle: ResourceHandle, reportClean: Boolean = true) {
            val closeFailure = closeCollecting(handle)
            closeFailure?.let { collected.add(it) }
            if (closeFailure == null && !reportClean) return
            attempt { late.onLateClose(generation, resource, reportable(closeFailure)) }?.let {
                collected.add(it)
                fromReports.add(it)
            }
        }

        /** The same, for a resource this ownership never owned. */
        fun closeAndDisown(resource: CaptureResource, handle: ResourceHandle) {
            val closeFailure = closeCollecting(handle)
            closeFailure?.let { collected.add(it) }
            attempt { late.onForeignDisposal(generation, resource, reportable(closeFailure)) }?.let {
                collected.add(it)
                fromReports.add(it)
            }
        }

        /** A failure raised outside this transaction's own attempts. */
        fun add(raised: Throwable?) {
            raised?.let { collected.add(it) }
        }

        fun collected(): List<Throwable> = collected.toList()

        fun firstReportFailure(): Throwable? = fromReports.firstOrNull()

        /** The decision: first terminal by occurrence, else the first report failure. */
        fun settle() {
            val carrier = terminalOf(collected) ?: fromReports.firstOrNull() ?: return
            attachOthers(carrier, collected)
            throw carrier
        }
    }

    /** A cancellation or an Error must leave; an ordinary failure is carried by its report. */
    private fun terminalOf(collected: List<Throwable>): Throwable? =
        collected.firstOrNull { it is CancellationException || it is Error }

    /** Attaches every other collected failure to [carrier], once each, never itself. */
    private fun attachOthers(carrier: Throwable, collected: List<Throwable>) {
        val attached = mutableListOf<Throwable>()
        for (other in collected) {
            if (other === carrier) continue
            if (attached.any { it === other }) continue
            attached.add(other)
            carrier.addSuppressed(other)
        }
    }

}

/** Every release, in order; a scripted index may throw. */
internal class HostReleases(
    private val throwOn: Set<Int> = emptySet(),
    private val order: MutableList<String>? = null,
) : CodecBufferOwner {
    private val log = mutableListOf<Int>()

    @Synchronized
    override fun release(index: Int) {
        log.add(index)
        order?.let { synchronized(it) { it.add("release:$index") } }
        if (index in throwOn) throw IllegalStateException("release of $index failed")
    }

    @Synchronized
    fun indices(): List<Int> = log.toList()
}

/** Codec-owned buffers on the host: by index, possibly read-only/direct, absent, or throwing. */
internal class HostBuffers : OutputBufferAccess {
    private val buffers = mutableMapOf<Int, ByteBuffer>()
    private val throwing = mutableSetOf<Int>()
    private val fetched = mutableListOf<Int>()

    @Synchronized
    fun put(index: Int, buffer: ByteBuffer) = apply { buffers[index] = buffer }

    @Synchronized
    fun throwOn(index: Int) = apply { throwing.add(index) }

    @Synchronized
    override fun outputBuffer(index: Int): ByteBuffer? {
        fetched.add(index)
        if (index in throwing) throw IllegalStateException("codec is not executing")
        return buffers[index]
    }

    @Synchronized
    fun fetched(): List<Int> = fetched.toList()
}

/** Faults and witnesses, in order. */
internal class HostFaults : CaptureFaultSink {
    private val events = mutableListOf<Any>()

    @Synchronized
    override fun onFault(generation: Long, fault: CaptureFault) {
        events.add(fault)
    }

    @Synchronized
    override fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) {
        events.add(Discarded(kind, outputs))
    }

    data class Discarded(val kind: MediaKind, val outputs: Int)

    @Synchronized
    fun all(): List<Any> = events.toList()

    fun faults(): List<CaptureFault> = all().filterIsInstance<CaptureFault>()

    fun discards(): List<Discarded> = all().filterIsInstance<Discarded>()
}

/**
 * The audio keying under the stated framing: units accumulate, keys are one
 * floored division of the exact frame count, and anything that cannot be
 * computed is refused by name rather than wrapped.
 */
internal class HostAudioKeying(
    override val sampleRate: Int,
    override val framesPerUnit: Int,
    override val originUs: Long,
) : AudioKeying {
    private val lock = Any()
    private var retained = 0
    private var nextUnit = 0L

    override val retainedFrames: Int get() = synchronized(lock) { retained }

    override fun keyFor(unitIndex: Long): AudioKeyOutcome {
        if (sampleRate <= 0) return AudioKeyOutcome.Refused("a sample rate of $sampleRate places no unit")
        if (framesPerUnit <= 0) return AudioKeyOutcome.Refused("a unit of $framesPerUnit frames places no unit")
        if (unitIndex < 0L) return AudioKeyOutcome.Refused("unit $unitIndex is before the origin")
        // ONE floor over the whole offset, evaluated exactly: only the FINAL
        // timestamp's representability decides. An intermediate that overflows
        // a Long -- the frame count, or the duration before a negative origin
        // is added -- says nothing about the key.
        val offset =
            java.math.BigInteger.valueOf(unitIndex)
                .multiply(java.math.BigInteger.valueOf(framesPerUnit.toLong()))
                .multiply(java.math.BigInteger.valueOf(1_000_000L))
                .divide(java.math.BigInteger.valueOf(sampleRate.toLong()))
        val key = java.math.BigInteger.valueOf(originUs).add(offset)
        if (key < java.math.BigInteger.valueOf(Long.MIN_VALUE) || key > java.math.BigInteger.valueOf(Long.MAX_VALUE)) {
            return overflow(unitIndex)
        }
        return AudioKeyOutcome.Key(key.toLong())
    }

    override fun onRead(frames: Int): List<Long> {
        require(frames >= 0) { "a read cannot return $frames frames" }
        return synchronized(lock) {
            if (framesPerUnit <= 0) return@synchronized emptyList()
            // WIDENED before anything is mutated: an Int accumulation wraps at
            // a unit size near Int.MAX_VALUE and loses the whole read.
            val total = retained.toLong() + frames.toLong()
            val completed = total / framesPerUnit
            val last = sum(nextUnit, completed)
            require(last != null) { "unit index $nextUnit + $completed is not representable" }
            val indices = (0 until completed).map { nextUnit + it }
            nextUnit += completed
            retained = (total % framesPerUnit).toInt()
            indices
        }
    }

    private fun overflow(unitIndex: Long): AudioKeyOutcome =
        AudioKeyOutcome.Refused("unit $unitIndex has no key in Long microseconds: computing it would overflow")

    private fun sum(a: Long, b: Long): Long? =
        try {
            Math.addExact(a, b)
        } catch (raised: ArithmeticException) {
            null
        }
}

/** A correct ledger, so handoff rows fail at the handoff. */
internal class HostLedger(
    override val kind: MediaKind,
    override val generation: Long,
    override val capacity: Int = 8,
) : CaptureLedger {
    private val byKey = sortedMapOf<Long, Long>()
    private var retired = false

    override val outstanding: Int get() = synchronized(this) { byKey.size }

    @Synchronized
    override fun observe(generation: Long, kind: MediaKind, observation: CaptureObservation): ObserveOutcome {
        if (retired || generation != this.generation) return ObserveOutcome.Ignored("not this generation")
        if (kind != this.kind) return ObserveOutcome.Refused("wrong track")
        if (byKey.containsKey(observation.key)) return ObserveOutcome.Refused("duplicate key")
        if (byKey.size >= capacity) return ObserveOutcome.Refused("ledger is full")
        byKey[observation.key] = observation.captureElapsedNanos
        return ObserveOutcome.Recorded
    }

    @Synchronized
    override fun claim(generation: Long, kind: MediaKind, key: Long): ClaimOutcome {
        if (retired || generation != this.generation) return ClaimOutcome.Ignored("not this generation")
        if (kind != this.kind) return ClaimOutcome.Missing("wrong track")
        val nanos = byKey.remove(key) ?: return ClaimOutcome.Missing("no observation for $key")
        byKey.headMap(key).keys.toList().forEach { byKey.remove(it) }
        return ClaimOutcome.Matched(nanos)
    }

    @Synchronized
    override fun retire(reason: String) {
        retired = true
    }
}

/**
 * The handoff under the three-phase rule: admit under the monitor, transfer
 * outside it, commit under it; close never waits; notify outside.
 */
internal class ConformingHandoff(
    override val kind: MediaKind,
    override val generation: Long,
    private val buffers: OutputBufferAccess,
    private val owner: CodecBufferOwner,
    private val ledger: CaptureLedger,
    private val faults: CaptureFaultSink,
    private val capacity: Int,
    private val lifetime: CodecLifetime = CodecLifetime.RETAINED,
    private val schedule: HandoffSchedule = HandoffSchedule.NONE,
) : CaptureHandoff {
    private val lock = Any()
    private val queue = ArrayDeque<CapturedOutput>()
    private var reserved = 0
    private var closed = false
    private var closeMode: CloseMode? = null
    private var terminal: CaptureFault? = null
    private var lastMediaPts: Long? = null
    private var discarded = 0
    private var witnessed = false
    private var sink: OutputSignal? = null
    private var endedFlag = false

    override val ended: Boolean get() = synchronized(lock) { endedFlag }

    override fun signal(sink: OutputSignal) {
        synchronized(lock) { this.sink = sink }
    }

    override fun onOutput(generation: Long, output: CodecOutput): HandoffOutcome {
        schedule.beforeAdmission()
        val media = output.flags and CaptureContract.FLAG_CODEC_CONFIG == 0 && output.size > 0
        var refusal: CaptureFault? = null
        synchronized(lock) {
            if (generation != this.generation) return HandoffOutcome.Ignored("not generation ${this.generation}")
            if (closed || terminal != null) return HandoffOutcome.Ignored("this track has ended")
            val last = lastMediaPts
            if (media && last != null && output.presentationTimeUs <= last) {
                refusal = CaptureFault.Reordered(kind, "pts ${output.presentationTimeUs} does not advance past $last")
            } else if (queue.size + reserved >= capacity) {
                refusal = CaptureFault.Overflow(kind, "the $kind queue holds $capacity outputs")
            } else {
                reserved++
                if (media) lastMediaPts = output.presentationTimeUs
            }
        }
        // A refusal decided at admission still hands an index back, and that IS
        // a codec access: it takes a lease like any other.
        refusal?.let { return refuseAtAdmission(output.index, it) }

        // ADMITTED. From here the reservation is this callback's to settle, and
        // it is settled EXACTLY ONCE -- by whichever of the refusal, the commit
        // or the unwind gets there first. The guard spans the whole callback
        // through commit, not just the codec access, because a cleanup that
        // throws after the index went back would otherwise consume capacity
        // for good.
        val slot = AtomicBoolean(false)
        var primary: Throwable? = null
        try {
            // TRANSFER, outside the monitor, under a codec lease.
            val copied =
                when (val result = transfer(output, media, slot)) {
                    is TransferResult.Rejected -> return result.outcome
                    is TransferResult.Copied -> result
                }
            schedule.beforeCommit()

            // COMMIT, under the monitor.
            val captured = CapturedOutput(EncodedOutput(copied.bytes, 0, output.size, output.presentationTimeUs, output.flags), copied.captureNanos)
            var witness = 0
            var signalNow = false
            synchronized(lock) {
                if (slot.compareAndSet(false, true)) {
                    reserved--
                    if (closed) {
                        discarded++
                        witness = witnessIfSettledLocked()
                    } else {
                        queue.addLast(captured)
                        signalNow = queue.size == 1
                    }
                }
            }
            if (witness > 0) faults.onDiscarded(generation, kind, witness)
            if (signalNow) currentSink()?.onOutputs(kind)
            return HandoffOutcome.Queued
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
            carry(primary, attempt { settleAbandonedSlot(slot) })
        }
    }

    /** What the transfer produced: the copy and its claimed instant, or the refusal that ended it. */
    private sealed interface TransferResult {
        class Copied(val bytes: ByteArray, val captureNanos: Long) : TransferResult

        class Rejected(val outcome: HandoffOutcome) : TransferResult
    }

    /**
     * Everything that touches the codec, between taking the lease and giving it
     * back.
     *
     * An ADMITTED index is this handoff's responsibility until it goes back, so
     * every way out of here settles it: a refusal releases it and names the
     * fault, and a cancellation or any other throw releases it ONCE -- while
     * the lease still holds the codec live -- settles the reservation once, and
     * is rethrown as the primary with the cleanup failure attached. A release
     * that has already been attempted is never attempted again, because it may
     * already have taken effect. The lease is given back last, in a `finally`,
     * so nothing here can run against a destroyed codec.
     */
    private fun transfer(output: CodecOutput, media: Boolean, slot: AtomicBoolean): TransferResult {
        if (!lifetime.retainForTransfer()) {
            return TransferResult.Rejected(
                refuse(null, CaptureFault.Codec(kind, "the codec was disposed before this transfer began"), slot),
            )
        }
        var released = false
        var completed = false
        var raised: Throwable? = null

        fun reject(fault: CaptureFault): TransferResult {
            // The index is attempted by refuse; it is marked attempted FIRST so
            // a throw in there cannot make it happen twice, and `completed` is
            // set only once that cleanup has actually finished.
            released = true
            val outcome = refuse(output.index, fault, slot)
            completed = true
            return TransferResult.Rejected(outcome)
        }
        try {
            val buffer =
                try {
                    buffers.outputBuffer(output.index)
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (failure: Exception) {
                    return reject(CaptureFault.BufferAccess(kind, "getOutputBuffer threw: ${failure.message}"))
                }
                    ?: return reject(CaptureFault.BufferAccess(kind, "getOutputBuffer returned null"))
            if (output.offset < 0 || output.size < 0 || output.offset.toLong() + output.size > buffer.limit()) {
                return reject(CaptureFault.Malformed(kind, "range past the buffer limit"))
            }
            val bytes = ByteArray(output.size)
            val view = buffer.duplicate()
            view.position(output.offset)
            view.limit(output.offset + output.size)
            view.get(bytes)
            val captureNanos =
                if (!media) {
                    0L
                } else {
                    when (val claim = ledger.claim(generation, kind, output.presentationTimeUs)) {
                        is ClaimOutcome.Matched -> claim.captureElapsedNanos
                        is ClaimOutcome.Missing -> return reject(CaptureFault.Correlation(kind, claim.reason))
                        is ClaimOutcome.Ignored -> return reject(CaptureFault.Correlation(kind, claim.reason))
                    }
                }
            schedule.beforeRelease()
            released = true
            val releaseFailure = releaseOnce(output.index)
            if (releaseFailure != null) {
                val outcome = refuse(null, CaptureFault.BufferAccess(kind, "release threw: ${releaseFailure.message}"), slot)
                completed = true
                return TransferResult.Rejected(outcome)
            }
            completed = true
            return TransferResult.Copied(bytes, captureNanos)
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
            // EVERY obligation is attempted, whatever an earlier one raised.
            // The RESERVATION is settled by the callback's own guard, not here,
            // so every later step is covered by the same one-shot.
            val cleanup = mutableListOf<Throwable>()
            if (!completed && !released) {
                released = true
                attempt { owner.release(output.index) }?.let { cleanup.add(it) }
            }
            attempt { lifetime.releaseAfterTransfer() }?.let { cleanup.add(it) }
            carry(raised, cleanup)
        }
    }

    /** The admitted callback's reservation, settled exactly once, whoever gets here first. */
    private fun settleAbandonedSlot(slot: AtomicBoolean) {
        if (!slot.compareAndSet(false, true)) return
        var witness = 0
        synchronized(lock) {
            reserved--
            if (closed) discarded++
            witness = witnessIfSettledLocked()
        }
        if (witness > 0) faults.onDiscarded(generation, kind, witness)
    }

    /**
     * A refusal decided under the monitor, before any lease: the index still
     * goes back, so the codec is held for exactly that call. If the codec is
     * already gone its indices went with it, and nothing is released.
     */
    private fun refuseAtAdmission(index: Int, fault: CaptureFault): HandoffOutcome {
        if (!lifetime.retainForTransfer()) return refuse(null, fault, slot = null)
        var primary: Throwable? = null
        try {
            return refuse(index, fault, slot = null)
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
            carry(primary, attempt { lifetime.releaseAfterTransfer() })
        }
    }

    /** A refusal after the index was fetched or admitted: release once, fault once, settle the slot. */
    private fun refuse(indexToRelease: Int?, fault: CaptureFault, slot: AtomicBoolean?): HandoffOutcome {
        var report = false
        var witness = 0
        var endNow = false
        // Every obligation is attempted, whatever an earlier one raised: the
        // index goes back once, the refusal is recorded, the owner is told,
        // the witness and the wake are delivered. Then the FIRST failure
        // leaves by identity, carrying the later distinct ones in the order
        // they happened. The refusal was decided, so it stays terminal.
        val raised = mutableListOf<Throwable>()
        indexToRelease?.let { index -> attempt { owner.release(index) }?.let { raised.add(it) } }
        synchronized(lock) {
            if (slot != null && slot.compareAndSet(false, true)) reserved--
            if (terminal == null && !closed) {
                terminal = fault
                report = true
                endedFlag = true
                endNow = true
            }
            witness = witnessIfSettledLocked()
        }
        if (report) attempt { faults.onFault(generation, fault) }?.let { raised.add(it) }
        if (witness > 0) attempt { faults.onDiscarded(generation, kind, witness) }?.let { raised.add(it) }
        if (endNow) attempt { currentSink()?.onOutputs(kind) }?.let { raised.add(it) }
        carry(null, raised)
        return HandoffOutcome.Refused(fault.toString())
    }

    private fun releaseOnce(index: Int): Exception? =
        try {
            owner.release(index)
            null
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (failure: Exception) {
            failure
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

    private fun carry(primary: Throwable?, raised: Throwable?) = carry(primary, listOfNotNull(raised))

    /**
     * The primary keeps its identity and carries what the cleanup found. With
     * no primary the first cleanup failure escapes rather than disappearing.
     */
    private fun carry(primary: Throwable?, cleanup: List<Throwable>) {
        val carrier = primary ?: cleanup.firstOrNull() ?: return
        // Distinct by IDENTITY, against what the carrier already holds too:
        // one instance raised twice is one root, and never a root of itself.
        val attached = carrier.suppressed.toMutableList()
        for (other in cleanup) {
            if (other === carrier || attached.any { it === other }) continue
            attached.add(other)
            carrier.addSuppressed(other)
        }
        if (primary == null) throw carrier
    }

    private fun witnessIfSettledLocked(): Int {
        if (closed && closeMode == CloseMode.Failed && reserved == 0 && discarded > 0 && !witnessed) {
            witnessed = true
            return discarded
        }
        return 0
    }

    private fun currentSink(): OutputSignal? = synchronized(lock) { sink }

    override fun onCodecError(generation: Long, reason: String) {
        schedule.beforeTerminal()
        val fault = CaptureFault.Codec(kind, reason)
        var report = false
        synchronized(lock) {
            if (generation == this.generation && terminal == null && !closed) {
                terminal = fault
                endedFlag = true
                report = true
            }
        }
        if (report) {
            faults.onFault(generation, fault)
            currentSink()?.onOutputs(kind)
        }
    }

    override fun next(): CapturedOutput? =
        synchronized(lock) {
            val out = queue.removeFirstOrNull()
            if (out != null && out.output.isEndOfStream) endedFlag = true
            out
        }

    override fun close(mode: CloseMode) {
        var witness = 0
        synchronized(lock) {
            if (closed) return
            closed = true
            closeMode = mode
            endedFlag = true
            if (mode == CloseMode.Failed) {
                discarded += queue.size
                queue.clear()
            }
            witness = witnessIfSettledLocked()
        }
        if (witness > 0) faults.onDiscarded(generation, kind, witness)
        currentSink()?.onOutputs(kind)
    }
}

/** Runs [body] on its own thread with a fail-only watchdog; re-throws what it raised. */
internal class HostWorker(name: String, private val watchdogMs: Long = WATCHDOG_MS, body: () -> Unit) {
    private val failure = AtomicReference<Throwable?>(null)
    private val thread =
        Thread({ body() }, name).apply {
            isDaemon = true
            setUncaughtExceptionHandler { _, raised -> failure.set(raised) }
        }

    val name: String get() = thread.name

    /** Whether the thread has actually ended. Never a wait, never an inference. */
    val isSettled: Boolean get() = !thread.isAlive

    fun start() = apply { thread.start() }

    /** What this worker has already died of, if anything. Never a wait. */
    fun failureNow(): Throwable? = failure.get()

    /**
     * Joins with a WATCHDOG. Expiry is a FAILURE -- it never counts as ordering
     * evidence -- and exists only so a broken schedule cannot hang the suite.
     *
     * An interrupt is a REQUEST, not a termination: after asking, this waits
     * again and says which of the two actually happened, so a thread still
     * running at the end of the row is reported as still running rather than
     * quietly counted as settled.
     */
    fun join() {
        thread.join(watchdogMs)
        if (thread.isAlive) {
            thread.interrupt()
            thread.join(SETTLE_MS)
            val ended = !thread.isAlive
            throw AssertionError(
                "$name did not finish within the watchdog and was interrupted; it " +
                    (if (ended) "ended after the interrupt" else "is STILL RUNNING") +
                    ": the schedule is not satisfiable",
            )
        }
        failure.get()?.let { throw it }
    }

    private companion object {
        const val WATCHDOG_MS = 10_000L
        const val SETTLE_MS = 1_000L
    }
}

/** Readiness and termination, in order. */
internal class HostSessionListener : CaptureSessionListener {
    private val events = mutableListOf<Any>()

    @Synchronized
    override fun onReady(generation: Long, ready: CaptureReady) {
        events.add(ready)
    }

    @Synchronized
    override fun onTerminated(generation: Long, termination: CaptureTermination) {
        events.add(termination)
    }

    @Synchronized
    fun all(): List<Any> = events.toList()
}

/**
 * The session under the stated rule: termination is decided under the
 * session's own lock, and the children and ownership are closed AFTER it is
 * released -- so a track that reports a fault from outside its own monitor can
 * never form a cycle with the session closing the other track.
 */
internal class ConformingSession(
    override val generation: Long,
    private val ownership: CaptureOwnership,
    private val acquirer: CaptureAcquirer,
    private val videoTrack: CaptureHandoff,
    private val audioTrack: CaptureHandoff,
    private val listener: CaptureSessionListener,
    private val schedule: StartupSchedule = StartupSchedule.NONE,
) : CaptureSession {
    private val lock = Any()
    private var startClaimed = false
    private var started = false
    private var decided: CaptureTermination? = null
    private val endedTracks = mutableSetOf<MediaKind>()

    /**
     * Submitted steps that have not settled, BY RESOURCE. A count alone cannot
     * tell a settlement of the step that was submitted from a settlement of
     * some other step with the same arithmetic, so the identity is what is
     * checked. The guard is NOT one of these.
     */
    private val outstandingSteps = mutableMapOf<CaptureResource, Int>()

    /** True while the driver is still submitting: readiness cannot be decided. */
    private var guarded = false
    private var readyReported = false

    /** Whether a SUCCESSFUL readiness was decided; [readyReported] also covers a refusal. */
    private var readySucceeded = false

    /** Decided events, in decision order, delivered outside the lock in that order. */
    private val outbox = ArrayDeque<() -> Unit>()

    /** One drain owner at a time; a re-entrant or concurrent caller leaves it to that owner. */
    private var delivering = false

    /** Outstanding submissions, however many resources they are spread over (lock held). */
    private fun outstandingLocked(): Int = outstandingSteps.values.sum()

    override val video: CaptureHandoff? get() = synchronized(lock) { if (started) videoTrack else null }

    override val audio: CaptureHandoff? get() = synchronized(lock) { if (started) audioTrack else null }

    override val termination: CaptureTermination? get() = synchronized(lock) { decided }

    /** The same rule as the product session: this generation, and no ending claimed, decided under this lock. */
    override fun admitPublication(generation: Long): Boolean = synchronized(lock) { generation == this.generation && decided == null }

    /**
     * This model keeps no retirement record and admits no format callbacks: a
     * decided ending is received with no causes and no cleanup facts, and with
     * none decided there is nothing to receive.
     */
    override suspend fun retirement(): RetirementReceipt = RetirementReceipt(checkNotNull(termination) { "no ending was claimed" }, causes = emptyList(), cleanup = emptyList())

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
        // One start per session: a second, a concurrent one and one after the
        // ending are all refused by name, and none re-enters the driver.
        val refusal =
            synchronized(lock) {
                when {
                    decided != null -> "the session has already ended"
                    startClaimed -> "this session has already been started"
                    else -> {
                        startClaimed = true
                        null
                    }
                }
            }
        if (refusal != null) return CaptureStart.Refused(refusal)
        // The STARTUP GUARD is its own flag, NOT a phantom entry in the step
        // count. A settlement reported for a step that was never submitted
        // cannot consume it, and cannot be mistaken for the last prerequisite.
        synchronized(lock) { guarded = true }
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
                raised?.let {
                    val termination = CaptureTermination.Cancelled(it.message ?: "submission threw")
                    val mine =
                        synchronized(lock) {
                            guarded = false
                            claimLocked(termination)
                        }
                    schedule.afterFailureRecorded()
                    if (mine) complete(termination, CloseMode.Failed, it)
                }
            }
        if (failed != null) {
            // Steps already submitted are NOT cancelled by this: their
            // callbacks still arrive and settle late into the closing
            // ownership, which is what unwinds what they carry.
            val termination = CaptureTermination.Cancelled(failed.reason)
            val mine =
                synchronized(lock) {
                    guarded = false
                    claimLocked(termination)
                }
            schedule.afterFailureRecorded()
            if (mine) complete(termination, CloseMode.Failed, null)
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

    /** The accounting the driver reports through; neither call runs under this session's lock. */
    private val steps =
        object : AcquisitionSteps {
            override fun submitted(resource: CaptureResource) {
                synchronized(lock) { outstandingSteps[resource] = (outstandingSteps[resource] ?: 0) + 1 }
            }

            override fun settled(resource: CaptureResource, outcome: AdoptOutcome) =
                this@ConformingSession.settled(resource, outcome)
        }

    /** An asynchronous step settled: the last successful one makes the session ready; a failure ends it. */
    private fun settled(resource: CaptureResource, outcome: AdoptOutcome) {
        val reason = if (outcome is AdoptOutcome.Failed) "$resource: ${outcome.reason}" else null
        var mine: CaptureTermination? = null
        val ready =
            synchronized(lock) {
                val open = outstandingSteps[resource] ?: 0
                check(open > 0) { "$resource settled a step that was never submitted" }
                if (open == 1) outstandingSteps.remove(resource) else outstandingSteps[resource] = open - 1
                if (reason != null) {
                    // Recorded in the same step that consumes the failed
                    // prerequisite: nothing can read the zero it leaves as a
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
            is AdoptOutcome.Failed -> mine?.let { refuseStartup(checkNotNull(reason), it) }
            is AdoptOutcome.Retired, is AdoptOutcome.Unknown -> Unit
        }
    }

    /** An asynchronous step failed: unwind first, then say so, exactly once, and never Ready. */
    private fun refuseStartup(reason: String, termination: CaptureTermination) {
        // The owned cleanup reaches its boundary BEFORE the ending is announced.
        val cleanup = mutableListOf<Throwable>()
        closeChildren(CloseMode.Failed, null, cleanup)
        synchronized(lock) {
            if (!readyReported) {
                readyReported = true
                outbox.addLast { listener.onReady(generation, CaptureReady.Refused(reason)) }
            }
            outbox.addLast { listener.onTerminated(generation, termination) }
        }
        cleanup.addAll(deliver())
        carry(null, cleanup)
    }

    /**
     * Readiness is a decision about prerequisites: never after a terminal one,
     * never twice. The decision and its place in the notification order are
     * published in ONE critical section -- marking it first and queueing it
     * afterwards would let a termination decide, queue and deliver in between,
     * and the readiness would then arrive behind the ending it preceded.
     */
    private fun reportReady(): Boolean {
        schedule.beforeReadiness()
        val queued =
            synchronized(lock) {
                if (readyReported || decided != null) {
                    false
                } else {
                    readyReported = true
                    started = true
                    readySucceeded = true
                    outbox.addLast { listener.onReady(generation, CaptureReady.Ready) }
                    true
                }
            }
        if (queued) carry(null, deliver())
        return queued
    }

    /**
     * Delivers what has been decided, in decision order, OUTSIDE the session's
     * lock.
     *
     * ONE drain owner at a time. A listener that calls back into this session
     * -- or another thread that decides something while a callback is running
     * -- queues its event and returns; the owner picks it up when the current
     * callback finishes. So there is no recursive delivery from inside a
     * callback and no lock held across one, and the order is still the order
     * the decisions were made in.
     */
    private fun deliver(): List<Throwable> {
        val failures = mutableListOf<Throwable>()
        while (true) {
            val mine = synchronized(lock) { if (delivering) false else { delivering = true; true } }
            if (!mine) return failures
            try {
                while (true) {
                    val next = synchronized(lock) { outbox.removeFirstOrNull() } ?: break
                    // Attempted ONCE: a notification that threw is neither
                    // re-queued nor allowed to end the drain or replace a
                    // primary that is already leaving.
                    attempt { next() }?.let { failures.add(it) }
                }
            } finally {
                synchronized(lock) { delivering = false }
            }
            if (synchronized(lock) { outbox.isEmpty() }) return failures
        }
    }

    override fun onTrackEnded(generation: Long, kind: MediaKind) {
        if (generation != this.generation) return
        val complete = synchronized(lock) { endedTracks.add(kind); endedTracks.size == 2 && decided == null }
        if (complete) terminate(CaptureTermination.Completed, CloseMode.Completed, null)
    }

    override fun onFault(generation: Long, fault: CaptureFault) {
        if (generation != this.generation) return
        terminate(CaptureTermination.Failed(fault), CloseMode.Failed, null)
    }

    override fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) = Unit

    override fun stop(primary: Exception?): Exception? {
        val termination =
            when (primary) {
                null -> CaptureTermination.Completed
                is CancellationException -> CaptureTermination.Cancelled(primary.message ?: "cancelled")
                else -> CaptureTermination.Cancelled(primary.message ?: "failed")
            }
        val mode = if (primary == null) CloseMode.Completed else CloseMode.Failed
        return terminate(termination, mode, primary)
    }

    /** Decide under the lock; close outside it; exactly once. */
    private fun terminate(termination: CaptureTermination, mode: CloseMode, primary: Throwable?): Exception? {
        val first = synchronized(lock) { claimLocked(termination) }
        if (!first) return primary as? Exception
        return complete(termination, mode, primary)
    }

    /** Records the one terminal decision; the lock is held and no work is done here. */
    private fun claimLocked(termination: CaptureTermination): Boolean {
        if (decided != null) return false
        decided = termination
        return true
    }

    private fun complete(termination: CaptureTermination, mode: CloseMode, primary: Throwable?): Exception? {
        // The children reach their defined boundary before the ending is
        // announced; a readiness decided before this point was queued while
        // `decided` was still null, so it is already ahead of this in the order.
        val cleanup = mutableListOf<Throwable>()
        val ending = closeChildren(mode, primary, cleanup)
        synchronized(lock) { outbox.addLast { listener.onTerminated(generation, termination) } }
        cleanup.addAll(deliver())
        // A failure the ownership REPORTED is returned when nothing else
        // escapes. When something does, the return value is unreachable, so it
        // is carried rather than lost.
        if (ending != null && ending !== primary && cleanup.isNotEmpty() && cleanup.none { it === ending }) {
            cleanup.add(ending)
        }
        carry(primary, cleanup)
        return ending
    }

    /**
     * The children, in order, outside the lock. The codec is NOT disposed here:
     * it is one of the ownership's resources, and closing that handle asks its
     * lifetime for a disposal that happens when the last transfer settles.
     */
    private fun closeChildren(mode: CloseMode, primary: Throwable?, cleanup: MutableList<Throwable>): Exception? {
        // EVERY one is attempted, whatever an earlier one did.
        attempt { videoTrack.close(mode) }?.let { cleanup.add(it) }
        attempt { audioTrack.close(mode) }?.let { cleanup.add(it) }
        val carried = primary as? Exception
        var ending = carried
        attempt { ownership.close(carried).also { ending = it } }?.let { cleanup.add(it) }
        val reported = ending
        if (primary != null && carried == null && reported != null) cleanup.add(reported)
        return ending
    }

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

    /** The primary keeps its identity; with none, the first cleanup failure escapes. */
    private fun carry(primary: Throwable?, cleanup: List<Throwable>) {
        if (primary != null) {
            val attached = mutableListOf<Throwable>()
            for (other in cleanup) {
                if (other === primary || attached.any { it === other }) continue
                attached.add(other)
                primary.addSuppressed(other)
            }
            return
        }
        val carrier = cleanup.firstOrNull() ?: return
        for (other in cleanup) if (other !== carrier) carrier.addSuppressed(other)
        throw carrier
    }
    private companion object {
        const val ENDED_BEFORE_READY = "the session ended before the startup was ready"
    }

}

/**
 * One scripted platform encoder, shared by the encoder, camera and audio rows:
 * every call logged as `step:name`, in order, with failures, holds and inline
 * callbacks scripted per device.
 */
internal class HostEncoderDevice(val name: String, private val log: MutableList<String>) : EncoderDevice {
    override val identity: Any = HostResource(name)
    val buffers = mutableMapOf<Int, ByteBuffer>()

    @Volatile var registered: EncoderCallbacks? = null

    @Volatile var configured: EncoderConfiguration? = null

    /** The step that raises, and what it raises. */
    @Volatile var failAt: String? = null

    @Volatile var failure: Throwable? = null

    /** Raised by `release`, after it is logged. */
    @Volatile var releaseFailure: Throwable? = null

    /** Raised by the input surface's release, after it is logged. */
    @Volatile var surfaceFailure: Throwable? = null

    /** Runs inside `start`, as a callback delivered while the codec starts. */
    @Volatile var duringStart: (EncoderCallbacks) -> Unit = {}

    /** When set, the fetch of that index is held here until released. */
    @Volatile var holdFetch: Triple<Int, HostGate, HostGate>? = null

    val surface = HostResource("$name-surface")

    fun steps(): List<String> = synchronized(log) { log.filter { it.endsWith(":$name") } }

    private fun step(what: String) {
        synchronized(log) { log.add("$what:$name") }
        if (failAt == what) throw checkNotNull(failure)
    }

    override fun setCallbacks(callbacks: EncoderCallbacks) {
        step("callbacks")
        registered = callbacks
    }

    override fun configure(configuration: EncoderConfiguration) {
        step("configure")
        configured = configuration
    }

    override fun createInputSurface(): ResourceHandle {
        step("surface")
        return object : ResourceHandle {
            override val identity: Any = surface

            override fun close() {
                step("surface-release")
                surfaceFailure?.let { throw it }
            }
        }
    }

    override fun start() {
        step("start")
        duringStart(checkNotNull(registered))
    }

    override fun outputBuffer(index: Int): ByteBuffer? {
        step("get $index")
        holdFetch?.let { (at, reached, release) ->
            if (at == index) {
                reached.open()
                release.await()
            }
        }
        return synchronized(buffers) { buffers[index] }
    }

    override fun releaseOutputBuffer(index: Int) = step("release $index")

    /** The codec's input buffers: direct, one per index, as large as any unit a row queues. */
    val inputs = mutableMapOf<Int, ByteBuffer>()

    /** Every queued input: index, the exact bytes, and the presentation time. */
    val queued = mutableListOf<Triple<Int, ByteArray, Long>>()

    /** Raised by `queueInputBuffer`, after it is logged. */
    @Volatile var queueFailure: Throwable? = null

    override fun inputBuffer(index: Int): ByteBuffer? {
        step("input $index")
        return synchronized(inputs) { inputs.getOrPut(index) { ByteBuffer.allocateDirect(1 shl 14) } }
    }

    override fun queueInputBuffer(index: Int, size: Int, presentationTimeUs: Long) {
        step("queue $index $size $presentationTimeUs")
        queueFailure?.let { throw it }
        val bytes = ByteArray(size)
        synchronized(inputs) { checkNotNull(inputs[index]).duplicate().apply { position(0); limit(size) }.get(bytes) }
        synchronized(queued) { queued.add(Triple(index, bytes, presentationTimeUs)) }
    }

    override fun stop() = step("stop")

    override fun release() {
        step("release-codec")
        releaseFailure?.let { throw it }
    }
}

/** Creates one [HostEncoderDevice] per kind, logged under the kind's name; [refuse] scripts an unsupported encoder. */
internal class HostEncoderBackend(private val log: MutableList<String>) : EncoderBackend {
    val created = mutableMapOf<MediaKind, HostEncoderDevice>()

    @Volatile var refuse: String? = null

    /** Configures each device before it is handed to the encoder. */
    @Volatile var prepare: (HostEncoderDevice) -> Unit = {}

    override fun create(configuration: EncoderConfiguration): EncoderDevice {
        synchronized(log) { log.add("create:${configuration.kind}") }
        refuse?.let { throw EncoderUnsupported(configuration.kind, it) }
        return HostEncoderDevice(configuration.kind.name.lowercase(), log).also {
            prepare(it)
            synchronized(created) { created[configuration.kind] = it }
        }
    }

    fun device(kind: MediaKind): HostEncoderDevice = checkNotNull(synchronized(created) { created[kind] }) { "no $kind encoder was created" }
}
