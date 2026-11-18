package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException

/**
 * What a close may destroy, and when.
 *
 * A reserved queue slot is an agreement between the handoff and its consumer;
 * it says nothing about the codec. EVERY call that reaches codec-owned memory
 * -- the fetch, the copy, and the index release, including the release a
 * refusal makes -- must happen while the codec is held, and the destruction has
 * to wait for the last of them. The dispatcher those callbacks arrive on has to
 * outlive it too.
 *
 * An admitted index is owned until it goes back: a cancelled or throwing
 * callback releases it exactly once and settles its reservation exactly once,
 * and neither leaves a lease behind.
 *
 * Every controlling wait has a fail-only deadline, every gate is opened and
 * every worker settled in a `finally`, and a watchdog expiry is a failure,
 * never evidence.
 *
 * There is no codec here. A destroyed [HostCodec] is a flag in a fixture, not a
 * released `MediaCodec`; these rows are about ORDER, not about what the
 * platform does with a buffer after `release`.
 */
class CaptureResourceOrderTest {

    private class Rig(
        val kind: MediaKind = MediaKind.Video,
        eager: Boolean = false,
        val capacity: Int = CAPACITY,
        dispatcherOwnedByCodec: Boolean = true,
        codecIs: ((MutableList<String>, HostDispatcher?) -> HostCodecOwner)? = null,
    ) {
        val order = mutableListOf<String>()
        val dispatcher = HostDispatcher()
        private val codecDispatcher = if (dispatcherOwnedByCodec) dispatcher else null
        val codec: HostCodecOwner =
            codecIs?.invoke(order, codecDispatcher)
                ?: if (eager) EagerDisposalCodec(order, codecDispatcher) else HostCodec(order, codecDispatcher)
        val buffers = HostBuffers()
        val releases = HostReleases(order = order)
        val faults = HostFaults()
        val ledger = HostLedger(kind, GENERATION, capacity = 16)

        /** An observation under [pts] and a codec buffer at [index]. */
        fun stage(index: Int, pts: Long): CodecOutput {
            val payload = ByteArray(4) { (index + 1).toByte() }
            buffers.put(index, ByteBuffer.wrap(payload))
            ledger.observe(GENERATION, kind, CaptureObservation(pts, 1_000L + pts))
            return CodecOutput(index, 0, payload.size, pts, CaptureContract.FLAG_KEY_FRAME)
        }

        fun orderNow(): List<String> = synchronized(order) { order.toList() }
    }

    /** A fetch that parks INSIDE the codec, then returns the buffer or raises. */
    private class GatedBuffers(
        private val inner: HostBuffers,
        private val arrived: HostGate,
        private val resume: HostGate,
        private val raise: (() -> Throwable)? = null,
    ) : OutputBufferAccess {
        private val parked = AtomicBoolean(false)

        /** Parks and raises for the FIRST fetch only; later fetches are ordinary. */
        override fun outputBuffer(index: Int): ByteBuffer? {
            if (!parked.compareAndSet(false, true)) return inner.outputBuffer(index)
            arrived.open()
            resume.await()
            val buffer = inner.outputBuffer(index)
            raise?.let { throw it() }
            return buffer
        }
    }

    /** A release that runs [during] while the codec is still being given its index back. */
    private class ReleaseHook(private val inner: CodecBufferOwner, private val during: (Int) -> Unit) : CodecBufferOwner {
        override fun release(index: Int) {
            inner.release(index)
            during(index)
        }
    }

    /** A release that raises instead of returning, for ONE named index. */
    private class RaisingReleases(
        private val inner: CodecBufferOwner,
        private val at: Int,
        private val raise: () -> Throwable,
    ) : CodecBufferOwner {
        override fun release(index: Int) {
            inner.release(index)
            if (index == at) throw raise()
        }
    }

    /**
     * Parks each named thread at its OWN gate inside the transfer, and releases
     * it from its own gate too. One shared release gate would let both workers
     * run at once, which orders nothing.
     */
    private class ParkEach(
        private val reached: Map<String, HostGate>,
        private val proceed: Map<String, HostGate>,
        private val shared: HostGate? = null,
    ) : HandoffSchedule {
        override fun beforeRelease() {
            val name = Thread.currentThread().name
            reached[name]?.open()
            (proceed[name] ?: shared)?.await()
        }
    }

    /** Builds the ownership under test: the host fixture, or the sample's own class. */
    private fun interface Ownerships {
        fun make(generation: Long, dispatcher: CallbackDispatcher, late: LateCleanupSink): CaptureOwnership
    }

    private val conformingOwnership = Ownerships { g, d, l -> ConformingOwnership(g, d, l) }
    private val orderedOwnership = Ownerships { g, d, l -> OrderedCaptureOwnership(g, d, l) }

    /**
     * The production class exposes no counter, by design. Its evidence is the
     * observable retirement every row already asserts -- the dispatcher quit,
     * and exactly one close in the recorded order. Where a host fixture does
     * expose the count, it is checked as well.
     */
    private fun assertDependents(ownership: CaptureOwnership, expected: Int, message: String = "worker dependents") {
        (ownership as? ConformingOwnership)?.let { assertEquals(expected, it.workerDependents, message) }
    }

    private fun interface Make {
        fun make(rig: Rig, schedule: HandoffSchedule, buffers: OutputBufferAccess, owner: CodecBufferOwner): CaptureHandoff
    }

    private fun build(make: Make, rig: Rig, schedule: HandoffSchedule = HandoffSchedule.NONE, buffers: OutputBufferAccess = rig.buffers, owner: CodecBufferOwner = rig.releases) =
        make.make(rig, schedule, buffers, owner)

    // ---- the rows --------------------------------------------------------------

    /**
     * Close arrives while two admitted transfers are between the copy and the
     * index release. Neither the codec nor the dispatcher may go until both
     * indices are back. The two admissions are ORDERED by gates: thread start
     * order proves nothing, and a second output admitted first would be a
     * legitimate reordering refusal rather than the window under test.
     */
    private fun theCodecOutlivesEveryAdmittedTransfer(
        make: Make,
        rig: Rig,
        releaseOrder: List<Int> = listOf(0, 1),
        owner: CodecBufferOwner = rig.releases,
    ) {
        val firstIn = HostGate("the first transfer reached its index release")
        val secondIn = HostGate("the second transfer reached its index release")
        val proceed0 = HostGate("the row released transfer-0")
        val proceed1 = HostGate("the row released transfer-1")
        val schedule =
            ParkEach(
                mapOf("transfer-0" to firstIn, "transfer-1" to secondIn),
                mapOf("transfer-0" to proceed0, "transfer-1" to proceed1),
            )
        val handoff = build(make, rig, schedule, owner = owner)
        val first = rig.stage(index = 0, pts = 0L)
        val second = rig.stage(index = 1, pts = 33_333L)
        val a = HostWorker("transfer-0") { handoff.onOutput(GENERATION, first) }.start()
        val workers = mutableListOf(a)
        withSettlement(listOf(firstIn, secondIn, proceed0, proceed1), workers) {
            // The first output is admitted, leased and parked BEFORE the second
            // is even offered, so the presentation order IS the admission order.
            // Thread.start order would prove nothing, and a second output
            // admitted first would be a legitimate reordering refusal.
            firstIn.await(a)
            val b = HostWorker("transfer-1") { handoff.onOutput(GENERATION, second) }.start()
            workers.add(b)
            secondIn.await(b)
            handoff.close(CloseMode.Failed)
            rig.codec.requestDisposal()
            assertTrue(
                !rig.codec.isDisposed,
                "the codec must not be disposed before the final index release: two admitted transfers are still inside it",
            )
            assertTrue(rig.dispatcher.isAlive, "nor may the callback dispatcher be quit while a transfer is still running")
            assertEquals(emptyList(), rig.orderNow(), "and nothing has been released or destroyed yet")
            // The two returns are FORCED, one at a time, in the order this arm
            // was given. Releasing both together would order nothing, and the
            // obligation is not that returns happen in admission order -- it is
            // that BOTH happen before the codec goes.
            val proceeds = listOf(proceed0, proceed1)
            val firstBack = releaseOrder[0]
            val lastBack = releaseOrder[1]
            proceeds[firstBack].openFully()
            workers[firstBack].join()
            assertTrue(
                !rig.codec.isDisposed,
                "the codec must not be disposed while the other admitted transfer is still inside it",
            )
            assertTrue(rig.dispatcher.isAlive, "nor may its dispatcher be quit")
            assertEquals(listOf("release:$firstBack"), rig.orderNow(), "one index is back, and nothing else has happened")
            proceeds[lastBack].openFully()
            workers[lastBack].join()
            assertEquals(
                listOf("release:$firstBack", "release:$lastBack", "dispose:codec", "quit:dispatcher"),
                rig.orderNow(),
                "exactly those two indices, each once, then ONE disposal, then the dispatcher",
            )
            assertEquals(1, rig.codec.disposalCount, "destroyed exactly once")
            assertEquals(
                listOf(HostFaults.Discarded(rig.kind, 2)),
                rig.faults.discards(),
                "both copies landed after the close: discarded and witnessed exactly once",
            )
            assertNull(handoff.next(), "a failed close publishes nothing")
            val fetched = rig.buffers.fetched()
            val released = rig.releases.indices()
            assertIs<HandoffOutcome.Ignored>(
                handoff.onOutput(GENERATION, rig.stage(index = 2, pts = 66_666L)),
                "a callback that arrives after the codec is gone",
            )
            assertEquals(fetched, rig.buffers.fetched(), "nothing is fetched from a destroyed codec")
            assertEquals(released, rig.releases.indices(), "and no index is handed back to one")
        }
    }

    /** The same ordering when the fetch itself throws: the index still goes back first. */
    private fun aFetchThatThrewStillSettlesBeforeDisposal(make: Make, rig: Rig) {
        val inFetch = HostGate("the transfer parked inside getOutputBuffer")
        val proceed = HostGate("the row released the parked fetch")
        val gated = GatedBuffers(rig.buffers, inFetch, proceed) { IllegalStateException("codec is not executing") }
        val handoff = build(make, rig, buffers = gated)
        val out = rig.stage(index = 0, pts = 0L)
        val worker = HostWorker("transfer") { handoff.onOutput(GENERATION, out) }.start()
        withSettlement(listOf(inFetch, proceed), listOf(worker)) {
            inFetch.await(worker)
            handoff.close(CloseMode.Failed)
            rig.codec.requestDisposal()
            assertTrue(
                !rig.codec.isDisposed,
                "the codec must not be disposed before the final index release: a transfer is inside getOutputBuffer",
            )
            proceed.openFully()
            worker.join()
            assertEquals(
                listOf("release:0", "dispose:codec", "quit:dispatcher"),
                rig.orderNow(),
                "a fetch that threw still returns its index, and the codec goes after it",
            )
            assertEquals(1, rig.codec.disposalCount, "destroyed exactly once")
            assertEquals(emptyList(), rig.faults.all(), "the owner closed this track already; it is not told again")
            assertNull(handoff.next())
        }
    }

    /**
     * Cancellation while the session closes. The callback is over; the index it
     * was given is NOT -- it goes back once, while the codec is still held.
     */
    private fun aCancelledFetchDuringCloseStillReturnsItsIndex(make: Make, rig: Rig) {
        val inFetch = HostGate("the transfer parked inside getOutputBuffer")
        val proceed = HostGate("the row released the parked fetch")
        val gated = GatedBuffers(rig.buffers, inFetch, proceed) { CancellationException("the capture scope was cancelled") }
        val handoff = build(make, rig, buffers = gated)
        val out = rig.stage(index = 0, pts = 0L)
        val cancelled = AtomicBoolean(false)
        val worker =
            HostWorker("transfer") {
                try {
                    handoff.onOutput(GENERATION, out)
                } catch (raised: CancellationException) {
                    cancelled.set(true)
                }
            }.start()
        withSettlement(listOf(inFetch, proceed), listOf(worker)) {
            inFetch.await(worker)
            handoff.close(CloseMode.Failed)
            rig.codec.requestDisposal()
            assertTrue(!rig.codec.isDisposed, "a transfer inside getOutputBuffer still holds the codec")
            proceed.openFully()
            worker.join()
            assertTrue(cancelled.get(), "cancellation propagates out of the callback")
            assertEquals(
                listOf("release:0", "dispose:codec", "quit:dispatcher"),
                rig.orderNow(),
                "a cancelled callback is not proof that the codec took its index back: it is released here, once, before disposal",
            )
            assertEquals(1, rig.codec.disposalCount, "destroyed exactly once")
            assertEquals(
                listOf(HostFaults.Discarded(rig.kind, 1)),
                rig.faults.discards(),
                "the admitted output it never delivered is counted once",
            )
        }
    }

    /** Cancellation with no close in sight: the same index and slot discipline. */
    private fun aCancelledFetchLeavesNoLeaseAndNoReservation(make: Make, rig: Rig) {
        val inFetch = HostGate("the transfer parked inside getOutputBuffer")
        val proceed = HostGate("the row released the parked fetch")
        val gated = GatedBuffers(rig.buffers, inFetch, proceed) { CancellationException("cancelled mid-fetch") }
        val handoff = build(make, rig, buffers = gated)
        val out = rig.stage(index = 0, pts = 0L)
        val worker =
            HostWorker("transfer") {
                try {
                    handoff.onOutput(GENERATION, out)
                } catch (raised: CancellationException) {
                    Unit
                }
            }.start()
        withSettlement(listOf(inFetch, proceed), listOf(worker)) {
            inFetch.await(worker)
            proceed.openFully()
            worker.join()
        }
        assertEquals(listOf(0), rig.releases.indices(), "the admitted index went back exactly once")
        assertEquals(0, leasesOf(rig), "and the lease went with it")
        assertEquals(emptyList(), rig.faults.all(), "nothing was closed, so there is nothing to witness")
        // A leaked reservation would show up as an overflow here: the queue is
        // this small on purpose.
        for (i in 1..rig.capacity) {
            assertEquals(
                HandoffOutcome.Queued,
                handoff.onOutput(GENERATION, rig.stage(index = i, pts = i * 10_000L)),
                "the cancelled output's reservation settled: the whole queue is available again",
            )
        }
    }

    /** A release that is cancelled is still a release: it is never attempted twice. */
    private fun aCancelledReleaseIsNotRetriedAndStillSettlesItsSlot(make: Make, rig: Rig) {
        val raising = RaisingReleases(rig.releases, at = 0) { CancellationException("cancelled while releasing") }
        val handoff = build(make, rig, owner = raising)
        val out = rig.stage(index = 0, pts = 0L)
        assertFailsWith<CancellationException> { handoff.onOutput(GENERATION, out) }
        assertEquals(listOf(0), rig.releases.indices(), "attempted exactly once: it may already have taken effect")
        assertEquals(0, leasesOf(rig), "the lease was given back after the codec access ended")
        for (i in 1..rig.capacity) {
            assertEquals(
                HandoffOutcome.Queued,
                handoff.onOutput(GENERATION, rig.stage(index = i, pts = i * 10_000L)),
                "the reservation settled exactly once, so the queue is whole",
            )
        }
        rig.codec.requestDisposal()
        assertEquals(listOf("release:0", "release:1", "release:2", "dispose:codec", "quit:dispatcher"), rig.orderNow())
    }

    /** A scheduled boundary that throws is not a way to keep an index or a slot. */
    private fun aThrowingBoundarySettlesTheIndexAndTheSlot(make: Make, rig: Rig) {
        val once = AtomicBoolean(false)
        val schedule =
            object : HandoffSchedule {
                override fun beforeRelease() {
                    if (once.compareAndSet(false, true)) throw IllegalStateException("the boundary threw")
                }
            }
        val handoff = build(make, rig, schedule = schedule)
        val out = rig.stage(index = 0, pts = 0L)
        assertFailsWith<IllegalStateException> { handoff.onOutput(GENERATION, out) }
        assertEquals(listOf(0), rig.releases.indices(), "the admitted index went back once")
        assertEquals(0, leasesOf(rig), "and no lease is left holding the codec")
        for (i in 1..rig.capacity) {
            assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, rig.stage(index = i, pts = i * 10_000L)))
        }
    }

    /**
     * The refusal path is a codec access too. A full queue refuses and hands
     * the index back; a close and a disposal that arrive DURING that release
     * must wait for it.
     */
    private fun aRefusalHoldsTheCodecAcrossItsOwnIndexRelease(make: Make, rig: Rig) {
        val duringRelease = mutableListOf<String>()
        var track: CaptureHandoff? = null
        val owner =
            ReleaseHook(rig.releases) { index ->
                if (index == REFUSED_INDEX) {
                    // The owner gives up exactly while the refused index is
                    // being handed back to the codec.
                    track?.close(CloseMode.Failed)
                    rig.codec.requestDisposal()
                    duringRelease.add("index=$index disposed=${rig.codec.isDisposed}")
                }
            }
        val handoff = build(make, rig, owner = owner)
        track = handoff
        assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, rig.stage(index = 0, pts = 0L)), "the one slot is taken")
        assertIs<HandoffOutcome.Refused>(
            handoff.onOutput(GENERATION, rig.stage(index = REFUSED_INDEX, pts = 10_000L)),
            "the next output does not fit the one-slot queue",
        )
        assertEquals(
            listOf("index=$REFUSED_INDEX disposed=false"),
            duringRelease,
            "the codec must not be disposed while the refusal is still handing its index back",
        )
        assertEquals(1, rig.codec.disposalCount, "destroyed exactly once, after that release returned")
        val order = rig.orderNow()
        assertTrue(
            order.indexOf("release:$REFUSED_INDEX") < order.indexOf("dispose:codec"),
            "and in that order: $order",
        )
        assertEquals(
            listOf(HostFaults.Discarded(rig.kind, 1)),
            rig.faults.discards(),
            "the close that arrived during the release witnessed the output it discarded, once",
        )
        assertEquals(emptyList(), rig.faults.faults(), "the owner had already given this track up; it is not told again")
    }

    /**
     * A TRANSFER rejection (malformed range, or a null fetch) whose own index
     * release is cancelled. The refusal was already decided, so it stays
     * terminal: the original cancellation leaves by identity, the declared
     * fault and the owed wake are still delivered, and the track admits
     * nothing more. The reservation the rejected output held is settled --
     * proven by the positive discard witness a failed close emits only once
     * every reservation has settled, counting exactly the one valid output
     * that was queued before, and never the rejected one.
     */
    private fun aCancelledReleaseOnARefusalStillSettlesTheSlot(make: Make, rig: Rig, malformed: Boolean) {
        val rejected = 1
        val sentinel = CancellationException(if (malformed) "cancelled releasing a malformed range" else "cancelled releasing a null fetch")
        val handoff = build(make, rig, owner = RaisingReleases(rig.releases, at = rejected) { sentinel })
        var wakes = 0
        handoff.signal { wakes++ }
        val codec = rig.codec as HostCodec

        // Geometry: one valid output queued, one slot of the two still free,
        // so the rejected output is ADMITTED and refused in transfer -- not
        // refused for overflow at admission.
        assertEquals(2, rig.capacity, "two slots")
        assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, rig.stage(index = 0, pts = 0L)), "the valid output takes one slot")
        assertEquals(1, wakes, "its queueing woke the drain once")
        assertEquals(1, codec.leaseReturns)

        val out =
            if (malformed) {
                rig.stage(index = rejected, pts = 10_000L).let { CodecOutput(it.index, 2, 8, it.presentationTimeUs, it.flags) }
            } else {
                // Nothing staged at this index: getOutputBuffer returns null.
                CodecOutput(rejected, 0, 4, 10_000L, CaptureContract.FLAG_KEY_FRAME)
            }
        val thrown = assertFailsWith<CancellationException> { handoff.onOutput(GENERATION, out) }
        assertSame(sentinel, thrown, "the original cancellation is what propagates, unchanged")
        assertEquals(emptyList<Throwable>(), thrown.suppressed.toList(), "carrying nothing: no other obligation raised")
        assertEquals(listOf(0, rejected), rig.buffers.fetched(), "each admitted transfer fetched once")
        assertEquals(listOf(0, rejected), rig.releases.indices(), "the rejected index was attempted exactly once, and never retried")
        assertEquals(2, codec.leaseReturns, "one lease per attempted transfer")
        assertEquals(0, leasesOf(rig), "and no lease is left holding the codec")
        val declared =
            if (malformed) CaptureFault.Malformed(rig.kind, "range past the buffer limit")
            else CaptureFault.BufferAccess(rig.kind, "getOutputBuffer returned null")
        assertEquals(listOf(declared), rig.faults.faults(), "the decided refusal's fault, reported once")
        assertEquals(2, wakes, "the refusal's owed wake")
        assertTrue(handoff.ended, "the refusal is terminal")
        val outstandingBefore = rig.ledger.outstanding

        val later = rig.stage(index = 2, pts = 20_000L)
        val outstandingStaged = rig.ledger.outstanding
        assertEquals(outstandingBefore + 1, outstandingStaged)
        assertIs<HandoffOutcome.Ignored>(handoff.onOutput(GENERATION, later), "the ended track admits nothing more")
        assertEquals(listOf(0, rejected), rig.buffers.fetched(), "no fetch for it")
        assertEquals(listOf(0, rejected), rig.releases.indices(), "no release for it")
        assertEquals(outstandingStaged, rig.ledger.outstanding, "no claim for it")
        assertEquals(2, codec.leaseReturns, "no lease for it")
        assertEquals(2, wakes)

        handoff.close(CloseMode.Failed)
        assertEquals(
            listOf(HostFaults.Discarded(rig.kind, 1)),
            rig.faults.discards(),
            "ONE witness, emitted only when every reservation has settled, counting only the queued valid output",
        )
        assertNull(handoff.next(), "the failed close dropped the queued output")
        assertEquals(3, wakes, "the close's own wake")
        handoff.close(CloseMode.Failed)
        assertEquals(listOf(HostFaults.Discarded(rig.kind, 1)), rig.faults.discards(), "a repeated close witnesses nothing new")
        assertEquals(listOf(0, rejected), rig.releases.indices(), "and attempts no index")
        assertEquals(3, wakes, "and does not wake again")
        assertEquals(listOf(declared), rig.faults.faults())
    }

    /** The commit boundary is inside the callback's guard, not outside it. */
    private fun aCancelledCommitBoundaryStillSettlesTheSlot(make: Make, rig: Rig) {
        val sentinel = CancellationException("cancelled at the commit boundary")
        val once = AtomicBoolean(false)
        val schedule =
            object : HandoffSchedule {
                override fun beforeCommit() {
                    if (once.compareAndSet(false, true)) throw sentinel
                }
            }
        val handoff = build(make, rig, schedule = schedule)
        val thrown = assertFailsWith<CancellationException> { handoff.onOutput(GENERATION, rig.stage(index = 0, pts = 0L)) }
        assertSame(sentinel, thrown, "the original cancellation is what propagates, unchanged")
        assertEquals(listOf(0), rig.releases.indices(), "the index had already gone back, once")
        assertEquals(0, leasesOf(rig), "the lease ended with the codec access")
        assertNull(handoff.next(), "nothing was committed")
        for (i in 1..rig.capacity) {
            assertEquals(
                HandoffOutcome.Queued,
                handoff.onOutput(GENERATION, rig.stage(index = i, pts = i * 10_000L)),
                "the reservation settled even though the boundary threw after the release",
            )
        }
    }

    /** A close that arrives at that boundary: counted once, witnessed once. */
    private fun aCloseAtTheCommitBoundaryDiscardsAndWitnessesOnce(make: Make, rig: Rig) {
        val sentinel = CancellationException("cancelled as the owner gave up")
        val once = AtomicBoolean(false)
        var track: CaptureHandoff? = null
        val schedule =
            object : HandoffSchedule {
                override fun beforeCommit() {
                    if (!once.compareAndSet(false, true)) return
                    track?.close(CloseMode.Failed)
                    throw sentinel
                }
            }
        val handoff = build(make, rig, schedule = schedule)
        track = handoff
        val thrown = assertFailsWith<CancellationException> { handoff.onOutput(GENERATION, rig.stage(index = 0, pts = 0L)) }
        assertSame(sentinel, thrown)
        assertEquals(listOf(0), rig.releases.indices(), "the index went back once")
        assertNull(handoff.next(), "a failed close publishes nothing")
        // The witness only fires when no reservation is outstanding, so this
        // row would not pass with a leaked slot.
        assertEquals(
            listOf(HostFaults.Discarded(rig.kind, 1)),
            rig.faults.discards(),
            "the admitted output it never delivered is counted and witnessed exactly once",
        )
        assertEquals(0, leasesOf(rig))
    }

    /** A transfer the codec can no longer serve touches nothing at all. */
    private fun aTransferRefusedTheCodecTouchesNothing(make: Make, rig: Rig) {
        val handoff = build(make, rig)
        val out = rig.stage(index = 0, pts = 0L)
        rig.codec.requestDisposal()
        assertTrue(rig.codec.isDisposed, "no transfer was outstanding, so the codec goes immediately")
        assertIs<HandoffOutcome.Refused>(handoff.onOutput(GENERATION, out), "the codec was disposed before this transfer began")
        assertEquals(emptyList(), rig.buffers.fetched(), "no buffer is fetched from a destroyed codec")
        assertEquals(emptyList(), rig.releases.indices(), "and no index is handed back to one")
        assertEquals(1, rig.faults.faults().size, "the owner is told, exactly once")
        assertIs<CaptureFault.Codec>(rig.faults.faults().single(), "the owner is told once, by name")
        assertNull(handoff.next())
    }

    private fun leasesOf(rig: Rig): Int = (rig.codec as? HostCodec)?.outstandingLeases ?: 0

    private fun runResourceOrder(make: Make) {
        // BOTH release schedules, each forced: the obligation holds whichever
        // admitted transfer returns its index first.
        theCodecOutlivesEveryAdmittedTransfer(make, Rig(), releaseOrder = listOf(0, 1))
        theCodecOutlivesEveryAdmittedTransfer(make, Rig(), releaseOrder = listOf(1, 0))
        aFetchThatThrewStillSettlesBeforeDisposal(make, Rig())
        aCancelledFetchDuringCloseStillReturnsItsIndex(make, Rig())
        aCancelledFetchLeavesNoLeaseAndNoReservation(make, Rig())
        aCancelledReleaseIsNotRetriedAndStillSettlesItsSlot(make, Rig())
        aThrowingBoundarySettlesTheIndexAndTheSlot(make, Rig())
        aRefusalHoldsTheCodecAcrossItsOwnIndexRelease(make, Rig(capacity = 1))
        aTransferRefusedTheCodecTouchesNothing(make, Rig())
        aCancelledReleaseOnARefusalStillSettlesTheSlot(make, Rig(), malformed = false)
        aCancelledReleaseOnARefusalStillSettlesTheSlot(make, Rig(), malformed = true)
        aCancelledCommitBoundaryStillSettlesTheSlot(make, Rig())
        aCloseAtTheCommitBoundaryDiscardsAndWitnessesOnce(make, Rig())
    }

    private val real =
        Make { rig, schedule, buffers, owner ->
            BoundedCaptureHandoff(rig.kind, GENERATION, buffers, owner, rig.ledger, rig.faults, rig.capacity, HOST_MAX_OUTPUT_BYTES, rig.codec, schedule)
        }
    private val conforming =
        Make { rig, schedule, buffers, owner ->
            ConformingHandoff(rig.kind, GENERATION, buffers, owner, rig.ledger, rig.faults, rig.capacity, rig.codec, schedule)
        }

    @Test
    fun x_theVideoHandoffHoldsItsCodecThroughEveryAccess() = runResourceOrder(real)

    @Test
    fun s_theOrderingIsSatisfiableByAConformingFixture() = runResourceOrder(conforming)

    /**
     * The whole wiring, through the resources the OWNERSHIP holds.
     *
     * The worker here is a REAL one: closing it quits the dispatcher the codec's
     * callbacks arrive on. So an ownership that retires the worker while an
     * admitted transfer is still inside the codec is observable -- the transfer
     * would be delivering onto a quit looper -- and this row fails on it.
     */
    private fun terminatingTheSessionKeepsTheWorkerUntilEveryDependentSettles(make: OwnershipMake) {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val ownership = make.make(GENERATION, rig.dispatcher, LateLog())
        val codec = HostCodec(rig.order, dispatcher = null, worker = ownership)
        ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order) }
        ownership.acquire(CaptureResource.VideoEncoder) { HostCodecHandle(codec, "codec-handle", rig.order) }
        val inRelease = HostGate("the transfer reached its index release")
        val proceed = HostGate("the row released the parked transfer")
        val schedule = ParkEach(mapOf("transfer" to inRelease), emptyMap(), proceed)
        val video = ConformingHandoff(MediaKind.Video, GENERATION, rig.buffers, rig.releases, rig.ledger, rig.faults, rig.capacity, codec, schedule)
        val audio = ConformingHandoff(MediaKind.Audio, GENERATION, HostBuffers(), HostReleases(), HostLedger(MediaKind.Audio, GENERATION), HostFaults(), rig.capacity)
        val listener = HostSessionListener()
        val session = ConformingSession(GENERATION, ownership, NoAcquire(), video, audio, listener)
        assertEquals(CaptureStart.Started, session.start(GrantedFlow(GENERATION), CaptureCapability.Available, CaptureCapability.Available))
        val out = rig.stage(index = 0, pts = 0L)
        val worker = HostWorker("transfer") { video.onOutput(GENERATION, out) }.start()
        withSettlement(listOf(inRelease, proceed), listOf(worker)) {
            inRelease.await(worker)
            session.stop(null)
            assertTrue(ownership.isClosed, "the ownership closed with the session")
            assertTrue(!codec.isDisposed, "the codec it holds is still in use by an admitted transfer")
            assertTrue(
                rig.dispatcher.isAlive,
                "and the worker the callbacks arrive on must still be alive: an admitted transfer is a dependent of it",
            )
            assertEquals(listOf("close:codec-handle"), rig.orderNow(), "closing the handle only ASKS for the disposal")
            proceed.openFully()
            worker.join()
            assertEquals(
                listOf("close:codec-handle", "release:0", "dispose:codec", "close:worker", "quit:dispatcher"),
                rig.orderNow(),
                "the codec goes after the last index, and the worker it was driven from goes after the codec",
            )
            assertTrue(!rig.dispatcher.isAlive, "only then is the dispatcher quit")
            assertEquals(1, codec.disposalCount, "destroyed exactly once")
            assertEquals(1, listener.all().count { it is CaptureTermination }, "terminated once")
        }
    }

    private fun interface OwnershipMake {
        fun make(generation: Long, dispatcher: HostDispatcher, late: LateCleanupSink): CaptureOwnership
    }

    /**
     * Ownership as it was before the worker had dependents: it retires the
     * worker for tickets only, so an admitted transfer does not hold it.
     */
    private class RetiringOwnership(
        generation: Long,
        dispatcher: HostDispatcher,
        late: LateCleanupSink,
    ) : CaptureOwnership by ConformingOwnership(generation, dispatcher, late) {
        override fun retainWorker(): Boolean = true

        override fun releaseWorker() = Unit
    }

    @Test
    fun theOwnedWorkerOutlivesEveryDependent_conforming() =
        terminatingTheSessionKeepsTheWorkerUntilEveryDependentSettles(OwnershipMake { g, d, l -> ConformingOwnership(g, d, l) })

    @Test
    fun theOwnedWorkerOutlivesEveryDependent_ordered() =
        terminatingTheSessionKeepsTheWorkerUntilEveryDependentSettles(OwnershipMake { g, d, l -> OrderedCaptureOwnership(g, d, l) })

    @Test
    fun anOwnershipThatRetiresTheWorkerForTicketsOnlyFailsThatRow() {
        val thrown =
            assertFailsWith<AssertionError> {
                terminatingTheSessionKeepsTheWorkerUntilEveryDependentSettles(
                    OwnershipMake { g, d, l -> RetiringOwnership(g, d, l) },
                )
            }
        assertTrue(
            thrown.message.orEmpty().contains("must still be alive"),
            "the stand-in must fail at the worker/dispatcher lifetime assertion, not on a different total: ${thrown.message}",
        )
    }

    /** A retention that runs [before] on the way in, so a close can land between the two acquisitions. */
    private class HookedRetention(private val inner: WorkerRetention, private val before: () -> Unit) : WorkerRetention {
        private val armed = AtomicBoolean(true)

        override fun retainWorker(): Boolean {
            if (armed.compareAndSet(true, false)) before()
            return inner.retainWorker()
        }

        override fun releaseWorker() = inner.releaseWorker()
    }

    /**
     * The ownership closes exactly between the two acquisitions a transfer
     * needs. The worker is gone, so the transfer must not take a codec lease,
     * must not touch a buffer, and must not give back a retention it never got.
     */
    private fun aWorkerRetiredBetweenTheTwoAcquisitionsRefusesTheTransfer(ownerships: Ownerships) {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val ownership = ownerships.make(GENERATION, rig.dispatcher, LateLog())
        val hooked = HookedRetention(ownership) { ownership.close(null) }
        val codec = HostCodec(rig.order, dispatcher = null, worker = hooked)
        ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order) }
        ownership.acquire(CaptureResource.VideoEncoder) { HostCodecHandle(codec, "codec-handle", rig.order) }
        val handoff = ConformingHandoff(MediaKind.Video, GENERATION, rig.buffers, rig.releases, rig.ledger, rig.faults, rig.capacity, codec)
        val out = rig.stage(index = 0, pts = 0L)

        val outcome = handoff.onOutput(GENERATION, out)

        assertIs<HandoffOutcome.Refused>(outcome, "there is no worker left to deliver this transfer on")
        assertEquals(emptyList(), rig.buffers.fetched(), "so no buffer was fetched")
        assertEquals(emptyList(), rig.releases.indices(), "and no index was handed back")
        assertEquals(0, codec.outstandingLeases, "no lease was taken on a transfer that cannot be delivered")
        assertDependents(ownership, 0, "and no retention was released that was never acquired")
        assertTrue(!rig.dispatcher.isAlive, "the worker really was retired: its dispatcher is quit")
        assertEquals(
            listOf("close:codec-handle", "dispose:codec", "close:worker", "quit:dispatcher"),
            rig.orderNow(),
            "and the close that ran in that window still disposed the codec before the worker it ran on",
        )
    }

    @Test
    fun aWorkerRetiredBetweenTheTwoAcquisitionsRefusesTheTransfer_conforming() = aWorkerRetiredBetweenTheTwoAcquisitionsRefusesTheTransfer(conformingOwnership)

    @Test
    fun aWorkerRetiredBetweenTheTwoAcquisitionsRefusesTheTransfer_ordered() = aWorkerRetiredBetweenTheTwoAcquisitionsRefusesTheTransfer(orderedOwnership)

    /**
     * The last transfer settles while the ownership is closing -- after the
     * codec's handle has been closed, but BEFORE that handle has asked for the
     * disposal. Close intent is not the close boundary: the worker waits.
     */
    private fun theWorkerWaitsForTheDisposalItsCloseAskedFor(ownerships: Ownerships) {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val ownership = ownerships.make(GENERATION, rig.dispatcher, LateLog())
        val codec = HostCodec(rig.order, dispatcher = null, worker = ownership)
        val inRelease = HostGate("the transfer reached its index release")
        val proceed = HostGate("the row released the parked transfer")
        val schedule = ParkEach(mapOf("transfer" to inRelease), emptyMap(), proceed)
        val handoff = ConformingHandoff(MediaKind.Video, GENERATION, rig.buffers, rig.releases, rig.ledger, rig.faults, rig.capacity, codec, schedule)
        val worker = HostWorker("transfer") { handoff.onOutput(GENERATION, rig.stage(index = 0, pts = 0L)) }.start()
        ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order) }
        // The last transfer settles INSIDE the codec handle's close, before the
        // disposal that close asks for.
        ownership.acquire(CaptureResource.VideoEncoder) {
            HostCodecHandle(codec, "codec-handle", rig.order) {
                proceed.openFully()
                worker.join()
            }
        }
        withSettlement(listOf(inRelease, proceed), listOf(worker)) {
            inRelease.await(worker)
            handoff.close(CloseMode.Failed)
            ownership.close(null)
            assertEquals(
                listOf("close:codec-handle", "release:0", "dispose:codec", "close:worker", "quit:dispatcher"),
                rig.orderNow(),
                "the codec is destroyed before the worker its disposal ran on is retired",
            )
            assertEquals(1, codec.disposalCount, "destroyed exactly once")
            assertTrue(!rig.dispatcher.isAlive, "and only then is the dispatcher quit")
            assertDependents(ownership, 0)
        }
    }

    @Test
    fun theWorkerWaitsForTheDisposalItsCloseAskedFor_conforming() = theWorkerWaitsForTheDisposalItsCloseAskedFor(conformingOwnership)

    @Test
    fun theWorkerWaitsForTheDisposalItsCloseAskedFor_ordered() = theWorkerWaitsForTheDisposalItsCloseAskedFor(orderedOwnership)

    /** A dependent refused its retention takes nothing and gives nothing back. */
    @Test
    fun aRefusedWorkerRetentionRefusesTheTransferWithoutTouchingTheCodec() {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val refusing =
            object : WorkerRetention {
                var released = 0
                    private set

                override fun retainWorker(): Boolean = false

                override fun releaseWorker() {
                    released++
                }
            }
        val codec = HostCodec(rig.order, dispatcher = null, worker = refusing)
        val handoff = ConformingHandoff(MediaKind.Video, GENERATION, rig.buffers, rig.releases, rig.ledger, rig.faults, rig.capacity, codec)
        assertIs<HandoffOutcome.Refused>(handoff.onOutput(GENERATION, rig.stage(index = 0, pts = 0L)))
        assertEquals(0, codec.outstandingLeases, "no lease")
        assertEquals(0, refusing.released, "and nothing released that was never taken")
        assertEquals(emptyList(), rig.buffers.fetched())
        assertEquals(emptyList(), rig.releases.indices())
    }

    /** A handle that records its attempt into the shared order log, then returns. */
    private class LoggedHandle(override val identity: Any, private val order: MutableList<String>) : ResourceHandle {
        override fun close() {
            synchronized(order) { order.add("close:$identity") }
        }
    }

    /** A handle that records its attempt and then raises what it was given. */
    private class RaisingHandle(
        override val identity: Any,
        private val order: MutableList<String>,
        private val raise: () -> Throwable,
    ) : ResourceHandle {
        override fun close() {
            synchronized(order) { order.add("close:$identity") }
            throw raise()
        }
    }

    /**
     * One resource's close raises. Every other close still happens, the worker
     * is still retired, and the cancellation still leaves -- by identity, and
     * only once the cleanup it interrupted has finished.
     */
    private fun aCancellingCloseDoesNotAbandonTheRestOfTheCloseOrDispatcher(ownerships: Ownerships) {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val ownership = ownerships.make(GENERATION, rig.dispatcher, LateLog())
        val sentinel = CancellationException("cancelled while closing the codec")
        ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order) }
        ownership.acquire(CaptureResource.Microphone) { LoggedHandle("microphone", rig.order) }
        ownership.acquire(CaptureResource.VideoEncoder) { RaisingHandle("codec-handle", rig.order) { sentinel } }

        val thrown = assertFailsWith<CancellationException> { ownership.close(null) }

        assertSame(sentinel, thrown, "the cancellation leaves by identity")
        assertEquals(
            listOf("close:codec-handle", "close:microphone", "close:worker", "quit:dispatcher"),
            rig.orderNow(),
            "every handle behind the one that threw was still closed, in reverse acquisition order",
        )
        assertTrue(!rig.dispatcher.isAlive, "the worker really was retired")
        assertDependents(ownership, 0)
        assertNull(ownership.close(null), "closing again closes nothing again")
        assertEquals(4, rig.orderNow().size, "and attempts nothing again")
    }

    @Test
    fun aCancellingCloseDoesNotAbandonTheRestOfTheCloseOrDispatcher_conforming() = aCancellingCloseDoesNotAbandonTheRestOfTheCloseOrDispatcher(conformingOwnership)

    @Test
    fun aCancellingCloseDoesNotAbandonTheRestOfTheCloseOrDispatcher_ordered() = aCancellingCloseDoesNotAbandonTheRestOfTheCloseOrDispatcher(orderedOwnership)

    /** The same, with a dependent holding the worker across that close. */
    private fun aCancellingCloseStillLeavesTheRetainedWorkerReleasable(ownerships: Ownerships) {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val ownership = ownerships.make(GENERATION, rig.dispatcher, LateLog())
        val sentinel = CancellationException("cancelled while closing the codec")
        ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order) }
        ownership.acquire(CaptureResource.Microphone) { LoggedHandle("microphone", rig.order) }
        ownership.acquire(CaptureResource.VideoEncoder) { RaisingHandle("codec-handle", rig.order) { sentinel } }
        assertTrue(ownership.retainWorker(), "a dependent is holding the worker across this close")

        val thrown = assertFailsWith<CancellationException> { ownership.close(null) }

        assertSame(sentinel, thrown)
        assertEquals(
            listOf("close:codec-handle", "close:microphone"),
            rig.orderNow(),
            "the retained worker is not closed while its dependent is outstanding",
        )
        assertTrue(rig.dispatcher.isAlive, "so its dispatcher is still alive")
        assertDependents(ownership, 1)

        ownership.releaseWorker()

        assertEquals(
            listOf("close:codec-handle", "close:microphone", "close:worker", "quit:dispatcher"),
            rig.orderNow(),
            "the last dependent settling retires the worker the interrupted close left behind",
        )
        assertTrue(!rig.dispatcher.isAlive)
        assertDependents(ownership, 0)
        assertNull(ownership.close(null), "and a repeated close is still a no-op")
    }

    @Test
    fun aCancellingCloseStillLeavesTheRetainedWorkerReleasable_conforming() = aCancellingCloseStillLeavesTheRetainedWorkerReleasable(conformingOwnership)

    @Test
    fun aCancellingCloseStillLeavesTheRetainedWorkerReleasable_ordered() = aCancellingCloseStillLeavesTheRetainedWorkerReleasable(orderedOwnership)

    /** A later ordinary failure cannot vanish behind the cancellation that propagates. */
    private fun aLaterCloseFailureTravelsWithTheCancellation(ownerships: Ownerships) {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val ownership = ownerships.make(GENERATION, rig.dispatcher, LateLog())
        val sentinel = CancellationException("cancelled while closing the codec")
        val ordinary = IllegalStateException("the microphone close failed")
        ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order) }
        ownership.acquire(CaptureResource.Microphone) { RaisingHandle("microphone", rig.order) { ordinary } }
        ownership.acquire(CaptureResource.VideoEncoder) { RaisingHandle("codec-handle", rig.order) { sentinel } }

        val thrown = assertFailsWith<CancellationException> { ownership.close(null) }

        assertSame(sentinel, thrown)
        assertTrue(
            thrown.suppressed.any { it === ordinary },
            "the later close failure travels with it, by identity: ${thrown.suppressed.toList()}",
        )
        assertEquals(listOf("close:codec-handle", "close:microphone", "close:worker", "quit:dispatcher"), rig.orderNow())
        assertTrue(!rig.dispatcher.isAlive)
    }

    @Test
    fun aLaterCloseFailureTravelsWithTheCancellation_conforming() = aLaterCloseFailureTravelsWithTheCancellation(conformingOwnership)

    @Test
    fun aLaterCloseFailureTravelsWithTheCancellation_ordered() = aLaterCloseFailureTravelsWithTheCancellation(orderedOwnership)

    /** An Error from a close obeys the same all-closes contract. */
    private fun anErrorFromACloseStillFinishesTheCloseAndPropagatesByIdentity(ownerships: Ownerships) {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val ownership = ownerships.make(GENERATION, rig.dispatcher, LateLog())
        val sentinel = CloseSentinelError()
        val ordinary = IllegalStateException("the microphone close failed")
        ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order) }
        ownership.acquire(CaptureResource.Microphone) { RaisingHandle("microphone", rig.order) { ordinary } }
        ownership.acquire(CaptureResource.VideoEncoder) { RaisingHandle("codec-handle", rig.order) { sentinel } }

        val thrown = assertFailsWith<CloseSentinelError> { ownership.close(null) }

        assertSame(sentinel, thrown, "the Error leaves unconverted, by identity")
        assertTrue(thrown.suppressed.any { it === ordinary }, "carrying what the rest of the close found")
        assertEquals(listOf("close:codec-handle", "close:microphone", "close:worker", "quit:dispatcher"), rig.orderNow())
        assertTrue(!rig.dispatcher.isAlive)
        assertDependents(ownership, 0)
    }

    @Test
    fun anErrorFromACloseStillFinishesTheCloseAndPropagatesByIdentity_conforming() = anErrorFromACloseStillFinishesTheCloseAndPropagatesByIdentity(conformingOwnership)

    @Test
    fun anErrorFromACloseStillFinishesTheCloseAndPropagatesByIdentity_ordered() = anErrorFromACloseStillFinishesTheCloseAndPropagatesByIdentity(orderedOwnership)

    /** A late settlement's carried handle cannot skip a worker release it already owes. */
    private fun aCarriedHandleThatThrowsStillReleasesTheOwedWorker(ownerships: Ownerships) {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val ownership = ownerships.make(GENERATION, rig.dispatcher, LateLog())
        val sentinel = CancellationException("cancelled while closing the carried camera")
        ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order) }
        val ticket = ownership.request(CaptureResource.Camera)
        assertNull(ownership.close(null), "the worker is retained for the acquisition still in flight")
        assertEquals(emptyList(), rig.orderNow())

        val thrown =
            assertFailsWith<CancellationException> {
                ownership.fail(ticket, "the camera never opened", RaisingHandle("camera", rig.order) { sentinel })
            }

        assertSame(sentinel, thrown, "the cancellation leaves by identity")
        assertEquals(
            listOf("close:camera", "close:worker", "quit:dispatcher"),
            rig.orderNow(),
            "and the worker this settlement owed was released anyway, after the carried handle",
        )
        assertTrue(!rig.dispatcher.isAlive)
        assertEquals(0, ownership.pending)
    }

    @Test
    fun aCarriedHandleThatThrowsStillReleasesTheOwedWorker_conforming() = aCarriedHandleThatThrowsStillReleasesTheOwedWorker(conformingOwnership)

    @Test
    fun aCarriedHandleThatThrowsStillReleasesTheOwedWorker_ordered() = aCarriedHandleThatThrowsStillReleasesTheOwedWorker(orderedOwnership)

    /**
     * `ResourceHandle.close` does not say which exceptions it may raise, and
     * Kotlin will throw a checked one straight out of it. The cleanup has to
     * survive that exactly as it survives a cancellation.
     */
    private fun checkedCloseFinishesTheCleanup(ownerships: Ownerships, retained: Boolean) {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val ownership = ownerships.make(GENERATION, rig.dispatcher, LateLog())
        val sentinel = java.io.IOException("the codec close failed")
        ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order) }
        ownership.acquire(CaptureResource.Microphone) { LoggedHandle("microphone", rig.order) }
        ownership.acquire(CaptureResource.VideoEncoder) { RaisingHandle("codec-handle", rig.order) { sentinel } }
        if (retained) assertTrue(ownership.retainWorker(), "a dependent is holding the worker across this close")

        // A checked exception is an ORDINARY failure: it keeps the declared
        // return contract rather than being thrown.
        val returned = ownership.close(null)

        assertSame(sentinel, returned, "the checked failure is returned, by identity")
        val expected =
            if (retained) {
                listOf("close:codec-handle", "close:microphone")
            } else {
                listOf("close:codec-handle", "close:microphone", "close:worker", "quit:dispatcher")
            }
        assertEquals(expected, rig.orderNow(), "every handle behind the one that threw was still attempted")
        if (retained) {
            assertTrue(rig.dispatcher.isAlive, "the retained worker waits for its dependent")
            assertDependents(ownership, 1)
            ownership.releaseWorker()
            assertEquals(
                listOf("close:codec-handle", "close:microphone", "close:worker", "quit:dispatcher"),
                rig.orderNow(),
                "and is retired when that dependent settles",
            )
        }
        assertTrue(!rig.dispatcher.isAlive, "the worker really was retired")
        assertDependents(ownership, 0)
        assertNull(ownership.close(null), "closing again closes nothing again")
        assertEquals(4, rig.orderNow().size, "and attempts nothing again")
    }

    private fun aCheckedCloseFailureDoesNotAbandonTheCleanup(ownerships: Ownerships) = checkedCloseFinishesTheCleanup(ownerships, retained = false)

    private fun aCheckedCloseFailureLeavesTheRetainedWorkerReleasable(ownerships: Ownerships) = checkedCloseFinishesTheCleanup(ownerships, retained = true)

    @Test
    fun aCheckedCloseFailureLeavesTheRetainedWorkerReleasable_conforming() = aCheckedCloseFailureLeavesTheRetainedWorkerReleasable(conformingOwnership)

    @Test
    fun aCheckedCloseFailureLeavesTheRetainedWorkerReleasable_ordered() = aCheckedCloseFailureLeavesTheRetainedWorkerReleasable(orderedOwnership)

    private enum class LatePath { Adopt, Fail }

    /**
     * A late settlement where the carried handle fails ORDINARILY and the
     * worker's own close then raises a terminal. The terminal is what leaves,
     * by identity; the ordinary failure rides along instead of hiding it.
     */
    private fun lateSettlementCarriesItsTerminal(ownerships: Ownerships, path: LatePath, terminal: Throwable?) {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val late = LateLog()
        val ownership = ownerships.make(GENERATION, rig.dispatcher, late)
        val ordinary = IllegalStateException("the carried camera close failed")
        ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order, raise = terminal?.let { { it } }) }
        val ticket = ownership.request(CaptureResource.Camera)
        assertNull(ownership.close(null), "the worker is retained for the acquisition still in flight")
        val carried = RaisingHandle("camera", rig.order) { ordinary }

        val settle = { if (path == LatePath.Adopt) ownership.adopt(ticket, carried) else ownership.fail(ticket, "never opened", carried) }
        var returned: AdoptOutcome? = null
        // Nothing is asserted inside this try: an AssertionError is an Error,
        // and the arms below would catch the row's own verdict.
        val raised =
            try {
                returned = settle()
                null
            } catch (cancelled: CancellationException) {
                cancelled
            } catch (error: Error) {
                error
            }
        if (terminal != null && raised == null) {
            throw AssertionError("a terminal cleanup failure must not be swallowed: $path returned $returned")
        }
        if (terminal == null && raised != null) throw raised

        assertEquals(
            listOf("close:camera", "close:worker", "quit:dispatcher"),
            rig.orderNow(),
            "both handles were attempted, in order, whatever they raised",
        )
        assertTrue(!rig.dispatcher.isAlive, "the worker this settlement owed was really retired")
        assertEquals(0, ownership.pending, "no ticket is left outstanding")
        assertDependents(ownership, 0)
        if (terminal == null) {
            assertNull(raised, "an ordinary-only late failure is reported, not thrown")
            assertTrue(
                late.all().any { it.startsWith("late-close:Camera:") },
                "and it is what the sink was told: ${late.all()}",
            )
            return
        }
        val thrown = assertNotNull(raised, "the terminal failure leaves")
        assertSame(terminal, thrown, "the terminal that leaves is the one the worker's close raised, by identity")
        assertTrue(
            thrown.suppressed.any { it === ordinary },
            "carrying the earlier ordinary failure, by identity: ${thrown.suppressed.toList()}",
        )
        assertTrue(thrown.suppressed.none { it === thrown }, "and nothing suppressed itself")
    }

    @Test
    fun aCheckedCloseFailureDoesNotAbandonTheCleanup_conforming() = aCheckedCloseFailureDoesNotAbandonTheCleanup(conformingOwnership)

    @Test
    fun aCheckedCloseFailureDoesNotAbandonTheCleanup_ordered() = aCheckedCloseFailureDoesNotAbandonTheCleanup(orderedOwnership)

    private fun aLateFailPropagatesACancellationBehindAnOrdinaryFailure(ownerships: Ownerships) {
        val sentinel = CancellationException("cancelled closing the worker")
        lateSettlementCarriesItsTerminal(ownerships, LatePath.Fail, sentinel)
    }

    @Test
    fun aLateFailPropagatesACancellationBehindAnOrdinaryFailure_conforming() = aLateFailPropagatesACancellationBehindAnOrdinaryFailure(conformingOwnership)

    @Test
    fun aLateFailPropagatesACancellationBehindAnOrdinaryFailure_ordered() = aLateFailPropagatesACancellationBehindAnOrdinaryFailure(orderedOwnership)

    private fun aLateAdoptPropagatesACancellationBehindAnOrdinaryFailure(ownerships: Ownerships) {
        val sentinel = CancellationException("cancelled closing the worker")
        lateSettlementCarriesItsTerminal(ownerships, LatePath.Adopt, sentinel)
    }

    @Test
    fun aLateAdoptPropagatesACancellationBehindAnOrdinaryFailure_conforming() = aLateAdoptPropagatesACancellationBehindAnOrdinaryFailure(conformingOwnership)

    @Test
    fun aLateAdoptPropagatesACancellationBehindAnOrdinaryFailure_ordered() = aLateAdoptPropagatesACancellationBehindAnOrdinaryFailure(orderedOwnership)

    private fun aLateFailPropagatesAnErrorBehindAnOrdinaryFailure(ownerships: Ownerships) {
        val sentinel = CloseSentinelError()
        lateSettlementCarriesItsTerminal(ownerships, LatePath.Fail, sentinel)
    }

    @Test
    fun aLateFailPropagatesAnErrorBehindAnOrdinaryFailure_conforming() = aLateFailPropagatesAnErrorBehindAnOrdinaryFailure(conformingOwnership)

    @Test
    fun aLateFailPropagatesAnErrorBehindAnOrdinaryFailure_ordered() = aLateFailPropagatesAnErrorBehindAnOrdinaryFailure(orderedOwnership)

    private fun aLateAdoptPropagatesAnErrorBehindAnOrdinaryFailure(ownerships: Ownerships) {
        val sentinel = CloseSentinelError()
        lateSettlementCarriesItsTerminal(ownerships, LatePath.Adopt, sentinel)
    }

    @Test
    fun aLateAdoptPropagatesAnErrorBehindAnOrdinaryFailure_conforming() = aLateAdoptPropagatesAnErrorBehindAnOrdinaryFailure(conformingOwnership)

    @Test
    fun aLateAdoptPropagatesAnErrorBehindAnOrdinaryFailure_ordered() = aLateAdoptPropagatesAnErrorBehindAnOrdinaryFailure(orderedOwnership)

    private fun anOrdinaryOnlyLateFailureIsStillOnlyReported(ownerships: Ownerships) {
        lateSettlementCarriesItsTerminal(ownerships, LatePath.Fail, terminal = null)
        lateSettlementCarriesItsTerminal(ownerships, LatePath.Adopt, terminal = null)
    }

    @Test
    fun anOrdinaryOnlyLateFailureIsStillOnlyReported_conforming() = anOrdinaryOnlyLateFailureIsStillOnlyReported(conformingOwnership)

    @Test
    fun anOrdinaryOnlyLateFailureIsStillOnlyReported_ordered() = anOrdinaryOnlyLateFailureIsStillOnlyReported(orderedOwnership)

    /** The caller's own cancellation is the original one; a later cleanup one does not replace it. */
    private fun aCallerCancellationOutranksACleanupCancellation(ownerships: Ownerships) {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val ownership = ownerships.make(GENERATION, rig.dispatcher, LateLog())
        val original = CancellationException("the capture scope was cancelled")
        val fromCleanup = CancellationException("cancelled closing the codec")
        ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order) }
        ownership.acquire(CaptureResource.VideoEncoder) { RaisingHandle("codec-handle", rig.order) { fromCleanup } }

        val thrown = assertFailsWith<CancellationException> { ownership.close(original) }

        assertSame(original, thrown, "the ORIGINAL cancellation is what leaves")
        assertTrue(thrown.suppressed.any { it === fromCleanup }, "carrying the one the cleanup raised: ${thrown.suppressed.toList()}")
        assertEquals(listOf("close:codec-handle", "close:worker", "quit:dispatcher"), rig.orderNow())
        assertTrue(!rig.dispatcher.isAlive)
    }

    @Test
    fun aCallerCancellationOutranksACleanupCancellation_conforming() = aCallerCancellationOutranksACleanupCancellation(conformingOwnership)

    @Test
    fun aCallerCancellationOutranksACleanupCancellation_ordered() = aCallerCancellationOutranksACleanupCancellation(orderedOwnership)

    /** A sink that raises when one named resource is reported, as application code may. */
    private class RaisingLateLog(private val raiseFor: CaptureResource, private val raise: () -> Throwable) : LateCleanupSink {
        private val events = mutableListOf<String>()

        @Synchronized
        override fun onLateClose(generation: Long, resource: CaptureResource, failure: Exception?) {
            events.add("late-close:$resource")
            if (resource == raiseFor) throw raise()
        }

        @Synchronized
        override fun onForeignDisposal(generation: Long, resource: CaptureResource, failure: Exception?) {
            events.add("foreign:$resource")
        }

        @Synchronized
        fun all(): List<String> = events.toList()
    }

    /**
     * A late settlement's carried close is itself a worker dependency.
     *
     * The ticket stops protecting the worker the moment it is removed, so a
     * transfer finishing WHILE the carried close runs would otherwise retire
     * the worker out from under it. The row forces exactly that: the last
     * transfer's retention is released from inside the carried close.
     */
    private fun lateCleanupHoldsTheWorkerThroughItsOwnClose(
        ownerships: Ownerships,
        path: LatePath,
        carriedRaise: (() -> Throwable)?,
        sink: LateCleanupSink = LateLog(),
    ): Throwable? {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val ownership = ownerships.make(GENERATION, rig.dispatcher, sink)
        ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order) }
        val ticket = ownership.request(CaptureResource.Camera)
        assertTrue(ownership.retainWorker(), "one admitted transfer is holding the worker")
        assertNull(ownership.close(null), "the worker is retained for both dependents")
        assertEquals(emptyList(), rig.orderNow())

        val observed = mutableListOf<String>()
        val carried =
            object : ResourceHandle {
                override val identity: Any = "camera"

                override fun close() {
                    synchronized(rig.order) { rig.order.add("close:camera-enter") }
                    // The last transfer finishes HERE, inside the carried close.
                    ownership.releaseWorker()
                    synchronized(observed) { observed.add("alive-during-carried-close=${rig.dispatcher.isAlive}") }
                    synchronized(rig.order) { rig.order.add("close:camera-exit") }
                    carriedRaise?.let { throw it() }
                }
            }

        var returned: AdoptOutcome? = null
        val raised =
            try {
                returned = if (path == LatePath.Adopt) ownership.adopt(ticket, carried) else ownership.fail(ticket, "never opened", carried)
                null
            } catch (cancelled: CancellationException) {
                cancelled
            } catch (failure: RuntimeException) {
                failure
            } catch (error: Error) {
                error
            }

        assertEquals(
            listOf("alive-during-carried-close=true"),
            observed,
            "the worker the carried close runs against must still be alive when a transfer settles inside it",
        )
        assertEquals(
            listOf("close:camera-enter", "close:camera-exit", "close:worker", "quit:dispatcher"),
            rig.orderNow(),
            "and it is retired exactly once, after that close has exited ($path, returned $returned)",
        )
        assertTrue(!rig.dispatcher.isAlive, "only then is the dispatcher quit")
        assertEquals(0, ownership.pending, "no ticket is left outstanding")
        assertDependents(ownership, 0, "and no protection is left behind")
        return raised
    }

    private fun aLateFailHoldsTheWorkerThroughItsCarriedClose(ownerships: Ownerships) {
        assertNull(lateCleanupHoldsTheWorkerThroughItsOwnClose(ownerships, LatePath.Fail, carriedRaise = null))
    }

    @Test
    fun aLateFailHoldsTheWorkerThroughItsCarriedClose_conforming() = aLateFailHoldsTheWorkerThroughItsCarriedClose(conformingOwnership)

    @Test
    fun aLateFailHoldsTheWorkerThroughItsCarriedClose_ordered() = aLateFailHoldsTheWorkerThroughItsCarriedClose(orderedOwnership)

    private fun aLateAdoptHoldsTheWorkerThroughItsCarriedClose(ownerships: Ownerships) {
        assertNull(lateCleanupHoldsTheWorkerThroughItsOwnClose(ownerships, LatePath.Adopt, carriedRaise = null))
    }

    @Test
    fun aLateAdoptHoldsTheWorkerThroughItsCarriedClose_conforming() = aLateAdoptHoldsTheWorkerThroughItsCarriedClose(conformingOwnership)

    @Test
    fun aLateAdoptHoldsTheWorkerThroughItsCarriedClose_ordered() = aLateAdoptHoldsTheWorkerThroughItsCarriedClose(orderedOwnership)

    private fun aCancellingCarriedCloseDoesNotLeakThatProtection(ownerships: Ownerships) {
        val sentinel = CancellationException("cancelled closing the carried camera")
        val raised = lateCleanupHoldsTheWorkerThroughItsOwnClose(ownerships, LatePath.Fail, carriedRaise = { sentinel })
        assertSame(sentinel, assertNotNull(raised), "the cancellation still leaves, by identity")
    }

    @Test
    fun aCancellingCarriedCloseDoesNotLeakThatProtection_conforming() = aCancellingCarriedCloseDoesNotLeakThatProtection(conformingOwnership)

    @Test
    fun aCancellingCarriedCloseDoesNotLeakThatProtection_ordered() = aCancellingCarriedCloseDoesNotLeakThatProtection(orderedOwnership)

    private fun anErrorFromTheCarriedCloseDoesNotLeakThatProtection(ownerships: Ownerships) {
        val sentinel = CloseSentinelError()
        val raised = lateCleanupHoldsTheWorkerThroughItsOwnClose(ownerships, LatePath.Adopt, carriedRaise = { sentinel })
        assertSame(sentinel, assertNotNull(raised), "the Error still leaves, by identity")
    }

    @Test
    fun anErrorFromTheCarriedCloseDoesNotLeakThatProtection_conforming() = anErrorFromTheCarriedCloseDoesNotLeakThatProtection(conformingOwnership)

    @Test
    fun anErrorFromTheCarriedCloseDoesNotLeakThatProtection_ordered() = anErrorFromTheCarriedCloseDoesNotLeakThatProtection(orderedOwnership)

    /** Even a sink that raises out of the report cannot strand that protection. */
    private fun aReportThatThrowsStillReleasesTheCleanupProtection(ownerships: Ownerships) {
        val sentinel = IllegalStateException("the late-cleanup sink failed")
        val sink = RaisingLateLog(CaptureResource.Camera) { sentinel }
        val raised = lateCleanupHoldsTheWorkerThroughItsOwnClose(ownerships, LatePath.Fail, carriedRaise = null, sink = sink)
        assertSame(sentinel, assertNotNull(raised), "what the sink raised is what leaves")
        assertTrue(sink.all().contains("late-close:Worker"), "and the worker was still closed and reported: ${sink.all()}")
    }

    @Test
    fun aReportThatThrowsStillReleasesTheCleanupProtection_conforming() = aReportThatThrowsStillReleasesTheCleanupProtection(conformingOwnership)

    @Test
    fun aReportThatThrowsStillReleasesTheCleanupProtection_ordered() = aReportThatThrowsStillReleasesTheCleanupProtection(orderedOwnership)

    /** A sink that raises for named resources, and records what it was told. */
    private class ScriptedLateLog(private val raiseFor: Map<CaptureResource, () -> Throwable>) : LateCleanupSink {
        private val events = mutableListOf<String>()

        @Synchronized
        override fun onLateClose(generation: Long, resource: CaptureResource, failure: Exception?) {
            events.add("late-close:$resource" + (failure?.let { ":" + it.message } ?: ""))
            raiseFor[resource]?.let { throw it() }
        }

        @Synchronized
        override fun onForeignDisposal(generation: Long, resource: CaptureResource, failure: Exception?) {
            events.add("foreign:$resource")
        }

        @Synchronized
        fun all(): List<String> = events.toList()
    }

    /**
     * An INSTALLED resource that is lost -- `onDisconnected` after `onOpened` --
     * is detached under the lock and closed outside it. That close is a worker
     * dependency too: an ownership close overlapping it must not retire the
     * worker until it has exited.
     */
    private fun lostResourceHoldsTheWorkerThroughItsClose(ownerships: Ownerships, carriedRaise: (() -> Throwable)?): Throwable? {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val ownership = ownerships.make(GENERATION, rig.dispatcher, LateLog())
        ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order) }
        val ticket = ownership.request(CaptureResource.Camera)
        val observed = mutableListOf<String>()
        val camera =
            object : ResourceHandle {
                override val identity: Any = "camera"

                override fun close() {
                    synchronized(rig.order) { rig.order.add("close:camera-enter") }
                    // The owner gives up WHILE the lost resource is closing.
                    ownership.close(null)
                    synchronized(observed) { observed.add("alive-during-lost-close=${rig.dispatcher.isAlive}") }
                    synchronized(rig.order) { rig.order.add("close:camera-exit") }
                    carriedRaise?.let { throw it() }
                }
            }
        assertEquals(AdoptOutcome.Adopted, ownership.adopt(ticket, camera), "the camera opened and is installed")

        var outcome: LostOutcome? = null
        val raised =
            try {
                outcome = ownership.lost(CaptureResource.Camera, "disconnected")
                null
            } catch (cancelled: CancellationException) {
                cancelled
            } catch (failure: RuntimeException) {
                failure
            } catch (error: Error) {
                error
            }

        assertEquals(
            listOf("alive-during-lost-close=true"),
            observed,
            "the worker must stay alive until the lost resource's close has exited",
        )
        assertEquals(
            listOf("close:camera-enter", "close:camera-exit", "close:worker", "quit:dispatcher"),
            rig.orderNow(),
            "and is retired exactly once, after it (returned $outcome)",
        )
        assertTrue(!rig.dispatcher.isAlive, "only then is the dispatcher quit")
        assertDependents(ownership, 0, "no protection is left behind")
        return raised
    }

    private fun aLostResourceHoldsTheWorkerThroughItsClose(ownerships: Ownerships) {
        assertNull(lostResourceHoldsTheWorkerThroughItsClose(ownerships, carriedRaise = null))
    }

    @Test
    fun aLostResourceHoldsTheWorkerThroughItsClose_conforming() = aLostResourceHoldsTheWorkerThroughItsClose(conformingOwnership)

    @Test
    fun aLostResourceHoldsTheWorkerThroughItsClose_ordered() = aLostResourceHoldsTheWorkerThroughItsClose(orderedOwnership)

    private fun aThrowingLostCloseDoesNotLeakThatProtection(ownerships: Ownerships) {
        val sentinel = CancellationException("cancelled closing the lost camera")
        val raised = lostResourceHoldsTheWorkerThroughItsClose(ownerships, carriedRaise = { sentinel })
        assertSame(sentinel, assertNotNull(raised), "the cancellation still leaves, by identity")
    }

    @Test
    fun aThrowingLostCloseDoesNotLeakThatProtection_conforming() = aThrowingLostCloseDoesNotLeakThatProtection(conformingOwnership)

    @Test
    fun aThrowingLostCloseDoesNotLeakThatProtection_ordered() = aThrowingLostCloseDoesNotLeakThatProtection(orderedOwnership)

    /** Losing the same resource twice loses nothing twice, and neither does closing again. */
    private fun aRepeatedLossOrCloseAttemptsNothingAgain(ownerships: Ownerships) {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val ownership = ownerships.make(GENERATION, rig.dispatcher, LateLog())
        ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order) }
        val ticket = ownership.request(CaptureResource.Camera)
        assertEquals(AdoptOutcome.Adopted, ownership.adopt(ticket, LoggedHandle("camera", rig.order)))

        assertIs<LostOutcome.Released>(ownership.lost(CaptureResource.Camera, "disconnected"))
        assertEquals(listOf("close:camera"), rig.orderNow())
        assertIs<LostOutcome.Ignored>(ownership.lost(CaptureResource.Camera, "again"), "it is no longer installed")
        assertEquals(listOf("close:camera"), rig.orderNow(), "and nothing was attempted again")
        assertDependents(ownership, 0)

        assertNull(ownership.close(null))
        assertEquals(listOf("close:camera", "close:worker", "quit:dispatcher"), rig.orderNow(), "the lost resource is not closed twice")
        assertNull(ownership.close(null), "closing again closes nothing again")
        assertEquals(3, rig.orderNow().size)
    }

    @Test
    fun aRepeatedLossOrCloseAttemptsNothingAgain_conforming() = aRepeatedLossOrCloseAttemptsNothingAgain(conformingOwnership)

    @Test
    fun aRepeatedLossOrCloseAttemptsNothingAgain_ordered() = aRepeatedLossOrCloseAttemptsNothingAgain(orderedOwnership)

    /**
     * A report that raises must not stop the transaction from reaching its
     * decision: what the close raised would otherwise vanish behind it.
     *
     * Every exception here is created fresh by the caller for THIS arm, and the
     * whole suppressed sequence is declared by identity and order.
     */
    private fun reportFailureDoesNotHideTheCloseFailure(
        ownerships: Ownerships,
        path: LatePath,
        cameraRaise: Throwable?,
        workerRaise: Throwable?,
        cameraSink: Throwable?,
        workerSink: Throwable?,
        expectedPrimary: Throwable,
        expectedSuppressed: List<Throwable>,
    ) {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val script = mutableMapOf<CaptureResource, () -> Throwable>()
        cameraSink?.let { script[CaptureResource.Camera] = { it } }
        workerSink?.let { script[CaptureResource.Worker] = { it } }
        val sink = ScriptedLateLog(script)
        val ownership = ownerships.make(GENERATION, rig.dispatcher, sink)
        ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order, raise = workerRaise?.let { { it } }) }
        val ticket = ownership.request(CaptureResource.Camera)
        assertTrue(ownership.retainWorker(), "an admitted transfer is holding the worker")
        assertNull(ownership.close(null))
        ownership.releaseWorker()
        val camera =
            if (cameraRaise == null) LoggedHandle("camera", rig.order) else RaisingHandle("camera", rig.order) { cameraRaise }

        var returned: AdoptOutcome? = null
        val raised =
            try {
                returned = if (path == LatePath.Adopt) ownership.adopt(ticket, camera) else ownership.fail(ticket, "never opened", camera)
                null
            } catch (cancelled: CancellationException) {
                cancelled
            } catch (failure: RuntimeException) {
                failure
            } catch (error: Error) {
                error
            }

        val thrown = assertNotNull(raised, "something must leave: $path returned $returned")
        assertSame(expectedPrimary, thrown, "the failure that leaves is the one the contract selects, by identity")
        val suppressed = thrown.suppressed.toList()
        assertEquals(expectedSuppressed.size, suppressed.size, "exactly the expected evidence travels with it: $suppressed")
        expectedSuppressed.forEachIndexed { at, expected ->
            assertSame(expected, suppressed[at], "suppressed[$at], in occurrence order")
        }
        assertEquals(
            listOf("close:camera", "close:worker", "quit:dispatcher"),
            rig.orderNow(),
            "exactly one close per resource, in order",
        )
        assertEquals(
            listOf("late-close:Camera" + (cameraRaise?.let { ":" + it.message } ?: ""), "late-close:Worker"),
            sink.all(),
            "and exactly one report per resource",
        )
        assertTrue(!rig.dispatcher.isAlive, "the dispatcher was really retired")
        assertEquals(0, ownership.pending)
        assertDependents(ownership, 0, "no protection is left behind")
    }

    private fun aFailingReportDoesNotHideACancellingCarriedClose(ownerships: Ownerships) {
        for (path in LatePath.entries) {
            val terminal = CancellationException("cancelled closing the carried camera")
            val sinkFailure = IllegalStateException("the late-cleanup sink failed")
            reportFailureDoesNotHideTheCloseFailure(ownerships,
                path,
                cameraRaise = terminal,
                workerRaise = null,
                cameraSink = sinkFailure,
                workerSink = null,
                expectedPrimary = terminal,
                expectedSuppressed = listOf(sinkFailure),
            )
        }
    }

    @Test
    fun aFailingReportDoesNotHideACancellingCarriedClose_conforming() = aFailingReportDoesNotHideACancellingCarriedClose(conformingOwnership)

    @Test
    fun aFailingReportDoesNotHideACancellingCarriedClose_ordered() = aFailingReportDoesNotHideACancellingCarriedClose(orderedOwnership)

    private fun aFailingReportDoesNotHideALaterWorkerError(ownerships: Ownerships) {
        for (path in LatePath.entries) {
            val terminal = CloseSentinelError()
            val sinkFailure = IllegalStateException("the late-cleanup sink failed")
            reportFailureDoesNotHideTheCloseFailure(ownerships,
                path,
                cameraRaise = null,
                workerRaise = terminal,
                cameraSink = sinkFailure,
                workerSink = null,
                expectedPrimary = terminal,
                expectedSuppressed = listOf(sinkFailure),
            )
        }
    }

    @Test
    fun aFailingReportDoesNotHideALaterWorkerError_conforming() = aFailingReportDoesNotHideALaterWorkerError(conformingOwnership)

    @Test
    fun aFailingReportDoesNotHideALaterWorkerError_ordered() = aFailingReportDoesNotHideALaterWorkerError(orderedOwnership)

    private fun anOrdinarySinkFailureStillEscapesOnBothPaths(ownerships: Ownerships) {
        for (path in LatePath.entries) {
            val sinkFailure = IllegalStateException("the late-cleanup sink failed")
            reportFailureDoesNotHideTheCloseFailure(ownerships,
                path,
                cameraRaise = null,
                workerRaise = null,
                cameraSink = sinkFailure,
                workerSink = null,
                expectedPrimary = sinkFailure,
                expectedSuppressed = emptyList(),
            )
        }
    }

    @Test
    fun anOrdinarySinkFailureStillEscapesOnBothPaths_conforming() = anOrdinarySinkFailureStillEscapesOnBothPaths(conformingOwnership)

    @Test
    fun anOrdinarySinkFailureStillEscapesOnBothPaths_ordered() = anOrdinarySinkFailureStillEscapesOnBothPaths(orderedOwnership)

    /**
     * The WORKER's own report fails too, after an earlier terminal. The
     * retirement's own reporting failure is evidence that travels with that
     * terminal; it can neither replace it nor be dropped.
     */
    private fun aFailingWorkerReportIsEvidenceAndNeverThePrimary(ownerships: Ownerships) {
        for (path in LatePath.entries) {
            val terminal = CancellationException("cancelled closing the carried camera")
            val cameraSink = IllegalStateException("the camera report failed")
            val workerSink = IllegalStateException("the worker report failed")
            reportFailureDoesNotHideTheCloseFailure(ownerships,
                path,
                cameraRaise = terminal,
                workerRaise = null,
                cameraSink = cameraSink,
                workerSink = workerSink,
                expectedPrimary = terminal,
                expectedSuppressed = listOf(cameraSink, workerSink),
            )
        }
    }

    @Test
    fun aFailingWorkerReportIsEvidenceAndNeverThePrimary_conforming() = aFailingWorkerReportIsEvidenceAndNeverThePrimary(conformingOwnership)

    @Test
    fun aFailingWorkerReportIsEvidenceAndNeverThePrimary_ordered() = aFailingWorkerReportIsEvidenceAndNeverThePrimary(orderedOwnership)

    // ---- foreign disposal ------------------------------------------------------

    /**
     * Disposing a handle this ownership never owned is still cleanup work. While
     * a live owned worker exists, that work holds it: an ownership close
     * overlapping the disposal must not retire the worker underneath it.
     */
    private fun foreignDisposalHoldsALiveWorker(ownerships: Ownerships, path: LatePath) {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val late = LateLog()
        val ownership = ownerships.make(GENERATION, rig.dispatcher, late)
        ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order) }
        val observed = mutableListOf<String>()
        val stranger =
            object : ResourceHandle {
                override val identity: Any = "stranger"

                override fun close() {
                    synchronized(rig.order) { rig.order.add("close:stranger-enter") }
                    ownership.close(null)
                    synchronized(observed) { observed.add("alive-during-foreign-close=${rig.dispatcher.isAlive}") }
                    synchronized(rig.order) { rig.order.add("close:stranger-exit") }
                }
            }
        // A ticket this ownership never issued: the settlement is foreign.
        val ghost = AcquisitionTicket(GENERATION, CaptureResource.Camera)

        val outcome = if (path == LatePath.Adopt) ownership.adopt(ghost, stranger) else ownership.fail(ghost, "never opened", stranger)

        assertIs<AdoptOutcome.Unknown>(outcome, "no outstanding ticket for it")
        assertEquals(
            listOf("alive-during-foreign-close=true"),
            observed,
            "the worker must stay alive while the disposal it is running is still going",
        )
        assertEquals(
            listOf("close:stranger-enter", "close:stranger-exit", "close:worker", "quit:dispatcher"),
            rig.orderNow(),
            "and is retired exactly once, after that disposal",
        )
        assertTrue(!rig.dispatcher.isAlive)
        assertEquals(listOf("foreign:Camera", "late-close:Worker"), late.all(), "the disposal and the retirement were each reported once")
        assertDependents(ownership, 0, "no protection is left behind")
    }

    private fun aForeignDisposalHoldsALiveWorkerOnBothPaths(ownerships: Ownerships) {
        for (path in LatePath.entries) foreignDisposalHoldsALiveWorker(ownerships, path)
    }

    @Test
    fun aForeignDisposalHoldsALiveWorkerOnBothPaths_conforming() = aForeignDisposalHoldsALiveWorkerOnBothPaths(conformingOwnership)

    @Test
    fun aForeignDisposalHoldsALiveWorkerOnBothPaths_ordered() = aForeignDisposalHoldsALiveWorkerOnBothPaths(orderedOwnership)

    /** With the worker already gone, the disposal still happens, once, and resurrects nothing. */
    private fun aForeignDisposalAfterTheWorkerIsGoneStillDisposesOnce(ownerships: Ownerships) {
        for (path in LatePath.entries) {
            val rig = Rig(dispatcherOwnedByCodec = false)
            val late = LateLog()
            val ownership = ownerships.make(GENERATION, rig.dispatcher, late)
            ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order) }
            assertNull(ownership.close(null))
            assertEquals(listOf("close:worker", "quit:dispatcher"), rig.orderNow(), "the worker is already gone")
            val ghost = AcquisitionTicket(GENERATION, CaptureResource.Camera)

            val outcome =
                if (path == LatePath.Adopt) {
                    ownership.adopt(ghost, LoggedHandle("stranger", rig.order))
                } else {
                    ownership.fail(ghost, "never opened", LoggedHandle("stranger", rig.order))
                }

            assertIs<AdoptOutcome.Unknown>(outcome)
            assertEquals(
                listOf("close:worker", "quit:dispatcher", "close:stranger"),
                rig.orderNow(),
                "the stranger is disposed exactly once, and no worker is reopened or closed again",
            )
            assertEquals(
                listOf("foreign:Camera"),
                late.all(),
                "the worker was closed by the ownership's own close, so only the disposal is a late fact",
            )
            assertDependents(ownership, 0, "and no protection was reserved against a worker that is gone")
        }
    }

    @Test
    fun aForeignDisposalAfterTheWorkerIsGoneStillDisposesOnce_conforming() = aForeignDisposalAfterTheWorkerIsGoneStillDisposesOnce(conformingOwnership)

    @Test
    fun aForeignDisposalAfterTheWorkerIsGoneStillDisposesOnce_ordered() = aForeignDisposalAfterTheWorkerIsGoneStillDisposesOnce(orderedOwnership)

    /** A handle whose identity is still installed is never disposed as foreign. */
    private fun aLiveIdentityIsNotDisposed(ownerships: Ownerships) {
        for (path in LatePath.entries) {
            val rig = Rig(dispatcherOwnedByCodec = false)
            val ownership = ownerships.make(GENERATION, rig.dispatcher, LateLog())
            ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order) }
            val ticket = ownership.request(CaptureResource.Camera)
            val camera = LoggedHandle("camera", rig.order)
            assertEquals(AdoptOutcome.Adopted, ownership.adopt(ticket, camera), "the camera is installed")
            val ghost = AcquisitionTicket(GENERATION, CaptureResource.Camera)
            // A DIFFERENT wrapper around the same platform resource.
            val duplicate = LoggedHandle(camera.identity, rig.order)

            val outcome =
                if (path == LatePath.Adopt) ownership.adopt(ghost, duplicate) else ownership.fail(ghost, "never opened", duplicate)

            assertIs<AdoptOutcome.Unknown>(outcome)
            assertEquals(emptyList(), rig.orderNow(), "the live resource is not closed by a second wrapper around it")
            assertNull(ownership.close(null))
            assertEquals(
                listOf("close:camera", "close:worker", "quit:dispatcher"),
                rig.orderNow(),
                "it is closed once, by the owner, in reverse order",
            )
        }
    }

    @Test
    fun aLiveIdentityIsNotDisposed_conforming() = aLiveIdentityIsNotDisposed(conformingOwnership)

    @Test
    fun aLiveIdentityIsNotDisposed_ordered() = aLiveIdentityIsNotDisposed(orderedOwnership)

    /**
     * An acquisition already IN FLIGHT when the owner closes. The worker it is
     * opening on is protected across the whole acquisition, so the owner's
     * close retains it; the handle the acquisition returns is rejected and
     * closed, and only then does the worker quit. The bookkeeping balances.
     */
    private fun anInFlightAcquisitionAcrossCloseBalancesItsReservation(ownerships: Ownerships, rejectedThrows: Boolean) {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val ownership = ownerships.make(GENERATION, rig.dispatcher, LateLog())
        ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order) }
        val sentinel = IllegalStateException("the rejected encoder close failed")

        val outcome =
            ownership.acquire(CaptureResource.VideoEncoder) {
                // The owner gives up while this acquisition is still running.
                ownership.close(null)
                if (rejectedThrows) RaisingHandle("codec", rig.order) { sentinel } else LoggedHandle("codec", rig.order)
            }

        assertIs<AcquireOutcome.Retired>(outcome, "the encoder arrived after the owner had given up")
        assertEquals(
            listOf("close:codec", "close:worker", "quit:dispatcher"),
            rig.orderNow(),
            "the late encoder was closed once, and the worker it opened on quit only after it",
        )
        assertTrue(!rig.dispatcher.isAlive, "the worker really was retired")
        // A refused retention is NOT a retention: the contract says it is never
        // given back, so this row does not give it back either.
        assertFalse(ownership.retainWorker(), "and no retention is granted against a worker that is gone")
        assertNull(ownership.close(null), "closing again closes nothing again")
        assertEquals(3, rig.orderNow().size, "and attempts nothing again")
        assertReservationsBalanced(ownership, "after an acquisition rejected across close")
    }

    @Test
    fun anInFlightAcquisitionAcrossCloseBalances_conforming() =
        anInFlightAcquisitionAcrossCloseBalancesItsReservation(conformingOwnership, rejectedThrows = false)

    @Test
    fun anInFlightAcquisitionAcrossCloseBalances_ordered() =
        anInFlightAcquisitionAcrossCloseBalancesItsReservation(orderedOwnership, rejectedThrows = false)

    @Test
    fun aThrowingRejectedHandleStillBalances_conforming() =
        anInFlightAcquisitionAcrossCloseBalancesItsReservation(conformingOwnership, rejectedThrows = true)

    @Test
    fun aThrowingRejectedHandleStillBalances_ordered() =
        anInFlightAcquisitionAcrossCloseBalancesItsReservation(orderedOwnership, rejectedThrows = true)

    /**
     * The PRODUCT handoff, holding a lease from the PRODUCT ownership, while
     * the session terminates. The codec and the worker its callbacks arrive on
     * are both retained until the admitted transfer has released its index.
     */
    @Test
    fun theProductHandoffHoldsTheProductOwnershipsWorkerThroughATransfer() {
        val rig = Rig(dispatcherOwnedByCodec = false)
        val ownership = OrderedCaptureOwnership(GENERATION, rig.dispatcher, LateLog())
        val codec = HostCodec(rig.order, dispatcher = null, worker = ownership)
        ownership.acquire(CaptureResource.Worker) { HostWorkerHandle(rig.dispatcher, rig.order) }
        ownership.acquire(CaptureResource.VideoEncoder) { HostCodecHandle(codec, "codec-handle", rig.order) }
        val inRelease = HostGate("the transfer reached its index release")
        val proceed = HostGate("the row released the parked transfer")
        val schedule = ParkEach(mapOf("transfer" to inRelease), emptyMap(), proceed)
        val video = BoundedCaptureHandoff(MediaKind.Video, GENERATION, rig.buffers, rig.releases, rig.ledger, rig.faults, rig.capacity, HOST_MAX_OUTPUT_BYTES, codec, schedule)
        val audio = BoundedCaptureHandoff(MediaKind.Audio, GENERATION, HostBuffers(), HostReleases(), HostLedger(MediaKind.Audio, GENERATION), HostFaults(), rig.capacity, HOST_MAX_OUTPUT_BYTES)
        val listener = HostSessionListener()
        val session = ConformingSession(GENERATION, ownership, NoAcquire(), video, audio, listener)
        assertEquals(CaptureStart.Started, session.start(GrantedFlow(GENERATION), CaptureCapability.Available, CaptureCapability.Available))
        val out = rig.stage(index = 0, pts = 0L)
        val worker = HostWorker("transfer") { video.onOutput(GENERATION, out) }.start()
        withSettlement(listOf(inRelease, proceed), listOf(worker)) {
            inRelease.await(worker)
            // A FAILED ending, so the discard witness is in play as well.
            val cause = IllegalStateException("the broadcast failed")
            assertSame(cause, session.stop(cause))
            assertTrue(ownership.isClosed, "the ownership closed with the session")
            assertTrue(!codec.isDisposed, "the codec the product handoff is inside is still alive")
            assertTrue(rig.dispatcher.isAlive, "and so is the worker its callbacks arrive on")
            assertEquals(listOf("close:codec-handle"), rig.orderNow(), "closing the handle only ASKS for the disposal")
            proceed.openFully()
            worker.join()
            assertEquals(
                listOf("close:codec-handle", "release:0", "dispose:codec", "close:worker", "quit:dispatcher"),
                rig.orderNow(),
                "the index goes back, then the codec, then the worker it was driven from",
            )
            assertEquals(1, codec.disposalCount, "destroyed exactly once")
            assertEquals(
                listOf(HostFaults.Discarded(MediaKind.Video, 1)),
                rig.faults.discards(),
                "the copy that landed after the close is witnessed once",
            )
            assertNull(video.next(), "and a failed close publishes nothing")
        }
    }

    /**
     * Destroys the codec at the FIRST returned lease, while the other admitted
     * transfer is still inside it. The correct codec defers until the last one.
     */
    private class DisposeAtFirstReturnCodec(
        private val order: MutableList<String>,
        private val dispatcher: HostDispatcher?,
        private val name: String = "codec",
    ) : HostCodecOwner {
        private val lock = Any()
        private var leases = 0
        private var requested = false
        private var disposed = false
        private var disposals = 0

        override val isDisposed: Boolean get() = synchronized(lock) { disposed }

        override val disposalCount: Int get() = synchronized(lock) { disposals }

        override fun retainForTransfer(): Boolean = synchronized(lock) { if (disposed) false else { leases++; true } }

        override fun releaseAfterTransfer() {
            val destroy =
                synchronized(lock) {
                    leases--
                    if (requested && !disposed) {
                        disposed = true
                        disposals++
                        true
                    } else {
                        false
                    }
                }
            if (!destroy) return
            synchronized(order) { order.add("dispose:$name") }
            dispatcher?.quit()
            if (dispatcher != null) synchronized(order) { order.add("quit:dispatcher") }
        }

        override fun requestDisposal() {
            synchronized(lock) { requested = true }
        }
    }

    @Test
    fun aCodecDestroyedAtTheFirstReturnedLeaseFailsBothReleaseSchedules() {
        for (schedule in listOf(listOf(0, 1), listOf(1, 0))) {
            val bad = Rig(codecIs = { order, dispatcher -> DisposeAtFirstReturnCodec(order, dispatcher) })
            val thrown =
                assertFailsWith<AssertionError> {
                    theCodecOutlivesEveryAdmittedTransfer(conforming, bad, releaseOrder = schedule)
                }
            assertTrue(
                thrown.message.orEmpty().contains("while the other admitted transfer is still inside it"),
                "returning $schedule must fail at the lifetime assertion, not at an ordering one: ${thrown.message}",
            )
        }
    }

    /** Hands an index back twice, or under a neighbour's name. */
    private class MisReleases(
        private val inner: CodecBufferOwner,
        private val duplicate: Int? = null,
        private val shift: Boolean = false,
    ) : CodecBufferOwner {
        override fun release(index: Int) {
            inner.release(if (shift) index + 1 else index)
            if (index == duplicate) inner.release(index)
        }
    }

    /**
     * The row no longer demands that concurrent returns happen in admission
     * order, because nothing establishes that. It still demands that each
     * admitted index -- that index, exactly once -- is back before the codec
     * goes, and these are the violations that must still be caught.
     */
    @Test
    fun aDuplicateOrWrongIndexReleaseFailsTheRow() {
        val early = Rig()
        val duplicatedFirst =
            assertFailsWith<AssertionError> {
                theCodecOutlivesEveryAdmittedTransfer(conforming, early, owner = MisReleases(early.releases, duplicate = 0))
            }
        assertTrue(
            duplicatedFirst.message.orEmpty().contains("one index is back, and nothing else has happened"),
            "a doubled first return: ${duplicatedFirst.message}",
        )

        val late = Rig()
        val duplicatedLast =
            assertFailsWith<AssertionError> {
                theCodecOutlivesEveryAdmittedTransfer(conforming, late, owner = MisReleases(late.releases, duplicate = 1))
            }
        assertTrue(
            duplicatedLast.message.orEmpty().contains("exactly those two indices, each once"),
            "a doubled final return: ${duplicatedLast.message}",
        )

        val wrong = Rig()
        val misnamed =
            assertFailsWith<AssertionError> {
                theCodecOutlivesEveryAdmittedTransfer(conforming, wrong, owner = MisReleases(wrong.releases, shift = true))
            }
        assertTrue(
            misnamed.message.orEmpty().contains("one index is back, and nothing else has happened"),
            "an index returned under the wrong name: ${misnamed.message}",
        )
    }

    @Test
    fun destroyingTheCodecAtCloseFailsTheResourceOrderRowWithTheSameQueueCounts() {
        val good = Rig()
        theCodecOutlivesEveryAdmittedTransfer(conforming, good)

        val bad = Rig(eager = true)
        val thrown = assertFailsWith<AssertionError> { theCodecOutlivesEveryAdmittedTransfer(conforming, bad) }
        assertTrue(
            thrown.message.orEmpty().contains("must not be disposed before the final index release"),
            "the premature-disposal stand-in must fail at the resource-order assertion, not somewhere else: ${thrown.message}",
        )
        assertEquals(good.releases.indices().sorted(), bad.releases.indices().sorted(), "the same indices went back")
        assertEquals(good.buffers.fetched().sorted(), bad.buffers.fetched().sorted(), "the same buffers were fetched")
        assertEquals(1, bad.codec.disposalCount, "the counts are identical: only the ORDER is wrong")
    }

    @Test
    fun aCodecDestroyedDuringARefusalsReleaseFailsWithTheSameReleaseCount() {
        val good = Rig(capacity = 1)
        aRefusalHoldsTheCodecAcrossItsOwnIndexRelease(conforming, good)

        val bad = Rig(eager = true, capacity = 1)
        val thrown = assertFailsWith<AssertionError> { aRefusalHoldsTheCodecAcrossItsOwnIndexRelease(conforming, bad) }
        assertTrue(
            thrown.message.orEmpty().contains("while the refusal is still handing its index back"),
            "the refusal path must fail at the lifetime assertion: ${thrown.message}",
        )
        assertEquals(good.releases.indices(), bad.releases.indices(), "the same indices went back, in the same order")
        assertEquals(1, bad.codec.disposalCount, "and it was destroyed once: only the ORDER is wrong")
    }

    @Test
    fun aCodecThatIsNeverHeldFailsTheRefusedTransferRow() {
        assertFailsWith<AssertionError> { aTransferRefusedTheCodecTouchesNothing(conforming, Rig(eager = true)) }
    }

    private class NoAcquire : CaptureAcquirer {
        override val order: List<CaptureResource> = emptyList()

        override fun begin(ownership: CaptureOwnership, steps: AcquisitionSteps): AcquireOutcome.Failed? = null
    }

    private class GrantedFlow(override val generation: Long) : PermissionFlow {
        override fun stateOf(permission: CapturePermission) = PermissionState.Granted

        override fun request(permissions: Set<CapturePermission>) = emptySet<CapturePermission>()

        override fun onResult(generation: Long, permission: CapturePermission, facts: PermissionFacts) = PermissionOutcome.Ignored("granted")

        override fun retire(reason: String) = Unit

        override fun readiness(): CaptureReadiness = CaptureReadiness.Ready
    }

    /** An Error a close may raise; it is never converted into an ordinary failure. */
    private class CloseSentinelError : Error("a sentinel Error raised by a resource close")

    private companion object {
        const val CAPACITY = 2
        const val GENERATION = 1L
        const val REFUSED_INDEX = 6
    }
}
