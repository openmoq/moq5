package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException

/**
 * The encoder handoff: codec-owned indices in, [CapturedOutput]s out.
 *
 * [CodecOutput] carries exactly what `onOutputBufferAvailable(codec, index,
 * BufferInfo)` carries. The BYTES are fetched inside the boundary through
 * [OutputBufferAccess] -- `getOutputBuffer(index)` -- which returns a
 * `ByteBuffer` that may be direct or read-only, whose position/limit bracket
 * the valid data, and which can return `null` or throw. Every one of those
 * shapes has a row, and every one still settles the index it was given.
 *
 * Every row runs against the Phase A declaration (RED at its named `TODO`),
 * against the conforming host fixture (must PASS), and the discrimination rows
 * against a stand-in with the right totals. The ledger is the correct host
 * fake, so these rows fail at the handoff's own boundary.
 *
 * There is no codec here. Nothing below is evidence that a hardware encoder
 * produced decodable output.
 */
class CodecHandoffRedTest {

    private class Rig(val kind: MediaKind = MediaKind.Video, throwOnRelease: Set<Int> = emptySet()) {
        val buffers = HostBuffers()
        val releases = HostReleases(throwOnRelease)
        val faults = HostFaults()
        val ledger = HostLedger(kind, GENERATION)

        /** An observation under [pts], and a codec buffer at [index] holding [bytes] at [offset]. */
        fun stage(index: Int, bytes: ByteArray, pts: Long, offset: Int = 0, direct: Boolean = false, readOnly: Boolean = false, observe: Boolean = true): CodecOutput {
            val backing =
                if (direct) ByteBuffer.allocateDirect(offset + bytes.size + 4).also { it.position(offset); it.put(bytes); it.position(offset); it.limit(offset + bytes.size) }
                else ByteBuffer.wrap(ByteArray(offset + bytes.size + 4).also { bytes.copyInto(it, offset) }).also { it.position(offset); it.limit(offset + bytes.size) }
            buffers.put(index, if (readOnly) backing.asReadOnlyBuffer() else backing)
            if (observe) ledger.observe(GENERATION, kind, CaptureObservation(pts, 1_000L + pts))
            return CodecOutput(index, offset, bytes.size, pts, CaptureContract.FLAG_KEY_FRAME)
        }
    }

    private fun interface Make {
        fun make(rig: Rig): CaptureHandoff
    }

    private fun bytesOf(out: CapturedOutput): ByteArray = out.output.buffer.copyOfRange(out.output.offset, out.output.offset + out.output.size)

    /**
     * Same totals, wrong facts: it never fetches through the accessor (so a
     * null/throwing buffer is never seen), invents the capture instant from an
     * arrival ordinal instead of claiming, releases on a late callback, never
     * refuses a reordered or malformed output, and tells the owner nothing.
     */
    private class SameCountsHandoff(override val kind: MediaKind, override val generation: Long, private val owner: CodecBufferOwner, private val capacity: Int) :
        CaptureHandoff {
        private val queue = ArrayDeque<CapturedOutput>()
        private var arrivals = 0L
        override var ended: Boolean = false
            private set

        override fun signal(sink: OutputSignal) = Unit

        override fun onOutput(generation: Long, output: CodecOutput): HandoffOutcome {
            owner.release(output.index)
            if (queue.size >= capacity) return HandoffOutcome.Refused("full")
            queue.addLast(CapturedOutput(EncodedOutput(ByteArray(output.size), 0, output.size, output.presentationTimeUs, output.flags), arrivals++ * 33_333_000L))
            return HandoffOutcome.Queued
        }

        override fun onCodecError(generation: Long, reason: String) = Unit

        override fun next(): CapturedOutput? = queue.removeFirstOrNull()

        override fun close(mode: CloseMode) {
            ended = true
            queue.clear()
        }
    }

    // ---- the contract rows -----------------------------------------------------

    private fun h1_exactlyTheDeclaredRangeIsCopiedThroughTheAccessor(make: Make, kind: MediaKind) {
        for ((direct, readOnly) in listOf(false to false, true to false, false to true, true to true)) {
            val rig = Rig(kind)
            val handoff = make.make(rig)
            val payload = ByteArray(8) { (it + 1).toByte() }
            val out = rig.stage(index = 3, bytes = payload, pts = 0L, offset = 5, direct = direct, readOnly = readOnly)
            assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, out))
            assertEquals(listOf(3), rig.buffers.fetched(), "fetched once, through getOutputBuffer (direct=$direct readOnly=$readOnly)")
            assertEquals(listOf(3), rig.releases.indices(), "released exactly once")
            val got = assertNotNull(handoff.next())
            assertContentEquals(payload, bytesOf(got), "exactly [offset, offset+size), from a buffer with nonzero position/limit, without array()")
            val borrowed = rig.buffers.outputBuffer(3)!!
            assertEquals(5, borrowed.position(), "the borrowed view's position was not moved")
            assertEquals(13, borrowed.limit(), "nor its limit")
        }
    }

    private fun h2_theCaptureInstantIsTheClaimedObservationForBothOutputs(make: Make, kind: MediaKind) {
        val rig = Rig(kind)
        val handoff = make.make(rig)
        // Nonuniform, increasing keys: an arrival ordinal times a nominal
        // interval cannot reproduce either instant, and BOTH are asserted.
        val a = rig.stage(index = 0, bytes = ByteArray(4) { 1 }, pts = 10_000L)
        val b = rig.stage(index = 1, bytes = ByteArray(4) { 2 }, pts = 47_500L)
        assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, a))
        assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, b))
        val first = assertNotNull(handoff.next())
        val second = assertNotNull(handoff.next())
        assertEquals(11_000L, first.captureElapsedNanos, "the instant filed under 10000, claimed by the output with pts 10000")
        assertEquals(48_500L, second.captureElapsedNanos, "the instant filed under 47500, claimed by the output with pts 47500")
        assertEquals(10_000L, first.output.presentationTimeUs)
        assertEquals(47_500L, second.output.presentationTimeUs)
        assertEquals(0, rig.ledger.outstanding, "both observations were consumed")
        assertNull(handoff.next())
    }

    private fun h3_reorderedOutputIsANamedRefusalWithItsInventory(make: Make, kind: MediaKind) {
        val rig = Rig(kind)
        val handoff = make.make(rig)
        val later = rig.stage(index = 0, bytes = ByteArray(4), pts = 66_666L)
        val earlier = rig.stage(index = 1, bytes = ByteArray(4), pts = 33_333L)
        assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, later))
        assertIs<HandoffOutcome.Refused>(handoff.onOutput(GENERATION, earlier), "a presentation time that does not advance is reordering, which this encoder contract excludes")
        assertEquals(listOf(0, 1), rig.releases.indices(), "the refused index is still released, once")
        assertEquals(listOf(0), rig.buffers.fetched(), "and it was refused at admission, before any fetch")
        assertEquals(1, rig.faults.faults().size, "a reordered output is reported to the OWNER, exactly once")
        assertIs<CaptureFault.Reordered>(rig.faults.faults().single())
        assertNotNull(handoff.next(), "the output already accepted is still there")
        assertNull(handoff.next())
    }

    private fun h4_noObservationIsRefusedReleasedAndReported(make: Make, kind: MediaKind) {
        val rig = Rig(kind)
        val handoff = make.make(rig)
        val out = rig.stage(index = 7, bytes = ByteArray(4), pts = 12_345L, observe = false)
        assertIs<HandoffOutcome.Refused>(handoff.onOutput(GENERATION, out))
        assertEquals(listOf(7), rig.releases.indices())
        assertNull(handoff.next())
        assertEquals(1, rig.faults.faults().size, "a correlation failure reaches the OWNER")
        assertIs<CaptureFault.Correlation>(rig.faults.faults().single())
    }

    private fun h5_aMalformedRangeIsRefusedReleasedAndReported(make: Make, kind: MediaKind) {
        val rig = Rig(kind)
        val handoff = make.make(rig)
        val out = rig.stage(index = 2, bytes = ByteArray(4), pts = 0L)
        assertIs<HandoffOutcome.Refused>(handoff.onOutput(GENERATION, CodecOutput(2, 2, 8, 0L, out.flags)), "a range past the buffer's limit")
        assertEquals(listOf(2), rig.releases.indices())
        assertNull(handoff.next())
        assertEquals(1, rig.faults.faults().size)
        assertIs<CaptureFault.Malformed>(rig.faults.faults().single())
    }

    private fun h6_nullAndThrowingBufferAccessStillSettleTheIndex(make: Make, kind: MediaKind) {
        val absent = Rig(kind)
        val a = make.make(absent)
        absent.ledger.observe(GENERATION, absent.kind, CaptureObservation(0L, 1L))
        assertIs<HandoffOutcome.Refused>(a.onOutput(GENERATION, CodecOutput(4, 0, 4, 0L, CaptureContract.FLAG_KEY_FRAME)), "getOutputBuffer returned null")
        assertEquals(listOf(4), absent.releases.indices(), "the index was admitted, so it is released once")
        assertIs<CaptureFault.BufferAccess>(absent.faults.faults().single())

        val throwing = Rig(kind)
        val b = make.make(throwing)
        throwing.buffers.throwOn(5)
        throwing.ledger.observe(GENERATION, throwing.kind, CaptureObservation(0L, 1L))
        assertIs<HandoffOutcome.Refused>(b.onOutput(GENERATION, CodecOutput(5, 0, 4, 0L, CaptureContract.FLAG_KEY_FRAME)), "getOutputBuffer threw")
        assertEquals(listOf(5), throwing.releases.indices(), "released once even though the fetch threw")
        assertIs<CaptureFault.BufferAccess>(throwing.faults.faults().single())
        assertNull(b.next())
    }

    private fun h7_aReleaseThatThrewIsNeverRetried(make: Make, kind: MediaKind) {
        val rig = Rig(kind, throwOnRelease = setOf(6))
        val handoff = make.make(rig)
        val out = rig.stage(index = 6, bytes = ByteArray(4), pts = 0L)
        assertIs<HandoffOutcome.Refused>(handoff.onOutput(GENERATION, out), "a release that threw is a fault")
        assertEquals(listOf(6), rig.releases.indices(), "attempted exactly once: it may already have taken effect")
        assertIs<CaptureFault.BufferAccess>(rig.faults.faults().single())
        assertNull(handoff.next(), "an output whose index could not be settled is not published")
    }

    private fun h8_aFullQueueRefusesRatherThanDroppingOrBlocking(make: Make, kind: MediaKind) {
        val rig = Rig(kind)
        val handoff = make.make(rig)
        val first = ByteArray(4) { 1 }
        val outs = (0..CAPACITY).map { i -> rig.stage(index = i, bytes = if (i == 0) first else ByteArray(4) { (i + 1).toByte() }, pts = i * 1_000L) }
        for (i in 0 until CAPACITY) assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, outs[i]))
        assertIs<HandoffOutcome.Refused>(handoff.onOutput(GENERATION, outs[CAPACITY]))
        assertEquals((0..CAPACITY).toList(), rig.releases.indices())
        assertEquals(CAPACITY, rig.buffers.fetched().size, "overflow is refused at admission, before a fetch")
        assertIs<CaptureFault.Overflow>(rig.faults.faults().single())
        assertContentEquals(first, bytesOf(assertNotNull(handoff.next())), "the media already accepted is kept")
    }

    private fun h9_lateAndForeignCallbacksNeitherFetchNorRelease(make: Make, kind: MediaKind) {
        val closed = Rig(kind)
        val a = make.make(closed)
        a.close(CloseMode.Completed)
        assertIs<HandoffOutcome.Ignored>(a.onOutput(GENERATION, closed.stage(9, ByteArray(4), 0L)))
        assertEquals(emptyList(), closed.buffers.fetched(), "nothing is fetched from a codec this session gave up")
        assertEquals(emptyList(), closed.releases.indices())

        val foreign = Rig(kind)
        val b = make.make(foreign)
        val out = foreign.stage(4, ByteArray(4), 0L)
        assertIs<HandoffOutcome.Ignored>(b.onOutput(GENERATION - 1L, out))
        assertEquals(emptyList(), foreign.buffers.fetched())
        assertEquals(emptyList(), foreign.releases.indices())
        assertEquals(1, foreign.ledger.outstanding, "and nothing was claimed")
    }

    private fun h10_configurationAndEmptyTerminalClaimNothingAndSignalEnd(make: Make, kind: MediaKind) {
        val rig = Rig(kind)
        val handoff = make.make(rig)
        val signals = mutableListOf<MediaKind>()
        handoff.signal { signals.add(it) }
        rig.ledger.observe(GENERATION, rig.kind, CaptureObservation(0L, 42L))
        rig.buffers.put(0, ByteBuffer.wrap(ByteArray(4)))
        rig.buffers.put(1, ByteBuffer.wrap(ByteArray(0)))
        assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, CodecOutput(0, 0, 4, 0L, CaptureContract.FLAG_CODEC_CONFIG)))
        assertEquals(listOf(rig.kind), signals, "the queue became non-empty once")
        assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, CodecOutput(1, 0, 0, 0L, CaptureContract.FLAG_END_OF_STREAM)))
        assertEquals(1, rig.ledger.outstanding, "neither consumes an observation")
        val config = assertNotNull(handoff.next())
        assertEquals(CaptureContract.FLAG_CODEC_CONFIG, config.output.flags)
        assertEquals(0L, config.captureElapsedNanos, "PublishPipeline.step reads none for these")
        assertTrue(!handoff.ended, "not ended until the terminal is drained")
        val eos = assertNotNull(handoff.next())
        assertTrue(eos.output.isEndOfStream)
        assertTrue(handoff.ended, "the terminal was delivered through next(): the track has ended")
        assertNull(handoff.next())
        assertEquals(emptyList(), rig.faults.all())
    }

    private fun h11_aFaultAfterQueuedOutputTerminatesOnceAndCloseWitnessesTheRest(make: Make, kind: MediaKind) {
        val rig = Rig(kind)
        val handoff = make.make(rig)
        assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, rig.stage(0, ByteArray(4) { 7 }, 0L)))
        assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, rig.stage(1, ByteArray(4) { 8 }, 1_000L)))
        handoff.onCodecError(GENERATION, "CodecException: transient=false")
        handoff.onCodecError(GENERATION, "a second one")
        assertEquals(listOf<Any>(CaptureFault.Codec(rig.kind, "CodecException: transient=false")), rig.faults.all(), "ONE terminal fault, and no witness yet: nothing has been discarded")
        assertTrue(handoff.ended)
        assertIs<HandoffOutcome.Ignored>(handoff.onOutput(GENERATION, rig.stage(2, ByteArray(4), 2_000L)), "terminal: later callbacks are ignored")
        assertEquals(listOf(0, 1), rig.releases.indices(), "and release nothing")
        // The owner, told of the fault, closes the track as failed.
        handoff.close(CloseMode.Failed)
        assertNull(handoff.next())
        assertEquals(listOf(HostFaults.Discarded(rig.kind, 2)), rig.faults.discards(), "the two accepted outputs are witnessed, once, separately from the fault")
        assertEquals(1, rig.faults.faults().size, "and the fault is not repeated")
        handoff.close(CloseMode.Failed)
        assertEquals(1, rig.faults.discards().size, "closing twice witnesses nothing twice")
    }

    private fun h12_aCompletedCloseKeepsAcceptedOutputDrainable(make: Make, kind: MediaKind) {
        val rig = Rig(kind)
        val handoff = make.make(rig)
        assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, rig.stage(0, ByteArray(4) { 9 }, 0L)))
        handoff.close(CloseMode.Completed)
        assertTrue(handoff.ended)
        assertContentEquals(ByteArray(4) { 9 }, bytesOf(assertNotNull(handoff.next())), "accepted media is still published after a clean end")
        assertEquals(emptyList(), rig.faults.all(), "a clean completion is neither a fault nor a discard")
    }

    private fun runContract(kind: MediaKind, make: Make) {
        h1_exactlyTheDeclaredRangeIsCopiedThroughTheAccessor(make, kind)
        h2_theCaptureInstantIsTheClaimedObservationForBothOutputs(make, kind)
        h3_reorderedOutputIsANamedRefusalWithItsInventory(make, kind)
        h4_noObservationIsRefusedReleasedAndReported(make, kind)
        h5_aMalformedRangeIsRefusedReleasedAndReported(make, kind)
        h6_nullAndThrowingBufferAccessStillSettleTheIndex(make, kind)
        h7_aReleaseThatThrewIsNeverRetried(make, kind)
        h8_aFullQueueRefusesRatherThanDroppingOrBlocking(make, kind)
        h9_lateAndForeignCallbacksNeitherFetchNorRelease(make, kind)
        h10_configurationAndEmptyTerminalClaimNothingAndSignalEnd(make, kind)
        h11_aFaultAfterQueuedOutputTerminatesOnceAndCloseWitnessesTheRest(make, kind)
        h12_aCompletedCloseKeepsAcceptedOutputDrainable(make, kind)
        assertEquals(kind, make.make(Rig(kind)).kind)
    }

    private val real = Make { rig -> BoundedCaptureHandoff(rig.kind, GENERATION, rig.buffers, rig.releases, rig.ledger, rig.faults, CAPACITY, HOST_MAX_OUTPUT_BYTES) }
    private val conforming = Make { rig -> ConformingHandoff(rig.kind, GENERATION, rig.buffers, rig.releases, rig.ledger, rig.faults, CAPACITY) }
    private val counting = Make { rig -> SameCountsHandoff(rig.kind, GENERATION, rig.releases, CAPACITY) }

    @Test
    fun h_theVideoHandoffMeetsItsContract() = runContract(MediaKind.Video, real)

    @Test
    fun h_theAudioHandoffMeetsItsContract() = runContract(MediaKind.Audio, real)

    @Test
    fun s_theContractIsSatisfiableByAConformingFixture() {
        runContract(MediaKind.Video, conforming)
        runContract(MediaKind.Audio, conforming)
    }

    @Test
    fun m1_neverFetchingThroughTheAccessorFails() {
        assertFailsWithAssertion { h1_exactlyTheDeclaredRangeIsCopiedThroughTheAccessor(counting, MediaKind.Video) }
        assertFailsWithAssertion { h6_nullAndThrowingBufferAccessStillSettleTheIndex(counting, MediaKind.Video) }
    }

    @Test
    fun m2_stampingFromAnArrivalOrdinalFails() = assertFailsWithAssertion { h2_theCaptureInstantIsTheClaimedObservationForBothOutputs(counting, MediaKind.Video) }

    @Test
    fun m3_acceptingReorderedOrMalformedOutputFails() {
        assertFailsWithAssertion { h3_reorderedOutputIsANamedRefusalWithItsInventory(counting, MediaKind.Video) }
        assertFailsWithAssertion { h5_aMalformedRangeIsRefusedReleasedAndReported(counting, MediaKind.Video) }
    }

    @Test
    fun m4_aLateCallbackThatReleasesFails() = assertFailsWithAssertion { h9_lateAndForeignCallbacksNeitherFetchNorRelease(counting, MediaKind.Video) }

    @Test
    fun m5_droppingTheOwnerNotificationFailsWhileEveryLocalCountHolds() {
        val rig = Rig(MediaKind.Video)
        val handoff = counting.make(rig)
        val outs = (0..CAPACITY).map { i -> rig.stage(i, ByteArray(4), i * 1_000L) }
        for (i in 0 until CAPACITY) assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, outs[i]))
        assertIs<HandoffOutcome.Refused>(handoff.onOutput(GENERATION, outs[CAPACITY]))
        assertEquals((0..CAPACITY).toList(), rig.releases.indices(), "every index returned exactly once")
        assertEquals(emptyList(), rig.faults.all(), "and yet the owner was never told")
        assertFailsWithAssertion { h8_aFullQueueRefusesRatherThanDroppingOrBlocking(counting, MediaKind.Video) }
        assertFailsWithAssertion { h11_aFaultAfterQueuedOutputTerminatesOnceAndCloseWitnessesTheRest(counting, MediaKind.Video) }
    }

    /**
     * The product handoff against the PRODUCT ledger, on both tracks: exact
     * nonuniform keys and instants, exact bytes, exact release identities, and
     * an empty inventory at the terminal.
     */
    @Test
    fun p1_theProductHandoffCorrelatesThroughTheProductLedger() {
        for (kind in MediaKind.entries) {
            val buffers = HostBuffers()
            val releases = HostReleases()
            val faults = HostFaults()
            val ledger = BoundedCaptureLedger(kind, GENERATION, capacity = 8)
            val handoff = BoundedCaptureHandoff(kind, GENERATION, buffers, releases, ledger, faults, capacity = 4, maxOutputBytes = HOST_MAX_OUTPUT_BYTES)
            // Nonuniform keys and instants: no ordinal or nominal interval can
            // reproduce either of them.
            val staged = listOf(Triple(3, 10_000L, 1_111L), Triple(1, 47_500L, 90_009L), Triple(7, 47_501L, 90_010L))
            staged.forEach { (index, pts, nanos) ->
                buffers.put(index, java.nio.ByteBuffer.wrap(ByteArray(4) { (index + it).toByte() }))
                assertEquals(ObserveOutcome.Recorded, ledger.observe(GENERATION, kind, CaptureObservation(pts, nanos)))
            }
            staged.forEach { (index, pts, _) ->
                assertEquals(
                    HandoffOutcome.Queued,
                    handoff.onOutput(GENERATION, CodecOutput(index, 0, 4, pts, CaptureContract.FLAG_KEY_FRAME)),
                    "$kind output at $pts is admitted",
                )
            }
            staged.forEach { (index, pts, nanos) ->
                val got = assertNotNull(handoff.next(), "$kind output at $pts is published")
                assertContentEquals(ByteArray(4) { (index + it).toByte() }, bytesOf(got), "$kind: the exact bytes of index $index")
                assertEquals(pts, got.output.presentationTimeUs, "$kind: its own presentation time")
                assertEquals(nanos, got.captureElapsedNanos, "$kind: the instant the ledger filed under $pts")
            }
            assertNull(handoff.next(), "$kind: nothing else is published")
            assertEquals(staged.map { it.first }, releases.indices(), "$kind: every index went back once, in arrival order")
            assertEquals(0, ledger.outstanding, "$kind: every observation was consumed or discarded")
            assertEquals(emptyList(), faults.all(), "$kind: a clean run tells the owner nothing")
            buffers.put(9, java.nio.ByteBuffer.wrap(ByteArray(0)))
            assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, CodecOutput(9, 0, 0, 0L, CaptureContract.FLAG_END_OF_STREAM)))
            val eos = assertNotNull(handoff.next())
            assertTrue(eos.output.isEndOfStream && handoff.ended, "$kind: the terminal ends the track")
            assertNull(handoff.next(), "$kind: and the inventory is empty at the terminal")
        }
    }

    /**
     * Capacity counts an IN-FLIGHT reservation, not just what is queued. A
     * one-slot queue with a transfer still inside the codec is full.
     */
    @Test
    fun p2_capacityCountsAnInFlightReservation() {
        val rig = Rig(MediaKind.Video)
        val parked = HostGate("the first transfer reached its index release")
        val proceed = HostGate("the row released the first transfer")
        val schedule =
            object : HandoffSchedule {
                // ONLY the worker parks. The row's own offer must never block,
                // so a candidate that admits it fails at the assertion below
                // rather than at a watchdog.
                override fun beforeRelease() {
                    if (Thread.currentThread().name != "transfer") return
                    parked.open()
                    proceed.await()
                }
            }
        val handoff = BoundedCaptureHandoff(rig.kind, GENERATION, rig.buffers, rig.releases, rig.ledger, rig.faults, capacity = 1, maxOutputBytes = HOST_MAX_OUTPUT_BYTES, schedule = schedule)
        val first = rig.stage(index = 0, bytes = ByteArray(4) { 1 }, pts = 0L)
        val second = rig.stage(index = 1, bytes = ByteArray(4) { 2 }, pts = 10_000L)
        val worker = HostWorker("transfer") { handoff.onOutput(GENERATION, first) }.start()
        withSettlement(listOf(parked, proceed), listOf(worker)) {
            parked.await(worker)
            // The queue is EMPTY and the one slot is reserved.
            assertIs<HandoffOutcome.Refused>(
                handoff.onOutput(GENERATION, second),
                "the one slot is held by a transfer in flight: the queue is full",
            )
            assertEquals(1, rig.faults.faults().size, "and the owner is told about the overflow, exactly once")
            assertIs<CaptureFault.Overflow>(rig.faults.faults().single(), "and it is refused by name")
            assertEquals(listOf(1), rig.releases.indices(), "the refused index went back once")
            assertEquals(
                listOf(0),
                rig.buffers.fetched(),
                "and nothing was fetched FOR IT: overflow is decided at admission, so only the transfer in flight fetched",
            )
            proceed.openFully()
            worker.join()
        }
        // The in-flight transfer still settles into the one slot it reserved.
        assertContentEquals(ByteArray(4) { 1 }, bytesOf(assertNotNull(handoff.next())), "the admitted output is published")
        assertNull(handoff.next())
    }

    // ---- the cleanup transaction ----------------------------------------------

    /** Every release attempt, in order; a scripted index raises whatever it is given. */
    private class ScriptedReleases(private val raise: Map<Int, () -> Throwable>) : CodecBufferOwner {
        private val log = mutableListOf<Int>()

        @Synchronized
        override fun release(index: Int) {
            log.add(index)
            raise[index]?.let { throw it() }
        }

        @Synchronized
        fun indices(): List<Int> = log.toList()
    }

    /** A sink whose discard witness raises; every call is still recorded. */
    private class RaisingFaults(private val onDiscard: () -> Throwable) : CaptureFaultSink {
        private val discards = mutableListOf<Int>()
        private val faults = mutableListOf<CaptureFault>()

        @Synchronized
        override fun onFault(generation: Long, fault: CaptureFault) {
            faults.add(fault)
        }

        @Synchronized
        override fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) {
            discards.add(outputs)
            throw onDiscard()
        }

        @Synchronized
        fun discardAttempts(): List<Int> = discards.toList()

        @Synchronized
        fun faults(): List<CaptureFault> = faults.toList()
    }

    /**
     * The cleanup a throwing callback leaves behind is a TRANSACTION: the index
     * goes back, the lease goes back, the slot settles -- every one of them
     * attempted even when an earlier one raised -- and the original failure is
     * what leaves, carrying what the cleanup found.
     */
    /** Builds a handoff with every seam a cleanup row needs to script. */
    private fun interface Handoffs {
        fun make(
            kind: MediaKind,
            buffers: OutputBufferAccess,
            owner: CodecBufferOwner,
            ledger: CaptureLedger,
            faults: CaptureFaultSink,
            lifetime: CodecLifetime,
            schedule: HandoffSchedule,
        ): CaptureHandoff
    }

    private val productHandoff =
        Handoffs { k, b, o, l, f, c, sch -> BoundedCaptureHandoff(k, GENERATION, b, o, l, f, CAPACITY, HOST_MAX_OUTPUT_BYTES, c, sch) }
    private val fixtureHandoff =
        Handoffs { k, b, o, l, f, c, sch -> ConformingHandoff(k, GENERATION, b, o, l, f, CAPACITY, c, sch) }

    private fun cleanupTransaction(
        handoffs: Handoffs,
        primary: Throwable,
        releaseRaises: Throwable?,
        leaseReturnRaises: Throwable?,
        atCommit: Boolean = false,
        witnessRaises: Throwable? = null,
    ) {
        val order = mutableListOf<String>()
        val codec = HostCodec(order, dispatcher = null, raiseOnReturn = leaseReturnRaises?.let { { it } })
        val buffers = HostBuffers()
        val releases = ScriptedReleases(releaseRaises?.let { mapOf(0 to { it }) } ?: emptyMap())
        val faults: CaptureFaultSink = witnessRaises?.let { RaisingFaults { it } } ?: HostFaults()
        val ledger = HostLedger(MediaKind.Video, GENERATION, capacity = 8)
        var track: CaptureHandoff? = null
        // ONE-SHOT: only the callback under test raises, so the rows that
        // check the reservation settled can still use the queue afterwards.
        val fired = java.util.concurrent.atomic.AtomicBoolean(false)
        val schedule =
            object : HandoffSchedule {
                override fun beforeRelease() {
                    if (!atCommit && fired.compareAndSet(false, true)) throw primary
                }

                override fun beforeCommit() {
                    if (atCommit && fired.compareAndSet(false, true)) {
                        track?.close(CloseMode.Failed)
                        throw primary
                    }
                }
            }
        val handoff = handoffs.make(MediaKind.Video, buffers, releases, ledger, faults, codec, schedule)
        track = handoff
        buffers.put(0, java.nio.ByteBuffer.wrap(ByteArray(4)))
        ledger.observe(GENERATION, MediaKind.Video, CaptureObservation(0L, 42L))

        val thrown =
            try {
                handoff.onOutput(GENERATION, CodecOutput(0, 0, 4, 0L, CaptureContract.FLAG_KEY_FRAME))
                null
            } catch (cancelled: CancellationException) {
                cancelled
            } catch (failure: RuntimeException) {
                failure
            } catch (checked: java.io.IOException) {
                checked
            } catch (error: Error) {
                error
            }

        // The OBLIGATIONS first: every one of them is attempted whatever the
        // callback raised, so a candidate that skips one fails here by name.
        assertEquals(listOf(0), releases.indices(), "the index was attempted exactly once, and never retried")
        assertEquals(1, codec.leaseReturns, "and the granted lease was given back exactly once")
        assertEquals(0, codec.outstandingLeases, "so no lease is left holding the codec")
        // Then the failure that leaves, and what it carries.
        assertSame(primary, assertNotNull(thrown, "the callback's own failure is what leaves"), "the primary keeps its identity")
        val expected = listOfNotNull(releaseRaises, leaseReturnRaises, witnessRaises)
        val suppressed = thrown.suppressed.toList()
        assertEquals(expected.size, suppressed.size, "exactly what the cleanup found travels with it: $suppressed")
        expected.forEachIndexed { at, e -> assertSame(e, suppressed[at], "suppressed[$at], in cleanup order") }
        assertTrue(suppressed.none { it === thrown }, "nothing suppressed itself")
        if (witnessRaises != null) {
            assertEquals(listOf(1), (faults as RaisingFaults).discardAttempts(), "the witness was attempted once, and not retried")
        }
        // The reservation settled: the whole queue is available again.
        for (i in 1..CAPACITY) {
            buffers.put(i, java.nio.ByteBuffer.wrap(ByteArray(4)))
            ledger.observe(GENERATION, MediaKind.Video, CaptureObservation(i * 1_000L, 50L + i))
        }
        if (!atCommit) {
            for (i in 1..CAPACITY) {
                assertEquals(
                    HandoffOutcome.Queued,
                    handoff.onOutput(GENERATION, CodecOutput(i, 0, 4, i * 1_000L, CaptureContract.FLAG_KEY_FRAME)),
                    "the reservation settled: slot $i is available",
                )
            }
        }
    }

    @Test
    fun p3_aCancelledTransferWhoseIndexReturnAlsoThrows() {
        for (handoffs in listOf(productHandoff, fixtureHandoff)) {
            val primary = CancellationException("the capture scope was cancelled")
            cleanupTransaction(handoffs, primary, releaseRaises = java.io.IOException("the index return failed"), leaseReturnRaises = null)
        }
    }

    @Test
    fun p4_aCancelledTransferWhoseLeaseReturnThrows() {
        for (handoffs in listOf(productHandoff, fixtureHandoff)) {
            val primary = CancellationException("the capture scope was cancelled")
            cleanupTransaction(handoffs, primary, releaseRaises = null, leaseReturnRaises = IllegalStateException("the lease return failed"))
        }
    }

    @Test
    fun p5_anErrorPrimaryStillCarriesItsCleanupFailure() {
        for (handoffs in listOf(productHandoff, fixtureHandoff)) {
            val primary = AssertionError("the boundary asserted")
            cleanupTransaction(handoffs, primary, releaseRaises = IllegalStateException("the index return failed"), leaseReturnRaises = null)
        }
    }

    @Test
    fun p6_aCancelledCommitWhoseDiscardWitnessThrows() {
        for (handoffs in listOf(productHandoff, fixtureHandoff)) {
            val primary = CancellationException("cancelled as the owner gave up")
            cleanupTransaction(
                handoffs,
                primary,
                releaseRaises = null,
                leaseReturnRaises = null,
                atCommit = true,
                witnessRaises = IllegalStateException("the discard witness failed"),
            )
        }
    }

    /** Releases into the shared trace; a scripted index raises what it is given. */
    private class TracedReleases(private val trace: MutableList<String>, private val raise: Map<Int, () -> Throwable>) : CodecBufferOwner {
        @Synchronized
        override fun release(index: Int) {
            trace.add("index:$index")
            raise[index]?.let { throw it() }
        }
    }

    /** Notifications into the same trace; either kind may raise what it is given. */
    private class TracedFaults(
        private val trace: MutableList<String>,
        private val onFaultRaises: (() -> Throwable)? = null,
        private val onDiscardRaises: (() -> Throwable)? = null,
    ) : CaptureFaultSink {
        @Synchronized
        override fun onFault(generation: Long, fault: CaptureFault) {
            trace.add("fault")
            onFaultRaises?.let { throw it() }
        }

        @Synchronized
        override fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) {
            trace.add("witness:$outputs")
            onDiscardRaises?.let { throw it() }
        }
    }

    /**
     * An ADMISSION refusal whose fault notification raises, and whose lease
     * return then raises too. The refusal still hands its index back, the lease
     * is still returned once, and the notification's own failure is what
     * leaves -- carrying the lease failure.
     */
    @Test
    fun p7_anAdmissionRefusalPreservesItsNotificationFailureAndReturnsItsLease() {
        for (handoffs in listOf(productHandoff, fixtureHandoff)) {
            val trace = mutableListOf<String>()
            val primary = CancellationException("the owner was told and cancelled")
            val cleanup = IllegalStateException("the lease return failed")
            // Armed only for the REFUSAL's lease return: the positive setup
            // below must behave normally.
            var armed = false
            val codec = HostCodec(mutableListOf(), dispatcher = null, raiseOnReturn = { if (armed) cleanup else null }, trace = trace)
            val buffers = HostBuffers()
            val releases = TracedReleases(trace, emptyMap())
            val faults = TracedFaults(trace, onFaultRaises = { primary })
            val ledger = HostLedger(MediaKind.Video, GENERATION, capacity = 8)
            val handoff = handoffs.make(MediaKind.Video, buffers, releases, ledger, faults, codec, HandoffSchedule.NONE)
            // A real, public setup: fill the positive capacity first.
            for (i in 0 until CAPACITY) {
                buffers.put(i, java.nio.ByteBuffer.wrap(ByteArray(4)))
                ledger.observe(GENERATION, MediaKind.Video, CaptureObservation(i * 1_000L, 10L + i))
                assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, CodecOutput(i, 0, 4, i * 1_000L, CaptureContract.FLAG_KEY_FRAME)))
            }
            val fetchedBefore = buffers.fetched()
            val outstandingBefore = ledger.outstanding
            val returnsBefore = codec.leaseReturns
            trace.clear()
            armed = true

            val refusedIndex = 9
            buffers.put(refusedIndex, java.nio.ByteBuffer.wrap(ByteArray(4)))
            ledger.observe(GENERATION, MediaKind.Video, CaptureObservation(90_000L, 900L))
            val thrown =
                try {
                    handoff.onOutput(GENERATION, CodecOutput(refusedIndex, 0, 4, 90_000L, CaptureContract.FLAG_KEY_FRAME))
                    null
                } catch (cancelled: CancellationException) {
                    cancelled
                } catch (failure: RuntimeException) {
                    failure
                } catch (error: Error) {
                    error
                }

            assertEquals(
                listOf("retain", "index:$refusedIndex", "fault", "lease"),
                trace,
                "the refusal holds the codec for its index release, tells the owner, and returns the lease -- once each, in that order",
            )
            assertEquals(fetchedBefore, buffers.fetched(), "an admission refusal fetches no buffer")
            assertEquals(outstandingBefore + 1, ledger.outstanding, "and claims no observation")
            assertEquals(returnsBefore + 1, codec.leaseReturns, "the granted lease was given back exactly once")
            assertEquals(0, codec.outstandingLeases)
            assertSame(primary, assertNotNull(thrown, "the notification's failure leaves"), "the primary keeps its identity")
            assertEquals(listOf<Throwable>(cleanup), thrown.suppressed.toList(), "carrying exactly the lease failure")
        }
    }

    /**
     * Index cleanup raises THE SAME instance that is already leaving. It must
     * not suppress itself, and the later lease failure must still travel.
     */
    @Test
    fun p8_cleanupThatRaisesThePrimaryItselfDoesNotSuppressItself() {
        for (handoffs in listOf(productHandoff, fixtureHandoff)) {
            val trace = mutableListOf<String>()
            val primary = CancellationException("the capture scope was cancelled")
            val cleanup = IllegalStateException("the lease return failed")
            val codec = HostCodec(mutableListOf(), dispatcher = null, raiseOnReturn = { cleanup }, trace = trace)
            val buffers = HostBuffers()
            val releases = TracedReleases(trace, mapOf(0 to { primary }))
            val faults = TracedFaults(trace)
            val ledger = HostLedger(MediaKind.Video, GENERATION, capacity = 8)
            val fired = java.util.concurrent.atomic.AtomicBoolean(false)
            val schedule =
                object : HandoffSchedule {
                    override fun beforeRelease() {
                        if (fired.compareAndSet(false, true)) throw primary
                    }
                }
            val handoff = handoffs.make(MediaKind.Video, buffers, releases, ledger, faults, codec, schedule)
            buffers.put(0, java.nio.ByteBuffer.wrap(ByteArray(4)))
            ledger.observe(GENERATION, MediaKind.Video, CaptureObservation(0L, 42L))

            val thrown =
                try {
                    handoff.onOutput(GENERATION, CodecOutput(0, 0, 4, 0L, CaptureContract.FLAG_KEY_FRAME))
                    null
                } catch (cancelled: CancellationException) {
                    cancelled
                } catch (failure: RuntimeException) {
                    failure
                } catch (error: Error) {
                    error
                }

            assertEquals(listOf("retain", "index:0", "lease"), trace, "every obligation was attempted, in order")
            assertSame(primary, assertNotNull(thrown), "the original survives the cleanup that raised it again")
            assertEquals(listOf<Throwable>(cleanup), thrown.suppressed.toList(), "it carries the distinct failure and never itself")
            assertEquals(1, codec.leaseReturns)
            assertEquals(0, codec.outstandingLeases)
        }
    }

    /**
     * The ONLY cleanup failure is the primary instance itself. It must leave
     * unchanged, carrying nothing -- a self-suppression would be an exception
     * of its own.
     */
    @Test
    fun p11_cleanupRaisingOnlyThePrimaryCarriesNothing() {
        for (handoffs in listOf(productHandoff, fixtureHandoff)) {
            val trace = mutableListOf<String>()
            val primary = CancellationException("the capture scope was cancelled")
            val codec = HostCodec(mutableListOf(), dispatcher = null, trace = trace)
            val buffers = HostBuffers()
            val releases = TracedReleases(trace, mapOf(0 to { primary }))
            val faults = TracedFaults(trace)
            val ledger = HostLedger(MediaKind.Video, GENERATION, capacity = 8)
            val fired = java.util.concurrent.atomic.AtomicBoolean(false)
            val schedule =
                object : HandoffSchedule {
                    override fun beforeRelease() {
                        if (fired.compareAndSet(false, true)) throw primary
                    }
                }
            val handoff = handoffs.make(MediaKind.Video, buffers, releases, ledger, faults, codec, schedule)
            buffers.put(0, java.nio.ByteBuffer.wrap(ByteArray(4)))
            ledger.observe(GENERATION, MediaKind.Video, CaptureObservation(0L, 42L))

            val thrown =
                try {
                    handoff.onOutput(GENERATION, CodecOutput(0, 0, 4, 0L, CaptureContract.FLAG_KEY_FRAME))
                    null
                } catch (cancelled: CancellationException) {
                    cancelled
                } catch (failure: RuntimeException) {
                    failure
                } catch (error: Error) {
                    error
                }

            assertEquals(listOf("retain", "index:0", "lease"), trace, "every obligation was still attempted")
            assertSame(primary, assertNotNull(thrown), "the original leaves unchanged")
            assertEquals(emptyList(), thrown.suppressed.toList(), "and carries nothing: it never suppressed itself")
            assertEquals(1, codec.leaseReturns)
        }
    }

    /** Index AND lease cleanup both raise: the order is the trace's, the evidence is both. */
    @Test
    fun p9_bothCleanupFailuresTravelInTheOrderTheyHappened() {
        for (handoffs in listOf(productHandoff, fixtureHandoff)) {
            val trace = mutableListOf<String>()
            val primary = CancellationException("the capture scope was cancelled")
            val fromIndex = java.io.IOException("the index return failed")
            val fromLease = IllegalStateException("the lease return failed")
            val codec = HostCodec(mutableListOf(), dispatcher = null, raiseOnReturn = { fromLease }, trace = trace)
            val buffers = HostBuffers()
            val releases = TracedReleases(trace, mapOf(0 to { fromIndex }))
            val faults = TracedFaults(trace)
            val ledger = HostLedger(MediaKind.Video, GENERATION, capacity = 8)
            val fired = java.util.concurrent.atomic.AtomicBoolean(false)
            val schedule =
                object : HandoffSchedule {
                    override fun beforeRelease() {
                        if (fired.compareAndSet(false, true)) throw primary
                    }
                }
            val handoff = handoffs.make(MediaKind.Video, buffers, releases, ledger, faults, codec, schedule)
            buffers.put(0, java.nio.ByteBuffer.wrap(ByteArray(4)))
            ledger.observe(GENERATION, MediaKind.Video, CaptureObservation(0L, 42L))

            val thrown =
                try {
                    handoff.onOutput(GENERATION, CodecOutput(0, 0, 4, 0L, CaptureContract.FLAG_KEY_FRAME))
                    null
                } catch (cancelled: CancellationException) {
                    cancelled
                } catch (failure: RuntimeException) {
                    failure
                } catch (checked: java.io.IOException) {
                    checked
                } catch (error: Error) {
                    error
                }

            assertEquals(listOf("retain", "index:0", "lease"), trace, "the index return really did precede the lease return")
            assertSame(primary, assertNotNull(thrown), "the primary keeps its identity")
            assertEquals(
                listOf<Throwable>(fromIndex, fromLease),
                thrown.suppressed.toList(),
                "both cleanup failures travel, in the order they happened",
            )
            assertEquals(1, codec.leaseReturns)
        }
    }

    /** No primary: a lease failure after a clean copy still escapes, and publishes nothing. */
    @Test
    fun p10_aLeaseFailureWithNoPrimaryEscapesAndPublishesNothing() {
        for (handoffs in listOf(productHandoff, fixtureHandoff)) {
            val trace = mutableListOf<String>()
            val cleanup = IllegalStateException("the lease return failed")
            val codec = HostCodec(mutableListOf(), dispatcher = null, raiseOnReturn = { cleanup }, trace = trace)
            val buffers = HostBuffers()
            val releases = TracedReleases(trace, emptyMap())
            val faults = TracedFaults(trace)
            val ledger = HostLedger(MediaKind.Video, GENERATION, capacity = 8)
            val handoff = handoffs.make(MediaKind.Video, buffers, releases, ledger, faults, codec, HandoffSchedule.NONE)
            buffers.put(0, java.nio.ByteBuffer.wrap(ByteArray(4)))
            ledger.observe(GENERATION, MediaKind.Video, CaptureObservation(0L, 42L))

            val thrown =
                try {
                    handoff.onOutput(GENERATION, CodecOutput(0, 0, 4, 0L, CaptureContract.FLAG_KEY_FRAME))
                    null
                } catch (failure: RuntimeException) {
                    failure
                }

            assertSame(cleanup, assertNotNull(thrown, "a cleanup failure with no primary still escapes"), "by identity")
            assertEquals(emptyList(), thrown.suppressed.toList(), "with nothing else attached")
            assertEquals(listOf("retain", "index:0", "lease"), trace, "the copy and the index return both happened first")
            assertNull(handoff.next(), "and nothing is published as though the cleanup had succeeded")
            assertEquals(1, codec.leaseReturns)
            // The reservation settled: the queue is whole again.
            for (i in 1..CAPACITY) {
                buffers.put(i, java.nio.ByteBuffer.wrap(ByteArray(4)))
                ledger.observe(GENERATION, MediaKind.Video, CaptureObservation(i * 1_000L, 50L + i))
                assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, CodecOutput(i, 0, 4, i * 1_000L, CaptureContract.FLAG_KEY_FRAME)))
            }
        }
    }

    /**
     * The refusal's own cleanup is a transaction too. An ADMISSION refusal
     * (the queue is full) whose index return, fault notification, wake or
     * lease return may raise: every obligation is attempted once, in order
     * -- the lease for the index, the index, the refusal state, the fault,
     * the wake, the lease back -- and the FIRST failure is what leaves, by
     * identity, carrying the later distinct ones in the order they happened.
     * With nothing raised, the refusal returns.
     */
    private class RefusalCleanup(val trace: List<String>, val thrown: Throwable?, val outcome: HandoffOutcome?)

    private fun admissionRefusal(
        handoffs: Handoffs,
        indexRaises: Throwable? = null,
        faultRaises: Throwable? = null,
        wakeRaises: Throwable? = null,
        leaseRaises: Throwable? = null,
    ): RefusalCleanup {
        val trace = mutableListOf<String>()
        var armed = false
        val codec = HostCodec(mutableListOf(), dispatcher = null, raiseOnReturn = { if (armed) leaseRaises else null }, trace = trace)
        val buffers = HostBuffers()
        val refusedIndex = 9
        val releases = TracedReleases(trace, indexRaises?.let { mapOf(refusedIndex to { it }) } ?: emptyMap())
        val faults = TracedFaults(trace, onFaultRaises = faultRaises?.let { { it } })
        val ledger = HostLedger(MediaKind.Video, GENERATION, capacity = 8)
        val handoff = handoffs.make(MediaKind.Video, buffers, releases, ledger, faults, codec, HandoffSchedule.NONE)
        for (i in 0 until CAPACITY) {
            buffers.put(i, java.nio.ByteBuffer.wrap(ByteArray(4)))
            ledger.observe(GENERATION, MediaKind.Video, CaptureObservation(i * 1_000L, 10L + i))
            assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, CodecOutput(i, 0, 4, i * 1_000L, CaptureContract.FLAG_KEY_FRAME)))
        }
        handoff.signal { trace.add("wake"); wakeRaises?.let { throw it } }
        val fetchedBefore = buffers.fetched()
        val outstandingBefore = ledger.outstanding
        val returnsBefore = codec.leaseReturns
        trace.clear()
        armed = true
        buffers.put(refusedIndex, java.nio.ByteBuffer.wrap(ByteArray(4)))
        ledger.observe(GENERATION, MediaKind.Video, CaptureObservation(90_000L, 900L))
        val result = runCatching { handoff.onOutput(GENERATION, CodecOutput(refusedIndex, 0, 4, 90_000L, CaptureContract.FLAG_KEY_FRAME)) }
        armed = false

        assertEquals(
            listOf("retain", "index:$refusedIndex", "fault", "wake", "lease"),
            trace.toList(),
            "every owed obligation attempted exactly once, in order, whatever an earlier one raised",
        )
        assertEquals(fetchedBefore, buffers.fetched(), "an admission refusal fetches no buffer")
        assertEquals(outstandingBefore + 1, ledger.outstanding, "and claims no observation")
        assertEquals(returnsBefore + 1, codec.leaseReturns, "the one granted lease was given back once")
        assertEquals(0, codec.outstandingLeases)
        assertTrue(handoff.ended, "the refusal state was established")
        trace.clear()
        assertIs<HandoffOutcome.Ignored>(handoff.onOutput(GENERATION, CodecOutput(10, 0, 4, 100_000L, CaptureContract.FLAG_KEY_FRAME)), "the ended track admits nothing more")
        assertEquals(emptyList<String>(), trace.toList(), "and touches nothing for it")
        return RefusalCleanup(trace, result.exceptionOrNull(), result.getOrNull())
    }

    private fun expectLeaves(first: Throwable, carried: List<Throwable>, cleanup: RefusalCleanup) {
        val thrown = cleanup.thrown ?: throw AssertionError("expected $first to leave the refusal, but it returned ${cleanup.outcome}")
        if (thrown !== first) throw AssertionError("expected $first to leave by identity, but $thrown left", thrown)
        assertEquals(carried.size, thrown.suppressed.size, "exactly the later distinct failures travel: ${thrown.suppressed.toList()}")
        carried.forEachIndexed { at, e -> assertSame(e, thrown.suppressed[at], "suppressed[$at], in invocation order") }
        assertTrue(thrown.suppressed.none { it === thrown }, "nothing suppressed itself")
    }

    @Test
    fun p12_anAdmissionRefusalWithNothingRaisedReturnsAndAttemptsEverything() {
        for (handoffs in listOf(productHandoff, fixtureHandoff)) {
            val cleanup = admissionRefusal(handoffs)
            assertNull(cleanup.thrown)
            assertIs<HandoffOutcome.Refused>(cleanup.outcome)
        }
    }

    @Test
    fun p13_anIndexReturnExceptionAtAnAdmissionRefusalLeavesByIdentity() {
        for (handoffs in listOf(productHandoff, fixtureHandoff)) {
            val index = IllegalStateException("the index return failed")
            expectLeaves(index, emptyList(), admissionRefusal(handoffs, indexRaises = index))
        }
    }

    @Test
    fun p14_anIndexReturnCancellationAtAnAdmissionRefusalLeavesByIdentity() {
        for (handoffs in listOf(productHandoff, fixtureHandoff)) {
            val index = CancellationException("the index return was cancelled")
            expectLeaves(index, emptyList(), admissionRefusal(handoffs, indexRaises = index))
        }
    }

    @Test
    fun p15_anIndexReturnErrorAtAnAdmissionRefusalSkipsNoLaterObligation() {
        for (handoffs in listOf(productHandoff, fixtureHandoff)) {
            val index = AssertionError("the index return asserted")
            expectLeaves(index, emptyList(), admissionRefusal(handoffs, indexRaises = index))
        }
    }

    /** Index, fault, wake and lease all raise: the index failure leads, carrying the other three in the order they happened. */
    @Test
    fun p16_everyRefusalCleanupFailureTravelsBehindTheFirst() {
        for (handoffs in listOf(productHandoff, fixtureHandoff)) {
            val index = IllegalStateException("the index return failed")
            val fault = CancellationException("the owner was told and cancelled")
            val wake = AssertionError("the wake asserted")
            val lease = IllegalStateException("the lease return failed")
            expectLeaves(index, listOf(fault, wake, lease), admissionRefusal(handoffs, indexRaises = index, faultRaises = fault, wakeRaises = wake, leaseRaises = lease))
        }
    }

    /** With the index returned cleanly, the notification's failure is the first and leads; the wake is still attempted and the lease failure still travels. */
    @Test
    fun p17_withoutAnIndexFailureTheNotificationLeadsAndTheWakeIsNotSkipped() {
        for (handoffs in listOf(productHandoff, fixtureHandoff)) {
            val fault = CancellationException("the owner was told and cancelled")
            val lease = IllegalStateException("the lease return failed")
            expectLeaves(fault, listOf(lease), admissionRefusal(handoffs, faultRaises = fault, leaseRaises = lease))
        }
    }

    /** Two LATER obligations raise the SAME instance: it travels behind the first failure exactly once. */
    @Test
    fun p19_aSharedLaterFailureIsCarriedOnce() {
        for (handoffs in listOf(productHandoff, fixtureHandoff)) {
            val index = IllegalStateException("the index return failed")
            val shared = IllegalStateException("the wake and the lease return failed alike")
            expectLeaves(index, listOf(shared), admissionRefusal(handoffs, indexRaises = index, wakeRaises = shared, leaseRaises = shared))
        }
    }

    /** A later obligation raises the FIRST failure itself again: it is not a root of itself. */
    @Test
    fun p20_aLaterObligationRaisingTheFirstFailureAgainAddsNothing() {
        for (handoffs in listOf(productHandoff, fixtureHandoff)) {
            val index = CancellationException("the index return was cancelled")
            val lease = IllegalStateException("the lease return failed")
            expectLeaves(index, listOf(lease), admissionRefusal(handoffs, indexRaises = index, faultRaises = index, leaseRaises = lease))
        }
    }

    /**
     * The same helper serves a TRANSFER rejection: a null platform buffer is
     * refused and its index returned. If that index return raises, it leaves
     * by identity, and the fault, the wake and the lease are still attempted.
     */
    @Test
    fun p18_anIndexReturnFailureAtATransferRejectionLeavesByIdentity() {
        for (handoffs in listOf(productHandoff, fixtureHandoff)) {
            val trace = mutableListOf<String>()
            val index = IllegalStateException("the index return failed")
            val codec = HostCodec(mutableListOf(), dispatcher = null, trace = trace)
            val buffers = HostBuffers()
            val faults = TracedFaults(trace)
            val ledger = HostLedger(MediaKind.Video, GENERATION, capacity = 8)
            val handoff = handoffs.make(MediaKind.Video, buffers, TracedReleases(trace, mapOf(0 to { index })), ledger, faults, codec, HandoffSchedule.NONE)
            handoff.signal { trace.add("wake") }
            ledger.observe(GENERATION, MediaKind.Video, CaptureObservation(0L, 42L))
            val result = runCatching { handoff.onOutput(GENERATION, CodecOutput(0, 0, 4, 0L, CaptureContract.FLAG_KEY_FRAME)) }
            assertEquals(listOf("retain", "index:0", "fault", "wake", "lease"), trace.toList(), "every owed obligation, once, in order")
            assertEquals(listOf(0), buffers.fetched(), "fetched once, found nothing")
            assertEquals(1, ledger.outstanding, "nothing claimed")
            assertEquals(1, codec.leaseReturns)
            assertEquals(0, codec.outstandingLeases)
            assertTrue(handoff.ended)
            val thrown = result.exceptionOrNull() ?: throw AssertionError("expected $index to leave the rejection, but it returned ${result.getOrNull()}")
            if (thrown !== index) throw AssertionError("expected $index to leave by identity, but $thrown left", thrown)
            assertEquals(emptyList<Throwable>(), thrown.suppressed.toList())
        }
    }

    private fun assertFailsWithAssertion(body: () -> Unit) {
        val thrown =
            try {
                body()
                null
            } catch (assertion: AssertionError) {
                assertion
            }
        assertTrue(thrown != null, "this row is satisfied by a handoff that only gets the totals right")
    }

    private companion object {
        const val CAPACITY = 2
        const val GENERATION = 1L
    }
}
