package org.openmoq.moq5.jni

import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.assertFailsWith
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * SELF-CHECKS of the host recorder's codec instrumentation, through its own
 * controls rather than the product rows: the forwarding names reach the real
 * utility and are recorded exactly, a synthetic result replaces exactly the
 * call it was armed for, and every JNI fault site is REACHED and stops what
 * follows it. Passing here proves the instrument, not the product.
 */
internal class CodecRecorderControlTest {
    @BeforeTest
    fun load() {
        assertIs<BridgeInit.Ready>(NativeBridgeOwner.initialize(), "setup: the host product image must load")
        TrustRouteProbe.nativeCodecReset()
    }

    @AfterTest
    fun noInstrumentFault() {
        assertEquals(null, TrustRouteProbe.nativeFault(), "the recorder latched an instrumentation fault")
    }

    private fun control(kind: Int, format: Int, input: ByteArray, lengthSize: Int = 4, entry: ByteArray? = null, oti: Int? = null): Pair<Int, ByteArray?> {
        val status = IntArray(1)
        val out = TrustRouteProbe.nativeCodecControl(kind, format, input, lengthSize, entry, oti != null, oti ?: 0, status)
        return status[0] to out
    }

    @Test
    fun rc1_theForwardingNamesReachTheRealUtilityAndRecordEveryCall() {
        val (rc, out) = control(1, 1, CodecVectors.AVC_ANNEXB)
        assertEquals(CodecVectors.MOQ_OK, rc)
        assertContentEquals(CodecVectors.AVCC, out, "the real builder's reviewed avcC")
        assertEquals(
            listOf(
                CodecCall(1, 1, 4, 0, 0, 0, CodecVectors.MOQ_ERR_BUFFER.toLong(), 21, 18, 0, 0),
                CodecCall(1, 1, 4, 0, 0, 21, CodecVectors.MOQ_OK.toLong(), 21, 18, 0, 0),
            ),
            CodecCall.all(),
        )
        assertContentEquals(CodecVectors.AVC_ANNEXB, TrustRouteProbe.nativeCodecBytes(0, false))
        assertContentEquals(CodecVectors.AVC_ANNEXB, TrustRouteProbe.nativeCodecBytes(1, false))
        TrustRouteProbe.nativeCodecReset()
        val (src, str) = control(2, 1, CodecVectors.AVCC, entry = "avc3".toByteArray(Charsets.US_ASCII))
        assertEquals(CodecVectors.MOQ_OK, src)
        assertEquals("avc3.42001e", String(str!!, Charsets.US_ASCII))
        assertEquals(listOf(2L, 2L), CodecCall.all().map { it.kind })
        assertContentEquals("avc3".toByteArray(Charsets.US_ASCII), TrustRouteProbe.nativeCodecBytes(1, true))
    }

    @Test
    fun rc2_aSyntheticResultReplacesExactlyTheArmedCall() {
        TrustRouteProbe.nativeCodecInject(0, CodecVectors.MOQ_OK, 0)
        val (rc0, out0) = control(1, 1, CodecVectors.AVC_ANNEXB)
        assertEquals(CodecVectors.MOQ_OK, rc0)
        assertNull(out0, "the control publishes nothing for an OK size query")
        assertEquals(listOf(CodecCall(1, 1, 4, 0, 0, 0, 0, 0, 18, 0, 1)), CodecCall.all())
        TrustRouteProbe.nativeCodecReset()
        TrustRouteProbe.nativeCodecInject(1, CodecVectors.MOQ_ERR_PROTO, 0)
        val (rc1, _) = control(1, 1, CodecVectors.AVC_ANNEXB)
        assertEquals(CodecVectors.MOQ_ERR_PROTO, rc1)
        assertEquals(listOf(0L, 1L), CodecCall.all().map { it.injected }, "the real query, then the synthetic build")
    }

    private class Declared(message: String) : RuntimeException(message)

    /** Arms [site] (after [skip] occurrences) through the scoped observer, runs the control, and returns (thrown, returned, log). */
    private fun envControl(site: Int, skip: Int = 0, out: Array<ByteArray?> = arrayOfNulls(1)): Triple<Throwable?, Int?, IntArray> {
        val declared = Declared("the declared fault at $site")
        val o = observed(envSite = site, envSkip = skip, envThrowable = declared) { TrustRouteProbe.nativeCodecEnvControl(byteArrayOf(1, 2, 3), out) }
        val thrown = o.result.exceptionOrNull()
        if (thrown != null) assertSame(declared, thrown, "the declared Throwable leaves by identity at site $site")
        return Triple(thrown, o.result.getOrNull(), o.env!!)
    }

    @Test
    fun rc3_everyJniFaultSiteIsReachedAndStopsWhatFollows() {
        val gr = TrustRouteProbe.OP_GET_REGION
        val na = TrustRouteProbe.OP_NEW_ARRAY
        val sr = TrustRouteProbe.OP_SET_REGION
        val se = TrustRouteProbe.OP_SET_ELEMENT
        val ec = TrustRouteProbe.OP_EXCEPTION_CHECK
        val dl = TrustRouteProbe.OP_DELETE_LOCAL
        val clean = arrayOfNulls<ByteArray>(1)
        val (t0, r0, l0) = envControl(0, out = clean)
        assertNull(t0)
        assertEquals(0, r0)
        assertContentEquals(intArrayOf(0, gr, ec, na, sr, ec, se, ec, dl), l0, "no arm: the whole sequence, nothing fired")
        assertContentEquals(byteArrayOf(1, 2, 3), clean[0])
        for ((site, expected) in listOf(
            gr to intArrayOf(1, gr, ec),
            na to intArrayOf(1, gr, ec, na, ec),
            sr to intArrayOf(1, gr, ec, na, sr, ec, dl),
            se to intArrayOf(1, gr, ec, na, sr, ec, se, ec, dl),
        )) {
            val holder = arrayOfNulls<ByteArray>(1)
            val (thrown, _, log) = envControl(site, out = holder)
            assertTrue(thrown != null, "site $site raised")
            assertContentEquals(expected, log, "site $site: reached, fired, and nothing after it")
            assertNull(holder[0], "site $site: nothing published")
        }
        val (tn, rn, ln) = envControl(TrustRouteProbe.SITE_NEW_ARRAY_NULL)
        assertNull(tn, "the NULL arm leaves nothing pending")
        assertEquals(TrustRouteProbe.SITE_NEW_ARRAY_NULL, rn)
        assertContentEquals(intArrayOf(1, gr, ec, na, ec), ln)
        val (ts, _, ls) = envControl(gr, skip = 1)
        assertNull(ts, "the control copies once, so a second-occurrence arm never fires")
        assertEquals(0, ls[0])
    }

    /** The NULL arm counts occurrences like the throwing arm: the skipped allocation runs normally, the next one answers NULL, and it fires once. */
    @Test
    fun rc4_theNullArmHonoursItsSkipAndFiresOnce() {
        val gr = TrustRouteProbe.OP_GET_REGION
        val na = TrustRouteProbe.OP_NEW_ARRAY
        val sr = TrustRouteProbe.OP_SET_REGION
        val se = TrustRouteProbe.OP_SET_ELEMENT
        val ec = TrustRouteProbe.OP_EXCEPTION_CHECK
        val dl = TrustRouteProbe.OP_DELETE_LOCAL
        val o =
            observed(envSite = TrustRouteProbe.SITE_NEW_ARRAY_NULL, envSkip = 1, envThrowable = Declared("never raised by the NULL arm")) {
                listOf(0, 1, 2).map { TrustRouteProbe.nativeCodecEnvControl(byteArrayOf(1, 2, 3), arrayOfNulls(1)) }
            }
        val statuses = o.get()
        val log = o.env!!
        assertEquals(listOf(0, TrustRouteProbe.SITE_NEW_ARRAY_NULL, 0), statuses, "skipped, fired, then one-shot spent")
        assertContentEquals(
            intArrayOf(
                1,
                gr, ec, na, sr, ec, se, ec, dl,
                gr, ec, na, ec,
                gr, ec, na, sr, ec, se, ec, dl,
            ),
            log,
        )
    }

    /**
     * The fidelity oracle rejects what the rows must never accept: a replaced
     * input whose OUTPUT is identical (12 10 twice where 12 10 then 12 14 was
     * declared), and a single uncompared-looking field (the OTI, the length
     * size). Driven through the recorder's own control, so this measures the
     * ORACLE, not the product.
     */
    @Test
    fun rc5_theFidelityOracleRejectsAReplacedInputWithTheSameOutputAndAWrongField() {
        val mp4a = "mp4a".toByteArray(Charsets.US_ASCII)
        fun declared(second: ByteArray, oti: Long = 0x40) =
            queryAndBuild(2, 2, -1, 9, CodecVectors.ASC_LC, mp4a, hasOti = 1, oti = oti) + queryAndBuild(2, 2, -1, 9, second, mp4a, hasOti = 1, oti = oti)
        fun record(second: ByteArray) {
            TrustRouteProbe.nativeCodecReset()
            assertEquals("mp4a.40.2", String(control(2, 2, CodecVectors.ASC_LC, entry = mp4a, oti = 0x40).second!!, Charsets.US_ASCII))
            assertEquals("mp4a.40.2", String(control(2, 2, second, entry = mp4a, oti = 0x40).second!!, Charsets.US_ASCII))
        }
        record(CodecVectors.ASC_LC_960)
        expectCodecCalls(declared(CodecVectors.ASC_LC_960), "the true inputs")
        record(CodecVectors.ASC_LC)
        assertFailsWith<AssertionError>("the replaced input, same output string") { expectCodecCalls(declared(CodecVectors.ASC_LC_960), "replaced") }
        record(CodecVectors.ASC_LC_960)
        assertFailsWith<AssertionError>("a wrong OTI field") { expectCodecCalls(declared(CodecVectors.ASC_LC_960, oti = 0x41), "oti") }
        TrustRouteProbe.nativeCodecReset()
        control(1, 1, CodecVectors.AVC_ANNEXB, lengthSize = 4)
        assertFailsWith<AssertionError>("a wrong length size") { expectCodecCalls(queryAndBuild(1, 1, 5, 21, CodecVectors.AVC_ANNEXB), "lengthSize") }
    }

    /** The allocation ledger records every attempt and retirement, forces a chosen failure, and names an omitted free -- then frees it itself. */
    @Test
    fun rc6_theAllocationLedgerSeesAttemptsRetirementsFailuresAndAnOmittedFree() {
        val clean = observed(allocFailAt = -1) { TrustRouteProbe.nativeCodecAllocControl(false) }
        assertEquals(CodecVectors.MOQ_OK, clean.get())
        expectAllocations(listOf(8L to ALLOC_RETIRED, 8L to ALLOC_RETIRED, 16L to ALLOC_RETIRED), clean.alloc, "all retired")
        for ((at, expected) in listOf(
            0 to listOf(8L to ALLOC_FAILED),
            1 to listOf(8L to ALLOC_RETIRED, 8L to ALLOC_FAILED),
            2 to listOf(8L to ALLOC_RETIRED, 8L to ALLOC_RETIRED, 16L to ALLOC_FAILED),
        )) {
            val o = observed(allocFailAt = at) { TrustRouteProbe.nativeCodecAllocControl(false) }
            assertEquals(CodecVectors.MOQ_ERR_NOMEM, o.get(), "failure at $at")
            expectAllocations(expected, o.alloc, "failure at $at: earlier blocks retired, no later attempt")
        }
        val leaked = observed(allocFailAt = -1) { TrustRouteProbe.nativeCodecAllocControl(true) }
        val named = assertFailsWith<AssertionError> { expectAllocations(listOf(8L to ALLOC_RETIRED, 8L to ALLOC_RETIRED, 16L to ALLOC_RETIRED), leaked.alloc, "omitted free") }
        assertTrue(named.message!!.contains("state 0 live"), named.message)
        expectAllocations(listOf(8L to ALLOC_LIVE, 8L to ALLOC_RETIRED, 16L to ALLOC_RETIRED), leaked.alloc, "the omitted free is reported live and reclaimed")
        val later = observed(allocFailAt = 1) { TrustRouteProbe.nativeCodecAllocControl(true) }
        expectAllocations(listOf(8L to ALLOC_LIVE, 8L to ALLOC_FAILED), later.alloc, "a later failure that forgets the first input is reported live and reclaimed")
    }

    /**
     * The free classification at ONE address, made certain rather than left to
     * the allocator: noted, freed, noted again at the same address and freed
     * again -- two ordinary frees -- then freed a third time (a true double
     * free) and a never-owned address freed (a foreign free).
     */
    @Test
    fun rc7_aReusedAddressIsAnOrdinaryFreeAndTrueDoubleAndForeignFreesAreStillCaught() {
        val o = observed(allocFailAt = -1) { TrustRouteProbe.nativeCodecAllocReuseControl() }
        assertContentEquals(intArrayOf(1, 1, 0, 0, 0), o.get(), "retire, retire the reused owner, double, foreign")
        val ledger = assertNotNull(o.alloc)
        assertEquals(listOf(16L to ALLOC_RETIRED, 16L to ALLOC_RETIRED), ledger.entries, "both owners of the one address retired")
        assertEquals(1L, ledger.doubleFrees, "exactly the true double free")
        assertEquals(1L, ledger.foreignFrees, "exactly the foreign free")
        assertEquals(0L, ledger.reclaimed)
        // Two legitimate allocate/free cycles under ONE arm: whether or not the
        // allocator reuses addresses, nothing is a double or foreign free.
        val twice = observed(allocFailAt = -1) { listOf(TrustRouteProbe.nativeCodecAllocControl(false), TrustRouteProbe.nativeCodecAllocControl(false)) }
        assertEquals(listOf(CodecVectors.MOQ_OK, CodecVectors.MOQ_OK), twice.get())
        expectAllocations(List(2) { listOf(8L to ALLOC_RETIRED, 8L to ALLOC_RETIRED, 16L to ALLOC_RETIRED) }.flatten(), twice.alloc, "two cycles, one arm")
    }

    /** A declared exception, and a failed assertion, after owned allocations: the scoped observer still settles, frees every held block, and the next arm is clean. */
    @Test
    fun rc8_theScopedObserverSettlesAndReclaimsAfterAThrowAndAFailedAssertion() {
        val declared = Declared("the declared failure after an owned allocation")
        val thrown =
            observed(allocFailAt = -1) {
                assertEquals(CodecVectors.MOQ_OK, TrustRouteProbe.nativeCodecAllocHold())
                assertEquals(CodecVectors.MOQ_OK, TrustRouteProbe.nativeCodecAllocHold())
                throw declared
            }
        assertSame(declared, thrown.result.exceptionOrNull(), "the original Throwable is kept as itself")
        expectAllocations(listOf(32L to ALLOC_LIVE, 32L to ALLOC_LIVE), thrown.alloc, "both held blocks, each freed after the throw")
        val asserted = observed(allocFailAt = -1) { assertEquals(CodecVectors.MOQ_OK, TrustRouteProbe.nativeCodecAllocHold()); assertEquals(1, 2, "the declared failed assertion") }
        assertIs<AssertionError>(asserted.result.exceptionOrNull())
        expectAllocations(listOf(32L to ALLOC_LIVE), asserted.alloc, "the held block, reclaimed after the failed assertion")
        val next = observed(allocFailAt = -1) { Unit }
        expectAllocations(emptyList(), next.alloc, "the next arm starts clean")
    }

    /** Runs the ledger child [scenario] (its first argument) and retires it, whatever happens. */
    private fun ledgerChild(scenario: String): EndpointAbi.ChildOutcome =
        EndpointAbi.awaitOwnedChild(EndpointAbi.startChild(scenario, null, mainClass = CodecLedgerChildMain::class.java.name))

    private fun facts(lines: List<String>): Map<String, String> =
        lines.mapNotNull { l -> l.indexOf(' ').takeIf { it > 0 }?.let { l.substring(0, it) to l.substring(it + 1) } }.toMap()

    /**
     * A second arm, and a disarm or arm from another thread, are refused without
     * touching the owner's inventory; that thread's own allocations and frees stay
     * out of it; the owner settles and reclaims only after that thread is joined.
     * The two threads live in a child JVM this test owns, retired whatever happens.
     */
    @Test
    fun rc9_aRejectedRearmOrForeignDisarmLeavesTheOwnersInventoryAndCleanup() {
        val f = facts(EndpointAbi.requireOwnedChild(ledgerChild("isolation"), EndpointAbi.CHILD_CEILING_MILLIS))
        assertEquals("0", f["HOLD"], "the owner held a block")
        assertEquals("false", f["SECOND_ARM"], "a second arm is refused")
        assertEquals("null", f["FOREIGN_DISARM"], "another thread cannot settle the owner's inventory")
        assertEquals("false", f["FOREIGN_ARM"], "nor replace it")
        assertEquals("0", f["FOREIGN_WORK"], "its own allocate/free cycle ran unobserved")
        assertEquals("0", f["OWNER_WORK"], "the owner keeps allocating into the same inventory")
        assertEquals("ok", f["RESULT"])
        assertEquals("null", f["FAULT"], "no instrumentation fault in the child")
        val ledger = AllocLedger.of(assertNotNull(f["LEDGER"], "the ledger was settled").split(' ').map { it.toLong() }.toLongArray())
        expectAllocations(listOf(32L to ALLOC_LIVE, 8L to ALLOC_RETIRED, 8L to ALLOC_RETIRED, 16L to ALLOC_RETIRED), ledger, "the first inventory intact, its held block reclaimed")
    }

    /**
     * The one oracle for a child whose worker could not be retired: the child
     * reported the live worker with its inventory still armed and ended
     * WITHOUT settling it -- no ledger, no body result -- and this JVM reaped
     * its own child and judges the outcome a failure, not a cleanup.
     */
    private fun assertUnsettledAndJudgedFailed(outcome: EndpointAbi.ChildOutcome, reachedFact: String) {
        assertTrue(outcome.reaped, "the child was reaped")
        assertEquals(CodecLedgerChildMain.EXIT_WORKER_LIVE, outcome.exitStatus, outcome.diagnostics)
        val f = facts(outcome.output)
        assertEquals("true", f["WORKER_HELD"], outcome.diagnostics)
        assertEquals("true", f[reachedFact], "the scenario reached its own boundary: $reachedFact\n${outcome.diagnostics}")
        assertEquals("true", f["WORKER_LIVE"], "the worker was still live at the bound")
        assertEquals("true", f["STILL_ARMED"], "the inventory was still armed when the child ended")
        assertNull(f["LEDGER"], "nothing settled the inventory while the worker was live\n${outcome.diagnostics}")
        assertNull(f["RESULT"], "the scoped body never completed")
        val judged = assertFailsWith<AssertionError> { EndpointAbi.requireOwnedChild(outcome, EndpointAbi.CHILD_CEILING_MILLIS) }
        assertTrue(judged.message!!.contains("it exited ${CodecLedgerChildMain.EXIT_WORKER_LIVE}"), judged.message)
    }

    /** The unjoinable-worker branch, taken by construction: the child's worker waits on a latch nothing releases and the join bound expires. */
    @Test
    fun rc10_anUnjoinableWorkerLeavesTheInventoryUnsettledAndItsChildIsReapedAndJudgedFailed() {
        assertUnsettledAndJudgedFailed(ledgerChild("held"), "WORKER_HELD")
    }

    /** The owner's join is INTERRUPTED while its worker is held: the unwind still retires the worker first, and cannot. */
    @Test
    fun rc11_anInterruptedOwnerDoesNotSettleWhileItsWorkerIsLive() {
        assertUnsettledAndJudgedFailed(ledgerChild("held-interrupt"), "OWNER_INTERRUPTED")
    }

    /** A declared exception after the worker started: the same retirement guard, not an interruption special case. */
    @Test
    fun rc12_aThrowingOwnerDoesNotSettleWhileItsWorkerIsLive() {
        assertUnsettledAndJudgedFailed(ledgerChild("held-throw"), "OWNER_THROWING")
    }
}
