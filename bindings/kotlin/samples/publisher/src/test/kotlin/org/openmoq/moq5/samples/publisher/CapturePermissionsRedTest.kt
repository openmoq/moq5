package org.openmoq.moq5.samples.publisher

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * The runtime permission flow, as a contract rather than as one implementation.
 *
 * Every row below is written against the [PermissionFlow] INTERFACE and run
 * twice: once against the flow this sample will ship, which is a Phase A
 * declaration and therefore stops at a named `TODO`, and once against a
 * deliberately weak stand-in that settles the right NUMBER of results while
 * getting the generation, the duplicate and the permanent-denial derivation
 * wrong. The second run is what proves a row is not satisfied by counting.
 *
 * Nothing here touches an Activity, a dialog, or a device: the grant result and
 * the platform's own "may I still explain?" fact are passed in.
 */
class CapturePermissionsRedTest {

    private val granted = PermissionFacts(granted = true, mayExplain = false)
    private val deniedOnce = PermissionFacts(granted = false, mayExplain = true)
    private val deniedForGood = PermissionFacts(granted = false, mayExplain = false)

    private val both = setOf(CapturePermission.Camera, CapturePermission.Microphone)

    /**
     * A flow that settles results and nothing more: it never checks the
     * generation, treats every arrival as fresh, and calls one denial
     * permanent. It reaches the same states after the same number of results,
     * which is exactly why the rows must not be counting them.
     */
    private class CountingPermissionFlow(override val generation: Long) : PermissionFlow {
        private val states =
            CapturePermission.entries.associateWith { PermissionState.NotRequested }.toMutableMap()

        override fun stateOf(permission: CapturePermission): PermissionState = states.getValue(permission)

        override fun request(permissions: Set<CapturePermission>): Set<CapturePermission> {
            permissions.forEach { states[it] = PermissionState.Requested }
            return permissions
        }

        override fun onResult(
            generation: Long,
            permission: CapturePermission,
            facts: PermissionFacts,
        ): PermissionOutcome {
            val state =
                when {
                    facts.granted -> PermissionState.Granted
                    else -> PermissionState.PermanentlyDenied
                }
            states[permission] = state
            return PermissionOutcome.Settled(permission, state)
        }

        override fun retire(reason: String) = Unit

        override fun readiness(): CaptureReadiness =
            when {
                states.values.all { it == PermissionState.Granted } -> CaptureReadiness.Ready
                states.values.any {
                    it == PermissionState.Denied || it == PermissionState.PermanentlyDenied
                } -> CaptureReadiness.Refused("denied")
                else -> CaptureReadiness.Waiting
            }
    }

    // ---- the contract rows, over any implementation ---------------------------

    private fun p1_startsUnrequestedAndWaiting(make: (Long) -> PermissionFlow) {
        val flow = make(1L)
        for (permission in CapturePermission.entries) {
            assertEquals(PermissionState.NotRequested, flow.stateOf(permission), "$permission starts unrequested")
        }
        assertIs<CaptureReadiness.Waiting>(flow.readiness(), "nothing is refused before anything is asked")
    }

    private fun p2_asksForWhatMayStillBeAsked(make: (Long) -> PermissionFlow) {
        val flow = make(1L)
        assertEquals(both, flow.request(both), "both permissions are asked for the first time")
        assertEquals(
            emptySet(),
            flow.request(both),
            "a request already outstanding is not asked again; the platform would show nothing and the retry spins",
        )
    }

    private fun p3_partialGrantIsNotReadiness(make: (Long) -> PermissionFlow) {
        val flow = make(1L)
        flow.request(both)
        assertEquals(
            PermissionOutcome.Settled(CapturePermission.Camera, PermissionState.Granted),
            flow.onResult(1L, CapturePermission.Camera, granted),
        )
        assertIs<CaptureReadiness.Waiting>(
            flow.readiness(),
            "one of two permissions is not permission to capture",
        )
        flow.onResult(1L, CapturePermission.Microphone, granted)
        assertIs<CaptureReadiness.Ready>(flow.readiness(), "both granted is ready")
    }

    private fun p4_oneDenialIsNotPermanent(make: (Long) -> PermissionFlow) {
        val flow = make(1L)
        flow.request(both)
        flow.onResult(1L, CapturePermission.Camera, deniedOnce)
        assertEquals(
            PermissionState.Denied,
            flow.stateOf(CapturePermission.Camera),
            "the platform will still explain, so this denial can be asked again; " +
                "calling it permanent stops asking a user who said not now",
        )
        assertEquals(
            setOf(CapturePermission.Camera),
            flow.request(both),
            "an ordinary denial may be re-asked; the outstanding microphone request may not",
        )
    }

    private fun p5_permanentDenialIsDerivedAndNeverReAsked(make: (Long) -> PermissionFlow) {
        val flow = make(1L)
        flow.request(both)
        flow.onResult(1L, CapturePermission.Camera, deniedForGood)
        assertEquals(
            PermissionState.PermanentlyDenied,
            flow.stateOf(CapturePermission.Camera),
            "denied with no rationale left is the platform's own permanent denial",
        )
        assertEquals(
            emptySet(),
            flow.request(setOf(CapturePermission.Camera)),
            "a permanently denied permission is never re-asked; the dialog would not appear",
        )
        assertIs<CaptureReadiness.Refused>(flow.readiness())
    }

    private fun p6_aDuplicateResultSettlesNothingTwice(make: (Long) -> PermissionFlow) {
        val flow = make(1L)
        flow.request(both)
        flow.onResult(1L, CapturePermission.Camera, granted)
        assertIs<PermissionOutcome.Ignored>(
            flow.onResult(1L, CapturePermission.Camera, deniedForGood),
            "a second result for a settled permission is a duplicate, not a revocation",
        )
        assertEquals(PermissionState.Granted, flow.stateOf(CapturePermission.Camera))
    }

    private fun p7_aResultNobodyAskedForIsIgnored(make: (Long) -> PermissionFlow) {
        val flow = make(1L)
        flow.request(setOf(CapturePermission.Camera))
        assertIs<PermissionOutcome.Ignored>(
            flow.onResult(1L, CapturePermission.Microphone, granted),
            "an out-of-order result for a permission this flow never asked for settles nothing",
        )
        assertEquals(PermissionState.NotRequested, flow.stateOf(CapturePermission.Microphone))
    }

    private fun p8_aRetiredGenerationCannotBecomeReady(make: (Long) -> PermissionFlow) {
        val flow = make(7L)
        flow.request(both)
        flow.retire("the owner was destroyed while the dialog was open")
        assertIs<PermissionOutcome.Ignored>(
            flow.onResult(7L, CapturePermission.Camera, granted),
            "a result that arrives after retirement settles nothing",
        )
        flow.onResult(7L, CapturePermission.Microphone, granted)
        assertTrue(
            flow.readiness() !is CaptureReadiness.Ready,
            "no permission result may start a retired session generation",
        )
    }

    private fun p9_aResultForAnotherGenerationIsIgnored(make: (Long) -> PermissionFlow) {
        val flow = make(7L)
        flow.request(both)
        assertIs<PermissionOutcome.Ignored>(
            flow.onResult(6L, CapturePermission.Camera, granted),
            "this result belongs to a request a previous generation made",
        )
        assertEquals(
            PermissionState.Requested,
            flow.stateOf(CapturePermission.Camera),
            "a stale result leaves the outstanding request exactly as it was",
        )
    }

    /**
     * The same contract expressed as TABLES rather than branches: what may be
     * asked, what a result settles to, and what refuses readiness. It is a
     * comparator, not a copy of the production expression.
     */
    private class TablePermissionModel(override val generation: Long) : PermissionFlow {
        private val askable = setOf(PermissionState.NotRequested, PermissionState.Denied)
        private val settlesTo =
            mapOf(
                (true to true) to PermissionState.Granted,
                (true to false) to PermissionState.Granted,
                (false to true) to PermissionState.Denied,
                (false to false) to PermissionState.PermanentlyDenied,
            )
        private val refusing = setOf(PermissionState.Denied, PermissionState.PermanentlyDenied)
        private val lock = Any()
        private val states = CapturePermission.entries.associateWithTo(mutableMapOf()) { PermissionState.NotRequested }
        private var retired = false

        override fun stateOf(permission: CapturePermission): PermissionState = synchronized(lock) { states.getValue(permission) }

        override fun request(permissions: Set<CapturePermission>): Set<CapturePermission> =
            synchronized(lock) {
                if (retired) emptySet() else permissions.filter { states.getValue(it) in askable }.toSet()
                    .also { asked -> asked.forEach { states[it] = PermissionState.Requested } }
            }

        override fun onResult(generation: Long, permission: CapturePermission, facts: PermissionFacts): PermissionOutcome =
            synchronized(lock) {
                when {
                    retired -> PermissionOutcome.Ignored("retired")
                    generation != this.generation -> PermissionOutcome.Ignored("another generation")
                    states.getValue(permission) != PermissionState.Requested -> PermissionOutcome.Ignored("not outstanding")
                    else -> {
                        val settled = settlesTo.getValue(facts.granted to facts.mayExplain)
                        states[permission] = settled
                        PermissionOutcome.Settled(permission, settled)
                    }
                }
            }

        override fun retire(reason: String) {
            synchronized(lock) { retired = true }
        }

        override fun readiness(): CaptureReadiness =
            synchronized(lock) {
                val refused = states.entries.firstOrNull { it.value in refusing }
                when {
                    retired -> CaptureReadiness.Refused("retired")
                    refused != null -> CaptureReadiness.Refused("${refused.key}")
                    states.values.all { it == PermissionState.Granted } -> CaptureReadiness.Ready
                    else -> CaptureReadiness.Waiting
                }
            }
    }

    /** An empty ask asks nothing; a subset asks exactly that subset. */
    private fun p10_emptyAndSubsetRequests(make: (Long) -> PermissionFlow) {
        val flow = make(1L)
        assertEquals(emptySet(), flow.request(emptySet()), "asking for nothing asks for nothing")
        for (permission in CapturePermission.entries) {
            assertEquals(PermissionState.NotRequested, flow.stateOf(permission), "and changes no state")
        }
        assertEquals(
            setOf(CapturePermission.Microphone),
            flow.request(setOf(CapturePermission.Microphone)),
            "a subset asks exactly that subset",
        )
        assertEquals(PermissionState.Requested, flow.stateOf(CapturePermission.Microphone))
        assertEquals(PermissionState.NotRequested, flow.stateOf(CapturePermission.Camera), "and leaves the other alone")
    }

    /** All four grant/rationale combinations, by exact per-permission state. */
    private fun p11_everyGrantAndRationaleCombination(make: (Long) -> PermissionFlow) {
        val cases =
            listOf(
                PermissionFacts(granted = true, mayExplain = false) to PermissionState.Granted,
                PermissionFacts(granted = true, mayExplain = true) to PermissionState.Granted,
                PermissionFacts(granted = false, mayExplain = true) to PermissionState.Denied,
                PermissionFacts(granted = false, mayExplain = false) to PermissionState.PermanentlyDenied,
            )
        for ((facts, expected) in cases) {
            val flow = make(1L)
            flow.request(setOf(CapturePermission.Camera))
            assertEquals(
                PermissionOutcome.Settled(CapturePermission.Camera, expected),
                flow.onResult(1L, CapturePermission.Camera, facts),
                "granted=${facts.granted} mayExplain=${facts.mayExplain} settles to $expected",
            )
            assertEquals(expected, flow.stateOf(CapturePermission.Camera), "and that is the state it keeps")
            assertEquals(
                PermissionState.NotRequested,
                flow.stateOf(CapturePermission.Microphone),
                "a grant is a grant whatever the rationale says, and the other permission is untouched",
            )
        }
    }

    /** A denial beside an outstanding request: exact states, and only the denial is re-asked. */
    private fun p12_mixedDeniedAndPending(make: (Long) -> PermissionFlow) {
        val flow = make(1L)
        flow.request(both)
        flow.onResult(1L, CapturePermission.Camera, deniedOnce)
        assertEquals(PermissionState.Denied, flow.stateOf(CapturePermission.Camera))
        assertEquals(PermissionState.Requested, flow.stateOf(CapturePermission.Microphone), "the other request is still outstanding")
        assertIs<CaptureReadiness.Refused>(flow.readiness(), "a denial refuses even while something is pending")
        assertEquals(setOf(CapturePermission.Camera), flow.request(both), "only the ordinary denial is asked again")
        assertEquals(PermissionState.Requested, flow.stateOf(CapturePermission.Camera), "and it is outstanding again")
        assertEquals(
            PermissionOutcome.Settled(CapturePermission.Camera, PermissionState.Granted),
            flow.onResult(1L, CapturePermission.Camera, granted),
            "the re-asked permission settles on its next result",
        )
        assertIs<CaptureReadiness.Waiting>(flow.readiness(), "and the still-outstanding microphone is what is left")
    }

    /** Retirement is absorbing, before and after a full set of grants. */
    private fun p13_retirementIsAbsorbing(make: (Long) -> PermissionFlow) {
        val pending = make(1L)
        pending.request(both)
        pending.retire("the owner was destroyed")
        assertEquals(emptySet(), pending.request(both), "a retired flow asks for nothing")
        assertIs<CaptureReadiness.Refused>(pending.readiness(), "and a retired flow refuses readiness")

        val allGranted = make(1L)
        allGranted.request(both)
        for (permission in CapturePermission.entries) allGranted.onResult(1L, permission, granted)
        assertIs<CaptureReadiness.Ready>(allGranted.readiness(), "both grants are readiness")
        allGranted.retire("the owner was destroyed after both grants")
        assertTrue(allGranted.readiness() !is CaptureReadiness.Ready, "retirement takes that back and nothing restores it")
        assertEquals(emptySet(), allGranted.request(both), "and nothing may be asked again")
        assertIs<PermissionOutcome.Ignored>(
            allGranted.onResult(1L, CapturePermission.Camera, granted),
            "not even a repeated grant revives it",
        )
    }

    private fun runContract(make: (Long) -> PermissionFlow) {
        p1_startsUnrequestedAndWaiting(make)
        p2_asksForWhatMayStillBeAsked(make)
        p3_partialGrantIsNotReadiness(make)
        p4_oneDenialIsNotPermanent(make)
        p5_permanentDenialIsDerivedAndNeverReAsked(make)
        p6_aDuplicateResultSettlesNothingTwice(make)
        p7_aResultNobodyAskedForIsIgnored(make)
        p8_aRetiredGenerationCannotBecomeReady(make)
        p9_aResultForAnotherGenerationIsIgnored(make)
        p10_emptyAndSubsetRequests(make)
        p11_everyGrantAndRationaleCombination(make)
        p12_mixedDeniedAndPending(make)
        p13_retirementIsAbsorbing(make)
    }

    // ---- RED: the flow this sample will ship ---------------------------------

    @Test
    fun p_thePermissionFlowMeetsItsContract() {
        runContract { generation -> CapturePermissionFlow(generation) }
    }

    @Test
    fun s_theContractIsSatisfiableByASeparatelyExpressedModel() {
        runContract { generation -> TablePermissionModel(generation) }
    }

    /**
     * Retirement versus a result in flight, ordered by gates rather than by
     * timing: the retirement happens first, and the result that follows it
     * settles nothing on either implementation.
     */
    @Test
    fun p14_aResultThatLosesTheRaceToRetirementSettlesNothing() {
        for (make in listOf<(Long) -> PermissionFlow>({ CapturePermissionFlow(it) }, { TablePermissionModel(it) })) {
            val flow = make(7L)
            flow.request(both)
            val retired = HostGate("the row retired the generation")
            val outcome = java.util.concurrent.atomic.AtomicReference<PermissionOutcome?>(null)
            val worker =
                HostWorker("permission-result") {
                    retired.await()
                    outcome.set(flow.onResult(7L, CapturePermission.Camera, granted))
                }.start()
            withSettlement(listOf(retired), listOf(worker)) {
                flow.retire("the dialog was cancelled")
                retired.openFully()
                worker.join()
                assertIs<PermissionOutcome.Ignored>(outcome.get(), "a result delivered after retirement settles nothing")
                assertEquals(PermissionState.Requested, flow.stateOf(CapturePermission.Camera), "and the snapshot is untouched")
                assertTrue(flow.readiness() !is CaptureReadiness.Ready, "retirement cannot be undone")
            }
        }
    }

    // ---- discrimination: a counts-only flow must NOT satisfy these rows -------

    @Test
    fun m1_aFlowThatCallsOneDenialPermanentFails() {
        val failure = assertFailsWithAssertion { p4_oneDenialIsNotPermanent { CountingPermissionFlow(it) } }
        assertTrue(
            "not now" in failure,
            "the row must reject a guessed permanent denial, not merely count results: $failure",
        )
    }

    @Test
    fun m2_aFlowThatReSettlesADuplicateFails() {
        assertFailsWithAssertion { p6_aDuplicateResultSettlesNothingTwice { CountingPermissionFlow(it) } }
    }

    @Test
    fun m3_aFlowThatIgnoresTheGenerationFails() {
        assertFailsWithAssertion { p8_aRetiredGenerationCannotBecomeReady { CountingPermissionFlow(it) } }
        assertFailsWithAssertion { p9_aResultForAnotherGenerationIsIgnored { CountingPermissionFlow(it) } }
    }

    private fun assertFailsWithAssertion(body: () -> Unit): String {
        val thrown =
            try {
                body()
                null
            } catch (assertion: AssertionError) {
                assertion
            }
        assertTrue(
            thrown != null,
            "this row is satisfied by an implementation that only counts results; it proves nothing",
        )
        return thrown?.message.orEmpty()
    }
}
