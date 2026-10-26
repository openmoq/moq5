package org.openmoq.moq5.samples.publisher

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * The capture-stamp correlation, which exists because
 * `onOutputBufferAvailable(codec, index, BufferInfo)` carries no capture instant.
 *
 * Every ledger row is written against the [CaptureLedger] INTERFACE, on BOTH
 * tracks, and run against the Phase A declaration (RED), the correct host
 * ledger (must PASS), and a nearest-neighbour stand-in that records and returns
 * the same NUMBER of observations.
 *
 * The audio rows below declare the key authority this sample uses -- unit
 * framing, integer keys with a carried remainder, first-frame instants -- and
 * what it refuses. They do NOT establish that a given encoder's output
 * `presentationTimeUs` equals the queued input's: that is a device fact for
 * Stage C, and the contract's answer when it does not hold is a refusal.
 */
class CaptureCorrelationRedTest {

    private fun obs(key: Long, nanos: Long) = CaptureObservation(key, nanos)

    private fun other(kind: MediaKind) = if (kind == MediaKind.Video) MediaKind.Audio else MediaKind.Video

    /** Same totals, wrong facts: nearest key, any track, any generation, re-served. */
    private class NearestCountingLedger(override val kind: MediaKind, override val generation: Long, override val capacity: Int) : CaptureLedger {
        private val recorded = mutableListOf<CaptureObservation>()

        override val outstanding: Int get() = recorded.size

        override fun observe(generation: Long, kind: MediaKind, observation: CaptureObservation): ObserveOutcome {
            if (recorded.size >= capacity) return ObserveOutcome.Refused("full")
            recorded.add(observation)
            return ObserveOutcome.Recorded
        }

        override fun claim(generation: Long, kind: MediaKind, key: Long): ClaimOutcome {
            val nearest = recorded.minByOrNull { kotlin.math.abs(it.key - key) } ?: return ClaimOutcome.Missing("nothing recorded")
            return ClaimOutcome.Matched(nearest.captureElapsedNanos)
        }

        override fun retire(reason: String) = Unit
    }

    // ---- the ledger rows, on any track ----------------------------------------

    private fun c1_claimedExactlyOnceByItsOwnKey(make: (MediaKind) -> CaptureLedger, k: MediaKind) {
        val ledger = make(k)
        assertEquals(ObserveOutcome.Recorded, ledger.observe(1L, k, obs(33_333L, 900_000_000L)))
        assertEquals(ClaimOutcome.Matched(900_000_000L), ledger.claim(1L, k, 33_333L), "a claim returns the instant filed under that exact key")
        assertIs<ClaimOutcome.Missing>(ledger.claim(1L, k, 33_333L), "consumed once; serving it twice stamps two frames with one capture")
        assertEquals(0, ledger.outstanding)
    }

    private fun c2_unmatchedKeyIsARefusalNotANeighbour(make: (MediaKind) -> CaptureLedger, k: MediaKind) {
        val ledger = make(k)
        ledger.observe(1L, k, obs(33_333L, 900_000_000L))
        assertIs<ClaimOutcome.Missing>(ledger.claim(1L, k, 33_334L), "one microsecond away is a DIFFERENT frame")
        assertEquals(1, ledger.outstanding, "a refused claim consumes nothing")
    }

    private fun c3_outputBeforeObservationIsMissing(make: (MediaKind) -> CaptureLedger, k: MediaKind) {
        assertIs<ClaimOutcome.Missing>(make(k).claim(1L, k, 0L))
    }

    private fun c4_duplicateKeyRefusedNotOverwritten(make: (MediaKind) -> CaptureLedger, k: MediaKind) {
        val ledger = make(k)
        ledger.observe(1L, k, obs(10L, 100L))
        assertIs<ObserveOutcome.Refused>(ledger.observe(1L, k, obs(10L, 999L)), "two captures cannot share one timestamp")
        assertEquals(ClaimOutcome.Matched(100L), ledger.claim(1L, k, 10L), "the first survives")
    }

    private fun c5_claimingDiscardsStrictlyOlder(make: (MediaKind) -> CaptureLedger, k: MediaKind) {
        // Conditional on this sample's declared no-reordering encoder contract.
        val ledger = make(k)
        ledger.observe(1L, k, obs(10L, 100L))
        ledger.observe(1L, k, obs(20L, 200L))
        assertEquals(ClaimOutcome.Matched(200L), ledger.claim(1L, k, 20L))
        assertEquals(0, ledger.outstanding, "the encoder that emitted 20 will never ask for 10 under this contract")
        assertIs<ClaimOutcome.Missing>(ledger.claim(1L, k, 10L))
    }

    private fun c6_fullLedgerRefusesByName(make: (MediaKind) -> CaptureLedger, k: MediaKind) {
        val ledger = make(k)
        for (i in 0 until CAPACITY) assertEquals(ObserveOutcome.Recorded, ledger.observe(1L, k, obs(i.toLong(), i.toLong())))
        assertIs<ObserveOutcome.Refused>(ledger.observe(1L, k, obs(99L, 99L)), "a full ledger refuses the NEW observation")
        assertEquals(ClaimOutcome.Matched(0L), ledger.claim(1L, k, 0L), "overflow refuses the NEW observation, evicts nothing")
    }

    private fun c7_wrongTrackCannotObserveOrClaim(make: (MediaKind) -> CaptureLedger, k: MediaKind) {
        val ledger = make(k)
        assertIs<ObserveOutcome.Refused>(ledger.observe(1L, other(k), obs(5L, 50L)))
        ledger.observe(1L, k, obs(5L, 50L))
        assertIs<ClaimOutcome.Missing>(ledger.claim(1L, other(k), 5L))
    }

    private fun c8_retiredOrForeignGenerationSettlesNothing(make: (MediaKind) -> CaptureLedger, k: MediaKind) {
        val ledger = make(k)
        ledger.observe(1L, k, obs(5L, 50L))
        assertIs<ObserveOutcome.Ignored>(ledger.observe(0L, k, obs(6L, 60L)))
        assertIs<ClaimOutcome.Ignored>(ledger.claim(0L, k, 5L))
        ledger.retire("the session stopped")
        assertIs<ClaimOutcome.Ignored>(ledger.claim(1L, k, 5L))
    }

    private fun c9_aKeyObservedAgainAfterConsumptionIsRefused(make: (MediaKind) -> CaptureLedger, k: MediaKind) {
        val ledger = make(k)
        ledger.observe(1L, k, obs(10L, 100L))
        assertEquals(ClaimOutcome.Matched(100L), ledger.claim(1L, k, 10L))
        assertIs<ObserveOutcome.Refused>(ledger.observe(1L, k, obs(10L, 101L)), "the encoder already asked for 10; a second capture under it would stamp a frame that has been published")
        assertIs<ClaimOutcome.Missing>(ledger.claim(1L, k, 10L))
    }

    /** The ledger is ordered by KEY, not by the order the callbacks arrived in. */
    private fun c10_observationsMayArriveInAnyOrder(make: (MediaKind) -> CaptureLedger, k: MediaKind) {
        val ledger = make(k)
        for ((key, nanos) in listOf(30L to 300L, 10L to 100L, 20L to 200L)) {
            assertEquals(ObserveOutcome.Recorded, ledger.observe(1L, k, obs(key, nanos)), "arrival order does not decide what is recordable")
        }
        assertEquals(ClaimOutcome.Matched(100L), ledger.claim(1L, k, 10L), "the earliest key is claimable first, however it arrived")
        assertEquals(2, ledger.outstanding, "and nothing later was discarded by it")
        assertEquals(ClaimOutcome.Matched(300L), ledger.claim(1L, k, 30L), "a later key claims its own instant")
        assertEquals(0, ledger.outstanding, "which then discards the one between as strictly older")
    }

    /**
     * The settled-key boundary: nothing at or below a successful claim may be
     * observed again -- including a key that was DISCARDED as older rather than
     * claimed. This is what keeps the state bounded without a consumed-key set.
     */
    private fun c11_staleObservationsAtOrBelowTheSettledKeyAreRefused(make: (MediaKind) -> CaptureLedger, k: MediaKind) {
        val ledger = make(k)
        ledger.observe(1L, k, obs(10L, 100L))
        ledger.observe(1L, k, obs(20L, 200L))
        assertEquals(ClaimOutcome.Matched(200L), ledger.claim(1L, k, 20L))
        assertIs<ObserveOutcome.Refused>(ledger.observe(1L, k, obs(20L, 999L)), "the settled key itself is not observable again")
        assertIs<ObserveOutcome.Refused>(ledger.observe(1L, k, obs(10L, 999L)), "nor is a key that was discarded as older than it")
        assertIs<ObserveOutcome.Refused>(ledger.observe(1L, k, obs(1L, 999L)), "nor any key below the boundary")
        assertEquals(ObserveOutcome.Recorded, ledger.observe(1L, k, obs(21L, 210L)), "but a later key is ordinary")
        assertEquals(1, ledger.outstanding)
    }

    /** A claim that matches nothing changes nothing -- it is not a new boundary. */
    private fun c12_aMissingClaimLeavesTheStateUntouched(make: (MediaKind) -> CaptureLedger, k: MediaKind) {
        val ledger = make(k)
        ledger.observe(1L, k, obs(10L, 100L))
        ledger.observe(1L, k, obs(20L, 200L))
        assertIs<ClaimOutcome.Missing>(ledger.claim(1L, k, 50L), "nothing is filed under 50")
        assertEquals(2, ledger.outstanding, "a miss consumes nothing")
        assertEquals(ObserveOutcome.Recorded, ledger.observe(1L, k, obs(15L, 150L)), "and sets no boundary: 15 is still observable")
        assertEquals(ClaimOutcome.Matched(100L), ledger.claim(1L, k, 10L), "every earlier observation is still exactly claimable")
        assertEquals(ClaimOutcome.Matched(150L), ledger.claim(1L, k, 15L))
        assertEquals(ClaimOutcome.Matched(200L), ledger.claim(1L, k, 20L))
    }

    /** A full ledger recovers its capacity as claims consume and discard. */
    private fun c13_capacityRecoversAfterAClaim(make: (MediaKind) -> CaptureLedger, k: MediaKind) {
        val ledger = make(k)
        for (i in 0 until CAPACITY) assertEquals(ObserveOutcome.Recorded, ledger.observe(1L, k, obs(i.toLong(), i.toLong())))
        assertIs<ObserveOutcome.Refused>(ledger.observe(1L, k, obs(100L, 100L)), "full")
        assertEquals(ClaimOutcome.Matched((CAPACITY - 1).toLong()), ledger.claim(1L, k, (CAPACITY - 1).toLong()), "the newest claims its own instant")
        assertEquals(0, ledger.outstanding, "and the rest went as strictly older")
        assertEquals(ObserveOutcome.Recorded, ledger.observe(1L, k, obs(100L, 100L)), "so the capacity is available again")
    }

    /** Retirement settles nothing afterwards, in either direction. */
    private fun c14_retirementEndsObservationAndClaiming(make: (MediaKind) -> CaptureLedger, k: MediaKind) {
        val ledger = make(k)
        ledger.observe(1L, k, obs(10L, 100L))
        ledger.retire("the session stopped")
        assertIs<ObserveOutcome.Ignored>(ledger.observe(1L, k, obs(20L, 200L)), "a retired ledger records nothing")
        assertIs<ClaimOutcome.Ignored>(ledger.claim(1L, k, 10L), "and serves nothing, not even what it held")
    }

    private fun runContract(make: (MediaKind) -> CaptureLedger) {
        for (k in MediaKind.entries) {
            c1_claimedExactlyOnceByItsOwnKey(make, k)
            c2_unmatchedKeyIsARefusalNotANeighbour(make, k)
            c3_outputBeforeObservationIsMissing(make, k)
            c4_duplicateKeyRefusedNotOverwritten(make, k)
            c5_claimingDiscardsStrictlyOlder(make, k)
            c6_fullLedgerRefusesByName(make, k)
            c7_wrongTrackCannotObserveOrClaim(make, k)
            c8_retiredOrForeignGenerationSettlesNothing(make, k)
            c9_aKeyObservedAgainAfterConsumptionIsRefused(make, k)
            c10_observationsMayArriveInAnyOrder(make, k)
            c11_staleObservationsAtOrBelowTheSettledKeyAreRefused(make, k)
            c12_aMissingClaimLeavesTheStateUntouched(make, k)
            c13_capacityRecoversAfterAClaim(make, k)
            c14_retirementEndsObservationAndClaiming(make, k)
        }
    }

    private val real: (MediaKind) -> CaptureLedger = { k -> BoundedCaptureLedger(k, generation = 1L, capacity = CAPACITY) }
    private val counting: (MediaKind) -> CaptureLedger = { k -> NearestCountingLedger(k, 1L, CAPACITY) }

    /**
     * The host ledger, extended with the SETTLED-KEY boundary c9/c11 pin: one
     * Long, not a set that grows for the life of the broadcast.
     */
    private class HostLedgerWithBoundary(kind: MediaKind) : CaptureLedger {
        private val inner = HostLedger(kind, 1L, CAPACITY)
        private var settled: Long? = null
        override val kind: MediaKind get() = inner.kind
        override val generation: Long get() = inner.generation
        override val capacity: Int get() = inner.capacity
        override val outstanding: Int get() = inner.outstanding

        override fun observe(generation: Long, kind: MediaKind, observation: CaptureObservation): ObserveOutcome {
            val boundary = settled
            if (generation == this.generation && kind == this.kind && boundary != null && observation.key <= boundary) {
                return ObserveOutcome.Refused("at or below the settled key $boundary")
            }
            return inner.observe(generation, kind, observation)
        }

        override fun claim(generation: Long, kind: MediaKind, key: Long): ClaimOutcome =
            inner.claim(generation, kind, key).also {
                if (it is ClaimOutcome.Matched) {
                    val boundary = settled
                    if (boundary == null || key > boundary) settled = key
                }
            }

        override fun retire(reason: String) = inner.retire(reason)
    }

    @Test
    fun c_theLedgerMeetsItsContractOnBothTracks() = runContract(real)

    @Test
    fun s_theLedgerContractIsSatisfiable() = runContract { k -> HostLedgerWithBoundary(k) }

    @Test
    fun m1_aNearestNeighbourMatchFails() = assertFailsWithAssertion { c2_unmatchedKeyIsARefusalNotANeighbour(counting, MediaKind.Video) }

    @Test
    fun m2_reServingAConsumedObservationFails() = assertFailsWithAssertion { c1_claimedExactlyOnceByItsOwnKey(counting, MediaKind.Audio) }

    @Test
    fun m3_ignoringTheTrackFails() = assertFailsWithAssertion { c7_wrongTrackCannotObserveOrClaim(counting, MediaKind.Video) }

    @Test
    fun m4_ignoringTheGenerationFails() = assertFailsWithAssertion { c8_retiredOrForeignGenerationSettlesNothing(counting, MediaKind.Video) }

    @Test
    fun m5_overwritingADuplicateOrConsumedKeyFails() {
        assertFailsWithAssertion { c4_duplicateKeyRefusedNotOverwritten(counting, MediaKind.Video) }
        assertFailsWithAssertion { c9_aKeyObservedAgainAfterConsumptionIsRefused(counting, MediaKind.Audio) }
    }

    // ---- the audio key authority --------------------------------------------------

    /** 44.1 kHz, 1024-frame units: 23219.95 us per unit, which a per-unit rounding would drift. */
    private fun a1_keysAreExactIntegerMicrosecondsWithTheRemainderCarried(make: (Int, Int, Long) -> AudioKeying) {
        val keying = make(44_100, 1_024, 5_000_000L)
        assertEquals(AudioKeyOutcome.Key(5_000_000L), keying.keyFor(0L))
        assertEquals(AudioKeyOutcome.Key(5_000_000L + 23_219L), keying.keyFor(1L))
        // 100 * 1024 * 1_000_000 / 44_100 = 2_321_995.46..., floored ONCE, not
        // 100 * 23_219 = 2_321_900: a per-unit rounding would be 95 us adrift.
        assertEquals(
            AudioKeyOutcome.Key(5_000_000L + 2_321_995L),
            keying.keyFor(100L),
            "exactly one floor over the whole expression: a per-unit rounding drifts",
        )
    }

    private fun a2_aUnitIsCompleteOnlyWhenEveryFrameHasBeenRead(make: (Int, Int, Long) -> AudioKeying) {
        val keying = make(48_000, 1_024, 0L)
        assertEquals(emptyList(), keying.onRead(500), "a short read completes no unit: partial audio is not an access unit")
        assertEquals(500, keying.retainedFrames)
        assertEquals(listOf(0L), keying.onRead(600), "the next read completes unit 0")
        assertEquals(76, keying.retainedFrames, "and 76 frames carry into unit 1")
        assertEquals(listOf(1L, 2L), keying.onRead(2_048 - 76), "a long read may complete several units, in order")
        assertEquals(0, keying.retainedFrames)
    }

    private fun a3_unsupportedFramingIsRefusedByName(make: (Int, Int, Long) -> AudioKeying) {
        assertIs<AudioKeyOutcome.Refused>(make(0, 1_024, 0L).keyFor(0L), "no sample rate places no unit")
        assertIs<AudioKeyOutcome.Refused>(make(48_000, 0, 0L).keyFor(0L), "no unit size places no unit")
    }

    private fun a4_aDelayedOutputStillClaimsItsOwnUnit(make: (Int, Int, Long) -> AudioKeying, ledgers: (MediaKind) -> CaptureLedger) {
        // The encoder may hold units before emitting; the ledger retains the
        // observations, so a late claim still matches its own key exactly.
        val keying = make(48_000, 1_024, 0L)
        val ledger = ledgers(MediaKind.Audio)
        for (i in 0L until 3L) {
            val key = assertIs<AudioKeyOutcome.Key>(keying.keyFor(i)).presentationTimeUs
            ledger.observe(1L, MediaKind.Audio, obs(key, 100L + i))
        }
        val key2 = assertIs<AudioKeyOutcome.Key>(keying.keyFor(2L)).presentationTimeUs
        assertEquals(ClaimOutcome.Matched(102L), ledger.claim(1L, MediaKind.Audio, key2), "unit 2, claimed late, is still unit 2")
        assertEquals(
            0,
            ledger.outstanding,
            "units 0 and 1 are STRICTLY OLDER than the claimed key: under this sample's no-reordering contract their own " +
                "outputs can no longer arrive, so they are discarded rather than left to be mis-stamped",
        )
    }

    /**
     * What a coalesced output would actually do here. Its key can match the
     * FIRST unit it carries; the units behind that key are LATER, so the
     * strictly-older discard does not touch them and they stay outstanding.
     * Nothing claims them, nothing re-stamps them, and this sample does not
     * count access units inside an encoded buffer to find out.
     */
    private fun a5_unitsBehindACoalescedKeyAreNotOlderAndAreNotDiscarded(make: (Int, Int, Long) -> AudioKeying, ledgers: (MediaKind) -> CaptureLedger) {
        val keying = make(48_000, 1_024, 0L)
        val ledger = ledgers(MediaKind.Audio)
        val keys = (0L until 3L).map { assertIs<AudioKeyOutcome.Key>(keying.keyFor(it)).presentationTimeUs }
        keys.forEachIndexed { i, key -> ledger.observe(1L, MediaKind.Audio, obs(key, 100L + i)) }
        assertEquals(ClaimOutcome.Matched(100L), ledger.claim(1L, MediaKind.Audio, keys[0]), "the key matches unit 0, so unit 0 is what it claims")
        assertEquals(2, ledger.outstanding, "units 1 and 2 are later than that key: they are neither claimed nor discarded")
        // They can still be claimed by their own keys, in order, if their own
        // outputs do arrive; nothing about them has been decided here.
        assertEquals(ClaimOutcome.Matched(101L), ledger.claim(1L, MediaKind.Audio, keys[1]))
        assertEquals(ClaimOutcome.Matched(102L), ledger.claim(1L, MediaKind.Audio, keys[2]))
        assertEquals(0, ledger.outstanding)

        // And what actually happens when the NEXT output arrives instead: its
        // later key discards the leftovers as strictly older. The bound is
        // never reached, so there is no refusal and no fault -- the units
        // simply never publish. That is why one access unit per output is a
        // device qualification condition and not something the ledger detects.
        val again = ledgers(MediaKind.Audio)
        val laterKeys = (0L until 4L).map { assertIs<AudioKeyOutcome.Key>(keying.keyFor(it)).presentationTimeUs }
        laterKeys.take(3).forEachIndexed { i, key -> again.observe(1L, MediaKind.Audio, obs(key, 200L + i)) }
        assertEquals(ClaimOutcome.Matched(200L), again.claim(1L, MediaKind.Audio, laterKeys[0]), "the coalesced output claims the first unit")
        again.observe(1L, MediaKind.Audio, obs(laterKeys[3], 203L))
        assertEquals(ClaimOutcome.Matched(203L), again.claim(1L, MediaKind.Audio, laterKeys[3]), "the NEXT output claims its own unit")
        assertEquals(
            0,
            again.outstanding,
            "and the units behind the first key went as strictly older: no refusal, no fault, nothing detected -- they simply never published",
        )
    }

    /** A key that cannot be computed is refused, never wrapped into a timestamp. */
    private fun a6_aUnitIndexThatWouldOverflowIsRefusedByName(make: (Int, Int, Long) -> AudioKeying) {
        val keying = make(48_000, 1_024, 0L)
        val refused = assertIs<AudioKeyOutcome.Refused>(keying.keyFor(Long.MAX_VALUE / 1_000L), "the frames-to-microseconds product does not fit")
        assertTrue(refused.reason.contains("overflow"), "named: ${refused.reason}")
        assertIs<AudioKeyOutcome.Refused>(make(48_000, 1_024, Long.MAX_VALUE - 1L).keyFor(1L), "nor does the origin plus the offset")
        // And the exact remainder arithmetic is unaffected by the bound: the
        // largest index that DOES fit still floors exactly once.
        val big = 1_000_000_000L
        val exact = assertIs<AudioKeyOutcome.Key>(keying.keyFor(big))
        assertEquals(big * 1_024L * 1_000_000L / 48_000L, exact.presentationTimeUs, "one floored division of the exact frame count")
    }

    /**
     * An INDEPENDENT exact oracle. It is deliberately not the production
     * expression: arbitrary-precision integers, one floor at the end.
     */
    private fun exactKey(rate: Int, framesPerUnit: Int, originUs: Long, unitIndex: Long): java.math.BigInteger =
        java.math.BigInteger.valueOf(originUs) +
            java.math.BigInteger.valueOf(unitIndex)
                .multiply(java.math.BigInteger.valueOf(framesPerUnit.toLong()))
                .multiply(java.math.BigInteger.valueOf(1_000_000L))
                .divide(java.math.BigInteger.valueOf(rate.toLong()))

    /** Every key matches exact arithmetic, including the fractional-rate drift case. */
    private fun a7_keysMatchAnIndependentExactOracle(make: (Int, Int, Long) -> AudioKeying) {
        for ((rate, frames, origin) in listOf(Triple(44_100, 1_024, 5_000_000L), Triple(48_000, 1_024, 0L), Triple(44_100, 960, -1_000L))) {
            val keying = make(rate, frames, origin)
            for (index in listOf(0L, 1L, 2L, 7L, 100L, 4_321L, 1_000_003L)) {
                val expected = exactKey(rate, frames, origin, index)
                val key = assertIs<AudioKeyOutcome.Key>(keying.keyFor(index), "unit $index at $rate Hz has a key")
                assertEquals(
                    expected.toLong(),
                    key.presentationTimeUs,
                    "unit $index at $rate Hz: exactly one floor over the whole expression, with the remainder carried",
                )
            }
        }
    }

    /**
     * A key whose NAIVE intermediate product overflows a Long while the result
     * itself is perfectly representable. Refusing this would be wrong.
     */
    private fun a8_aRepresentableKeyIsNotRefusedForAnIntermediateOverflow(make: (Int, Int, Long) -> AudioKeying) {
        val rate = 48_000
        val framesPerUnit = 1_024
        val index = 10_000_000_000L
        val frames = java.math.BigInteger.valueOf(index).multiply(java.math.BigInteger.valueOf(framesPerUnit.toLong()))
        assertTrue(
            frames.multiply(java.math.BigInteger.valueOf(1_000_000L)) > java.math.BigInteger.valueOf(Long.MAX_VALUE),
            "the naive frames * 1_000_000 really does not fit",
        )
        val expected = exactKey(rate, framesPerUnit, 0L, index)
        assertTrue(expected <= java.math.BigInteger.valueOf(Long.MAX_VALUE), "but the key itself does")
        val key = assertIs<AudioKeyOutcome.Key>(make(rate, framesPerUnit, 0L).keyFor(index), "a representable key is not an overflow")
        assertEquals(expected.toLong(), key.presentationTimeUs, "and it is the exact value")
    }

    /** What IS unrepresentable is refused by name, and never wrapped. */
    private fun a9_unrepresentableKeysAreRefusedNotWrapped(make: (Int, Int, Long) -> AudioKeying) {
        val huge = assertIs<AudioKeyOutcome.Refused>(make(48_000, 1_024, 0L).keyFor(Long.MAX_VALUE), "the frame count itself does not fit")
        assertTrue(huge.reason.contains("overflow"), "named: ${huge.reason}")
        val origin = assertIs<AudioKeyOutcome.Refused>(make(48_000, 1_024, Long.MAX_VALUE - 1L).keyFor(1L), "the origin plus the offset does not fit")
        assertTrue(origin.reason.contains("overflow"), "named: ${origin.reason}")
        val negative = assertIs<AudioKeyOutcome.Refused>(make(48_000, 1_024, 0L).keyFor(-1L), "a unit before the origin is refused")
        assertTrue(negative.reason.contains("-1"), "named: ${negative.reason}")
    }

    /** Zero and partial reads move nothing they should not. */
    private fun a10_zeroAndPartialReadsCompleteNothing(make: (Int, Int, Long) -> AudioKeying) {
        val keying = make(48_000, 1_024, 0L)
        assertEquals(emptyList(), keying.onRead(0), "a read of nothing completes nothing")
        assertEquals(0, keying.retainedFrames, "and retains nothing")
        assertEquals(emptyList(), keying.onRead(1_023), "one frame short is still short")
        assertEquals(1_023, keying.retainedFrames)
        assertEquals(emptyList(), keying.onRead(0), "a zero read does not complete the unit either")
        assertEquals(1_023, keying.retainedFrames, "nor change the remainder")
        assertEquals(listOf(0L), keying.onRead(1), "the frame that completes it completes exactly one unit")
        assertEquals(0, keying.retainedFrames)
    }

    /**
     * Only the FINAL timestamp's representability decides. An intermediate --
     * the frame count, or the duration before the origin is added -- may
     * overflow a Long while the key itself is perfectly representable.
     */
    private fun a11_onlyTheFinalKeyDecidesRepresentability(make: (Int, Int, Long) -> AudioKeying) {
        val long = java.math.BigInteger.valueOf(Long.MAX_VALUE)
        // The frame count itself does not fit; the division brings it back.
        val fastRate = 2_000_000
        val bigIndex = 4_611_686_018_427_387_904L
        assertTrue(
            java.math.BigInteger.valueOf(bigIndex).multiply(java.math.BigInteger.valueOf(2L)) > long,
            "the frame count really does overflow a Long",
        )
        val byFrames = exactKey(fastRate, 2, 0L, bigIndex)
        assertTrue(byFrames <= long, "but the key does not")
        assertEquals(
            byFrames.toLong(),
            assertIs<AudioKeyOutcome.Key>(make(fastRate, 2, 0L).keyFor(bigIndex), "an overflowing frame count is not an unrepresentable key").presentationTimeUs,
            "the key is the exact value the whole expression gives",
        )

        // The duration exceeds Long.MAX; a negative origin brings it back.
        val index = 500_000_000_000_000L
        val duration =
            java.math.BigInteger.valueOf(index)
                .multiply(java.math.BigInteger.valueOf(1_024L))
                .multiply(java.math.BigInteger.valueOf(1_000_000L))
                .divide(java.math.BigInteger.valueOf(48_000L))
        assertTrue(duration > long, "the duration really does overflow a Long")
        val byOrigin = exactKey(48_000, 1_024, Long.MIN_VALUE, index)
        assertTrue(byOrigin <= long && byOrigin >= java.math.BigInteger.valueOf(Long.MIN_VALUE), "but the key does not")
        assertEquals(
            byOrigin.toLong(),
            assertIs<AudioKeyOutcome.Key>(
                make(48_000, 1_024, Long.MIN_VALUE).keyFor(index),
                "an overflowing duration is not an unrepresentable key either",
            ).presentationTimeUs,
            "the key is the exact value the whole expression gives",
        )
    }

    /** The boundaries themselves: exactly representable, and just past it. */
    private fun a12_representableBoundariesAreKeysAndTheRestAreRefused(make: (Int, Int, Long) -> AudioKeying) {
        // origin + offset == Long.MAX_VALUE exactly, at one microsecond per unit.
        val atMax = make(1_000_000, 1, Long.MAX_VALUE - 10L)
        assertEquals(
            Long.MAX_VALUE,
            assertIs<AudioKeyOutcome.Key>(atMax.keyFor(10L), "the largest representable key is a key").presentationTimeUs,
            "exactly Long.MAX_VALUE is representable",
        )
        assertIs<AudioKeyOutcome.Refused>(atMax.keyFor(11L), "one microsecond past it is not")
        // And at the bottom: the origin itself is the smallest key.
        val atMin = make(1_000_000, 1, Long.MIN_VALUE)
        assertEquals(
            Long.MIN_VALUE,
            assertIs<AudioKeyOutcome.Key>(atMin.keyFor(0L), "the origin is unit 0's key").presentationTimeUs,
            "exactly Long.MIN_VALUE is representable",
        )
    }

    /** The read accumulation is widened: a huge unit and a huge read still settle exactly. */
    private fun a13_readAccumulationDoesNotWrap(make: (Int, Int, Long) -> AudioKeying) {
        val keying = make(48_000, Int.MAX_VALUE, 0L)
        assertEquals(emptyList(), keying.onRead(Int.MAX_VALUE - 1), "one frame short of the unit")
        assertEquals(Int.MAX_VALUE - 1, keying.retainedFrames, "and all of it is retained")
        assertEquals(listOf(0L), keying.onRead(2), "the next read completes exactly one unit")
        assertEquals(1, keying.retainedFrames, "with exactly one frame carried")
    }

    /** A refused read is a no-op transaction: nothing accumulates, nothing advances. */
    private fun a14_aRefusedReadChangesNothing(make: (Int, Int, Long) -> AudioKeying) {
        val keying = make(48_000, 1_024, 0L)
        assertEquals(emptyList(), keying.onRead(1_023))
        assertFailsWith<IllegalArgumentException>("a negative read is refused") { keying.onRead(-1) }
        assertEquals(1_023, keying.retainedFrames, "and it changed nothing")
        assertEquals(listOf(0L), keying.onRead(1), "the unit still completes on its own next frame")
        assertEquals(0, keying.retainedFrames)
    }

    /**
     * A STATE-BOUNDARY setup, not a production seam: the unit index is placed
     * next to its limit by reflection, because reaching it by reading frames
     * would take billions of calls. Nothing in production exposes it, and this
     * is the only row that touches it.
     */
    private fun placeUnitIndexAt(keying: AudioKeying, value: Long) {
        val field = keying.javaClass.getDeclaredField("nextUnit")
        field.isAccessible = true
        field.setLong(keying, value)
    }

    private fun unitIndexOf(keying: AudioKeying): Long {
        val field = keying.javaClass.getDeclaredField("nextUnit")
        field.isAccessible = true
        return field.getLong(keying)
    }

    /** An index advance that cannot be represented refuses, and mutates nothing. */
    private fun a15_anUnrepresentableIndexAdvanceIsRefusedAndChangesNothing(make: (Int, Int, Long) -> AudioKeying) {
        val keying = make(48_000, 1_024, 0L)
        assertEquals(emptyList(), keying.onRead(1_000), "a partial unit is retained")
        placeUnitIndexAt(keying, Long.MAX_VALUE - 1L)

        assertFailsWith<IllegalArgumentException>("three more units cannot be indexed from here") { keying.onRead(3 * 1_024) }

        assertEquals(1_000, keying.retainedFrames, "the read accumulated nothing")
        assertEquals(Long.MAX_VALUE - 1L, unitIndexOf(keying), "and the index did not advance")
    }

    private fun runAudioContract(make: (Int, Int, Long) -> AudioKeying, ledgers: (MediaKind) -> CaptureLedger) {
        a1_keysAreExactIntegerMicrosecondsWithTheRemainderCarried(make)
        a2_aUnitIsCompleteOnlyWhenEveryFrameHasBeenRead(make)
        a3_unsupportedFramingIsRefusedByName(make)
        a4_aDelayedOutputStillClaimsItsOwnUnit(make, ledgers)
        a5_unitsBehindACoalescedKeyAreNotOlderAndAreNotDiscarded(make, ledgers)
        a6_aUnitIndexThatWouldOverflowIsRefusedByName(make)
        a7_keysMatchAnIndependentExactOracle(make)
        a8_aRepresentableKeyIsNotRefusedForAnIntermediateOverflow(make)
        a9_unrepresentableKeysAreRefusedNotWrapped(make)
        a10_zeroAndPartialReadsCompleteNothing(make)
        a11_onlyTheFinalKeyDecidesRepresentability(make)
        a12_representableBoundariesAreKeysAndTheRestAreRefused(make)
        a13_readAccumulationDoesNotWrap(make)
        a14_aRefusedReadChangesNothing(make)
        a15_anUnrepresentableIndexAdvanceIsRefusedAndChangesNothing(make)
    }

    /** Per-unit rounding, summed: right unit COUNT, drifting keys. */
    private class DriftingKeying(override val sampleRate: Int, override val framesPerUnit: Int, override val originUs: Long) : AudioKeying {
        private val perUnit = if (sampleRate > 0) framesPerUnit.toLong() * 1_000_000L / sampleRate else 0L
        override var retainedFrames: Int = 0
            private set
        private var units = 0L

        override fun keyFor(unitIndex: Long): AudioKeyOutcome = AudioKeyOutcome.Key(originUs + unitIndex * perUnit)

        override fun onRead(frames: Int): List<Long> {
            retainedFrames += frames
            val done = mutableListOf<Long>()
            while (retainedFrames >= framesPerUnit) {
                retainedFrames -= framesPerUnit
                done.add(units++)
            }
            return done
        }
    }

    @Test
    fun a_theAudioKeyingMeetsItsContract() =
        // The PRODUCT keying against the PRODUCT ledger: this arm is the
        // integration evidence, not keying alone.
        runAudioContract({ sr, fpu, origin -> UnitAudioKeying(sr, fpu, origin) }, { k -> BoundedCaptureLedger(k, 1L, capacity = 8) })

    @Test
    fun sa_theAudioKeyingContractIsSatisfiable() =
        runAudioContract({ sr, fpu, origin -> HostAudioKeying(sr, fpu, origin) }, { k -> HostLedger(k, 1L, capacity = 8) })

    @Test
    fun m7_aWrappedKeyInsteadOfARefusalFails() =
        assertFailsAt("the frames-to-microseconds product does not fit") {
            a6_aUnitIndexThatWouldOverflowIsRefusedByName { sr, fpu, o -> DriftingKeying(sr, fpu, o) }
        }

    @Test
    fun m6_perUnitRoundingDriftsAndFails() =
        assertFailsAt("a per-unit rounding drifts") {
            a1_keysAreExactIntegerMicrosecondsWithTheRemainderCarried { sr, fpu, o -> DriftingKeying(sr, fpu, o) }
        }

    /** A discriminator must fail at its INTENDED assertion, named, not at any AssertionError. */
    private fun assertFailsAt(marker: String, body: () -> Unit) {
        val thrown =
            try {
                body()
                null
            } catch (assertion: AssertionError) {
                assertion
            }
        assertTrue(thrown != null, "this row is satisfied by the stand-in; it should not be")
        assertTrue(
            thrown.message.orEmpty().contains(marker),
            "the stand-in must fail at \"$marker\", not at: ${thrown.message}",
        )
    }

    private fun assertFailsWithAssertion(body: () -> Unit) {
        val thrown =
            try {
                body()
                null
            } catch (assertion: AssertionError) {
                assertion
            }
        assertTrue(thrown != null, "this row is satisfied by a stand-in that only gets the counts right")
    }

    private companion object {
        const val CAPACITY = 4
    }
}
