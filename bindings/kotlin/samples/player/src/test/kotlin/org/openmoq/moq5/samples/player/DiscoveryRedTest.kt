package org.openmoq.moq5.samples.player

import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest
import org.openmoq.moq5.Namespace
import org.openmoq.moq5.NamespacePart
import org.openmoq.moq5.OverflowPolicy
import org.openmoq.moq5.ReceiverFatalCode
import org.openmoq.moq5.ReceiverInventory
import org.openmoq.moq5.ReceiverTerminal
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * The player's discovery decision, driven through the function
 * [PlaybackSession] itself calls for every inventory revision, with the
 * library's own [ReceiverTerminal] rather than prose about it.
 *
 * Track handles are stand-ins: `MediaTrack` has an `internal` constructor, so a
 * consumer module cannot build one -- the same position a real application is
 * in. `ReceiverInventory` is an ordinary public data class, so the mapping
 * boundary is pinned against a real snapshot below.
 */
class DiscoveryRedTest {

    private data class Handle(val name: String)

    private fun facts(
        ready: Boolean,
        terminal: ReceiverTerminal? = null,
        video: List<Handle> = emptyList(),
    ) = DiscoveryFacts(catalogReady = ready, terminal = terminal, videoTracks = video)

    // ---- P: positive controls, true of the sample today --------------------

    @Test
    fun p1_anUnreadyCatalogWithNoTerminalKeepsWaiting() {
        assertEquals(DiscoveryOutcome.KeepWaiting, discoveryStep(facts(ready = false)))
    }

    @Test
    fun p2_aReadyCatalogPlaysTheFirstVideoTrackByIdentity() {
        val first = Handle("cam1")
        val outcome = discoveryStep(facts(ready = true, video = listOf(first, Handle("cam2"))))
        assertTrue(outcome is DiscoveryOutcome.Play, "a ready catalog with video plays; got " + outcome)
        assertTrue(
            (outcome as DiscoveryOutcome.Play).track === first,
            "the decision must return the very handle it selected, because that is the handle " +
                "the session subscribes; got " + outcome.track,
        )
    }

    @Test
    fun p3_theMappingCarriesTheTerminalAndTheReadyFlagFromTheSnapshot() {
        val fatal = ReceiverTerminal.Fatal(ReceiverFatalCode(7uL))
        val mapped =
            ReceiverInventory(
                tracks = emptyList(),
                catalogReady = true,
                terminal = fatal,
                anomalies = 0,
                revision = 3uL,
            ).playbackFacts()
        assertEquals(true, mapped.catalogReady, "the mapping must carry catalogReady")
        assertEquals(fatal, mapped.terminal, "the mapping must carry the PUBLIC terminal, not a rendering of it")
        assertEquals(emptyList(), mapped.videoTracks, "an empty snapshot maps to no video tracks")
    }

    // ---- R: the R15 discovery defects --------------------------------------

    @Test
    fun r4_aTerminalReceiverEndsDiscoveryInsteadOfWaitingForever() {
        val outcome = discoveryStep(facts(ready = false, terminal = ReceiverTerminal.Closed))
        assertTrue(
            outcome is DiscoveryOutcome.Give,
            "the receiver reached its terminal before any catalog arrived, and discovery still " +
                "waits for a catalogReady that can never come; got " + outcome,
        )
        assertEquals(
            ReceiverTerminal.Closed,
            (outcome as DiscoveryOutcome.Give).terminal,
            "the outcome must carry the terminal it stopped for",
        )
    }

    @Test
    fun r5_aFatalTerminalKeepsItsExactCode() {
        val outcome =
            discoveryStep(facts(ready = false, terminal = ReceiverTerminal.Fatal(ReceiverFatalCode(42uL))))
        assertTrue(outcome is DiscoveryOutcome.Give, "a fatal terminal ends discovery; got " + outcome)
        assertEquals(
            ReceiverTerminal.Fatal(ReceiverFatalCode(42uL)),
            (outcome as DiscoveryOutcome.Give).terminal,
            "the exact fatal code must survive; an application branches on it",
        )
    }

    /**
     * The discriminator for r5: two terminals whose PROSE would read the same
     * must still be different outcomes. A decision that rendered the terminal
     * to text and compared the text would pass r5 and fail here.
     */
    @Test
    fun r5b_twoFatalCodesAreNotTheSameOutcome() {
        val seven = discoveryStep(facts(ready = false, terminal = ReceiverTerminal.Fatal(ReceiverFatalCode(7uL))))
        val nine = discoveryStep(facts(ready = false, terminal = ReceiverTerminal.Fatal(ReceiverFatalCode(9uL))))
        assertTrue(
            seven is DiscoveryOutcome.Give && nine is DiscoveryOutcome.Give,
            "both fatal terminals must end discovery; got " + seven + " and " + nine,
        )
        assertNotEquals(
            (seven as DiscoveryOutcome.Give).terminal,
            (nine as DiscoveryOutcome.Give).terminal,
            "two different fatal codes are two different facts",
        )
    }

    /**
     * The sequence 1744 asked for: a ready but empty catalog on a LIVE
     * receiver, then a track announced on the next revision. The two calls are
     * one story -- the first must keep waiting so the second can be reached.
     */
    @Test
    fun r6_anEmptyButLiveCatalogStillPermitsALaterTrack() {
        val later = Handle("cam-late")
        val revisions =
            listOf(
                facts(ready = true, video = emptyList()),
                facts(ready = true, video = listOf(later)),
            )
        val outcomes = revisions.map { discoveryStep(it) }
        assertEquals(
            DiscoveryOutcome.KeepWaiting,
            outcomes[0],
            "the first catalog was enumerated and carried no video track, but the receiver is " +
                "still live and a track may still be announced; the sample gives up instead",
        )
        assertTrue(
            outcomes[1] is DiscoveryOutcome.Play && (outcomes[1] as DiscoveryOutcome.Play).track === later,
            "the later track must be the one played; got " + outcomes[1],
        )
    }

    @Test
    fun r7_manualSelectionDeclaresItsAutoSubscribePolicy() {
        val config = playbackReceiverConfig(Namespace(listOf(NamespacePart("live".encodeToByteArray()))))
        assertEquals(
            false,
            config.autoSubscribe,
            "the sample chooses one track and subscribes to it by hand, but leaves autoSubscribe " +
                "at its default, so the receiver also subscribes to everything the catalog " +
                "announces; a manual-selection sample must state the matching policy",
        )
        assertEquals(OverflowPolicy.DropToKeyframe, config.overflow, "the sample's declared overflow policy")
    }

    // ---- the decision as the SESSION consumes it ---------------------------

    @Test
    fun r8d_aFatalTerminalReachesTheSessionAsAFailureCarryingItsCode() = runTest {
        val reported = mutableListOf<String>()
        val fatal = ReceiverTerminal.Fatal(ReceiverFatalCode(42uL))
        val outcome =
            runCatching {
                discoverTrack(flowOf(facts(ready = false, terminal = fatal))) { reported.add(it) }
            }
        val thrown = outcome.exceptionOrNull()
        assertTrue(
            thrown is PlaybackTerminatedException,
            "a fatal receiver must reach the session as a FAILURE, not as an ordinary return " +
                "that the Activity renders as a clean stop; got " + (thrown ?: outcome.getOrNull()),
        )
        assertEquals(
            fatal,
            (thrown as PlaybackTerminatedException).terminal,
            "and it must carry the exact fatal code",
        )
    }

    @Test
    fun r9d_aCleanTerminalIsNotTheSameOutcomeAsAFatalOne() = runTest {
        val clean =
            runCatching {
                discoverTrack(flowOf(facts(ready = false, terminal = ReceiverTerminal.Closed))) { }
            }
        val fatal =
            runCatching {
                discoverTrack(flowOf(facts(ready = false, terminal = ReceiverTerminal.Fatal(ReceiverFatalCode(3uL))))) { }
            }
        assertTrue(
            clean.isSuccess && clean.getOrNull() == null,
            "a clean close ends discovery without a track and without a failure; got " + clean,
        )
        assertTrue(
            fatal.exceptionOrNull() is PlaybackTerminatedException,
            "a fatal terminal is a failure; got " + fatal,
        )
    }

    @Test
    fun r10d_aTerminalReceiverDoesNotHandBackATrackToSubscribe() = runTest {
        val remaining = Handle("cam1")
        val outcome =
            runCatching {
                discoverTrack(
                    flowOf(
                        facts(ready = true, terminal = ReceiverTerminal.Closed, video = listOf(remaining)),
                    ),
                ) { }
            }
        assertTrue(
            outcome.isSuccess,
            "a clean terminal is not a failure, so this must SUCCEED rather than throw -- " +
                "`getOrNull() == null` alone would also pass on an exception; got " + outcome,
        )
        assertEquals(
            null,
            outcome.getOrNull(),
            "and the receiver has terminated, so its final inventory still listing a handle must " +
                "not start a new subscription",
        )
    }

    @Test
    fun r11d_theLoopReachesALaterTrackOnlyBecauseTheEmptyCatalogKeptWaiting() = runTest {
        val later = Handle("cam-late")
        val seen = mutableListOf<Int>()
        val revisions =
            flow {
                seen.add(1)
                emit(facts(ready = true, video = emptyList()))
                seen.add(2)
                emit(facts(ready = true, video = listOf(later)))
            }
        val chosen = discoverTrack(revisions) { }
        assertTrue(
            chosen === later,
            "the selected handle must be the very object announced, by identity: equality of a " +
                "stand-in data class would pass for a different instance. got " + chosen + "; " +
            "the real loop must keep waiting on a ready-but-empty catalog so the later track is " +
                "reached; it gave up on the first revision instead",
        )
        assertEquals(listOf(1, 2), seen, "and the second revision must actually have been consumed")
    }
}
