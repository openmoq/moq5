package org.openmoq.moq5.samples.player

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import org.openmoq.moq5.MediaType
import org.openmoq.moq5.Namespace
import org.openmoq.moq5.OverflowPolicy
import org.openmoq.moq5.ReceiverConfig
import org.openmoq.moq5.ReceiverInventory
import org.openmoq.moq5.ReceiverTerminal

/**
 * What the sample knows about the receiver at one instant, reduced to the
 * facts its discovery decision actually uses.
 *
 * [T] is the caller's own handle for a track. [PlaybackSession] passes the
 * library's `MediaTrack`, so the decision returns the very object the sample
 * then subscribes; a test passes a stand-in. That is what lets the decision be
 * driven without making `MediaTrack` publicly constructible -- it has an
 * `internal` constructor, while `ReceiverInventory` itself is an ordinary
 * public data class.
 *
 * [terminal] is the PUBLIC terminal, not prose: a clean close and a fatal code
 * are different facts to an application and stay different here.
 */
internal data class DiscoveryFacts<T>(
    val catalogReady: Boolean,
    val terminal: ReceiverTerminal?,
    val videoTracks: List<T>,
)

/** What the sample does next with [DiscoveryFacts]. */
internal sealed interface DiscoveryOutcome<out T> {
    /** Nothing is decidable yet; wait for the next inventory revision. */
    data object KeepWaiting : DiscoveryOutcome<Nothing>

    /** Subscribe to exactly this track -- the handle the decision selected -- and pull its objects. */
    data class Play<T>(val track: T) : DiscoveryOutcome<T>

    /** Discovery is over without a track to play, for this stated reason. */
    data class Give(val reason: String, val terminal: ReceiverTerminal?) : DiscoveryOutcome<Nothing>
}

/**
 * The sample's discovery decision.
 *
 * Three facts decide it, in this order:
 *  * A TERMINAL ends discovery whatever else is true. The receiver has stopped,
 *    so a `catalogReady` that has not arrived never will, and a handle still
 *    listed in its final inventory is not something to subscribe to. The
 *    terminal travels with the outcome so a clean close and a fatal code stay
 *    different facts.
 *  * An unready catalog waits.
 *  * A ready catalog with no video track ALSO waits, because the receiver is
 *    live and a track may still be announced -- the first catalog being empty
 *    is not the end of discovery. What ends a live wait is the caller's own
 *    budget or the user pressing Stop.
 */
internal fun <T> discoveryStep(facts: DiscoveryFacts<T>): DiscoveryOutcome<T> {
    val terminal = facts.terminal
    if (terminal != null) {
        return DiscoveryOutcome.Give("the receiver terminated during discovery: " + terminal, terminal)
    }
    if (!facts.catalogReady) return DiscoveryOutcome.KeepWaiting
    val video = facts.videoTracks.firstOrNull() ?: return DiscoveryOutcome.KeepWaiting
    return DiscoveryOutcome.Play(video)
}

/**
 * The published snapshot, reduced to the facts the decision uses. This is the
 * one mapping boundary, so a test can pin it directly.
 */
internal fun ReceiverInventory.playbackFacts(): DiscoveryFacts<org.openmoq.moq5.MediaTrack> =
    DiscoveryFacts(
        catalogReady = catalogReady,
        terminal = terminal,
        videoTracks = tracks.filter { it.description.mediaType == MediaType.Video },
    )

/**
 * The receiver configuration this sample attaches with.
 *
 * `autoSubscribe = false` is the policy that matches what the sample DOES: it
 * discovers a catalog, chooses one track and subscribes to that one by hand.
 * Left at its default the receiver would also subscribe to everything the
 * catalog announced, which is a different application.
 */
internal fun playbackReceiverConfig(namespace: Namespace): ReceiverConfig =
    ReceiverConfig(
        namespace = namespace,
        overflow = OverflowPolicy.DropToKeyframe,
        autoSubscribe = false,
    )



/**
 * The session ended because the RECEIVER did, and the sample says which way.
 *
 * It carries the public [org.openmoq.moq5.ReceiverTerminal] so a fatal code
 * survives all the way to the Activity. It is the SAMPLE's own type: the
 * library's `ReceiverFatalException` has an internal constructor, and a sample
 * neither calls it nor asks for it to be opened.
 */
internal class PlaybackTerminatedException(
    internal val terminal: ReceiverTerminal,
) : IllegalStateException("the receiver terminated before playback could finish: " + terminal)

/**
 * Run the discovery loop over successive [DiscoveryFacts], returning the track
 * to subscribe, or nothing if the receiver closed cleanly first.
 *
 * A FATAL terminal is not a normal end: it is raised as
 * [PlaybackTerminatedException] carrying the public terminal, so the Activity
 * renders a failure with its exact code instead of an ordinary stop.
 */
internal suspend fun <T : Any> discoverTrack(
    revisions: Flow<DiscoveryFacts<T>>,
    report: (String) -> Unit,
): T? {
    var chosen: T? = null
    var terminal: ReceiverTerminal? = null
    revisions.first { facts ->
        when (val step = discoveryStep(facts)) {
            is DiscoveryOutcome.KeepWaiting -> false
            is DiscoveryOutcome.Give -> {
                report(step.reason)
                terminal = step.terminal
                true
            }
            is DiscoveryOutcome.Play -> {
                chosen = step.track
                true
            }
        }
    }
    val ended = terminal
    if (ended is ReceiverTerminal.Fatal) throw PlaybackTerminatedException(ended)
    return chosen
}
