package org.openmoq.moq5.samples.showcase

import java.net.URLDecoder
import org.openmoq.moq5.MoqVersion
import org.openmoq.moq5.ReceiverStats
import org.openmoq.moq5.StartMode
import org.openmoq.moq5.VersionOffer
import org.openmoq.moq5.samples.playback.AudioTrackInfo
import org.openmoq.moq5.samples.playback.PlaybackEvent
import org.openmoq.moq5.samples.playback.PlaybackProblem
import org.openmoq.moq5.samples.playback.PlaybackRequest
import org.openmoq.moq5.samples.playback.SessionUpdate
import org.openmoq.moq5.samples.playback.VideoTrackInfo

/*
 * The application's state, as plain values with no Android in them: what to
 * watch, how, and what the running session has said so far. The ViewModel
 * holds one of these and the screens draw it.
 */

/** A broadcast to watch: the relay URL and the broadcast's namespace. */
data class WatchTarget(val url: String, val namespace: String) {
    /** The namespace's last part, as a title. */
    val title: String get() = namespace.trimEnd('/').substringAfterLast('/').ifEmpty { namespace }

    /** The relay's host and port, as a subtitle. */
    val relay: String get() = url.substringAfter("://").substringBefore('/')
}

/** Why [url] and [namespace] cannot be watched, or null when they can. */
fun targetProblem(url: String, namespace: String): String? {
    val scheme = url.substringBefore("://", "")
    return when {
        url.isBlank() -> "Enter a relay URL"
        scheme !in RELAY_SCHEMES -> "The relay URL starts with https:// or moqt://"
        url.substringAfter("://").substringBefore('/').substringBefore(':').isEmpty() -> "The relay URL names no host"
        namespace.isBlank() -> "Enter the broadcast's namespace"
        namespace.split('/').any { it.isEmpty() } -> "Namespace parts are separated by single slashes"
        else -> null
    }
}

/** The relay schemes the endpoint connects to: WebTransport over HTTPS, and raw QUIC. */
val RELAY_SCHEMES: Set<String> = setOf("https", "moqt")

/** The drafts the player offers. Automatic is the default and offers every draft this build speaks. */
enum class DraftChoice(val label: String, val offer: VersionOffer) {
    Automatic("Automatic", VersionOffer.Automatic),
    Draft16("Draft 16", VersionOffer.Offer(listOf(MoqVersion.Draft16))),
    Draft18("Draft 18", VersionOffer.Offer(listOf(MoqVersion.Draft18))),
}

/** Where each track starts, as the receiver's supported start modes. */
enum class StartChoice(val label: String, val detail: String, val mode: StartMode) {
    LiveEdge("Live edge", "Join at the newest object, without waiting for a group boundary", StartMode.Current),
    NextGroup("Next group", "Wait for the next group boundary: a clean start, up to one group later", StartMode.NextGroup),
}

/** How the player connects and what it shows. */
data class ShowcaseSettings(
    val draft: DraftChoice = DraftChoice.Automatic,
    val start: StartChoice = StartChoice.LiveEdge,
    val showStats: Boolean = false,
) {
    /** The engine request for [target]: unbounded, with receiver statistics for the stats panel. */
    fun request(target: WatchTarget): PlaybackRequest =
        PlaybackRequest(target.url, target.namespace, versions = draft.offer, start = start.mode, stats = true)
}

/** What the player screen shows about the session. */
sealed interface PlayerPhase {
    /** Connecting to the relay. */
    data object Connecting : PlayerPhase

    /** Connected; waiting for the broadcast's catalog and tracks. */
    data object Waiting : PlayerPhase

    /** Decoding and presenting the selected tracks. */
    data object Playing : PlayerPhase

    /** Not playing: stopped by the viewer or ended by the broadcast. [reason] is the session's own last word. */
    data class Stopped(val reason: String?) : PlayerPhase

    /** The session failed; see [problem]. */
    data class Failed(val problem: PlaybackProblem) : PlayerPhase
}

/** One visit to the player screen. [id] tells a session's updates from an earlier visit's. */
data class PlayerState(
    val id: Long,
    val target: WatchTarget,
    val phase: PlayerPhase = PlayerPhase.Connecting,
    /** The draft the session negotiated; kept after it ends, until the next one starts. */
    val version: MoqVersion? = null,
    /** The selected tracks; kept after the session ends, until the next one starts. */
    val video: VideoTrackInfo? = null,
    val audio: AudioTrackInfo? = null,
    val stats: ReceiverStats? = null,
    /** A transient remark from the engine (audio stalled or resumed), shown briefly. */
    val notice: String? = null,
    /** The session's own end line, kept for [PlayerPhase.Stopped]. */
    val ended: String? = null,
) {
    /** A session is connecting or playing for this visit. */
    val running: Boolean
        get() = phase is PlayerPhase.Connecting || phase is PlayerPhase.Waiting || phase is PlayerPhase.Playing

    /** This visit after [update]. */
    fun after(update: SessionUpdate): PlayerState =
        when (update) {
            SessionUpdate.Started -> PlayerState(id, target)
            is SessionUpdate.Event ->
                when (val event = update.event) {
                    is PlaybackEvent.Connected -> copy(phase = PlayerPhase.Waiting, version = event.version)
                    is PlaybackEvent.Selected -> copy(phase = PlayerPhase.Playing, video = event.video, audio = event.audio)
                    is PlaybackEvent.Stats -> copy(stats = event.stats)
                }
            is SessionUpdate.Reported ->
                when {
                    update.line.startsWith(AUDIO_NOTICE) -> copy(notice = update.line)
                    update.line.startsWith(END_LINE) -> copy(ended = update.line.removePrefix(END_LINE))
                    else -> this
                }
            is SessionUpdate.Failed -> copy(phase = PlayerPhase.Failed(PlaybackProblem.of(update.failure)))
            SessionUpdate.Stopped -> if (phase is PlayerPhase.Failed) this else copy(phase = PlayerPhase.Stopped(ended))
        }

    private companion object {
        /** The engine's audio-output remarks: "audio output stalled; playing video only", "audio output resumed", ... */
        const val AUDIO_NOTICE = "audio output"

        /** The engine's end line: "stopped: <end>; N objects, M decoder inputs". */
        const val END_LINE = "stopped: "
    }
}

/** A draft as people write it: "draft-16". */
fun MoqVersion.label(): String = "draft-" + code

/** [target] first, then the others without it, at most [keep]. */
fun List<WatchTarget>.remembering(target: WatchTarget, keep: Int = RECENTS_KEPT): List<WatchTarget> =
    (listOf(target) + filterNot { it == target }).take(keep)

/** How many recent broadcasts are kept. */
const val RECENTS_KEPT: Int = 8

/** A `moq5://watch` link: the broadcast, and the settings the link asks for. */
data class WatchLink(val target: WatchTarget, val draft: DraftChoice?, val start: StartChoice?)

/**
 * Parse `moq5://watch?relay=<url>&namespace=<ns>[&draft=16|18][&start=live|next]`,
 * values URL-encoded. Null when it is not such a link or names no valid broadcast.
 */
fun parseWatchLink(link: String): WatchLink? {
    if (!link.startsWith(LINK_PREFIX)) return null
    val params =
        try {
            link.removePrefix(LINK_PREFIX).split('&').mapNotNull { pair ->
                val i = pair.indexOf('=')
                if (i <= 0) null else pair.substring(0, i) to URLDecoder.decode(pair.substring(i + 1), "UTF-8")
            }.toMap()
        } catch (malformed: IllegalArgumentException) {
            // A broken %-escape: not a link this app can read.
            return null
        }
    val relay = params["relay"] ?: return null
    val namespace = params["namespace"] ?: return null
    if (targetProblem(relay, namespace) != null) return null
    val draft =
        when (params["draft"]) {
            null -> null
            "16" -> DraftChoice.Draft16
            "18" -> DraftChoice.Draft18
            else -> return null
        }
    val start =
        when (params["start"]) {
            null -> null
            "live" -> StartChoice.LiveEdge
            "next" -> StartChoice.NextGroup
            else -> return null
        }
    return WatchLink(WatchTarget(relay, namespace), draft, start)
}

private const val LINK_PREFIX = "moq5://watch?"
