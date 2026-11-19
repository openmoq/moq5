package org.openmoq.moq5.samples.showcase

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import org.openmoq.moq5.samples.playback.PlaybackRequest
import org.openmoq.moq5.samples.playback.SessionUpdate

/** What [ShowcaseCore] asks of the playback engine. */
interface PlaybackPort {
    /** Start a session for [request]. Called only while no session is running or stopping. */
    fun start(request: PlaybackRequest)

    /** Stop the running session, if any; its Stopped follows through [ShowcaseCore.onUpdate]. */
    fun stop()
}

/** Audio focus, as [ShowcaseCore] holds it for a session. */
interface FocusPort {
    /** Ask for focus before a session starts. True only when granted; a later loss arrives as [ShowcaseCore.interrupt]. */
    fun acquire(): Boolean

    /** Give back focus that [acquire] granted. */
    fun release()
}

/** Which screen is showing. */
sealed interface Screen {
    data object Connect : Screen

    data class Watching(val player: PlayerState) : Screen
}

/** Everything the UI draws. */
data class ShowcaseUi(
    val screen: Screen,
    val settings: ShowcaseSettings,
    val recents: List<WatchTarget>,
    val url: String,
    val namespace: String,
) {
    val player: PlayerState? get() = (screen as? Screen.Watching)?.player
}

/**
 * The application's behaviour, with no Android in it: what each user action
 * and each engine update does to [ui], and when a session starts.
 *
 * Main thread only. [post] runs work after the current engine delivery has
 * returned; [persist] saves what is worth keeping.
 *
 * Starting a session has ONE owner, this class, and one order: the viewer's
 * intent, then audio focus, then the engine.
 *  * INTENT. [watch] and [play] record an intent to play the visit now
 *    showing, before anything observable is published. [stop],
 *    [interrupt], [leave] and the next [watch] invalidate it; a start
 *    consumes it. Nothing ever starts from an intent that is gone, so a
 *    cancellation, even one an observer makes while reacting to a
 *    publication, is final.
 *  * FOCUS. A start asks [focus] first and consumes the intent before
 *    asking. A refusal settles the visit as Stopped with the reason and
 *    starts nothing; a cancellation made while the request was outstanding
 *    gives the grant back and starts nothing. Focus is held for the
 *    session's lifetime and given back on its Stopped.
 *  * ENGINE. Only then does the engine start. A session belongs to one
 *    visit ([PlayerState.id]); its updates apply only while that visit is
 *    showing. A new session starts only once the previous one has stopped:
 *    the engine ignores a start while a session is still stopping, so the
 *    start waits for its Stopped.
 *
 * A visit with no session of its own -- waiting for the surface, for an
 * older session to finish stopping, or refused focus -- hears no Stopped,
 * so a stop or an interruption settles it directly.
 */
class ShowcaseCore(
    private val engine: PlaybackPort,
    private val focus: FocusPort,
    private val post: (() -> Unit) -> Unit,
    private val persist: (Saved) -> Unit,
    saved: Saved,
) {
    private val state =
        MutableStateFlow(
            ShowcaseUi(
                screen = Screen.Connect,
                settings = saved.settings,
                recents = saved.recents,
                url = saved.url ?: DEFAULT_URL,
                namespace = saved.namespace ?: "",
            ),
        )

    /** What the UI draws. */
    val ui: StateFlow<ShowcaseUi> = state.asStateFlow()

    private var surfaceReady = false
    private var nextVisit = 0L

    /** The visit the viewer wants playing now, or null: set by watch and play, cleared by every cancellation and by a start. */
    private var intent: Long? = null

    /** Counts cancellations, so a start can tell one happened while it asked for focus. */
    private var cancellations = 0L

    /** A session is running or stopping: from its start until its Stopped. */
    private var active = false

    /** The visit the active session belongs to. */
    private var sessionVisit = -1L

    /** Focus is held for the active session. */
    private var focused = false

    /** Why the active session is being stopped, when the viewer did not ask; shown on its Stopped. */
    private var stopReason: String? = null

    fun editUrl(url: String) = state.update { it.copy(url = url) }

    fun editNamespace(namespace: String) = state.update { it.copy(namespace = namespace) }

    fun changeSettings(settings: ShowcaseSettings) {
        state.update { it.copy(settings = settings) }
        save()
    }

    /** Open [target] in the player and start watching it. */
    fun watch(target: WatchTarget) {
        val visit = ++nextVisit
        cancel()
        intent = visit
        if (active) engine.stop()
        val recents = state.value.recents.remembering(target)
        state.update {
            it.copy(
                screen = Screen.Watching(PlayerState(visit, target)),
                recents = recents,
                url = target.url,
                namespace = target.namespace,
            )
        }
        save()
        startIfReady()
    }

    /** Open a `moq5://watch` link: its settings first, then the broadcast. */
    fun open(link: WatchLink) {
        val current = state.value.settings
        changeSettings(current.copy(draft = link.draft ?: current.draft, start = link.start ?: current.start))
        watch(link.target)
    }

    /** Play again after a stop or a failure: a fresh session joins the broadcast where its start mode says. */
    fun play() {
        val player = state.value.player ?: return
        if (player.running) return
        intent = player.id
        setPlayer(player.copy(phase = PlayerPhase.Connecting, ended = null))
        startIfReady()
    }

    /**
     * Stop watching. A live broadcast has nothing to pause: stopping leaves
     * it, releasing the decoders and the audio track, and [play] rejoins it.
     */
    fun stop() = halt(reason = null)

    /** Stop, saying why: the viewer did not ask, the platform did (audio focus, noisy output). */
    fun interrupt(reason: String) = halt(reason)

    /** Back to the connect screen; any session stops. */
    fun leave() {
        stop()
        state.update { it.copy(screen = Screen.Connect) }
    }

    fun removeRecent(target: WatchTarget) {
        state.update { it.copy(recents = it.recents - target) }
        save()
    }

    fun clearNotice() {
        state.value.player?.let { setPlayer(it.copy(notice = null)) }
    }

    /** The video surface became available, or went away. */
    fun surface(ready: Boolean) {
        surfaceReady = ready
        if (ready) startIfReady()
    }

    /** An engine update, on the main thread. */
    fun onUpdate(update: SessionUpdate) {
        val player = state.value.player?.takeIf { it.id == sessionVisit }
        // A visit already waiting to play again keeps its intent: its old
        // session's Stopped does not settle it.
        if (player != null && !(update == SessionUpdate.Stopped && intent == player.id)) {
            val next = player.after(update)
            val reason = stopReason
            setPlayer(
                if (update == SessionUpdate.Stopped && reason != null && next.phase is PlayerPhase.Stopped) {
                    next.copy(phase = PlayerPhase.Stopped(reason), ended = reason)
                } else {
                    next
                },
            )
        }
        if (update == SessionUpdate.Stopped) {
            stopReason = null
            active = false
            if (focused) {
                focused = false
                focus.release()
            }
            // Not from inside the engine's own Stopped delivery: it retires the
            // finished session just after this returns.
            post { startIfReady() }
        }
    }

    /** The owner is going away; the engine is destroyed with it and no Stopped follows. Give back held focus. */
    fun close() {
        cancel()
        if (focused) {
            focused = false
            focus.release()
        }
    }

    /** Invalidate the intent; then stop the session, and settle a visit that has none. */
    private fun halt(reason: String?) {
        cancel()
        if (active) {
            // Kept for the session's own Stopped, which may arrive inside
            // engine.stop() or after a Started that resets the visit.
            if (reason != null) stopReason = reason
            engine.stop()
        }
        val player = state.value.player ?: return
        val owned = active && sessionVisit == player.id
        if (player.running && !owned) {
            val ended = reason ?: player.ended
            setPlayer(player.copy(phase = PlayerPhase.Stopped(ended), ended = ended))
        }
    }

    private fun cancel() {
        intent = null
        cancellations++
    }

    private fun startIfReady() {
        val player = state.value.player ?: return
        val visit = intent ?: return
        if (visit != player.id || !surfaceReady || active) return
        intent = null
        val before = cancellations
        if (!focus.acquire()) {
            state.value.player?.takeIf { it.id == visit && it.running }?.let {
                setPlayer(it.copy(phase = PlayerPhase.Stopped(FOCUS_REFUSED), ended = FOCUS_REFUSED))
            }
            return
        }
        if (cancellations != before || state.value.player?.id != visit) {
            // Cancelled while focus was being granted: give it back, start nothing.
            focus.release()
            return
        }
        focused = true
        active = true
        sessionVisit = visit
        engine.start(state.value.settings.request(player.target))
    }

    private fun setPlayer(player: PlayerState) =
        state.update { current ->
            if (current.player?.id == player.id) current.copy(screen = Screen.Watching(player)) else current
        }

    private fun save() {
        val now = state.value
        persist(Saved(now.settings, now.url, now.namespace, now.recents))
    }

    internal companion object {
        const val DEFAULT_URL = "https://"

        /** Shown when audio focus is refused at start. */
        const val FOCUS_REFUSED = "Audio is in use by another app"
    }
}
