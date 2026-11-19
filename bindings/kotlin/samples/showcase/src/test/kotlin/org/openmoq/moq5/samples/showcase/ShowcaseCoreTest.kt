package org.openmoq.moq5.samples.showcase

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.openmoq.moq5.samples.playback.PlaybackEvent
import org.openmoq.moq5.samples.playback.PlaybackRequest
import org.openmoq.moq5.samples.playback.SessionUpdate
import org.openmoq.moq5.samples.playback.VideoTrackInfo

/**
 * The composed start path: the real [ShowcaseCore] and the real [AudioPolicy]
 * over a recording focus platform and a recording engine.
 *
 * The engine keeps the controller's rules: Started once a start is accepted,
 * one Stopped once a running session's stop completes, nothing for a stop with
 * no session or a stop already under way. It delivers them either IMMEDIATELY,
 * inside the call that caused them (harsher than the real controller), or
 * DEFERRED, through the same main-thread queue the core posts to (as the real
 * controller does).
 */
@OptIn(ExperimentalCoroutinesApi::class)
class ShowcaseCoreTest {
    private enum class Delivery { Immediate, Deferred }

    private class Platform : AudioFocusPlatform {
        var grant = true
        var requests = 0
        var abandons = 0
        val lost = mutableListOf<(Boolean) -> Unit>()
        var noisy: (() -> Unit)? = null

        /** Runs inside a request, as a reentrant caller would. */
        var during: (() -> Unit)? = null

        override fun request(lost: (transient: Boolean) -> Unit): Boolean {
            requests++
            this.lost += lost
            during?.invoke()
            return grant
        }

        override fun abandon() {
            abandons++
        }

        override fun listenNoisy(noisy: () -> Unit) {
            this.noisy = noisy
        }

        override fun stopListeningNoisy() {
            noisy = null
        }
    }

    private class Rig(val delivery: Delivery) {
        val posted = ArrayDeque<() -> Unit>()
        val platform = Platform()
        val starts = mutableListOf<String>()
        var stops = 0
        var session = false
        var stopping = false
        lateinit var core: ShowcaseCore
        val audio = AudioPolicy(platform) { reason -> core.interrupt(reason) }
        val saves = mutableListOf<Saved>()

        private val engine =
            object : PlaybackPort {
                override fun start(request: PlaybackRequest) {
                    check(!session) { "the core started a session while one was running or stopping" }
                    starts += request.namespace
                    session = true
                    deliver(SessionUpdate.Started)
                }

                override fun stop() {
                    if (!session || stopping) return
                    stops++
                    stopping = true
                    deliver(SessionUpdate.Stopped)
                }
            }

        init {
            core = ShowcaseCore(engine, audio, { posted += it }, { saves += it }, Saved())
        }

        fun deliver(update: SessionUpdate) {
            val act = {
                if (update == SessionUpdate.Stopped) {
                    session = false
                    stopping = false
                }
                core.onUpdate(update)
            }
            if (delivery == Delivery.Immediate) act() else posted += act
        }

        fun drain() {
            while (posted.isNotEmpty()) posted.removeFirst()()
        }

        val player get() = core.ui.value.player!!

        /** Nothing left running while the UI says the visit is not. */
        fun settled() {
            drain()
            assertTrue(player.running || !session, "$delivery: the UI never says stopped while a session runs")
            assertEquals(session, audio.holding, "$delivery: focus is held exactly while a session runs")
        }
    }

    private val a = WatchTarget("https://relay.example.com:4433/moq", "live/a")
    private val b = WatchTarget("https://relay.example.com:4433/moq", "live/b")

    private fun both(body: (Rig) -> Unit) = Delivery.entries.forEach { body(Rig(it)) }

    @Test fun `focus is granted before the engine starts, held for the session and given back on its Stopped`() =
        both { r ->
            r.core.surface(ready = true)
            r.core.watch(a)
            assertEquals(1, r.platform.requests, "${r.delivery}: focus first")
            assertEquals(listOf("live/a"), r.starts, "${r.delivery}: then the engine")
            r.drain()
            r.core.onUpdate(SessionUpdate.Event(PlaybackEvent.Selected(VideoTrackInfo("video/avc", 1280, 720), null)))
            assertTrue(r.audio.holding)
            r.core.stop()
            r.settled()
            assertEquals(PlayerPhase.Stopped(null), r.player.phase)
            assertEquals(1, r.platform.abandons, "${r.delivery}: given back on the Stopped")
            assertEquals(1, r.starts.size)
        }

    @Test fun `a visit waits for its surface and asks for focus only when it can start`() =
        both { r ->
            r.core.watch(a)
            assertEquals(0, r.platform.requests)
            assertEquals(0, r.starts.size)
            r.core.surface(ready = true)
            assertEquals(1, r.platform.requests)
            assertEquals(listOf("live/a"), r.starts)
            r.settled()
        }

    @Test fun `a refused request settles the visit with zero starts, and an explicit Play retries`() =
        both { r ->
            r.core.surface(ready = true)
            r.platform.grant = false
            r.core.watch(a)
            assertEquals(0, r.starts.size, "${r.delivery}: no start from a refused intent")
            assertEquals(PlayerPhase.Stopped(ShowcaseCore.FOCUS_REFUSED), r.player.phase)
            assertNull(r.platform.noisy, "nothing listened to without focus")
            r.settled()
            assertEquals(0, r.starts.size, "${r.delivery}: and none later")
            r.platform.grant = true
            r.core.play()
            assertEquals(2, r.platform.requests)
            assertEquals(listOf("live/a"), r.starts, "${r.delivery}: the retry starts once")
            r.settled()
        }

    @Test fun `Play while running is ignored, and Play after a stop rejoins once with fresh focus`() =
        both { r ->
            r.core.surface(ready = true)
            r.core.watch(a)
            r.core.play()
            assertEquals(1, r.starts.size, "${r.delivery}: already running")
            r.core.stop()
            r.drain()
            r.core.play()
            assertEquals(listOf("live/a", "live/a"), r.starts)
            assertEquals(2, r.platform.requests)
            assertEquals(1, r.platform.abandons)
            r.settled()
        }

    @Test fun `stop while a new visit waits for the old session settles it, and the old completion releases only the old grant`() {
        val r = Rig(Delivery.Deferred)
        r.core.surface(ready = true)
        r.core.watch(a)
        r.drain()
        r.core.watch(b)
        assertEquals(1, r.stops, "the old session is asked to stop; b waits for it")
        assertTrue(r.player.running)
        r.core.stop()
        assertEquals(PlayerPhase.Stopped(null), r.player.phase, "b settles at once")
        r.drain()
        assertEquals(listOf("live/a"), r.starts, "the old completion starts nothing")
        assertEquals(1, r.platform.requests)
        assertEquals(1, r.platform.abandons, "the old grant, once")
        r.settled()
        r.core.play()
        assertEquals(listOf("live/a", "live/b"), r.starts)
        r.settled()
    }

    @Test fun `an interruption while a new visit waits ends that intent too, with its reason`() {
        val r = Rig(Delivery.Deferred)
        r.core.surface(ready = true)
        r.core.watch(a)
        r.drain()
        r.core.watch(b)
        r.core.interrupt(AudioPolicy.NOISY)
        assertEquals(PlayerPhase.Stopped(AudioPolicy.NOISY), r.player.phase)
        r.drain()
        assertEquals(listOf("live/a"), r.starts)
        r.settled()
    }

    @Test fun `a replacement intent survives the old session's completion and starts once it is done`() {
        val r = Rig(Delivery.Deferred)
        r.core.surface(ready = true)
        r.core.watch(a)
        r.drain()
        r.core.watch(b)
        assertEquals(listOf("live/a"), r.starts, "b waits for a to stop")
        r.drain()
        assertEquals(listOf("live/a", "live/b"), r.starts)
        assertEquals(2, r.platform.requests)
        assertEquals(1, r.platform.abandons)
        assertTrue(r.audio.holding, "b's grant is held")
        r.settled()
    }

    @Test fun `a visit replaced while its predecessor stops immediately starts at once`() {
        val r = Rig(Delivery.Immediate)
        r.core.surface(ready = true)
        r.core.watch(a)
        r.core.watch(b)
        assertEquals(listOf("live/a", "live/b"), r.starts)
        assertEquals(1, r.platform.abandons)
        r.settled()
    }

    @Test fun `a start cancelled while focus is being granted gives the grant back and starts nothing`() =
        both { r ->
            r.core.surface(ready = true)
            r.platform.during = { r.core.stop() }
            r.core.watch(a)
            assertEquals(1, r.platform.requests)
            assertEquals(1, r.platform.abandons, "${r.delivery}: released at once; no Stopped can come for a session that never existed")
            assertEquals(0, r.starts.size)
            assertEquals(PlayerPhase.Stopped(null), r.player.phase)
            r.platform.during = null
            r.settled()
        }

    @Test fun `a focus loss stops the session with its reason, and a callback from an earlier grant is ignored`() =
        both { r ->
            r.core.surface(ready = true)
            r.core.watch(a)
            r.drain()
            val first = r.platform.lost.single()
            r.core.stop()
            r.drain()
            r.core.play()
            r.drain()
            first(false)
            assertTrue(r.player.running, "${r.delivery}: a stale loss from the first grant changes nothing")
            assertEquals(1, r.stops)
            r.platform.lost.last()(true)
            r.drain()
            assertEquals(PlayerPhase.Stopped(AudioPolicy.INTERRUPTED_TRANSIENT), r.player.phase)
            assertFalse(r.audio.holding)
            r.settled()
        }

    @Test fun `an observer that cancels as it sees Connecting wins over the start when collected immediately`() =
        runTest {
            val r = Rig(Delivery.Deferred)
            r.core.surface(ready = true)
            var reacted = 0
            val observer =
                launch(UnconfinedTestDispatcher(testScheduler)) {
                    r.core.ui.collect { ui ->
                        if (ui.player?.phase == PlayerPhase.Connecting && reacted++ == 0) r.core.interrupt("observer")
                    }
                }
            r.core.watch(a)
            assertEquals(0, r.platform.requests, "cancelled inside the publication: focus is never asked")
            assertEquals(0, r.starts.size, "and nothing starts")
            assertEquals(PlayerPhase.Stopped("observer"), r.player.phase)
            r.settled()
            reacted = 0
            r.core.play()
            assertEquals(0, r.starts.size, "Play's publication is cancelled the same way")
            r.settled()
            observer.cancel()
        }

    @Test fun `an observer that cancels as it sees Connecting stops the started session when collected later`() =
        runTest {
            val r = Rig(Delivery.Deferred)
            r.core.surface(ready = true)
            var reacted = 0
            val observer =
                launch(StandardTestDispatcher(testScheduler)) {
                    r.core.ui.collect { ui ->
                        if (ui.player?.phase == PlayerPhase.Connecting && reacted++ == 0) r.core.interrupt("observer")
                    }
                }
            advanceUntilIdle()
            r.core.watch(a)
            assertEquals(listOf("live/a"), r.starts, "the observer has not run yet")
            advanceUntilIdle()
            r.drain()
            assertEquals(PlayerPhase.Stopped("observer"), r.player.phase)
            assertEquals(1, r.platform.abandons, "the started session's grant is given back")
            r.settled()
            observer.cancel()
        }

    @Test fun `settings, watched broadcasts and removals are saved as they change`() {
        val r = Rig(Delivery.Deferred)
        r.core.changeSettings(ShowcaseSettings(draft = DraftChoice.Draft18))
        r.core.watch(a)
        r.core.removeRecent(a)
        assertEquals(3, r.saves.size)
        assertEquals(DraftChoice.Draft18, r.saves.last().settings.draft)
        assertTrue(r.saves.last().recents.isEmpty())
    }

    @Test fun `closing gives back held focus, since no Stopped follows a destroyed engine`() {
        val r = Rig(Delivery.Deferred)
        r.core.surface(ready = true)
        r.core.watch(a)
        assertTrue(r.audio.holding)
        r.core.close()
        assertFalse(r.audio.holding)
        assertEquals(1, r.platform.abandons)
    }
}
