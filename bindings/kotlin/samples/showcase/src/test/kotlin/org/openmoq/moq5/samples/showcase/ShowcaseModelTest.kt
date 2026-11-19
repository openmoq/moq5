package org.openmoq.moq5.samples.showcase

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import org.openmoq.moq5.MoqVersion
import org.openmoq.moq5.StartMode
import org.openmoq.moq5.VersionOffer
import org.openmoq.moq5.samples.playback.AudioTrackInfo
import org.openmoq.moq5.samples.playback.PlaybackEvent
import org.openmoq.moq5.samples.playback.PlaybackProblemKind
import org.openmoq.moq5.samples.playback.SessionUpdate
import org.openmoq.moq5.samples.playback.VideoTrackInfo

class ShowcaseModelTest {
    private val target = WatchTarget("https://relay.example.com:4433/moq", "live/channel-1")
    private val video = VideoTrackInfo("video/avc", 1432, 1080)
    private val audio = AudioTrackInfo("audio/mp4a-latm", 48_000, 2)

    private fun PlayerState.through(vararg updates: SessionUpdate) = updates.fold(this) { state, update -> state.after(update) }

    @Test fun `a session moves from connecting to waiting to playing, carrying the negotiated draft`() {
        val start = PlayerState(1, target).through(SessionUpdate.Started)
        assertSame(PlayerPhase.Connecting, start.phase)
        val connected = start.after(SessionUpdate.Event(PlaybackEvent.Connected(MoqVersion.Draft16)))
        assertSame(PlayerPhase.Waiting, connected.phase)
        assertEquals(MoqVersion.Draft16, connected.version)
        val playing = connected.after(SessionUpdate.Event(PlaybackEvent.Selected(video, audio)))
        assertSame(PlayerPhase.Playing, playing.phase)
        assertEquals(listOf(MoqVersion.Draft16, video, audio), listOf(playing.version, playing.video, playing.audio))
        assertEquals(true, playing.running)
    }

    @Test fun `the session's facts outlive its stop, and only the next session clears them`() {
        val stopped =
            PlayerState(1, target).through(
                SessionUpdate.Started,
                SessionUpdate.Event(PlaybackEvent.Connected(MoqVersion.Draft16)),
                SessionUpdate.Event(PlaybackEvent.Selected(video, audio)),
                SessionUpdate.Stopped,
            )
        assertEquals(PlayerPhase.Stopped(null), stopped.phase)
        assertEquals(listOf(MoqVersion.Draft16, video, audio), listOf(stopped.version, stopped.video, stopped.audio))
        assertEquals(PlayerState(1, target), stopped.after(SessionUpdate.Started))
    }

    @Test fun `audio remarks become a notice, the end line becomes the stop reason, other lines change nothing`() {
        val playing = PlayerState(1, target).through(SessionUpdate.Started, SessionUpdate.Event(PlaybackEvent.Selected(video, null)))
        val stalled = playing.after(SessionUpdate.Reported("audio output stalled; playing video only"))
        assertEquals("audio output stalled; playing video only", stalled.notice)
        assertSame(stalled, stalled.after(SessionUpdate.Reported("established: MoqVersion.Draft16")))
        val stopped = stalled.through(SessionUpdate.Reported("stopped: ReceiverEnded; 900 objects, 700 decoder inputs"), SessionUpdate.Stopped)
        assertEquals(PlayerPhase.Stopped("ReceiverEnded; 900 objects, 700 decoder inputs"), stopped.phase)
        assertEquals(false, stopped.running)
    }

    @Test fun `a viewer's stop has no end line and a failure survives the session's Stopped`() {
        val stoppedByViewer = PlayerState(1, target).through(SessionUpdate.Started, SessionUpdate.Stopped)
        assertEquals(PlayerPhase.Stopped(null), stoppedByViewer.phase)
        val failed = PlayerState(1, target).through(SessionUpdate.Started, SessionUpdate.Failed(IllegalStateException("boom")), SessionUpdate.Stopped)
        val phase = assertIs<PlayerPhase.Failed>(failed.phase)
        assertEquals(PlaybackProblemKind.Other, phase.problem.kind)
        assertEquals("boom", phase.problem.detail)
    }

    @Test fun `a new session clears the previous one's statistics, notice and end line`() {
        val old = PlayerState(1, target, phase = PlayerPhase.Stopped("x"), notice = "audio output resumed", ended = "x")
        val fresh = old.after(SessionUpdate.Started)
        assertEquals(PlayerState(1, target), fresh)
    }

    @Test fun `targets need an https or moqt relay with a host and a namespace of non-empty parts`() {
        assertNull(targetProblem("https://relay.example.com:4433/moq", "live/channel-1"))
        assertNull(targetProblem("moqt://relay.example.com:4433", "studio"))
        assertEquals("Enter a relay URL", targetProblem("", "a"))
        assertEquals("The relay URL starts with https:// or moqt://", targetProblem("http://relay", "a"))
        assertEquals("The relay URL names no host", targetProblem("https://:4433/moq", "a"))
        assertEquals("Enter the broadcast's namespace", targetProblem("https://relay", " "))
        assertEquals("Namespace parts are separated by single slashes", targetProblem("https://relay", "live//one"))
        assertEquals("channel-1", target.title)
        assertEquals("relay.example.com:4433", target.relay)
    }

    @Test fun `recents keep the newest first without duplicates, at most eight, and survive storage`() {
        val many = (1..10).map { WatchTarget("https://relay.example.com", "live/$it") }
        val kept = many.fold(emptyList<WatchTarget>()) { list, t -> list.remembering(t) }
        assertEquals((10 downTo 3).map { "live/$it" }, kept.map { it.namespace })
        assertEquals(listOf(many[4]) + kept.filterNot { it == many[4] }.take(7), kept.remembering(many[4]))
    }

    @Test fun `the saved state survives its text form, and damaged lines fall back to the defaults`() {
        val saved = Saved(ShowcaseSettings(DraftChoice.Draft18, StartChoice.NextGroup, showStats = true), target.url, target.namespace, listOf(target))
        assertEquals(saved, decodeSaved(encodeSaved(saved)))
        val valid = encodeSaved(Saved(recents = listOf(target))).lines().first { it.startsWith("recent=") }
        val damaged = "draft=Draft99\nstart=\nrecent=http%3A%2F%2Fx y\nrecent=%\nurl=%G\n" + valid + "\nnonsense\n"
        assertEquals(Saved(recents = listOf(target)), decodeSaved(damaged))
        assertEquals(Saved(), decodeSaved(""))
    }

    @Test fun `watch links carry a broadcast and optional draft and start, URL-encoded`() {
        val link = parseWatchLink("moq5://watch?relay=https%3A%2F%2Frelay.example.com%3A4433%2Fmoq&namespace=live%2Fchannel-1&draft=16&start=next")
        assertEquals(WatchLink(target, DraftChoice.Draft16, StartChoice.NextGroup), link)
        assertEquals(WatchLink(target, null, null), parseWatchLink("moq5://watch?namespace=live/channel-1&relay=https://relay.example.com:4433/moq"))
        assertNull(parseWatchLink("moq5://watch?relay=https://relay.example.com&namespace=a&draft=17"), "an unknown draft is refused, not ignored")
        assertNull(parseWatchLink("moq5://watch?relay=http://relay&namespace=a"))
        assertNull(parseWatchLink("https://example.com/watch?relay=https://relay&namespace=a"))
    }

    @Test fun `malformed escapes in a link make it no link, and an ordinary encoded link still parses`() {
        assertNull(parseWatchLink("moq5://watch?relay=https://relay.example.com&namespace=%"))
        assertNull(parseWatchLink("moq5://watch?relay=https://relay.example.com&namespace=%GG"))
        assertNull(parseWatchLink("moq5://watch?relay=%E0%A4&namespace=live"))
        assertEquals(WatchTarget("https://relay.example.com", "live/a b"), parseWatchLink("moq5://watch?relay=https%3A%2F%2Frelay.example.com&namespace=live%2Fa+b")?.target)
    }

    @Test fun `saved values round-trip whatever characters they hold and can never become records of their own`() {
        val relay = "https://relay.example.com"
        for (namespace in listOf("live\rdraft=Draft16", "live\ndraft=Draft18", "live\tx", "live/直播/ünïcode", "a%b=c&d")) {
            val saved = Saved(ShowcaseSettings(), relay, namespace, listOf(WatchTarget(relay, namespace)))
            assertEquals(saved, decodeSaved(encodeSaved(saved)), "namespace " + namespace.replace("\r", "\\r").replace("\n", "\\n"))
        }
    }

    @Test fun `settings become the engine request, Automatic and the live edge by default`() {
        val request = ShowcaseSettings().request(target)
        assertSame(VersionOffer.Automatic, request.versions)
        assertEquals(StartMode.Current, request.start)
        assertEquals(Int.MAX_VALUE, request.objectBudget)
        assertEquals(true, request.stats)
        val pinned = ShowcaseSettings(draft = DraftChoice.Draft16, start = StartChoice.NextGroup).request(target)
        assertEquals(listOf(MoqVersion.Draft16), assertIs<VersionOffer.Offer>(pinned.versions).versions)
        assertEquals(StartMode.NextGroup, pinned.start)
        assertEquals("draft-16", MoqVersion.Draft16.label())
    }
}
