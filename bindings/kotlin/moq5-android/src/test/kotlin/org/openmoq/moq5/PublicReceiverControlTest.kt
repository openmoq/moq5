package org.openmoq.moq5

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.engine.BackendCommandResult
import org.openmoq.moq5.engine.BackendTrackEvent
import org.openmoq.moq5.engine.BackendTrackEventKind
import org.openmoq.moq5.engine.BridgeContractException
import org.openmoq.moq5.engine.BridgeFailure
import org.openmoq.moq5.engine.EngineFixture
import org.openmoq.moq5.engine.ScriptedEndpointBackend
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * The receiver's manual controls, RED-first over the scripted backend
 * (`media_receiver.h` "Per-track subscription control":
 * `moq_media_receiver_subscribe_track` / `_unsubscribe_track` /
 * `_track_state`).
 *
 * Derived contract:
 *  * With `autoSubscribe = false` discovery subscribes nothing; objects for
 *    a track flow only after the application subscribes it AND the peer
 *    accepts (the backend's acceptance path). `MOQ_OK` is "validated and
 *    recorded", never acceptance; a rejection is a track ENDED event.
 *  * [MediaReceiver.subscribe] and [MediaReceiver.unsubscribe] are
 *    receiver-scoped suspend commands that run on the service worker; the
 *    binding keeps the desired state and forwards ONE backend command per
 *    off-to-on or on-to-off transition. A subscribe while already wanted is
 *    no command at all; a subscribe after an unsubscribe is a NEW command
 *    carrying that call's start/priority -- whether the C applies them is
 *    the C's business (it does not reconfigure an already-issued
 *    subscription; `service/tests/test_media_receiver_subscribe.c`), and
 *    nothing here claims to prove that.
 *  * [StartMode] is closed over the C values; the priority stays nullable
 *    (the C `has_priority`), with no invented default.
 *  * Unsubscribe purges that track's queued objects and pauses delivery;
 *    the [MediaTrack] and its catalog handle stay valid.
 *  * [TrackState] keeps all six C states; app pause and flow pause are
 *    distinct. [MediaReceiver.trackState] is a point-in-time native read.
 *  * Every refusal is TYPED: a foreign track (an argument error), a closed
 *    (detached) receiver, an unavailable track -- Ended (subscribe only) or
 *    Removed (every command), by its [TrackPresence] -- a track that cannot
 *    be subscribed (UNSUPPORTED), the receiver's fatal terminal with its
 *    exact code, and a bridge refusal (quarantine). Never null.
 *  * "Command" means subscribe and unsubscribe; [MediaReceiver.trackState]
 *    is a state READ. A command's refusal code carries no payload of its
 *    own. `MOQ_ERR_CLOSED` and `MOQ_ERR_WRONG_STATE` answered while the
 *    model shows the receiver live / the track Present make the binding
 *    drain the receiver's events AGAIN and decide from what it learned: a
 *    fatal terminal -> [ReceiverFatalException] with the exact code, a
 *    clean one -> [ReceiverClosedException]; a Removed or Ended learned ->
 *    [TrackUnavailableException] with that presence. A code the drain
 *    cannot explain is a bridge-contract failure, never an invented
 *    terminal or presence. Once a terminal is recorded, commands are refused
 *    locally, and a state read answers [TrackState.Ended] locally -- the C's
 *    own rule for a terminal receiver -- while a Removed track keeps its
 *    [TrackUnavailableException] and a closed (detached) receiver its
 *    [ReceiverClosedException].
 */
internal class PublicReceiverControlTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"
        val BOUND = 5.seconds
        val NAMESPACE = Namespace(listOf(NamespacePart("live".toByteArray())))
        val MANUAL = ReceiverConfig(namespace = NAMESPACE, overflow = OverflowPolicy.DropToKeyframe, autoSubscribe = false)
        val T1 = -0x7fff_ffff_ffff_fefdL
        val T2 = -0x7fff_ffff_ffff_fdfdL
        val VIDEO = TrackDescription(name = MediaBytes("v".toByteArray()), packaging = MediaBytes("loc".toByteArray()), role = MediaBytes("video".toByteArray()))
        val AUDIO = TrackDescription(name = MediaBytes("a".toByteArray()), packaging = MediaBytes("loc".toByteArray()), role = MediaBytes("audio".toByteArray()))
        fun added(t: Long, d: TrackDescription) = BackendTrackEvent(BackendTrackEventKind.Added, t, d)
        fun ended(t: Long) = BackendTrackEvent(BackendTrackEventKind.Ended, t, null)
        fun removed(t: Long) = BackendTrackEvent(BackendTrackEventKind.Removed, t, null)
        val ready = BackendTrackEvent(BackendTrackEventKind.CatalogReady, 0L, null)
    }

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    private suspend fun discovered(backend: ScriptedEndpointBackend, receiver: MediaReceiver): ReceiverInventory {
        backend.scriptTrackEvents(added(T1, VIDEO), added(T2, AUDIO), ready)
        backend.markActivity()
        return withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }
    }

    @Test
    fun `manual selection -- discovery subscribes nothing, subscribe records intent on the worker, objects arrive only after acceptance`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val inv = discovered(backend, receiver)
            val video = inv.tracks[0]
            assertEquals(0, backend.observed().subscribeCalls.size, "discovery issued no subscribe")
            assertEquals(TrackState.Discovered, withTimeout(BOUND) { receiver.trackState(video) })
            receiver.subscribe(video, start = StartMode.NextGroup, priority = 7u)
            val call = backend.observed().subscribeCalls.single()
            assertEquals(T1, call.track)
            assertEquals(StartMode.NextGroup, call.start)
            assertEquals(7u.toUByte(), call.priority)
            assertEquals(backend.observed().serviceThreadId, call.threadId, "recorded on the service worker")
            assertEquals(TrackState.Pending, withTimeout(BOUND) { receiver.trackState(video) }, "recorded, not accepted")
            backend.scriptObjects(objectFor(T1, "early"))
            backend.markActivity()
            assertEquals(0, backend.observed().objectsDeliverable, "nothing is deliverable before the peer accepts")
            backend.acceptSubscription(T1)
            assertEquals(TrackState.Active, withTimeout(BOUND) { receiver.trackState(video) })
            val o = withTimeout(BOUND) { receiver.objects.next() }!!
            assertTrue(o.track === video)
            endpoint.closeAndJoin()
        }

    @Test
    fun `the priority is nullable and crosses as absent when not given -- no invented default`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val inv = discovered(backend, receiver)
            receiver.subscribe(inv.tracks[0])
            val call = backend.observed().subscribeCalls.single()
            assertEquals(StartMode.Current, call.start, "the C default start")
            assertEquals(null, call.priority, "has_priority false")
            receiver.subscribe(inv.tracks[1], priority = 0u)
            assertEquals(0u.toUByte(), backend.observed().subscribeCalls[1].priority, "zero is a priority, not absence")
            endpoint.closeAndJoin()
        }

    @Test
    fun `unsubscribe purges that track's queued objects and pauses it, while another track is unaffected and the MediaTrack stays valid`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val inv = discovered(backend, receiver)
            val (video, audio) = inv.tracks
            receiver.subscribe(video)
            receiver.subscribe(audio)
            backend.acceptSubscription(T1)
            backend.acceptSubscription(T2)
            backend.scriptObjects(objectFor(T1, "v1"), objectFor(T2, "a1"), objectFor(T1, "v2"))
            receiver.unsubscribe(video)
            assertEquals(listOf(T1), backend.observed().unsubscribeCalls.map { it.track })
            assertEquals(1, backend.observed().objectsLeftQueued, "the video objects were purged; the audio one remains")
            assertEquals("a1", String(withTimeout(BOUND) { receiver.objects.next() }!!.mediaBytes().bytes()))
            assertEquals(TrackState.PausedApp, withTimeout(BOUND) { receiver.trackState(video) })
            assertEquals(TrackState.Active, withTimeout(BOUND) { receiver.trackState(audio) })
            assertTrue(!video.isRetired && video.presence == TrackPresence.Present, "the handle and the public track stay valid")
            assertTrue(receiver.inventory.value.tracks[0] === video)
            endpoint.closeAndJoin()
        }

    @Test
    fun `repeated subscribe and unsubscribe are idempotent -- one backend command per transition, and a subscribe after an unsubscribe carries that call's arguments`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val video = discovered(backend, receiver).tracks[0]
            receiver.subscribe(video, start = StartMode.NextGroup, priority = 3u)
            receiver.subscribe(video, start = StartMode.Current, priority = 9u)
            receiver.subscribe(video)
            assertEquals(1, backend.observed().subscribeCalls.size, "already wanted: no second backend command, whatever the arguments")
            receiver.unsubscribe(video)
            receiver.unsubscribe(video)
            assertEquals(1, backend.observed().unsubscribeCalls.size, "already not wanted: no second backend command")
            receiver.subscribe(video, start = StartMode.Current, priority = 9u)
            assertEquals(2, backend.observed().subscribeCalls.size, "off-to-on again: one new command")
            val again = backend.observed().subscribeCalls[1]
            assertEquals(StartMode.Current, again.start, "the new command carries THIS call's start; whether the C applies it to an existing subscription is the C's rule, not asserted here")
            assertEquals(9u.toUByte(), again.priority)
            assertEquals(TrackState.Pending, withTimeout(BOUND) { receiver.trackState(video) }, "wanted again, not yet accepted")
            endpoint.closeAndJoin()
        }

    @Test
    fun `track state maps all six C values and keeps app pause and flow pause distinct`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val (video, audio) = discovered(backend, receiver).tracks
            assertEquals(TrackState.Discovered, withTimeout(BOUND) { receiver.trackState(video) })
            receiver.subscribe(video)
            assertEquals(TrackState.Pending, withTimeout(BOUND) { receiver.trackState(video) })
            backend.acceptSubscription(T1)
            assertEquals(TrackState.Active, withTimeout(BOUND) { receiver.trackState(video) })
            backend.flowPaused = true
            assertEquals(TrackState.PausedFlow, withTimeout(BOUND) { receiver.trackState(video) }, "backpressure, not the app")
            receiver.unsubscribe(video)
            assertEquals(TrackState.PausedApp, withTimeout(BOUND) { receiver.trackState(video) }, "the app's pause outranks flow")
            backend.flowPaused = false
            backend.scriptTrackEvents(ended(T2))
            backend.markActivity()
            withTimeout(BOUND) { receiver.inventory.first { it.tracks[1].presence == TrackPresence.Ended } }
            assertEquals(TrackState.Ended, withTimeout(BOUND) { receiver.trackState(audio) })
            endpoint.closeAndJoin()
        }

    @Test
    fun `an unknown native state value fails closed by field name`() {
        for ((raw, state) in listOf(0 to TrackState.Discovered, 1 to TrackState.Pending, 2 to TrackState.Active, 3 to TrackState.PausedApp, 4 to TrackState.PausedFlow, 5 to TrackState.Ended)) {
            assertEquals(state, TrackState.ofNative(raw))
        }
        assertEquals("track state", assertFailsWith<UnknownNativeValueException> { TrackState.ofNative(6) }.field)
        assertEquals("track state", assertFailsWith<UnknownNativeValueException> { TrackState.ofNative(-1) }.field)
    }

    @Test
    fun `a foreign track -- another receiver's -- is refused by name before any backend call`() =
        runTest {
            val backendA = newBackend()
            val endpointA = MoqEndpoint.open(EndpointConfig(URL)) { backendA }
            val receiverA = endpointA.attachReceiver(MANUAL)
            val videoA = discovered(backendA, receiverA).tracks[0]
            val backendB = newBackend()
            val endpointB = MoqEndpoint.open(EndpointConfig(URL)) { backendB }
            val receiverB = endpointB.attachReceiver(MANUAL)
            discovered(backendB, receiverB)
            val foreign = assertFailsWith<ForeignTrackException> { receiverB.subscribe(videoA) }
            assertTrue(IllegalArgumentException::class.java.isAssignableFrom(foreign.javaClass), "a foreign track is an argument error, not a state")
            assertFailsWith<ForeignTrackException> { receiverB.unsubscribe(videoA) }
            assertFailsWith<ForeignTrackException> { receiverB.trackState(videoA) }
            assertEquals(0, backendB.observed().subscribeCalls.size + backendB.observed().unsubscribeCalls.size + backendB.observed().trackStateCalls.size)
            endpointA.closeAndJoin()
            endpointB.closeAndJoin()
        }

    @Test
    fun `a retired track and a closed receiver are refused by name, with no backend call`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val video = discovered(backend, receiver).tracks[0]
            receiver.close()
            assertTrue(video.isRetired)
            val closed = assertFailsWith<ReceiverClosedException> { receiver.subscribe(video) }
            assertTrue(IllegalStateException::class.java.isAssignableFrom(closed.javaClass))
            assertFailsWith<ReceiverClosedException> { receiver.unsubscribe(video) }
            assertFailsWith<ReceiverClosedException> { receiver.trackState(video) }
            assertEquals(0, backend.observed().subscribeCalls.size + backend.observed().unsubscribeCalls.size + backend.observed().trackStateCalls.size)
            endpoint.closeAndJoin()
        }

    @Test
    fun `an ended track -- subscribe is refused with presence Ended, unsubscribe is a no-op, and its state reads Ended`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val audio = discovered(backend, receiver).tracks[1]
            receiver.subscribe(audio)
            backend.acceptSubscription(T2)
            backend.scriptTrackEvents(ended(T2))
            backend.markActivity()
            withTimeout(BOUND) { receiver.inventory.first { it.tracks[1].presence == TrackPresence.Ended } }
            val refused = assertFailsWith<TrackUnavailableException> { receiver.subscribe(audio) }
            assertEquals(TrackPresence.Ended, refused.presence)
            receiver.unsubscribe(audio) // the C's OK: a no-op on an ended track
            assertEquals(TrackState.Ended, withTimeout(BOUND) { receiver.trackState(audio) })
            endpoint.closeAndJoin()
        }

    @Test
    fun `a removed track -- every command is refused with presence Removed, matching the C WRONG_STATE for all three`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val audio = discovered(backend, receiver).tracks[1]
            backend.scriptTrackEvents(removed(T2))
            backend.markActivity()
            withTimeout(BOUND) { receiver.inventory.first { it.tracks[1].presence == TrackPresence.Removed } }
            assertEquals(TrackPresence.Removed, assertFailsWith<TrackUnavailableException> { receiver.subscribe(audio) }.presence)
            assertEquals(TrackPresence.Removed, assertFailsWith<TrackUnavailableException> { receiver.unsubscribe(audio) }.presence)
            assertEquals(TrackPresence.Removed, assertFailsWith<TrackUnavailableException> { receiver.trackState(audio) }.presence)
            assertTrue(receiver.inventory.value.tracks[1] === audio, "the public track stays listed; only its operations are unavailable")
            endpoint.closeAndJoin()
        }

    @Test
    fun `a not-subscribable track is refused with the C UNSUPPORTED, typed`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val audio = discovered(backend, receiver).tracks[1]
            backend.subscribeResult = BackendCommandResult.Refused(MoqResultCode(-14))
            assertFailsWith<TrackNotSubscribableException> { receiver.subscribe(audio) }
            assertEquals(TrackState.Discovered, withTimeout(BOUND) { receiver.trackState(audio) })
            endpoint.closeAndJoin()
        }

    @Test
    fun `a command answered CLOSED before the model knew the terminal drains the events to learn it, and carries the exact fatal code`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val video = discovered(backend, receiver).tracks[0]
            assertEquals(null, receiver.inventory.value.terminal)
            // The terminal becomes visible to the track poll ONLY once the
            // command has returned: the pre-command drain sees Empty, the
            // command answers a bare CLOSED, and the post-command drain is
            // what learns the exact code.
            backend.subscribeResult = BackendCommandResult.Refused(MoqResultCode(-4))
            backend.afterSubscribe = { backend.pollTrackTerminal = org.openmoq.moq5.engine.BackendTrackEventResult.Closed(fatal = true, code = 0x3uL) }
            val pollsBefore = backend.observed().trackPolls.size
            val fatal = assertFailsWith<ReceiverFatalException> { receiver.subscribe(video) }
            assertEquals(ReceiverFatalCode.SETUP_FAILED, fatal.code, "the exact code, learned by the post-command drain")
            val polls = backend.observed().trackPolls.drop(pollsBefore)
            val commandAt = backend.observed().callOrder.lastIndexOf("track-subscribe")
            assertTrue(commandAt >= 0, "the command reached the backend")
            assertTrue(polls.first().result is org.openmoq.moq5.engine.BackendTrackEventResult.Empty, "the pre-command drain saw no terminal; saw ${polls.first().result}")
            assertTrue(polls.last().result is org.openmoq.moq5.engine.BackendTrackEventResult.Closed, "the post-command drain learned it")
            assertEquals(ReceiverTerminal.Fatal(ReceiverFatalCode.SETUP_FAILED), receiver.inventory.value.terminal, "and recorded on the inventory")
            assertEquals(1, backend.observed().subscribeCalls.size, "exactly one command reached the backend")
            assertFailsWith<ReceiverFatalException> { receiver.unsubscribe(video) }
            assertEquals(TrackState.Ended, withTimeout(BOUND) { receiver.trackState(video) }, "a state read is not a command: after a terminal every track reads Ended, the C's own rule, answered locally")
            assertEquals(0, backend.observed().unsubscribeCalls.size, "a recorded terminal refuses commands locally: no unsubscribe reached the backend")
            assertEquals(0, backend.observed().trackStateCalls.size, "and the state read was answered without a native read")
            assertEquals(1, backend.observed().subscribeCalls.size, "still the one subscribe")
            endpoint.closeAndJoin()
        }

    @Test
    fun `a command answered CLOSED with a clean terminal learnable only after it surfaces ReceiverClosedException`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val video = discovered(backend, receiver).tracks[0]
            backend.subscribeResult = BackendCommandResult.Refused(MoqResultCode(-4))
            backend.afterSubscribe = { backend.pollTrackTerminal = org.openmoq.moq5.engine.BackendTrackEventResult.Closed(fatal = false, code = 0uL) }
            val pollsBefore = backend.observed().trackPolls.size
            assertFailsWith<ReceiverClosedException> { receiver.subscribe(video) }
            val polls = backend.observed().trackPolls.drop(pollsBefore)
            assertTrue(polls.first().result is org.openmoq.moq5.engine.BackendTrackEventResult.Empty, "the pre-command drain saw no terminal")
            assertTrue(polls.last().result is org.openmoq.moq5.engine.BackendTrackEventResult.Closed, "the post-command drain learned the clean close")
            assertEquals(ReceiverTerminal.Closed, receiver.inventory.value.terminal)
            assertEquals(1, backend.observed().subscribeCalls.size)
            assertFailsWith<ReceiverClosedException> { receiver.unsubscribe(video) }
            assertEquals(0, backend.observed().unsubscribeCalls.size)
            assertEquals(TrackState.Ended, withTimeout(BOUND) { receiver.trackState(video) }, "the state read after a clean terminal: Ended, as the C reports it")
            assertEquals(0, backend.observed().trackStateCalls.size)
            endpoint.closeAndJoin()
        }

    @Test
    fun `a command answered CLOSED while no terminal can be learned is a bridge-contract failure, never an invented clean close`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val video = discovered(backend, receiver).tracks[0]
            backend.subscribeResult = BackendCommandResult.Refused(MoqResultCode(-4))
            // The track poll keeps answering Empty: the backend lies.
            val failure = assertFailsWith<BridgeContractException> { receiver.subscribe(video) }
            assertEquals("track-subscribe-closed", failure.failure.op)
            assertEquals(null, receiver.inventory.value.terminal, "nothing was invented")
            backend.expireWait()
            runCatching { endpoint.closeAndJoin() }
        }

    @Test
    fun `a subscribe answered WRONG_STATE while the model shows Present drains again -- a Removed learned there is the refusal's presence`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val audio = discovered(backend, receiver).tracks[1]
            assertEquals(TrackPresence.Present, audio.presence)
            backend.subscribeResult = BackendCommandResult.Refused(MoqResultCode(-5))
            backend.afterSubscribe = { backend.scriptTrackEvents(removed(T2)) }
            val refused = assertFailsWith<TrackUnavailableException> { receiver.subscribe(audio) }
            assertEquals(TrackPresence.Removed, refused.presence, "learned by the post-command drain, never inferred from the bare code")
            assertEquals(TrackPresence.Removed, receiver.inventory.value.tracks[1].presence, "and recorded")
            assertEquals(1, backend.observed().subscribeCalls.size)
            endpoint.closeAndJoin()
        }

    @Test
    fun `a subscribe answered WRONG_STATE while the model shows Present drains again -- an Ended learned there is the refusal's presence`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val audio = discovered(backend, receiver).tracks[1]
            backend.subscribeResult = BackendCommandResult.Refused(MoqResultCode(-5))
            backend.afterSubscribe = { backend.scriptTrackEvents(ended(T2)) }
            val refused = assertFailsWith<TrackUnavailableException> { receiver.subscribe(audio) }
            assertEquals(TrackPresence.Ended, refused.presence)
            assertEquals(TrackPresence.Ended, receiver.inventory.value.tracks[1].presence)
            endpoint.closeAndJoin()
        }

    @Test
    fun `an unsubscribe and a state read answered WRONG_STATE while Present are explained only by a Removed learned afterwards`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val audio = discovered(backend, receiver).tracks[1]
            receiver.subscribe(audio)
            backend.acceptSubscription(T2)
            backend.trackStateResult = BackendCommandResult.Refused(MoqResultCode(-5))
            backend.afterTrackState = { backend.scriptTrackEvents(removed(T2)) }
            val refused = assertFailsWith<TrackUnavailableException> { receiver.trackState(audio) }
            assertEquals(TrackPresence.Removed, refused.presence, "a removed track has no state: WRONG_STATE, explained by the drain")
            assertEquals(1, backend.observed().trackStateCalls.size)
            // Now recorded as Removed: the unsubscribe is refused locally, no backend call.
            assertEquals(TrackPresence.Removed, assertFailsWith<TrackUnavailableException> { receiver.unsubscribe(audio) }.presence)
            assertEquals(0, backend.observed().unsubscribeCalls.size)
            endpoint.closeAndJoin()
        }

    @Test
    fun `an unsubscribe answered WRONG_STATE while Present is explained by a Removed learned afterwards, and later commands are refused locally`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val audio = discovered(backend, receiver).tracks[1]
            receiver.subscribe(audio)
            backend.acceptSubscription(T2)
            assertEquals(TrackState.Active, withTimeout(BOUND) { receiver.trackState(audio) })
            backend.unsubscribeResult = BackendCommandResult.Refused(MoqResultCode(-5))
            backend.afterUnsubscribe = { backend.scriptTrackEvents(removed(T2)) }
            val refused = assertFailsWith<TrackUnavailableException> { receiver.unsubscribe(audio) }
            assertEquals(TrackPresence.Removed, refused.presence, "learned by the post-command drain")
            assertEquals(TrackPresence.Removed, receiver.inventory.value.tracks[1].presence, "recorded")
            assertEquals(1, backend.observed().unsubscribeCalls.size, "exactly one backend unsubscribe")
            val subscribes = backend.observed().subscribeCalls.size
            val states = backend.observed().trackStateCalls.size
            assertEquals(TrackPresence.Removed, assertFailsWith<TrackUnavailableException> { receiver.subscribe(audio) }.presence)
            assertEquals(TrackPresence.Removed, assertFailsWith<TrackUnavailableException> { receiver.trackState(audio) }.presence)
            assertEquals(subscribes, backend.observed().subscribeCalls.size, "refused locally: no further command")
            assertEquals(states, backend.observed().trackStateCalls.size, "nor a state read")
            assertEquals(1, backend.observed().unsubscribeCalls.size)
            endpoint.closeAndJoin()
        }

    @Test
    fun `an unsubscribe answered CLOSED while no terminal can be learned quarantines with track-unsubscribe-closed and invents no terminal`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val audio = discovered(backend, receiver).tracks[1]
            receiver.subscribe(audio)
            backend.acceptSubscription(T2)
            backend.unsubscribeResult = BackendCommandResult.Refused(MoqResultCode(-4))
            val failure = assertFailsWith<BridgeContractException> { receiver.unsubscribe(audio) }
            assertEquals("track-unsubscribe-closed", failure.failure.op)
            assertEquals(null, receiver.inventory.value.terminal, "nothing was invented")
            assertEquals(1, backend.observed().unsubscribeCalls.size)
            backend.expireWait()
            runCatching { endpoint.closeAndJoin() }
        }

    @Test
    fun `a WRONG_STATE the post-command drain cannot explain is a bridge-contract failure, never an invented presence`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val audio = discovered(backend, receiver).tracks[1]
            backend.subscribeResult = BackendCommandResult.Refused(MoqResultCode(-5))
            val failure = assertFailsWith<BridgeContractException> { receiver.subscribe(audio) }
            assertEquals("track-subscribe-wrong-state", failure.failure.op)
            assertEquals(TrackPresence.Present, receiver.inventory.value.tracks[1].presence, "nothing was invented")
            backend.expireWait()
            runCatching { endpoint.closeAndJoin() }
        }

    @Test
    fun `a bridge refusal on a command quarantines the endpoint and is not a receiver outcome`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val video = discovered(backend, receiver).tracks[0]
            backend.subscribeResult = BackendCommandResult.Bridge(BridgeFailure("track-subscribe", -1004))
            val failure = assertFailsWith<BridgeContractException> { receiver.subscribe(video) }
            assertEquals(-1004, failure.failure.code)
            assertEquals(null, receiver.inventory.value.terminal)
            backend.expireWait()
            runCatching { endpoint.closeAndJoin() }
        }

    private fun objectFor(t: Long, bytes: String) =
        org.openmoq.moq5.engine.BackendObject(
            track = t, payload = MediaPayload.Raw(MediaBytes(bytes.toByteArray())), status = ObjectStatus.Normal,
            endOfGroup = false, viaDatagram = false, keyframe = true, captureTime = null,
            decodeTime = MediaMicros(0uL), compositionOffset = MediaOffsetMicros(0L), presentationTime = MediaMicros(0uL),
        )
}
