package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * The audio capture: the AAC encoder, the microphone that feeds it, and the
 * bounded pump between them, driven through the real session, ownership,
 * handoff, ledger and [TrackEncoder]. The microphone, the encoder, the worker
 * dispatcher and its timer are scripted: every platform call is logged in
 * order, the dispatcher QUEUES what is posted to the worker until the row runs
 * it, and the timer fires only when the row says. `AudioRecordCapture.kt`
 * implements [MicrophonePort] over `AudioRecord` and is compiled against the
 * SDK; these rows cannot execute it.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class AudioCaptureTest {

    /** The owned worker as a queue: posts wait until the row runs them, and a quit worker takes nothing. */
    private class QueuedDispatcher : CallbackDispatcher {
        private val queue = ArrayDeque<() -> Unit>()

        @Volatile var refuse = false

        @Volatile private var quit = false

        override val isAlive: Boolean get() = !quit

        override fun deliver(callback: () -> Unit): Boolean {
            if (refuse || quit) return false
            synchronized(queue) { queue.addLast(callback) }
            return true
        }

        override fun quit() {
            quit = true
        }

        val pending: Int get() = synchronized(queue) { queue.size }

        /** Runs what is queued, in order, including anything queued while running. */
        fun drain() {
            while (true) {
                val next = synchronized(queue) { queue.removeFirstOrNull() } ?: return
                next()
            }
        }
    }

    /** The worker's one-shot timer: each post logged with its delay; fired only by the row, cancelled or not. */
    private class ScriptedTimer(private val log: MutableList<String>) : WorkerTimer {
        class Posted(val action: () -> Unit) {
            @Volatile var cancelled = false
        }

        val posts = mutableListOf<Posted>()

        @Volatile var refuse = false

        override fun post(delayMillis: Long, action: () -> Unit): TimerTicket? {
            synchronized(log) { log.add("timer:post $delayMillis") }
            if (refuse) return null
            val posted = Posted(action)
            synchronized(posts) { posts.add(posted) }
            return TimerTicket { posted.cancelled = true }
        }

        fun count(): Int = synchronized(posts) { posts.size }

        /** Runs arm [i]'s action as a queued delayed callback would: whether or not it was cancelled. */
        fun fire(i: Int) = synchronized(posts) { posts[i] }.action()

        fun fireLatest() = fire(count() - 1)
    }

    /** One scripted recorder: its reads, timestamps, failures and holds. */
    private class Mic(private val log: MutableList<String>) : MicrophoneLink {
        override val identity: Any = HostResource("mic")

        @Volatile override var initialized = true

        @Volatile override var format = MicrophoneFormat(48_000, 2, pcm16 = true)

        @Volatile override var bufferSizeInFrames = 2048

        @Volatile var listenResult = 0

        @Volatile var wake: (() -> Unit)? = null

        @Volatile var startsRecording = true

        @Volatile override var recording = false

        /** The step that raises, and what it raises. */
        @Volatile var failAt: String? = null

        @Volatile var failure: Throwable? = null

        /** Raised by `release`, after it is logged. */
        @Volatile var releaseFailure: Throwable? = null

        @Volatile var onStart: () -> Unit = {}

        /** Scripted read results, in order: a positive count fills that many pattern bytes; 0 or a negative code as itself. */
        val reads = ArrayDeque<Int>()

        @Volatile var timestamp: AudioTimestampFacts? = null

        /**
         * FIXTURE-ONLY source truth, which nothing in [MicrophoneLink]
         * exposes: the cursor over the frames the scripted platform captured,
         * and the exact source-frame range each successful read delivered.
         * [skipSourceFrames] loses frames at the source, between reads,
         * without delivering them -- a loss the recorder does not report.
         */
        private var sourceCursor = 0L

        val delivered = mutableListOf<LongRange>()

        /**
         * FIXTURE-ONLY: what each call actually RETURNED to the product --
         * every read's count and a digest of its bytes, every timestamp's
         * values -- in order. The call log records only the calls.
         */
        val returned = mutableListOf<String>()

        fun skipSourceFrames(frames: Long) {
            sourceCursor += frames
        }

        /** When set, the next read is held here until released. */
        @Volatile var holdRead: Pair<HostGate, HostGate>? = null

        private var pattern = 0

        private fun step(what: String) {
            synchronized(log) { log.add("mic:$what") }
            if (failAt == what) throw checkNotNull(failure)
        }

        override fun listen(periodFrames: Int, wake: () -> Unit): Int {
            step("listen $periodFrames")
            this.wake = wake
            return listenResult
        }

        override fun start() {
            step("start")
            recording = startsRecording
            onStart()
        }

        override fun read(into: ByteBuffer, bytes: Int): Int {
            step("read $bytes")
            holdRead?.let { (reached, release) ->
                holdRead = null
                reached.open()
                release.await()
            }
            val result = synchronized(reads) { reads.removeFirstOrNull() } ?: 0
            if (result <= 0) {
                synchronized(returned) { returned.add("read $bytes -> $result") }
                return result
            }
            val n = minOf(result, bytes)
            val view = into.duplicate()
            view.position(0)
            repeat(n) { view.put((pattern++ and 0xff).toByte()) }
            val digest = java.util.zip.CRC32().also { crc -> val copy = into.duplicate(); copy.position(0); copy.limit(n); val bytesRead = ByteArray(n); copy.get(bytesRead); crc.update(bytesRead) }.value
            synchronized(returned) { returned.add("read $bytes -> $n crc $digest") }
            val frames = (n / (format.channelCount * 2)).toLong()
            synchronized(delivered) { delivered.add(sourceCursor until sourceCursor + frames) }
            sourceCursor += frames
            return n
        }

        override fun timestamp(): AudioTimestampFacts? {
            synchronized(log) { log.add("mic:timestamp") }
            val facts = timestamp
            synchronized(returned) { returned.add("timestamp -> $facts") }
            return facts
        }

        override fun detach() = step("detach")

        override fun stop() = step("stop")

        override fun release() {
            step("release")
            releaseFailure?.let { throw it }
        }

        /** The pattern bytes the first [count] bytes read carried. */
        fun expected(from: Int, count: Int) = ByteArray(count) { ((from + it) and 0xff).toByte() }
    }

    private class MicPort(private val mic: Mic, private val log: MutableList<String>) : MicrophonePort {
        @Volatile var buildFailure: Throwable? = null

        override fun build(configuration: AudioEncoderConfiguration): MicrophoneLink {
            synchronized(log) { log.add("mic:build") }
            buildFailure?.let { throw it }
            return mic
        }
    }

    /** The audio ledger, every observation logged in the one order the row reads. */
    private class ObservingLedger(private val inner: CaptureLedger, private val log: MutableList<String>) : CaptureLedger by inner {
        override fun observe(generation: Long, kind: MediaKind, observation: CaptureObservation): ObserveOutcome {
            synchronized(log) { log.add("observe ${observation.key} ${observation.captureElapsedNanos}") }
            return inner.observe(generation, kind, observation)
        }
    }

    /** The real ownership, with the moment it installs the microphone logged. */
    private class InstallLogging(private val inner: CaptureOwnership, private val log: MutableList<String>) : CaptureOwnership by inner {
        override fun acquire(resource: CaptureResource, open: () -> ResourceHandle): AcquireOutcome =
            inner.acquire(resource, open).also {
                if (resource == CaptureResource.Microphone && it == AcquireOutcome.Acquired) synchronized(log) { log.add("installed:Microphone") }
            }
    }

    // A minimal camera, for the rows that interleave the two parts.
    private class Cam(private val log: MutableList<String>) : CameraLink {
        override val identity: Any = HostResource("camera")

        @Volatile var events: StreamEvents? = null

        @Volatile var inline = false

        override fun close() = synchronized(log) { log.add("close:camera") }.let { }

        override fun createStream(surface: Any, events: StreamEvents) {
            synchronized(log) { log.add("create-stream:camera") }
            this.events = events
            if (inline) events.onConfigured(stream)
        }

        val stream =
            object : StreamLink {
                override val identity: Any = HostResource("stream")

                override fun close() = synchronized(log) { log.add("close:stream") }.let { }

                override fun startRepeating(surface: Any, fps: IntRange) = synchronized(log) { log.add("repeat:stream") }.let { }
            }
    }

    private class CamPort(private val cam: Cam, private val log: MutableList<String>) : CameraPort {
        @Volatile var events: CameraEvents? = null

        override fun facts() = CameraFacts(listOf(640 to 480), listOf(30..30))

        override fun open(events: CameraEvents) {
            synchronized(log) { log.add("open-camera") }
            this.events = events
            if (cam.inline) events.onOpened(cam)
        }
    }

    private class Rig(config: AudioEncoderConfiguration = AUDIO_48K, withCamera: Boolean = false) {
        val log = mutableListOf<String>()
        val backend = HostEncoderBackend(log)
        val audio = TrackEncoder(config, backend)
        val mic = Mic(log).also { it.format = MicrophoneFormat(config.sampleRate, config.channelCount, pcm16 = true) }
        val port = MicPort(mic, log)
        val timer = ScriptedTimer(log)
        val dispatcher = QueuedDispatcher()
        val late = LateLog()
        val part = AudioPart(audio, port, timer, dispatcher, late)
        val video: TrackEncoder? = if (withCamera) TrackEncoder(VIDEO, backend) else null
        val cam = Cam(log)
        val camPort = CamPort(cam, log)
        private val worker =
            WorkerStart {
                synchronized(log) { log.add("open:worker") }
                object : ResourceHandle {
                    override val identity: Any = HostResource("worker")

                    override fun close() {
                        synchronized(log) { log.add("quit:worker") }
                        dispatcher.quit()
                    }
                }
            }
        val acquirer = SequencedAcquirer(worker, video?.let { CameraPart(it, camPort, late) }, part)
        val ownership = OrderedCaptureOwnership(GENERATION, dispatcher, late)
        val forward = Forward()
        val ledger = ObservingLedger(BoundedCaptureLedger(MediaKind.Audio, GENERATION, capacity = 8), log)
        val keying = UnitAudioKeying(config.sampleRate, AudioEncoderConfiguration.FRAMES_PER_UNIT, ORIGIN)
        val audioHandoff = BoundedCaptureHandoff(MediaKind.Audio, GENERATION, audio, audio, ledger, forward, CAPACITY, HOST_MAX_OUTPUT_BYTES, lifetime = audio)
        val videoHandoff: CaptureHandoff =
            video?.let { BoundedCaptureHandoff(MediaKind.Video, GENERATION, it, it, BoundedCaptureLedger(MediaKind.Video, GENERATION, 8), forward, CAPACITY, HOST_MAX_OUTPUT_BYTES, lifetime = it) }
                ?: Lane(MediaKind.Video, forward).handoff
        val listener = HostSessionListener()
        val session = PlatformCaptureSession(GENERATION, InstallLogging(ownership, log), acquirer, videoHandoff, audioHandoff, listener)

        init {
            forward.to = session
            acquirer.bindFaults(forward)
            acquirer.bindAudioInputs(ledger, keying, GENERATION)
            audio.bind(EncoderRoute(session, session.bindCodecCallbacks(MediaKind.Audio), GENERATION, audioHandoff) { _, _ -> })
            video?.bind(EncoderRoute(session, session.bindCodecCallbacks(MediaKind.Video), GENERATION, videoHandoff) { _, _ -> })
        }

        fun start(): CaptureStart = session.start(Granted(GENERATION), CaptureCapability.Available, CaptureCapability.Available)

        fun logNow(): List<String> = synchronized(log) { log.toList() }

        fun count(entry: String): Int = logNow().count { it == entry }

        fun ordered(vararg wanted: String): List<String> = logNow().filter { it in wanted }

        /** Whether [first] was logged, and before [then]. */
        fun before(first: String, then: String): Boolean {
            val steps = logNow()
            return first in steps && then in steps && steps.indexOf(first) < steps.indexOf(then)
        }

        fun device(): HostEncoderDevice = backend.device(MediaKind.Audio)

        /** The codec offers PCM input [index], as its callback would. */
        fun input(index: Int) = checkNotNull(device().registered).onInputAvailable(index)

        /** A position notification: a wake hint on the worker. */
        fun wake() = checkNotNull(mic.wake).invoke()

        /** The last complete read (at or after [from]). */
        fun reads(): List<String> = logNow().filter { it.startsWith("mic:read") }
    }

    private val audioOpening =
        listOf("open:worker", "create:Audio", "callbacks:audio", "configure:audio", "start:audio", "mic:build", "mic:listen 1024", "mic:start")

    private fun codecDisposal(name: String = "audio") = arrayOf("stop:$name", "release-codec:$name")

    private val micDisposal = arrayOf("mic:detach", "mic:stop", "mic:release")

    /** One unit of 48 kHz stereo 16-bit PCM. */
    private val unit48 = AUDIO_48K.maxInputBytes

    /** A timestamp placing frame 1024 at one second on the elapsed-realtime base. */
    private fun stamp(framePosition: Long = 1024, nanos: Long = 1_000_000_000, base: PlatformTimebase = PlatformTimebase.ElapsedRealtime) =
        AudioTimestampFacts(framePosition, nanos, base)

    /** The ending settles: the worker quit once and last, and every protection balanced. */
    private suspend fun settled(rig: Rig, label: String): RetirementReceipt {
        assertEquals(1, rig.count("quit:worker"), "$label: ${rig.logNow()}")
        assertEquals("quit:worker", rig.logNow().last(), "$label: ${rig.logNow()}")
        assertFalse(rig.ownership.retainWorker(), label)
        assertReservationsBalanced(rig.ownership, label)
        return rig.session.retirement()
    }

    // -- acquisition and activation -----------------------------------------------------------------

    @Test
    fun `a1 the worker, the encoder and then the microphone, then activation arms the watchdog and posts the bootstrap`() = runTest {
        val rig = Rig()
        assertEquals(CaptureStart.Started, rig.start())
        assertEquals(audioOpening + "installed:Microphone" + "timer:post 1000", rig.logNow(), "activation follows the installed microphone")
        assertEquals(1, rig.timer.count(), "one watchdog armed")
        assertEquals(1, rig.dispatcher.pending, "one bootstrap pass posted")
        rig.mic.reads.addLast(0)
        rig.dispatcher.drain()
        assertEquals("mic:read $unit48", rig.logNow().last(), "the bootstrap pass reads at most one unit")
        rig.session.stop(null)
        assertEquals(listOf(*micDisposal, *codecDisposal(), "quit:worker"), rig.ordered(*micDisposal, *codecDisposal(), "quit:worker"), "the microphone stops and releases before its codec is disposed")
        settled(rig, "success")
    }

    @Test
    fun `a2 a pass the worker runs before the microphone is installed cannot be the one that is lost`() = runTest {
        val rig = Rig()
        // Inside the open: the worker runs whatever is posted, and a position
        // notification arrives -- both before the microphone is installed.
        rig.mic.onStart = {
            rig.dispatcher.drain()
            rig.wake()
        }
        assertEquals(CaptureStart.Started, rig.start())
        assertEquals(audioOpening + "installed:Microphone" + "timer:post 1000", rig.logNow(), "nothing read, stamped, taken or armed before installation")
        assertEquals(1, rig.timer.count(), "the watchdog is armed by activation")
        assertEquals(1, rig.dispatcher.pending, "and the bootstrap is still owed")
        rig.dispatcher.drain()
        assertEquals("mic:read $unit48", rig.logNow().last(), "the bootstrap runs once the microphone is installed")
        rig.session.stop(null)
        settled(rig, "startup race")
    }

    @Test
    fun `a3 an activation that cannot be scheduled ends the startup by name and closes the installed microphone`() = runTest {
        for (what in listOf("timer", "bootstrap")) {
            val rig = Rig()
            if (what == "timer") rig.timer.refuse = true else rig.dispatcher.refuse = true
            val reason = if (what == "timer") "the capture watchdog could not be scheduled" else "the capture bootstrap could not be posted"
            assertEquals(CaptureStart.Refused(reason), rig.start(), what)
            assertEquals(0, rig.reads().size, "$what: nothing was read")
            assertEquals(listOf(*micDisposal, *codecDisposal(), "quit:worker"), rig.ordered(*micDisposal, *codecDisposal(), "quit:worker"), what)
            settled(rig, "activation refused: $what")
        }
    }

    @Test
    fun `a4 a microphone open that fails part way releases its own record, then its codec lease, keeping the failure`() = runTest {
        class Case(val label: String, val script: (Rig) -> Unit, val reason: String, val cleanup: List<String>)
        val boom = IllegalStateException("the platform refused")
        val cases =
            listOf(
                Case("build", { it.port.buildFailure = boom }, "the platform refused", emptyList()),
                Case("state", { it.mic.initialized = false }, "the microphone could not be initialized", listOf("mic:detach", "mic:release")),
                Case("format", { it.mic.format = MicrophoneFormat(44_100, 2, pcm16 = true) }, "the microphone records 44100 Hz, 2 channel(s), 16-bit PCM; the encoder was configured for 48000 Hz, 2 channel(s)", listOf("mic:detach", "mic:release")),
                Case("listen", { it.mic.listenResult = -3 }, "the microphone's position notification could not be set: -3", listOf("mic:detach", "mic:release")),
                Case("start", { it.mic.failAt = "start"; it.mic.failure = boom }, "the platform refused", listOf(*micDisposal)),
                Case("recording", { it.mic.startsRecording = false }, "the microphone did not start recording", listOf(*micDisposal)),
            )
        for (case in cases) {
            val rig = Rig()
            case.script(rig)
            assertEquals(CaptureStart.Refused(case.reason), rig.start(), case.label)
            val after = rig.logNow().drop(rig.logNow().indexOf("mic:build") + 1).filter { it.startsWith("mic:") && !it.startsWith("mic:listen") && it != "mic:start" }
            assertEquals(case.cleanup, after, "${case.label}: every owed release step, once")
            assertEquals(0, rig.timer.count(), "${case.label}: nothing armed")
            assertTrue(rig.before(case.cleanup.lastOrNull() ?: "mic:build", "release-codec:audio"), "${case.label}: the record settles before its codec: ${rig.logNow()}")
            settled(rig, case.label)
        }
        // The release fails too: the fact is stated, and the opening failure leaves the startup as itself.
        val rig = Rig()
        val primary = IllegalStateException("start refused")
        val release = IllegalStateException("release failed")
        rig.mic.failAt = "start"
        rig.mic.failure = primary
        rig.mic.releaseFailure = release
        assertSame(primary, runCatching { rig.start() }.exceptionOrNull(), "never a clean named refusal")
        assertEquals(1, primary.suppressed.count { it === release }, "carrying the release failure once")
        settled(rig, "release failed")
    }

    // -- the codec outlives the microphone ---------------------------------------------------------

    @Test
    fun `a5 a stop while the microphone is opening leaves the codec until the record has stopped and released`() = runTest {
        val rig = Rig()
        val reached = HostGate("the record has started, its open not yet returned")
        val release = HostGate("the row lets the open return")
        rig.mic.onStart = {
            reached.open()
            release.await()
        }
        val opener = HostWorker("opening") { rig.start() }.start()
        var disposedEarly = true
        withSettlement(listOf(release), listOf(opener)) {
            reached.await(opener)
            rig.session.stop(null)
            disposedEarly = "release-codec:audio" in rig.logNow() || "stop:audio" in rig.logNow()
            release.open()
            opener.join()
        }
        assertFalse(disposedEarly, "the codec is not disposed under a microphone that is still opening")
        assertEquals(listOf(*micDisposal, *codecDisposal(), "quit:worker"), rig.ordered(*micDisposal, *codecDisposal(), "quit:worker"))
        assertEquals(0, rig.timer.count(), "a microphone the ending refused is never activated")
        settled(rig, "stop during open")
    }

    @Test
    fun `a6 a stop while a read is in flight leaves the microphone and codec until it returns, and queues nothing after`() = runTest {
        val rig = Rig()
        rig.start()
        rig.input(3)
        rig.mic.timestamp = stamp()
        rig.mic.reads.addLast(unit48)
        val reached = HostGate("the pump is reading")
        val release = HostGate("the row lets the read return")
        rig.mic.holdRead = reached to release
        val pump = HostWorker("worker") { rig.dispatcher.drain() }.start()
        var stoppedEarly = true
        var disposedEarly = true
        var workerAlive = false
        withSettlement(listOf(release), listOf(pump)) {
            reached.await(pump)
            rig.session.stop(null)
            stoppedEarly = "mic:stop" in rig.logNow() || "mic:release" in rig.logNow()
            disposedEarly = "release-codec:audio" in rig.logNow()
            workerAlive = rig.dispatcher.isAlive
            release.open()
            pump.join()
        }
        assertFalse(stoppedEarly, "the record is not released under a read")
        assertFalse(disposedEarly, "and neither is the codec")
        assertTrue(workerAlive, "the worker the read runs on is kept")
        assertEquals(0, rig.logNow().count { it.startsWith("queue ") }, "the unit read across the stop is never queued")
        assertEquals(listOf(*micDisposal, *codecDisposal(), "quit:worker"), rig.ordered(*micDisposal, *codecDisposal(), "quit:worker"))
        settled(rig, "stop during read")
    }

    @Test
    fun `a7 a microphone stop that raises still releases it, then the codec, and the failure is the ending's`() = runTest {
        val rig = Rig()
        val raised = IllegalStateException("the record would not stop")
        rig.start()
        rig.mic.failAt = "stop"
        rig.mic.failure = raised
        rig.session.stop(null)
        assertEquals(listOf(*micDisposal, *codecDisposal(), "quit:worker"), rig.ordered(*micDisposal, *codecDisposal(), "quit:worker"), "every step attempted, in order")
        assertEquals(1, rig.count("release-codec:audio"), "the codec is settled once")
        val receipt = settled(rig, "stop raised")
        assertEquals(1, receipt.cleanup.count { it === raised }, "the stop's failure is the ending's cleanup, once")
    }

    @Test
    fun `a8 a late wake, a wake from a failed open, and an older timer arm do no work`() = runTest {
        // After the ending.
        val ended = Rig()
        ended.start()
        ended.session.stop(null)
        val before = ended.logNow()
        ended.wake()
        ended.timer.fireLatest()
        ended.dispatcher.drain()
        assertEquals(before, ended.logNow(), "no read, timestamp, index, queue or timer after the ending")

        // From a record whose open failed.
        val failed = Rig()
        failed.mic.failAt = "start"
        failed.mic.failure = IllegalStateException("start refused")
        failed.start()
        val afterFailure = failed.logNow()
        failed.wake()
        failed.dispatcher.drain()
        assertEquals(afterFailure, failed.logNow(), "a failed open's listener does no work")

        // An older arm while the microphone is still live.
        val live = Rig()
        live.start()
        live.mic.reads.addLast(100)
        live.dispatcher.drain()
        assertEquals(2, live.timer.count(), "progress re-armed the watchdog")
        val current = live.logNow()
        live.timer.fire(0)
        assertEquals(current, live.logNow(), "the replaced arm does nothing, though the microphone is live")
        live.session.stop(null)
        settled(live, "older arm")
    }

    // -- the pump -----------------------------------------------------------------------------------

    @Test
    fun `b1 a unit read in parts is observed, then queued once, whole, with its key`() = runTest {
        val rig = Rig()
        rig.start()
        rig.mic.reads.addAll(listOf(2000, 0))
        rig.dispatcher.drain()
        assertEquals(listOf("mic:read $unit48", "mic:read ${unit48 - 2000}"), rig.reads(), "each read asks only for what the unit still needs")
        rig.mic.timestamp = stamp()
        rig.mic.reads.addLast(unit48 - 2000)
        rig.input(3)
        val tail = rig.logNow().takeLastWhile { it != "mic:read ${unit48 - 2000}" }
        assertEquals(listOf("mic:timestamp", "observe 0 978667000", "input 3:audio", "queue 3 $unit48 0:audio"), tail.filter { !it.startsWith("timer:") }, "the observation precedes the queue")
        val queued = rig.device().queued.single()
        assertContentEquals(rig.mic.expected(0, unit48), queued.second, "exactly the bytes read, in order")
        rig.session.stop(null)
        settled(rig, "partial reads")
    }

    @Test
    fun `b2 keys carry the remainder at 44 point 1 kHz`() = runTest {
        val rig = Rig(AUDIO_44K)
        rig.start()
        rig.mic.timestamp = stamp(framePosition = 0)
        val unit = AUDIO_44K.maxInputBytes
        for (i in 0 until 3) {
            rig.mic.reads.addLast(unit)
            rig.input(i)
        }
        rig.dispatcher.drain()
        assertEquals(listOf(0L, 23_219L, 46_439L), rig.device().queued.map { it.third }, "floor(i * 1024 * 1e6 / 44100), never a rounded step")
        rig.session.stop(null)
        settled(rig, "44.1 kHz")
    }

    @Test
    fun `b3 a complete unit with no input index is held, and no more is read until one arrives`() = runTest {
        val rig = Rig()
        rig.start()
        rig.mic.timestamp = stamp()
        rig.mic.reads.addLast(unit48)
        rig.dispatcher.drain()
        val reads = rig.reads().size
        rig.wake()
        rig.wake()
        assertEquals(reads, rig.reads().size, "a staged unit stops the reading: nothing is overwritten")
        assertEquals(0, rig.device().queued.size)
        rig.input(7)
        assertEquals(listOf(7), rig.device().queued.map { it.first }, "queued once an index arrives")
        rig.session.stop(null)
        settled(rig, "starvation")
    }

    @Test
    fun `b4 an unavailable timestamp holds the unit, and a monotonic, distant or backwards one is a named fault`() = runTest {
        val held = Rig()
        held.start()
        held.input(1)
        held.mic.reads.addLast(unit48)
        held.dispatcher.drain()
        assertEquals(0, held.device().queued.size, "no capture instant, no queue")
        held.mic.timestamp = stamp()
        held.wake()
        assertEquals(1, held.device().queued.size, "queued once the instant is known")
        held.session.stop(null)
        settled(held, "timestamp later")

        class Case(val label: String, val stamp: AudioTimestampFacts, val reason: String)
        for (case in listOf(
            Case("monotonic", stamp(base = PlatformTimebase.Monotonic), "unit 0 has no capture instant: the audio timestamp is on Monotonic, not the elapsed-realtime base"),
            Case("distant", stamp(framePosition = 96_000), "unit 0 has no capture instant: this frame is 2000000 us from the reported one, beyond the 500000 us this sample will extrapolate"),
        )) {
            val rig = Rig()
            rig.start()
            rig.input(1)
            rig.mic.timestamp = case.stamp
            rig.mic.reads.addLast(unit48)
            rig.dispatcher.drain()
            assertEquals(CaptureTermination.Failed(CaptureFault.Source(MediaKind.Audio, case.reason)), rig.session.termination, case.label)
            assertEquals(0, rig.device().queued.size, case.label)
            settled(rig, case.label)
        }

        val backwards = Rig()
        backwards.start()
        backwards.input(1)
        backwards.input(2)
        backwards.mic.timestamp = stamp(framePosition = 2048)
        backwards.mic.reads.addLast(unit48)
        backwards.dispatcher.drain()
        backwards.mic.timestamp = stamp(framePosition = 1024)
        backwards.mic.reads.addLast(unit48)
        backwards.wake()
        assertEquals(
            CaptureTermination.Failed(CaptureFault.Source(MediaKind.Audio, "the audio timestamp's frame position went backwards: 2048 then 1024")),
            backwards.session.termination,
        )
        assertEquals(1, backwards.device().queued.size, "only the unit before the inconsistency")
        settled(backwards, "backwards")
    }

    @Test
    fun `b5 the watchdog recovers lost wakes, and names no progress and no timestamp`() = runTest {
        // Lost wakes: PCM arrives and nothing wakes the pump; the deadline does, and re-arms.
        val lost = Rig()
        lost.start()
        lost.dispatcher.drain()
        lost.mic.reads.addLast(1000)
        val arms = lost.timer.count()
        lost.timer.fireLatest()
        assertEquals("mic:read ${unit48 - 0}", lost.reads()[1], "the deadline ran one pass")
        assertEquals(arms + 1, lost.timer.count(), "progress re-armed it")
        assertEquals(null, lost.session.termination)
        lost.session.stop(null)
        settled(lost, "lost wakes")

        val stalled = Rig()
        stalled.start()
        stalled.dispatcher.drain()
        stalled.timer.fireLatest()
        assertEquals(CaptureTermination.Failed(CaptureFault.Source(MediaKind.Audio, "no audio capture progress within 1000000 us")), stalled.session.termination)
        settled(stalled, "no progress")

        val untimed = Rig()
        untimed.start()
        untimed.input(1)
        untimed.mic.reads.addLast(unit48)
        untimed.dispatcher.drain()
        untimed.timer.fireLatest()
        assertEquals(CaptureTermination.Failed(CaptureFault.Source(MediaKind.Audio, "no usable audio timestamp within 1000000 us")), untimed.session.termination)
        settled(untimed, "no timestamp")
    }

    @Test
    fun `b6 a capture backlog beyond the buffer is a named uncertainty, and a lagging timestamp is a limitation this does not catch`() = runTest {
        // The predicate itself: framePosition - framesConsumed against the buffer read at acquisition.
        for ((backlog, fails) in listOf(2048L to false, 2049L to true)) {
            val rig = Rig()
            rig.start()
            rig.input(1)
            rig.mic.timestamp = stamp(framePosition = 1024 + backlog)
            rig.mic.reads.addLast(unit48)
            rig.dispatcher.drain()
            if (fails) {
                assertEquals(
                    CaptureTermination.Failed(CaptureFault.Source(MediaKind.Audio, "capture backlog exceeded bound: continuity cannot be established")),
                    rig.session.termination,
                    "backlog $backlog",
                )
            } else {
                assertEquals(null, rig.session.termination, "backlog $backlog is within the bound -- which proves nothing about loss")
                rig.session.stop(null)
            }
            settled(rig, "backlog $backlog")
        }
        // The stated limitation. Two captures that return the product the same
        // things -- each read's count and bytes, each timestamp's values -- and
        // queue the same units; in one of them the platform lost 4096 source
        // frames between the two units, which only the fixture's source ledger
        // records. Neither is faulted: platform-side loss can be invisible to
        // this heuristic.
        fun capture(skip: Long): Rig {
            val rig = Rig()
            rig.start()
            rig.input(1)
            rig.input(2)
            rig.mic.timestamp = stamp(framePosition = 1024)
            rig.mic.reads.addLast(unit48)
            rig.dispatcher.drain()
            rig.mic.skipSourceFrames(skip)
            rig.mic.timestamp = stamp(framePosition = 2048, nanos = 1_021_333_333)
            rig.mic.reads.addLast(unit48)
            rig.wake()
            return rig
        }
        val control = capture(skip = 0)
        val gapped = capture(skip = 4096)
        assertEquals(listOf(0L until 1024L, 1024L until 2048L), control.mic.delivered, "the control delivered contiguous source frames")
        assertEquals(listOf(0L until 1024L, 5120L until 6144L), gapped.mic.delivered, "the platform lost exactly 4096 source frames between the two units")
        assertEquals(
            control.device().queued.map { Triple(it.first, it.second.toList(), it.third) },
            gapped.device().queued.map { Triple(it.first, it.second.toList(), it.third) },
            "yet the product queued the same bytes, under the same keys, into the same indexes",
        )
        assertEquals(control.mic.returned, gapped.mic.returned, "and every read and timestamp returned it the same thing")
        assertEquals(control.logNow(), gapped.logNow(), "and made the same calls")
        for ((label, rig) in listOf("control" to control, "gapped" to gapped)) {
            assertEquals(listOf(1, 2), rig.device().queued.map { it.first }, "$label: both units queued")
            assertEquals(null, rig.session.termination, "$label: no fault -- the documented limitation, not a no-loss proof")
            rig.session.stop(null)
            settled(rig, "lagging timestamp: $label")
        }
    }

    @Test
    fun `b7 a queue that raises is never retried, and is the codec's fault or the ending's cause`() = runTest {
        val ordinary = Rig()
        ordinary.start()
        ordinary.input(3)
        ordinary.mic.timestamp = stamp()
        ordinary.mic.reads.addLast(unit48)
        ordinary.backend.prepare = {}
        ordinary.device().queueFailure = IllegalStateException("the codec rejected the input")
        ordinary.dispatcher.drain()
        assertEquals(
            CaptureTermination.Failed(CaptureFault.Codec(MediaKind.Audio, "the audio input could not be queued: the codec rejected the input")),
            ordinary.session.termination,
        )
        assertEquals(1, ordinary.logNow().count { it.startsWith("queue 3") }, "attempted once, never retried")
        settled(ordinary, "queue raised")

        for (raised in listOf<Throwable>(CancellationException("queue cancelled"), AssertionError("queue raised an Error"))) {
            val rig = Rig()
            rig.start()
            rig.input(3)
            rig.mic.timestamp = stamp()
            rig.mic.reads.addLast(unit48)
            rig.device().queueFailure = raised
            rig.dispatcher.drain()
            assertEquals(CaptureTermination.Failed(CaptureFault.Unexpected(MediaKind.Audio, raised)), rig.session.termination, raised::class.simpleName)
            assertEquals(listOf(raised), settled(rig, "queue raised ${raised::class.simpleName}").causes)
        }
    }

    @Test
    fun `b8 a read error is the microphone's named fault`() = runTest {
        val rig = Rig()
        rig.start()
        rig.mic.reads.addLast(-6)
        rig.dispatcher.drain()
        assertEquals(CaptureTermination.Failed(CaptureFault.Source(MediaKind.Audio, "the microphone read failed: -6")), rig.session.termination)
        settled(rig, "read error")
    }

    @Test
    fun `b9 a complete unit is observed once, before any input index is taken`() = runTest {
        // Observed on the bootstrap pass with no index offered; repeated wakes
        // neither re-read nor re-observe; a later index queues it once.
        val rig = Rig()
        rig.start()
        rig.mic.timestamp = stamp()
        rig.mic.reads.addLast(unit48)
        rig.dispatcher.drain()
        assertEquals(1, rig.logNow().count { it.startsWith("observe ") }, "observed on the pass that completed it")
        val reads = rig.reads().size
        rig.wake()
        rig.wake()
        assertEquals(reads, rig.reads().size, "not re-read")
        assertEquals(1, rig.logNow().count { it.startsWith("observe ") }, "not re-observed")
        assertEquals(null, rig.session.termination, "and nothing is wrong with waiting for an index")
        rig.input(4)
        assertEquals(listOf(4), rig.device().queued.map { it.first }, "queued once the index arrives")
        assertEquals(1, rig.logNow().count { it.startsWith("observe ") }, "without a second observation")
        rig.session.stop(null)
        settled(rig, "observed once")
    }

    @Test
    fun `b10 a refused observation is named on the pass that completed the unit, with no input index taken`() = runTest {
        // A ledger already full: the observation is refused on the bootstrap
        // pass itself, with no codec index ever offered.
        val full = Rig()
        repeat(8) { full.ledger.observe(GENERATION, MediaKind.Audio, CaptureObservation(1_000_000L + it, BASE_NANOS)) }
        full.start()
        full.mic.timestamp = stamp()
        full.mic.reads.addLast(unit48)
        full.dispatcher.drain()
        assertEquals(
            CaptureTermination.Failed(CaptureFault.Correlation(MediaKind.Audio, "the audio ledger refused unit 0: this ledger holds 8 observations")),
            full.session.termination,
            "the refusal is named on that pass, not after some later index",
        )
        assertEquals(0, full.logNow().count { it.startsWith("input ") || it.startsWith("queue ") }, "and no index was touched")
        settled(full, "ledger full")
    }

    // -- the two parts together --------------------------------------------------------------------

    @Test
    fun `c1 camera and audio interleave in every order, and every per-edge order holds`() = runTest {
        for (order in listOf("camera inline", "camera after audio")) {
            val rig = Rig(withCamera = true)
            rig.cam.inline = order == "camera inline"
            val started = rig.start()
            if (order == "camera inline") {
                assertEquals(CaptureStart.Started, started, order)
            } else {
                assertEquals(CaptureStart.Pending, started, order)
                checkNotNull(rig.camPort.events).onOpened(rig.cam)
                checkNotNull(rig.cam.events).onConfigured(rig.cam.stream)
            }
            assertTrue(rig.before("start:video", "create-stream:camera"), "$order E1: the video encoder before the stream")
            assertTrue(rig.before("start:audio", "mic:build"), "$order E2: the audio encoder before the microphone")
            rig.session.stop(null)
            checkNotNull(rig.camPort.events).onClosed(rig.cam)
            assertTrue(rig.before("close:camera", "release-codec:video"), "$order E1: the camera before its encoder")
            assertTrue(rig.before("mic:release", "release-codec:audio"), "$order E3: the microphone before its encoder")
            settled(rig, order)
        }
    }

    @Test
    fun `c2 a stop between the audio acquisition and the camera's settlement, and an audio failure while it is pending`() = runTest {
        val stopped = Rig(withCamera = true)
        assertEquals(CaptureStart.Pending, stopped.start())
        stopped.session.stop(null)
        assertTrue(stopped.before("mic:release", "release-codec:audio"), "E3")
        assertEquals(0, stopped.count("quit:worker"), "E5: the pending camera ticket keeps the worker")
        checkNotNull(stopped.camPort.events).onOpened(stopped.cam)
        assertEquals(1, stopped.count("close:camera"), "E4: the late camera is refused and closed once")
        assertEquals(0, stopped.count("create-stream:camera"))
        settled(stopped, "stopped before the camera settled")

        val failing = Rig(withCamera = true)
        failing.port.buildFailure = IllegalStateException("no microphone")
        assertEquals(CaptureStart.Refused("no microphone"), failing.start())
        checkNotNull(failing.camPort.events).onOpened(failing.cam)
        assertEquals(1, failing.count("close:camera"), "the camera opening meanwhile is closed once")
        settled(failing, "audio failed while the camera was pending")
    }

    // -- the frame clock ----------------------------------------------------------------------------

    @Test
    fun `d1 a frame's elapsed instant shares the frame clock's arithmetic and bound`() {
        assertEquals(FrameElapsed.At(978_667_000L), AudioFrameClock.elapsedNanosOf(stamp(), 0L, 48_000))
        assertEquals(
            FrameElapsed.Unavailable("this frame is 2000000 us from the reported one, beyond the 500000 us this sample will extrapolate"),
            AudioFrameClock.elapsedNanosOf(stamp(framePosition = 96_000), 0L, 48_000),
        )
        assertEquals(
            FrameElapsed.Unavailable("the audio timestamp is on Monotonic, not the elapsed-realtime base"),
            AudioFrameClock.elapsedNanosOf(stamp(base = PlatformTimebase.Monotonic), 0L, 48_000),
        )
    }

    // -- composition --------------------------------------------------------------------------------

    /** Around the real acquirer: records the audio inputs assembly binds, and forwards everything unchanged. */
    private class RecordingAcquirer(private val inner: SequencedAcquirer) : CaptureAcquirer by inner, CaptureFaultRouting, CaptureInputRouting {
        val bound = mutableListOf<Triple<CaptureLedger, AudioKeying, Long>>()

        override fun bindFaults(sink: CaptureFaultSink) = inner.bindFaults(sink)

        override fun bindAudioInputs(ledger: CaptureLedger, keying: AudioKeying, generation: Long) {
            bound.add(Triple(ledger, keying, generation))
            inner.bindAudioInputs(ledger, keying, generation)
        }
    }

    @Test
    fun `e1 the deferred composition binds the audio part to the audio track's own ledger, keying and generation`() {
        val log = mutableListOf<String>()
        val audio = TrackEncoder(AUDIO_48K, HostEncoderBackend(log))
        val video = TrackEncoder(VIDEO, HostEncoderBackend(log))
        val part = AudioPart(audio, MicPort(Mic(log), log), ScriptedTimer(log), QueuedDispatcher(), LateLog())
        val acquirer = RecordingAcquirer(SequencedAcquirer(WorkerStart { error("nothing starts during assembly") }, CameraPart(video, CamPort(Cam(log), log), LateLog()), part))
        val keying = AUDIO_48K.keying(ORIGIN)
        val deferred =
            composeDeferredBroadcast(
                generation = GENERATION,
                permissions = Granted(GENERATION),
                cameraFacts = CaptureCapability.Available,
                audioFacts = CaptureCapability.Available,
                calibration = ClockCalibration(elapsedRealtimeNanos = BASE_NANOS, epoch = org.openmoq.moq5.UnixMicros(EPOCH), uncertaintyMicros = 10L),
                origin = ORIGIN,
                audioKeying = keying,
                capacity = CAPACITY,
                maxOutputBytes = HOST_MAX_OUTPUT_BYTES,
                maxNalUnits = HOST_MAX_NAL_UNITS,
                videoName = VIDEO_CONFIG.name,
                audioName = AUDIO_CONFIG.name,
                formatBounds = FIXTURE_BOUNDS,
                formatBudget = kotlin.time.Duration.parse("PT5S"),
                environment = CaptureEnvironment(acquirer, HostDispatcher(), LateLog(), video.trackCodec, audio.trackCodec),
                tracks = ScriptedTracks(),
                completion = Completion(),
                record = CaptureDiscardRecord(GENERATION),
                writeTimeout = WRITE_TIMEOUT,
                drainTimeout = DRAIN_TIMEOUT,
                videoUtility = ScriptedVideoUtility,
                audioUtility = ScriptedAudioUtility,
            )
        assertEquals(emptyList(), log, "assembly started, built and opened nothing")
        val (ledger, boundKeying, generation) = acquirer.bound.single()
        assertSame(deferred.audio.ledger, ledger, "the audio track's own ledger")
        assertSame(deferred.audio.keying, boundKeying, "the audio track's own keying")
        assertSame(keying, boundKeying, "which is the one supplied")
        assertEquals(GENERATION, generation)
        val second = kotlin.runCatching { part.bindAudioInputs(BoundedCaptureLedger(MediaKind.Audio, GENERATION, 8), keying, GENERATION) }.exceptionOrNull()
        assertIs<IllegalStateException>(second, "and it is bound once")
    }

    @Test
    fun `e2 the preconfigured live composition binds the audio part to the audio track's own ledger, keying and generation`() {
        val log = mutableListOf<String>()
        val audio = TrackEncoder(AUDIO_48K, HostEncoderBackend(log))
        val video = TrackEncoder(VIDEO, HostEncoderBackend(log))
        val part = AudioPart(audio, MicPort(Mic(log), log), ScriptedTimer(log), QueuedDispatcher(), LateLog())
        val acquirer = RecordingAcquirer(SequencedAcquirer(WorkerStart { error("nothing starts during assembly") }, CameraPart(video, CamPort(Cam(log), log), LateLog()), part))
        val keying = UnitAudioKeying(48_000, AudioEncoderConfiguration.FRAMES_PER_UNIT, ORIGIN)
        val idle =
            object : TrackSink {
                override suspend fun write(obj: org.openmoq.moq5.SendObject, timeout: kotlin.time.Duration) = error("nothing is written during assembly")

                override suspend fun endTrack(timeout: kotlin.time.Duration) = error("nothing is ended during assembly")
            }
        val live =
            composeLiveBroadcast(
                generation = GENERATION,
                permissions = Granted(GENERATION),
                cameraFacts = CaptureCapability.Available,
                audioFacts = CaptureCapability.Available,
                calibration = ClockCalibration(elapsedRealtimeNanos = BASE_NANOS, epoch = org.openmoq.moq5.UnixMicros(EPOCH), uncertaintyMicros = 10L),
                origin = ORIGIN,
                audioKeying = keying,
                capacity = CAPACITY,
                maxOutputBytes = HOST_MAX_OUTPUT_BYTES,
                maxNalUnits = HOST_MAX_NAL_UNITS,
                video = validatedVideo(),
                audio = validatedAudio(),
                environment = CaptureEnvironment(acquirer, HostDispatcher(), LateLog(), video.trackCodec, audio.trackCodec),
                sinks = PublishSinks(idle, idle, Completion()),
                record = CaptureDiscardRecord(GENERATION),
                writeTimeout = WRITE_TIMEOUT,
                drainTimeout = DRAIN_TIMEOUT,
            )
        assertEquals(emptyList(), log, "assembly started, built and opened nothing")
        assertEquals(1, acquirer.bound.size, "bound once")
        val (ledger, boundKeying, generation) = acquirer.bound.single()
        assertSame(live.audio.ledger, ledger, "the audio track's own ledger")
        assertSame(keying, boundKeying, "the supplied keying")
        assertSame(live.audio.keying, boundKeying, "which is the audio track's")
        assertEquals(GENERATION, generation)
    }

    private companion object {
        val AUDIO_48K: AudioEncoderConfiguration = (AudioEncoderConfiguration.of(48_000, 2, 96_000, 128_000uL) as EncoderPlan.Planned<AudioEncoderConfiguration>).configuration
        val AUDIO_44K: AudioEncoderConfiguration = (AudioEncoderConfiguration.of(44_100, 1, 64_000, 96_000uL) as EncoderPlan.Planned<AudioEncoderConfiguration>).configuration
        val VIDEO: VideoEncoderConfiguration = (VideoEncoderConfiguration.of(640, 480, 30, 1_000_000, 1_500_000uL, 2) as EncoderPlan.Planned<VideoEncoderConfiguration>).configuration
    }
}
