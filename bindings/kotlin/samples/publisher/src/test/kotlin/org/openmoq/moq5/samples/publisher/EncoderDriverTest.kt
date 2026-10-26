package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import org.openmoq.moq5.UnixMicros

/**
 * The encoder driver: requested settings, the codec's lifetime, and its
 * platform callbacks admitted by the session's one callback authority.
 *
 * The platform encoder is a scripted [EncoderDevice] that logs every call in
 * order and delivers callbacks when a row tells it to -- inline, or from a
 * [HostWorker] standing in for the owned callback worker. Everything else is
 * real: the session, its ownership, both handoffs and ledgers, the format
 * holders, and [TrackEncoder]. `MediaCodecEncoder.kt` uses this same path and
 * is compiled against the SDK; these rows cannot execute it.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class EncoderDriverTest {

    /**
     * The platform steps [AllocatingBackend] sequences, scripted for the video
     * encoder: each logged as `step:Kind`, the native codec a [HostResource],
     * and the transfer handing over to the scripted [HostEncoderDevice] [devices] creates.
     */
    private class Allocation(private val log: MutableList<String>, private val devices: HostEncoderBackend) : EncoderAllocationPort<HostResource> {
        @Volatile var allocateFailure: Throwable? = null

        /** The video codec reports no surface input, so the configuration is refused by name. */
        @Volatile var refuse = false

        @Volatile var queryFailure: Throwable? = null

        @Volatile var releaseFailure: Throwable? = null

        private fun step(what: String, kind: MediaKind, failure: Throwable?) {
            synchronized(log) { log.add("$what:$kind") }
            if (kind == MediaKind.Video) failure?.let { throw it }
        }

        override fun allocate(configuration: EncoderConfiguration): HostResource {
            step("allocate", configuration.kind, allocateFailure)
            return HostResource("native-${configuration.kind}")
        }

        override fun capabilities(codec: HostResource, configuration: EncoderConfiguration): EncoderCapabilities {
            step("capabilities", configuration.kind, queryFailure)
            val supported = !(refuse && configuration.kind == MediaKind.Video)
            return object : EncoderCapabilities {
                override fun sizeAndRateSupported(width: Int, height: Int, frameRate: Int) = true

                override fun bitrateSupported(bitrate: Int) = true

                override val surfaceInput: Boolean = supported

                override fun sampleRateSupported(sampleRate: Int) = true

                override val maxInputChannels: Int = 2

                override val aacLowComplexity: Boolean = true
            }
        }

        override fun release(codec: HostResource) = step("release-native", if (codec.name.endsWith("Video")) MediaKind.Video else MediaKind.Audio, releaseFailure)

        override fun device(codec: HostResource, configuration: EncoderConfiguration): EncoderDevice = devices.create(configuration)
    }

    /** One output format, counted: every read in order. */
    private class Fields(private val values: Map<String, Any?>) : FormatFields {
        private val log = mutableListOf<String>()

        fun reads(): List<String> = synchronized(log) { log.toList() }

        private fun read(what: String) = synchronized(log) { log.add(what) }

        override fun containsKey(key: String): Boolean = values.containsKey(key).also { read("has:$key") }

        override fun getInteger(key: String): Int = checkNotNull(values[key] as Int?).also { read("int:$key") }

        override fun getString(key: String): String? = (values[key] as String?).also { read("string:$key") }

        override fun getByteBuffer(key: String): ByteBuffer? = (values[key] as ByteBuffer?)?.duplicate().also { read("buffer:$key") }
    }

    private fun videoFields() =
        Fields(
            mapOf(
                FormatKeys.MIME to FormatKeys.VIDEO_AVC,
                FormatKeys.WIDTH to 640,
                FormatKeys.HEIGHT to 480,
                FormatKeys.BIT_RATE to 1_000_000,
                FormatKeys.CSD_0 to ByteBuffer.wrap(byteArrayOf(0, 0, 0, 1, 0x67, 0x42, 0x00, 0x1e, 0xab.toByte(), 0xcd.toByte())),
                FormatKeys.CSD_1 to ByteBuffer.wrap(byteArrayOf(0, 0, 0, 1, 0x68, 0xce.toByte(), 0x3c, 0x80.toByte())),
            ),
        )

    private fun audioFields() =
        Fields(
            mapOf(
                FormatKeys.MIME to FormatKeys.AUDIO_AAC,
                FormatKeys.SAMPLE_RATE to 48_000,
                FormatKeys.CHANNEL_COUNT to 2,
                FormatKeys.BIT_RATE to 96_000,
                FormatKeys.CSD_0 to ByteBuffer.wrap(FIXTURE_ASC),
            ),
        )

    /**
     * The real session over two [TrackEncoder]s, bound the way the deferred
     * composition binds them: each encoder is its track's lifetime, buffer
     * access and index owner, and its callbacks are routed to that track's
     * handoff and format holder under a key the session minted. The driver
     * opens both encoders through the ownership, video first.
     */
    private class EncoderRig(inputCapacity: Int = 4, allocating: Boolean = false) {
        val log = mutableListOf<String>()
        val backend = HostEncoderBackend(log)

        /** When [allocating], the encoders are created through the platform backend's own control flow over this port. */
        val allocation = Allocation(log, backend)
        private val encoders: EncoderBackend = if (allocating) AllocatingBackend(allocation) else backend
        val video = TrackEncoder(VIDEO, encoders)
        val audio = TrackEncoder(AUDIO, encoders, inputCapacity)
        val forward = Forward()
        val videoLedger = BoundedCaptureLedger(MediaKind.Video, GENERATION, capacity = 8)
        val audioLedger = BoundedCaptureLedger(MediaKind.Audio, GENERATION, capacity = 8)
        val videoInner = BoundedCaptureHandoff(MediaKind.Video, GENERATION, video, video, videoLedger, forward, CAPACITY, HOST_MAX_OUTPUT_BYTES, lifetime = video)
        val audioInner = BoundedCaptureHandoff(MediaKind.Audio, GENERATION, audio, audio, audioLedger, forward, CAPACITY, HOST_MAX_OUTPUT_BYTES, lifetime = audio)
        val videoHandoff = EntryObserving(videoInner)
        val audioHandoff = EntryObserving(audioInner)
        val late = LateLog()
        val dispatcher = HostDispatcher()
        val ownership = OrderedCaptureOwnership(GENERATION, dispatcher, late)

        /** The owned callback worker: opened first, and quitting it really quits the dispatcher. */
        private val worker =
            object : ResourceHandle {
                override val identity: Any = HostResource("worker")

                override fun close() {
                    synchronized(log) { log.add("quit:worker") }
                    dispatcher.quit()
                }
            }
        val acquirer =
            object : CaptureAcquirer {
                override val order = listOf(CaptureResource.Worker, CaptureResource.VideoEncoder, CaptureResource.AudioEncoder)

                override fun begin(ownership: CaptureOwnership, steps: AcquisitionSteps): AcquireOutcome.Failed? {
                    val installed = ownership.acquire(CaptureResource.Worker) { worker.also { synchronized(log) { log.add("open:worker") } } }
                    if (installed is AcquireOutcome.Failed) return installed
                    for ((resource, encoder) in listOf(CaptureResource.VideoEncoder to video, CaptureResource.AudioEncoder to audio)) {
                        val outcome = ownership.acquire(resource) { encoder.open(ownership) }
                        if (outcome is AcquireOutcome.Failed) return outcome
                    }
                    return null
                }
            }
        val listener = HostSessionListener()
        val session = PlatformCaptureSession(GENERATION, ownership, acquirer, videoHandoff, audioHandoff, listener)
        val holders = FormatHolders(session, VIDEO_CONFIG.name, AUDIO_CONFIG.name, FIXTURE_BOUNDS, ScriptedVideoUtility, ScriptedAudioUtility)

        init {
            forward.to = session
            video.bind(EncoderRoute(session, session.bindCodecCallbacks(MediaKind.Video), GENERATION, videoHandoff) { f, m -> holders.video.onOutputFormat(f, m) })
            audio.bind(EncoderRoute(session, session.bindCodecCallbacks(MediaKind.Audio), GENERATION, audioHandoff) { f, m -> holders.audio.onOutputFormat(f, m) })
        }

        fun start(): CaptureStart = session.start(Granted(GENERATION), CaptureCapability.Available, CaptureCapability.Available)

        fun device(kind: MediaKind) = backend.device(kind)

        fun callbacks(kind: MediaKind): EncoderCallbacks = checkNotNull(device(kind).registered)

        fun logNow(): List<String> = synchronized(log) { log.toList() }

        /** How many times the worker was quit; one is the only correct answer once everything has settled. */
        fun quits(): Int = logNow().count { it == "quit:worker" }

        /** Whether [first] was logged, and before [then]. */
        fun before(first: String, then: String): Boolean {
            val steps = logNow()
            return first in steps && then in steps && steps.indexOf(first) < steps.indexOf(then)
        }

        /** One completed video access unit: observed on its own ledger, its bytes in the codec's buffer at [index]. */
        fun videoUnit(index: Int, bytes: ByteArray, pts: Long) {
            synchronized(device(MediaKind.Video).buffers) { device(MediaKind.Video).buffers[index] = ByteBuffer.wrap(bytes).asReadOnlyBuffer() }
            assertEquals(ObserveOutcome.Recorded, videoLedger.observe(GENERATION, MediaKind.Video, CaptureObservation(pts, BASE_NANOS + pts * 1_000L)))
        }

        fun audioUnit(index: Int, bytes: ByteArray, pts: Long) {
            synchronized(device(MediaKind.Audio).buffers) { device(MediaKind.Audio).buffers[index] = ByteBuffer.wrap(bytes) }
            assertEquals(ObserveOutcome.Recorded, audioLedger.observe(GENERATION, MediaKind.Audio, CaptureObservation(pts, BASE_NANOS + pts * 1_000L)))
        }
    }

    /** A started codec's disposal steps, in order: stop, release, then the surface it created. */
    private fun disposal(kind: MediaKind, surface: Boolean): List<String> {
        val name = kind.name.lowercase()
        return listOfNotNull("stop:$name", "release-codec:$name", if (surface) "surface-release:$name" else null)
    }

    private fun <C : EncoderConfiguration> planned(plan: EncoderPlan<C>): C =
        when (plan) {
            is EncoderPlan.Planned -> plan.configuration
            is EncoderPlan.Refused -> throw AssertionError("refused: ${plan.reason}")
        }

    // -- settings ---------------------------------------------------------------------------------

    @Test
    fun `e1 requested settings are planned exactly or refused by name before any platform call`() {
        val video = planned(VideoEncoderConfiguration.of(1280, 720, 30, 2_000_000, 2_500_000uL, 2))
        assertEquals(listOf(MediaKind.Video, FormatKeys.VIDEO_AVC, 1280, 720, 30, 2_000_000, 2), listOf(video.kind, video.mime, video.width, video.height, video.frameRate, video.bitrate, video.keyFrameIntervalSeconds))
        assertEquals(2_500_000uL, video.maximumBitrate)
        assertTrue(video.surfaceInput)
        assertEquals(EncoderPlan.Refused("the video size 1281x720 is not even in both dimensions"), VideoEncoderConfiguration.of(1281, 720, 30, 2_000_000, 2_500_000uL, 2))
        assertEquals(EncoderPlan.Refused("the video size 0x720 is not positive"), VideoEncoderConfiguration.of(0, 720, 30, 2_000_000, 2_500_000uL, 2))
        assertEquals(EncoderPlan.Refused("the video frame rate 0 is not positive"), VideoEncoderConfiguration.of(1280, 720, 0, 2_000_000, 2_500_000uL, 2))
        assertEquals(EncoderPlan.Refused("the key-frame interval of 0 s is not positive"), VideoEncoderConfiguration.of(1280, 720, 30, 2_000_000, 2_500_000uL, 0))
        assertEquals(EncoderPlan.Refused("video: the requested bitrate 3000000 exceeds the declared maximum 2500000"), VideoEncoderConfiguration.of(1280, 720, 30, 3_000_000, 2_500_000uL, 2))
        assertEquals(EncoderPlan.Refused("video: the requested bitrate 0 is not positive"), VideoEncoderConfiguration.of(1280, 720, 30, 0, 2_500_000uL, 2))

        val audio = planned(AudioEncoderConfiguration.of(44_100, 1, 64_000, 64_000uL))
        assertEquals(listOf(MediaKind.Audio, FormatKeys.AUDIO_AAC, 44_100, 1, 64_000), listOf(audio.kind, audio.mime, audio.sampleRate, audio.channelCount, audio.bitrate))
        assertFalse(audio.surfaceInput)
        assertEquals(1024 * 1 * 2, audio.maxInputBytes, "one AAC-LC unit of 16-bit mono PCM")
        assertEquals(EncoderPlan.Refused("the audio sample rate 44000 Hz has no AAC sampling-frequency index"), AudioEncoderConfiguration.of(44_000, 2, 64_000, 64_000uL))
        assertEquals(EncoderPlan.Refused("the audio channel count 3 is neither mono nor stereo"), AudioEncoderConfiguration.of(48_000, 3, 64_000, 64_000uL))
        assertEquals(EncoderPlan.Refused("audio: the requested bitrate 64001 exceeds the declared maximum 64000"), AudioEncoderConfiguration.of(48_000, 2, 64_001, 64_000uL))

        // The keying is this PCM geometry's, and a keying of any other geometry is refused by name.
        val keying = audio.keying(originUs = 7L)
        assertEquals(listOf(44_100, AudioEncoderConfiguration.FRAMES_PER_UNIT, 7L), listOf(keying.sampleRate, keying.framesPerUnit, keying.originUs))
        assertNull(audio.keyingDisagreement(keying))
        assertEquals("the audio keying's sample rate 48000 is not the configured PCM rate 44100", audio.keyingDisagreement(UnitAudioKeying(48_000, 1024, 7L)))
        assertEquals("the audio keying's unit of 960 frames is not the configured AAC-LC unit of 1024", audio.keyingDisagreement(UnitAudioKeying(44_100, 960, 7L)))
    }

    @Test
    fun `e2 an encoder that does not support the configuration is refused by name`() {
        class Caps(
            val size: Boolean = true,
            val bitrate: Boolean = true,
            override val surfaceInput: Boolean = true,
            val rate: Boolean = true,
            override val maxInputChannels: Int = 2,
            override val aacLowComplexity: Boolean = true,
        ) : EncoderCapabilities {
            override fun sizeAndRateSupported(width: Int, height: Int, frameRate: Int) = size

            override fun bitrateSupported(bitrate: Int) = this.bitrate

            override fun sampleRateSupported(sampleRate: Int) = rate
        }
        assertNull(EncoderCapabilityCheck.refusal(VIDEO, Caps()))
        assertEquals("the AVC encoder does not accept surface input", EncoderCapabilityCheck.refusal(VIDEO, Caps(surfaceInput = false)))
        assertEquals("the AVC encoder does not support 640x480 at 30 fps", EncoderCapabilityCheck.refusal(VIDEO, Caps(size = false)))
        assertEquals("the AVC encoder does not support 1000000 bit/s", EncoderCapabilityCheck.refusal(VIDEO, Caps(bitrate = false)))
        assertNull(EncoderCapabilityCheck.refusal(AUDIO, Caps(surfaceInput = false)))
        assertEquals("the AAC encoder does not list the Low Complexity profile", EncoderCapabilityCheck.refusal(AUDIO, Caps(aacLowComplexity = false)))
        assertEquals("the AAC encoder does not support 48000 Hz", EncoderCapabilityCheck.refusal(AUDIO, Caps(rate = false)))
        assertEquals("the AAC encoder takes at most 1 channel(s), not 2", EncoderCapabilityCheck.refusal(AUDIO, Caps(maxInputChannels = 1)))
        assertEquals("the AAC encoder does not support 96000 bit/s", EncoderCapabilityCheck.refusal(AUDIO, Caps(bitrate = false)))

        // Refused where it is created: nothing is registered, configured or started, and the acquisition fails by that name.
        val rig = EncoderRig()
        rig.backend.refuse = "no encoder for this size"
        assertEquals(CaptureStart.Refused("the Video encoder cannot be configured: no encoder for this size"), rig.start())
        assertEquals(listOf("open:worker", "create:Video", "quit:worker"), rig.logNow(), "nothing else was created, and the worker quit once")
    }

    // -- opening ----------------------------------------------------------------------------------

    @Test
    fun `e3 opening creates, registers callbacks, configures, creates the surface and starts, in that order, with the exact configuration`() {
        val rig = EncoderRig()
        assertEquals(CaptureStart.Started, rig.start())
        assertEquals(
            listOf(
                "open:worker",
                "create:Video", "callbacks:video", "configure:video", "surface:video", "start:video",
                "create:Audio", "callbacks:audio", "configure:audio", "start:audio",
            ),
            rig.logNow(),
            "callbacks are registered before configure and start; only the surface role creates a surface",
        )
        assertSame(VIDEO, rig.device(MediaKind.Video).configured)
        assertSame(AUDIO, rig.device(MediaKind.Audio).configured)
        assertSame(rig.device(MediaKind.Video).surface, rig.video.inputSurface)
        assertNull(rig.audio.inputSurface)
    }

    @Test
    fun `e4 an encoder whose callbacks are not bound is refused before any platform call`() {
        val log = mutableListOf<String>()
        val unbound = TrackEncoder(VIDEO, HostEncoderBackend(log))
        val failure = assertFailsWith<IllegalStateException> { unbound.open(OrderedCaptureOwnership(GENERATION, HostDispatcher(), LateLog())) }
        assertEquals("the Video encoder's callbacks are not bound to a session; bind them before opening", failure.message)
        assertEquals(emptyList(), log, "nothing was created")
        assertFailsWith<IllegalArgumentException>("a format key is not a codec key") {
            unbound.bind(EncoderRoute(Rig().codecGate, Rig().gate.bindFormatHolder(MediaKind.Video), GENERATION, Lane(MediaKind.Video, Forward()).handoff) { _, _ -> })
        }
    }

    @Test
    fun `e5 a configure failure unwinds through the same lifetime, keeping the failure and what the release raised`() {
        val rig = EncoderRig()
        val configureFailure = IllegalStateException("configure refused the format")
        val releaseFailure = IllegalStateException("release failed too")
        rig.backend.prepare = { device ->
            if (device.name == "video") {
                device.failAt = "configure"
                device.failure = configureFailure
            }
        }
        val raised = assertFailsWith<IllegalStateException> { rig.video.open(rig.ownership) }
        assertSame(configureFailure, raised, "the configure failure leaves as itself")
        assertEquals(listOf("create:Video", "callbacks:video", "configure:video", "release-codec:video"), rig.logNow(), "never started, so released without a stop")

        // A start failure whose release also raises: every later step is still attempted, and the failed disposal is stated explicitly.
        val second = EncoderRig()
        val startFailure = IllegalStateException("start failed")
        second.backend.prepare = { device ->
            device.failAt = "start"
            device.failure = startFailure
            device.releaseFailure = releaseFailure
        }
        val unproved = assertFailsWith<UnprovedDisposal> { second.video.open(second.ownership) }
        assertSame(startFailure, unproved.primary, "the opening failure, by identity")
        assertEquals(listOf<Throwable>(releaseFailure), unproved.failures, "and the one disposal step that failed")
        assertEquals(emptyList(), startFailure.suppressed.toList(), "nothing is attached until the ownership unwraps it")
        assertEquals(
            listOf("callbacks:video", "configure:video", "surface:video", "start:video", "release-codec:video", "surface-release:video"),
            second.device(MediaKind.Video).steps(),
            "never started, so no stop; the surface is released after the release that raised",
        )
    }

    @Test
    fun `e6 a start failure while a callback delivered during start holds the codec defers the disposal to that transfer`() {
        val rig = EncoderRig()
        val reached = HostGate("the start-time transfer is fetching")
        val release = HostGate("the row lets the start-time transfer go")
        val startFailure = IllegalStateException("start failed")
        val payload = byteArrayOf(0, 0, 0, 1, 0x65, 9, 9)
        val delivered = AtomicReference<EncoderCallbacks?>()
        val worker = HostWorker("codec-callback") { checkNotNull(delivered.get()).onOutputAvailable(3, 0, payload.size, 10L, CaptureContract.FLAG_KEY_FRAME) }
        rig.backend.prepare = { device ->
            if (device.name == "video") {
                device.holdFetch = Triple(3, reached, release)
                device.duringStart = { callbacks ->
                    delivered.set(callbacks)
                    worker.start()
                    reached.await(worker)
                    throw startFailure
                }
            }
        }
        withSettlement(listOf(release), listOf(worker)) {
            rig.videoLedger.observe(GENERATION, MediaKind.Video, CaptureObservation(10L, BASE_NANOS))
            // The device is created inside open; its buffer is installed from the start hook's own thread before the fetch returns.
            val raised = assertFailsWith<IllegalStateException> { rig.video.open(rig.ownership) }
            assertSame(startFailure, raised)
            assertFalse("release-codec:video" in rig.logNow(), "the codec is not disposed under the transfer that holds it")
            synchronized(rig.device(MediaKind.Video).buffers) { rig.device(MediaKind.Video).buffers[3] = ByteBuffer.wrap(payload) }
            release.open()
            worker.join()
        }
        val steps = rig.device(MediaKind.Video).steps()
        assertEquals(listOf("callbacks:video", "configure:video", "surface:video", "start:video", "get 3:video", "release 3:video", "release-codec:video", "surface-release:video"), steps, "the index goes back, then the codec that was never started is released, then its surface")
    }

    // -- routing ----------------------------------------------------------------------------------

    @Test
    fun `e7 an output callback reaches its own track's handoff with its exact index and BufferInfo values, and goes back to the same codec once`() {
        val rig = EncoderRig()
        assertEquals(CaptureStart.Started, rig.start())
        val bytes = byteArrayOf(0x55, 0x55, 0, 0, 0, 1, 0x65, 1, 2, 3, 0x55)
        rig.videoUnit(index = 5, bytes = bytes, pts = 33_333L)
        rig.callbacks(MediaKind.Video).onOutputAvailable(5, 2, 8, 33_333L, CaptureContract.FLAG_KEY_FRAME)

        val out = assertNotNull(rig.videoHandoff.next(), "the video output reached the video handoff")
        assertContentEquals(bytes.copyOfRange(2, 10), out.output.buffer.copyOfRange(out.output.offset, out.output.offset + out.output.size), "exactly [offset, offset + size)")
        assertEquals(33_333L, out.output.presentationTimeUs)
        assertEquals(CaptureContract.FLAG_KEY_FRAME, out.output.flags)
        assertEquals(BASE_NANOS + 33_333L * 1_000L, out.captureElapsedNanos, "the capture instant the ledger claimed for that timestamp")
        assertNull(rig.audioHandoff.next(), "and nothing reached the audio track")
        assertEquals(listOf("get 5:video", "release 5:video"), rig.device(MediaKind.Video).steps().drop(4), "fetched and released once, on the codec that named it")
        assertEquals(listOf("callbacks:audio", "configure:audio", "start:audio"), rig.device(MediaKind.Audio).steps(), "the other codec is untouched")

        // The audio codec's output, the same way, to its own track.
        rig.audioUnit(index = 2, bytes = byteArrayOf(0x21, 0x22, 0x23), pts = 0L)
        rig.callbacks(MediaKind.Audio).onOutputAvailable(2, 0, 3, 0L, 0)
        assertContentEquals(byteArrayOf(0x21, 0x22, 0x23), assertNotNull(rig.audioHandoff.next()).output.buffer)
        assertEquals(listOf("get 2:audio", "release 2:audio"), rig.device(MediaKind.Audio).steps().drop(3))
        assertEquals(CallbackInventory(emptySet(), emptyList(), null), rig.session.callbackInventory(), "every callback released its reservation")
    }

    @Test
    fun `e8 an output format is read inside the holder's admission with the declared maximum, and a callback during start is admitted like any other`() = runTest {
        val rig = EncoderRig()
        rig.backend.prepare = { device ->
            // Delivered while the codec starts: the encoder is already installed, so the format reaches its holder.
            if (device.name == "video") device.duringStart = { callbacks -> callbacks.onFormatChanged(videoFields()) }
        }
        assertEquals(CaptureStart.Started, rig.start())
        assertTrue(rig.holders.video.format.isCompleted, "the format delivered during start resolved the video slot")
        assertEquals(VIDEO_FORMAT_CONFIG.copy(bitrate = VIDEO.maximumBitrate), rig.holders.video.format.await().config, "the declared maximum, not the reported target")

        rig.callbacks(MediaKind.Audio).onFormatChanged(audioFields())
        assertEquals(AUDIO_FORMAT_CONFIG.copy(bitrate = AUDIO.maximumBitrate), rig.holders.audio.format.await().config)
        assertEquals(CallbackInventory(emptySet(), emptyList(), null), rig.session.callbackInventory())
    }

    @Test
    fun `e9 PCM input indices are held for the input driver, bounded, and a surface encoder offering one is a codec fault`() {
        val rig = EncoderRig(inputCapacity = 2)
        assertEquals(CaptureStart.Started, rig.start())
        rig.callbacks(MediaKind.Audio).onInputAvailable(4)
        rig.callbacks(MediaKind.Audio).onInputAvailable(6)
        assertEquals(listOf(4, 6, null), listOf(rig.audio.takeInput(), rig.audio.takeInput(), rig.audio.takeInput()))
        assertNull(rig.session.termination)

        rig.callbacks(MediaKind.Video).onInputAvailable(1)
        assertEquals(CaptureTermination.Failed(CaptureFault.Codec(MediaKind.Video, "the surface-input Video encoder offered input buffer 1")), rig.session.termination)

        val full = EncoderRig(inputCapacity = 1)
        assertEquals(CaptureStart.Started, full.start())
        full.callbacks(MediaKind.Audio).onInputAvailable(0)
        full.callbacks(MediaKind.Audio).onInputAvailable(1)
        assertEquals(CaptureTermination.Failed(CaptureFault.Codec(MediaKind.Audio, "the Audio encoder's input queue already holds 1 indices")), full.session.termination)
        full.callbacks(MediaKind.Audio).onInputAvailable(2)
        assertEquals(listOf(0, null), listOf(full.audio.takeInput(), full.audio.takeInput()), "nothing is held once the session has ended")
    }

    @Test
    fun `e10 a codec error is the track's codec fault through the real session`() = runTest {
        val rig = EncoderRig()
        assertEquals(CaptureStart.Started, rig.start())
        rig.callbacks(MediaKind.Audio).onError("the AAC encoder hit an unrecoverable error")
        assertEquals(CaptureTermination.Failed(CaptureFault.Codec(MediaKind.Audio, "the AAC encoder hit an unrecoverable error")), rig.session.termination)
        val receipt = rig.session.retirement()
        assertEquals(emptyList(), receipt.causes, "a typed codec fault is not an unexpected cause")
        assertEquals(disposal(MediaKind.Audio, surface = false), rig.device(MediaKind.Audio).steps().takeLast(2))
        assertEquals(disposal(MediaKind.Video, surface = true), rig.device(MediaKind.Video).steps().takeLast(3))
    }

    // -- the lifetime against a transfer in flight --------------------------------------------------

    @Test
    fun `e11 a transfer holding the codec defers its disposal past a stop, and the last lease disposes it after the index goes back`() = runTest {
        val rig = EncoderRig()
        val reached = HostGate("the video transfer is fetching")
        val release = HostGate("the row lets the video transfer go")
        rig.backend.prepare = { device -> if (device.name == "video") device.holdFetch = Triple(7, reached, release) }
        assertEquals(CaptureStart.Started, rig.start())
        rig.videoUnit(index = 7, bytes = byteArrayOf(0, 0, 0, 1, 0x65, 1), pts = 1L)
        val callbacks = rig.callbacks(MediaKind.Video)
        val worker = HostWorker("codec-callback") { callbacks.onOutputAvailable(7, 0, 6, 1L, CaptureContract.FLAG_KEY_FRAME) }.start()
        var frozenWhileHeld = true
        withSettlement(listOf(release), listOf(worker)) {
            reached.await(worker)
            rig.session.stop(null)
            assertFalse("release-codec:video" in rig.logNow(), "the stop asked for the disposal; the transfer still holds the codec")
            assertEquals(disposal(MediaKind.Audio, surface = false), rig.device(MediaKind.Audio).steps().takeLast(2), "the idle codec was disposed at once")
            val receipt = async { rig.session.retirement() }
            runCurrent()
            frozenWhileHeld = receipt.isCompleted
            release.open()
            worker.join()
        }
        assertFalse(frozenWhileHeld, "the ending did not retire while the codec callback held the codec")
        assertEquals(listOf("get 7:video", "release 7:video") + disposal(MediaKind.Video, surface = true), rig.device(MediaKind.Video).steps().drop(4))
        val receipt = rig.session.retirement()
        assertEquals(CaptureTermination.Completed, receipt.termination)
        assertEquals(emptyList(), receipt.causes)
    }

    @Test
    fun `e12 an output collaborator's Exception, Error or cancellation after a stop claim is recorded once, and the receipt waits for it`() = runTest {
        for (raised in listOf<Throwable>(IllegalStateException("the output signal failed"), AssertionError("the output signal raised an Error"), CancellationException("cancelled in the output signal"))) {
            val label = raised::class.simpleName
            val rig = EncoderRig()
            assertEquals(CaptureStart.Started, rig.start())
            val entered = HostGate("$label: the video callback is notifying")
            val release = HostGate("$label: the row lets the notification fail")
            val callbackThread = AtomicReference<Thread?>()
            rig.videoHandoff.signal {
                // Only the delivering callback's own notification is held and fails; the close's is not.
                if (Thread.currentThread() === callbackThread.get()) {
                    entered.open()
                    release.await()
                    throw raised
                }
            }
            rig.videoUnit(index = 1, bytes = byteArrayOf(0, 0, 0, 1, 0x65, 7), pts = 5L)
            val callbacks = rig.callbacks(MediaKind.Video)
            val worker = HostWorker("codec-callback") {
                callbackThread.set(Thread.currentThread())
                callbacks.onOutputAvailable(1, 0, 6, 5L, CaptureContract.FLAG_KEY_FRAME)
            }.start()
            var frozenWhileHeld = true
            withSettlement(listOf(release), listOf(worker)) {
                entered.await(worker)
                rig.session.stop(null)
                val receipt = async { rig.session.retirement() }
                runCurrent()
                frozenWhileHeld = receipt.isCompleted
                release.open()
                // The callback RETURNS: a join that rethrew would be the worker lost to what it raised.
                worker.join()
            }
            assertFalse(frozenWhileHeld, "$label: the receipt waited for the codec callback in flight")
            val receipt = rig.session.retirement()
            assertEquals(CaptureTermination.Completed, receipt.termination, "$label: the stop's ending stands")
            assertEquals(listOf(raised), receipt.causes, "$label: the original, exactly once")
            assertTrue(receipt.cleanup.none { it === raised }, "$label: and not again as cleanup")
            assertEquals(disposal(MediaKind.Video, surface = true), rig.device(MediaKind.Video).steps().takeLast(3), "$label: the codec was disposed once the transfer returned its lease")
        }
    }

    @Test
    fun `e13 a first unexpected cause claims the ending, and the claimant's cleanup failure stays the receipt's without reaching the worker`() = runTest {
        val rig = EncoderRig()
        val releaseError = AssertionError("the video codec's release raised an Error")
        rig.backend.prepare = { device ->
            if (device.name == "video") {
                device.failAt = "release-codec"
                device.failure = releaseError
            }
        }
        assertEquals(CaptureStart.Started, rig.start())
        val cause = IllegalStateException("the audio output signal failed")
        val callbackThread = AtomicReference<Thread?>()
        val raisedOnce = java.util.concurrent.atomic.AtomicBoolean(false)
        // The delivering callback's own notification fails, once. That callback is
        // also the claimant, and its close of this track notifies again, cleanly.
        rig.audioHandoff.signal { if (Thread.currentThread() === callbackThread.get() && raisedOnce.compareAndSet(false, true)) throw cause }
        rig.audioUnit(index = 0, bytes = byteArrayOf(1, 2, 3), pts = 0L)
        val callbacks = rig.callbacks(MediaKind.Audio)
        val worker = HostWorker("codec-callback") {
            callbackThread.set(Thread.currentThread())
            callbacks.onOutputAvailable(0, 0, 3, 0L, 0)
        }.start()
        // The callback RETURNS: a join that rethrew would be the worker lost to the receipt's own cleanup failure.
        worker.join()
        val receipt = rig.session.retirement()
        assertEquals(CaptureTermination.Failed(CaptureFault.Unexpected(MediaKind.Audio, cause)), receipt.termination)
        assertEquals(listOf<Throwable>(cause), receipt.causes)
        assertEquals(1, receipt.cleanup.count { it === releaseError }, "the disposal's Error is the ending's cleanup, once")
        assertTrue(receipt.cleanup.none { it === cause }, "and the cause is not cleanup")
    }

    @Test
    fun `e13b a cause raised again by the claimant's own close is once a cause and once cleanup, and the worker survives`() = runTest {
        val rig = EncoderRig()
        assertEquals(CaptureStart.Started, rig.start())
        val cause = IllegalStateException("the audio output signal keeps failing")
        val callbackThread = AtomicReference<Thread?>()
        // Every notification on the delivering callback's thread fails. That
        // callback is also the claimant, so its own close of this track raises
        // the same instance a second time.
        rig.audioHandoff.signal { if (Thread.currentThread() === callbackThread.get()) throw cause }
        rig.audioUnit(index = 0, bytes = byteArrayOf(1, 2, 3), pts = 0L)
        val callbacks = rig.callbacks(MediaKind.Audio)
        val worker = HostWorker("codec-callback") {
            callbackThread.set(Thread.currentThread())
            callbacks.onOutputAvailable(0, 0, 3, 0L, 0)
        }.start()
        worker.join()
        val receipt = rig.session.retirement()
        assertEquals(CaptureTermination.Failed(CaptureFault.Unexpected(MediaKind.Audio, cause)), receipt.termination)
        assertEquals(listOf<Throwable>(cause), receipt.causes, "the callback's raise, once, as a cause")
        assertEquals(1, receipt.cleanup.count { it === cause }, "the close's raise, once, as cleanup")
    }

    // -- refusals ---------------------------------------------------------------------------------

    @Test
    fun `e14 a callback refused as Ended while the handoff is still open fetches, releases and hands off nothing`() {
        val rig = EncoderRig()
        assertEquals(CaptureStart.Started, rig.start())
        val closing = HostGate("the claimant reached the video close")
        val proceed = HostGate("the row lets the claimant close the video track")
        rig.videoHandoff.beforeClose = {
            closing.open()
            proceed.await()
        }
        rig.videoUnit(index = 9, bytes = byteArrayOf(0, 0, 0, 1, 0x65, 3), pts = 2L)
        val stopper = HostWorker("stop") { rig.session.stop(null) }.start()
        val fields = videoFields()
        withSettlement(listOf(proceed), listOf(stopper)) {
            closing.await(stopper)
            assertFalse(rig.videoInner.ended, "the handoff is still open: the claim precedes its close")
            val before = rig.device(MediaKind.Video).steps()
            val callbacks = rig.callbacks(MediaKind.Video)
            callbacks.onOutputAvailable(9, 0, 6, 2L, CaptureContract.FLAG_KEY_FRAME)
            callbacks.onFormatChanged(fields)
            callbacks.onError("late")
            assertEquals(before, rig.device(MediaKind.Video).steps(), "no buffer fetched, no index released")
            assertNull(rig.videoInner.next(), "nothing handed off")
            assertEquals(emptyList(), fields.reads(), "no format field read")
            proceed.open()
            stopper.join()
        }
        assertEquals(CaptureTermination.Completed, rig.session.termination, "the late error changed nothing")
        assertEquals(disposal(MediaKind.Video, surface = true), rig.device(MediaKind.Video).steps().takeLast(3), "the index stayed the codec's, reclaimed by its disposal")
    }

    @Test
    fun `e15 an overlapping callback is the track's codec fault, touches nothing, and the outer transfer's codec is disposed only after it`() = runTest {
        val rig = EncoderRig()
        val reached = HostGate("the outer video transfer is fetching")
        val release = HostGate("the row lets the outer transfer go")
        rig.backend.prepare = { device -> if (device.name == "video") device.holdFetch = Triple(1, reached, release) }
        assertEquals(CaptureStart.Started, rig.start())
        rig.videoUnit(index = 1, bytes = byteArrayOf(0, 0, 0, 1, 0x65, 1), pts = 1L)
        rig.videoUnit(index = 2, bytes = byteArrayOf(0, 0, 0, 1, 0x65, 2), pts = 2L)
        val callbacks = rig.callbacks(MediaKind.Video)
        val outer = HostWorker("codec-callback") { callbacks.onOutputAvailable(1, 0, 6, 1L, CaptureContract.FLAG_KEY_FRAME) }.start()
        withSettlement(listOf(release), listOf(outer)) {
            reached.await(outer)
            callbacks.onOutputAvailable(2, 0, 6, 2L, CaptureContract.FLAG_KEY_FRAME)
            assertEquals(CaptureTermination.Failed(CaptureFault.Codec(MediaKind.Video, "overlapping codec callbacks")), rig.session.termination)
            val steps = rig.device(MediaKind.Video).steps()
            assertFalse(steps.any { it.startsWith("get 2") || it.startsWith("release 2") }, "the overlapping callback fetched and released nothing: $steps")
            assertFalse("release-codec:video" in steps, "and the codec the outer transfer holds was not disposed underneath it")
            assertEquals(setOf(MediaKind.Video to CallbackRole.Codec), rig.session.callbackInventory().active, "the outer callback is still the one admitted")
            release.open()
            outer.join()
        }
        assertEquals(listOf("get 1:video", "release 1:video") + disposal(MediaKind.Video, surface = true), rig.device(MediaKind.Video).steps().drop(4))
        val receipt = rig.session.retirement()
        assertEquals(CaptureTermination.Failed(CaptureFault.Codec(MediaKind.Video, "overlapping codec callbacks")), receipt.termination)
        assertEquals(emptyList(), receipt.causes)
    }

    @Test
    fun `e16 a callback under a key the session never minted is refused before any codec effect`() {
        val rig = EncoderRig()
        val stray = TrackEncoder(VIDEO, rig.backend)
        stray.bind(EncoderRoute(rig.session, Rig().codecGate.bindCodecCallbacks(MediaKind.Video), GENERATION, rig.videoHandoff) { _, _ -> })
        stray.open(rig.ownership)
        val device = rig.device(MediaKind.Video)
        val before = device.steps()
        synchronized(device.buffers) { device.buffers[0] = ByteBuffer.wrap(byteArrayOf(1)) }
        val refused = assertFailsWith<IllegalStateException> { checkNotNull(device.registered).onOutputAvailable(0, 0, 1, 0L, 0) }
        assertEquals("this Video encoder's codec key is not bound to its session", refused.message)
        assertEquals(before, device.steps(), "nothing fetched or released")
        assertNull(rig.session.termination)
    }

    // -- the worker across acquisition ------------------------------------------------------------

    @Test
    fun `w1 a codec error delivered inside start keeps the worker until the rejected codec is disposed`() = runTest {
        val rig = EncoderRig()
        var aliveInside = false
        rig.backend.prepare = { device ->
            if (device.name == "video") {
                device.duringStart = { callbacks ->
                    callbacks.onError("startup codec error")
                    aliveInside = rig.dispatcher.isAlive
                }
            }
        }
        assertIs<CaptureStart.Refused>(rig.start())
        assertTrue(aliveInside, "the worker outlives the ending while the codec is still opening on it")
        assertEquals(CaptureTermination.Failed(CaptureFault.Codec(MediaKind.Video, "startup codec error")), rig.session.termination)
        assertEquals(disposal(MediaKind.Video, surface = true), rig.device(MediaKind.Video).steps().takeLast(3), "the rejected codec was disposed")
        assertTrue(rig.before("surface-release:video", "quit:worker"), "before the worker quit: ${rig.logNow()}")
        assertEquals(1, rig.quits())
        assertFalse("create:Audio" in rig.logNow(), "nothing is opened once the worker is gone: ${rig.logNow()}")
        assertFalse(rig.ownership.retainWorker(), "every protection was given back, so the worker is gone")
        assertReservationsBalanced(rig.ownership, "after a codec error inside start")
        rig.session.retirement()
    }

    @Test
    fun `w2 a stop while start is still running keeps the worker until the late codec is disposed`() {
        val rig = EncoderRig()
        val inside = HostGate("the video codec is starting")
        val resume = HostGate("the row lets start return")
        rig.backend.prepare = { device ->
            if (device.name == "video") {
                device.duringStart = {
                    inside.open()
                    resume.await()
                }
            }
        }
        val outcome = AtomicReference<CaptureStart?>()
        val opener = HostWorker("opening encoder") { outcome.set(rig.start()) }.start()
        var aliveWhileOpening = false
        var releasedWhileOpening = true
        withSettlement(listOf(resume), listOf(opener)) {
            inside.await(opener)
            rig.session.stop(null)
            aliveWhileOpening = rig.dispatcher.isAlive
            releasedWhileOpening = "release-codec:video" in rig.logNow()
            resume.open()
            opener.join()
        }
        assertTrue(aliveWhileOpening, "the stop did not quit the worker the opening codec runs on")
        assertFalse(releasedWhileOpening, "and did not dispose a codec that was not yet the ownership's")
        assertIs<CaptureStart.Refused>(outcome.get())
        assertEquals(disposal(MediaKind.Video, surface = true), rig.device(MediaKind.Video).steps().takeLast(3), "the late handle was rejected and disposed")
        assertTrue(rig.before("surface-release:video", "quit:worker"), "before the worker quit: ${rig.logNow()}")
        assertEquals(1, rig.quits())
        assertFalse("create:Audio" in rig.logNow())
        assertFalse(rig.ownership.retainWorker())
        assertReservationsBalanced(rig.ownership, "after a stop across start")
    }

    @Test
    fun `w3 every acquisition protection balances on create, configure and start failure`() {
        for (at in listOf("create", "configure", "start")) {
            val rig = EncoderRig()
            if (at == "create") {
                rig.backend.refuse = "no encoder for this size"
            } else {
                rig.backend.prepare = { device ->
                    if (device.name == "video") {
                        device.failAt = at
                        device.failure = IllegalStateException("$at failed")
                    }
                }
            }
            val refused = assertIs<CaptureStart.Refused>(rig.start(), at)
            val expected =
                when (at) {
                    "create" -> "the Video encoder cannot be configured: no encoder for this size"
                    else -> "$at failed"
                }
            assertEquals(expected, refused.reason, at)
            assertEquals("quit:worker", rig.logNow().last(), "$at: the worker quit last: ${rig.logNow()}")
            assertEquals(1, rig.quits(), at)
            assertFalse(rig.ownership.retainWorker(), "$at: every protection was given back")
            assertReservationsBalanced(rig.ownership, "after a $at failure")
            if (at != "create") assertTrue(rig.before("release-codec:video", "quit:worker"), at)
        }
    }

    @Test
    fun `w4 a transfer admitted during a failed start keeps its own protection after the acquisition settles`() {
        val rig = EncoderRig()
        val reached = HostGate("the start-time transfer is fetching")
        val release = HostGate("the row lets the start-time transfer go")
        val payload = byteArrayOf(0, 0, 0, 1, 0x65, 9)
        val delivered = AtomicReference<EncoderCallbacks?>()
        val transfer = HostWorker("codec-callback") { checkNotNull(delivered.get()).onOutputAvailable(3, 0, payload.size, 10L, CaptureContract.FLAG_KEY_FRAME) }
        rig.backend.prepare = { device ->
            if (device.name == "video") {
                synchronized(device.buffers) { device.buffers[3] = ByteBuffer.wrap(payload) }
                device.holdFetch = Triple(3, reached, release)
                device.duringStart = { callbacks ->
                    delivered.set(callbacks)
                    transfer.start()
                    reached.await(transfer)
                    throw IllegalStateException("start failed")
                }
            }
        }
        assertEquals(ObserveOutcome.Recorded, rig.videoLedger.observe(GENERATION, MediaKind.Video, CaptureObservation(10L, BASE_NANOS)))
        var outcome: CaptureStart? = null
        var aliveAfterStartup = false
        var releasedEarly = true
        withSettlement(listOf(release), listOf(transfer)) {
            outcome = rig.start()
            aliveAfterStartup = rig.dispatcher.isAlive
            releasedEarly = "release-codec:video" in rig.logNow()
            release.open()
            transfer.join()
        }
        assertEquals(CaptureStart.Refused("start failed"), outcome, "the disposal is deferred, not failed, so the refusal is the named one")
        assertTrue(aliveAfterStartup, "the transfer's own retention held the worker after the acquisition and the startup settled")
        assertFalse(releasedEarly, "and its lease held the codec")
        val steps = rig.logNow()
        assertEquals(
            listOf("release 3:video", "release-codec:video", "surface-release:video", "quit:worker"),
            steps.filter { it in setOf("release 3:video", "release-codec:video", "surface-release:video", "quit:worker") },
            "the index, then the codec, then its surface, then the worker: $steps",
        )
        assertEquals(1, rig.quits())
        assertFalse(rig.ownership.retainWorker())
        assertReservationsBalanced(rig.ownership, "after a transfer held across a failed start")
    }

    // -- a failed open's cleanup, through the startup -----------------------------------------------

    @Test
    fun `f1 a failed open whose cleanup also failed leaves the startup as itself, carrying every cleanup failure`() = runTest {
        class Case(val label: String, val at: String, val primary: Throwable, val release: Throwable?, val surface: Throwable? = null)
        val cases =
            listOf(
                Case("configure, release Exception", "configure", IllegalStateException("configure failed"), IllegalStateException("codec release failed")),
                Case("configure, release Error", "configure", IllegalStateException("configure failed"), AssertionError("codec release raised an Error")),
                Case("configure, release cancellation", "configure", IllegalStateException("configure failed"), CancellationException("codec release cancelled")),
                Case("start, release and surface", "start", IllegalStateException("start failed"), IllegalStateException("codec release failed"), IllegalStateException("surface release failed")),
                Case("configure Error, release Exception", "configure", AssertionError("configure raised an Error"), IllegalStateException("codec release failed")),
            )
        for (case in cases) {
            val rig = EncoderRig()
            rig.backend.prepare = { device ->
                if (device.name == "video") {
                    device.failAt = case.at
                    device.failure = case.primary
                    device.releaseFailure = case.release
                    device.surfaceFailure = case.surface
                }
            }
            val escaped = runCatching { rig.start() }.exceptionOrNull()
            assertSame(case.primary, escaped, "${case.label}: the opening failure reaches the startup's caller as itself")
            for (cleanup in listOfNotNull(case.release, case.surface)) {
                assertEquals(1, case.primary.suppressed.count { it === cleanup }, "${case.label}: ${cleanup.message} is carried once, on the primary")
            }
            val steps = rig.device(MediaKind.Video).steps()
            assertTrue("release-codec:video" in steps, "${case.label}: the release was attempted")
            if (case.at == "start") assertTrue("surface-release:video" in steps, "${case.label}: and the surface after the release that failed")
            assertEquals(1, rig.quits(), case.label)
            assertTrue(rig.before("release-codec:video", "quit:worker"), case.label)
            assertIs<CaptureTermination.Cancelled>(rig.session.retirement().termination, case.label)
            assertFalse(rig.ownership.retainWorker(), case.label)
            assertReservationsBalanced(rig.ownership, "after ${case.label}")
        }
    }

    @Test
    fun `f3 a release that raises the opening failure's own instance still leaves the startup as that failure, cleaned up once`() = runTest {
        val rig = EncoderRig()
        val same = IllegalStateException("configure failed, and the release raised it again")
        rig.backend.prepare = { device ->
            if (device.name == "video") {
                device.failAt = "configure"
                device.failure = same
                device.releaseFailure = same
            }
        }
        val escaped = runCatching { rig.start() }.exceptionOrNull()
        assertSame(same, escaped, "the disposal was not proved, so the opening failure reaches the startup's caller as itself")
        assertTrue(same.suppressed.none { it === same }, "never suppressed onto itself")
        assertEquals(listOf("callbacks:video", "configure:video", "release-codec:video"), rig.device(MediaKind.Video).steps(), "the release was attempted once")
        assertEquals(1, rig.quits())
        assertTrue(rig.before("release-codec:video", "quit:worker"))
        assertIs<CaptureTermination.Cancelled>(rig.session.retirement().termination)
        assertFalse(rig.ownership.retainWorker())
        assertReservationsBalanced(rig.ownership, "after a release that raised the opening failure itself")
    }

    @Test
    fun `f2 a failed open whose cleanup succeeded stays the named refusal`() {
        val rig = EncoderRig()
        val primary = IllegalStateException("configure failed")
        rig.backend.prepare = { device ->
            if (device.name == "video") {
                device.failAt = "configure"
                device.failure = primary
            }
        }
        assertEquals(CaptureStart.Refused("configure failed"), rig.start())
        assertEquals(emptyList(), primary.suppressed.toList(), "the disposal succeeded: there is nothing it could not prove")
        assertEquals(listOf("callbacks:video", "configure:video", "release-codec:video"), rig.device(MediaKind.Video).steps())
        assertEquals(1, rig.quits())
    }

    // -- the backend's own allocation ----------------------------------------------------------------

    /** Every row's settlement: the worker quit once, after anything released, and every protection balanced. */
    private suspend fun settledOnce(rig: EncoderRig, label: String) {
        assertEquals(1, rig.quits(), label)
        assertEquals("quit:worker", rig.logNow().last(), "$label: ${rig.logNow()}")
        assertFalse(rig.ownership.retainWorker(), label)
        assertReservationsBalanced(rig.ownership, label)
        assertIs<CaptureTermination.Cancelled>(rig.session.retirement().termination, label)
    }

    @Test
    fun `b1 a capability refusal whose release succeeds is the named refusal, released once`() = runTest {
        val rig = EncoderRig(allocating = true)
        rig.allocation.refuse = true
        assertEquals(CaptureStart.Refused("the Video encoder cannot be configured: the AVC encoder does not accept surface input"), rig.start())
        assertEquals(listOf("open:worker", "allocate:Video", "capabilities:Video", "release-native:Video", "quit:worker"), rig.logNow())
        settledOnce(rig, "clean refusal")
    }

    @Test
    fun `b2 a capability query that throws, with a clean release, is the named failure, released once`() = runTest {
        val rig = EncoderRig(allocating = true)
        rig.allocation.queryFailure = IllegalStateException("capabilities could not be read")
        assertEquals(CaptureStart.Refused("capabilities could not be read"), rig.start())
        assertEquals(listOf("open:worker", "allocate:Video", "capabilities:Video", "release-native:Video", "quit:worker"), rig.logNow())
        settledOnce(rig, "query failure")
    }

    @Test
    fun `b3 a release that fails after a refusal or a failed query leaves the startup as that failure, carrying the release failure`() = runTest {
        class Case(val label: String, val query: Throwable?, val release: Throwable)
        val cases =
            listOf(
                Case("refusal, release Exception", null, IllegalStateException("native release failed")),
                Case("refusal, release Error", null, AssertionError("native release raised an Error")),
                Case("refusal, release cancellation", null, CancellationException("native release cancelled")),
                Case("query failure, release Exception", IllegalStateException("capabilities could not be read"), IllegalStateException("native release failed")),
            )
        for (case in cases) {
            val rig = EncoderRig(allocating = true)
            if (case.query == null) rig.allocation.refuse = true else rig.allocation.queryFailure = case.query
            rig.allocation.releaseFailure = case.release
            val escaped = runCatching { rig.start() }.exceptionOrNull()
            assertNotNull(escaped, "${case.label}: the release was not proved, so the startup leaves by throwing")
            if (case.query != null) {
                assertSame(case.query, escaped, "${case.label}: the query failure, as itself")
            } else {
                assertEquals("the Video encoder cannot be configured: the AVC encoder does not accept surface input", assertIs<EncoderUnsupported>(escaped, case.label).message)
            }
            assertEquals(1, escaped.suppressed.count { it === case.release }, "${case.label}: carrying the release failure once")
            assertEquals(1, rig.logNow().count { it == "release-native:Video" }, "${case.label}: released once")
            assertFalse("create:Video" in rig.logNow(), "${case.label}: never transferred")
            settledOnce(rig, case.label)
        }
    }

    @Test
    fun `b4 a release that raises the failure's own instance does not look clean`() = runTest {
        val rig = EncoderRig(allocating = true)
        val same = IllegalStateException("capabilities could not be read, and the release raised it again")
        rig.allocation.queryFailure = same
        rig.allocation.releaseFailure = same
        assertSame(same, runCatching { rig.start() }.exceptionOrNull(), "the failure leaves as itself rather than as a clean named refusal")
        assertTrue(same.suppressed.none { it === same }, "never suppressed onto itself")
        assertEquals(1, rig.logNow().count { it == "release-native:Video" }, "released once")
        settledOnce(rig, "same instance")
    }

    @Test
    fun `b5 an allocation that throws releases nothing and is the named failure`() = runTest {
        val rig = EncoderRig(allocating = true)
        rig.allocation.allocateFailure = IllegalStateException("no codec could be allocated")
        assertEquals(CaptureStart.Refused("no codec could be allocated"), rig.start())
        assertEquals(listOf("open:worker", "allocate:Video", "quit:worker"), rig.logNow(), "nothing to release")
        settledOnce(rig, "allocation failure")
    }

    @Test
    fun `b6 an accepted codec is transferred to its device, and only the device disposes of it`() = runTest {
        val rig = EncoderRig(allocating = true)
        assertEquals(CaptureStart.Started, rig.start())
        val opened = rig.logNow()
        assertEquals(
            listOf("open:worker", "allocate:Video", "capabilities:Video", "create:Video", "callbacks:video"),
            opened.take(5),
            "allocated, checked, then handed to its device",
        )
        rig.session.stop(null)
        assertFalse(rig.logNow().any { it.startsWith("release-native:") }, "the allocation released nothing it had transferred: ${rig.logNow()}")
        assertEquals(disposal(MediaKind.Video, surface = true), rig.device(MediaKind.Video).steps().takeLast(3), "the device disposed of it")
        assertEquals(1, rig.quits())
        assertReservationsBalanced(rig.ownership, "after a transferred codec")
    }

    // -- composition ------------------------------------------------------------------------------

    @Test
    fun `e17 the deferred composition binds each encoder to its own track, holder and the session's codec authority`() = runTest {
        val log = mutableListOf<String>()
        val backend = HostEncoderBackend(log)
        val video = TrackEncoder(VIDEO, backend)
        val audio = TrackEncoder(AUDIO, backend)
        val environment = CaptureEnvironment(NoAcquire(), HostDispatcher(), LateLog(), video.trackCodec, audio.trackCodec)
        val capture = compose(environment, AUDIO.keying(ORIGIN))
        assertEquals(emptyList(), log, "assembly created no codec")
        val retention = OrderedCaptureOwnership(GENERATION, HostDispatcher(), LateLog())
        video.open(retention)
        audio.open(retention)

        val videoDevice = backend.device(MediaKind.Video)
        synchronized(videoDevice.buffers) { videoDevice.buffers[3] = ByteBuffer.wrap(byteArrayOf(0, 0, 0, 1, 0x65, 4)) }
        capture.video.ledger.observe(GENERATION, MediaKind.Video, CaptureObservation(9L, BASE_NANOS))
        checkNotNull(videoDevice.registered).onOutputAvailable(3, 0, 6, 9L, CaptureContract.FLAG_KEY_FRAME)
        assertEquals(9L, assertNotNull(capture.video.handoff.next(), "the video codec's output is the video track's").output.presentationTimeUs)
        assertNull(capture.audio.handoff.next())
        checkNotNull(videoDevice.registered).onFormatChanged(videoFields())
        assertTrue(capture.videoFormat.format.isCompleted, "and its format is the video holder's")
        assertFalse(capture.audioFormat.format.isCompleted)

        val audioDevice = backend.device(MediaKind.Audio)
        checkNotNull(audioDevice.registered).onFormatChanged(audioFields())
        assertEquals(AUDIO_FORMAT_CONFIG.copy(bitrate = AUDIO.maximumBitrate), capture.audioFormat.format.await().config)

        // A keying of another PCM geometry is refused before anything is built.
        val refused = assertFailsWith<IllegalArgumentException> {
            compose(CaptureEnvironment(NoAcquire(), HostDispatcher(), LateLog(), TrackEncoder(VIDEO, backend).trackCodec, TrackEncoder(AUDIO, backend).trackCodec), UnitAudioKeying(44_100, 1024, ORIGIN))
        }
        assertEquals("the audio keying's sample rate 44100 is not the configured PCM rate 48000", refused.message)
    }

    private fun compose(environment: CaptureEnvironment, keying: AudioKeying): DeferredLiveCapture =
        composeDeferredBroadcast(
            generation = GENERATION,
            permissions = Granted(GENERATION),
            cameraFacts = CaptureCapability.Available,
            audioFacts = CaptureCapability.Available,
            calibration = ClockCalibration(elapsedRealtimeNanos = BASE_NANOS, epoch = UnixMicros(EPOCH), uncertaintyMicros = 10L),
            origin = ORIGIN,
            audioKeying = keying,
            capacity = CAPACITY,
            maxOutputBytes = HOST_MAX_OUTPUT_BYTES,
            maxNalUnits = HOST_MAX_NAL_UNITS,
            videoName = VIDEO_CONFIG.name,
            audioName = AUDIO_CONFIG.name,
            formatBounds = FIXTURE_BOUNDS,
            formatBudget = 5.seconds,
            environment = environment,
            tracks = ScriptedTracks(),
            completion = Completion(),
            record = CaptureDiscardRecord(GENERATION),
            writeTimeout = WRITE_TIMEOUT,
            drainTimeout = DRAIN_TIMEOUT,
            videoUtility = ScriptedVideoUtility,
            audioUtility = ScriptedAudioUtility,
        )

    private companion object {
        val VIDEO: VideoEncoderConfiguration = (VideoEncoderConfiguration.of(640, 480, 30, 1_000_000, 1_500_000uL, 2) as EncoderPlan.Planned<VideoEncoderConfiguration>).configuration
        val AUDIO: AudioEncoderConfiguration = (AudioEncoderConfiguration.of(48_000, 2, 96_000, 128_000uL) as EncoderPlan.Planned<AudioEncoderConfiguration>).configuration
    }
}
