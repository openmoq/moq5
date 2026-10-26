package org.openmoq.moq5.samples.player

import java.io.File
import java.nio.ByteBuffer
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.PackagingKind
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The observer on the sample's own path: the Activity's seat, the owned host
 * (`runOwnedPlayback`, which `AndroidPlaybackHost` is), the session's render
 * half, the receive loop and the real render pump over fake codecs. Nothing
 * here injects a released count or a decoder-EOS time: both come from the
 * pump and the receive loop.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class PresentationWiringTest {
    private companion object {
        const val BASE_NS = 9_000_000_000_000L
        const val MS = 1_000_000L
        val VIDEO = FormatOutcome.Video(
            mime = "video/avc", width = 320, height = 240,
            csd = listOf(byteArrayOf(0, 0, 0, 1, 0x67), byteArrayOf(0, 0, 0, 1, 0x68)),
            avc = AvcConfig(listOf(byteArrayOf(0x67)), listOf(byteArrayOf(0x68)), nalLengthSize = 4, profile = 0x42, constraints = 0xC0, level = 0x1E),
            packaging = PackagingKind.Cmaf, framing = FramingPolicy.StrictLengthPrefixed, timescale = 90_000u,
        )
    }

    private fun vector(name: String) = markerVectors.single { it.name == name }
    private fun grey(v: MarkerVector) = IntArray(v.luma.size) { val y = v.luma[it].toInt() and 0xFF; (0xFF shl 24) or (y shl 16) or (y shl 8) or y }

    // ---------------------------------------------------------------- capture conversion

    @Test fun greyArgbConvertsToExactlyTheLumaPlaneTheReaderConsumes() {
        for (v in markerVectors) {
            val capture = PlaybackCapture.fromArgb(grey(v), v.width, v.height)
            assertTrue(capture.luma.contentEquals(v.luma), v.name)
            val got = PlaybackMarker.classify(capture.luma, capture.width, capture.height)
            assertEquals(v.outcome to v.index, got.outcome.wire to got.index, v.name)
        }
    }

    @Test fun conversionIgnoresAlphaAndWeighsChannelsByRec601() {
        val pixels = intArrayOf(0x00808080, 0x7F808080, 0xFFFF0000.toInt(), 0xFF00FF00.toInt(), 0xFF0000FF.toInt(), 0xFFFFFFFF.toInt())
        val capture = PlaybackCapture.fromArgb(pixels, 3, 2)
        // (77 R + 150 G + 29 B + 128) >> 8
        assertEquals(listOf(128, 128, 77, 149, 29, 255), capture.luma.map { it.toInt() and 0xFF })
        assertEquals(3 to 2, capture.width to capture.height)
    }

    @Test fun aShortPixelArrayBecomesACaptureSizeRefusalNotAGuess() {
        val capture = PlaybackCapture.fromArgb(IntArray(5), 3, 2)
        assertEquals(PlaybackMarkerOutcome.CaptureSize, PlaybackMarker.classify(capture.luma, capture.width, capture.height).outcome)
    }

    // ---------------------------------------------------------------- decoder EOS observation time

    private class TimedRender(val scope: TestScope) : PlaybackRenderPort {
        var eosAtMs = Long.MAX_VALUE
        var released = 0
        override fun offer(lane: PlaybackLane, unit: AccessUnitOutcome.Unit, logicalUs: Long) = PlaybackOffer.Queued
        override fun pump() = false
        override fun endInputs() {}
        override val decoderOutputsEnded: Boolean get() = scope.testScheduler.currentTime >= eosAtMs
        override val videoOutputsReleased: Int get() = released
    }

    private fun packet() = PlaybackReceived(PlaybackLane.Video, listOf(PlaybackTimedUnit(AccessUnitOutcome.Unit(byteArrayOf(1), 30, true), 30)))

    @Test fun decoderEosTimeIsTheLoopsFirstObservationBeforeItReturns() = runTest {
        val render = TimedRender(this).also { it.eosAtMs = 500; it.released = 7 }
        val seen = mutableListOf<Pair<Long, Int>>()
        val hookDone = CompletableDeferred<Unit>()
        val result = async {
            receivePlayback({ packet() }, render, PlaybackFailure(), false, 2, waitLimitMs = 10_000,
                nowNs = { BASE_NS + testScheduler.currentTime * MS },
                onDecoderEos = { eos, released -> seen += eos to released; delay(100); hookDone.complete(Unit) })
        }
        advanceTimeBy(499); runCurrent()
        assertEquals(emptyList(), seen, "no decoder EOS observed yet")
        advanceTimeBy(3); runCurrent()
        assertEquals(1, seen.size)
        val (eos, released) = seen.single()
        assertTrue(eos in (BASE_NS + 500 * MS)..(BASE_NS + 502 * MS), "the first observation, on the monotonic clock: $eos")
        assertFalse(eos == 30_000L || eos == 30L, "never media time")
        assertEquals(7, released, "the renderer's own count")
        assertFalse(result.isCompleted, "the settlement hook runs before the loop returns")
        advanceTimeBy(100); runCurrent()
        assertEquals(PlaybackReadResult(PlaybackReadEnd.ObjectLimit, 2, 2), withTimeout(100) { result.await() })
        assertTrue(hookDone.isCompleted)
        assertEquals(1, seen.size, "exactly once")
    }

    @Test fun noDecoderEosMeansNoSettlement() = runTest {
        val render = TimedRender(this)
        val seen = mutableListOf<Long>()
        val result = async {
            runCatching {
                receivePlayback({ packet() }, render, PlaybackFailure(), false, 1, waitLimitMs = 20,
                    nowNs = { BASE_NS + testScheduler.currentTime * MS }, onDecoderEos = { eos, _ -> seen += eos })
            }
        }
        advanceTimeBy(50); runCurrent()
        assertTrue(result.await().exceptionOrNull() is PlaybackRenderRefused)
        assertEquals(emptyList(), seen)
    }

    // ---------------------------------------------------------------- the sample path end to end

    /** A real pump over a fake video codec that answers each input with one output. */
    private class Wire(val scope: TestScope) {
        val log = mutableListOf<String>()
        val texture = Any()
        var surfaceReleaseFailure: Throwable? = null
        var retained: Set<PlaybackRetainedResource> = emptySet()
        val registry = PlaybackDisplayRegistry<Any, Any>(
            createSurface = { log += "surface.create"; Any() },
            releaseSurface = { log += "surface.release"; surfaceReleaseFailure?.let { throw it } },
            releaseTransferredTexture = { log += "texture.release" },
        ).also { it.available(texture) }
        val nowNs = { BASE_NS + scope.testScheduler.currentTime * MS }
        val seat = PlaybackPresentationSeat(registry, nowNs)
        val gate = PlaybackRuntimeGate()
        val session = PlaybackSession { line -> log += line }
        /** The logcat channel: (tag, line), in emission order. */
        val logcat = mutableListOf<Pair<String, String>>()
        var reportFailure: ((String) -> Throwable?)? = null
        val sinks = PresentationSinks(
            report = { line -> reportFailure?.invoke(line)?.let { throw it }; log += line },
            log = { tag, line -> logcat += tag to line; log += "logcat:$tag" },
        )
        var rendererFailure: Throwable? = null
        var workerStartFailure: Throwable? = null
        /** gate.retained() as the release signal saw it, per invocation. */
        val releaseSignals = mutableListOf<Boolean>()
        var captureFailureAtFrame = -1
        /** Frame number -> the capture the TextureView reports when that frame is latched. */
        var captures: Map<Int, PlaybackCapture> = emptyMap()
        var failAtFrame = -1
        var destroyAtFrame = -1
        /** An input whose decoded output is empty: discarded by the pump, so never a rendered frame. */
        var emptyAtInput = -1

        val worker = object : PlaybackRenderWorker {
            override val dispatcher = StandardTestDispatcher(scope.testScheduler)
            override fun start() { log += "worker.start"; workerStartFailure?.let { throw it } }
            override suspend fun stopAndJoin(): Boolean { log += "worker.join"; return true }
        }

        inner class Codec : PlaybackCodecBackend {
            private val outputs = ArrayDeque<PlaybackCodecEvent>()
            private var next = 0
            private var inputs = 0
            var frames = 0
            override fun inputIndex() = 0
            override fun inputBuffer(index: Int): ByteBuffer = ByteBuffer.allocate(64)
            override fun queueInput(index: Int, size: Int, presentationUs: Long, end: Boolean) {
                val empty = end || inputs++ == emptyAtInput
                outputs.add(PlaybackCodecEvent.Buffer(next++, ByteBuffer.allocate(4), 0, if (empty) 0 else 4, presentationUs, end, false))
            }
            override fun output(): PlaybackCodecEvent = outputs.removeFirstOrNull() ?: PlaybackCodecEvent.Idle
            override fun releaseOutput(index: Int, renderTimeNs: Long?) {
                if (renderTimeNs == null) return
                val frame = frames
                if (frame == failAtFrame) throw IllegalStateException("releaseOutputBuffer failed at frame $frame")
                frames++
                // The TextureView latches the rendered frame and reports it on the UI thread.
                if (frame == destroyAtFrame) seat.onDestroyed(texture)
                if (frame == captureFailureAtFrame) seat.onFrame(texture, renderTimeNs, { throw IllegalStateException("getBitmap failed") })
                captures[frame]?.let { c -> seat.onFrame(texture, renderTimeNs, { c }) }
            }
            override fun stop() {}
            override fun release() {}
        }

        fun renderer(): ManagedPlaybackRender {
            rendererFailure?.let { throw it }
            val pump = PlaybackRenderPump(
                PlaybackCodec(Codec(), false, 64), null,
                PlaybackSync(object : PlaybackSyncBackend {
                    override fun queueAudio(bytes: ByteBuffer, token: Int, presentationUs: Long) = error("no audio")
                    override fun flush() {}
                    override fun release() {}
                }, 1, 8, {}, { throw AssertionError("unexpected sync failure", it) }),
                { error("no audio") },
            )
            return object : ManagedPlaybackRender {
                override fun start() { log += "render.start" }
                override fun close(): PlaybackRenderClosed { log += "render.close"; pump.stop(); return PlaybackRenderClosed(retained, null) }
                override fun offer(lane: PlaybackLane, unit: AccessUnitOutcome.Unit, logicalUs: Long) = pump.offer(lane, unit, logicalUs)
                override fun pump() = pump.pump()
                override fun endInputs() = pump.endInputs()
                override val decoderOutputsEnded: Boolean get() = pump.decoderOutputsEnded
                override val videoOutputsReleased: Int get() = pump.videoReleased
            }
        }

        /** Exactly what the Activity's Start does, with the Android worker and renderer replaced. */
        suspend fun start(budget: Int) {
            val attempt = seat.attempt(budget, sinks)
            val host = PlaybackRenderHost { _, _, consume ->
                runOwnedPlayback(gate, { worker }, { registry.acquire() }, { _, _ -> renderer() }, consume,
                    onDisplayOwned = attempt::onDisplayOwned,
                    onReleased = { releaseSignals += gate.retained(); attempt.displayReleased() })
            }
            var i = 0
            session.renderPlayback(host, VIDEO, null, budget, next = {
                val us = i++ * 41_666L
                PlaybackReceived(PlaybackLane.Video, listOf(PlaybackTimedUnit(AccessUnitOutcome.Unit(byteArrayOf(1, 2), us, true), us)))
            }, presentation = attempt, nowNs = nowNs)
        }

        fun lines(prefix: String) = log.filter { it.startsWith(prefix) }
    }

    private fun markers(vararg frames: Pair<Int, String>) =
        frames.associate { (frame, name) -> vector(name).let { frame to PlaybackCapture.fromArgb(grey(it), it.width, it.height) } }

    @Test fun startSettlesAtDecoderEosAndReportsBeforeCloseThenClosesTheObserverAfterTheDisplay() = runTest {
        val w = Wire(this)
        w.captures = markers(0 to "index_0", 1 to "index_1_one_bit", 59 to "index_59_final")
        w.start(60)
        assertEquals(listOf("presented: pass=true released=60 latched=3 distinctTs=3 indices=[0,1,59] preVideo=0 unreadable=0 " +
            "syncInvalid=0 complementInvalid=0 captureRefusals=0 ordered=true settled=true ordinal=1 leaseId=1 refusal=none " +
            "freezeReason=settled"), w.lines("presented:"))
        assertEquals(listOf("observer-closed: lateAfterFreeze=0 ignoredForeign=0 retiredAfterFreeze=0"), w.lines("observer-closed:"))
        val order = listOf("surface.create", "render.start", "presented:", "render.close", "worker.join", "surface.release",
            "stopped:", "observer-closed:")
        assertEquals(order, w.log.filter { l -> order.any { l.startsWith(it) } }.map { l -> order.first { l.startsWith(it) } })
    }

    @Test fun releasedIsThePumpsCountNotTheBudget() = runTest {
        val w = Wire(this)
        w.emptyAtInput = 30               // 60 objects decode to 59 rendered frames
        w.captures = markers(0 to "index_0", 1 to "index_1_one_bit", 58 to "index_59_final")
        w.start(60)
        assertEquals(listOf("presented: pass=false released=59 latched=3 distinctTs=3 indices=[0,1,59] preVideo=0 unreadable=0 " +
            "syncInvalid=0 complementInvalid=0 captureRefusals=0 ordered=true settled=true ordinal=1 leaseId=1 " +
            "refusal=released-mismatch freezeReason=settled"), w.lines("presented:"))
        assertEquals(listOf("stopped: ObjectLimit; 60 objects, 60 decoder inputs"), w.lines("stopped:"))
    }

    @Test fun theSameActivityGuardRefusesEveryLaterStartIncludingAReannouncedSameTexture() = runTest {
        val w = Wire(this)
        w.captures = markers(0 to "index_0", 1 to "index_1_one_bit", 59 to "index_59_final")
        w.start(60)
        w.start(60)
        assertTrue(w.registry.destroyed(w.texture))
        w.registry.available(w.texture)
        w.start(60)
        val lines = w.lines("presented:")
        assertEquals(3, lines.size)
        assertTrue(" pass=true " in lines[0])
        assertEquals("presented: pass=false released=60 latched=0 distinctTs=0 indices=[] preVideo=0 unreadable=0 syncInvalid=0 " +
            "complementInvalid=0 captureRefusals=0 ordered=true settled=false ordinal=2 leaseId=2 refusal=observer-refused:reused-surface " +
            "freezeReason=settled", lines[1])
        assertEquals("presented: pass=false released=60 latched=0 distinctTs=0 indices=[] preVideo=0 unreadable=0 syncInvalid=0 " +
            "complementInvalid=0 captureRefusals=0 ordered=true settled=false ordinal=1 leaseId=3 refusal=observer-refused:reused-surface " +
            "freezeReason=settled", lines[2], "the re-announced SAME texture's frames never count")
        assertEquals(3, w.lines("observer-closed:").size)
    }

    @Test fun aFailedRunStillFreezesBeforeCloseAndClosesTheObserverAfterTheDisplay() = runTest {
        val w = Wire(this)
        w.captures = markers(0 to "index_0", 1 to "index_1_one_bit", 59 to "index_59_final")
        w.failAtFrame = 10
        val raised = runCatching { w.start(60) }.exceptionOrNull()
        assertTrue(raised is IllegalStateException && raised.message == "releaseOutputBuffer failed at frame 10", "got $raised")
        // Frames 0-9 were released successfully while the renderer was still owned; the failed one never counts.
        assertEquals(listOf("presented: pass=false released=10 latched=2 distinctTs=2 indices=[0,1] preVideo=0 unreadable=0 " +
            "syncInvalid=0 complementInvalid=0 captureRefusals=0 ordered=true settled=false ordinal=1 leaseId=1 refusal=cancelled " +
            "freezeReason=cancelled"), w.lines("presented:"))
        val order = listOf("presented:", "render.close", "surface.release", "observer-closed:")
        assertEquals(order, w.log.filter { l -> order.any { l.startsWith(it) } }.map { l -> order.first { l.startsWith(it) } })
        assertFalse(w.gate.retained())
    }

    @Test fun aSurfaceDestroyedMidRunRetiresTheObserverThroughTheSeat() = runTest {
        val w = Wire(this)
        w.captures = markers(0 to "index_0", 1 to "index_1_one_bit", 59 to "index_59_final")
        w.destroyAtFrame = 5
        w.start(60)
        val line = w.lines("presented:").single()
        assertTrue(" refusal=display-retired " in line && " pass=false " in line, line)
        assertTrue(" indices=[0,1] " in line, "frames after retirement never count: $line")
        assertEquals(listOf("texture.release"), w.log.filter { it == "texture.release" }, "the destroyed texture transfers to the lease")
    }

    @Test fun framesWithNoAttemptInFlightAreIgnored() = runTest {
        val w = Wire(this)
        var captured = 0
        w.seat.onFrame(w.texture, 1, { captured++; throw AssertionError("no capture without an observer in flight") })
        assertEquals(0, captured, "no bitmap is taken when nothing observes")
        w.seat.onDestroyed(Any())
        w.captures = markers(0 to "index_0", 1 to "index_1_one_bit", 59 to "index_59_final")
        w.start(60)
        assertTrue(" pass=true " in w.lines("presented:").single())
    }

    // ---------------------------------------------------------------- failure counts, channels, opt-in, capture faults

    /** True when the seat still routes frames to an observer: its capture is taken. */
    private fun Wire.stillObserving(): Boolean {
        var taken = false
        seat.onFrame(texture, 99, { taken = true; null })
        return taken
    }

    @Test fun aRetainedRendererKeepsTheObserverSeatedAndEmitsNoClosedReceipt() = runTest {
        val w = Wire(this)
        w.captures = markers(0 to "index_0", 1 to "index_1_one_bit", 59 to "index_59_final")
        w.retained = setOf(PlaybackRetainedResource.Sync)
        val raised = runCatching { w.start(60) }.exceptionOrNull()
        assertEquals("renderer resources, worker or display remain retained after uncertain cleanup", raised?.message)
        assertTrue(w.gate.retained(), "the host keeps the whole attempt")
        assertFalse("surface.release" in w.log, "a retained renderer's display is never closed")
        assertEquals(1, w.lines("presented:").size, "the verdict froze before any close")
        assertEquals(emptyList(), w.lines("observer-closed:"), "no closed receipt while the display is still owned")
        assertEquals(1L, w.registry.leaseIdFor(w.texture), "the lease is still held")
        assertTrue(w.stillObserving(), "the observer stays bound to the retained lease")
        assertEquals(emptyList(), w.releaseSignals, "no release fact on a retained path")
    }

    @Test fun aThrowingDisplayCloseKeepsTheObserverSeatedAndEmitsNoClosedReceipt() = runTest {
        val w = Wire(this)
        w.captures = markers(0 to "index_0", 1 to "index_1_one_bit", 59 to "index_59_final")
        val closeFailed = IllegalStateException("native Surface release failed")
        w.surfaceReleaseFailure = closeFailed
        val raised = runCatching { w.start(60) }.exceptionOrNull()
        assertTrue(raised === closeFailed, "the original cleanup cause is primary: $raised")
        assertTrue(raised!!.suppressed.any { it.message == "renderer resources, worker or display remain retained after uncertain cleanup" })
        assertTrue(w.gate.retained())
        assertEquals(1, w.log.count { it == "surface.release" }, "one close attempt, never retried")
        assertEquals(1, w.lines("presented:").size)
        assertEquals(emptyList(), w.lines("observer-closed:"), "no closed receipt while the display is still owned")
        assertEquals(1L, w.registry.leaseIdFor(w.texture))
        assertTrue(w.stillObserving())
        assertEquals(emptyList(), w.releaseSignals, "no release fact on a retained path")
    }

    @Test fun aSettledCloseStillEmitsExactlyOneClosedReceiptAfterRelease() = runTest {
        val w = Wire(this)
        w.captures = markers(0 to "index_0", 1 to "index_1_one_bit", 59 to "index_59_final")
        w.start(60)
        assertFalse(w.gate.retained())
        assertEquals(1, w.lines("observer-closed:").size)
        assertTrue(w.log.indexOf("surface.release") < w.log.indexOf(w.lines("observer-closed:").single()))
        assertEquals(null, w.registry.leaseIdFor(w.texture))
        assertFalse(w.stillObserving(), "a closed observer takes no capture")
        assertEquals(listOf(false), w.releaseSignals, "signalled once, after the gate was released")
    }

    @Test fun aFailureBeforeAnyDisplayHasNoLeaseToCloseAndClaimsNone() = runTest {
        val w = Wire(this)
        w.workerStartFailure = IllegalStateException("worker looper failed")
        val raised = runCatching { w.start(60) }.exceptionOrNull()
        assertEquals("worker looper failed", raised?.message)
        assertFalse("surface.create" in w.log, "no display was ever acquired")
        assertFalse(w.gate.retained())
        assertEquals(listOf(false), w.releaseSignals, "nothing was retained, so the attempt was released")
        assertEquals(listOf("presented: pass=false released=0 latched=0 distinctTs=0 indices=[] preVideo=0 unreadable=0 " +
            "syncInvalid=0 complementInvalid=0 captureRefusals=0 ordered=true settled=false ordinal=0 leaseId=0 refusal=cancelled " +
            "freezeReason=cancelled"), w.lines("presented:"), "ordinal=0 leaseId=0: no lease was observed")
        assertEquals(1, w.lines("observer-closed:").size)
    }

    @Test fun aFailureBeforeTheRendererExistsFreezesOnceWithZeroReleased() = runTest {
        val w = Wire(this)
        w.rendererFailure = IllegalStateException("codec configure failed")
        val raised = runCatching { w.start(60) }.exceptionOrNull()
        assertEquals("codec configure failed", raised?.message)
        assertEquals(listOf("presented: pass=false released=0 latched=0 distinctTs=0 indices=[] preVideo=0 unreadable=0 " +
            "syncInvalid=0 complementInvalid=0 captureRefusals=0 ordered=true settled=false ordinal=1 leaseId=1 refusal=cancelled " +
            "freezeReason=cancelled"), w.lines("presented:"))
        assertEquals(1, w.lines("observer-closed:").size)
        val order = listOf("presented:", "observer-closed:")
        assertEquals(order, w.log.filter { l -> order.any { l.startsWith(it) } }.map { l -> order.first { l.startsWith(it) } })
        assertFalse(w.gate.retained())
    }

    @Test fun eachReceiptIsEmittedExactlyOnceOnEachChannel() = runTest {
        val w = Wire(this)
        w.captures = markers(0 to "index_0", 1 to "index_1_one_bit", 59 to "index_59_final")
        w.start(60)
        val presented = w.lines("presented:").single()
        val closed = w.lines("observer-closed:").single()
        assertEquals(listOf("MoqPlayer" to presented, "MoqPlayerObserver" to closed), w.logcat)
        // Each line's two channels are adjacent: nothing else is emitted between them.
        val p = w.log.indexOf(presented)
        val c = w.log.indexOf(closed)
        assertEquals(setOf(presented, "logcat:MoqPlayer"), setOf(w.log[p], w.log[p + 1].takeIf { it.startsWith("logcat:") } ?: w.log[p - 1]))
        assertTrue(w.log[c - 1] == "logcat:MoqPlayerObserver" || w.log[c + 1] == "logcat:MoqPlayerObserver")
    }

    @Test fun aFailingReportSinkNeitherDropsTheLogcatLineNorTheClosedReceipt() = runTest {
        val w = Wire(this)
        w.captures = markers(0 to "index_0", 1 to "index_1_one_bit", 59 to "index_59_final")
        val sinkFailure = IllegalStateException("UI dispatch refused")
        w.reportFailure = { line -> if (line.startsWith("presented:")) sinkFailure else null }
        val raised = runCatching { w.start(60) }.exceptionOrNull()
        assertTrue(raised === sinkFailure, "the sink failure surfaces rather than being swallowed: $raised")
        assertEquals(listOf("MoqPlayer", "MoqPlayerObserver"), w.logcat.map { it.first })
        assertTrue(w.logcat[0].second.startsWith("presented: pass=true "))
        assertEquals(1, w.lines("observer-closed:").size, "cleanup still reports the closed receipt")
        assertEquals(0, w.lines("presented:").size)
    }

    @Test fun anObserverSinkFailureNeverReplacesTheSessionsOwnFailure() = runTest {
        val w = Wire(this)
        w.captures = markers(0 to "index_0", 1 to "index_1_one_bit", 59 to "index_59_final")
        w.failAtFrame = 10
        val sinkFailure = IllegalStateException("UI dispatch refused")
        w.reportFailure = { line -> if (line.startsWith("presented:") || line.startsWith("observer-closed:")) sinkFailure else null }
        val raised = runCatching { w.start(60) }.exceptionOrNull()
        assertEquals("releaseOutputBuffer failed at frame 10", raised?.message, "the playback failure stays primary")
        assertTrue(raised!!.suppressed.any { it === sinkFailure }, "the observer failure is kept, not dropped")
        assertEquals(listOf("MoqPlayer", "MoqPlayerObserver"), w.logcat.map { it.first }, "the other channel still has both receipts")
        assertTrue(" released=10 " in w.logcat[0].second)
    }

    @Test fun aThrowingCaptureIsANamedRefusalAndPlaybackCompletes() = runTest {
        val w = Wire(this)
        w.captures = markers(0 to "index_0", 1 to "index_1_one_bit", 59 to "index_59_final")
        w.captureFailureAtFrame = 5
        w.start(60)
        assertEquals(listOf("stopped: ObjectLimit; 60 objects, 60 decoder inputs"), w.lines("stopped:"), "a healthy session is not crashed")
        val line = w.lines("presented:").single()
        assertTrue(" captureRefusals=1 " in line && " refusal=capture-refused " in line && " pass=false " in line, line)
        assertEquals(1, w.lines("observer-closed:").size)
    }

    @Test fun observationIsOffUnlessTheLaunchFlagIsPresent() = runTest {
        val registry = PlaybackDisplayRegistry<Any, Any>({ Any() }, {}, {})
        assertEquals(null, presentationSeatFor(false, registry) { 0L }, "no flag: no seat, no capture, no settlement")
        assertTrue(presentationSeatFor(true, registry) { 0L } != null)
        assertEquals("org.openmoq.moq5.samples.player.OBSERVE_PRESENTATION", OBSERVE_PRESENTATION_EXTRA)
    }

    @Test fun theSupervisedLauncherSetsTheFlag() {
        // The path is the test task's declared input; see the player build script.
        val launcher = File(checkNotNull(System.getProperty("moq5.player.mediaLauncher")) { "moq5.player.mediaLauncher is not set" })
        check(launcher.isFile) { "launcher not found at ${launcher.path}" }
        assertTrue("\"--ez\", \"$OBSERVE_PRESENTATION_EXTRA\", \"true\"" in launcher.readText(),
            "the supervised device start must arm the observer")
    }

    @Test fun withoutAnObserverTheSessionNeitherReportsNorWaits() = runTest {
        // With the observer, a missing final frame costs the full settlement window; without it, nothing.
        val on = Wire(this)
        val t0 = testScheduler.currentTime
        on.start(60)
        val observed = testScheduler.currentTime - t0
        assertTrue(observed >= 3_000, "the observed run waits out the window: $observed ms")
        val off = Wire(this)
        val t1 = testScheduler.currentTime
        var i = 0
        val host = PlaybackRenderHost { _, _, consume ->
            runOwnedPlayback(off.gate, { off.worker }, { off.registry.acquire() }, { _, _ -> off.renderer() }, consume)
        }
        off.session.renderPlayback(host, VIDEO, null, 60, next = {
            val us = i++ * 41_666L
            PlaybackReceived(PlaybackLane.Video, listOf(PlaybackTimedUnit(AccessUnitOutcome.Unit(byteArrayOf(1, 2), us, true), us)))
        }, presentation = null, nowNs = off.nowNs)
        val plain = testScheduler.currentTime - t1
        assertTrue(plain < 1_000, "no settlement wait without an observer: $plain ms")
        assertEquals(listOf("stopped: ObjectLimit; 60 objects, 60 decoder inputs"), off.log.filter { !it.contains('.') })
        assertEquals(emptyList(), off.logcat)
    }
}
