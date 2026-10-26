package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import org.openmoq.moq5.EndpointConfig
import org.openmoq.moq5.MediaSender
import org.openmoq.moq5.MoqEndpoint
import org.openmoq.moq5.Namespace
import org.openmoq.moq5.NamespacePart
import org.openmoq.moq5.SendBackpressure
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.SenderConfig
import org.openmoq.moq5.TrackName
import org.openmoq.moq5.UnixMicros
import org.openmoq.moq5.WriteOutcome
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * One broadcast session, written against the published Kotlin API and nothing
 * else.
 *
 * The frames are SYNTHETIC. A camera would add a dependency and a permission
 * flow while proving nothing about the MoQ surface; what this sample exists to
 * demonstrate is the publish lifecycle, including the completion barriers that
 * are easy to get wrong: drain the accepted media, then complete, then drain
 * the terminal catalog, then close.
 */
internal class BroadcastSession(
    private val report: (String) -> Unit,
    /**
     * The encoder, built once the capture clock is calibrated so its outputs
     * can carry real capture instants. Until a `MediaCodec` loop is wired this
     * sample ships [SyntheticEncoderOutputs] behind the same seam — a fixture,
     * not a second publish path.
     */
    private val encoders: (Long, Int) -> EncodedOutputSource = { base, frames ->
        SyntheticEncoderOutputs(base, frames = frames, gop = GOP)
    },
    private val elapsed: ElapsedRealtimeSource = AndroidClockSources.elapsedRealtime,
    private val wall: WallClockSource = WallClockSource { UnixMicros(System.currentTimeMillis().toULong() * 1_000uL) },
) {

    suspend fun broadcast(url: String, namespaceText: String, trackText: String, frameCount: Int) {
        val namespace = parseNamespace(namespaceText)
        report("connecting to $url")
        broadcastLifetime(
            connect = { MoqEndpoint.connect(EndpointConfig(url), CONNECT_TIMEOUT) },
            describe = { },
            attach = { endpoint ->
                endpoint.attachSender(
                    SenderConfig(
                        namespace = namespace,
                        // Never drop on the service's behalf: a full queue is
                        // answered as WouldBlock and the decision is this app's.
                        backpressure = SendBackpressure.ReturnWouldBlock,
                    ),
                )
            },
            run = { endpoint, sender -> publish(endpoint, sender, trackText, frameCount) },
            closeChild = { sender -> sender.close() },
            closeEndpoint = { endpoint -> endpoint.closeAndJoin() },
        )
    }

    private suspend fun publish(endpoint: MoqEndpoint, sender: MediaSender, trackText: String, frameCount: Int) {
        report("waiting for the namespace to be accepted")
        sender.awaitReady()
        report("ready; publishing")

        // The track's format goes through the same validator, and the real
        // codec utility, as a live track's. Its parameter sets are SYNTHETIC
        // declared literals: they make the validator's and the converter's
        // path real, not the stream a decodable picture.
        val observed =
            VideoFormatObservation(
                width = WIDTH,
                height = HEIGHT,
                targetBitrate = null,
                maximumBitrate = MAXIMUM_BITRATE,
                config = listOf(ByteBuffer.wrap(SYNTHETIC_SPS.copyOf()), ByteBuffer.wrap(SYNTHETIC_PPS.copyOf())),
            )
        val format =
            when (val validated = TrackFormats.video(TrackName(trackText.encodeToByteArray()), observed, CONFIG_BOUNDS)) {
                is TrackFormatVerdict.Complete -> validated.format
                is TrackFormatVerdict.Refused -> {
                    report("cannot publish: " + validated.refusal.description)
                    return
                }
            }
        // The track is added with exactly the configuration its pipeline
        // converts and compares against.
        val track = sender.addTrack(format.config)

        // The capture clock is CALIBRATED once, by bracketing a wall-clock
        // reading between two elapsed-realtime readings. Nothing downstream
        // reads a clock again, so no arrival time can become a capture time.
        val calibration = Calibrator.measure(elapsed, wall)
        if (calibration is CalibrationOutcome.Unusable) {
            report("cannot publish: " + calibration.reason)
            return
        }
        val clock = PlatformCaptureClock((calibration as CalibrationOutcome.Measured).calibration)
        // One origin for the whole broadcast, declared before any output is
        // admitted; the encoder's own timestamps are relative to it.
        val pipeline = PublishPipeline(MediaKind.Video, PublishTimeline(0L), clock, format, AvcPayloadLimits(MAX_NAL_UNITS, MAX_OUTPUT_BYTES))

        val encoder = encoders(clock.calibration.elapsedRealtimeNanos, frameCount)
        val sink =
            object : TrackSink {
                override suspend fun write(obj: SendObject, timeout: Duration): WriteOutcome = sender.write(track, obj, timeout)

                override suspend fun endTrack(timeout: Duration): WriteOutcome = sender.endTrack(track, timeout)
            }
        val finished =
            publishFinite(MediaKind.Video, encoder, pipeline, sink, WRITE_TIMEOUT, report) { seen, sent ->
                if (!sender.status.value.mediaDemand && seen % PROGRESS_EVERY == 0) {
                    report("no subscriber yet; " + sent + " frame(s) queued")
                }
                delay(FRAME_INTERVAL)
            }
        when (finished) {
            is TrackDrain.Ended ->
                report(
                    "wrote " + finished.written + " frame(s); " + finished.refused + " refused by flow control; " + finished.skipped +
                        " skipped by the publish contract",
                )
            is TrackDrain.Unfinished -> {
                report("cannot continue: " + finished.reason)
                return
            }
        }

        // The broadcast terminal, after the track's own. Completion REMOVES
        // the track and discards what it still holds, so the accepted frames
        // and the track's terminal drain first; completion only RECORDS the
        // terminal catalog, so a second drain is what carries it to the core.
        when (val finish = endpointCompletion(sender, endpoint).finishPublication(DRAIN_TIMEOUT, report)) {
            Finish.Flushed -> report("published: the accepted frames, the track's terminal and the terminal catalog reached the core")
            is Finish.MediaNotDrained -> report("not published: the accepted frames did not drain, so completion was not requested: " + finish.outcome)
            is Finish.CompletionNotDrained -> report("not published: completion was accepted but its terminal catalog did not drain: " + finish.outcome)
        }

        val stats = sender.statsSnapshot()
        report("sent " + stats.objectsSent + ", queued " + stats.objectsQueued + ", dropped " + stats.objectsDropped)
    }

    private fun parseNamespace(text: String): Namespace =
        Namespace(
            text.split('/')
                .filter { it.isNotEmpty() }
                .map { NamespacePart(it.encodeToByteArray()) },
        )

    private companion object {
        val CONNECT_TIMEOUT = 10.seconds
        val WRITE_TIMEOUT = 1.seconds
        val DRAIN_TIMEOUT = 5.seconds
        val FRAME_INTERVAL = 33.milliseconds
        const val GOP = 30
        const val PROGRESS_EVERY = 30
        /** The declared MAXIMUM bitrate the catalog carries; nothing here measures or enforces it. */
        const val MAXIMUM_BITRATE = 2_000_000uL
        const val WIDTH = 1280
        const val HEIGHT = 720

        /** Synthetic parameter sets (nal_ref_idc 3, types 7 and 8), each after its start code. */
        val SYNTHETIC_SPS = byteArrayOf(0, 0, 0, 1, 0x67, 0x42, 0x00, 0x1e, 0xab.toByte(), 0xcd.toByte())
        val SYNTHETIC_PPS = byteArrayOf(0, 0, 0, 1, 0x68, 0xce.toByte(), 0x3c, 0x80.toByte())
        val CONFIG_BOUNDS = ConfigBounds(maxUnits = 2, maxUnitBytes = 64, maxTotalBytes = 128)

        /** One NAL unit per synthetic output; the converted payload is no larger than the largest output. */
        const val MAX_NAL_UNITS = 1
        const val MAX_OUTPUT_BYTES = SyntheticEncoderOutputs.DEFAULT_KEYFRAME_BYTES
    }
}

/**
 * The finite demonstration's publish loop over [source]: every output
 * through [pipeline], every admitted object to [sink] through the live
 * drain's own [writeOne], so each write is settled exactly once whatever it
 * answered or raised. [pace] runs after each output with the outputs seen and
 * objects written so far.
 *
 * The terminal rules are the live drain's. The track ends when the source has
 * nothing left, on an empty terminal, or AFTER a nonempty terminal's write
 * is settled -- nothing after the terminal is read. New work stops at a
 * cancellation: before each read and before `endTrack`, so an answer already
 * given is settled and nothing further starts. A refused step ends the track
 * without `endTrack`, and an `endTrack` the sink refuses is [TrackDrain.Unfinished],
 * never a successful end.
 */
internal suspend fun publishFinite(
    kind: MediaKind,
    source: EncodedOutputSource,
    pipeline: PublishPipeline,
    sink: TrackSink,
    writeTimeout: kotlin.time.Duration,
    report: (String) -> Unit,
    pace: suspend (seen: Int, sent: Int) -> Unit,
): TrackDrain {
    var written = 0
    var refused = 0
    var skipped = 0
    var seen = 0
    while (true) {
        kotlin.coroutines.coroutineContext.ensureActive()
        // The output and the instant it was captured arrive together: the
        // session never derives one from the other.
        val captured = source.next() ?: break
        seen++
        var terminal = false
        when (val step = pipeline.step(captured.output, captured.captureElapsedNanos)) {
            is PublishStep.Configure -> report("codec configuration: " + step.bytes.size + " byte(s)")
            PublishStep.EndTrack -> terminal = true
            // Nothing was admitted for it, and the track cannot continue:
            // no endTrack or complete is reported for a refused track.
            is PublishStep.Refused -> return TrackDrain.Unfinished(step.reason, written, refused, skipped)
            is PublishStep.Skipped -> {
                skipped++
                report("skipped a frame: " + step.reason)
            }
            is PublishStep.Write -> {
                when (writeOne(pipeline, sink, step.obj, writeTimeout)) {
                    WriteOutcome.Accepted -> written++
                    WriteOutcome.WouldBlock -> refused++
                }
                // A final payload is BOTH media and the terminal: written and
                // settled first, and nothing follows it.
                if (step.endsStream) terminal = true
            }
        }
        if (terminal) break
        pace(seen, written)
    }
    // The finite-track terminal is a new operation: a cancelled loop does not start one.
    kotlin.coroutines.coroutineContext.ensureActive()
    check(pipeline.mayEndTrack()) { "a write is still admitted; ending the track would publish after it" }
    val end = sink.endTrack(writeTimeout)
    return if (end == WriteOutcome.Accepted) {
        TrackDrain.Ended(written, refused, skipped)
    } else {
        TrackDrain.Unfinished("$kind: endTrack was refused ($end)", written, refused, skipped)
    }
}
