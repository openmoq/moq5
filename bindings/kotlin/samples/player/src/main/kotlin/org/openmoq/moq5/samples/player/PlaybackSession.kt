package org.openmoq.moq5.samples.player

import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.first
import org.openmoq.moq5.EndpointConfig
import org.openmoq.moq5.MediaObject
import org.openmoq.moq5.MediaReceiver
import org.openmoq.moq5.MediaType
import org.openmoq.moq5.MediaTrack
import org.openmoq.moq5.MoqEndpoint
import org.openmoq.moq5.Namespace
import org.openmoq.moq5.NamespacePart
import org.openmoq.moq5.OverflowPolicy
import org.openmoq.moq5.ReceiverConfig
import org.openmoq.moq5.ReceiverInventory
import org.openmoq.moq5.ReceiverStats
import org.openmoq.moq5.ReceiverTerminal
import org.openmoq.moq5.TrackPresence
import kotlin.time.Duration.Companion.seconds

/**
 * Display owner supplies the serial looper and resource lifetime. It owns the
 * renderer BEFORE start, settles [consume] before closing it, and retains
 * failed-release resources and their borrowed display rather than dropping
 * them. Returning must not leave receive/pump work alive on that looper.
 */
internal fun interface PlaybackRenderHost {
    suspend fun run(
        video: FormatOutcome.Video, audio: FormatOutcome.Audio?,
        consume: suspend (PlaybackRenderPort, PlaybackFailure) -> PlaybackReadResult,
    ): PlaybackReadResult
}

/**
 * One playback session, written against the published Kotlin API and nothing
 * else: this sample is an acceptance test of the surface, so it does the
 * lifecycle itself rather than hiding it behind a helper.
 *
 * The lifetime -- connect, attach, run, and the two closes in order -- lives in
 * [playbackLifetime], so the ownership rules are stated once and can be
 * exercised directly rather than being spread across nested `finally` blocks.
 *
 * The caller supplies [report]; this class knows nothing about Android, and
 * [report] may be called from whatever thread the session runs on.
 */
internal class PlaybackSession(private val report: (String) -> Unit) {

    /** Render-capable public service path; the Activity's display owner supplies [host]. */
    suspend fun watchRendered(
        url: String, namespaceText: String, objectBudget: Int, host: PlaybackRenderHost,
        presentation: PlaybackPresentationAttempt? = null,
    ) {
        require(objectBudget > 0) { "object budget must be positive" }
        val namespace = parseNamespace(namespaceText)
        playbackLifetime(
            connect = { MoqEndpoint.connect(EndpointConfig(url), CONNECT_TIMEOUT) },
            describe = { report("established: " + it.snapshot().negotiatedVersion) },
            attach = { it.attachReceiver(playbackReceiverConfig(namespace)) },
            run = { _, receiver -> playRendered(receiver, objectBudget, host, presentation) },
            closeChild = { it.close() },
            closeEndpoint = { it.closeAndJoin() },
        )
    }

    private suspend fun playRendered(
        receiver: MediaReceiver, objectBudget: Int, host: PlaybackRenderHost, presentation: PlaybackPresentationAttempt?,
    ) {
        val diagnostic = if (PlaybackTrace.enabled) PlaybackDiagnostics.open { receiver.statsSnapshot() } else null
        try {
            playTraced(receiver, objectBudget, host, presentation)
        } finally {
            diagnostic?.let(PlaybackDiagnostics::close)
        }
    }

    private suspend fun playTraced(
        receiver: MediaReceiver, objectBudget: Int, host: PlaybackRenderHost, presentation: PlaybackPresentationAttempt?,
    ) {
        var chosen: PlaybackSelected<MediaTrack>? = null
        receiver.inventory.first { snapshot ->
            val terminal = snapshot.terminal
            if (terminal is ReceiverTerminal.Fatal) throw PlaybackTerminatedException(terminal)
            if (terminal != null) true
            else if (!snapshot.catalogReady) false
            else {
                chosen = selectPlayback(snapshot.tracks.map {
                    PlaybackCandidate(it, it.description, it.presence == TrackPresence.Present && !it.isRetired)
                })
                chosen != null
            }
        }
        val selected = chosen ?: return
        receiver.subscribe(selected.video)
        selected.audio?.let { receiver.subscribe(it) }
        val media = PlaybackReceiveMedia(selected.videoFormat, selected.audioFormat)
        renderPlayback(host, selected.videoFormat, selected.audioFormat, objectBudget, next = {
            val obj = receiver.objects.next()
            if (obj == null) null
            else {
                val lane = when {
                    obj.track === selected.video -> PlaybackLane.Video
                    obj.track === selected.audio -> PlaybackLane.Audio
                    else -> null
                }
                PlaybackTrace.mark { identityBreadcrumb(lane, obj.identity) }
                PlaybackReceived(lane, if (lane == null) emptyList() else media.map(
                    lane, obj.payload, obj.status, obj.keyframe, obj.presentationTime,
                    obj.decodeTime, obj.compositionOffset, obj.captureTime,
                ))
            }
        }, presentation = presentation)
    }

    /**
     * The render half of a session once tracks are selected: the host owns
     * the renderer and display, [next] supplies received objects, and an
     * optional acceptance [presentation] settles at decoder output EOS, before
     * the host closes anything, and finishes after the host has returned.
     */
    internal suspend fun renderPlayback(
        host: PlaybackRenderHost, video: FormatOutcome.Video, audio: FormatOutcome.Audio?, objectBudget: Int,
        next: suspend () -> PlaybackReceived?, presentation: PlaybackPresentationAttempt?,
        nowNs: () -> Long = System::nanoTime,
    ) {
        // Every outcome, cancellation included, is carried as data and rethrown
        // after the observer has closed; nothing here is swallowed.
        val played = runCatching {
            val outcome = host.run(video, audio) { render, failure ->
                val consumed = runCatching {
                    receivePlayback(
                        next = next, renderer = render, failure = failure, withAudio = audio != null,
                        objectBudget = objectBudget, nowNs = nowNs,
                        onDecoderEos = { eos, released -> presentation?.settle(eos, released) },
                    )
                }
                // Still inside the host: the renderer is owned, so its release count is real.
                if (presentation != null) {
                    runCatching { presentation.abandon(render.videoOutputsReleased) }.exceptionOrNull()
                        ?.let { behind(consumed.exceptionOrNull(), it) }
                }
                consumed.getOrThrow()
            }
            // Neither exit reason claims the MediaSync presentation tail was heard.
            report("stopped: ${outcome.end}; ${outcome.objects} objects, ${outcome.units} decoder inputs")
        }
        // After the host returned: the renderer and the display lease are closed.
        if (presentation != null) runCatching { presentation.finish() }.exceptionOrNull()?.let { behind(played.exceptionOrNull(), it) }
        played.getOrThrow()
    }

    /**
     * Connect, discover, subscribe to the first video track, and consume
     * objects until [objectBudget] have arrived.
     *
     * THE BUDGET IS A LIMIT, NOT COMPLETION. This sample does not stop when the
     * selected track finishes, because the public contract has no per-track
     * exhaustion fact to stop on: `objects` ends only with the RECEIVER's
     * terminal, and a per-track end event is not that (see
     * `PublicReceiverTrackCompletionTest`, where an object queued behind an
     * ENDED is still delivered). Closing on the event, on an empty queue or on
     * a quiet period would discard retained media, so the sample counts
     * instead and says so.
     *
     * Teardown is in a `finally`, so a cancelled caller still closes the
     * receiver and then the endpoint, in that order: the C endpoint refuses to
     * stop while a receiver is attached.
     */
    suspend fun watch(url: String, namespaceText: String, objectBudget: Int) {
        val namespace = parseNamespace(namespaceText)
        report("connecting to $url")
        playbackLifetime(
            connect = { MoqEndpoint.connect(EndpointConfig(url), CONNECT_TIMEOUT) },
            describe = { endpoint ->
                report("established: " + (endpoint.snapshot().negotiatedVersion?.toString() ?: "no version reported"))
            },
            attach = { endpoint -> endpoint.attachReceiver(playbackReceiverConfig(namespace)) },
            run = { _, receiver -> play(receiver, objectBudget) },
            closeChild = { receiver -> receiver.close() },
            closeEndpoint = { endpoint -> endpoint.closeAndJoin() },
        )
    }

    /** Discovery, subscription, and the pull loop. */
    private suspend fun play(receiver: MediaReceiver, objectBudget: Int) {
        // The catalog arrives asynchronously. Discovery is NOT just waiting for
        // `catalogReady`: every revision is judged, a terminal ends it either
        // way, and a ready-but-empty catalog keeps waiting because the receiver
        // is still live.
        report("waiting for the catalog")
        // Every inventory revision goes through the sample's own discovery
        // loop, which is what a test can drive; the loop returns the very
        // handle subscribed below, so the choice cannot drift.
        val video =
            discoverTrack(receiver.inventory.map { it.playbackFacts() }) { line -> report(line) } ?: return
        report("subscribing to " + (video.description.name.utf8OrNull() ?: video.description.name.toString()))
        receiver.subscribe(video)

        // The catalog's own description decides the decoder format, through
        // the same contract the tests drive. An unsupported track is a named
        // refusal here rather than a byte count that means nothing.
        val format = DecoderFormats.of(MediaFacts.ofTrack(video.description))
        if (format is FormatOutcome.Unsupported) {
            report("cannot play this track: " + format.reason)
            return
        }
        val decode = PlaybackDecode(format)

        var delivered = 0
        while (delivered < objectBudget) {
            // One pull, one native poll. A null answer is the RECEIVER's clean
            // terminal, not an empty queue and not the selected track ending:
            // an empty queue parks this coroutine until the endpoint next sees
            // activity.
            val obj = receiver.objects.next() ?: break
            if (obj.track !== video) continue
            delivered++
            // Every object goes through the accepted media contract: what this
            // sample counts is the access units a decoder would be queued, not
            // the bytes that arrived.
            decode.accept(objectFacts(obj))
            if (delivered % PROGRESS_EVERY == 0) report(delivered.toString() + " objects; " + decode.summary())
        }

        val stats = receiver.statsSnapshot()
        report(
            "done: " + delivered + " objects delivered; " + decode.summary() + "; the receiver saw " +
                stats.objectsReceived + " received, " + stats.objectsDropped + " dropped, " +
                stats.keyframesDropped + " keyframes dropped",
        )
    }

    /**
     * The one place a received object becomes the facts the media contract
     * reads. Everything downstream is a function of these values.
     */
    private fun objectFacts(obj: MediaObject): ObjectFacts =
        MediaFacts.ofObject(
            payload = obj.payload,
            status = obj.status,
            keyframe = obj.keyframe,
            presentationTime = obj.presentationTime,
            decodeTime = obj.decodeTime,
            compositionOffset = obj.compositionOffset,
            captureTime = obj.captureTime,
        )

    /** An observer failure never replaces the session's own; alone, it surfaces. */
    private fun behind(primary: Throwable?, observer: Throwable) {
        if (primary == null) throw observer
        if (primary !== observer && primary.suppressed.none { it === observer }) primary.addSuppressed(observer)
    }

    /**
     * A namespace is a sequence of byte fields, not a string: the wire carries
     * bytes and this sample keeps that honest by splitting on '/' and encoding
     * each part, rather than pretending a namespace is text.
     */
    private fun parseNamespace(text: String): Namespace =
        Namespace(
            text.split('/')
                .filter { it.isNotEmpty() }
                .map { NamespacePart(it.encodeToByteArray()) },
        )

    private companion object {
        val CONNECT_TIMEOUT = 10.seconds
        const val PROGRESS_EVERY = 30
    }
}
