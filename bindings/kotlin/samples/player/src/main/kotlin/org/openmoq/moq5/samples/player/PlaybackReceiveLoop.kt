package org.openmoq.moq5.samples.player

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.yield
import java.util.concurrent.atomic.AtomicReference

/** Callback failure is a wake, not a flag requiring transport traffic to observe. */
internal class PlaybackFailure {
    internal val signal = CompletableDeferred<Throwable>()
    private val first = AtomicReference<Throwable?>()
    fun record(cause: Throwable) {
        if (first.compareAndSet(null, cause)) signal.complete(cause)
    }
    fun current(): Throwable? = first.get()
}

/** Methods are called only on the enclosing renderer's serial owner dispatcher. */
internal interface PlaybackRenderPort {
    fun offer(lane: PlaybackLane, unit: AccessUnitOutcome.Unit, logicalUs: Long): PlaybackOffer
    fun pump(): Boolean
    fun endInputs()
    val decoderOutputsEnded: Boolean
    /** Successfully released, rendered video outputs so far. */
    val videoOutputsReleased: Int
}

internal data class PlaybackReceived(val lane: PlaybackLane?, val units: List<PlaybackTimedUnit>)
internal enum class PlaybackReadEnd { ObjectLimit, ReceiverEnded }
internal data class PlaybackReadResult(val end: PlaybackReadEnd, val objects: Int, val units: Int)

// Coroutine debug stack recovery may copy standard exceptions when a nested
// scope rethrows them. Carry the owned cause through those scopes as data and
// throw it only AFTER the final scope has returned its Result.
private class PlaybackWorkFailure(val original: Throwable) : RuntimeException(null, original)
private class PlaybackReadStop(val ticket: Any = Any()) : CancellationException("playback read selection settled")
private fun Throwable.playbackOriginal(): Throwable = if (this is PlaybackWorkFailure) original else this
private inline fun <T> playbackWork(block: () -> T): T = try {
    block()
} catch (cancelled: CancellationException) {
    throw PlaybackWorkFailure(cancelled)
} catch (raised: Exception) {
    throw if (raised is PlaybackWorkFailure) raised else PlaybackWorkFailure(raised)
} catch (raised: Error) {
    throw PlaybackWorkFailure(raised)
}

/**
 * One public read at a time, with no prefetch queue. Decoder work is serviced
 * on this SAME serial dispatcher in finite passes; only codec mechanics use
 * a timer. Callback failure races a suspended read directly. Return means
 * the reader and pump have both settled, so the caller may close its renderer.
 * Either end -- the receiver's stream ending or the caller's exact object
 * budget -- closes decoder input and waits, under the same bound, for decoder
 * output EOS, so the last admitted objects are decoded before that close.
 * ObjectLimit claims nothing about the receiver; neither end proves
 * presentation completion.
 */
internal suspend fun receivePlayback(
    next: suspend () -> PlaybackReceived?, renderer: PlaybackRenderPort, failure: PlaybackFailure,
    withAudio: Boolean, objectBudget: Int, waitLimitMs: Long = 10_000,
    nowNs: () -> Long = System::nanoTime,
    onDecoderEos: suspend (decoderEosNs: Long, released: Int) -> Unit = { _, _ -> },
): PlaybackReadResult {
    // Receipts must outlive the cancellable scope: it may replace a returned
    // Result with the caller's cancellation while leaving the scope.
    var readCleanup: Throwable? = null
    val result = runCatching { supervisorScope {
        require(objectBudget > 0 && waitLimitMs > 0)
        var objects = 0
        var units = 0
        var videoStarted = false
        var audioStarted = !withAudio
        // Expiry is a causal wake, not inherited cancellation of the reader.
        // That reader is cancelled explicitly, once, with its owner's stop ticket.
        val startupDeadline = launch {
            delay(waitLimitMs)
            failure.record(PlaybackRenderRefused("selected tracks did not supply startup media within the sample's wait bound"))
        }
        val pump = launch {
            while (true) {
                val result = runCatching { renderer.pump() }
                result.exceptionOrNull()?.let { failure.record(it); return@launch }
                delay(2)
            }
        }
        suspend fun one(): PlaybackReadEnd? {
            failure.current()?.let { throw PlaybackWorkFailure(it) }
            if (objects == objectBudget) return PlaybackReadEnd.ObjectLimit
            PlaybackTrace.mark { "read-wait objects=$objects" }
            val packet = playbackRead(next, failure) { readCleanup = it }
            if (packet == null) {
                PlaybackTrace.mark { "read-end receiver objects=$objects" }
                return PlaybackReadEnd.ReceiverEnded
            }
            val lane = packet.lane ?: return null
            objects++
            PlaybackTrace.mark { "read objects=$objects lane=$lane units=${packet.units.size}" }
            val accepted = withTimeoutOrNull(waitLimitMs) {
                for (timed in packet.units) {
                    var offered: PlaybackOffer
                    do {
                        failure.current()?.let { throw PlaybackWorkFailure(it) }
                        offered = playbackWork { renderer.offer(lane, timed.unit, timed.logicalUs) }
                        if (offered == PlaybackOffer.Busy) delay(2)
                    } while (offered == PlaybackOffer.Busy)
                    if (offered == PlaybackOffer.Queued) {
                        if (units == Int.MAX_VALUE) throw PlaybackRenderRefused("decoded unit counter exhausted")
                        units++
                        PlaybackTrace.mark { "queued units=$units lane=$lane" }
                        when (lane) {
                            PlaybackLane.Video -> videoStarted = true
                            PlaybackLane.Audio -> audioStarted = true
                        }
                        if (videoStarted && audioStarted) startupDeadline.cancel()
                    }
                }
                true
            }
            if (accepted == null) throw PlaybackRenderRefused("decoder admission did not progress within the sample's wait bound")
            // Even an immediately available stream cannot monopolize the looper.
            yield()
            return null
        }
        val bodyResult = runCatching {
            var end: PlaybackReadEnd? = null
            while (!videoStarted || !audioStarted) {
                end = one()
                if (end != null) break
            }
            withContext(NonCancellable) { startupDeadline.cancel(); startupDeadline.join() }
            while (end == null) end = one()
            PlaybackTrace.mark { "end $end objects=$objects units=$units" }
            renderer.endInputs()
            PlaybackTrace.mark { "eos-wait enter" }
            val decoderEosNs = withTimeoutOrNull(waitLimitMs) {
                while (!playbackWork { renderer.decoderOutputsEnded }) {
                    failure.current()?.let { throw PlaybackWorkFailure(it) }
                    delay(2)
                }
                nowNs()
            }
            PlaybackTrace.mark { "eos-wait exit " + if (decoderEosNs == null) "timeout" else "ended" }
            if (decoderEosNs == null) throw PlaybackRenderRefused("decoder EOS did not arrive within the sample's wait bound")
            failure.current()?.let { throw it }
            val released = playbackWork { renderer.videoOutputsReleased }
            playbackWork { onDecoderEos(decoderEosNs, released) }
            failure.current()?.let { throw it }
            PlaybackReadResult(checkNotNull(end), objects, units)
        }
        withContext(NonCancellable) {
            startupDeadline.cancel(); startupDeadline.join()
            pump.cancel(); pump.join()
        }
        bodyResult
    } }.fold({ it }, { Result.failure(it) })
    val cause = result.exceptionOrNull()?.playbackOriginal()
    val callback = failure.current()
    if (cause != null) {
        if (callback != null && callback !== cause && cause.suppressed.none { it === callback }) cause.addSuppressed(callback)
        readCleanup?.let { if (it !== cause && cause.suppressed.none { s -> s === it }) cause.addSuppressed(it) }
        throw cause
    }
    callback?.let { throw it }
    return result.getOrThrow()
}

/** Preserve a read's exceptional cleanup even when another cause wins select. */
private suspend fun <T> playbackRead(next: suspend () -> T, failure: PlaybackFailure, cleanupFailure: (Throwable) -> Unit): T = supervisorScope {
    var receipt: Result<T>? = null
    val read = async { runCatching { next() }.also { receipt = it } }
    val result = runCatching {
        select {
            failure.signal.onAwait { Result.failure<T>(it) }
            read.onAwait { it }
        }
    }.fold({ it }, { Result.failure(it) })
    val stop = PlaybackReadStop()
    withContext(NonCancellable) { read.cancel(stop); read.join() }
    val cleanup = receipt?.exceptionOrNull()
    val primary = result.exceptionOrNull()
    val ownStop = cleanup is PlaybackReadStop && cleanup.ticket === stop.ticket
    if (cleanup != null && !ownStop && cleanup !== primary) {
        cleanupFailure(cleanup)
        if (primary == null) return@supervisorScope Result.failure(cleanup)
        if (primary.suppressed.none { it === cleanup }) primary.addSuppressed(cleanup)
    }
    result
}.getOrElse { throw PlaybackWorkFailure(it) }
