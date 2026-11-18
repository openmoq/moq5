package org.openmoq.moq5.samples.player

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext

internal interface ManagedPlaybackRender : PlaybackRenderPort {
    fun start()
    fun close(): PlaybackRenderClosed
}

/** Construction has no effects; even a failed start is followed by stop/join. */
internal interface PlaybackRenderWorker {
    val dispatcher: CoroutineDispatcher
    fun start()
    suspend fun stopAndJoin(): Boolean
}

/** The display's owner is responsible for partial failure before returning a lease. */
internal interface PlaybackDisplay<out S : Any> {
    val surface: S
    fun close()
}

internal class PlaybackRuntimeGate {
    internal class Attempt {
        var worker: PlaybackRenderWorker? = null
        var display: PlaybackDisplay<*>? = null
        var renderer: ManagedPlaybackRender? = null
    }

    private var current: Attempt? = null
    @Synchronized internal fun claim(): Attempt {
        check(current == null) { "a player render attempt is active or retained after uncertain cleanup" }
        return Attempt().also { current = it }
    }
    @Synchronized internal fun release(attempt: Attempt) {
        check(current === attempt) { "foreign renderer retirement" }
        current = null
    }
    @Synchronized internal fun retained(): Boolean = current != null
}

/**
 * One render attempt. Constructors supplied here have no platform effects;
 * start and display acquisition run only after the attempt has an owner.
 * Unproved disposal retains the whole attempt in the gate, which refuses any
 * replacement before effects. This intentionally bounds quarantine to one
 * attempt per gate; native operations of uncertain outcome are never retried.
 */
internal suspend fun <S : Any> runOwnedPlayback(
    gate: PlaybackRuntimeGate,
    createWorker: () -> PlaybackRenderWorker,
    acquireDisplay: () -> PlaybackDisplay<S>,
    createRenderer: (S, (Throwable) -> Unit) -> ManagedPlaybackRender,
    consume: suspend (PlaybackRenderPort, PlaybackFailure) -> PlaybackReadResult,
    onDisplayOwned: (PlaybackDisplay<S>) -> Unit = {},
    onReleased: () -> Unit = {},
): PlaybackReadResult {
    val attempt = gate.claim()
    val failure = PlaybackFailure()
    var work: Result<PlaybackReadResult>? = null
    val escaped = runCatching {
        currentCoroutineContext().ensureActive()
        val worker = createWorker().also { attempt.worker = it }
        worker.start()
        withContext(worker.dispatcher) {
            // Retain the actual result outside the scope: caller cancellation
            // can replace the scope's returned value while dispatching back.
            work = runCatching {
                val display = acquireDisplay().also { attempt.display = it }
                onDisplayOwned(display)
                val renderer = createRenderer(display.surface, failure::record).also { attempt.renderer = it }
                PlaybackTrace.around("renderer start") { renderer.start() }
                PlaybackTrace.around("consume") { consume(renderer, failure) }
            }
        }
    }.exceptionOrNull()
    var primary = escaped ?: work?.exceptionOrNull()
    fun record(cause: Throwable?) {
        if (cause == null) return
        val first = primary
        if (first == null) primary = cause
        else if (first !== cause && first.suppressed.none { it === cause }) first.addSuppressed(cause)
    }
    record(work?.exceptionOrNull())
    var settled = false
    val cleanupFailure = runCatching {
        withContext(NonCancellable) {
            val worker = attempt.worker
            val renderer = attempt.renderer
            var released = renderer == null
            if (renderer != null) {
                val close = runCatching {
                    withContext(checkNotNull(worker).dispatcher) { PlaybackTrace.around("renderer close") { renderer.close() } }
                }
                record(close.exceptionOrNull())
                close.getOrNull()?.let {
                    record(it.failure)
                    released = it.retained.isEmpty()
                }
            }
            if (released) {
                val joined = runCatching { PlaybackTrace.around("worker stopAndJoin") { worker?.stopAndJoin() ?: true } }
                record(joined.exceptionOrNull())
                if (joined.getOrNull() == true) {
                    val displayClose = runCatching { PlaybackTrace.around("display close") { attempt.display?.close() } }
                    record(displayClose.exceptionOrNull())
                    settled = displayClose.isSuccess
                }
            }
        }
    }.exceptionOrNull()
    record(cleanupFailure)
    record(failure.current())
    if (settled) {
        gate.release(attempt)
        // A fact for the caller, only once release is proved: not cleanup, and
        // a throw here is recorded without undoing the release.
        record(runCatching { onReleased() }.exceptionOrNull())
    } else record(IllegalStateException("renderer resources, worker or display remain retained after uncertain cleanup"))
    primary?.let { throw it }
    return checkNotNull(work).getOrThrow()
}
