package org.openmoq.moq5.samples.player

import java.nio.ByteBuffer
import kotlinx.coroutines.CancellationException

/** MediaSync operations; all calls and callbacks are on one serial owner. */
internal interface PlaybackSyncBackend {
    fun queueAudio(bytes: ByteBuffer, token: Int, presentationUs: Long)
    fun flush()
    fun release()
}

internal interface PlaybackSyncCallbacks {
    fun onAudioConsumed(token: Int, buffer: ByteBuffer)
    fun onFailure(cause: Throwable)
}

/**
 * [quiesced] proves return of MediaSync PCM borrows, not successful native
 * disposal or audible completion. If false, the renderer MUST retain affected
 * codecs. [released] separately confirms sync release; until true, resources
 * still attached to that sync (notably AudioTrack) must remain retained.
 * [failure] is cleanup's result; the operation's original [PlaybackSync.failure]
 * remains separately owned by the caller.
 */
internal class PlaybackSyncClosed(val quiesced: Boolean, val released: Boolean, val failure: Throwable?)

/**
 * Queued PCM ownership for the real MediaSync renderer. No reentrant backend
 * or consumer calls; no concurrent methods; no allocation-exhaustion promise.
 * [wakeFailure] must be nonthrowing and wake the owner even when it is waiting
 * for receiver input. It is called once for the first causal failure, not a
 * substitute for polling a status flag. Callbacks never throw codec failures
 * back into Android's looper.
 */
internal class PlaybackSync(
    private val backend: PlaybackSyncBackend,
    private val maxBuffers: Int,
    private val maxBytes: Int,
    private val releaseAudio: (PlaybackDecodedOutput) -> Unit,
    private val wakeFailure: (Throwable) -> Unit,
) : PlaybackSyncCallbacks {
    init { require(maxBuffers > 0 && maxBytes > 0) }

    private val borrows = PlaybackAudioBuffers(maxBuffers)
    private val pending = linkedMapOf<Int, PlaybackDecodedOutput>()
    private var pendingBytes = 0L
    private var closed = false
    private var closeResult: PlaybackSyncClosed? = null
    var failure: Throwable? = null
        private set

    /**
     * Diagnostic accounting of this owner's own PCM traffic, by identity: buffers the platform
     * accepted into MediaSync ([submitted]), buffers MediaSync returned by callback ([returned]), and
     * leases returned by [close]'s flush instead ([flushedAtClose]). None of these is acoustic
     * evidence, and none is compared to a count of compressed access units: one AAC unit need not
     * decode to one PCM buffer. With the trace on, each event is also a breadcrumb naming its token
     * and decoder index.
     */
    var submitted: Int = 0
        private set
    var submittedBytes: Long = 0
        private set
    var returned: Int = 0
        private set
    var returnedBytes: Long = 0
        private set
    var flushedAtClose: Int = 0
        private set

    /**
     * false is ordinary capacity refusal and owns none of [output]. Once the
     * platform queue is attempted, even a throw may have adopted the buffer:
     * only consumption or quiescence permits its decoder lease to return.
     */
    fun queueAudio(output: PlaybackDecodedOutput, presentationUs: Long): Boolean {
        check(!closed) { "the playback sync has closed" }
        failure?.let { throw IllegalStateException("the playback sync failed; only cleanup is legal", it) }
        require(presentationUs >= 0) { "MediaSync requires a nonnegative media timestamp" }
        require(output.size in 1..maxBytes && !output.config) { "not a bounded nonempty PCM output" }
        val buffer = requireNotNull(output.bytes) { "PCM output has no buffer" }
        require(buffer.remaining() == output.size) { "PCM output region changed before queueing" }
        require(!pending.containsKey(output.index)) { "this decoder output is already queued" }
        if (pending.size >= maxBuffers || pendingBytes + output.size > maxBytes) return false
        val token = borrows.retain(output.index, buffer)
        pending[output.index] = output
        pendingBytes += output.size
        try {
            backend.queueAudio(buffer, token, presentationUs)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (raised: Exception) {
            onFailure(raised)
            throw raised
        } catch (raised: Error) {
            onFailure(raised)
            throw raised
        }
        submitted++
        submittedBytes += output.size
        PlaybackTrace.mark { "pcm-submit token=$token index=${output.index} bytes=${output.size} pts=$presentationUs" }
        return true
    }

    override fun onAudioConsumed(token: Int, buffer: ByteBuffer) {
        if (closed) return
        val raised = runCatching {
            val index = checkNotNull(borrows.consumed(token, buffer)) { "sync returned a buffer after its inventory stopped" }
            val lease = checkNotNull(pending.remove(index)) { "sync returned an unknown decoder lease" }
            pendingBytes -= lease.size
            returned++
            returnedBytes += lease.size
            PlaybackTrace.mark { "pcm-returned token=$token index=$index bytes=${lease.size}" }
            releaseAudio(lease)
        }.exceptionOrNull()
        if (raised != null) onFailure(raised)
    }

    override fun onFailure(cause: Throwable) {
        if (closed || failure != null) return
        failure = cause
        wakeFailure(cause)
    }

    /** Terminal and idempotent. Caller must inspect quiesced before disposal. */
    fun close(): PlaybackSyncClosed {
        closeResult?.let { return it }
        check(!closed) { "reentrant sync close" }
        closed = true
        val flushed = runCatching { backend.flush() }.exceptionOrNull()
        val released = runCatching { backend.release() }.exceptionOrNull()
        val quiesced = flushed == null || released == null
        var first = flushed ?: released
        fun keep(next: Throwable?) {
            if (next == null) return
            val primary = first
            if (primary == null) first = next
            else if (next !== primary && primary.suppressed.none { it === next }) primary.addSuppressed(next)
        }
        keep(released)
        if (quiesced) {
            // Flush explicitly discards without callbacks. Return every remaining
            // lease once, even when a decoder release raises cancellation/Error.
            for (index in borrows.afterSyncStopped()) {
                val lease = checkNotNull(pending.remove(index)) { "audio inventory lost its decoder lease" }
                pendingBytes -= lease.size
                flushedAtClose++
                PlaybackTrace.mark { "pcm-flushed-at-close index=$index bytes=${lease.size}" }
                keep(runCatching { releaseAudio(lease) }.exceptionOrNull())
            }
        }
        return PlaybackSyncClosed(quiesced, released == null, first).also { closeResult = it }
    }
}
