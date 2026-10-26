package org.openmoq.moq5.samples.player

import java.nio.ByteBuffer
import kotlinx.coroutines.CancellationException

/** Decoder output facts, not an assertion that the output is playable PCM. */
internal data class PlaybackOutputFormat(
    val mime: String,
    val sampleRate: Int? = null,
    val channels: Int? = null,
    val channelMask: Int? = null,
    val pcmEncoding: Int? = null,
    val width: Int? = null,
    val height: Int? = null,
)

/** One nonblocking platform dequeue. Buffer offsets/sizes refer to [Buffer.bytes]. */
internal sealed interface PlaybackCodecEvent {
    data object Idle : PlaybackCodecEvent
    class Format(val format: PlaybackOutputFormat) : PlaybackCodecEvent
    class Buffer(
        val index: Int,
        val bytes: ByteBuffer?,
        val offset: Int,
        val size: Int,
        val presentationUs: Long,
        val end: Boolean,
        val config: Boolean,
    ) : PlaybackCodecEvent
}

/** A configured, started synchronous codec. Every call belongs to one serial owner. */
internal interface PlaybackCodecBackend {
    /** -1 means no input is available; all other valid results are indices. */
    fun inputIndex(): Int
    fun inputBuffer(index: Int): ByteBuffer
    fun queueInput(index: Int, size: Int, presentationUs: Long, end: Boolean)
    fun output(): PlaybackCodecEvent
    /**
     * null discards; a timestamp renders to the decoder's Surface. With
     * MediaSync it is relative media time; on the video-only path it is the
     * display-clock deadline, which a SurfaceTexture records as the frame's
     * timestamp.
     */
    fun releaseOutput(index: Int, renderTimeNs: Long?)
    fun stop()
    fun release()
}

/**
 * A codec output borrow. Its exact identity authorizes one release on its
 * original driver, even if the codec later reuses the index or ByteBuffer.
 * [bytes] is the actual platform buffer, positioned/limited to the region;
 * consumers must not retain it beyond release or codec teardown.
 */
internal class PlaybackDecodedOutput internal constructor(
    internal val index: Int,
    val bytes: ByteBuffer?,
    val size: Int,
    val presentationUs: Long,
    val end: Boolean,
    val config: Boolean,
)

/**
 * Bounded ownership around Android's synchronous decoder API. This is not a
 * scheduler: the renderer interleaves nonblocking passes for both codecs and
 * supplies media timestamps. It must stop MediaSync's borrowing BEFORE close.
 * No method is concurrent or reentrant; no backend operation invokes a user
 * callback synchronously. Allocation-exhaustion recovery is not promised.
 */
internal class PlaybackCodec(
    private val backend: PlaybackCodecBackend,
    private val audio: Boolean,
    private val maxUnitBytes: Int = 8 * 1024 * 1024,
    private val maxOutputBorrows: Int = 32,
) {
    init {
        require(maxUnitBytes > 0 && maxOutputBorrows > 0)
    }

    internal sealed interface Output {
        data object Idle : Output
        class Format(val format: PlaybackOutputFormat) : Output
        class Buffer(val lease: PlaybackDecodedOutput) : Output
    }

    private val outputs = mutableMapOf<Int, PlaybackDecodedOutput>()
    private var inputEnded = false
    private var outputEnded = false
    private var closed = false
    private var failed: Throwable? = null
    private var cleanupFailure: Throwable? = null

    /** false owns nothing new; true means bytes were copied and queued. */
    fun offer(bytes: ByteArray, presentationUs: Long): Boolean {
        usable()
        check(!inputEnded) { "the decoder's input has ended" }
        require(bytes.isNotEmpty() && bytes.size <= maxUnitBytes) { "access unit exceeds the decoder input policy" }
        return platform {
            val index = backend.inputIndex()
            check(index >= -1) { "invalid decoder input index $index" }
            if (index < 0) return@platform false
            val buffer = backend.inputBuffer(index)
            check(bytes.size <= buffer.capacity()) { "access unit does not fit the decoder input buffer" }
            buffer.clear()
            buffer.put(bytes)
            backend.queueInput(index, bytes.size, presentationUs, false)
            true
        }
    }

    /** Explicit exhaustion of supplied input, never a receiver track-state inference. */
    fun endInput(presentationUs: Long): Boolean {
        usable()
        if (inputEnded) return true
        return platform {
            val index = backend.inputIndex()
            check(index >= -1) { "invalid decoder input index $index" }
            if (index < 0) return@platform false
            backend.queueInput(index, 0, presentationUs, true)
            inputEnded = true
            true
        }
    }

    fun poll(): Output {
        usable()
        if (outputEnded || outputs.size >= maxOutputBorrows) return Output.Idle
        return platform {
            when (val event = backend.output()) {
                PlaybackCodecEvent.Idle -> Output.Idle
                is PlaybackCodecEvent.Format -> Output.Format(event.format)
                is PlaybackCodecEvent.Buffer -> {
                    check(event.index >= 0 && !outputs.containsKey(event.index)) { "decoder returned an invalid or already borrowed output index" }
                    check(event.offset >= 0 && event.size >= 0) { "decoder returned a negative output region" }
                    val bytes = event.bytes
                    check(!audio || event.size == 0 || bytes != null) { "audio decoder returned no PCM buffer" }
                    if (bytes != null) {
                        check(event.offset <= bytes.capacity() && event.size <= bytes.capacity() - event.offset) { "decoder output region exceeds its buffer" }
                        bytes.limit(event.offset + event.size)
                        bytes.position(event.offset)
                    }
                    val lease = PlaybackDecodedOutput(event.index, bytes, event.size, event.presentationUs, event.end, event.config)
                    outputs[event.index] = lease
                    if (event.end) outputEnded = true
                    Output.Buffer(lease)
                }
            }
        }
    }

    /** Release only after the synchronizer has returned this exact borrow. */
    fun release(output: PlaybackDecodedOutput, renderTimeNs: Long? = null) {
        usable()
        require(outputs[output.index] === output) { "stale or foreign decoder output lease" }
        require(!audio || renderTimeNs == null) { "audio output is not rendered to a Surface" }
        platform {
            backend.releaseOutput(output.index, renderTimeNs)
            outputs.remove(output.index)
        }
    }

    /**
     * No synchronizer may still borrow these outputs. Always attempts release
     * after stop, even on Error/cancellation. Failure is sticky: a repeated
     * close rethrows the same result without retrying uncertain platform work.
     * A raised release is NOT proof that platform resources were disposed.
     */
    fun close() {
        if (closed) {
            cleanupFailure?.let { throw it }
            return
        }
        closed = true
        val stopped = attempt { backend.stop() }
        val released = attempt { backend.release() }
        outputs.clear()
        val first = stopped ?: released
        if (first != null && released != null && released !== first && first.suppressed.none { it === released }) first.addSuppressed(released)
        cleanupFailure = first
        first?.let { throw it }
    }

    private fun usable() {
        check(!closed) { "the decoder has closed" }
        failed?.let { throw IllegalStateException("the decoder failed; only cleanup is legal", it) }
    }

    private inline fun <T> platform(block: () -> T): T =
        try {
            block()
        } catch (cancelled: CancellationException) {
            failed = cancelled
            throw cancelled
        } catch (failure: Exception) {
            failed = failure
            throw failure
        } catch (error: Error) {
            failed = error
            throw error
        }

    // Synchronous cleanup aggregation; close rethrows the collected failure
    // only after both attempts, including when that failure is cancellation.
    private inline fun attempt(block: () -> Unit): Throwable? = runCatching(block).exceptionOrNull()
}
