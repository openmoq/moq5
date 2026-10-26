package org.openmoq.moq5.samples.player

import java.nio.ByteBuffer

/**
 * Audio decoder output borrowed by MediaSync. Confined to one renderer owner
 * thread, including consumption callbacks and stop; this class synchronizes
 * no platform calls. A codec index cannot be released or reused until its
 * matching callback, or until the owner has stopped the sync that borrowed it.
 *
 * Callback tokens differ from codec indices: the same index and ByteBuffer
 * may be reused for a later output. An old callback must never release it.
 * Tokens do not wrap. The count bound is an explicit application limit, not
 * a claim about how many output buffers an arbitrary decoder allocates.
 */
internal class PlaybackAudioBuffers(private val capacity: Int) {
    init {
        require(capacity > 0) { "the audio borrow capacity must be positive" }
    }

    private class Borrow(val codecIndex: Int, val buffer: ByteBuffer)

    private val pending = linkedMapOf<Int, Borrow>()
    private var nextId = 0L
    private var stopped = false

    /** Reserve before queueAudio; on a queue failure, stop the sync before reclaiming. */
    fun retain(codecIndex: Int, buffer: ByteBuffer): Int {
        require(codecIndex >= 0) { "negative audio codec output index" }
        check(!stopped) { "the audio sync already stopped" }
        check(pending.size < capacity) { "too many audio buffers borrowed by the sync" }
        check(pending.values.none { it.codecIndex == codecIndex }) { "audio codec output is already borrowed" }
        check(nextId <= Int.MAX_VALUE) { "audio buffer callback tokens exhausted" }
        val id = nextId.toInt()
        pending[id] = Borrow(codecIndex, buffer)
        nextId++
        return id
    }

    /**
     * Return the index to release, exactly once. Position/content changes are
     * legitimate; another ByteBuffer with equal content is not this borrow.
     * A queued callback after terminal stop has no codec operation to perform.
     */
    fun consumed(bufferId: Int, buffer: ByteBuffer): Int? {
        if (stopped) return null
        val borrow = checkNotNull(pending[bufferId]) { "unknown or already consumed audio buffer token" }
        check(borrow.buffer === buffer) { "audio consumption callback has the wrong buffer identity" }
        pending.remove(bufferId)
        return borrow.codecIndex
    }

    /**
     * Caller must FIRST stop/flush the sync and exclude further queue calls.
     * Flush discards borrows without callbacks, so return all remaining indices
     * to the owner rather than waiting for callbacks. The owner must attempt
     * every release even if one fails; this inventory calls no platform code.
     * This is terminal, not support for restarting codecs or resuming a flush.
     */
    fun afterSyncStopped(): List<Int> {
        if (stopped) return emptyList()
        val remaining = pending.values.map { it.codecIndex }
        stopped = true
        pending.clear()
        return remaining
    }
}
