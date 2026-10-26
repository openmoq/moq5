package org.openmoq.moq5.samples.player

import java.util.IdentityHashMap

/**
 * A display lease that also names the image it borrows: [leaseId] is unique
 * and increasing across this registry, and [ordinal] counts leases of this
 * texture's entry since it was announced (1 for the first).
 */
internal interface PlaybackDisplayLease<out T : Any, out S : Any> : PlaybackDisplay<S> {
    val texture: T
    val leaseId: Long
    val ordinal: Int
}

/** Pure ownership protocol for a TextureView-owned image and its borrowed render Surface. */
internal class PlaybackDisplayRegistry<T : Any, S : Any>(
    private val createSurface: (T) -> S,
    private val releaseSurface: (S) -> Unit,
    private val releaseTransferredTexture: (T) -> Unit,
) {
    private inner class Entry(val texture: T) {
        var destroyed = false
        var leased = false
        var leases = 0
        var leaseId: Long? = null
    }

    private val lock = Any()
    private val entries = IdentityHashMap<T, Entry>()
    private var current: Entry? = null
    private var lastLeaseId = 0L

    fun available(texture: T) {
        synchronized(lock) {
            check(entries[texture] == null) { "a TextureView image was announced twice" }
            check(current?.destroyed != false) { "the previous TextureView image is still available" }
            Entry(texture).also { entries[texture] = it; current = it }
        }
    }

    /** True returns an unused texture to the framework; false transfers release to this registry. */
    fun destroyed(texture: T): Boolean = synchronized(lock) {
        val entry = entries[texture] ?: return@synchronized true
        entry.destroyed = true
        if (current === entry) current = null
        if (entry.leased) false
        else {
            entries.remove(texture)
            true
        }
    }

    /** The id of the lease currently holding [texture], or null when none does. */
    fun leaseIdFor(texture: T): Long? = synchronized(lock) { entries[texture]?.takeIf { it.leased }?.leaseId }

    fun acquire(): PlaybackDisplayLease<T, S> = synchronized(lock) {
        val entry = checkNotNull(current) { "the display SurfaceTexture is not available" }
        check(!entry.destroyed && !entry.leased) {
            "the display SurfaceTexture is already borrowed or retired"
        }
        val surface = createSurface(entry.texture)
        entry.leased = true
        entry.leases++
        val id = ++lastLeaseId
        entry.leaseId = id
        val count = entry.leases
        object : PlaybackDisplayLease<T, S> {
            override val surface: S = surface
            override val texture: T = entry.texture
            override val leaseId: Long = id
            override val ordinal: Int = count
            private var closeAttempted = false

            override fun close() {
                synchronized(lock) {
                    check(!closeAttempted) { "display lease close is one-shot" }
                    closeAttempted = true
                    releaseSurface(surface)
                    if (entry.destroyed) {
                        releaseTransferredTexture(entry.texture)
                        entries.remove(entry.texture)
                    }
                    entry.leased = false
                    entry.leaseId = null
                }
            }
        }
    }
}
