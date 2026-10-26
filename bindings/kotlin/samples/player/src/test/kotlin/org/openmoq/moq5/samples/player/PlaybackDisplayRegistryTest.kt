package org.openmoq.moq5.samples.player

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

class PlaybackDisplayRegistryTest {
    private class Rig {
        val log = mutableListOf<String>()
        val texture = Any()
        val surface = Any()
        var surfaceFailure: Throwable? = null
        var textureFailure: Throwable? = null
        val registry = PlaybackDisplayRegistry<Any, Any>(
            createSurface = { assertSame(texture, it); log += "makeSurface"; surface },
            releaseSurface = { assertSame(surface, it); log += "releaseSurface"; surfaceFailure?.let { e -> throw e } },
            releaseTransferredTexture = { assertSame(texture, it); log += "releaseTexture"; textureFailure?.let { e -> throw e } },
        )
    }

    @Test fun noLeaseLeavesTextureViewAsOwner() {
        val r = Rig()
        r.registry.available(r.texture)
        assertTrue(r.registry.destroyed(r.texture))
        assertEquals(emptyList(), r.log)
        assertFailsWith<IllegalStateException> { r.registry.acquire() }
    }

    @Test fun destroyedDuringLeaseTransfersTextureUntilSurfaceCloses() {
        val r = Rig()
        r.registry.available(r.texture)
        val lease = r.registry.acquire()
        assertSame(r.surface, lease.surface)
        assertFalse(r.registry.destroyed(r.texture))
        assertEquals(listOf("makeSurface"), r.log)
        lease.close()
        assertEquals(listOf("makeSurface", "releaseSurface", "releaseTexture"), r.log)
        assertFailsWith<IllegalStateException> { r.registry.acquire() }
    }

    @Test fun closeBeforeDestructionReturnsTextureToFramework() {
        val r = Rig()
        r.registry.available(r.texture)
        r.registry.acquire().close()
        assertEquals(listOf("makeSurface", "releaseSurface"), r.log)
        assertTrue(r.registry.destroyed(r.texture))
        assertEquals(0, r.log.count { it == "releaseTexture" })
    }

    @Test fun aCleanlyReturnedAvailableTextureCanBeBorrowedAgain() {
        val r = Rig()
        r.registry.available(r.texture)
        val first = r.registry.acquire()
        first.close()
        val second = r.registry.acquire()
        assertFailsWith<IllegalStateException> { first.close() }
        second.close()
        assertEquals(2, r.log.count { it == "makeSurface" })
        assertEquals(2, r.log.count { it == "releaseSurface" })
        assertEquals(0, r.log.count { it == "releaseTexture" })
    }

    @Test fun surfaceFailureRetainsTextureAndCannotBeRetried() {
        val r = Rig()
        r.registry.available(r.texture)
        val lease = r.registry.acquire()
        assertFalse(r.registry.destroyed(r.texture))
        val cause = IllegalStateException("native Surface release failed")
        r.surfaceFailure = cause
        assertSame(cause, runCatching { lease.close() }.exceptionOrNull())
        assertEquals(0, r.log.count { it == "releaseTexture" })
        assertFailsWith<IllegalStateException> { r.registry.acquire() }
        assertEquals(1, r.log.count { it == "releaseSurface" })
    }

    @Test fun transferredTextureFailureRemainsAnOwnedLease() {
        val r = Rig()
        r.registry.available(r.texture)
        val lease = r.registry.acquire()
        assertFalse(r.registry.destroyed(r.texture))
        val cause = IllegalStateException("SurfaceTexture release failed")
        r.textureFailure = cause
        assertSame(cause, runCatching { lease.close() }.exceptionOrNull())
        assertFailsWith<IllegalStateException> { r.registry.acquire() }
        assertEquals(1, r.log.count { it == "releaseTexture" })
    }

    /** Several textures, each with a counted surface and counted releases. */
    private class Multi {
        val made = mutableListOf<Pair<Any, Any>>()
        val surfaceReleases = mutableListOf<Any>()
        val textureReleases = mutableListOf<Any>()
        val registry = PlaybackDisplayRegistry<Any, Any>(
            createSurface = { t -> Any().also { made += t to it } },
            releaseSurface = { surfaceReleases += it },
            releaseTransferredTexture = { textureReleases += it },
        )
    }

    @Test fun leaseNamesItsAnnouncedTextureWithIncreasingIdsAndPerEntryOrdinals() {
        val m = Multi()
        val t1 = Any()
        m.registry.available(t1)
        val a = m.registry.acquire()
        assertSame(t1, a.texture)
        assertEquals(1, a.ordinal)
        assertEquals(a.leaseId, m.registry.leaseIdFor(t1))
        a.close()
        assertEquals(null, m.registry.leaseIdFor(t1), "a closed lease holds nothing")
        val b = m.registry.acquire()
        assertSame(t1, b.texture)
        assertTrue(b.leaseId > a.leaseId, "lease ids strictly increase")
        assertEquals(2, b.ordinal, "the same entry leased again")
        assertEquals(b.leaseId, m.registry.leaseIdFor(t1))
        b.close()
        assertEquals(listOf(m.made[0].second, m.made[1].second), m.surfaceReleases, "each lease releases its own surface once")
        assertEquals(emptyList(), m.textureReleases)
    }

    @Test fun reannouncedSameTextureResetsItsOrdinalButNeverItsLeaseId() {
        // The registry's per-entry ordinal is not provenance: a destroyed and
        // re-announced identity starts again at 1. The id still moves forward.
        val m = Multi()
        val t = Any()
        m.registry.available(t)
        val first = m.registry.acquire()
        first.close()
        assertTrue(m.registry.destroyed(t))
        m.registry.available(t)
        val again = m.registry.acquire()
        assertSame(t, again.texture)
        assertEquals(1, again.ordinal)
        assertTrue(again.leaseId > first.leaseId)
        assertEquals(again.leaseId, m.registry.leaseIdFor(t))
        again.close()
        assertEquals(null, m.registry.leaseIdFor(t))
    }

    @Test fun replacementWhileAnOlderLeaseIsRetainedKeepsIdsApartAndReleasesOnce() {
        val m = Multi()
        val t1 = Any()
        val t2 = Any()
        m.registry.available(t1)
        val old = m.registry.acquire()
        assertFalse(m.registry.destroyed(t1))
        assertEquals(old.leaseId, m.registry.leaseIdFor(t1), "a destroyed but still-leased texture keeps its lease")
        m.registry.available(t2)
        val fresh = m.registry.acquire()
        assertSame(t2, fresh.texture)
        assertEquals(1, fresh.ordinal)
        assertTrue(fresh.leaseId > old.leaseId)
        assertEquals(fresh.leaseId, m.registry.leaseIdFor(t2))
        assertEquals(old.leaseId, m.registry.leaseIdFor(t1))
        old.close()
        assertEquals(null, m.registry.leaseIdFor(t1))
        assertEquals(listOf(t1), m.textureReleases, "the transferred texture is released exactly once")
        assertFailsWith<IllegalStateException> { old.close() }
        assertEquals(listOf(t1), m.textureReleases)
        fresh.close()
        assertEquals(2, m.surfaceReleases.size)
    }

    @Test fun unknownOrNeverLeasedTextureHasNoLease() {
        val m = Multi()
        val t = Any()
        assertEquals(null, m.registry.leaseIdFor(t))
        m.registry.available(t)
        assertEquals(null, m.registry.leaseIdFor(t))
    }

    @Test fun newerTextureDoesNotStealAnOlderLease() {
        val r = Rig()
        r.registry.available(r.texture)
        val old = r.registry.acquire()
        assertFalse(r.registry.destroyed(r.texture))
        val fresh = Any()
        r.registry.available(fresh)
        assertTrue(r.registry.destroyed(fresh))
        old.close()
        assertEquals(listOf("makeSurface", "releaseSurface", "releaseTexture"), r.log)
    }
}
