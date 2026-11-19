package org.openmoq.moq5.samples.showcase

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class AudioPolicyTest {
    private class Platform(var grant: Boolean = true) : AudioFocusPlatform {
        var requests = 0
        var abandons = 0
        val lost = mutableListOf<(Boolean) -> Unit>()
        var noisy: (() -> Unit)? = null

        override fun request(lost: (transient: Boolean) -> Unit): Boolean {
            requests++
            this.lost += lost
            return grant
        }

        override fun abandon() {
            abandons++
        }

        override fun listenNoisy(noisy: () -> Unit) {
            this.noisy = noisy
        }

        override fun stopListeningNoisy() {
            noisy = null
        }
    }

    private class Rig(grant: Boolean = true) {
        val platform = Platform(grant)
        val interrupted = mutableListOf<String>()
        val policy = AudioPolicy(platform) { interrupted += it }
    }

    @Test fun `a grant is held, its loss and noisy output interrupt, and release gives back exactly that`() {
        val r = Rig()
        assertTrue(r.policy.acquire())
        assertTrue(r.policy.holding)
        r.platform.lost.single()(true)
        r.platform.noisy!!()
        assertEquals(listOf(AudioPolicy.INTERRUPTED_TRANSIENT, AudioPolicy.NOISY), r.interrupted)
        r.policy.release()
        assertFalse(r.policy.holding)
        assertNull(r.platform.noisy)
        assertEquals(1, r.platform.abandons)
        assertTrue(r.policy.acquire(), "after a release, the next session asks again")
        assertEquals(2, r.platform.requests)
    }

    @Test fun `a refusal is not a grant - nothing is held, listened to or given back`() {
        val r = Rig(grant = false)
        assertFalse(r.policy.acquire())
        assertFalse(r.policy.holding)
        assertNull(r.platform.noisy)
        r.policy.release()
        assertEquals(0, r.platform.abandons)
        r.platform.grant = true
        assertTrue(r.policy.acquire(), "a later request can still be granted")
    }

    @Test fun `a callback from an earlier grant is stale and interrupts nothing`() {
        val r = Rig()
        r.policy.acquire()
        val first = r.platform.lost.single()
        val firstNoisy = r.platform.noisy!!
        r.policy.release()
        r.policy.acquire()
        first(false)
        firstNoisy()
        assertEquals(emptyList(), r.interrupted)
        r.platform.lost.last()(false)
        assertEquals(listOf(AudioPolicy.INTERRUPTED), r.interrupted)
    }
}
