package org.openmoq.moq5.jni

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A registry token must survive the ACTUAL crossing:
 * C `uint64_t` -> JNI `jlong` -> Kotlin `Long` -> JNI `jlong` -> C `uint64_t`.
 *
 * A C-side cast proves nothing about that path. The token is minted by the
 * production registry, so what crosses is what the product would mint --
 * including the set high bit that makes an ordinary token NEGATIVE as a `Long`,
 * which is exactly where a narrowing or signed-comparison implementation breaks.
 */
class RegistryRoundTripTest {

    private fun load() {
        val dir = File("../.native/_bridge").absoluteFile
        val lib = File(dir, "libregistry_roundtrip.dylib")
        assertTrue(lib.isFile, "the registry round-trip fixture is not built: $lib")
        System.load(lib.absolutePath)
    }

    @Test
    fun `a real token survives the JNI round trip byte for byte`() {
        load()

        val token = RegistryRoundTripBridge.mintToken()
        assertTrue(token != 0L, "the registry minted no token")

        // The high bit is set by construction, so this is an ordinary token and
        // not a contrived value.
        assertTrue(token < 0L, "an ordinary token must be negative as a Long: $token")
        assertTrue(
            (token.toULong() and (1UL shl 63)) != 0UL,
            "bit 63 must be set in an ordinary token",
        )

        // Byte-for-byte through the boundary in both directions.
        assertEquals(token, RegistryRoundTripBridge.echo(token), "the bits changed in transit")

        // And the registry itself still resolves it: the judge is the product,
        // not a comparison of two numbers on the Kotlin side.
        assertTrue(RegistryRoundTripBridge.resolves(token), "the round-tripped token stopped resolving")

        // A narrowed token must NOT resolve, which is what makes the above
        // meaningful rather than incidental.
        val narrowed = (token and 0xFFFFFFFFL)
        assertTrue(narrowed != token, "the fixture token has no high half to lose")
        assertTrue(
            !RegistryRoundTripBridge.resolves(narrowed),
            "a token narrowed to 32 bits must not resolve",
        )

        assertEquals(0L, RegistryRoundTripBridge.retire(token), "retirement must succeed")
        assertTrue(!RegistryRoundTripBridge.resolves(token), "a retired token must be stale")
    }
}
