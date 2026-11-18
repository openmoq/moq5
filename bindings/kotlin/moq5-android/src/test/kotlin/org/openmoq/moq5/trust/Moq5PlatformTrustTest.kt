package org.openmoq.moq5.trust

import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The library-owned trust bridge's DECISION and CHAIN CONVERSION.
 *
 * What these rows can and cannot prove is worth stating up front, because the
 * distinction is the whole reason the seam exists. `X509TrustManagerExtensions`
 * is an Android platform class whose host-JVM stub throws, so the PLATFORM
 * VERDICT -- domain anchors, debug overrides, pinning, CT -- is device-earned
 * and is not claimed anywhere here. What IS host-provable, and is proved, is
 * everything around it: that the DER chain is converted deterministically and
 * leaf-first, that malformed and empty inputs are refused rather than passed
 * on shortened, that the platform is consulted with exactly what the caller was
 * given, and that no answer other than an explicit accept can become trust.
 *
 * The bridge is also never allowed to throw: its caller is C, where an
 * exception crossing back is undefined. Every failure is a value.
 */
internal class Moq5PlatformTrustTest {
    private companion object {
        /** A real DER certificate, built once with the JDK's own tools. */
        val LEAF: ByteArray = TestCerts.selfSigned("CN=leaf")
        val INTERMEDIATE: ByteArray = TestCerts.selfSigned("CN=intermediate")
    }

    @AfterTest
    fun restoreChecker() {
        Moq5PlatformTrust.checker = PlatformChecker
    }

    @Test
    fun `a chain the platform accepts is trusted, and crosses leaf-first and byte-exact`() {
        var seenChain: Array<X509Certificate>? = null
        var seenAuth: String? = null
        var seenHost: String? = null
        Moq5PlatformTrust.checker =
            Moq5PlatformTrust.Checker { chain, authType, host ->
                seenChain = chain
                seenAuth = authType
                seenHost = host
            }

        // The auth type is a pass-through here: this object never chooses it.
        // The native caller supplies the TLS 1.3 key-exchange constant GENERIC,
        // and what this row pins is that the value reaches the platform exactly.
        assertEquals(
            Moq5PlatformTrust.TRUSTED,
            Moq5PlatformTrust.checkServerTrusted(arrayOf(LEAF, INTERMEDIATE), "moq.example", "GENERIC"),
        )

        val chain = seenChain
        assertTrue(chain != null && chain.size == 2, "both elements must reach the platform")
        assertContentEquals(
            LEAF,
            chain!![0].encoded,
            "index 0 must be the LEAF, re-encoding to the exact bytes it was given",
        )
        assertContentEquals(INTERMEDIATE, chain[1].encoded, "index 1 must be the INTERMEDIATE")
        assertEquals("moq.example", seenHost, "the host selects the per-domain config and pins")
        assertEquals("GENERIC", seenAuth, "the auth type must cross unchanged")
    }

    @Test
    fun `a platform refusal is a rejection, not an exception`() {
        Moq5PlatformTrust.checker =
            Moq5PlatformTrust.Checker { _, _, _ -> throw CertificateException("refused") }
        assertEquals(
            Moq5PlatformTrust.ERR_REJECTED,
            Moq5PlatformTrust.checkServerTrusted(arrayOf(LEAF), "moq.example", "RSA"),
        )
    }

    @Test
    fun `any other throwable from the platform is also a rejection, never an accept`() {
        Moq5PlatformTrust.checker =
            Moq5PlatformTrust.Checker { _, _, _ -> throw IllegalStateException("stub!") }
        assertEquals(
            Moq5PlatformTrust.ERR_REJECTED,
            Moq5PlatformTrust.checkServerTrusted(arrayOf(LEAF), "moq.example", "RSA"),
        )
    }

    @Test
    fun `an empty chain is refused before the platform is consulted`() {
        var consulted = false
        Moq5PlatformTrust.checker = Moq5PlatformTrust.Checker { _, _, _ -> consulted = true }
        assertEquals(
            Moq5PlatformTrust.ERR_CHAIN,
            Moq5PlatformTrust.checkServerTrusted(emptyArray(), "moq.example", "RSA"),
        )
        assertTrue(!consulted, "there is nothing to judge, so nothing may be asked")
    }

    @Test
    fun `a malformed element is refused, never dropped to a shorter chain`() {
        var consulted = false
        Moq5PlatformTrust.checker = Moq5PlatformTrust.Checker { _, _, _ -> consulted = true }
        assertEquals(
            Moq5PlatformTrust.ERR_CHAIN,
            Moq5PlatformTrust.checkServerTrusted(
                arrayOf(LEAF, byteArrayOf(0x30, 0x03, 0x02, 0x01, 0x01)),
                "moq.example",
                "RSA",
            ),
            "a chain that cannot be decoded in full is not a shorter valid chain",
        )
        assertTrue(!consulted, "a chain that could not be built must not be judged")
    }

    @Test
    fun `an empty element is refused too`() {
        assertEquals(
            Moq5PlatformTrust.ERR_CHAIN,
            Moq5PlatformTrust.checkServerTrusted(arrayOf(LEAF, ByteArray(0)), "moq.example", "RSA"),
        )
    }

    @Test
    fun `decodeChain preserves order and refuses what it cannot decode`() {
        val decoded = Moq5PlatformTrust.decodeChain(arrayOf(LEAF, INTERMEDIATE))
        assertContentEquals(LEAF, decoded[0].encoded)
        assertContentEquals(INTERMEDIATE, decoded[1].encoded)
        assertFailsWith<CertificateException> { Moq5PlatformTrust.decodeChain(emptyArray()) }
        assertFailsWith<CertificateException> {
            Moq5PlatformTrust.decodeChain(arrayOf(byteArrayOf(1, 2, 3)))
        }
    }
}
