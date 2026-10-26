package org.openmoq.moq5.jni

import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The picotls WRAPPER, driven through its real
 * `ptls_verify_certificate_t::cb` -- no handshake, no network, no relay.
 *
 * The shape signed off in chats/libmoq/1303: our verifier owns the decision,
 * and picotls' OpenSSL verifier is consulted ONLY after that
 * decision accepts, solely so picotls installs its own `verify_sign` /
 * `verify_data`. That delegation is not a convenience -- `verify_sign` is
 * file-static in picotls' openssl.c and is not exported, so a fully custom
 * verifier could not complete a handshake at all.
 *
 * Two facts these rows exist to pin, neither of which a handshake test would
 * isolate:
 *
 *  * the wrapper decides against the CALLBACK's `server_name`, never
 *    `ptls_get_server_name(tls)`. Every row here sets a DIFFERENT name on the
 *    `ptls_t`, so a wrapper that read the wrong one decides against the wrong
 *    identity and the row fails;
 *  * a rejection returns BEFORE picotls' OpenSSL verifier is called, so no
 *    accept override can run and no `verify_sign` is installed. That ordering
 *    is what makes the override's unconditional success safe rather than a
 *    blanket accept.
 */
internal class TrustWrapperTest {
    private companion object {
        const val HOST = "moq.example"
        const val OTHER_HOST = "other.example"

        /** Set on the ptls_t, and never the identity a row expects to be used. */
        const val DECOY = "ech-public-name.example"

        const val IP = "192.0.2.10"
        const val OTHER_IP = "198.51.100.7"

        init {
            System.load(BridgeDsos.path("trust_delegate"))
        }

        /** A leaf whose ONLY SAN is [sanValue] of kind [sanKind]. */
        fun cert(algorithm: String, sanKind: String, sanValue: String): ByteArray {
            val der = TrustDelegateBridge.nativeMakeSelfSigned(algorithm, sanKind, sanValue)
            assertNotNull(der, "setup: could not generate a $algorithm cert with $sanKind:$sanValue")
            return der
        }

        fun dnsCert(algorithm: String, name: String) = cert(algorithm, "DNS", name)

        fun ipCert(algorithm: String, addr: String) = cert(algorithm, "IP", addr)
    }

    @BeforeTest
    fun resetDelegate() = TrustDelegateBridge.reset()

    /** Accept, and the delegation that only picotls can perform. */
    @Test
    fun `a DNS SAN accept calls the OpenSSL verifier exactly once and installs verify_sign`() {
        val chain = arrayOf(dnsCert("RSA", HOST))
        assertEquals(
            TrustDelegateBridge.OK,
            TrustDelegateBridge.nativeVerifyCb(chain, HOST, DECOY, true, false),
            "a matching DNS SAN with a trusting Java trust bridge must be accepted",
        )
        assertAcceptedToPicotls(osslCalls = 1)
        // The identity actually used was the callback's, not the ptls_t's.
        assertEquals(
            HOST,
            TrustDelegateBridge.lastServerName,
            "the decision must use the callback's server_name, never ptls_get_server_name",
        )
    }

    @Test
    fun `an IP SAN accept matches through the OpenSSL IP path`() {
        val chain = arrayOf(ipCert("RSA", IP))
        assertEquals(
            TrustDelegateBridge.OK,
            TrustDelegateBridge.nativeVerifyCb(chain, IP, DECOY, true, false),
            "an IP literal must be matched against iPAddress SANs, not compared as a DNS string",
        )
        assertAcceptedToPicotls(osslCalls = 1)
    }

    @Test
    fun `a DNS mismatch is refused before the Java trust bridge and before the OpenSSL verifier`() {
        val chain = arrayOf(dnsCert("RSA", HOST))
        assertEquals(
            TrustDelegateBridge.ERR_HOST,
            TrustDelegateBridge.nativeVerifyCb(chain, OTHER_HOST, DECOY, true, false),
            "a name the leaf does not carry must be refused natively",
        )
        assertRefusedBeforeOpenSslVerifier()
    }

    @Test
    fun `an IP mismatch is refused before the Java trust bridge and before the OpenSSL verifier`() {
        val chain = arrayOf(ipCert("RSA", IP))
        assertEquals(
            TrustDelegateBridge.ERR_HOST,
            TrustDelegateBridge.nativeVerifyCb(chain, OTHER_IP, DECOY, true, false),
            "an IP the leaf does not carry must be refused natively",
        )
        assertRefusedBeforeOpenSslVerifier()
    }

    @Test
    fun `an IP reference identity is not satisfied by a dNSName carrying the same text`() {
        // THE discriminator for the DNS/IP split, and it only works because the
        // SAN kind is chosen explicitly: this certificate carries the literal
        // "192.0.2.10" as a dNSName, so a wrapper routing IP literals through
        // X509_check_host would match the text and accept.
        val dnsSpelledLikeIp = arrayOf(cert("RSA", "DNS", IP))
        assertEquals(
            TrustDelegateBridge.ERR_HOST,
            TrustDelegateBridge.nativeVerifyCb(dnsSpelledLikeIp, IP, DECOY, true, false),
            "an IP reference identity must never be satisfied by a dNSName entry, even one " +
                "whose text is exactly that address",
        )
        assertRefusedBeforeOpenSslVerifier()

        // Not vacuous: the SAME address as a real iPAddress SAN is accepted, so
        // the refusal above is about the SAN KIND and nothing else.
        assertEquals(
            TrustDelegateBridge.OK,
            TrustDelegateBridge.nativeVerifyCb(arrayOf(ipCert("RSA", IP)), IP, DECOY, true, false),
        )
        assertAcceptedToPicotls(osslCalls = 1)
    }

    @Test
    fun `the full chain crosses leaf-first and byte-complete, intermediates included`() {
        val leaf = dnsCert("RSA", HOST)
        val intermediate = dnsCert("EC", "intermediate.example")
        assertEquals(
            TrustDelegateBridge.OK,
            TrustDelegateBridge.nativeVerifyCb(arrayOf(leaf, intermediate), HOST, DECOY, true, false),
            "a two-element chain must be accepted",
        )
        assertAcceptedToPicotls(osslCalls = 1)
        // BYTE-EXACT, not length-only: a same-length mutation anywhere in the
        // chain would pass a size comparison unnoticed.
        val got = TrustDelegateBridge.lastChain
        assertEquals(2, got.size, "both elements must cross, neither dropped")
        assertTrue(
            leaf.contentEquals(got[0]),
            "element 0 must be the LEAF, byte for byte",
        )
        assertTrue(
            intermediate.contentEquals(got[1]),
            "element 1 must be the INTERMEDIATE, byte for byte",
        )
    }

    @Test
    fun `every closed failure refuses without reaching the OpenSSL verifier and leaves nothing pending`() {
        val chain = arrayOf(dnsCert("RSA", HOST))

        assertEquals(
            TrustDelegateBridge.ERR_EMPTY_CHAIN,
            TrustDelegateBridge.nativeVerifyCb(emptyArray(), HOST, DECOY, true, false),
        )
        assertRefusedBeforeOpenSslVerifier()

        assertEquals(
            TrustDelegateBridge.ERR_NO_NAME,
            TrustDelegateBridge.nativeVerifyCb(chain, null, DECOY, true, false),
        )
        assertRefusedBeforeOpenSslVerifier()

        assertEquals(
            TrustDelegateBridge.ERR_BAD_DER,
            TrustDelegateBridge.nativeVerifyCb(
                arrayOf(byteArrayOf(0x30, 0x03, 0x02, 0x01, 0x01)), HOST, DECOY, true, false,
            ),
        )
        assertRefusedBeforeOpenSslVerifier()

        assertEquals(
            TrustDelegateBridge.ERR_NO_DELEGATE,
            TrustDelegateBridge.nativeVerifyCb(chain, HOST, DECOY, false, false),
            "an absent Java bridge is a refusal, never a fall-through to no verification",
        )
        assertRefusedBeforeOpenSslVerifier()
    }

    @Test
    fun `a Java trust-bridge refusal and either throwable each reject without installing verify_sign`() {
        val chain = arrayOf(dnsCert("RSA", HOST))

        TrustDelegateBridge.behaviour = TrustDelegateBridge.Behaviour.Refuse
        assertEquals(
            TrustDelegateBridge.ERR_REJECTED,
            TrustDelegateBridge.nativeVerifyCb(chain, HOST, DECOY, true, false),
        )
        assertRejectedToPicotls()

        TrustDelegateBridge.behaviour = TrustDelegateBridge.Behaviour.ThrowCertificate
        assertEquals(
            TrustDelegateBridge.ERR_CERT_THROW,
            TrustDelegateBridge.nativeVerifyCb(chain, HOST, DECOY, true, false),
        )
        assertRejectedToPicotls()

        TrustDelegateBridge.behaviour = TrustDelegateBridge.Behaviour.ThrowOther
        assertEquals(
            TrustDelegateBridge.ERR_OTHER_THROW,
            TrustDelegateBridge.nativeVerifyCb(chain, HOST, DECOY, true, false),
        )
        assertRejectedToPicotls()

        // Nothing was left pending: an ordinary accept still works afterwards.
        TrustDelegateBridge.behaviour = TrustDelegateBridge.Behaviour.Accept
        assertEquals(
            TrustDelegateBridge.OK,
            TrustDelegateBridge.nativeVerifyCb(chain, HOST, DECOY, true, false),
        )
        assertAcceptedToPicotls(osslCalls = 1)
    }

    @Test
    fun `a malformed intermediate fails closed inside the OpenSSL verifier`() {
        // The leaf is well formed, so the decision core accepts and the OpenSSL
        // verifier IS reached -- and it is what refuses. The row exists because
        // the wrapper's own verdict is OK here: reading only that verdict would
        // report an accepted handshake.
        val chain = arrayOf(dnsCert("RSA", HOST), byteArrayOf(0x30, 0x03, 0x02, 0x01, 0x01))
        assertEquals(
            TrustDelegateBridge.OK,
            TrustDelegateBridge.nativeVerifyCb(chain, HOST, DECOY, true, false),
            "the decision core judges the leaf, which is valid",
        )
        assertEquals(
            1,
            TrustDelegateBridge.nativeOsslDelegateCalls(),
            "the OpenSSL verifier must have been reached: that is where this fails",
        )
        assertEquals(
            TrustDelegateBridge.PTLS_ALERT_BAD_CERTIFICATE,
            TrustDelegateBridge.nativeLastAlert(),
            "picotls must be told the alert, whatever the wrapper's own verdict was",
        )
        assertFalse(
            TrustDelegateBridge.nativeVerifySignInstalled(),
            "no verify_sign may be installed for a chain picotls refused",
        )
    }

    @Test
    fun `a rejection leaves seeded output parameters untouched`() {
        // Stronger than "verify_sign is NULL": with the caller's own sentinels
        // in place, UNCHANGED proves the wrapper wrote nothing at all, so it
        // cannot have left picotls stale success data of its own making.
        val chain = arrayOf(dnsCert("RSA", HOST))
        assertEquals(
            TrustDelegateBridge.ERR_HOST,
            TrustDelegateBridge.nativeVerifyCb(chain, OTHER_HOST, DECOY, true, true),
        )
        assertTrue(
            TrustDelegateBridge.nativeOutputsUntouched(),
            "a rejecting wrapper must not write either output parameter",
        )
        assertFalse(
            TrustDelegateBridge.nativeVerifySignInstalled(),
            "nothing was INSTALLED: the pointers are non-NULL only because the caller seeded " +
                "them, and a non-NULL test alone would call that an installed verifier",
        )
        assertEquals(
            TrustDelegateBridge.PTLS_ALERT_BAD_CERTIFICATE,
            TrustDelegateBridge.nativeLastAlert(),
        )

        // On ACCEPT the same seeds must be replaced by picotls' own values, so
        // the check above is about the reject path and not about seeding.
        assertEquals(
            TrustDelegateBridge.OK,
            TrustDelegateBridge.nativeVerifyCb(chain, HOST, DECOY, true, true),
        )
        assertFalse(
            TrustDelegateBridge.nativeOutputsUntouched(),
            "an accepted chain must have picotls' own verify_sign, not the seed",
        )
        assertAcceptedToPicotls(osslCalls = 1)
    }

    /**
     * A refusal must not have reached picotls' OPENSSL VERIFIER.
     *
     * Deliberately not "before the delegate": there are two, and they are not
     * refused at the same point. A NAME mismatch is refused before the JAVA
     * TRUST BRIDGE as well, whereas a Java refusal or throwable necessarily
     * means the bridge WAS consulted -- but in neither case may the OpenSSL
     * verifier run, and in neither case may picotls be told success.
     */
    private fun assertRefusedBeforeOpenSslVerifier() {
        assertEquals(
            0,
            TrustDelegateBridge.nativeOsslDelegateCalls(),
            "a rejection must return before picotls' OpenSSL verifier is called",
        )
        assertRejectedToPicotls()
    }

    /**
     * An acceptance must be reported to picotls as an acceptance, with real
     * material to verify CertificateVerify with.
     *
     * [osslCalls] is passed only by rows that OWN the count -- a row that
     * accepts twice cannot claim "exactly once" for either call.
     */
    private fun assertAcceptedToPicotls(osslCalls: Int? = null) {
        assertEquals(
            TrustDelegateBridge.PTLS_OK,
            TrustDelegateBridge.nativeLastAlert(),
            "an accepted chain must return picotls success exactly",
        )
        assertTrue(
            TrustDelegateBridge.nativeVerifySignInstalled(),
            "picotls must have installed BOTH verify_sign and verify_data, replacing whatever " +
                "the caller left: without them the handshake cannot verify CertificateVerify",
        )
        if (osslCalls != null) {
            assertEquals(
                osslCalls,
                TrustDelegateBridge.nativeOsslDelegateCalls(),
                "the OpenSSL verifier must run exactly $osslCalls time(s), and only after accept",
            )
        }
    }

    /** picotls must have been told the exact alert, and given nothing usable. */
    private fun assertRejectedToPicotls() {
        assertEquals(
            TrustDelegateBridge.PTLS_ALERT_BAD_CERTIFICATE,
            TrustDelegateBridge.nativeLastAlert(),
            "picotls must receive the exact alert: a local rejection that returned success " +
                "would accept the handshake",
        )
        assertFalse(
            TrustDelegateBridge.nativeVerifySignInstalled(),
            "a rejected chain must leave verify_sign uninstalled",
        )
    }
}
