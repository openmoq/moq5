package org.openmoq.moq5.jni

import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The Android platform-trust decision core, RED-first and ahead of any public
 * API (chats/libmoq/1301).
 *
 * These rows judge the DECISION, not a TLS handshake: the core takes a DER
 * chain and a server name as data, so every row here is deterministic and needs
 * no network, no relay, no localhost and no timing. The picotls wrapper that
 * will feed it -- and the `verify_sign` delegation only picotls' own OpenSSL
 * verifier can install -- is a separate concern and is deliberately not here.
 *
 * The load-bearing rule the rows exist to pin is that **hostname verification
 * is native and non-delegable**. MOQ5's raw QUIC/picotls path has no
 * `SSLSocket` / `SSLEngine` hostname-verifier layer beside the platform trust
 * manager, so nothing else in this stack would perform the reference-identity
 * check. The core therefore verifies it against the leaf BEFORE the Java trust
 * bridge is consulted, and no bridge answer can skip it. Platform chain,
 * pinning and CT authority stay the trust manager's; they are a separate
 * concern from this host-side substrate.
 *
 * "Delegate" in this file means the JAVA TRUST BRIDGE, and only that: the
 * decision core never reaches picotls' OpenSSL verifier, which is the wrapper's
 * concern and is covered in TrustWrapperTest.
 *
 * The `authType` the platform receives is the PROTOCOL-DERIVED constant
 * [AUTH_TYPE]: Android's server-side parameter is the negotiated key-exchange
 * algorithm, a QUIC client negotiates nothing older than TLS 1.3 (RFC 9001
 * section 4.2), and Conscrypt names `GENERIC` for every TLS 1.3 suite. It is
 * therefore the same for an RSA leaf and an EC leaf, and the two chains are
 * kept independent so a one-certificate coincidence cannot satisfy the rows.
 *
 * Every other outcome fails closed: absent delegate, empty chain, missing name,
 * malformed DER, refusal, and any throwable at all.
 */
internal class TrustDelegateTest {
    private companion object {
        const val HOST = "moq.example"

        /** Mirrors `MOQ5_TRUST_AUTH_TYPE`: exact and case-sensitive. */
        const val AUTH_TYPE = "GENERIC"
        const val REPEATS: Int = 4
        const val OTHER_HOST = "other.example"

        init {
            System.load(BridgeDsos.path("trust_delegate"))
        }

        /** Generated in C, not checked in: the row owns the SAN it asserts on. */
        fun cert(algorithm: String, dnsName: String): ByteArray {
            val der = TrustDelegateBridge.nativeMakeSelfSigned(algorithm, "DNS", dnsName)
            assertNotNull(der, "setup: could not generate a $algorithm certificate for $dnsName")
            assertTrue(der.isNotEmpty(), "setup: the generated certificate was empty")
            return der
        }
    }

    @BeforeTest
    fun resetDelegate() = TrustDelegateBridge.reset()

    @Test
    fun `an RSA leaf the delegate trusts is accepted, and the delegate sees the real inputs`() {
        val chain = arrayOf(cert("RSA", HOST))
        assertEquals(
            TrustDelegateBridge.OK,
            TrustDelegateBridge.nativeDecide(chain, HOST, true),
            "a trusted RSA leaf whose SAN matches the reference identity must be accepted",
        )
        assertEquals(1, TrustDelegateBridge.calls, "the delegate must be consulted exactly once")
        assertEquals(
            listOf(chain[0].size),
            TrustDelegateBridge.lastChainSizes,
            "the delegate must receive the chain leaf-first and byte-complete",
        )
        assertEquals(HOST, TrustDelegateBridge.lastServerName, "the server name must cross exactly")
        assertEquals(
            AUTH_TYPE,
            TrustDelegateBridge.lastAuthType,
            "the auth type is the TLS 1.3 key-exchange constant GENERIC, not a value read off " +
                "the RSA leaf: the platform asks about the key exchange, which the leaf cannot answer",
        )
        // The SAME two values, observed on the NATIVE side of the crossing.
        // Comparing the two observations is what makes each of them evidence:
        // agreement between them cannot be produced by a bridge that mangles a
        // value on the way over, and the Kotlin record alone could not tell.
        assertEquals(
            TrustDelegateBridge.nativeSeenServerName(),
            TrustDelegateBridge.lastServerName,
            "the name the native core decided on must be the name the delegate received",
        )
        assertEquals(
            TrustDelegateBridge.nativeSeenAuthType(),
            TrustDelegateBridge.lastAuthType,
            "the auth type the native core derived must be the one the delegate received",
        )
    }

    @Test
    fun `an ECDSA leaf receives the same GENERIC auth type as an RSA leaf`() {
        // An INDEPENDENT chain with a different key family. A substrate that
        // derived the value from the leaf would answer "EC" here and "RSA"
        // above; a substrate that guessed one key family for everything would
        // answer that family for both. Only the protocol-derived constant
        // answers GENERIC for both, so the two rows together are the oracle.
        val chain = arrayOf(cert("EC", HOST))
        assertEquals(
            TrustDelegateBridge.OK,
            TrustDelegateBridge.nativeDecide(chain, HOST, true),
            "a trusted EC leaf must be accepted",
        )
        assertEquals(
            AUTH_TYPE,
            TrustDelegateBridge.lastAuthType,
            "an EC leaf must not turn the auth type into EC: the TLS 1.3 key exchange is " +
                "the same whatever the leaf key family",
        )
        assertEquals(
            AUTH_TYPE,
            TrustDelegateBridge.nativeSeenAuthType(),
            "the native side must have chosen GENERIC too, not merely reported what it sent",
        )
        assertTrue(
            TrustDelegateBridge.lastAuthType!!.isNotEmpty() &&
                TrustDelegateBridge.lastAuthType == AUTH_TYPE,
            "an empty or differently-cased value is not the accepted constant",
        )
    }

    @Test
    fun `a name the leaf does not carry is rejected NATIVELY, before the delegate is asked`() {
        val chain = arrayOf(cert("RSA", HOST))
        assertEquals(
            TrustDelegateBridge.ERR_HOST,
            TrustDelegateBridge.nativeDecide(chain, OTHER_HOST, true),
            "a reference identity absent from the leaf must be refused by X509_check_host",
        )
        // The discriminating assertion: a design that delegated the name would
        // reach the delegate and be accepted by it, because the scripted
        // adapter trusts everything it is shown.
        assertEquals(
            0,
            TrustDelegateBridge.calls,
            "hostname verification must precede the delegate and must not be delegable",
        )
    }

    @Test
    fun `an untrusted chain is rejected when the delegate refuses`() {
        val chain = arrayOf(cert("RSA", HOST))
        TrustDelegateBridge.behaviour = TrustDelegateBridge.Behaviour.Refuse
        assertEquals(
            TrustDelegateBridge.ERR_REJECTED,
            TrustDelegateBridge.nativeDecide(chain, HOST, true),
            "a delegate refusal is a reject, carried as its own cause",
        )
        assertEquals(1, TrustDelegateBridge.calls, "the delegate must have been consulted")
    }

    @Test
    fun `a CertificateException is the expected refusal and is cleared`() {
        val chain = arrayOf(cert("RSA", HOST))
        TrustDelegateBridge.behaviour = TrustDelegateBridge.Behaviour.ThrowCertificate
        assertEquals(
            TrustDelegateBridge.ERR_CERT_THROW,
            TrustDelegateBridge.nativeDecide(chain, HOST, true),
            "a CertificateException must be classified as the certificate refusal it is",
        )
        // If the exception were left pending, this second crossing would fail
        // or abort under -Xcheck:jni rather than answering.
        TrustDelegateBridge.behaviour = TrustDelegateBridge.Behaviour.Accept
        assertEquals(
            TrustDelegateBridge.OK,
            TrustDelegateBridge.nativeDecide(chain, HOST, true),
            "the throwable must have been cleared: the next call must work normally",
        )
    }

    @Test
    fun `any other throwable is also a reject, never an accept`() {
        val chain = arrayOf(cert("RSA", HOST))
        TrustDelegateBridge.behaviour = TrustDelegateBridge.Behaviour.ThrowOther
        assertEquals(
            TrustDelegateBridge.ERR_OTHER_THROW,
            TrustDelegateBridge.nativeDecide(chain, HOST, true),
            "an unexpected throwable must fail CLOSED and stay distinguishable from a " +
                "certificate refusal",
        )
        TrustDelegateBridge.behaviour = TrustDelegateBridge.Behaviour.Accept
        assertEquals(
            TrustDelegateBridge.OK,
            TrustDelegateBridge.nativeDecide(chain, HOST, true),
            "it must also have been cleared",
        )
    }

    @Test
    fun `an absent delegate fails closed rather than falling back to no verification`() {
        val chain = arrayOf(cert("RSA", HOST))
        assertEquals(
            TrustDelegateBridge.ERR_NO_DELEGATE,
            TrustDelegateBridge.nativeDecide(chain, HOST, false),
            "with no delegate there is no trust decision, so there is no accept",
        )
        assertEquals(0, TrustDelegateBridge.calls, "nothing may be consulted")
    }

    @Test
    fun `malformed DER, an empty chain and an absent name each fail closed`() {
        assertEquals(
            TrustDelegateBridge.ERR_EMPTY_CHAIN,
            TrustDelegateBridge.nativeDecide(emptyArray(), HOST, true),
            "an empty chain is not a trusted chain",
        )
        assertEquals(
            TrustDelegateBridge.ERR_NO_NAME,
            TrustDelegateBridge.nativeDecide(arrayOf(cert("RSA", HOST)), null, true),
            "without a reference identity the name cannot be verified, so nothing may be accepted",
        )
        assertEquals(
            TrustDelegateBridge.ERR_BAD_DER,
            TrustDelegateBridge.nativeDecide(arrayOf(byteArrayOf(0x30, 0x03, 0x02, 0x01, 0x01)), HOST, true),
            "DER that is not a certificate must be refused, not passed on",
        )
        assertEquals(
            0,
            TrustDelegateBridge.calls,
            "none of these may reach the delegate: they are decided before it",
        )
    }

    @Test
    fun `a foreign thread attaches once, not once per call`() {
        val chain = arrayOf(cert("RSA", HOST))
        val before = TrustDelegateBridge.nativeAttachCount()
        assertEquals(
            TrustDelegateBridge.OK,
            TrustDelegateBridge.nativeDecideForeign(chain, HOST, REPEATS),
            "the decision must succeed on a thread the JVM has never seen",
        )
        assertEquals(REPEATS, TrustDelegateBridge.calls, "every repeat must reach the delegate")
        assertEquals(
            1,
            TrustDelegateBridge.nativeAttachCount() - before,
            "one attach per foreign THREAD; per-call attach/detach is churn on the network thread",
        )
    }

    @Test
    fun `the delegate never runs on the JVM main thread`() {
        val chain = arrayOf(cert("RSA", HOST))
        assertEquals(
            TrustDelegateBridge.OK,
            TrustDelegateBridge.nativeDecideForeign(chain, HOST, 1),
            "setup: the foreign decision must succeed",
        )
        val ran = TrustDelegateBridge.lastThreadName
        assertNotNull(ran, "the delegate must have recorded its thread")
        assertTrue(
            ran != Thread.currentThread().name,
            "the delegate ran on the calling thread; on device this path is picoquic's network " +
                "thread and must never touch the main looper",
        )
    }
}
