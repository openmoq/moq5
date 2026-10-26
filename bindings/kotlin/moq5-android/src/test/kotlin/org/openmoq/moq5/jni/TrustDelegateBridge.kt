package org.openmoq.moq5.jni

/**
 * TEST-ONLY bridge to the trust-decision fixture.
 *
 * It is the Kotlin half of `jni/testfixtures/moq5_trust_jni_fixture.c`: the
 * native side resolves this class and [checkServerTrusted] ONCE in
 * `JNI_OnLoad`, on the loading thread, and holds the class as a global
 * reference. That is not an optimisation -- a native thread attached later
 * resolves classes through the system loader and cannot see an application
 * class at all.
 *
 * [checkServerTrusted] stands in for the library-owned platform adapter. It is
 * deliberately NOT an application callback: the shipped shape would build its
 * verdict from `TrustManagerFactory` + `X509TrustManagerExtensions`, and user
 * code would never be on this path. Here it is scriptable so a row can pin what
 * each answer -- accept, refuse, throw -- must do.
 */
internal object TrustDelegateBridge {
    /** Native verdicts. Each refusal is distinct: "not accepted" is not a cause. */
    const val OK: Int = 0
    const val ERR_NO_DELEGATE: Int = -1
    const val ERR_EMPTY_CHAIN: Int = -2
    const val ERR_NO_NAME: Int = -3
    const val ERR_BAD_DER: Int = -4
    const val ERR_HOST: Int = -5
    const val ERR_REJECTED: Int = -7
    const val ERR_CERT_THROW: Int = -8
    const val ERR_OTHER_THROW: Int = -9
    const val ERR_ATTACH: Int = -10
    const val ERR_JNI_LIMIT: Int = -11

    /** picotls success, and `PTLS_ALERT_BAD_CERTIFICATE` (picotls.h:238). */
    const val PTLS_OK: Int = 0
    const val PTLS_ALERT_BAD_CERTIFICATE: Int = 42

    /** What the scripted adapter does when the native side consults it. */
    internal sealed interface Behaviour {
        data object Accept : Behaviour

        data object Refuse : Behaviour

        /** The EXPECTED refusal shape from a real `X509TrustManager`. */
        data object ThrowCertificate : Behaviour

        /** Anything else a delegate can throw. It must not become an accept. */
        data object ThrowOther : Behaviour
    }

    @Volatile var behaviour: Behaviour = Behaviour.Accept

    /** Every call the native side made, for rows that assert what it was given. */
    @Volatile var calls: Int = 0

    /**
     * The chain the native side handed over, COPIED BYTE FOR BYTE.
     *
     * Sizes alone are not evidence: a same-length mutation anywhere in the
     * chain would pass a length-only oracle unnoticed. The copy is taken here,
     * inside the call, because the arrays are the bridge's own and a row must
     * compare what actually crossed.
     */
    @Volatile var lastChain: List<ByteArray> = emptyList()

    val lastChainSizes: List<Int> get() = lastChain.map { it.size }

    @Volatile var lastServerName: String? = null

    @Volatile var lastAuthType: String? = null

    /** The thread each call ran on, so a row can prove it was not the main one. */
    @Volatile var lastThreadName: String? = null

    fun reset() {
        behaviour = Behaviour.Accept
        calls = 0
        lastChain = emptyList()
        lastServerName = null
        lastAuthType = null
        lastThreadName = null
    }

    /**
     * The delegate the native decision core calls. Resolved by exact descriptor
     * in `JNI_OnLoad`, so a signature change here fails the load rather than
     * silently missing.
     */
    @JvmStatic
    fun checkServerTrusted(chain: Array<ByteArray>, serverName: String, authType: String): Int {
        calls += 1
        lastChain = chain.map { it.copyOf() }
        lastServerName = serverName
        lastAuthType = authType
        lastThreadName = Thread.currentThread().name
        return when (behaviour) {
            Behaviour.Accept -> 0
            Behaviour.Refuse -> 1
            Behaviour.ThrowCertificate ->
                throw java.security.cert.CertificateException("scripted refusal")
            Behaviour.ThrowOther -> throw IllegalStateException("scripted non-certificate failure")
        }
    }

    /**
     * A self-signed leaf with a FIXED non-matching CN and exactly one SAN of
     * the caller's chosen [sanKind] (`"DNS"` or `"IP"`).
     *
     * The kind is the caller's and is never inferred from [sanValue]: inferring
     * it would make the one case that matters unconstructible -- a `dNSName`
     * whose text is an IP literal.
     */
    external fun nativeMakeSelfSigned(
        algorithm: String,
        sanKind: String,
        sanValue: String,
    ): ByteArray?

    external fun nativeDecide(chain: Array<ByteArray>, serverName: String?, useDelegate: Boolean): Int

    /** Runs [repeat] decisions on a thread the JVM has never seen. */
    external fun nativeDecideForeign(chain: Array<ByteArray>, serverName: String?, repeat: Int): Int

    /** Attaches performed so far. The contract is one per foreign THREAD. */
    external fun nativeAttachCount(): Int

    external fun nativeSeenAuthType(): String?

    external fun nativeSeenServerName(): String?

    /**
     * Drives the picotls wrapper's `ptls_verify_certificate_t::cb` DIRECTLY --
     * no handshake, no network.
     *
     * [tlsServerName] is set on the `ptls_t` itself and is deliberately
     * different from [serverName], which is what the callback is handed. A
     * wrapper that consulted `ptls_get_server_name(tls)` would decide against
     * the wrong identity, which is precisely the ECH divergence picotls
     * documents. Returns the wrapper's own verdict.
     */
    external fun nativeVerifyCb(
        chain: Array<ByteArray>,
        serverName: String?,
        tlsServerName: String?,
        useDelegate: Boolean,
        seedOutputs: Boolean,
    ): Int

    /** How many times the PRIVATE OpenSSL delegate ran during the last call. */
    external fun nativeOsslDelegateCalls(): Int

    /** Whether the last call left BOTH `verify_sign` and `verify_data` set. */
    external fun nativeVerifySignInstalled(): Boolean

    /**
     * What the wrapper actually RETURNED to picotls.
     *
     * Distinct from the wrapper's own verdict on purpose: a wrapper that
     * recorded a local rejection and still answered picotls with success would
     * satisfy every verdict assertion while accepting the handshake.
     */
    external fun nativeLastAlert(): Int

    /** Whether seeded output parameters survived the call untouched. */
    external fun nativeOutputsUntouched(): Boolean
}
