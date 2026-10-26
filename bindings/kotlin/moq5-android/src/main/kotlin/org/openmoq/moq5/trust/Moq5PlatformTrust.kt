package org.openmoq.moq5.trust

import java.io.ByteArrayInputStream
import java.security.cert.CertificateException
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate

/**
 * The LIBRARY-OWNED Android platform trust bridge.
 *
 * It is `internal` and it takes no application input: the only thing an app
 * chooses is a `TrustPolicy` value, and user code never appears on this path.
 * The native side calls exactly one entry point here, and every answer other
 * than an explicit accept is a rejection.
 *
 * Scope, stated so it cannot be over-read: this object decides CHAIN and
 * PLATFORM POLICY. It does **not** verify the hostname. MOQ5's raw
 * QUIC/picotls path has no `SSLSocket` / `SSLEngine` hostname-verifier layer
 * beside the platform trust manager, so the reference identity is verified
 * NATIVELY, before this object is consulted at all. Android's
 * `X509TrustManagerExtensions` takes a host to select the app's trust
 * configuration and pin set -- which is why the host is still passed here --
 * and platform chain, pinning and CT authority remain the trust manager's.
 */
internal object Moq5PlatformTrust {
    /** Accepted. Any other value, and any throwable, is a rejection. */
    const val TRUSTED: Int = 0

    /** The chain could not be decoded, so there is nothing to judge. */
    const val ERR_CHAIN: Int = 1

    /** The platform refused, or could not be consulted. */
    const val ERR_REJECTED: Int = 2

    /**
     * Decides one chain, or refuses.
     *
     * Never throws: the native caller is C, and an exception crossing back
     * into it is undefined. Every failure is a value.
     */
    fun checkServerTrusted(chainDer: Array<ByteArray>, serverName: String, authType: String): Int {
        val chain =
            try {
                decodeChain(chainDer)
            } catch (_: CertificateException) {
                return ERR_CHAIN
            } catch (_: IllegalArgumentException) {
                return ERR_CHAIN
            }
        return try {
            checker.check(chain, authType, serverName)
            TRUSTED
        } catch (_: Throwable) {
            // Deliberately Throwable, not CertificateException: the contract is
            // fail-closed, and an unexpected failure is still a failure to
            // establish trust. Both collapse to ERR_REJECTED here; the native
            // side does not see them apart, and keeps its own pending-exception
            // handling only for failures of the JNI crossing itself.
            ERR_REJECTED
        }
    }

    /**
     * DER to `X509Certificate[]`, leaf first, or a refusal.
     *
     * Deterministic and total: an empty chain and a malformed element are both
     * rejections, never a shorter chain silently handed on. Order is preserved
     * exactly -- the platform expects the leaf at index 0, and reordering would
     * ask it to validate a different chain from the one the peer sent.
     */
    fun decodeChain(chainDer: Array<ByteArray>): Array<X509Certificate> {
        if (chainDer.isEmpty()) throw CertificateException("empty chain")
        val factory = CertificateFactory.getInstance("X.509")
        return Array(chainDer.size) { i ->
            val der = chainDer[i]
            if (der.isEmpty()) throw CertificateException("empty certificate at index $i")
            val cert = factory.generateCertificate(ByteArrayInputStream(der))
            // DEFENSIVE, and proven so: the "X.509" factory throws on input it
            // cannot decode before this cast is reached, and never returns a
            // non-X509 certificate, so no input reaches the null branch. Kept
            // because the API's return type permits it, not because a row
            // covers it.
            cert as? X509Certificate
                ?: throw CertificateException("element $i is not an X.509 certificate")
        }
    }

    /**
     * The platform call, behind a seam.
     *
     * The seam exists because `X509TrustManagerExtensions` is an Android
     * platform class: on a host JVM its stub throws, so the ordering and
     * fail-closed rules above would be untestable without it. It is NOT an
     * application hook -- it is `internal`, has no public setter, and the
     * shipped default is the platform itself.
     */
    internal fun interface Checker {
        /** Returns normally when trusted; throws otherwise. */
        fun check(chain: Array<X509Certificate>, authType: String, host: String)
    }

    @Volatile internal var checker: Checker = PlatformChecker
}
