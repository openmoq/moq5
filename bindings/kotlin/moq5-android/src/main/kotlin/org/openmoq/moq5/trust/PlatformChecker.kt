package org.openmoq.moq5.trust

import android.net.http.X509TrustManagerExtensions
import java.security.KeyStore
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509TrustManager

/**
 * The real platform check: the app's own Network Security Config, reached
 * through the default trust manager.
 *
 * `TrustManagerFactory.init(null)` is what selects the PLATFORM configuration
 * -- the app's domain anchors, its debug-only overrides in a debuggable build,
 * and its declared pin sets -- rather than any store this library chose.
 * Wrapping it in [X509TrustManagerExtensions] is what makes the call
 * HOST-AWARE, which is required: without the host the platform cannot select
 * the per-domain configuration or apply pins.
 *
 * The three-argument overload is API 17 and is the only one used here.
 * The API-36 OCSP/SCT overload is deliberately NOT used: picotls' client
 * offers neither `status_request` nor `signed_certificate_timestamp`, so there
 * is no stapled OCSP response and no SCT list to pass, and supplying nothing
 * while claiming CT was represented would be a false claim. CT is handled by
 * refusing before connect where the platform requires it.
 *
 * Construction is lazy and cached: building a trust manager per handshake would
 * put avoidable work on the transport thread, and the platform configuration
 * does not change within a process.
 */
internal object PlatformChecker : Moq5PlatformTrust.Checker {
    /*
     * There is no API-level guard here any more, and its absence is the
     * point. `X509TrustManagerExtensions` exists from API 17; the module now
     * declares `minSdk = 21`, so a runtime check against 17 is unreachable --
     * lint's ObsoleteSdkInt says so, and a dead refusal branch reads as though
     * some device could still take it. The floor is the contract; every
     * supported device has this class.
     */

    /** Built once and reused: the platform configuration cannot change within a process. */
    @Volatile private var extensions: X509TrustManagerExtensions? = null

    override fun check(chain: Array<X509Certificate>, authType: String, host: String) {
        val ext =
            extensions ?: run {
                val factory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
                factory.init(null as KeyStore?)
                val x509 =
                    factory.trustManagers.filterIsInstance<X509TrustManager>().firstOrNull()
                        ?: throw CertificateException(
                            "no X509TrustManager in the platform default set",
                        )
                X509TrustManagerExtensions(x509).also { extensions = it }
            }
        // Returns the validated chain, which this slice does not consume: the
        // user-added-CA reporting it enables is a device-earned row and is not
        // claimed here. A throw is the refusal.
        ext.checkServerTrusted(chain, authType, host)
    }
}
