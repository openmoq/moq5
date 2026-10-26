package org.openmoq.moq5.trust

import android.os.Build
import android.security.NetworkSecurityPolicy
import java.net.URI
import org.openmoq.moq5.EndpointConfig
import org.openmoq.moq5.TrustPolicy
import org.openmoq.moq5.TrustPolicyUnsupportedException

/**
 * What the platform can say about a trust policy BEFORE an endpoint exists.
 *
 * Two seams, both INTERNAL and both defaulting to the device: the API level,
 * and the per-host certificate-transparency requirement. Host tests script
 * them; nothing here is application API, and nothing here claims what a device
 * answers -- only what the binding does with the answer.
 *
 * The rule (chats/libmoq/1312 section 6): below the API that has the CT query
 * no claim is made either way, so the query is never consulted; from that API
 * on, a host for which CT is required is refused before connecting, because
 * this transport presents no SCT or OCSP material the platform could verify.
 */
internal object PlatformTrustSupport {
    /** The API level `NetworkSecurityPolicy.isCertificateTransparencyVerificationRequired` first exists at. */
    private const val CT_API = 36

    private val deviceSdkInt: () -> Int = { Build.VERSION.SDK_INT }

    private val deviceCtRequired: (String) -> Boolean = { host ->
        // Guarded here as well as by the caller: the module declares no
        // minSdk, so lint treats the floor as 1 and the guard is required.
        if (Build.VERSION.SDK_INT >= CT_API) {
            NetworkSecurityPolicy.getInstance().isCertificateTransparencyVerificationRequired(host)
        } else {
            false
        }
    }

    @Volatile internal var sdkInt: () -> Int = deviceSdkInt

    @Volatile internal var ctRequired: (String) -> Boolean = deviceCtRequired

    internal fun reset() {
        sdkInt = deviceSdkInt
        ctRequired = deviceCtRequired
    }

    /** The refusal [config] earns before construction, or null to proceed. */
    internal fun refusalFor(config: EndpointConfig): TrustPolicyUnsupportedException? =
        when (config.trustPolicy) {
            TrustPolicy.PlatformTrustManager -> {
                if (sdkInt() < CT_API) {
                    null
                } else {
                    // No host, no question: an unparsable URL is the native
                    // route's to refuse, and no host is ever invented for it.
                    val host = hostOf(config.url)
                    if (host != null && ctRequired(host)) {
                        TrustPolicyUnsupportedException(
                            config.trustPolicy,
                            host,
                            "certificate transparency is required for this host, and this " +
                                "transport presents no SCT or OCSP material the platform could verify",
                        )
                    } else {
                        null
                    }
                }
            }
        }

    private fun hostOf(url: String): String? =
        try {
            URI(url).host
        } catch (_: Exception) {
            null
        }
}
