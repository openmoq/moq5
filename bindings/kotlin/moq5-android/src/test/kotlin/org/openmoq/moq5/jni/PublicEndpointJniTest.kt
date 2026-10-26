package org.openmoq.moq5.jni

import kotlin.test.Test

/**
 * The PUBLIC endpoint surface at the real JNI boundary (chats/libmoq/1380):
 * a default `EndpointConfig` must select `nativeEndpointCreateTrusted` with
 * ordinal 0 -- observed as the trusted route entered with the library delegate
 * -- and a certificate-transparency refusal must precede every native step.
 *
 * The nonzero-ordinal refusal itself is pinned by `TrustedCreateRouteTest`
 * (`trustedroute`: policies 1, -1, INT_MAX, INT_MIN refused with INVAL before
 * the route, zero reservations, zero constructions); the public surface cannot
 * express a nonzero ordinal at all, which `PublicEndpointTrustTest` pins as the
 * one-case vocabulary. The discriminator added here is that the DEFAULT public
 * path reaches the trusted route, not the ordinary one.
 */
class PublicEndpointJniTest {

    @Test
    fun `a public endpoint opened with the default config selects the trusted native create path`() {
        val out = EndpointAbi.child("publicopen", null, TrustedChild.probeArmed)
        TrustedChild.assertOk(
            out,
            "MoqEndpoint.open(EndpointConfig(url)) must enter the trusted route with the library " +
                "delegate, reserve once, construct once, and surface the native refusal as " +
                "EndpointCreateFailedException with the exact code",
        )
    }

    @Test
    fun `a fresh process opens a public endpoint with no explicit bridge initialization`() {
        val out = EndpointAbi.child("publicopenfresh", null)
        TrustedChild.assertOk(
            out,
            "MoqEndpoint.open must decide the process-wide bridge itself and then surface the " +
                "native refusal as EndpointCreateFailedException with the exact code",
        )
    }

    @Test
    fun `at API 36 with CT required the public open refuses before any native step`() {
        val out = EndpointAbi.child("publicct", null, TrustedChild.probeArmed)
        TrustedChild.assertOk(
            out,
            "a certificate-transparency requirement the policy cannot represent must refuse " +
                "before the route, the reservation and the construction",
        )
    }
}
