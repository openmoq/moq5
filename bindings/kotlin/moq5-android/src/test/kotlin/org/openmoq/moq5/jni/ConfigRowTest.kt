package org.openmoq.moq5.jni

import kotlin.test.Test

/**
 * The endpoint configuration through the real one-DSO bridge, observed by
 * the host recorder at the trusted route: the offer and the handshake
 * timeout the C tier receives are exactly what the public config said, and
 * the config carries its tail (the sized init). Scenarios in [EndpointChildMain].
 */
internal class ConfigRowTest {
    @Test
    fun `the default config reaches the route as an AUTO offer, no list, backend-default handshake, tail covered`() {
        TrustedChild.assertOk(EndpointAbi.child("publicopendefaults", null, TrustedChild.probeArmed),
            "the default EndpointConfig must cross as policy 0, no versions, handshake 0, with the cfg tail covered")
    }

    @Test
    fun `an explicit offer and handshake timeout reach the route verbatim and in order`() {
        TrustedChild.assertOk(EndpointAbi.child("publicopenconfigured", null, TrustedChild.probeArmed),
            "Offer([Draft18, Draft16]) + 2.5 s must cross as policy 1, [18, 16], 2500000 us")
    }

    @Test
    fun `an exact offer reaches the route as POLICY_EXACT with its one code`() {
        TrustedChild.assertOk(EndpointAbi.child("publicopenexact", null, TrustedChild.probeArmed),
            "Exactly(Draft18) must cross as policy 2, [18]")
    }

    @Test
    fun `the largest offerable code reaches the route verbatim and is not malformed to the bridge`() {
        TrustedChild.assertOk(EndpointAbi.child("publicopenwide", null, TrustedChild.probeArmed),
            "Exactly(MoqVersion(Int.MAX_VALUE)) must cross as policy 2, [2147483647]")
    }

    @Test
    fun `the row refuses every malformed offer before any reservation and passes an unknown positive code through`() {
        TrustedChild.assertOk(EndpointAbi.child("createmalformed", null, TrustedChild.probeArmed),
            "oversized, duplicate, empty-LIST, bad-EXACT, AUTO-with-list, non-positive code, bad ordinal and null array " +
                "are MOQ_ERR_INVAL before reservation and route with out[] untouched; an unknown positive code reaches the route")
    }

    @Test
    fun `the row refuses a negative handshake timeout before any reservation`() {
        TrustedChild.assertOk(EndpointAbi.child("createnegativehandshake", null, TrustedChild.probeArmed),
            "a negative jlong is MOQ_ERR_INVAL before the registry reserves anything")
    }
}
