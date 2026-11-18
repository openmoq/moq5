package org.openmoq.moq5.jni

import kotlin.test.Test

/**
 * Transport selection through the real one-DSO bridge, observed by the host
 * recorder at the trusted route. Scenarios in [EndpointChildMain].
 */
internal class TransportRowTest {
    @Test
    fun `the default config reaches the route as AUTO with no path override`() {
        TrustedChild.assertOk(EndpointAbi.child("publicopentransportdefaults", null, TrustedChild.probeArmed),
            "the default EndpointConfig must cross as protocol 0 and no WebTransport path")
    }

    @Test
    fun `an explicit WebTransport protocol and path reach the route verbatim on an https URL`() {
        TrustedChild.assertOk(EndpointAbi.child("publicopentransport", null, TrustedChild.probeArmed),
            "WebTransport + \"/relay\" on https://… must cross as protocol 2 and the exact path")
    }

    @Test
    fun `an explicit raw-QUIC override crosses on an https URL, the override being the knob`() {
        TrustedChild.assertOk(EndpointAbi.child("publicopentransportoverride", null, TrustedChild.probeArmed),
            "RawQuic on https://… must cross as protocol 1; the scheme/field conflict is not an error")
    }

    @Test
    fun `a raw-QUIC override with a well-formed path reaches the route with the path present, not second-guessed`() {
        TrustedChild.assertOk(EndpointAbi.child("publicopentransportrawpath", null, TrustedChild.probeArmed),
            "RawQuic + \"/relay\" must cross as protocol 1 with cfg->wt_path = /relay")
    }

    @Test
    fun `the row refuses a bad protocol ordinal or a malformed path before any reservation, output untouched`() {
        TrustedChild.assertOk(EndpointAbi.child("createbadtransport", null, TrustedChild.probeArmed),
            "bad ordinals and every malformed request target (empty, relative, NUL, bad escape, non-ASCII, space, control, fragment) " +
                "are MOQ_ERR_INVAL before reservation and route; out[] untouched")
    }
}
