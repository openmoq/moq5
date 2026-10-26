package org.openmoq.moq5.jni

import kotlin.test.Test

/**
 * R6: a CMAF track may legally deliver a payload-free STATUS object.
 *
 * The C accepts `END_OF_GROUP` with no payload and returns before parsing any
 * media, and the receiver converts only `END_OF_TRACK` into a track event -- so
 * the object path really can carry `packaging=CMAF, status=END_OF_GROUP` with
 * every media field empty. This row pins that carrier field by field on the
 * real service, then decodes a second identical object through
 * `JniEndpointBackend.pollObject`, which is the call production makes.
 */
internal class CmafStatusObjectTest {
    @Test
    fun `a payload-free CMAF end-of-group object crosses the carrier empty and decodes through the production path`() {
        val env = TrustedChild.probeArmed + (EndpointAbi.MODE to EndpointAbi.MODE_DRAIN_PENDING)
        TrustedChild.assertOk(
            EndpointAbi.child("cmafstatusonly", EndpointAbi.SCRIPTED_TOKEN, env),
            "the C delivers CMAF/END_OF_GROUP with an empty payload, empty fragment, mdat 0,0 and no samples, " +
                "and the production decode surfaces it as a CMAF status object with empty media",
        )
    }
}
