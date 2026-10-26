package org.openmoq.moq5.jni

import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * The trusted creation row against each REAL unready host object, one child
 * process per variant so no earlier load can mask the outcome. Each row pins
 * the public class AND the exact values the decision was made on, printed by
 * the child from the public `failure` value, so a mapping that dropped or
 * renumbered a field fails here against the shipped substrate rather than
 * against a hand-built internal value.
 */
class BridgeUnreadyCreateTest {

    private fun run(variant: String, expectedFailure: String) {
        val out = EndpointAbi.child("createunready", null, mapOf("MOQ5_BRIDGE_VARIANT" to variant))
        TrustedChild.assertOk(out, "creation over $variant must refuse with the typed public failure")
        val line = out.firstOrNull { it.startsWith("FAILURE ") }
        assertTrue(line != null, "the child must print the public failure; output:\n" + out.joinToString("\n"))
        assertTrue(
            line == "FAILURE $expectedFailure",
            "expected exactly\n  FAILURE $expectedFailure\ngot\n  $line",
        )
    }

    @Test
    fun `a newer native ABI epoch refuses creation as an ABI mismatch`() {
        val out = EndpointAbi.child("createunready", null, mapOf("MOQ5_BRIDGE_VARIANT" to "bridge_epoch_high"))
        TrustedChild.assertOk(out, "creation over bridge_epoch_high must refuse with the typed public failure")
        val line = out.firstOrNull { it.startsWith("FAILURE ") }.orEmpty()
        // The diagnostic version is MOQ_VERSION_NUMBER of the substrate build;
        // its exact value is the C header's, so the row pins the epoch pair
        // exactly and requires the version to have crossed as a positive value.
        val expected =
            Regex(
                "FAILURE AbiMismatch\\(expected=${NativeBridge.ABI_EPOCH}, observed=${NativeBridge.ABI_EPOCH + 1}, " +
                    "nativeLibmoqVersion=[1-9][0-9]*\\)",
            )
        assertTrue(expected.matches(line), "expected $expected; got: $line")
    }

    @Test
    fun `an older native ABI epoch refuses creation as an ABI mismatch`() {
        val out = EndpointAbi.child("createunready", null, mapOf("MOQ5_BRIDGE_VARIANT" to "bridge_epoch_low"))
        TrustedChild.assertOk(out, "creation over bridge_epoch_low must refuse with the typed public failure")
        val line = out.firstOrNull { it.startsWith("FAILURE ") }.orEmpty()
        val expected =
            Regex(
                "FAILURE AbiMismatch\\(expected=${NativeBridge.ABI_EPOCH}, observed=${NativeBridge.ABI_EPOCH - 1}, " +
                    "nativeLibmoqVersion=[1-9][0-9]*\\)",
            )
        assertTrue(expected.matches(line), "expected $expected; got: $line")
    }

    @Test
    fun `an absent required feature refuses creation as missing features`() =
        run(
            "bridge_features_absent",
            "MissingFeatures(required=${NativeBridge.FEATURES_REQUIRED}, observed=0, " +
                "missing=${NativeBridge.FEATURES_REQUIRED})",
        )

    @Test
    fun `an unbound row refuses creation as a registration failure`() {
        val out = EndpointAbi.child("createunready", null, mapOf("MOQ5_BRIDGE_VARIANT" to "bridge_omit_one"))
        TrustedChild.assertOk(out, "creation over bridge_omit_one must refuse with the typed public failure")
        val line = out.firstOrNull { it.startsWith("FAILURE ") }.orEmpty()
        assertTrue(
            line.startsWith("FAILURE RegistrationFailed(method=nativeFeatureBits, detail="),
            "expected a RegistrationFailed naming nativeFeatureBits; got: $line",
        )
    }

    @Test
    fun `a refused load refuses creation as a load failure that saw the object`() {
        val out = EndpointAbi.child("createunready", null, mapOf("MOQ5_BRIDGE_VARIANT" to "bridge_bad_descriptor"))
        TrustedChild.assertOk(out, "creation over bridge_bad_descriptor must refuse with the typed public failure")
        val line = out.firstOrNull { it.startsWith("FAILURE ") }.orEmpty()
        val path = BridgeDsos.path("bridge_bad_descriptor")
        assertTrue(
            line.startsWith("FAILURE LoadFailed(libraryName=$path, presentBeforeLoad=true, detail=UnsatisfiedLinkError: "),
            "expected a LoadFailed that provably saw the object at $path; got: $line",
        )
    }
}
