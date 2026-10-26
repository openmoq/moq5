package org.openmoq.moq5

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import org.openmoq.moq5.engine.EndpointCreateException
import org.openmoq.moq5.engine.JniEndpointBackend
import org.openmoq.moq5.jni.BridgeInit
import org.openmoq.moq5.jni.BridgeInitializer
import org.openmoq.moq5.jni.NativeBridge
import org.openmoq.moq5.jni.NativeImage

/**
 * The public initialization-failure contract, judged on a JVM that has loaded
 * NO native library.
 *
 * That absence is the oracle for "zero endpoint-native creates": on this JVM
 * every `external` row throws `UnsatisfiedLinkError` when called, so a
 * creation path that reached native code could not throw the typed failure --
 * it would throw the link error. The setup row below proves the precondition
 * rather than assuming it.
 */
class PublicNativeBridgeInitializationTest {

    private val url = "https://relay.example:4433/moq"

    /** Every failure case, each with values that would betray narrowing or reordering. */
    private val loadFailed = BridgeInit.LoadFailed("moq5_jni", null, "UnsatisfiedLinkError: dlopen failed")
    private val loadFailedSeen = BridgeInit.LoadFailed("/x/libmoq5_jni.so", true, "refused")
    private val registrationFailed = BridgeInit.RegistrationFailed("nativeFeatureBits", "unlinked")
    private val abiMismatch = BridgeInit.AbiMismatch(NativeBridge.ABI_EPOCH, NativeBridge.ABI_EPOCH + 7, 0x000100)
    private val missingWide =
        BridgeInit.MissingFeatures(
            required = (1L shl 40) or 1L,
            observed = Long.MIN_VALUE, // bit 63 alone: a 32-bit narrowing zeroes it
            missing = (1L shl 40) or 1L,
        )

    @Test
    fun `setup - no native library is loaded in this JVM`() {
        assertFailsWith<UnsatisfiedLinkError> { NativeBridge.nativeAbiEpoch() }
    }

    @Test
    fun `the public failure vocabulary has exactly four cases`() {
        val iface = NativeBridgeInitializationFailure::class.java
        // Sealed: the JVM records the permitted subclasses when the compiler
        // targets a class-file level that has them; otherwise the nested
        // classes are the vocabulary. Both views must agree on FOUR cases.
        val expected = listOf("AbiMismatch", "LoadFailed", "MissingFeatures", "RegistrationFailed")
        val nested = iface.classes.map { it.simpleName }.sorted()
        assertEquals(expected, nested, "the nested cases of NativeBridgeInitializationFailure")
        iface.permittedSubclasses?.map { it.simpleName }?.sorted()?.let { permitted ->
            assertEquals(expected, permitted, "the permitted subclasses of NativeBridgeInitializationFailure")
        }
        // And the compiler's own view: this `when` has no `else`, so a fifth
        // case fails to compile here rather than slipping past the list above.
        fun name(f: NativeBridgeInitializationFailure): String =
            when (f) {
                is NativeBridgeInitializationFailure.LoadFailed -> "LoadFailed"
                is NativeBridgeInitializationFailure.RegistrationFailed -> "RegistrationFailed"
                is NativeBridgeInitializationFailure.AbiMismatch -> "AbiMismatch"
                is NativeBridgeInitializationFailure.MissingFeatures -> "MissingFeatures"
            }
        assertEquals(
            expected,
            listOf(loadFailed, registrationFailed, abiMismatch, missingWide)
                .map { name(it.toInitializationFailure()) }.sorted(),
        )
    }

    @Test
    fun `each internal failure maps to its own public class with every field intact`() {
        val a = loadFailed.toInitializationFailure()
        assertEquals(NativeBridgeInitializationFailure.LoadFailed("moq5_jni", null, "UnsatisfiedLinkError: dlopen failed"), a)
        val a2 = loadFailedSeen.toInitializationFailure()
        assertEquals(NativeBridgeInitializationFailure.LoadFailed("/x/libmoq5_jni.so", true, "refused"), a2)

        val b = registrationFailed.toInitializationFailure()
        assertEquals(NativeBridgeInitializationFailure.RegistrationFailed("nativeFeatureBits", "unlinked"), b)

        val c = abiMismatch.toInitializationFailure()
        assertEquals(
            NativeBridgeInitializationFailure.AbiMismatch(NativeBridge.ABI_EPOCH, NativeBridge.ABI_EPOCH + 7, 0x000100),
            c,
        )

        val d = missingWide.toInitializationFailure()
        assertEquals(
            NativeBridgeInitializationFailure.MissingFeatures((1L shl 40) or 1L, Long.MIN_VALUE, (1L shl 40) or 1L),
            d,
        )

        val classes = listOf(a, b, c, d).map { it::class }
        assertEquals(4, classes.toSet().size, "no two internal failures may collapse into one public class")
    }

    @Test
    fun `the feature masks cross at full 64-bit width`() {
        val d = missingWide.toInitializationFailure() as NativeBridgeInitializationFailure.MissingFeatures
        assertNotEquals(0L, d.observed and Long.MIN_VALUE, "bit 63 must survive")
        assertNotEquals(0L, d.required and (1L shl 40), "bit 40 must survive")
        assertNotEquals(0L, d.missing and (1L shl 40), "bit 40 must survive in the missing mask")
    }

    @Test
    fun `a ready bridge is not a failure and refuses translation`() {
        assertFailsWith<IllegalArgumentException> {
            BridgeInit.Ready(NativeBridge.ABI_EPOCH, NativeBridge.FEATURES_REQUIRED, 1).toInitializationFailure()
        }
    }

    @Test
    fun `an unready bridge refuses the ordinary creation row before any native call`() {
        val e = assertFailsWith<NativeBridgeInitializationException> {
            JniEndpointBackend.create(url) { abiMismatch }
        }
        assertEquals(abiMismatch.toInitializationFailure(), e.failure)
    }

    @Test
    fun `an unready bridge refuses the trusted creation row before any native call`() {
        for (decision in listOf(loadFailed, registrationFailed, abiMismatch, missingWide)) {
            val e = assertFailsWith<NativeBridgeInitializationException>("for $decision") {
                JniEndpointBackend.createTrusted(url, 0, NativeEndpointOffer(0, emptyList()), 0L, 0, null) { decision }
            }
            assertEquals(decision.toInitializationFailure(), e.failure, "every field must cross for $decision")
        }
    }

    @Test
    fun `repeated creations report the first decision and load exactly once`() {
        var loads = 0
        val image =
            NativeImage(
                "moq5_jni",
                present = { null },
                loader = { loads++; throw UnsatisfiedLinkError("no such library") },
            )
        val initializer = BridgeInitializer()
        val first = assertFailsWith<NativeBridgeInitializationException> {
            JniEndpointBackend.create(url) { initializer.initialize(image) }
        }
        val second = assertFailsWith<NativeBridgeInitializationException> {
            JniEndpointBackend.createTrusted(url, 0, NativeEndpointOffer(0, emptyList()), 0L, 0, null) {
                initializer.initialize(image)
            }
        }
        assertEquals(1, loads, "a decided failure is never retried into a different answer")
        assertEquals(first.failure, second.failure)
        assertEquals(
            NativeBridgeInitializationFailure.LoadFailed("moq5_jni", null, "UnsatisfiedLinkError: no such library"),
            first.failure,
        )
    }

    @Test
    fun `open passes the initialization failure through unchanged`() = runBlocking {
        val thrown = NativeBridgeInitializationException(abiMismatch.toInitializationFailure())
        val seen = assertFailsWith<NativeBridgeInitializationException> {
            MoqEndpoint.open(EndpointConfig(url)) { throw thrown }
        }
        assertSame(thrown, seen, "open must neither wrap nor re-create the public failure")
    }

    @Test
    fun `a native creation failure after a ready bridge stays in its own domain`() = runBlocking {
        val e = assertFailsWith<EndpointCreateFailedException> {
            MoqEndpoint.open(EndpointConfig(url)) { throw EndpointCreateException(-14) }
        }
        assertEquals(-14, e.nativeCode)
    }
}
