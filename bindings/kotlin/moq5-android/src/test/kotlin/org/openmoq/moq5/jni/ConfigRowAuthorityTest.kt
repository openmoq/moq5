package org.openmoq.moq5.jni

import java.io.File
import java.lang.reflect.Modifier
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The trusted create row carrying the endpoint configuration: the version
 * offer (policy + codes) and the handshake timeout in microseconds, so the
 * config reaches `moq_endpoint_connect_with_trust` verbatim. One create
 * path, one row; the descriptor changes, so the epoch moves.
 */
internal class ConfigRowAuthorityTest {
    private companion object {
        const val NAME = "nativeEndpointCreateTrusted"
        /** The configuration parameters the row must carry, as a descriptor prefix and suffix. */
        const val DESCRIPTOR_PREFIX = "(Ljava/lang/String;II[IJ"
        const val DESCRIPTOR_SUFFIX = "[J)I"
        const val EPOCH_WITH_ROW = 7
        val abiHeader = File("../jni/moq5_abi.h")
        val keepRules = File("consumer-rules.pro")
        val bridgeSource = File("../jni/moq5_jni.c")
        val serviceInternal = File("../../../service/src/endpoint_internal.h")

        fun define(file: File, name: String): Int? =
            Regex("""^#define $name\s+(\d+)\b""", RegexOption.MULTILINE).find(file.readText())?.groupValues?.get(1)?.toInt()
    }

    @Test
    fun `the offer cap on both bridge sides is the service tier's own MOQ_ENDPOINT_MAX_VERSIONS`() {
        assertTrue(serviceInternal.isFile, "setup: ${serviceInternal.absolutePath}")
        assertTrue(bridgeSource.isFile, "setup: ${bridgeSource.absolutePath}")
        val service = define(serviceInternal, "MOQ_ENDPOINT_MAX_VERSIONS")
        val bridge = define(bridgeSource, "MOQ5_MAX_VERSIONS")
        assertTrue(service != null, "endpoint_internal.h must define MOQ_ENDPOINT_MAX_VERSIONS as a literal")
        assertEquals(service, bridge, "the bridge's private MOQ5_MAX_VERSIONS mirrors the service cap")
        assertEquals(service, org.openmoq.moq5.VersionOffer.MAX_VERSIONS, "VersionOffer.MAX_VERSIONS mirrors the service cap")
    }

    @Test
    fun `the authority table carries the trusted create row with the configuration parameters`() {
        assertTrue(abiHeader.isFile, "setup: ${abiHeader.absolutePath}")
        val row = Regex("""X\(\s*$NAME\s*,\s*"([^"]*)"\s*\)""").find(abiHeader.readText())?.groupValues?.get(1)
        assertTrue(row != null && row.startsWith(DESCRIPTOR_PREFIX) && row.endsWith(DESCRIPTOR_SUFFIX),
            "MOQ5_ABI_METHODS must carry $NAME with the offer and handshake parameters first and out[] last; found $row")
    }

    @Test
    fun `the bound bytecode carries the row with the configured parameters`() {
        val m = NativeBridge::class.java.declaredMethods.filter { it.name == NAME }
        assertEquals(1, m.size, "exactly one $NAME on NativeBridge; found ${m.map { it.toGenericString() }}")
        val method = m.single()
        val params = method.parameterTypes.toList()
        assertEquals(listOf(String::class.java, Integer.TYPE, Integer.TYPE, IntArray::class.java, java.lang.Long.TYPE), params.take(5),
            "url, trust ordinal, version policy, versions, handshake micros lead the row")
        assertEquals(LongArray::class.java, params.last(), "out[] ends the row")
        assertEquals(Integer.TYPE, method.returnType)
        val mods = method.modifiers
        assertTrue(Modifier.isPublic(mods) && Modifier.isFinal(mods) && Modifier.isNative(mods) && !Modifier.isStatic(mods))
    }

    @Test
    fun `the shipped keep rule carries the configured row so it survives R8`() {
        assertTrue(keepRules.isFile, "setup: ${keepRules.absolutePath}")
        assertTrue(
            keepRules.readLines().any { Regex("""public final native int \Q$NAME\E\(java\.lang\.String, int, int, int\[\], long, .*long\[\]\);""").matches(it.trim()) },
            "consumer-rules.pro must keep $NAME with the configuration parameters",
        )
    }

    @Test
    fun `the ABI epoch advanced with the table on both sides`() {
        val declared = Regex("""^#define MOQ5_ABI_EPOCH (\d+)$""", RegexOption.MULTILINE)
            .find(abiHeader.readText())?.groupValues?.get(1)?.toInt()
        // The configured row moved the epoch to 7; later rows move it further.
        assertTrue(declared != null && declared >= EPOCH_WITH_ROW, "the authority's epoch is $declared, needs $EPOCH_WITH_ROW or later")
        assertEquals(declared, NativeBridge.ABI_EPOCH, "the Kotlin side's epoch")
    }
}
