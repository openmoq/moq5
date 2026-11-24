package org.openmoq.moq5

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * The receiver-terminal domain: `moq_media_receiver_fatal_code` is the
 * receiver's OWN code (`MOQ_MEDIA_RECEIVER_FATAL_*`), or the endpoint's when
 * the endpoint failed -- it is not a MoQ wire error code and must never be
 * spelt as one. `ReceiverTerminal.Fatal` and `ReceiverFatalException` take a
 * [ReceiverFatalCode] only; that is a compile-time fact these rows pin by
 * constructing them so (a `MoqErrorCode` parameter would not compile them).
 * The JNI carrier is a `jlong` holding all 64 raw bits.
 *
 * The description's open sets and its key material: [InitSource] and
 * [CatalogRevision] carry every native value, an unknown one as `Unknown`,
 * never refused or folded into a known case; [TrackEncryption] keeps its four
 * MSF fields verbatim and never renders the track base key.
 */
internal class PublicReceiverDomainTest {
    @Test
    fun `the receiver fatal code is its own value type, never equal to a wire error code with the same bits`() {
        val receiver: Any = ReceiverFatalCode(0x2uL)
        val wire: Any = MoqErrorCode(0x2uL)
        assertNotEquals(wire, receiver)
        assertEquals(ReceiverTerminal.Fatal(ReceiverFatalCode(0x2uL)), ReceiverTerminal.Fatal(ReceiverFatalCode(0x2uL)))
        assertEquals(ReceiverFatalCode(0x2uL), ReceiverFatalException(ReceiverFatalCode(0x2uL)).code)
        assertEquals("ReceiverFatalCode(2)", ReceiverFatalCode(0x2uL).toString())
    }

    @Test
    fun `the well-known receiver codes read by name`() {
        assertEquals(ReceiverFatalCode.CATALOG_UNUSABLE, ReceiverFatalCode(0x1uL))
        assertEquals(ReceiverFatalCode.EVENT_OVERFLOW, ReceiverFatalCode(0x2uL))
        assertEquals(ReceiverFatalCode.SETUP_FAILED, ReceiverFatalCode(0x3uL))
        assertEquals(ReceiverFatalCode.CATALOG_REJECTED, ReceiverFatalCode(0x4uL))
    }

    @Test
    fun `a high-bit native value round-trips through the jlong carrier as ULong`() {
        assertEquals(ULong.MAX_VALUE, ReceiverFatalCode.fromNative(-1L).value)
        assertEquals(0x8000_0000_0000_0000uL, ReceiverFatalCode.fromNative(Long.MIN_VALUE).value)
        assertEquals(0x2uL, ReceiverFatalCode.fromNative(2L).value)
        assertTrue(ReceiverFatalCode.fromNative(-1L) != ReceiverFatalCode(0x1uL), "never signed-compared or narrowed")
    }

    @Test
    fun `the init source names the four C sources and carries any other value as Unknown`() {
        assertEquals(
            listOf(InitSource.None, InitSource.Inline, InitSource.TrackProperty, InitSource.ObjectProperty),
            listOf(0u, 1u, 2u, 3u).map { InitSource.ofNative(it) },
        )
        assertEquals(InitSource.Unknown(4u), InitSource.ofNative(4u))
        assertEquals(InitSource.Unknown(UInt.MAX_VALUE), InitSource.ofNative(UInt.MAX_VALUE))
        assertNotEquals<InitSource>(InitSource.Unknown(1u), InitSource.Inline, "an unknown wrapper is never a known source")
        val d = TrackDescription(name = MediaBytes("v".toByteArray()), initSource = InitSource.ObjectProperty, initPropertyType = 0x0FuL, configGeneration = 3u)
        assertEquals(InitSource.ObjectProperty, d.initSource)
        assertEquals(0x0FuL, d.initPropertyType)
        assertEquals(3u, d.configGeneration)
        val plain = TrackDescription(name = MediaBytes("v".toByteArray()))
        assertEquals(listOf<Any?>(null, null, null, null), listOf(plain.initSource, plain.initPropertyType, plain.configGeneration, plain.encryption), "unreported is null, never a zero")
    }

    @Test
    fun `the catalog revision names MSF-01 and MSF-02 and carries any other value as Unknown`() {
        assertEquals(CatalogRevision.Msf01, CatalogRevision.ofNative(0u))
        assertEquals(CatalogRevision.Msf02, CatalogRevision.ofNative(1u))
        assertEquals(CatalogRevision.Unknown(2u), CatalogRevision.ofNative(2u))
    }

    @Test
    fun `the encryption value is the catalog's four fields by content, and its rendering never shows the track base key`() {
        val key = byteArrayOf(0x13, 0x37, 0x42, 0x7f)
        val e = TrackEncryption(MediaBytes("moq-secure-objects".toByteArray()), MediaBytes("aes-128-gcm-sha256".toByteArray()), MediaBytes(byteArrayOf(1)), MediaBytes(key))
        assertEquals(e, TrackEncryption(MediaBytes("moq-secure-objects".toByteArray()), MediaBytes("aes-128-gcm-sha256".toByteArray()), MediaBytes(byteArrayOf(1)), MediaBytes(key.copyOf())), "content equality")
        assertEquals(e.hashCode(), TrackEncryption(e.scheme, e.cipherSuite, e.keyId, MediaBytes(key.copyOf())).hashCode())
        assertNotEquals(e, TrackEncryption(e.scheme, e.cipherSuite, e.keyId, MediaBytes(byteArrayOf(0x13))), "the key is part of the value")
        assertNotEquals(e, TrackEncryption(e.scheme, e.cipherSuite, e.keyId, null))
        val d = TrackDescription(name = MediaBytes("v".toByteArray()), encryption = e)
        val rendered = listOf(e.toString(), d.toString())
        for (text in rendered) {
            assertTrue(!text.contains("1337427f"), "the key's bytes never appear: $text")
            assertTrue(text.contains("trackBaseKey=<redacted>"), "its presence does: $text")
            assertTrue(text.contains(MediaBytes("moq-secure-objects".toByteArray()).toString()), "the scheme is not secret: $text")
        }
        assertEquals("TrackEncryption(scheme=null, cipherSuite=null, keyId=null, trackBaseKey=null)", TrackEncryption().toString())
    }
}
