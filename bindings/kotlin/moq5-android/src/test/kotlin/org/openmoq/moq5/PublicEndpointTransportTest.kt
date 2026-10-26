package org.openmoq.moq5

import kotlinx.coroutines.runBlocking
import org.openmoq.moq5.engine.EngineFixture
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Transport selection on `EndpointConfig`, RED-first: the protocol override
 * and the WebTransport path (`moq_endpoint_cfg_t.protocol` / `.wt_path`).
 *
 * Derived contract (`endpoint.h` §5.1, `endpoint.c` resolve):
 *  * `TransportProtocol.Automatic` derives from the URL scheme -- `moqt://`
 *    or `moq://` is raw QUIC, `https://` is WebTransport. An explicit
 *    [TransportProtocol.RawQuic] / [TransportProtocol.WebTransport] WINS over
 *    the scheme by design (the field is the override knob; a scheme/field
 *    conflict is not an error). The URL itself must still parse in a
 *    recognised scheme -- the URL parser's job, left to the native route.
 *  * `webTransportPath`: null = no override (the C tier then uses the URL
 *    path, or "/moq" when the URL has none); a value is the exact request
 *    target the CONNECT carries. Kotlin validates the full RFC 3986
 *    `path-abempty [ "?" query ]` grammar the draft's URI rules name (§3.1.1,
 *    §3.1.3): rooted; `pchar` and `/` before the first `?`, plus `?` after it;
 *    complete `%HH` escapes; no `#`, control, space, DEL, non-ASCII, or
 *    printable characters RFC 3986 requires to be percent-encoded. The
 *    bridge applies the same grammar at its direct-call boundary. On raw QUIC
 *    the C tier ignores the path, so it crosses unchanged rather than being
 *    second-guessed.
 *  * Both cross on the one trusted create row, validated before any factory
 *    or native call. Backend selection stays unexposed (Android links the
 *    picoquic family only, which serves both protocols).
 */
internal class PublicEndpointTransportTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"
        const val WT_URL = "https://moq.example:4433/live"
    }

    @Test
    fun `TransportProtocol has exactly the three C values and EndpointConfig defaults to Automatic with no path override`() {
        assertEquals(setOf("Automatic", "RawQuic", "WebTransport"), TransportProtocol::class.java.declaredClasses.map { it.simpleName }.filter { it != "Companion" }.toSet())
        val cfg = EndpointConfig(URL)
        assertEquals(TransportProtocol.Automatic, cfg.protocol)
        assertEquals(null, cfg.webTransportPath)
    }

    @Test
    fun `the protocol crosses as the exact C ordinal`() {
        assertEquals(0, EndpointConfig(URL).nativeProtocol())
        assertEquals(1, EndpointConfig(URL, protocol = TransportProtocol.RawQuic).nativeProtocol())
        assertEquals(2, EndpointConfig(WT_URL, protocol = TransportProtocol.WebTransport).nativeProtocol())
        // The override is the knob: an explicit protocol against the other scheme is accepted here.
        assertEquals(2, EndpointConfig(URL, protocol = TransportProtocol.WebTransport).nativeProtocol())
    }

    @Test
    fun `a raw-QUIC override with a well-formed path passes public validation and crosses unchanged`() {
        val cfg = EndpointConfig(URL, protocol = TransportProtocol.RawQuic, webTransportPath = "/relay")
        assertEquals(1, cfg.nativeProtocol())
        assertEquals("/relay", cfg.nativeWebTransportPath(), "the path is not second-guessed against the protocol")
    }

    @Test
    fun `the WebTransport path crosses verbatim, null as no override`() {
        assertEquals(null, EndpointConfig(WT_URL).nativeWebTransportPath())
        assertEquals("/relay", EndpointConfig(WT_URL, webTransportPath = "/relay").nativeWebTransportPath())
        assertEquals("/a/b?c=d", EndpointConfig(WT_URL, webTransportPath = "/a/b?c=d").nativeWebTransportPath())
    }

    @Test
    fun `an empty, relative or NUL-bearing path is rejected before any factory call`() {
        val outsidePchar = listOf("/a\\b", "/a<b", "/a>b", "/a[b]", "/a^b", "/a|b", "/a`b", "/a{b}", "/a\"b", "/ok?x=[")
        for (bad in listOf("", "relay", "moq/", "/re\u0000lay", "/a%2", "/a%zz", "/caf\u00e9", "/a b", "/a\u0001b", "/a#b", "/a\u007fb") + outsidePchar) {
            assertFailsWith<IllegalArgumentException>("'$bad'") { EndpointConfig(WT_URL, webTransportPath = bad) }
        }
        // Well-formed targets: a query, complete escapes, a second '?' inside the query.
        for (good in listOf("/", "/relay", "/a/b?c=d", "/a%2Fb?x=1&y=%20", "/q?a=1?b=2", "/v1/track:0@edge/(x)!$&'*+,;=~-._", "/a/?b=c:d/e@f")) {
            assertEquals(good, EndpointConfig(WT_URL, webTransportPath = good).nativeWebTransportPath())
        }
        var factoryCalls = 0
        val backend = newBackend()
        runBlocking {
            assertFailsWith<IllegalArgumentException> {
                MoqEndpoint.open(EndpointConfig(WT_URL, webTransportPath = "relay")) { factoryCalls++; backend }
            }
        }
        assertEquals(0, factoryCalls, "validation precedes construction")
        assertTrue(EndpointConfig(WT_URL, webTransportPath = "/").webTransportPath == "/", "the root path is a path")
    }
}
