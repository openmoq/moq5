package org.openmoq.moq5

import kotlinx.coroutines.runBlocking
import org.openmoq.moq5.engine.EngineFixture
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

/**
 * `EndpointConfig` parity with `moq_endpoint_cfg_t`, RED-first: the version
 * offer (plan §3a `VersionOffer`) and the transport handshake timeout (the
 * cfg tail field, §5a), both validated in Kotlin BEFORE any native call and
 * both crossing verbatim.
 *
 * Derived contract (`endpoint.h`):
 *  * `VersionOffer.Automatic` = a zero offer (policy AUTO, no list): the
 *    build's full set, never "newest and hope". `Offer(list)` = POLICY_LIST
 *    with exactly that list in that order (the peer/TLS picks); `Exactly(v)`
 *    = POLICY_EXACT with one version (mismatch is a fatal connect). Codes
 *    cross as `moq_version_t` values, which ARE the draft numbers (16, 18).
 *    An offered version whose profile is not compiled in is refused by the
 *    C tier at create with UNSUPPORTED, never downgraded. Kotlin validates
 *    what C cannot express or the bridge cannot carry: an empty list, a
 *    duplicate, more than the service cap, and a code outside the positive
 *    signed 32-bit width an offer crosses in (an OFFER rule only -- the
 *    version domain read back from the peer keeps its full unsigned width).
 *  * `handshakeTimeout: Duration?`: null leaves the backend's own value (C 0);
 *    a value must be a real duration, 1 µs .. INT64_MAX µs (C refuses larger
 *    with INVAL before any backend check; there is no infinite sentinel and
 *    nothing is clamped). `Duration.ZERO` and a sub-microsecond value are
 *    rejected rather than silently meaning "default" -- null already says
 *    that. It lives past the v0 floor, so the create row must use the SIZED
 *    init; the JNI rows pin that.
 *  * Everything else stays as ruled: no insecure / CA-file / custom-trust
 *    knobs (1382); backend and perspective are not exposed (Android links the
 *    picoquic family only, and C is client-only); protocol derives from the
 *    URL scheme (AUTO).
 */
internal class PublicEndpointConfigTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"
    }

    @Test
    fun `VersionOffer has exactly the three C policies and EndpointConfig defaults to Automatic and no handshake bound`() {
        // The cases, excluding the companion that carries the cap constant.
        assertEquals(setOf("Automatic", "Offer", "Exactly"), VersionOffer::class.java.declaredClasses.map { it.simpleName }.filter { it != "Companion" }.toSet())
        val cfg = EndpointConfig(URL)
        assertEquals(VersionOffer.Automatic, cfg.versions)
        assertEquals(null, cfg.handshakeTimeout)
        assertEquals(TrustPolicy.PlatformTrustManager, cfg.trustPolicy)
        // Still no trust knob beyond the one ruled case.
        val names = EndpointConfig::class.java.declaredFields.map { it.name.lowercase() }
        assertTrue(names.none { it.contains("insecure") || it.contains("cafile") || it.contains("sni") })
    }

    @Test
    fun `the offer crosses as the exact C policy and codes, in order`() {
        assertEquals(NativeEndpointOffer(0, emptyList()), EndpointConfig(URL).nativeOffer())
        assertEquals(
            NativeEndpointOffer(1, listOf(18, 16)),
            EndpointConfig(URL, versions = VersionOffer.Offer(listOf(MoqVersion.Draft18, MoqVersion.Draft16))).nativeOffer(),
        )
        assertEquals(NativeEndpointOffer(2, listOf(18)), EndpointConfig(URL, versions = VersionOffer.Exactly(MoqVersion.Draft18)).nativeOffer())
        // A future draft is an additive code, never an enum case.
        assertEquals(NativeEndpointOffer(2, listOf(20)), EndpointConfig(URL, versions = VersionOffer.Exactly(MoqVersion(20u))).nativeOffer())
    }

    @Test
    fun `an Offer snapshots the caller's list, so a later mutation cannot change what crosses`() {
        val source = mutableListOf(MoqVersion.Draft18, MoqVersion.Draft16)
        val cfg = EndpointConfig(URL, versions = VersionOffer.Offer(source))
        source.clear()
        source += MoqVersion(20u)
        assertEquals(NativeEndpointOffer(1, listOf(18, 16)), cfg.nativeOffer(), "the offer is the list as it was at construction")
    }

    @Test
    fun `an offer of nine distinct versions is rejected before the endpoint factory is called`() {
        assertFailsWith<IllegalArgumentException> { VersionOffer.Offer((16..24).map { MoqVersion(it.toUInt()) }) }
        assertEquals(8, VersionOffer.MAX_VERSIONS, "the service tier's own cap")
        var factoryCalls = 0
        val backend = newBackend()
        runBlocking {
            assertFailsWith<IllegalArgumentException> {
                MoqEndpoint.open(EndpointConfig(URL, versions = VersionOffer.Offer((16..24).map { MoqVersion(it.toUInt()) }))) { factoryCalls++; backend }
            }
        }
        assertEquals(0, factoryCalls)
        // Eight distinct versions are within the cap.
        assertEquals(8, VersionOffer.Offer((16..23).map { MoqVersion(it.toUInt()) }).versions.size)
    }

    @Test
    fun `the version domain keeps its full width, but an offer may only carry a positive signed 32-bit code`() {
        // Inbound/domain: unchanged, the full unsigned width constructs.
        val wide = MoqVersion(UInt.MAX_VALUE)
        assertEquals(UInt.MAX_VALUE, wide.code)
        // Offer: refused at construction, before any factory or native call.
        assertFailsWith<IllegalArgumentException> { VersionOffer.Exactly(wide) }
        assertFailsWith<IllegalArgumentException> { VersionOffer.Offer(listOf(wide)) }
        assertFailsWith<IllegalArgumentException> { VersionOffer.Offer(listOf(MoqVersion.Draft16, MoqVersion(Int.MAX_VALUE.toUInt() + 1u))) }
        var factoryCalls = 0
        val backend = newBackend()
        runBlocking {
            assertFailsWith<IllegalArgumentException> {
                MoqEndpoint.open(EndpointConfig(URL, versions = VersionOffer.Exactly(wide))) { factoryCalls++; backend }
            }
        }
        assertEquals(0, factoryCalls)
        // The largest offerable code crosses verbatim and positive.
        val top = MoqVersion(Int.MAX_VALUE.toUInt())
        assertEquals(NativeEndpointOffer(2, listOf(Int.MAX_VALUE)), EndpointConfig(URL, versions = VersionOffer.Exactly(top)).nativeOffer())
        assertEquals(NativeEndpointOffer(1, listOf(16, Int.MAX_VALUE)), EndpointConfig(URL, versions = VersionOffer.Offer(listOf(MoqVersion.Draft16, top))).nativeOffer())
        assertEquals(Int.MAX_VALUE.toUInt(), VersionOffer.MAX_OFFERABLE_CODE)
    }

    @Test
    fun `an empty or duplicated offer is rejected in Kotlin`() {
        assertFailsWith<IllegalArgumentException> { VersionOffer.Offer(emptyList()) }
        assertFailsWith<IllegalArgumentException> { VersionOffer.Offer(listOf(MoqVersion.Draft16, MoqVersion.Draft16)) }
    }

    @Test
    fun `the handshake timeout crosses as whole microseconds, null as the backend default`() {
        assertEquals(0L, EndpointConfig(URL).nativeHandshakeMicros())
        assertEquals(2_500_000L, EndpointConfig(URL, handshakeTimeout = 2500.milliseconds).nativeHandshakeMicros())
        assertEquals(1L, EndpointConfig(URL, handshakeTimeout = 1.microseconds).nativeHandshakeMicros())
    }

    @Test
    fun `a zero, negative, infinite or sub-microsecond handshake timeout is rejected before any factory call`() {
        for (bad in listOf(Duration.ZERO, (-1).seconds, Duration.INFINITE, -Duration.INFINITE, 500.nanoseconds)) {
            assertFailsWith<IllegalArgumentException>("$bad") { EndpointConfig(URL, handshakeTimeout = bad) }
        }
        var factoryCalls = 0
        val backend = newBackend()
        runBlocking {
            assertFailsWith<IllegalArgumentException> {
                MoqEndpoint.open(EndpointConfig(URL, versions = VersionOffer.Offer(listOf(MoqVersion.Draft16, MoqVersion.Draft16)))) { factoryCalls++; backend }
            }
        }
        assertEquals(0, factoryCalls, "validation precedes construction")
    }
}
