package org.openmoq.moq5

import kotlinx.coroutines.runBlocking
import org.openmoq.moq5.engine.EndpointBackend
import org.openmoq.moq5.engine.EndpointCreateException
import org.openmoq.moq5.engine.EngineFixture
import org.openmoq.moq5.trust.PlatformTrustSupport
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The PUBLIC endpoint trust surface (chats/libmoq/1380, step 5 of 1312),
 * RED-first.
 *
 *     public data class EndpointConfig(url, trustPolicy = TrustPolicy.PlatformTrustManager)
 *     public sealed interface TrustPolicy { data object PlatformTrustManager }
 *
 * `PlatformTrustManager` is the only policy this slice admits. Insecure, PEM
 * bundles, custom roots and application callbacks are not representable from
 * Kotlin; the internal C route independently refuses a delegate together with
 * insecure or a CA file, and that defence is not what these rows lean on.
 *
 * The rows here drive [MoqEndpoint] through its INTERNAL backend seam over the
 * scripted engine backend, so what they judge is the wrapper's own contract:
 * which policy is selected, when construction is refused before any native
 * work, how a native creation failure surfaces, and that cleanup preserves the
 * engine's exact stop/destroy contract. The JNI boundary -- that the default
 * config reaches `nativeEndpointCreateTrusted` with ordinal 0 -- is judged in
 * `PublicEndpointJniTest` against the real product image.
 *
 * Android API behaviour goes through [PlatformTrustSupport], a host-testable
 * INTERNAL seam for the API level and the per-host certificate-transparency
 * requirement. It is not application API, and nothing here claims what a
 * device answers: only what the wrapper does with the answer.
 */
internal class PublicEndpointTrustTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"
        const val HOST = "moq.example"

        /** The API level `NetworkSecurityPolicy.isCertificateTransparencyVerificationRequired` first exists at. */
        const val CT_API = 36
    }

    @AfterTest
    fun restorePlatform() = PlatformTrustSupport.reset()

    // -- 1. the default -------------------------------------------------------

    @Test
    fun `EndpointConfig defaults to the platform trust manager`() {
        assertEquals(
            TrustPolicy.PlatformTrustManager,
            EndpointConfig(URL).trustPolicy,
            "an EndpointConfig that names only a URL must trust through the platform",
        )
        assertEquals(URL, EndpointConfig(URL).url)
    }

    // -- 2. exactly one case --------------------------------------------------

    @Test
    fun `the public trust vocabulary has exactly one case`() {
        val iface = TrustPolicy::class.java
        // Sealed: the JVM records the permitted subclasses when the compiler
        // targets a class-file level that has them; otherwise the nested
        // classes are the vocabulary. Both views must agree on ONE case.
        val permitted = iface.permittedSubclasses?.map { it.simpleName }?.sorted()
        val nested = iface.classes.map { it.simpleName }.sorted()
        assertEquals(listOf("PlatformTrustManager"), nested, "the nested cases of TrustPolicy")
        if (permitted != null) {
            assertEquals(listOf("PlatformTrustManager"), permitted, "the permitted subclasses of TrustPolicy")
        }
        val forbidden = listOf("insecure", "pem", "custom", "root", "callback", "hook", "default")
        for (name in nested) {
            for (f in forbidden) {
                assertTrue(
                    !name.contains(f, ignoreCase = true),
                    "no insecure, PEM, custom-root, callback or 'default' policy may exist: $name",
                )
            }
        }
        // The one case is a value, not a factory for application input.
        assertTrue(
            TrustPolicy.PlatformTrustManager::class.java.declaredFields.none { !it.isSynthetic && it.name != "INSTANCE" },
            "PlatformTrustManager carries no application-supplied state",
        )
    }

    // -- 5. Android API behaviour, through the internal seam -----------------

    @Test
    fun `below API 36 no certificate-transparency claim is made and construction proceeds`() =
        runTest {
            PlatformTrustSupport.sdkInt = { CT_API - 1 }
            var ctQueried = false
            PlatformTrustSupport.ctRequired = { ctQueried = true; true } // would refuse if it were consulted
            val backend = newBackend()
            var factoryHost: String? = null
            val endpoint =
                MoqEndpoint.open(EndpointConfig(URL)) { cfg ->
                    factoryHost = cfg.url
                    backend
                }
            assertTrue(!ctQueried, "below API $CT_API the CT query does not exist and must not be consulted")
            assertEquals(URL, factoryHost, "the configuration must reach the backend factory unchanged")
            endpoint.closeAndJoin()
            assertTeardownOnce(backend)
        }

    @Test
    fun `at API 36 with CT not required for the host construction proceeds`() =
        runTest {
            PlatformTrustSupport.sdkInt = { CT_API }
            var askedHost: String? = null
            PlatformTrustSupport.ctRequired = { askedHost = it; false }
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            assertEquals(HOST, askedHost, "the CT requirement is asked for the URL's HOST, exactly")
            endpoint.closeAndJoin()
            assertTeardownOnce(backend)
        }

    @Test
    fun `at API 36 with CT required for the host construction fails closed before any native work`() =
        runTest {
            PlatformTrustSupport.sdkInt = { CT_API }
            PlatformTrustSupport.ctRequired = { it == HOST }
            val backend = newBackend()
            var factoryCalls = 0
            val refusal =
                assertFailsWith<TrustPolicyUnsupportedException> {
                    MoqEndpoint.open(EndpointConfig(URL)) { factoryCalls++; backend }
                }
            assertEquals(0, factoryCalls, "the backend factory must never run: refusal precedes construction")
            assertEquals(emptyList(), backend.observed().callOrder, "no native call of any kind")
            assertEquals(TrustPolicy.PlatformTrustManager, refusal.policy, "the refusal names the policy")
            assertEquals(HOST, refusal.host, "the refusal names the host")
            assertTrue(
                refusal.message.orEmpty().contains("certificate transparency", ignoreCase = true),
                "the cause must be named: certificate transparency is required for the host and this " +
                    "policy cannot represent it; message was '${refusal.message}'",
            )
        }

    @Test
    fun `a URL that yields no host is not judged for CT and is left to native validation`() =
        runTest {
            PlatformTrustSupport.sdkInt = { CT_API }
            var ctQueried = false
            PlatformTrustSupport.ctRequired = { ctQueried = true; true }
            val backend = newBackend()
            var factoryCalls = 0
            // No host can be parsed from these; the CT question has no subject,
            // so it is not asked, and the URL's validity is the native route's.
            for (url in listOf("", "moqt:///p", "not a url at all")) {
                val endpoint = MoqEndpoint.open(EndpointConfig(url)) { factoryCalls++; backend }
                endpoint.closeAndJoin()
            }
            assertTrue(!ctQueried, "no host, no CT query: a host must never be invented")
            assertEquals(3, factoryCalls, "construction proceeds to the native route, which owns URL validation")
        }

    // -- 6. a native creation failure is typed and immediate -----------------

    @Test
    fun `a native creation failure surfaces from open as a typed failure with the exact code`() =
        runTest {
            val backend = newBackend()
            val failure =
                assertFailsWith<EndpointCreateFailedException> {
                    MoqEndpoint.open(EndpointConfig(URL)) { throw EndpointCreateException(-14) }
                }
            assertEquals(-14, failure.nativeCode, "the native result must cross exactly")
            assertTrue(!failure.isBridgeRefusal, "-14 is an endpoint result, not a bridge refusal")
            assertEquals(MoqResultCode(-14), failure.endpointCode, "an endpoint result is also offered in its domain")
            assertEquals(
                emptyList(),
                backend.observed().callOrder,
                "a root that was never created cannot be stopped or destroyed",
            )
        }

    @Test
    fun `a private registry refusal at creation is also typed and not narrowed`() =
        runTest {
            // -1007 is MOQ5_ERR_REGISTRY_BASE + MOQ5_REG_ERR_CAPACITY: a
            // bridge-domain code. It is carried exactly and NOT presented as a
            // MoqResultCode the endpoint answered.
            val failure =
                assertFailsWith<EndpointCreateFailedException> {
                    MoqEndpoint.open(EndpointConfig(URL)) { throw EndpointCreateException(-1007) }
                }
            assertEquals(-1007, failure.nativeCode, "the raw native code is kept")
            assertTrue(failure.isBridgeRefusal, "a registry-space code is a bridge refusal, not an endpoint result")
            assertEquals(null, failure.endpointCode, "a bridge refusal has NO endpoint-domain code: it must not be narrowed into MoqResultCode")
        }

    // -- 7. cleanup preserves the engine contract ---------------------------

    @Test
    fun `close after a successful open stops then destroys exactly once and is idempotent`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val snap = endpoint.snapshot()
            assertTrue(snap.state == EndpointState.Connecting || snap.state == EndpointState.Closed)
            endpoint.closeAndJoin()
            endpoint.closeAndJoin()
            assertTeardownOnce(backend)
            // The immediate close is a plain function; the join is the
            // suspending resource boundary.
            val close = MoqEndpoint::class.java.methods.single { it.name == "close" }
            assertEquals(0, close.parameterCount, "close() is non-suspending: no continuation parameter")
        }

    @Test
    fun `open only returns once the root exists so a failure is never discovered later`() =
        runTest {
            // Neither of these may need a later snapshot() or close() to learn
            // that construction failed: open() itself resolves the outcome.
            val backend = newBackend()
            var produced = false
            val endpoint =
                MoqEndpoint.open(EndpointConfig(URL)) {
                    produced = true
                    backend
                }
            assertTrue(produced, "open() must not return before the backend factory has produced the root")
            endpoint.closeAndJoin()
        }

    private fun runTest(body: suspend () -> Unit) = runBlocking { body() }

    /**
     * One stop, one destroy, in that order, and every other backend call a
     * record read: the seed, plus exactly one per activity the engine's own
     * wakes (job submissions, the close) caused the wait to report.
     */
    private fun assertTeardownOnce(backend: org.openmoq.moq5.engine.ScriptedEndpointBackend) {
        val o = backend.observed()
        assertEquals(listOf("stop", "destroy"), o.callOrder.filter { it != "snapshot" },
            "one stop, one destroy, in order; saw ${o.callOrder}")
        val activities = o.waitReturns.count { it == org.openmoq.moq5.engine.BackendWaitResult.ACTIVITY }
        val explicit = o.callOrder.count { it == "snapshot" } - 1 - activities
        assertTrue(explicit >= 0, "records = seed + one per activity + explicit snapshot() calls; saw ${o.callOrder}")
    }

    @Suppress("unused")
    private fun EndpointBackend.unused() = Unit
}
