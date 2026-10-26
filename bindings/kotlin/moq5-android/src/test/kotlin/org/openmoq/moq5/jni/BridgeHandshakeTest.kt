package org.openmoq.moq5.jni

import java.io.File
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * Shared resolution for the host shared objects built by
 * `tools/build-bridge-testdsos.sh`.
 *
 * `RegisterNatives` binds to a class inside ONE JVM and a later load replaces
 * what an earlier one bound, so a variant that omits or re-descriptors a row
 * would be masked by whatever ran before it. Each variant therefore lives in
 * its own test class and the test task forks per class; that isolation is what
 * makes the omission and descriptor rows mean anything.
 */
internal object BridgeDsos {

    private val dir: File = File("../.native/_bridge").absoluteFile

    /**
     * The shared object for [name], with the SETUP precondition proven
     * separately from anything the bridge does: an absent or unbuilt object is
     * a harness failure, named as such, and never a bridge diagnosis.
     */
    fun path(name: String): String {
        val ext = if (System.getProperty("os.name").orEmpty().startsWith("Mac")) "dylib" else "so"
        val f = File(dir, "lib$name.$ext")
        assertTrue(
            f.isFile,
            "setup: ${f.path} was not built -- run tools/build-bridge-testdsos.sh " +
                "(this is a harness failure, not a bridge failure)",
        )
        return f.path
    }

    /** A host object loaded from an explicit path -- test-only by design. */
    fun image(name: String): NativeImage = at(path(name))

    fun at(p: String, onLoad: (String) -> Unit = { System.load(it) }): NativeImage =
        NativeImage(p, present = { File(p).isFile }, loader = { onLoad(p) })

    /** A packaged-by-name image, whose presence is not knowable in advance. */
    fun packaged(name: String): NativeImage = NativeImage.packaged(name)
}

/**
 * THE PRODUCT ROW, and the RED.
 *
 * `libbridge_product` is `jni/moq5_jni.c` itself, compiled for the host with
 * only the LibMoQ anchor table omitted -- the registration and handshake code
 * is the shipped code. The object loads (setup proven), `JNI_OnLoad` returns
 * JNI 1.6, and it registers nothing, so the handshake cannot complete.
 */
internal class BridgeProductHandshakeTest {

    @Test
    fun `the shipped substrate registers its table and completes the handshake`() {
        val bridge = BridgeInitializer()
        val outcome = bridge.initialize(BridgeDsos.image("bridge_product"))

        assertTrue(
            outcome is BridgeInit.Ready,
            "the shipped JNI substrate did not complete the ABI handshake: $outcome",
        )
        assertTrue(bridge.isReady, "the bridge must report ready once the handshake succeeded")
        assertEquals(
            NativeBridge.ABI_EPOCH,
            (outcome as BridgeInit.Ready).abiEpoch,
            "the native side must report the authority's ABI epoch",
        )
        assertEquals(
            0L,
            NativeBridge.FEATURES_REQUIRED and outcome.features.inv(),
            "every required feature must be present",
        )
    }
}

/** Control: the oracle can recognise a compatible bridge. */
internal class BridgeCompatibleFixtureTest {

    @Test
    fun `a compatible native side reaches ready`() {
        val bridge = BridgeInitializer()
        val outcome = bridge.initialize(BridgeDsos.image("bridge_compatible"))

        assertTrue(outcome is BridgeInit.Ready, "the compatible fixture must be accepted: $outcome")
        assertTrue(bridge.isReady)
        assertEquals(NativeBridge.ABI_EPOCH, (outcome as BridgeInit.Ready).abiEpoch)
        assertEquals(NativeBridge.FEATURES_REQUIRED, outcome.features)
        assertTrue(
            outcome.libmoqVersion > 0,
            "the semantic LibMoQ version must cross as diagnostic input",
        )
    }
}

/** ABI skew, native NEWER than this Kotlin build. */
internal class BridgeAbiSkewHighTest {

    @Test
    fun `a newer native ABI epoch fails by mismatch`() {
        val bridge = BridgeInitializer()
        val outcome = bridge.initialize(BridgeDsos.image("bridge_epoch_high"))

        assertTrue(outcome is BridgeInit.AbiMismatch, "expected an ABI mismatch, got $outcome")
        outcome as BridgeInit.AbiMismatch
        assertEquals(NativeBridge.ABI_EPOCH, outcome.expected, "the expected epoch must be recorded")
        assertEquals(NativeBridge.ABI_EPOCH + 1, outcome.observed, "the observed epoch must be recorded")
        assertFalse(bridge.isReady, "an ABI mismatch may never report ready")
    }
}

/** ABI skew, native OLDER than this Kotlin build. */
internal class BridgeAbiSkewLowTest {

    @Test
    fun `an older native ABI epoch fails by mismatch`() {
        val bridge = BridgeInitializer()
        val outcome = bridge.initialize(BridgeDsos.image("bridge_epoch_low"))

        assertTrue(outcome is BridgeInit.AbiMismatch, "expected an ABI mismatch, got $outcome")
        outcome as BridgeInit.AbiMismatch
        assertEquals(NativeBridge.ABI_EPOCH, outcome.expected)
        assertEquals(NativeBridge.ABI_EPOCH - 1, outcome.observed)
        assertFalse(bridge.isReady)
    }

    @Test
    fun `epoch 16, whose object carrier had no identity scalars, is refused at the handshake`() {
        // 16 is the last epoch at which `nativeReceiverPollObject` filled 17
        // scalars. This Kotlin side reads 23; a native side still at 16 must
        // be refused before any row runs, never tolerated as "no metadata".
        assertTrue(NativeBridge.ABI_EPOCH > 16, "this binding must sit past epoch 16, is at ${NativeBridge.ABI_EPOCH}")
        assertEquals(17, NativeBridge.ABI_EPOCH, "the fixture one epoch back must be 16 for this row to hold; update the row with the next bump")
        val bridge = BridgeInitializer()
        val outcome = bridge.initialize(BridgeDsos.image("bridge_epoch_low"))
        assertTrue(outcome is BridgeInit.AbiMismatch, "expected an ABI mismatch, got $outcome")
        outcome as BridgeInit.AbiMismatch
        assertEquals(16, outcome.observed)
        assertEquals(NativeBridge.ABI_EPOCH, outcome.expected)
        assertFalse(bridge.isReady)
    }
}

/** A required capability is absent: fail closed. */
internal class BridgeMissingFeatureTest {

    @Test
    fun `a missing required feature fails closed`() {
        val bridge = BridgeInitializer()
        val outcome = bridge.initialize(BridgeDsos.image("bridge_features_absent"))

        assertTrue(outcome is BridgeInit.MissingFeatures, "expected missing features, got $outcome")
        outcome as BridgeInit.MissingFeatures
        assertEquals(NativeBridge.FEATURES_REQUIRED, outcome.required)
        assertEquals(0L, outcome.observed)
        assertEquals(
            NativeBridge.FEATURES_REQUIRED,
            outcome.missing,
            "the diagnostic must name exactly which required bits are absent",
        )
        assertFalse(bridge.isReady)
    }
}

/** An unknown ADDITIVE capability must not fail a compatible bridge. */
internal class BridgeUnknownFeatureTest {

    @Test
    fun `an unknown additive native feature stays compatible`() {
        val bridge = BridgeInitializer()
        val outcome = bridge.initialize(BridgeDsos.image("bridge_features_extra"))

        assertTrue(
            outcome is BridgeInit.Ready,
            "an unknown feature bit must not fail an otherwise compatible bridge: $outcome",
        )
        outcome as BridgeInit.Ready
        assertTrue(
            outcome.features and NativeBridge.FEATURES_KNOWN.inv() != 0L,
            "the fixture must actually advertise a bit outside the known mask",
        )
        assertTrue(bridge.isReady)
    }
}

/** One registration row removed. */
internal class BridgeOmittedRegistrationTest {

    @Test
    fun `an unregistered method is detected as a registration failure`() {
        val bridge = BridgeInitializer()
        val outcome = bridge.initialize(BridgeDsos.image("bridge_omit_one"))

        assertTrue(
            outcome is BridgeInit.RegistrationFailed,
            "expected a registration failure, got $outcome",
        )
        assertEquals(
            "nativeFeatureBits",
            (outcome as BridgeInit.RegistrationFailed).method,
            "the diagnostic must name the method that was not bound",
        )
        assertFalse(bridge.isReady, "a partially registered bridge may never report ready")
    }
}

/** Only the descriptor of one row changed. */
internal class BridgeDescriptorDriftTest {

    @Test
    fun `a descriptor change alone is detected`() {
        val bridge = BridgeInitializer()
        val outcome = bridge.initialize(BridgeDsos.image("bridge_bad_descriptor"))

        // JNI reports this at LOAD time, not on first call: `RegisterNatives`
        // itself fails when a descriptor matches no declared method, so
        // `JNI_OnLoad` returns JNI_ERR and the JVM refuses the object. The
        // outcome is therefore a refusal, and it is distinguishable from a
        // missing build output because the object provably exists.
        assertTrue(
            outcome is BridgeInit.LoadFailed,
            "a row bound with the wrong descriptor must not pass as registered: $outcome",
        )
        outcome as BridgeInit.LoadFailed
        assertEquals(
            true,
            outcome.presentBeforeLoad,
            "the object provably existed, so this is a refusal and not a missing build output",
        )
        // A detail thrown from JNI_OnLoad survives System.load on the real JVM,
        // so a registration failure is NOT allowed to collapse into a generic
        // "unsupported JNI version": it must name the row that failed.
        assertTrue(
            outcome.detail.contains("nativeFeatureBits"),
            "the refusal must name the method whose registration failed: ${outcome.detail}",
        )
        // The descriptor reported is the one the native side ATTEMPTED to bind
        // -- the perturbed "()I" -- not the authority's "()J". That is the
        // useful fact: it names what the native bytes actually asked for.
        assertTrue(
            outcome.detail.contains("nativeFeatureBits()I"),
            "the refusal must name the descriptor it tried to bind: ${outcome.detail}",
        )
        assertTrue(
            outcome.detail.contains(NativeBridge.let { "org/openmoq/moq5/jni/NativeBridge" }),
            "the refusal must name the bound class: ${outcome.detail}",
        )
        assertFalse(bridge.isReady)
    }
}

/** `JNI_OnLoad` refuses: nothing may come up half-ready. */
internal class BridgeRegistrationRefusedTest {

    @Test
    fun `a refused JNI_OnLoad cannot leave the bridge ready`() {
        // The setup precondition is proven first, so this cannot be confused
        // with a missing build output: the object exists, and the JVM rejects
        // it because its JNI_OnLoad returned JNI_ERR.
        val path = BridgeDsos.path("bridge_registration_fails")
        assertTrue(File(path).isFile, "setup: the refusing fixture was not built")

        val bridge = BridgeInitializer()
        val outcome = bridge.initialize(BridgeDsos.image("bridge_registration_fails"))

        assertTrue(
            outcome is BridgeInit.LoadFailed,
            "a JNI_OnLoad that refuses must not yield a usable bridge: $outcome",
        )
        assertEquals(
            true,
            (outcome as BridgeInit.LoadFailed).presentBeforeLoad,
            "the refusing object provably existed",
        )
        assertFalse(bridge.isReady, "a refused load may never report ready")
    }
}

/** An absent image is a SETUP failure and must not be reported as a bridge defect. */
internal class BridgeAbsentImageTest {

    @Test
    fun `an absent shared object is reported as setup, not as a bridge failure`() {
        val bridge = BridgeInitializer()
        val outcome = bridge.initialize(BridgeDsos.at("/nonexistent/libmoq5_absent.dylib"))

        assertTrue(outcome is BridgeInit.LoadFailed, "expected a setup failure, got $outcome")
        assertEquals(
            false,
            (outcome as BridgeInit.LoadFailed).presentBeforeLoad,
            "an absent path image must record that it was absent BEFORE the load",
        )
        assertFalse(bridge.isReady)
    }
}

/** One authoritative result: sticky, concurrent-safe, never silently retried. */
internal class BridgeInitializationIdentityTest {

    @Test
    fun `a prior failure stays the authoritative result`() {
        val bridge = BridgeInitializer()
        val first = bridge.initialize(BridgeDsos.image("bridge_epoch_high"))
        assertTrue(first is BridgeInit.AbiMismatch, "expected the skew to fail first: $first")

        // A second attempt -- even one that would succeed -- must not turn a
        // decided failure into readiness.
        val second = bridge.initialize(BridgeDsos.image("bridge_compatible"))
        assertEquals(first, second, "the first decided result must remain authoritative")
        assertFalse(bridge.isReady, "a failed handshake may never later report ready")
    }

    @Test
    fun `initialization is one barrier-controlled load with one authoritative result`() {
        val bridge = BridgeInitializer()
        val threads = 8
        val inLoad = CountDownLatch(1)
        val release = CountDownLatch(1)
        val start = CountDownLatch(1)
        val done = CountDownLatch(threads)
        val results = java.util.Collections.synchronizedList(mutableListOf<BridgeInit>())
        val thrown = java.util.Collections.synchronizedList(mutableListOf<Throwable>())
        val loads = java.util.concurrent.atomic.AtomicInteger(0)

        repeat(threads) {
            Thread {
                try {
                    start.await()
                    results +=
                        bridge.initialize(
                            BridgeDsos.at(BridgeDsos.path("bridge_compatible")) { p ->
                                loads.incrementAndGet()
                                inLoad.countDown()
                                // Hold the ONE load open, so readiness can be
                                // observed while it is still in flight.
                                release.await()
                                System.load(p)
                            },
                        )
                } catch (t: Throwable) {
                    // Captured, so a worker failure is reported as itself and
                    // never degrades into the generic "hung" diagnostic.
                    thrown += t
                } finally {
                    done.countDown()
                }
            }.apply { isDaemon = true }.start()
        }
        start.countDown()

        assertTrue(inLoad.await(30, TimeUnit.SECONDS), "the single load never started")
        assertFalse(bridge.isReady, "readiness must be false while the load is still in flight")
        assertEquals(null, bridge.outcome, "no outcome may be published before the load completes")
        assertEquals(1, loads.get(), "a second caller must not start a second load")

        release.countDown()
        assertTrue(done.await(30, TimeUnit.SECONDS), "concurrent init hung: ${thrown.toList()}")
        assertEquals(emptyList(), thrown.toList(), "a worker threw: ${thrown.toList()}")

        val seen = results.toList()
        assertEquals(threads, seen.size, "every caller must receive a result")
        assertEquals(1, seen.toSet().size, "every caller must receive the SAME result: $seen")
        assertEquals(1, loads.get(), "the shared object must be loaded exactly once")

        val one = seen.first()
        assertTrue(one is BridgeInit.Ready, "the compatible image must reach ready: $one")
        one as BridgeInit.Ready
        assertEquals(NativeBridge.ABI_EPOCH, one.abiEpoch, "the handshake epoch must be exact")
        assertEquals(NativeBridge.FEATURES_REQUIRED, one.features, "the features must be exact")
        assertTrue(one.libmoqVersion > 0, "the diagnostic version must cross")
        assertTrue(bridge.isReady, "readiness must be true once the handshake succeeded")
        assertEquals(one, bridge.outcome, "the published outcome must be the decided one")
    }
}

/** A non-linkage failure must propagate unchanged and publish no outcome. */
internal class BridgeFatalPropagationTest {

    private class MarkerError : Error("moq5-marker-fatal")

    @Test
    fun `a non-linkage error from the loader is not relabelled as a bridge failure`() {
        val bridge = BridgeInitializer()
        val image =
            NativeImage(
                "marker",
                present = { true },
                loader = { throw MarkerError() },
            )

        var caught: Throwable? = null
        try {
            bridge.initialize(image)
        } catch (t: Throwable) {
            caught = t
        }

        assertTrue(
            caught is MarkerError,
            "a VM/assertion-class error must propagate unchanged, not become a bridge outcome: $caught",
        )
        assertEquals(
            null,
            bridge.outcome,
            "no outcome may be published for a failure the bridge does not classify",
        )
        assertFalse(bridge.isReady)
    }
}

/** The feature transport is 64-bit end to end. */
internal class BridgeFeatureWidthTest {

    @Test
    fun `an additive feature bit above 32 bits survives the handshake`() {
        val bridge = BridgeInitializer()
        val outcome = bridge.initialize(BridgeDsos.image("bridge_features_wide"))

        assertTrue(
            outcome is BridgeInit.Ready,
            "a high additive bit must not fail an otherwise compatible bridge: $outcome",
        )
        outcome as BridgeInit.Ready
        val high = 0x4000000000L
        assertTrue(
            high > UInt.MAX_VALUE.toLong(),
            "the probe bit must sit above 32 bits to detect truncation",
        )
        assertEquals(
            high,
            outcome.features and high,
            "the bit above 32 bits was lost crossing JNI: features=${outcome.features}",
        )
        assertEquals(
            NativeBridge.FEATURES_REQUIRED,
            outcome.features and NativeBridge.FEATURES_KNOWN,
            "the known bits must survive alongside the unknown one",
        )
        assertTrue(bridge.isReady)
    }
}

/**
 * The PRODUCT entry, by packaged name, on a real JVM.
 *
 * `System.loadLibrary` resolves the name through the loader's own directories
 * (`java.library.path` here, the application's library directory on device), so
 * this exercises the shipped call rather than a stand-in. Nothing in production
 * code is altered to make it reachable.
 *
 * The callers RACE the first decision: every thread is released from one
 * barrier and no initialization has happened before them, so this measures
 * concurrent FIRST initialization rather than concurrent reads of a decision
 * that already existed.
 *
 * What it proves is that all callers receive one and the same decision. That
 * the shared initializer invokes its loader EXACTLY ONCE is proven
 * independently by the barrier-controlled load-count row in
 * [BridgeInitializationIdentityTest]; the two together are the exactly-once
 * claim, and neither is asked to carry the other's half.
 */
class BridgePackagedNameOwnerTest {

    @Test
    fun `concurrent first callers all receive one Ready decision`() {
        val threads = 8
        val start = CountDownLatch(1)
        val done = CountDownLatch(threads)
        val seen = java.util.Collections.synchronizedList(mutableListOf<BridgeInit>())
        val failures = java.util.Collections.synchronizedList(mutableListOf<Throwable>())

        repeat(threads) {
            Thread {
                try {
                    start.await()
                    seen.add(NativeBridgeOwner.initialize())
                } catch (t: Throwable) {
                    failures.add(t)
                } finally {
                    done.countDown()
                }
            }.start()
        }
        // Nothing has decided yet: the race is over the FIRST initialization.
        assertNull(NativeBridgeOwner.outcome, "a decision existed before the race began")
        start.countDown()
        assertTrue(done.await(30, TimeUnit.SECONDS), "threads did not finish")

        assertTrue(failures.isEmpty(), "a caller threw: ${failures.firstOrNull()}")
        assertEquals(threads, seen.size)
        val first = seen.first()
        assertTrue(first is BridgeInit.Ready, "the packaged library must be Ready, got $first")
        seen.forEach { assertSame(first, it, "callers received different decisions") }
        assertTrue(NativeBridgeOwner.isReady, "readiness must follow a Ready outcome")
        assertSame(first, NativeBridgeOwner.initialize(), "a later call must reuse the decision")
    }
}
