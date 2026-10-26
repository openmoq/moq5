package org.openmoq.moq5.jni

/**
 * The outcome of one bridge initialization. Typed, and carrying the values the
 * decision was made on, so a failure is diagnosable without a rebuild.
 */
internal sealed interface BridgeInit {

    /** Registration and the handshake both succeeded. */
    data class Ready(val abiEpoch: Int, val features: Long, val libmoqVersion: Int) : BridgeInit

    /**
     * The library did not load.
     *
     * There is deliberately ONE outcome here rather than a "missing" and a
     * "refused" pair. The JVM reports both as `UnsatisfiedLinkError`, and for a
     * library resolved by name out of the application's own directory the
     * caller cannot know which happened without parsing vendor message text --
     * which would turn a string into a causal claim. So the outcome carries
     * only what is actually knowable: [presentBeforeLoad] is `true`/`false`
     * when the image is an inspectable path and `null` when it is a packaged
     * name, plus the JVM's own [detail].
     */
    data class LoadFailed(
        val path: String,
        val presentBeforeLoad: Boolean?,
        val detail: String,
    ) : BridgeInit

    /**
     * The object loaded successfully, and an expected native method was still
     * unbound when it was called.
     *
     * It deliberately does NOT cover a bad descriptor or a `JNI_OnLoad`
     * refusal. Both of those fail the LOAD itself -- a refusing `JNI_OnLoad`
     * makes `System.loadLibrary` throw, and a descriptor the JVM cannot match
     * makes registration fail inside it -- so they surface honestly as
     * [LoadFailed]. For an image loaded by name there is no vantage point from
     * which the loader can tell those apart, and this taxonomy does not
     * pretend otherwise: what distinguishes this case is that the image loaded
     * and a call still found no implementation.
     */
    data class RegistrationFailed(val method: String, val detail: String) : BridgeInit

    /** The native side is built to a different private ABI epoch. */
    data class AbiMismatch(val expected: Int, val observed: Int, val libmoqVersion: Int) :
        BridgeInit

    /** A required capability is absent. */
    data class MissingFeatures(val required: Long, val observed: Long, val missing: Long) :
        BridgeInit
}

/**
 * One loadable native image: how it is named, whether it is present, and how to
 * load it.
 *
 * There is deliberately NO default loader. The product path is
 * [packaged], which resolves the library by name out of the application's own
 * library directory -- loading an arbitrary file path in shipped code is the
 * unsafe-dynamic-load pattern, and the only caller that needs it is a host test
 * loading an object it just built.
 */
internal class NativeImage(
    val path: String,
    /**
     * Whether the image is there, when that is knowable. `null` for a packaged
     * library resolved by name: the loader searches directories this code does
     * not own, so claiming presence would be a guess.
     */
    private val present: () -> Boolean?,
    private val loader: () -> Unit,
) {
    fun presentBeforeLoad(): Boolean? = present()

    fun load() = loader()

    internal companion object {
        /** The library packaged with the application, resolved by name. */
        fun packaged(name: String): NativeImage =
            NativeImage(name, present = { null }, loader = { System.loadLibrary(name) })
    }
}

/**
 * The private ABI/feature handshake, performed once before any handle or
 * endpoint operation can exist.
 *
 * One authoritative result: the first [initialize] decides, later calls return
 * that same result, and a prior FAILURE stays a failure -- it is never quietly
 * retried into a different answer, because a bridge that failed its handshake
 * has not become compatible in the meantime. [isReady] is false until the
 * handshake has actually succeeded, so readiness can never be observed early.
 *
 * The decision order is the contract: load, then read the epoch, the feature
 * mask and the diagnostic version, then compare. Readiness is published only
 * after every one of those has succeeded, so no future native operation can
 * observe a ready bridge before registration and the handshake are done.
 */
internal class BridgeInitializer {

    private val lock = Any()

    /**
     * The decided result, published once.
     *
     * `@Volatile` and read WITHOUT the lock on purpose: the deciding thread
     * holds the lock for the whole load, so a readiness query that took the
     * same lock would block behind an in-flight load -- and "is the bridge
     * ready" must be answerable at any moment, including while one caller is
     * still loading. Reads see either no decision or the one decision; there
     * is no intermediate state to observe because the field is written once,
     * after the handshake has finished.
     */
    @Volatile private var result: BridgeInit? = null

    /** True only once the handshake has succeeded. Never blocks. */
    val isReady: Boolean
        get() = result is BridgeInit.Ready

    /** The decided result, or null while no attempt has completed. Never blocks. */
    val outcome: BridgeInit?
        get() = result

    /**
     * Load [image], then handshake. Idempotent: the first completed attempt is
     * authoritative for every later call.
     */
    fun initialize(image: NativeImage): BridgeInit =
        synchronized(lock) {
            result?.let { return it }
            val decided = attempt(image)
            result = decided
            decided
        }

    private fun attempt(image: NativeImage): BridgeInit {
        // Recorded BEFORE the load, so the outcome says what was knowable at
        // the time rather than what a message string implies afterwards.
        val present = image.presentBeforeLoad()
        try {
            image.load()
        } catch (t: UnsatisfiedLinkError) {
            // ONLY the failure the loader API actually defines is caught here.
            // Catching Throwable would relabel an AssertionError, a VM error or
            // a cancellation as a bridge diagnosis and publish an outcome for
            // it; those must propagate untouched, leaving no decided result.
            return BridgeInit.LoadFailed(image.path, present, "UnsatisfiedLinkError: ${t.message}")
        } catch (t: SecurityException) {
            // Deliberately classified: a security manager refusing the load is
            // a load failure, not a bridge incompatibility.
            return BridgeInit.LoadFailed(image.path, present, "SecurityException: ${t.message}")
        }

        val epoch =
            try {
                NativeBridge.nativeAbiEpoch()
            } catch (t: UnsatisfiedLinkError) {
                return BridgeInit.RegistrationFailed("nativeAbiEpoch", t.message ?: "unlinked")
            }
        val features =
            try {
                NativeBridge.nativeFeatureBits()
            } catch (t: UnsatisfiedLinkError) {
                return BridgeInit.RegistrationFailed("nativeFeatureBits", t.message ?: "unlinked")
            }
        val libmoq =
            try {
                NativeBridge.nativeLibmoqVersion()
            } catch (t: UnsatisfiedLinkError) {
                return BridgeInit.RegistrationFailed(
                    "nativeLibmoqVersion",
                    t.message ?: "unlinked",
                )
            }

        if (epoch != NativeBridge.ABI_EPOCH) {
            return BridgeInit.AbiMismatch(NativeBridge.ABI_EPOCH, epoch, libmoq)
        }

        val missing = NativeBridge.FEATURES_REQUIRED and features.inv()
        if (missing != 0L) {
            return BridgeInit.MissingFeatures(NativeBridge.FEATURES_REQUIRED, features, missing)
        }

        return BridgeInit.Ready(epoch, features, libmoq)
    }
}


/**
 * The process-wide owner every native operation must pass through.
 *
 * The handshake decides ONCE per process: a second decision would let one part
 * of the app act on a bridge another part had already rejected. [BridgeInitializer]
 * is deliberately instantiable so tests can drive many independent handshakes,
 * and that is precisely why it is not the authority -- freely created instances
 * enforce nothing. This object is the authority, it has no reset, and no future
 * endpoint, handle or media entry point may reach native code except through it.
 *
 * [initialize] takes no argument and is the ONLY entry: it resolves the packaged
 * library by name out of the application's own library directory. There is no
 * product path variant and no image overload -- a caller-selected file path is
 * the unsafe dynamic load pattern, and an image parameter here would let a test
 * satisfy this owner's contract without ever exercising the packaged-name route
 * the product uses. A test that wants to load a bridge it built itself uses the
 * separately instantiable [BridgeInitializer] with its own [NativeImage].
 */
internal object NativeBridgeOwner {

    /**
     * The packaged library's name.
     *
     * It is the name only, never a path: `System.loadLibrary` resolves it out
     * of the directories the platform owns. Changing it can only ever produce
     * a load failure -- never a successful handshake against a different
     * object -- because the handshake that follows still has to agree with the
     * authority this Kotlin side was compiled against.
     */
    const val LIBRARY_NAME: String = "moq5_jni"

    private val initializer = BridgeInitializer()

    val isReady: Boolean
        get() = initializer.isReady

    val outcome: BridgeInit?
        get() = initializer.outcome

    /**
     * The product entry, and the ONLY one: decide once for this process, using
     * the packaged library. Later calls return the first decision, success or
     * failure.
     *
     * There is deliberately no overload taking an image. A test that wants to
     * load an arbitrary object already has [BridgeInitializer], which is
     * instantiable for exactly that reason; an image parameter here would let a
     * test satisfy the process-wide owner's contract without ever exercising
     * the packaged-name route the product actually uses (chats/libmoq/1222 F3).
     */
    fun initialize(): BridgeInit = initializer.initialize(NativeImage.packaged(LIBRARY_NAME))
}
