package org.openmoq.moq5

import org.openmoq.moq5.jni.BridgeInit

/**
 * Why the packaged native bridge could not be brought up in this process.
 *
 * This is a decision made BEFORE any C code runs: it is not a `moq_result_t`,
 * not a private registry refusal and not an endpoint terminal, and it is never
 * narrowed into any of those. Each case retains exactly the values the decision
 * was made on, so an application can diagnose a bad installation, a mismatched
 * artifact or a truncated build without parsing message text.
 *
 * The family is closed. There is deliberately no success case and no
 * "unknown" case: success is the ordinary return of [MoqEndpoint.open], and an
 * outcome the library cannot classify does not exist -- the loader API defines
 * the failures it can raise, and the handshake compares two integers and two
 * masks.
 */
public sealed interface NativeBridgeInitializationFailure {

    /**
     * The packaged library did not load.
     *
     * [libraryName] is the name `System.loadLibrary` was asked to resolve --
     * never a path. [presentBeforeLoad] is the pre-load presence observation
     * when the image was an inspectable file, and `null` for a library resolved
     * by name out of directories this library does not own, where claiming
     * presence would be a guess. [detail] is the JVM's own message. A missing
     * object and a refusing `JNI_OnLoad` both arrive here, because the JVM
     * reports both as one error and the library does not pretend to tell them
     * apart.
     */
    public data class LoadFailed(
        public val libraryName: String,
        public val presentBeforeLoad: Boolean?,
        public val detail: String,
    ) : NativeBridgeInitializationFailure

    /**
     * The object loaded, and an expected native [method] was still unbound
     * when the handshake called it. [detail] is the JVM's own message.
     */
    public data class RegistrationFailed(
        public val method: String,
        public val detail: String,
    ) : NativeBridgeInitializationFailure

    /**
     * The object was built to a different private bridge ABI epoch. [expected]
     * is the epoch this Kotlin side was compiled against, [observed] the
     * object's, and [nativeLibmoqVersion] the diagnostic LibMoQ version the
     * object reports -- diagnostic only, never the compatibility decision.
     */
    public data class AbiMismatch(
        public val expected: Int,
        public val observed: Int,
        public val nativeLibmoqVersion: Int,
    ) : NativeBridgeInitializationFailure

    /**
     * A capability this Kotlin side requires is absent. All three are
     * full-width 64-bit masks: [required] is the compiled-in requirement,
     * [observed] what the object advertised, and [missing] is
     * `required and observed.inv()`.
     */
    public data class MissingFeatures(
        public val required: Long,
        public val observed: Long,
        public val missing: Long,
    ) : NativeBridgeInitializationFailure
}

/**
 * The packaged native bridge could not be initialized, so no endpoint can
 * exist in this process.
 *
 * Thrown by [MoqEndpoint.open] (and therefore by [MoqEndpoint.connect] and
 * [withMoqEndpoint]) before any native endpoint work: nothing was reserved,
 * nothing was published, and there is nothing to close. The decision is made
 * once per process and is sticky -- a later open reports the same [failure],
 * because a bridge that failed its handshake has not become compatible in the
 * meantime. There is no retry and no reset.
 *
 * It is distinct from [EndpointCreateFailedException], which carries a native
 * result from an endpoint or registry that DID run, and from
 * [TrustPolicyUnsupportedException], which is a policy decision. The three
 * never share a value domain.
 */
public class NativeBridgeInitializationException internal constructor(
    public val failure: NativeBridgeInitializationFailure,
) : IllegalStateException("the native bridge could not be initialized: $failure")

/**
 * THE ONE translation from the internal decision to the public family.
 *
 * Total over the failure cases and undefined for [BridgeInit.Ready], which is
 * not a failure and must never reach a caller as one: the engine factory asks
 * this only after it has already decided the outcome is not ready. Every field
 * crosses unchanged -- no message is parsed, no mask is narrowed.
 */
internal fun BridgeInit.toInitializationFailure(): NativeBridgeInitializationFailure =
    when (this) {
        is BridgeInit.LoadFailed ->
            NativeBridgeInitializationFailure.LoadFailed(path, presentBeforeLoad, detail)
        is BridgeInit.RegistrationFailed ->
            NativeBridgeInitializationFailure.RegistrationFailed(method, detail)
        is BridgeInit.AbiMismatch ->
            NativeBridgeInitializationFailure.AbiMismatch(expected, observed, libmoqVersion)
        is BridgeInit.MissingFeatures ->
            NativeBridgeInitializationFailure.MissingFeatures(required, observed, missing)
        is BridgeInit.Ready ->
            throw IllegalArgumentException("a ready bridge is not an initialization failure")
    }
