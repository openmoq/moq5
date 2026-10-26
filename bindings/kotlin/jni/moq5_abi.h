/* The single authority for the Kotlin <-> native bridge identity.
 *
 * Everything the two sides must agree on lives here: the bound class, the
 * method names and descriptors, the private ABI epoch, and the feature
 * vocabulary. The Kotlin side declares the same values and a verification task
 * compares them against this file, so a change on one side alone is a build
 * failure rather than a runtime surprise.
 *
 * THE ABI EPOCH IS NOT `MOQ_VERSION_NUMBER`. The LibMoQ semantic version says
 * what protocol/library release this is; it says nothing about the memory
 * layout, calling convention or method table this binding compiled against. A
 * patch release can leave the epoch untouched and a binding-only change can
 * require a bump, so the two are independent. The semantic version is carried
 * across the handshake as DIAGNOSTIC INPUT only -- it may appear in an error
 * message, and it may never decide compatibility.
 */
#ifndef MOQ5_ABI_H
#define MOQ5_ABI_H

/* The private bridge ABI epoch. Bumped whenever the registered method table,
 * its descriptors, or the layout of anything passed across it changes. */
#define MOQ5_ABI_EPOCH 17

/* The JNI version the bridge requires. */
#define MOQ5_JNI_VERSION JNI_VERSION_1_6

/* The one bound class. */
#define MOQ5_BRIDGE_CLASS "org/openmoq/moq5/jni/NativeBridge"

/* INVOCATION SHAPE: INSTANCE methods on the Kotlin `object` singleton.
 *
 * Chosen, not inherited. Members of a Kotlin `object` compile to instance
 * methods; making them static would need `@JvmStatic`, which emits a static
 * bridge ALONGSIDE the instance method and leaves two members with the same
 * name for `RegisterNatives` to choose between. One member per row is worth
 * more than the convention of a static bridge, so the second C parameter is
 * `jobject` (the ignored singleton receiver) and never `jclass`.
 *
 * The compiled bytecode, the C signatures, this header and the consumer keep
 * rules must all state that same shape; the authority gate compares them
 * against `javap` output rather than against Kotlin source text. */
#define MOQ5_BRIDGE_METHOD_STATIC 0

/* The feature vocabulary.
 *
 * Deliberately minimal: this tranche establishes exactly one capability, the
 * registration/handshake bridge itself. Endpoint, media and TLS capabilities
 * are NOT pre-declared here -- a feature bit that nothing can satisfy is a
 * promise, not a contract.
 *
 * Required bits must be present or the bridge fails closed. Bits outside
 * MOQ5_FEATURES_KNOWN are additive: a newer native side may advertise them and
 * an older Kotlin side must still accept the bridge.
 */
/* The transport is a 64-BIT bitset (`nativeFeatureBits()J`, Kotlin `Long`), so
 * every constant and mask here is explicitly 64-bit. A 32-bit expression would
 * make a bit above 2^31 truncate silently on the way out. */
#define MOQ5_FEATURE_BRIDGE_HANDSHAKE 0x1ULL

#define MOQ5_FEATURES_KNOWN    ((unsigned long long)(MOQ5_FEATURE_BRIDGE_HANDSHAKE))
#define MOQ5_FEATURES_REQUIRED ((unsigned long long)(MOQ5_FEATURE_BRIDGE_HANDSHAKE))
#define MOQ5_FEATURES_PROVIDED ((unsigned long long)(MOQ5_FEATURE_BRIDGE_HANDSHAKE))

/* The registered method table. One row per native method: name, descriptor.
 * `MOQ5_ABI_METHODS(X)` is the only place the table is written down. */
#define MOQ5_ABI_METHODS(X)                                                   \
    X(nativeAbiEpoch, "()I")                                                  \
    X(nativeFeatureBits, "()J")                                               \
    X(nativeLibmoqVersion, "()I")                                             \
    X(nativeEndpointCreate, "(Ljava/lang/String;[J)I")                        \
    X(nativeEndpointCreateTrusted, "(Ljava/lang/String;II[IJILjava/lang/String;[J)I") \
    X(nativeEndpointRelease, "(J)I")                                          \
    X(nativeEndpointWake, "(J)I")                                             \
    X(nativeEndpointWait, "(JJ)I")                                            \
    X(nativeEndpointSnapshot, "(J[J)I")                                       \
    X(nativeEndpointStop, "(J)I")                                             \
    X(nativeEndpointDestroy, "(J)I")                                       \
    X(nativeEndpointSetInterrupted, "(JZ)I")                              \
    X(nativeEndpointDrain, "(JJ)I")                                       \
    X(nativeReceiverAttach, "(J[[BIIJIZ[BI[J)I")                          \
    X(nativeReceiverClose, "(JJ)I")                                       \
    X(nativeReceiverPollTrack, "(JJ[J)I")                                 \
    X(nativeTrackDescribe, "(JJJ[J)[[B")                                  \
    X(nativeReceiverPollObject, "(JJ[J)[[B")                              \
    X(nativeTrackSubscribe, "(JJJIZI)I")                                  \
    X(nativeTrackUnsubscribe, "(JJJ)I")                                   \
    X(nativeTrackState, "(JJJ[J)I")                                       \
    X(nativeReceiverPollSap, "(JJ[J)I")                                   \
    X(nativeReceiverPollMediaTimeline, "(JJ[J)I")                         \
    X(nativeReceiverStats, "(JJ[J)I")                                     \
    X(nativeSenderAttach, "(J[[B[B[J[J)I")                                \
    X(nativeSenderClose, "(JJ)I")                                         \
    X(nativeSenderAddTrack, "(JJ[[B[J[J)I")                               \
    X(nativeSenderRemoveTrack, "(JJJ)I")                                  \
    X(nativeSenderWrite, "(JJJ[B[B[J)I")                                  \
    X(nativeSenderEndTrack, "(JJJ)I")                                     \
    X(nativeSenderConvertToVod, "(JJ[J[J)I")                              \
    X(nativeSenderComplete, "(JJ)I")                                      \
    X(nativeSenderProbe, "(JJ[J)I")                                       \
    X(nativeSenderTrackDemand, "(JJJ[J)I")                                \
    X(nativeSenderStats, "(JJ[J)I")                                       \
    X(nativeCodecInitDataBuild, "(I[BI[[B)I")                             \
    X(nativeCodecStringFormat, "(I[BZI[B[[B)I")

#endif /* MOQ5_ABI_H */
