/* A known-compatible native fixture for the bridge handshake, and its skews.
 *
 * The COMPATIBLE table is an EXPANSION of `MOQ5_ABI_METHODS`, not a
 * transcription of it: each row's NAME, DESCRIPTOR and MEMBERSHIP come from the
 * authority, so a descriptor or membership change in `moq5_abi.h` moves this
 * fixture with it. A stale "compatible" control can therefore never bless
 * native bytes the authority no longer describes.
 *
 * The C FUNCTION BODIES are handwritten, one per row, and are NOT generated:
 * a descriptor string cannot produce an implementation. What the authority
 * guarantees is that an unimplemented row FAILS THE BUILD BY NAME rather than
 * being silently omitted -- an added authority row expands to a reference to
 * `fx_<name>` and to `MOQ5_FIXTURE_EMIT_<name>`, neither of which exists until
 * its implementation and its permitted transforms are deliberately supplied.
 *
 * An adversarial variant may TRANSFORM exactly one row: drop it, or bind it
 * with a different descriptor. No variant maintains a second complete table.
 *
 * The second parameter is `jobject`, not `jclass`: the authority's chosen
 * invocation shape is instance methods on the Kotlin `object` singleton.
 */
#include <stdint.h>
#include <stdio.h>

#include <jni.h>

#include "../moq5_abi.h"

#ifndef MOQ5_FIXTURE_EPOCH
#define MOQ5_FIXTURE_EPOCH MOQ5_ABI_EPOCH
#endif

#ifndef MOQ5_FIXTURE_FEATURES
#define MOQ5_FIXTURE_FEATURES MOQ5_FEATURES_PROVIDED
#endif

/* A stand-in for the semantic LibMoQ version: diagnostic input only. */
#ifndef MOQ5_FIXTURE_LIBMOQ_VERSION
#define MOQ5_FIXTURE_LIBMOQ_VERSION 100
#endif

/* -- per-row knobs ---------------------------------------------------------- *
 * Every row is emitted by default. A variant sets exactly one of these. */
#ifndef MOQ5_FIXTURE_EMIT_nativeAbiEpoch
#define MOQ5_FIXTURE_EMIT_nativeAbiEpoch 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeFeatureBits
#define MOQ5_FIXTURE_EMIT_nativeFeatureBits 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeLibmoqVersion
#define MOQ5_FIXTURE_EMIT_nativeLibmoqVersion 1
#endif

/* The descriptor a row is bound with: the authority's, unless a variant
 * overrides that ONE row. */
#ifndef MOQ5_FIXTURE_EMIT_nativeEndpointCreate
#define MOQ5_FIXTURE_EMIT_nativeEndpointCreate 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeEndpointCreateTrusted
#define MOQ5_FIXTURE_EMIT_nativeEndpointCreateTrusted 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeEndpointRelease
#define MOQ5_FIXTURE_EMIT_nativeEndpointRelease 1
#endif

#ifndef MOQ5_FIXTURE_EMIT_nativeEndpointWake
#define MOQ5_FIXTURE_EMIT_nativeEndpointWake 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeEndpointWait
#define MOQ5_FIXTURE_EMIT_nativeEndpointWait 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeEndpointSnapshot
#define MOQ5_FIXTURE_EMIT_nativeEndpointSnapshot 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeEndpointStop
#define MOQ5_FIXTURE_EMIT_nativeEndpointStop 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeEndpointDestroy
#define MOQ5_FIXTURE_EMIT_nativeEndpointDestroy 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeEndpointSetInterrupted
#define MOQ5_FIXTURE_EMIT_nativeEndpointSetInterrupted 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeEndpointDrain
#define MOQ5_FIXTURE_EMIT_nativeEndpointDrain 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeReceiverAttach
#define MOQ5_FIXTURE_EMIT_nativeReceiverAttach 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeReceiverClose
#define MOQ5_FIXTURE_EMIT_nativeReceiverClose 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeReceiverPollTrack
#define MOQ5_FIXTURE_EMIT_nativeReceiverPollTrack 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeTrackDescribe
#define MOQ5_FIXTURE_EMIT_nativeTrackDescribe 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeReceiverPollObject
#define MOQ5_FIXTURE_EMIT_nativeReceiverPollObject 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeTrackSubscribe
#define MOQ5_FIXTURE_EMIT_nativeTrackSubscribe 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeTrackUnsubscribe
#define MOQ5_FIXTURE_EMIT_nativeTrackUnsubscribe 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeTrackState
#define MOQ5_FIXTURE_EMIT_nativeTrackState 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeReceiverPollSap
#define MOQ5_FIXTURE_EMIT_nativeReceiverPollSap 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeReceiverPollMediaTimeline
#define MOQ5_FIXTURE_EMIT_nativeReceiverPollMediaTimeline 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeReceiverStats
#define MOQ5_FIXTURE_EMIT_nativeReceiverStats 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeSenderAttach
#define MOQ5_FIXTURE_EMIT_nativeSenderAttach 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeSenderClose
#define MOQ5_FIXTURE_EMIT_nativeSenderClose 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeSenderAddTrack
#define MOQ5_FIXTURE_EMIT_nativeSenderAddTrack 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeSenderRemoveTrack
#define MOQ5_FIXTURE_EMIT_nativeSenderRemoveTrack 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeSenderWrite
#define MOQ5_FIXTURE_EMIT_nativeSenderWrite 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeSenderEndTrack
#define MOQ5_FIXTURE_EMIT_nativeSenderEndTrack 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeSenderConvertToVod
#define MOQ5_FIXTURE_EMIT_nativeSenderConvertToVod 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeSenderComplete
#define MOQ5_FIXTURE_EMIT_nativeSenderComplete 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeSenderProbe
#define MOQ5_FIXTURE_EMIT_nativeSenderProbe 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeSenderTrackDemand
#define MOQ5_FIXTURE_EMIT_nativeSenderTrackDemand 1
#endif
#ifndef MOQ5_FIXTURE_EMIT_nativeSenderStats
#define MOQ5_FIXTURE_EMIT_nativeSenderStats 1
#endif

#ifndef MOQ5_FIXTURE_EMIT_nativeCodecInitDataBuild
#define MOQ5_FIXTURE_EMIT_nativeCodecInitDataBuild 1
#endif

#ifndef MOQ5_FIXTURE_EMIT_nativeCodecStringFormat
#define MOQ5_FIXTURE_EMIT_nativeCodecStringFormat 1
#endif

#ifndef MOQ5_FIXTURE_DESC_nativeAbiEpoch
#define MOQ5_FIXTURE_DESC_nativeAbiEpoch(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeFeatureBits
#define MOQ5_FIXTURE_DESC_nativeFeatureBits(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeLibmoqVersion
#define MOQ5_FIXTURE_DESC_nativeLibmoqVersion(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeEndpointCreate
#define MOQ5_FIXTURE_DESC_nativeEndpointCreate(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeEndpointCreateTrusted
#define MOQ5_FIXTURE_DESC_nativeEndpointCreateTrusted(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeEndpointRelease
#define MOQ5_FIXTURE_DESC_nativeEndpointRelease(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeEndpointWake
#define MOQ5_FIXTURE_DESC_nativeEndpointWake(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeEndpointWait
#define MOQ5_FIXTURE_DESC_nativeEndpointWait(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeEndpointSnapshot
#define MOQ5_FIXTURE_DESC_nativeEndpointSnapshot(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeEndpointStop
#define MOQ5_FIXTURE_DESC_nativeEndpointStop(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeEndpointDestroy
#define MOQ5_FIXTURE_DESC_nativeEndpointDestroy(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeEndpointSetInterrupted
#define MOQ5_FIXTURE_DESC_nativeEndpointSetInterrupted(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeEndpointDrain
#define MOQ5_FIXTURE_DESC_nativeEndpointDrain(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeReceiverAttach
#define MOQ5_FIXTURE_DESC_nativeReceiverAttach(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeReceiverClose
#define MOQ5_FIXTURE_DESC_nativeReceiverClose(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeReceiverPollTrack
#define MOQ5_FIXTURE_DESC_nativeReceiverPollTrack(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeTrackDescribe
#define MOQ5_FIXTURE_DESC_nativeTrackDescribe(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeReceiverPollObject
#define MOQ5_FIXTURE_DESC_nativeReceiverPollObject(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeTrackSubscribe
#define MOQ5_FIXTURE_DESC_nativeTrackSubscribe(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeTrackUnsubscribe
#define MOQ5_FIXTURE_DESC_nativeTrackUnsubscribe(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeTrackState
#define MOQ5_FIXTURE_DESC_nativeTrackState(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeReceiverPollSap
#define MOQ5_FIXTURE_DESC_nativeReceiverPollSap(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeReceiverPollMediaTimeline
#define MOQ5_FIXTURE_DESC_nativeReceiverPollMediaTimeline(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeReceiverStats
#define MOQ5_FIXTURE_DESC_nativeReceiverStats(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeSenderAttach
#define MOQ5_FIXTURE_DESC_nativeSenderAttach(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeSenderClose
#define MOQ5_FIXTURE_DESC_nativeSenderClose(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeSenderAddTrack
#define MOQ5_FIXTURE_DESC_nativeSenderAddTrack(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeSenderRemoveTrack
#define MOQ5_FIXTURE_DESC_nativeSenderRemoveTrack(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeSenderWrite
#define MOQ5_FIXTURE_DESC_nativeSenderWrite(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeSenderEndTrack
#define MOQ5_FIXTURE_DESC_nativeSenderEndTrack(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeSenderConvertToVod
#define MOQ5_FIXTURE_DESC_nativeSenderConvertToVod(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeSenderComplete
#define MOQ5_FIXTURE_DESC_nativeSenderComplete(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeSenderProbe
#define MOQ5_FIXTURE_DESC_nativeSenderProbe(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeSenderTrackDemand
#define MOQ5_FIXTURE_DESC_nativeSenderTrackDemand(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeSenderStats
#define MOQ5_FIXTURE_DESC_nativeSenderStats(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeCodecInitDataBuild
#define MOQ5_FIXTURE_DESC_nativeCodecInitDataBuild(d) d
#endif
#ifndef MOQ5_FIXTURE_DESC_nativeCodecStringFormat
#define MOQ5_FIXTURE_DESC_nativeCodecStringFormat(d) d
#endif


/* -- implementations, one per authority row --------------------------------- */
#if MOQ5_FIXTURE_EMIT_nativeAbiEpoch
static jint JNICALL fx_nativeAbiEpoch(JNIEnv *env, jobject self)
{
    (void)env;
    (void)self;
    return (jint)(MOQ5_FIXTURE_EPOCH);
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeFeatureBits
static jlong JNICALL fx_nativeFeatureBits(JNIEnv *env, jobject self)
{
    (void)env;
    (void)self;
    return (jlong)(unsigned long long)(MOQ5_FIXTURE_FEATURES);
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeLibmoqVersion
static jint JNICALL fx_nativeLibmoqVersion(JNIEnv *env, jobject self)
{
    (void)env;
    (void)self;
    return (jint)(MOQ5_FIXTURE_LIBMOQ_VERSION);
}
#endif

/* -- the variant's own configuration, embedded and readable ------------------ *
 * Comparing built objects byte-for-byte cannot tell a real variant from a
 * mis-specified one: two identically-configured builds still differ in
 * incidental bytes. This signature is the CONFIGURATION, stringified from the
 * knobs themselves, so a selection flag that failed to take effect produces a
 * signature identical to the compatible fixture and the build refuses it. */
#define MOQ5_STR_(x) #x
#define MOQ5_STR(x) MOQ5_STR_(x)
#define MOQ5_CAT_(a, b) a##b
#define MOQ5_CAT(a, b) MOQ5_CAT_(a, b)

/* One row of the signature, expanded from the authority: its NAME, the emit
 * knob's VALUE, and the descriptor AFTER this variant's permitted transform.
 * There is no second SIGNATURE or METHOD-TABLE inventory: both expand from the
 * authority, so an authority row cannot move without moving the evidence that
 * proves fixture selection, and a new row cannot appear in one and not the
 * other.
 *
 * The per-row emit knobs, the descriptor transforms and the handwritten C
 * function bodies above ARE separate per-row inventories, deliberately: a
 * variant's permitted transform and a row's implementation cannot be derived
 * from a descriptor string. What keeps them honest is that an authority row
 * missing from either one fails the BUILD by name. */
#define MOQ5_SIG_ROW(name, desc)                                              \
    " " #name "=" MOQ5_STR(MOQ5_CAT(MOQ5_FIXTURE_EMIT_, name))                \
    ":" MOQ5_CAT(MOQ5_FIXTURE_DESC_, name)(desc)

#ifdef MOQ5_FIXTURE_REGISTRATION_FAILS
#define MOQ5_FIXTURE_CLASS_TAG "absent"
#else
#define MOQ5_FIXTURE_CLASS_TAG "authority"
#endif

__attribute__((used))
static const char moq5_fixture_signature[] =
    "moq5-fixture-signature"
    " epoch=" MOQ5_STR(MOQ5_FIXTURE_EPOCH)
    " features=" MOQ5_STR(MOQ5_FIXTURE_FEATURES)
    " rows=" MOQ5_ABI_METHODS(MOQ5_SIG_ROW)
    " class=" MOQ5_FIXTURE_CLASS_TAG;

#if MOQ5_FIXTURE_EMIT_nativeEndpointCreate
/* The fixture proves REGISTRATION, never endpoint behaviour: a bound row is
 * enough, and a body that constructed anything would give this object a second
 * purpose. */
static jint JNICALL fx_nativeEndpointCreate(JNIEnv *env, jobject self,
                                            jstring url, jlongArray out)
{
    (void)env; (void)self; (void)url; (void)out;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeEndpointCreateTrusted
static jint JNICALL fx_nativeEndpointCreateTrusted(JNIEnv *env, jobject self,
                                                   jstring url, jint policy,
                                                   jint version_policy, jintArray versions,
                                                   jlong handshake_us, jint protocol,
                                                   jstring wt_path, jlongArray out)
{
    (void)env; (void)self; (void)url; (void)policy; (void)version_policy;
    (void)versions; (void)handshake_us; (void)protocol; (void)wt_path; (void)out;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeEndpointRelease
static jint JNICALL fx_nativeEndpointRelease(JNIEnv *env, jobject self, jlong token)
{
    (void)env; (void)self; (void)token;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeEndpointWake
static jint JNICALL fx_nativeEndpointWake(JNIEnv *env, jobject self, jlong token)
{
    (void)env; (void)self; (void)token;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeEndpointWait
static jint JNICALL fx_nativeEndpointWait(JNIEnv *env, jobject self, jlong token, jlong t)
{
    (void)env; (void)self; (void)token; (void)t;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeEndpointSnapshot
static jint JNICALL fx_nativeEndpointSnapshot(JNIEnv *env, jobject self, jlong token, jlongArray out)
{
    (void)env; (void)self; (void)token; (void)out;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeEndpointStop
static jint JNICALL fx_nativeEndpointStop(JNIEnv *env, jobject self, jlong token)
{
    (void)env; (void)self; (void)token;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeEndpointDestroy
static jint JNICALL fx_nativeEndpointDestroy(JNIEnv *env, jobject self, jlong token)
{
    (void)env; (void)self; (void)token;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeEndpointSetInterrupted
static jint JNICALL fx_nativeEndpointSetInterrupted(JNIEnv *env, jobject self, jlong token,
                                                    jboolean interrupted)
{
    (void)env; (void)self; (void)token; (void)interrupted;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeEndpointDrain
static jint JNICALL fx_nativeEndpointDrain(JNIEnv *env, jobject self, jlong token,
                                           jlong timeout_us)
{
    (void)env; (void)self; (void)token; (void)timeout_us;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeReceiverAttach
static jint JNICALL fx_nativeReceiverAttach(JNIEnv *env, jobject self, jlong root,
                                            jobjectArray parts, jint overflow, jint max_objects,
                                            jlong max_bytes, jint max_track_events,
                                            jboolean auto_subscribe, jbyteArray catalog,
                                            jint time_mode, jlongArray out)
{
    (void)env; (void)self; (void)root; (void)parts; (void)overflow; (void)max_objects;
    (void)max_bytes; (void)max_track_events; (void)auto_subscribe; (void)catalog;
    (void)time_mode; (void)out;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeReceiverClose
static jint JNICALL fx_nativeReceiverClose(JNIEnv *env, jobject self, jlong root, jlong child)
{
    (void)env; (void)self; (void)root; (void)child;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeReceiverPollTrack
static jint JNICALL fx_nativeReceiverPollTrack(JNIEnv *env, jobject self, jlong root, jlong child,
                                               jlongArray scalars)
{
    (void)env; (void)self; (void)root; (void)child; (void)scalars;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeTrackDescribe
static jobjectArray JNICALL fx_nativeTrackDescribe(JNIEnv *env, jobject self, jlong root,
                                                   jlong child, jlong track, jlongArray scalars)
{
    (void)env; (void)self; (void)root; (void)child; (void)track; (void)scalars;
    return NULL;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeReceiverPollObject
static jobjectArray JNICALL fx_nativeReceiverPollObject(JNIEnv *env, jobject self, jlong root,
                                                        jlong child, jlongArray scalars)
{
    (void)env; (void)self; (void)root; (void)child; (void)scalars;
    return NULL;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeTrackSubscribe
static jint JNICALL fx_nativeTrackSubscribe(JNIEnv *env, jobject self, jlong root, jlong child,
                                            jlong track, jint start, jboolean has_priority,
                                            jint priority)
{
    (void)env; (void)self; (void)root; (void)child; (void)track; (void)start;
    (void)has_priority; (void)priority;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeTrackUnsubscribe
static jint JNICALL fx_nativeTrackUnsubscribe(JNIEnv *env, jobject self, jlong root, jlong child,
                                              jlong track)
{
    (void)env; (void)self; (void)root; (void)child; (void)track;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeTrackState
static jint JNICALL fx_nativeTrackState(JNIEnv *env, jobject self, jlong root, jlong child,
                                        jlong track, jlongArray out)
{
    (void)env; (void)self; (void)root; (void)child; (void)track; (void)out;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeReceiverPollSap
static jint JNICALL fx_nativeReceiverPollSap(JNIEnv *env, jobject self, jlong root, jlong child,
                                             jlongArray scalars)
{
    (void)env; (void)self; (void)root; (void)child; (void)scalars;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeReceiverPollMediaTimeline
static jint JNICALL fx_nativeReceiverPollMediaTimeline(JNIEnv *env, jobject self, jlong root,
                                                       jlong child, jlongArray scalars)
{
    (void)env; (void)self; (void)root; (void)child; (void)scalars;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeReceiverStats
static jint JNICALL fx_nativeReceiverStats(JNIEnv *env, jobject self, jlong root, jlong child,
                                           jlongArray scalars)
{
    (void)env; (void)self; (void)root; (void)child; (void)scalars;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeSenderAttach
static jint JNICALL fx_nativeSenderAttach(JNIEnv *env, jobject self, jlong root, jobjectArray parts, jbyteArray catalog, jlongArray scalars, jlongArray out)
{
    (void)env; (void)self; (void)root; (void)parts; (void)catalog; (void)scalars; (void)out;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeSenderClose
static jint JNICALL fx_nativeSenderClose(JNIEnv *env, jobject self, jlong root, jlong child)
{
    (void)env; (void)self; (void)root; (void)child;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeSenderAddTrack
static jint JNICALL fx_nativeSenderAddTrack(JNIEnv *env, jobject self, jlong root, jlong child, jobjectArray bytes, jlongArray scalars, jlongArray out)
{
    (void)env; (void)self; (void)root; (void)child; (void)bytes; (void)scalars; (void)out;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeSenderRemoveTrack
static jint JNICALL fx_nativeSenderRemoveTrack(JNIEnv *env, jobject self, jlong root, jlong child, jlong track)
{
    (void)env; (void)self; (void)root; (void)child; (void)track;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeSenderWrite
static jint JNICALL fx_nativeSenderWrite(JNIEnv *env, jobject self, jlong root, jlong child, jlong track, jbyteArray payload, jbyteArray props, jlongArray scalars)
{
    (void)env; (void)self; (void)root; (void)child; (void)track; (void)payload; (void)props; (void)scalars;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeSenderEndTrack
static jint JNICALL fx_nativeSenderEndTrack(JNIEnv *env, jobject self, jlong root, jlong child, jlong track)
{
    (void)env; (void)self; (void)root; (void)child; (void)track;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeSenderConvertToVod
static jint JNICALL fx_nativeSenderConvertToVod(JNIEnv *env, jobject self, jlong root, jlong child, jlongArray tracks, jlongArray durations)
{
    (void)env; (void)self; (void)root; (void)child; (void)tracks; (void)durations;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeSenderComplete
static jint JNICALL fx_nativeSenderComplete(JNIEnv *env, jobject self, jlong root, jlong child)
{
    (void)env; (void)self; (void)root; (void)child;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeSenderProbe
static jint JNICALL fx_nativeSenderProbe(JNIEnv *env, jobject self, jlong root, jlong child, jlongArray scalars)
{
    (void)env; (void)self; (void)root; (void)child; (void)scalars;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeSenderTrackDemand
static jint JNICALL fx_nativeSenderTrackDemand(JNIEnv *env, jobject self, jlong root, jlong child, jlong track, jlongArray out)
{
    (void)env; (void)self; (void)root; (void)child; (void)track; (void)out;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeSenderStats
static jint JNICALL fx_nativeSenderStats(JNIEnv *env, jobject self, jlong root, jlong child, jlongArray scalars)
{
    (void)env; (void)self; (void)root; (void)child; (void)scalars;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeCodecInitDataBuild
static jint JNICALL fx_nativeCodecInitDataBuild(JNIEnv *env, jobject self, jint source_format, jbyteArray source,
                                                jint nal_length_size, jobjectArray out)
{
    (void)env; (void)self; (void)source_format; (void)source; (void)nal_length_size; (void)out;
    return 0;
}
#endif

#if MOQ5_FIXTURE_EMIT_nativeCodecStringFormat
static jint JNICALL fx_nativeCodecStringFormat(JNIEnv *env, jobject self, jint config_format, jbyteArray sample_entry,
                                               jboolean has_oti, jint oti, jbyteArray decoder_config, jobjectArray out)
{
    (void)env; (void)self; (void)config_format; (void)sample_entry; (void)has_oti; (void)oti;
    (void)decoder_config; (void)out;
    return 0;
}
#endif

/* -- the one table, expanded from the authority ----------------------------- */
#define MOQ5_ROW_EMIT_1(name, desc) \
    { #name, MOQ5_CAT(MOQ5_FIXTURE_DESC_, name)(desc), (void *)(uintptr_t)MOQ5_CAT(fx_, name) },
#define MOQ5_ROW_EMIT_0(name, desc)

#define MOQ5_FIXTURE_ROW(name, desc) \
    MOQ5_CAT(MOQ5_ROW_EMIT_, MOQ5_CAT(MOQ5_FIXTURE_EMIT_, name))(name, desc)

static const JNINativeMethod moq5_fixture_methods[] = {
    MOQ5_ABI_METHODS(MOQ5_FIXTURE_ROW)
};

__attribute__((visibility("default")))
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved)
{
    JNIEnv *env = NULL;
    jclass klass = NULL;
    jsize count = (jsize)(sizeof moq5_fixture_methods / sizeof moq5_fixture_methods[0]);

    (void)reserved;

    if ((*vm)->GetEnv(vm, (void **)&env, MOQ5_JNI_VERSION) != JNI_OK || env == NULL) {
        return JNI_ERR;
    }

#ifdef MOQ5_FIXTURE_REGISTRATION_FAILS
    klass = (*env)->FindClass(env, "org/openmoq/moq5/jni/NoSuchBridgeClass");
#else
    klass = (*env)->FindClass(env, MOQ5_BRIDGE_CLASS);
#endif
    if (klass == NULL) {
        (*env)->ExceptionClear(env);
        return JNI_ERR;
    }

    /* Registered ONE ROW AT A TIME so a failure can name the row that failed.
     *
     * A detail thrown here survives `System.load`: the JVM surfaces this exact
     * UnsatisfiedLinkError message to the caller, verified on the host JVM, so
     * a registration failure does not have to collapse into a generic
     * "unsupported JNI version". */
    for (jsize i = 0; i < count; i++) {
        if ((*env)->RegisterNatives(env, klass, &moq5_fixture_methods[i], 1) != JNI_OK) {
            char detail[256];
            jclass err = NULL;
            (*env)->ExceptionClear(env);
            snprintf(detail, sizeof detail,
                     "moq5-bridge: RegisterNatives failed for %s%s on %s",
                     moq5_fixture_methods[i].name,
                     moq5_fixture_methods[i].signature,
                     MOQ5_BRIDGE_CLASS);
            err = (*env)->FindClass(env, "java/lang/UnsatisfiedLinkError");
            if (err != NULL) {
                (*env)->ThrowNew(env, err, detail);
                (*env)->DeleteLocalRef(env, err);
            }
            (*env)->DeleteLocalRef(env, klass);
            return JNI_ERR;
        }
    }
    (*env)->DeleteLocalRef(env, klass);
    return MOQ5_JNI_VERSION;
}
