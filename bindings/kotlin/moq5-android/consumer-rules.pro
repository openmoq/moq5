# Consumer rules shipped inside the AAR.
#
# The bridge is bound by explicit RegisterNatives against ONE class named in
# jni/moq5_abi.h, and the native side looks that class and its methods up by
# their compile-time names. R8 in a consuming app must therefore not rename or
# remove them, or JNI_OnLoad fails at run time in exactly the builds that ship.
#
# The shape is the authority's chosen one: INSTANCE methods on the Kotlin
# `object` singleton (see MOQ5_BRIDGE_METHOD_STATIC), which compile to
# `public final native`. The INSTANCE field is kept because a Kotlin `object`
# call goes through it. Nothing else is exempted from minification.
-keep,includedescriptorclasses class org.openmoq.moq5.jni.NativeBridge {
    public static final org.openmoq.moq5.jni.NativeBridge INSTANCE;
    public final native int nativeAbiEpoch();
    public final native long nativeFeatureBits();
    public final native int nativeLibmoqVersion();
    public final native int nativeEndpointCreate(java.lang.String, long[]);
    public final native int nativeEndpointCreateTrusted(java.lang.String, int, int, int[], long, int, java.lang.String, long[]);
    public final native int nativeEndpointRelease(long);
    public final native int nativeEndpointWake(long);
    public final native int nativeEndpointWait(long, long);
    public final native int nativeEndpointSnapshot(long, long[]);
    public final native int nativeEndpointStop(long);
    public final native int nativeEndpointDestroy(long);
    public final native int nativeEndpointSetInterrupted(long, boolean);
    public final native int nativeEndpointDrain(long, long);
    public final native int nativeReceiverAttach(long, byte[][], int, int, long, int, boolean, byte[], int, long[]);
    public final native int nativeReceiverClose(long, long);
    public final native int nativeReceiverPollTrack(long, long, long[]);
    public final native byte[][] nativeTrackDescribe(long, long, long, long[]);
    public final native byte[][] nativeReceiverPollObject(long, long, long[]);
    public final native int nativeTrackSubscribe(long, long, long, int, boolean, int);
    public final native int nativeTrackUnsubscribe(long, long, long);
    public final native int nativeTrackState(long, long, long, long[]);
    public final native int nativeReceiverPollSap(long, long, long[]);
    public final native int nativeReceiverPollMediaTimeline(long, long, long[]);
    public final native int nativeReceiverStats(long, long, long[]);
    public final native int nativeSenderAttach(long, byte[][], byte[], long[], long[]);
    public final native int nativeSenderClose(long, long);
    public final native int nativeSenderAddTrack(long, long, byte[][], long[], long[]);
    public final native int nativeSenderRemoveTrack(long, long, long);
    public final native int nativeSenderWrite(long, long, long, byte[], byte[], long[]);
    public final native int nativeSenderEndTrack(long, long, long);
    public final native int nativeSenderConvertToVod(long, long, long[], long[]);
    public final native int nativeSenderComplete(long, long);
    public final native int nativeSenderProbe(long, long, long[]);
    public final native int nativeSenderTrackDemand(long, long, long, long[]);
    public final native int nativeSenderStats(long, long, long[]);
    public final native int nativeCodecInitDataBuild(int, byte[], int, byte[][]);
    public final native int nativeCodecStringFormat(int, byte[], boolean, int, byte[], byte[][]);
}

# The SECOND class the native side resolves by string: the library-owned trust
# bridge, reached from JNI_OnLoad through its object singleton and one instance
# method (see MOQ5_TRUST_CLASS / MOQ5_TRUST_METHOD / MOQ5_TRUST_DESCRIPTOR in
# jni/moq5_jni.c). Exactly that shape is kept and nothing else on the class.
-keep,includedescriptorclasses class org.openmoq.moq5.trust.Moq5PlatformTrust {
    public static final org.openmoq.moq5.trust.Moq5PlatformTrust INSTANCE;
    public final int checkServerTrusted(byte[][], java.lang.String, java.lang.String);
}
