package org.openmoq.moq5.jni

import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import java.util.Collections
import org.openmoq.moq5.trust.Moq5PlatformTrust

/**
 * TEST-ONLY ledger for the host recorder around the product bridge
 * (`jni/testfixtures/moq5_trust_route_probe.c`).
 *
 * The recorder is linked into the host product image. It owns the exported
 * `JNI_OnLoad` and hands the product a FORWARDING `JavaVM` invocation table,
 * so every `GetEnv` / `AttachCurrentThread(AsDaemon)` / `DetachCurrentThread`
 * the product makes is counted per thread and then forwarded; on every thread
 * the product attaches it swaps a forwarding `JNIEnv` function table, so local
 * reference creation, deletion and frame use are counted too. It also stands
 * in for the real `moq_endpoint_connect_with_trust()` name and forwards.
 *
 * Armed by [ENV]=[TOKEN], it reports what the JNI handed the route through
 * [route] and then invokes the delegate it was handed -- the PRODUCT's
 * library-owned delegate -- on a thread the JVM has never seen, once per
 * [Case] and [Case.repeats] times per case, reporting each outcome through
 * [record] and each thread's final counts through [threadDone].
 *
 * What is under observation is the shipped delegate. Its platform call lands
 * in `Moq5PlatformTrust.checkServerTrusted`, whose checker seam [arm] scripts
 * so a row can pin exactly what reached the platform bridge.
 *
 * Every static method the recorder resolves is `@JvmStatic` with a fixed
 * descriptor; a signature change here is a named recorder failure, never a
 * silently skipped case.
 */
internal object TrustRouteProbe {
    /** Mirrors `MOQ5_TRUST_PROBE_ENV` / `MOQ5_TRUST_PROBE_TOKEN`, host-only. */
    const val ENV: String = "MOQ5_JNI_HOST_TEST_TRUST_PROBE"
    const val TOKEN: String = "probe-v1"

    /**
     * Mirrors `MOQ5_TRUST_FAULT_ENV` and its tokens: the forwarded
     * `pthread_key_create` / `pthread_setspecific` fail while armed.
     */
    const val FAULT_ENV: String = "MOQ5_JNI_HOST_TEST_TRUST_FAULT"
    const val FAULT_KEY_CREATE: String = "key-create-v1"
    const val FAULT_SETSPECIFIC: String = "setspecific-v1"

    /**
     * The ONE-SHOT constructor faults, armed through [nativeArmCtorFault] by
     * the scenario right before the construction they are meant for: the
     * scripted constructor's bare-endpoint seam returns no endpoint once (a
     * constructor error), or the trusted route answers success with no
     * endpoint once (the constructor contract violation).
     */
    const val CTOR_FAULT_ERROR_ONCE: String = "error-once-v1"
    const val CTOR_FAULT_NULL_ONCE: String = "null-once-v1"

    /** Mirrors `MOQ5_CTOR_FAULT_CHILD_INSERT_ONCE`: the next child insertion is refused with CAPACITY. */
    const val CHILD_INSERT_REFUSE_ONCE: String = "child-insert-refuse-once-v1"

    /**
     * The exact Android `authType` for this route: the TLS 1.3 key-exchange
     * constant Conscrypt names for every TLS 1.3 suite. A QUIC client never
     * negotiates anything older (RFC 9001 section 4.2), so the value is
     * protocol-derived, identical for RSA and EC leaves, and never read off
     * the certificate.
     */
    const val AUTH_TYPE: String = "GENERIC"

    /** `MOQ_ENDPOINT_TRUST_ACCEPT` / `MOQ_ENDPOINT_TRUST_REJECT`. */
    const val ACCEPT: Int = 1
    const val REJECT: Int = 0

    /**
     * The bound on simultaneously live local references a callback may hold,
     * whatever the chain length. Counted over the recorder's forwarded
     * creators (FindClass, New*Array, NewString(UTF), GetObjectArrayElement,
     * Get(Static)ObjectField, ExceptionOccurred, NewLocalRef, PopLocalFrame's
     * result) and releases (DeleteLocalRef, PopLocalFrame); the variadic
     * Call*Method family is not counted.
     */
    const val LOCAL_REF_BOUND: Int = 8

    /** What the scripted platform checker does on one call. */
    internal sealed interface Answer {
        data object Accept : Answer

        /** The EXPECTED refusal shape from a real `X509TrustManager`. */
        data object ThrowCertificate : Answer

        /** Anything else the platform can throw. It must not become an accept. */
        data object ThrowOther : Answer
    }

    /**
     * One scripted invocation of the delegate: its inputs and the answers, one
     * per call. A [preAttached] case runs on a thread the RECORDER attached
     * before calling, so the library must attach nothing and detach nothing.
     */
    internal class Case(
        val chain: Array<ByteArray>,
        val serverName: String?,
        val answers: List<Answer>,
        val preAttached: Boolean = false,
    ) {
        val repeats: Int get() = answers.size
    }

    /** What the JNI handed the route, as the recorder saw it. */
    data class Route(
        val fnPresent: Boolean,
        val ctxPresent: Boolean,
        val insecure: Boolean,
        val caFileLen: Int,
        val url: String,
        /** `cfg->versions.policy` as received: 0 AUTO, 1 LIST, 2 EXACT. */
        val versionPolicy: Int,
        /** `cfg->versions.versions[0 until version_count]`, the C codes verbatim. */
        val versions: List<Int>,
        /** `cfg->handshake_timeout_us`, or -1 when the config has no tail. */
        val handshakeMicros: Long,
        /** True when `struct_size` covers the whole current struct (the sized init). */
        val tailCovered: Boolean,
        /** `cfg->protocol` as received: 0 AUTO, 1 RAW_QUIC, 2 WEBTRANSPORT. */
        val protocol: Int,
        /** `cfg->wt_path` as received, or null when the config carries no override. */
        val webTransportPath: String?,
    )

    /**
     * One delegate call's outcome, as observed on the foreign thread right
     * after the call returned, with the thread's cumulative counts so far.
     */
    data class Record(
        val case: Int,
        val call: Int,
        val verdict: Int,
        val attachedAfter: Boolean,
        val pendingAfter: Boolean,
        val getEnv: Int,
        val attachDaemon: Int,
        val attach: Int,
        val detach: Int,
        val highWater: Int,
        val live: Int,
        /** Cumulative counted local references created on the thread. */
        val created: Int,
        /**
         * Platform consultations so far, read on the foreign thread right
         * after THIS call returned and before the next could start.
         */
        val consultedPrefix: Long,
    )

    /** A case thread's final counts, read after the thread has ENDED. */
    data class ThreadStats(
        val case: Int,
        val getEnv: Int,
        val attachDaemon: Int,
        val attach: Int,
        val detach: Int,
        val highWater: Int,
        val live: Int,
    )

    /** One platform consultation, as the shipped bridge presented it. */
    class Consulted(
        val case: Int,
        val chain: List<ByteArray>,
        val authType: String,
        val host: String,
        val thread: Thread,
    )

    val routes: MutableList<Route> = Collections.synchronizedList(mutableListOf())
    val records: MutableList<Record> = Collections.synchronizedList(mutableListOf())
    val threads: MutableList<ThreadStats> = Collections.synchronizedList(mutableListOf())
    val consulted: MutableList<Consulted> = Collections.synchronizedList(mutableListOf())
    val doneCases: MutableList<Int> = Collections.synchronizedList(mutableListOf())

    /** The recorder's counters at one instant. */
    data class Counters(
        val reserveAttempts: Long,
        val reserveFailures: Long,
        val constructAttempts: Long,
        val lastReserveHandle: Long,
        val consulted: Long,
    )

    fun counters(): Counters {
        val v = nativeCounters()
        check(v.size == 5) { "the recorder reported ${v.size} counters, expected 5" }
        return Counters(v[0], v[1], v[2], v[3], v[4])
    }

    /** The recorder's latched instrumentation fault, or null. */
    fun fault(): String? = nativeFault()

    @Volatile private var cases: List<Case> = emptyList()

    @Volatile private var currentCase: Int = -1

    /** Script [cases] and route the platform seam through this ledger. */
    fun arm(cases: List<Case>) {
        armCases(cases)
        Moq5PlatformTrust.checker =
            Moq5PlatformTrust.Checker { chain: Array<X509Certificate>, authType: String, host: String ->
                val c = currentCase
                val callIndex = consulted.count { it.case == c }
                consulted +=
                    Consulted(c, chain.map { it.encoded }, authType, host, Thread.currentThread())
                // Reported to the recorder the instant it happens, on this
                // thread, so the recorder can read a causal prefix after each
                // delegate call.
                nativeConsulted()
                when (cases.getOrNull(c)?.answers?.getOrNull(callIndex) ?: Answer.Accept) {
                    Answer.Accept -> Unit
                    Answer.ThrowCertificate -> throw CertificateException("scripted platform refusal")
                    Answer.ThrowOther -> throw IllegalStateException("scripted non-certificate failure")
                }
            }
    }

    /**
     * Script [cases] WITHOUT touching the platform bridge. Used when the
     * process carries a shadow of `Moq5PlatformTrust` that the shipped seam
     * does not exist on.
     */
    fun armCases(cases: List<Case>) {
        reset()
        this.cases = cases
    }

    fun reset() {
        routes.clear()
        records.clear()
        threads.clear()
        consulted.clear()
        doneCases.clear()
        cases = emptyList()
        currentCase = -1
    }

    @JvmStatic
    fun route(
        fnPresent: Boolean,
        ctxPresent: Boolean,
        insecure: Boolean,
        caFileLen: Int,
        url: String,
        versionPolicy: Int,
        versions: IntArray,
        handshakeMicros: Long,
        tailCovered: Boolean,
        protocol: Int,
        webTransportPath: String?,
    ) {
        routes += Route(fnPresent, ctxPresent, insecure, caFileLen, url, versionPolicy, versions.toList(), handshakeMicros, tailCovered, protocol, webTransportPath)
    }

    @JvmStatic
    fun caseCount(): Int = cases.size

    @JvmStatic
    fun caseChain(index: Int): Array<ByteArray> {
        currentCase = index
        return cases[index].chain
    }

    @JvmStatic
    fun caseServerName(index: Int): String? = cases[index].serverName

    @JvmStatic
    fun caseRepeats(index: Int): Int = cases[index].repeats

    @JvmStatic
    fun casePreAttached(index: Int): Boolean = cases[index].preAttached

    @JvmStatic
    @Suppress("LongParameterList")
    fun record(
        case: Int,
        call: Int,
        verdict: Int,
        attachedAfter: Boolean,
        pendingAfter: Boolean,
        getEnv: Int,
        attachDaemon: Int,
        attach: Int,
        detach: Int,
        highWater: Int,
        live: Int,
        created: Int,
        consultedPrefix: Long,
    ) {
        records +=
            Record(
                case, call, verdict, attachedAfter, pendingAfter,
                getEnv, attachDaemon, attach, detach, highWater, live, created, consultedPrefix,
            )
    }

    /* Bound by the RECORDER's own JNI_OnLoad onto this class -- the product
     * table is untouched. Callable only after the host product image loaded. */
    @JvmStatic external fun nativeConsulted()

    @JvmStatic external fun nativeArmCtorFault(token: String)

    /**
     * How many times the product has reached the REAL
     * `moq_media_sender_add_track`. An observation, so "no track was added"
     * never has to be inferred from a name still being free.
     */
    @JvmStatic external fun nativeAddTrackCalls(): Long

    /**
     * Arm the product's ONE-SHOT result-array allocation failure, immediately
     * before the row it is meant for, so no setup row can consume it.
     */
    @JvmStatic external fun nativeArmAllocFault(pending: Boolean, atIndex: Int)

    /** Whether that armed failure is still waiting to fire. */
    @JvmStatic external fun nativeAllocFaultPending(): Boolean

    /**
     * Host-only: feed one catalog object to the receiver behind [receiver]
     * (an owned child of [root]) through the service's own testing seam, so
     * the real `moq_media_receiver_poll_track` has real discovery to answer.
     * Returns MOQ_OK or the registry's refusal as the bridge would return it.
     */
    @JvmStatic external fun nativeIngestCatalog(root: Long, receiver: Long, catalog: ByteArray): Int

    /** Host-only: the same seam at a chosen (group, object); a later group is a new catalog generation. */
    @JvmStatic external fun nativeIngestCatalogAt(root: Long, receiver: Long, group: Long, obj: Long, catalog: ByteArray): Int

    /** The recorder's sentinel for "the receiver's first track handle", for the one row that must reach a handle no token exists for yet. */
    const val FIRST_TRACK: Long = Long.MIN_VALUE or 0xFFL

    /** Host-only precondition: the transport draft the pump would read from a live session, set BEFORE the catalog is ingested. */
    @JvmStatic external fun nativeSetTransportVersion(root: Long, receiver: Long, version: Int): Int

    /** Host-only precondition: mark [track] (a borrowed token, or [FIRST_TRACK]) desired, exactly as a subscribe records it. Separate from the injector. */
    @JvmStatic external fun nativeMarkTrackDesired(root: Long, receiver: Long, track: Long): Int

    /** Host-only: one media object through the REAL intake; a nonzero [captureUs] is encoded as the LOC-01 timestamp header. */
    @JvmStatic external fun nativeIngestObject(root: Long, receiver: Long, track: Long, group: Long, obj: Long, status: Int, endOfGroup: Boolean, captureUs: Long, payload: ByteArray): Int
    /** One NORMAL object through the real intake with its wire identity: ids as raw 64-bit patterns, the priority byte, and whether it was a datagram (no subgroup). */
    @JvmStatic external fun nativeIngestObjectIdentity(root: Long, receiver: Long, track: Long, group: Long, subgroup: Long, obj: Long, priority: Int, datagram: Boolean, payload: ByteArray): Int

    /** A minimal CMAF chunk (moof + one-sample trun + 1-byte mdat) the CMAF parser accepts. */
    @JvmStatic external fun nativeCmafFragmentFixture(): ByteArray

    @JvmStatic external fun nativeObjectCleanups(): Long

    /** How many times the product released a polled object (`moq_media_object_cleanup`, redirected and counted). */
    fun objectCleanups(): Long = nativeObjectCleanups()

    @JvmStatic external fun nativeCounters(): LongArray

    @JvmStatic external fun nativeFault(): String?

    /** Clears the codec call log and any armed synthetic result. */
    @JvmStatic external fun nativeCodecReset()

    /** Arms ONE synthetic codec result for call [at]: that call returns [rc] and reports [outLen], and the utility is not called for it. */
    @JvmStatic external fun nativeCodecInject(at: Int, rc: Int, outLen: Long)

    /** Every recorded codec call, [CODEC_FIELDS] longs each: kind, format, lengthSize, hasOti, oti, cap, rc, outLen, inputLen, entryLen, injected. */
    @JvmStatic external fun nativeCodecCalls(): LongArray

    /** The recorded input bytes of call [index], or its sample entry when [entry]. */
    @JvmStatic external fun nativeCodecBytes(index: Int, entry: Boolean): ByteArray

    /** A CONTROL through the forwarding names: size query, then the call; OK bytes or null with the status in `status[0]`. */
    @JvmStatic external fun nativeCodecControl(kind: Int, format: Int, input: ByteArray, lengthSize: Int, entry: ByteArray?, hasOti: Boolean, oti: Int, status: IntArray): ByteArray?

    /** Arms the calling thread's JNI table: the ([skip] + 1)th occurrence of [site] (0 for none) is faulted once with [throwable] pending. */
    @JvmStatic external fun nativeCodecEnvArm(site: Int, skip: Int, throwable: Throwable?)

    /** Disarms the calling thread; `[fired, op, op, ...]` in the order this image performed them. */
    @JvmStatic external fun nativeCodecEnvDisarm(): IntArray

    /** A CONTROL performing a codec row's four byte-array operations in order; 0, or the operation it left at. */
    @JvmStatic external fun nativeCodecEnvControl(src: ByteArray, out: Array<ByteArray?>): Int

    /** Arms the calling thread's allocation ledger; attempt [failAt] (-1 for none) answers NULL. False, changing nothing, when one is already armed. */
    @JvmStatic external fun nativeCodecAllocArm(failAt: Int): Boolean

    /**
     * Disarms from the OWNER thread: `[attempts, doubleFrees, foreignFrees, reclaimed, (size, state)...]`,
     * state 0 live, 1 retired, 2 failed, as they stood before the live blocks were reclaimed.
     * Null, changing nothing, when nothing is armed or another thread owns it.
     */
    @JvmStatic external fun nativeCodecAllocDisarm(): LongArray?

    /** A CONTROL with a formatter's allocation shape; [omitFirstFree] skips retiring the first input. */
    @JvmStatic external fun nativeCodecAllocControl(omitFirstFree: Boolean): Int

    /** A CONTROL of the free classification at one reused address: `[1, 1, 0, 0, 0]` when correct. */
    @JvmStatic external fun nativeCodecAllocReuseControl(): IntArray?

    /** A CONTROL that allocates one 32-byte block through the ledger and keeps it. */
    @JvmStatic external fun nativeCodecAllocHold(): Int

    /** Mirrors `MOQ5_CODEC_FIELDS` and the operation/site codes of the recorder. */
    const val CODEC_FIELDS: Int = 11
    const val OP_GET_REGION: Int = 1
    const val OP_NEW_ARRAY: Int = 2
    const val OP_SET_REGION: Int = 3
    const val OP_SET_ELEMENT: Int = 4
    const val OP_EXCEPTION_CHECK: Int = 5
    const val OP_DELETE_LOCAL: Int = 6
    const val SITE_NEW_ARRAY_NULL: Int = 7

    @JvmStatic
    fun threadDone(case: Int, getEnv: Int, attachDaemon: Int, attach: Int, detach: Int, highWater: Int, live: Int) {
        threads += ThreadStats(case, getEnv, attachDaemon, attach, detach, highWater, live)
    }

    @JvmStatic
    fun caseDone(case: Int) {
        doneCases += case
    }
}

/**
 * The trusted-create row, reached by REFLECTION so these rows compile while
 * the row is absent and bind to the real native method once it exists.
 *
 * The frozen contract (chats/libmoq/1365):
 *
 *     nativeEndpointCreateTrusted(String url, int trustPolicy, int versionPolicy,
 *                                 int[] versions, long handshakeMicros, int protocol,
 *                                 String webTransportPath, long[] out) -> int
 *     descriptor (Ljava/lang/String;II[IJILjava/lang/String;[J)I, instance, `public final native`
 */
internal object TrustedRow {
    const val NAME: String = "nativeEndpointCreateTrusted"

    /** Trust policy 0: the library-owned Android platform trust manager. */
    const val PLATFORM_TRUST_MANAGER: Int = 0

    fun find(): Method? =
        try {
            NativeBridge::class.java.getDeclaredMethod(
                NAME,
                String::class.java,
                Int::class.javaPrimitiveType,
                Int::class.javaPrimitiveType,
                IntArray::class.java,
                Long::class.javaPrimitiveType,
                Int::class.javaPrimitiveType,
                String::class.java,
                LongArray::class.java,
            )
        } catch (_: NoSuchMethodException) {
            null
        }

    /**
     * Invoke the row on the singleton. A throwable the native side raises is
     * rethrown as itself, so a row sees the real exception and not the
     * reflective wrapper.
     */
    fun create(url: String?, trustPolicy: Int, out: LongArray?): Int {
        val m =
            find()
                ?: throw IllegalStateException(
                    "the trusted row $NAME(String, int, int, int[], long, int, String, long[]) is absent from the compiled bridge",
                )
        try {
            // The default configuration: an AUTO offer, the backend's handshake
            // bound, the scheme's protocol, and no path override.
            return m.invoke(NativeBridge, url, trustPolicy, 0, IntArray(0), 0L, 0, null, out) as Int
        } catch (e: InvocationTargetException) {
            throw e.targetException
        }
    }
}
