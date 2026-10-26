package org.openmoq.moq5.jni

import java.io.File
import java.lang.reflect.Modifier
import java.util.zip.ZipEntry
import java.util.zip.ZipFile
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream
import javax.tools.ToolProvider
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The trusted-create row (chats/libmoq/1365, step 4 of 1312), RED-first.
 *
 *     nativeEndpointCreateTrusted(String url, int trustPolicy, int versionPolicy,
 *                                 int[] versions, long handshakeMicros, int protocol,
 *                                 String webTransportPath, long[] out) -> int
 *     descriptor (Ljava/lang/String;II[IJILjava/lang/String;[J)I; instance; `public final native`
 *     trustPolicy 0: the library-owned Android platform trust manager
 *
 * The rows reach the row by reflection so they compile while it is absent,
 * and every scenario runs in its own child JVM because the constructor
 * selector, the host mode and the trust recorder are all read once at load.
 *
 * Each child is judged on three things: its own `RESULT ok`, the absence of
 * any checked-JNI diagnostic in its output, and -- because the child's output
 * is echoed here -- the run-wide diagnostics gate sees the same lines.
 */
internal object TrustedChild {
    /** The literal prefixes tools/check-jni-diagnostics.sh classifies. */
    private val diagnostic =
        Regex(
            "WARNING in native method|FATAL ERROR in native method|" +
                "Warning: Calling other JNI functions in the scope of",
        )

    fun assertOk(out: List<String>, what: String) {
        // Echoed so the JNI diagnostics gate judges the child's forks too.
        out.forEach { println("[child] $it") }
        val hit = out.firstOrNull { diagnostic.containsMatchIn(it) }
        assertTrue(hit == null, "checked-JNI diagnostic in the child: $hit")
        assertTrue(out.contains("RESULT ok"), "$what; output was:\n" + out.joinToString("\n"))
    }

    val probeArmed: Map<String, String> = mapOf(TrustRouteProbe.ENV to TrustRouteProbe.TOKEN)
}

/** The row's compiled shape, judged on the class the JVM actually loads. */
class TrustedCreateContractTest {

    @Test
    fun `the compiled bridge declares the trusted row with the frozen descriptor and instance shape`() {
        val m = TrustedRow.find()
        assertNotNull(
            m,
            "NativeBridge declares no ${TrustedRow.NAME}(String, int, int, int[], long, int, String, long[]): the " +
                "trusted-create contract is absent from the compiled bridge",
        )
        assertEquals(Int::class.javaPrimitiveType, m.returnType, "the row returns int")
        val mods = m.modifiers
        assertTrue(Modifier.isNative(mods), "the row must be native")
        assertTrue(Modifier.isPublic(mods) && Modifier.isFinal(mods), "the row must be public final")
        assertTrue(
            !Modifier.isStatic(mods),
            "the authority binds INSTANCE methods on the object singleton; a static row would " +
                "be resolved by GetMethodID as absent",
        )
    }
}

/** Policy handling, the internal route, and the callback boundary, with the REAL target. */
class TrustedCreateRouteTest {

    @Test
    fun `policy 0 reaches the trusted route with the library delegate and unknown policies are refused before it`() {
        val out = EndpointAbi.child("trustedroute", null, TrustedChild.probeArmed)
        TrustedChild.assertOk(
            out,
            "policy 0 must reach moq_endpoint_connect_with_trust() with a non-NULL delegate and " +
                "context, and unknown policies must be refused before it",
        )
    }

    @Test
    fun `the product callback boundary consults the platform bridge with the exact chain, name and GENERIC auth type`() {
        val out = EndpointAbi.child("trustedcallback", null, TrustedChild.probeArmed)
        TrustedChild.assertOk(
            out,
            "the library delegate must decide the reference identity natively, then consult " +
                "Moq5PlatformTrust.checkServerTrusted from a foreign thread it attached exactly once " +
                "and detaches exactly once at exit, within the local-reference bound, with the " +
                "leaf-first DER chain, the server name and authType GENERIC",
        )
    }

    @Test
    fun `a callback whose cleanup registration fails answers REJECT and returns its attachment at once`() {
        val out = EndpointAbi.child(
            "trustedfault", null,
            TrustedChild.probeArmed + (TrustRouteProbe.FAULT_ENV to TrustRouteProbe.FAULT_SETSPECIFIC),
        )
        TrustedChild.assertOk(
            out,
            "with pthread_setspecific failing after the attach, the callback must consult nothing, " +
                "answer REJECT, and detach immediately so no attached thread lacks a cleanup path",
        )
    }
}

/**
 * A Java exception CROSSING JNI, from the exact class and method the product
 * caches.
 *
 * The shipped `Moq5PlatformTrust.checkServerTrusted` is total by contract, so
 * no throwable can reach C through it. To make the cached target throw, the
 * child's class path is prefixed with a test-only class of the exact name,
 * INSTANCE field and method descriptor whose method throws -- compiled here,
 * from source, with the JDK's own compiler. The shadow counts its invocations,
 * which is what proves the cached method is the shadow's.
 */
class TrustedCreateExceptionTest {
    private companion object {
        const val SHADOW_SOURCE = """
package org.openmoq.moq5.trust;

public final class Moq5PlatformTrust {
    public static final Moq5PlatformTrust INSTANCE = new Moq5PlatformTrust();
    public static volatile int CALLS = 0;
    public static volatile String LAST_AUTH = null;

    private Moq5PlatformTrust() {}

    public final int checkServerTrusted(byte[][] chain, String serverName, String authType)
            throws Exception {
        CALLS++;
        LAST_AUTH = authType;
        String kind = System.getProperty("moq5.shadow.throw", "certificate");
        System.out.println("SHADOW-CALL " + CALLS + " " + kind);
        if (kind.equals("certificate")) {
            throw new java.security.cert.CertificateException("shadow refusal");
        }
        throw new IllegalStateException("shadow non-certificate failure");
    }
}
"""

        /** Compile the shadow into a fresh directory and return it. */
        fun compileShadow(): File {
            val dir = createTempDirectory("moq5-shadow").toFile()
            val src = File(dir, "org/openmoq/moq5/trust/Moq5PlatformTrust.java")
            check(src.parentFile!!.mkdirs()) { "setup: cannot create ${src.parentFile}" }
            src.writeText(SHADOW_SOURCE)
            val javac = ToolProvider.getSystemJavaCompiler()
            assertNotNull(javac, "setup: no system Java compiler in this JDK")
            val rc = javac.run(null, null, null, "-d", dir.path, src.path)
            assertEquals(0, rc, "setup: the shadow class did not compile")
            assertTrue(File(dir, "org/openmoq/moq5/trust/Moq5PlatformTrust.class").isFile)
            return dir
        }
    }

    private fun run(kind: String) {
        val shadow = compileShadow()
        try {
            val cp = shadow.path + File.pathSeparator + System.getProperty("java.class.path").orEmpty()
            val out = EndpointAbi.child(
                "trustedshadow", null, TrustedChild.probeArmed,
                classPath = cp, jvmArgs = listOf("-Dmoq5.shadow.throw=$kind"),
            )
            TrustedChild.assertOk(
                out,
                "a $kind exception thrown by the cached checkServerTrusted must be cleared and " +
                    "translated to REJECT with nothing left pending",
            )
            assertTrue(
                out.count { it.startsWith("SHADOW-CALL ") } == 2,
                "the shadow must have been invoked twice; output was:\n" + out.joinToString("\n"),
            )
        } finally {
            shadow.deleteRecursively()
        }
    }

    @Test
    fun `a CertificateException from the cached platform method is cleared and translated to REJECT`() =
        run("certificate")

    @Test
    fun `an unrelated runtime throwable from the cached platform method is cleared and translated to REJECT`() =
        run("runtime")
}

/** The transactional ownership model, through the trusted row. */
class TrustedCreateOwnershipTest {

    @Test
    fun `the trusted row reuses the reserve-construct-commit-publish model`() {
        val out = EndpointAbi.child("trustedscripted", EndpointAbi.SCRIPTED_TOKEN)
        TrustedChild.assertOk(
            out,
            "a refused policy must consume no slot or generation, and a trusted root must be " +
                "an ordinary root to every other operation",
        )
    }

    @Test
    fun `a failed trusted publication tears the committed root down exactly once`() {
        val out = EndpointAbi.child(
            "trustedpublishfail",
            EndpointAbi.SCRIPTED_TOKEN,
            mapOf(EndpointAbi.FAILPUB to EndpointAbi.FAILPUB_TOKEN),
        )
        TrustedChild.assertOk(
            out,
            "a failed publication must stop the registry-owned trusted root and retire it, " +
                "letting registry cleanup destroy it exactly once",
        )
    }

    @Test
    fun `a full registry refuses the trusted row before its constructor and consumes nothing`() {
        val out = EndpointAbi.child("trustedcapacity", EndpointAbi.SCRIPTED_TOKEN)
        TrustedChild.assertOk(
            out,
            "the capacity refusal must be the registry's, before construction, leaving the " +
                "output untouched and the freed slot reusable with a new generation",
        )
    }

    @Test
    fun `a scripted constructor error aborts the reservation and the next root lands on the exact slot and generation`() {
        val out = EndpointAbi.child("trustedctorerror", EndpointAbi.SCRIPTED_TOKEN)
        TrustedChild.assertOk(
            out,
            "a one-shot scripted constructor error must abort the unused reservation so the next " +
                "trusted construction lands on the independently expected slot and generation",
        )
    }

    @Test
    fun `a constructor answering success with no endpoint is refused and the exact reservation is restored`() {
        val out = EndpointAbi.child("trustedctornull", null)
        TrustedChild.assertOk(
            out,
            "success with no endpoint must be reported as INVAL, publish nothing, and hand the " +
                "next reservation the same slot and generation",
        )
    }

    @Test
    fun `a constructor refusal through the trusted row aborts the unused reservation`() {
        val out = EndpointAbi.child("trustedrefusals", null)
        TrustedChild.assertOk(
            out,
            "one more refusal than the registry has slots must still be the constructor's own " +
                "answer: a leaked reservation per refusal would exhaust the registry",
        )
    }
}

/**
 * Class and method lookup belong to `JNI_OnLoad`.
 *
 * A native thread attached later resolves classes through the system loader
 * and cannot see an application class, so the library-owned trust bridge must
 * be resolved on the loading thread -- and a bridge that cannot be resolved
 * must refuse the LOAD, not fail later on the transport thread. So must a
 * per-thread cleanup key that cannot be created.
 */
class TrustedCreateLookupTest {
    private companion object {
        const val TRUST_CLASS_DIR = "org/openmoq/moq5/trust"
        const val TRUST_CLASS = "Moq5PlatformTrust"

        /** The recorder's ledger of a refused load. */
        val REFUSED = Regex(
            """MOQ5-TRUST-PROBE: load refused: globals new=(\d+) deleted=(\d+), keys created=(\d+) deleted=(\d+)""",
        )

        /**
         * A `NativeBridge` shadow that lacks the trusted row: the load resolves
         * the trust bridge first, then refuses at row preflight -- after global
         * references and the thread key exist.
         */
        const val BRIDGE_SHADOW_SOURCE = """
package org.openmoq.moq5.jni;

public final class NativeBridge {
    public static final NativeBridge INSTANCE = new NativeBridge();
    private NativeBridge() {}
    public final native int nativeAbiEpoch();
    public final native long nativeFeatureBits();
    public final native int nativeLibmoqVersion();
    public final native int nativeEndpointCreate(String url, long[] out);
    public final native int nativeEndpointRelease(long token);
    public final native int nativeEndpointWake(long token);
    public final native int nativeEndpointWait(long token, long timeoutMicros);
    public final native int nativeEndpointSnapshot(long token, long[] out);
    public final native int nativeEndpointStop(long token);
    public final native int nativeEndpointDestroy(long token);
    public final native int nativeEndpointSetInterrupted(long token, boolean interrupted);
    public final native int nativeEndpointDrain(long token, long timeoutMicros);
}
"""
    }

    /**
     * A refused load must have RELEASED what it initialised: every global
     * reference the load created deleted, and the thread key deleted if it was
     * created. Judged on the recorder's counts, taken at the JNI table and the
     * forwarded pthread calls, never inferred from a later successful process.
     */
    private fun assertReleased(out: List<String>, globalsExpected: Int, keysExpected: Int) {
        val line = out.firstOrNull { REFUSED.containsMatchIn(it) }
        assertTrue(line != null, "the recorder must report the refused load's ledger; output was:\n" +
            out.joinToString("\n"))
        val m = REFUSED.find(line!!)!!
        val (gNew, gDel, kNew, kDel) = m.groupValues.drop(1).map { it.toInt() }
        assertEquals(globalsExpected, gNew, "global references the load must have created before refusing: $line")
        assertEquals(gNew, gDel, "every global reference the refused load created must be deleted: $line")
        assertEquals(keysExpected, kNew, "thread keys the load must have created before refusing: $line")
        assertEquals(kNew, kDel, "a refused load must delete the thread key it created: $line")
    }

    /** Compile [source] for class [relative] into a fresh directory. */
    private fun compileShadow(relative: String, source: String): File {
        val dir = createTempDirectory("moq5-shadow").toFile()
        val src = File(dir, relative)
        check(src.parentFile!!.mkdirs()) { "setup: cannot create ${src.parentFile}" }
        src.writeText(source)
        val javac = ToolProvider.getSystemJavaCompiler()
        assertNotNull(javac, "setup: no system Java compiler in this JDK")
        assertEquals(0, javac.run(null, null, null, "-d", dir.path, src.path), "setup: the shadow did not compile")
        return dir
    }

    /** Whether a class-path entry name is one of the trust bridge's class files. */
    private fun isTrustClass(relative: String): Boolean =
        relative.startsWith("$TRUST_CLASS_DIR/$TRUST_CLASS") && relative.endsWith(".class")

    /** Copy jar [src] to [dst] without the trust bridge classes; returns how many were dropped. */
    private fun filterJar(src: File, dst: File): Int {
        var dropped = 0
        ZipInputStream(src.inputStream().buffered()).use { zin ->
            ZipOutputStream(dst.outputStream().buffered()).use { zout ->
                while (true) {
                    val e = zin.nextEntry ?: break
                    if (isTrustClass(e.name)) {
                        dropped++
                        continue
                    }
                    zout.putNextEntry(ZipEntry(e.name))
                    zin.copyTo(zout)
                    zout.closeEntry()
                }
            }
        }
        return dropped
    }

    @Test
    fun `an image whose trust bridge class is absent refuses the load by name`() {
        val entries = System.getProperty("java.class.path").orEmpty().split(File.pathSeparator)
        val scratch = createTempDirectory("moq5-lookup-cp").toFile()
        var removed = 0
        // The main classes reach a unit test as a JAR under AGP and as a
        // directory under a plain JVM layout; both shapes are filtered, and a
        // class path on which the class was never found fails as SETUP.
        val filtered =
            entries.mapIndexed { i, entry ->
                val f = File(entry)
                val marker = File(f, "$TRUST_CLASS_DIR/$TRUST_CLASS.class")
                when {
                    f.isDirectory && marker.isFile -> {
                        val copy = File(scratch, "entry-$i")
                        f.copyRecursively(copy)
                        File(copy, TRUST_CLASS_DIR).listFiles { c -> c.name.startsWith(TRUST_CLASS) }
                            ?.forEach { c -> check(c.delete()) { "setup: cannot remove ${c.path}" }; removed++ }
                        copy.path
                    }
                    f.isFile && entry.endsWith(".jar") &&
                        ZipFile(f).use { z -> z.getEntry("$TRUST_CLASS_DIR/$TRUST_CLASS.class") != null } -> {
                        val copy = File(scratch, "entry-$i.jar")
                        removed += filterJar(f, copy)
                        copy.path
                    }
                    else -> entry
                }
            }
        try {
            assertTrue(removed > 0, "setup: $TRUST_CLASS was not found on any class-path entry")
            val out = EndpointAbi.child("load", null, classPath = filtered.joinToString(File.pathSeparator))
            out.forEach { println("[child] $it") }
            val line = out.firstOrNull { it.startsWith("LOADFAIL ") }
            assertTrue(
                line != null,
                "with $TRUST_CLASS absent the load must be refused in JNI_OnLoad; output was:\n" +
                    out.joinToString("\n"),
            )
            assertTrue(
                line!!.contains(TRUST_CLASS),
                "the refusal must name the class it could not resolve: $line",
            )
            // The key was created before the lookup; no global reference could
            // have been taken; both must be back where they started.
            assertReleased(out, globalsExpected = 0, keysExpected = 1)
        } finally {
            scratch.deleteRecursively()
        }
    }

    @Test
    fun `a load refused at row preflight releases the trust bridge references and the thread key`() {
        val shadow = compileShadow("org/openmoq/moq5/jni/NativeBridge.java", BRIDGE_SHADOW_SOURCE)
        try {
            val cp = shadow.path + File.pathSeparator + System.getProperty("java.class.path").orEmpty()
            val out = EndpointAbi.child("load", null, classPath = cp)
            out.forEach { println("[child] $it") }
            val line = out.firstOrNull { it.startsWith("LOADFAIL ") }
            assertTrue(line != null, "with a row missing from the bridge class the load must be refused at " +
                "preflight; output was:\n" + out.joinToString("\n"))
            assertTrue(line!!.contains(TrustedRow.NAME), "the refusal must name the missing row: $line")
            // Trust binding had completed: the singleton and the byte-array
            // class were held globally, and the key existed. All released.
            assertReleased(out, globalsExpected = 2, keysExpected = 1)
        } finally {
            shadow.deleteRecursively()
        }
    }

    @Test
    fun `a per-thread cleanup key that cannot be created refuses the load`() {
        val out = EndpointAbi.child(
            "load", null,
            mapOf(TrustRouteProbe.FAULT_ENV to TrustRouteProbe.FAULT_KEY_CREATE),
        )
        out.forEach { println("[child] $it") }
        val line = out.firstOrNull { it.startsWith("LOADFAIL ") }
        assertTrue(
            line != null,
            "with pthread_key_create failing, JNI_OnLoad must refuse the load: a callback with no " +
                "cleanup path must never become reachable; output was:\n" + out.joinToString("\n"),
        )
        assertTrue(line!!.contains("key", ignoreCase = true), "the refusal must name the key: $line")
        // Nothing was created, so nothing may have been deleted either.
        assertReleased(out, globalsExpected = 0, keysExpected = 0)
    }
}
