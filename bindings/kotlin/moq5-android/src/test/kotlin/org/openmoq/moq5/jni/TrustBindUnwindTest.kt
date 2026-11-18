package org.openmoq.moq5.jni

import java.io.File
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
 * The trust bridge's INITIALIZATION must not carry a pending exception into its
 * next lookup.
 *
 * `moq5_trust_bind` runs on the loading thread and makes a chain of fallible
 * JNI lookups. A failed lookup leaves its own exception pending, and with one
 * pending only a restricted set of JNI functions may be called -- another
 * lookup is not among them. These rows drive each fallible step to fail and
 * require the load to be refused by name, with a clean checked-JNI record and
 * every already-held resource returned.
 *
 * A missing field or a null singleton is driven with a REAL shadow class, so
 * the JVM produces exactly the state a real deployment would. Reference-table
 * exhaustion cannot be provoked that way and is injected in the host image
 * only; it stands for the table refusing, not for an actual VM out-of-memory.
 */
internal class TrustBindUnwindTest {

    private companion object {
        const val TRUST_DIR = "org/openmoq/moq5/trust"
        const val TRUST_CLASS = "Moq5PlatformTrust"

        /** The recorder's ledger of a refused load. */
        val REFUSED = Regex(
            """MOQ5-TRUST-PROBE: load refused: globals new=(\d+) deleted=(\d+), keys created=(\d+) deleted=(\d+)""",
        )

        /** The real class name and check method, and NO `INSTANCE` field. */
        const val NO_INSTANCE = """
package org.openmoq.moq5.trust;

public final class Moq5PlatformTrust {
    public int checkServerTrusted(byte[][] chain, String host, String authType) { return 0; }
}
"""

        /** The field exists and reads null: nothing is thrown, and it must still be refused. */
        const val NULL_INSTANCE = """
package org.openmoq.moq5.trust;

public final class Moq5PlatformTrust {
    public static final Moq5PlatformTrust INSTANCE = null;
    public int checkServerTrusted(byte[][] chain, String host, String authType) { return 0; }
}
"""

        /** The field and singleton exist and the check method does not. */
        const val NO_METHOD = """
package org.openmoq.moq5.trust;

public final class Moq5PlatformTrust {
    public static final Moq5PlatformTrust INSTANCE = new Moq5PlatformTrust();
}
"""
    }

    /** Compile a replacement trust class into a fresh directory. */
    private fun compileTrust(source: String): File {
        val dir = createTempDirectory("moq5-trust-shadow").toFile()
        val src = File(dir, "$TRUST_DIR/$TRUST_CLASS.java")
        check(src.parentFile!!.mkdirs()) { "setup: cannot create ${src.parentFile}" }
        src.writeText(source)
        val javac = ToolProvider.getSystemJavaCompiler()
        assertNotNull(javac, "setup: no system Java compiler in this JDK")
        assertEquals(0, javac.run(null, null, null, "-d", dir.path, src.path), "setup: the shadow did not compile")
        return dir
    }

    private fun isTrustClass(relative: String): Boolean =
        relative.startsWith("$TRUST_DIR/$TRUST_CLASS") && relative.endsWith(".class")

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

    /** This JVM's class path with the real trust class removed and [shadow] put first. */
    private fun classPathWithShadow(scratch: File, shadow: File): String {
        var removed = 0
        val filtered =
            System.getProperty("java.class.path").orEmpty().split(File.pathSeparator).mapIndexed { i, entry ->
                val f = File(entry)
                when {
                    f.isDirectory && File(f, "$TRUST_DIR/$TRUST_CLASS.class").isFile -> {
                        val copy = File(scratch, "entry-$i")
                        f.copyRecursively(copy)
                        File(copy, TRUST_DIR).listFiles { c -> c.name.startsWith(TRUST_CLASS) }
                            ?.forEach { c -> check(c.delete()) { "setup: cannot remove ${c.path}" }; removed++ }
                        copy.path
                    }
                    f.isFile && entry.endsWith(".jar") &&
                        ZipFile(f).use { z -> z.getEntry("$TRUST_DIR/$TRUST_CLASS.class") != null } -> {
                        val copy = File(scratch, "entry-$i.jar")
                        removed += filterJar(f, copy)
                        copy.path
                    }
                    else -> entry
                }
            }
        assertTrue(removed > 0, "setup: $TRUST_CLASS was not found on any class-path entry")
        return (listOf(shadow.path) + filtered).joinToString(File.pathSeparator)
    }

    /**
     * Drive one refused load and judge it whole: it is refused by name, the
     * child produced no checked-JNI diagnostic, and everything the load took
     * before refusing was returned.
     */
    private fun refusedLoad(
        out: List<String>,
        names: String,
        globalsExpected: Int,
        keysExpected: Int,
    ) {
        out.forEach { println("[child] $it") }
        val diagnostic = out.firstOrNull {
            it.contains("WARNING in native method") || it.contains("FATAL ERROR in native method") ||
                it.contains("Warning: Calling other JNI functions in the scope of")
        }
        assertTrue(diagnostic == null, "the refusal must make no illegal JNI call: $diagnostic")
        val line = out.firstOrNull { it.startsWith("LOADFAIL ") }
        assertTrue(line != null, "the load must be refused; output was:\n" + out.joinToString("\n"))
        assertTrue(line!!.contains(names), "the refusal must name what it could not use: $line")
        val ledger = out.firstOrNull { REFUSED.containsMatchIn(it) }
        assertTrue(ledger != null, "the recorder must report the refused load's ledger; output was:\n" + out.joinToString("\n"))
        val (gNew, gDel, kNew, kDel) = REFUSED.find(ledger!!)!!.groupValues.drop(1).map { it.toInt() }
        assertEquals(globalsExpected, gNew, "global references created before refusing: $ledger")
        assertEquals(gNew, gDel, "every global reference the refused load created must be deleted: $ledger")
        assertEquals(keysExpected, kNew, "thread keys created before refusing: $ledger")
        assertEquals(kNew, kDel, "a refused load must delete the thread key it created: $ledger")
    }

    private fun withShadow(source: String, body: (String) -> Unit) {
        val shadow = compileTrust(source)
        val scratch = createTempDirectory("moq5-trust-cp").toFile()
        try {
            body(classPathWithShadow(scratch, shadow))
        } finally {
            shadow.deleteRecursively()
            scratch.deleteRecursively()
        }
    }

    @Test
    fun `a trust class with no INSTANCE field is refused at that lookup, with no further JNI call`() =
        withShadow(NO_INSTANCE) { cp ->
            refusedLoad(
                EndpointAbi.child("load", null, TrustedChild.probeArmed, classPath = cp),
                names = "INSTANCE",
                globalsExpected = 0,
                keysExpected = 1,
            )
        }

    @Test
    fun `a trust singleton that reads null is refused although nothing was thrown`() =
        withShadow(NULL_INSTANCE) { cp ->
            refusedLoad(
                EndpointAbi.child("load", null, TrustedChild.probeArmed, classPath = cp),
                names = "null",
                globalsExpected = 0,
                keysExpected = 1,
            )
        }

    @Test
    fun `a trust class with no check method is refused at that lookup, with no further JNI call`() =
        withShadow(NO_METHOD) { cp ->
            refusedLoad(
                EndpointAbi.child("load", null, TrustedChild.probeArmed, classPath = cp),
                names = "checkServerTrusted",
                globalsExpected = 0,
                keysExpected = 1,
            )
        }

    @Test
    fun `a reference table that cannot hold the singleton refuses before the next lookup`() =
        refusedLoad(
            EndpointAbi.child(
                "load", null,
                TrustedChild.probeArmed + ("MOQ5_JNI_HOST_TEST_FAIL_GLOBAL" to "first"),
            ),
            names = "hold the trust bridge",
            globalsExpected = 0,
            keysExpected = 1,
        )

    @Test
    fun `a reference table that cannot hold the byte-array class refuses and returns the singleton`() =
        refusedLoad(
            EndpointAbi.child(
                "load", null,
                TrustedChild.probeArmed + ("MOQ5_JNI_HOST_TEST_FAIL_GLOBAL" to "second"),
            ),
            names = "byte-array class",
            globalsExpected = 1,
            keysExpected = 1,
        )
}
