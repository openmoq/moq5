package org.openmoq.moq5.jni

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * R6 through the PUBLIC object stream.
 *
 * `MoqEndpoint` builds the engine, the engine's worker drives the real
 * `JniEndpointBackend`, and the object is polled, decoded and handed to
 * `receiver.objects.next()` by shipped code. The fixture owns only the root and
 * the injected media, and does that work on the engine's own worker thread --
 * which it must, because the registry is authority-thread confined and the
 * worker is the authority for a root it created.
 *
 * HOST fixture: the root is the scripted target and the media is injected. This
 * is not authenticated-network and not Android-device evidence.
 */
internal class CmafStatusPublicTest {
    private val env get() = TrustedChild.probeArmed + (EndpointAbi.MODE to EndpointAbi.MODE_DRAIN_PENDING)

    @Test
    fun `a payload-free CMAF end-of-group object is delivered on the public object stream with empty media`() {
        TrustedChild.assertOk(
            EndpointAbi.child("cmafstatuspublic", EndpointAbi.SCRIPTED_TOKEN, env),
            "the public stream delivers the status object on the INVENTORY's own MediaTrack instance with " +
                "EndOfGroup, the declared metadata, an empty CMAF fragment, ByteRange(0,0), no samples and empty " +
                "mediaBytes(); the native object is cleaned exactly once and nothing stays queued",
        )
    }

    private companion object {
        /** The declared body failure, class and cause, exactly as the child emits it. */
        const val BODY =
            "PROBLEM|body|java.lang.UnsupportedOperationException|r6-injected-body|" +
                "java.lang.IllegalArgumentException|r6-injected-cause"

        /** The declared cleanup-reporting failure, which carries no throwable of its own. */
        const val CLEANUP = "PROBLEM|cleanup|note||r6-injected-cleanup|"

        /** Teardown really happened, reported from what it did. */
        const val TORN_DOWN = "DONE|teardown|receiver=closed|endpoint=retired"

        /** The audits really ran, reported from what they measured. */
        const val AUDITED = "DONE|audits|setups=1|injections=1|threads=1"

        const val RECORDER_CLEAN = "DONE|final-audit|recorder=clean"
    }

    /**
     * An expected-failure child, judged on its EXACT declared inventory.
     *
     * The problems are compared as a complete ordered list of field-delimited
     * records -- phase, throwable class, message, cause class, cause message --
     * so an unrelated exception, a missing failure or an extra one all fail.
     * The evidence lines are compared the same way, and they are POSITIVE
     * completion records emitted by the operations themselves: a row cannot
     * pass by skipping its teardown or its audits. The final `RESULT` line
     * pins which throwable actually reached the top handler.
     */
    private fun refused(
        scenario: String,
        problems: List<String>,
        evidence: List<String>,
        result: String,
    ) {
        val out = EndpointAbi.child(scenario, EndpointAbi.SCRIPTED_TOKEN, env)
        out.forEach { println("[child] $it") }
        val diagnostic = out.firstOrNull {
            it.contains("WARNING in native method") || it.contains("FATAL ERROR in native method") ||
                it.contains("Warning: Calling other JNI functions in the scope of")
        }
        assertTrue(diagnostic == null, "$scenario: a controlled failure must make no illegal JNI call: $diagnostic")
        assertTrue(!out.contains("RESULT ok"), "$scenario: a failing row must not record success; output was:\n" + out.joinToString("\n"))
        assertEquals(
            problems, out.filter { it.startsWith("PROBLEM|") },
            "$scenario: exactly the declared failures, with their classes and causes; output was:\n" + out.joinToString("\n"),
        )
        assertEquals(
            evidence, out.filter { it.startsWith("DONE|") },
            "$scenario: exactly the declared completion evidence; output was:\n" + out.joinToString("\n"),
        )
        assertTrue(
            out.contains(result),
            "$scenario: the original throwable must reach the top handler: expected '$result'; output was:\n" + out.joinToString("\n"),
        )
    }

    @Test
    fun `a body failure keeps its own class and cause while teardown and the audits still complete`() =
        refused(
            "cmafstatuspublicbody",
            problems = listOf(BODY),
            evidence = listOf(TORN_DOWN, AUDITED, RECORDER_CLEAN),
            result = "RESULT threw java.lang.UnsupportedOperationException: r6-injected-body",
        )

    @Test
    fun `a body failure and a cleanup failure are BOTH reported, and the body one is still the primary`() =
        refused(
            "cmafstatuspublicboth",
            problems = listOf(BODY, CLEANUP),
            evidence = listOf(TORN_DOWN, AUDITED, RECORDER_CLEAN),
            result = "RESULT threw java.lang.UnsupportedOperationException: r6-injected-body",
        )

    @Test
    fun `a cleanup-only failure fails on its own, with no success recorded`() =
        refused(
            "cmafstatuspubliccleanup",
            problems = listOf(CLEANUP),
            evidence = listOf(TORN_DOWN, AUDITED, RECORDER_CLEAN),
            result = "RESULT threw java.lang.IllegalStateException: public CMAF status delivery: cleanup: r6-injected-cleanup",
        )

    @Test
    fun `a throwing final audit is collected as one more failure and suppresses nothing`() =
        refused(
            "cmafstatuspublicaudit",
            problems = listOf(BODY, CLEANUP, "PROBLEM|final-audit|java.lang.IllegalStateException|r6-injected-audit||"),
            // The audits themselves completed and reported; only the recorder
            // check beyond them did not run, so its evidence is absent.
            evidence = listOf(TORN_DOWN, AUDITED),
            result = "RESULT threw java.lang.UnsupportedOperationException: r6-injected-body",
        )

    @Test
    fun `a fixture setup failure after a successful native attach releases that child before retiring`() =
        refused(
            "cmafstatuspublicsetup",
            problems = listOf(
                "PROBLEM|body|java.lang.IllegalStateException|" +
                    "fixture: injected setup failure after a successful native attach||",
            ),
            // The receiver never reached the caller, the endpoint still retired,
            // and the released child is stale by the registry's own answer.
            evidence = listOf(
                "DONE|teardown|receiver=absent|endpoint=retired",
                "DONE|audits|released=true|close=Ok|stale=${EndpointAbi.REG_STALE}",
                RECORDER_CLEAN,
            ),
            result = "RESULT threw java.lang.IllegalStateException: " +
                "fixture: injected setup failure after a successful native attach",
        )
}
