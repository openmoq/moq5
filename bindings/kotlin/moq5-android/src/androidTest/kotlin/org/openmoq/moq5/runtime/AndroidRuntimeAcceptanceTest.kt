package org.openmoq.moq5.runtime

import android.os.Handler
import android.os.Looper
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import java.io.File
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.openmoq.moq5.EndpointConfig
import org.openmoq.moq5.MoqEndpoint
import org.openmoq.moq5.jni.BridgeInit
import org.openmoq.moq5.jni.NativeBridge
import org.openmoq.moq5.jni.NativeBridgeOwner

/**
 * What only a device can answer.
 *
 * Every other test in this module runs on a host JVM against an object the test
 * itself located and loaded. That proves the C code works; it proves nothing
 * about Android, because the host has no APK, no packaging step, no platform
 * loader and no main looper. The observations here are the runtime
 * counterpart, in four classes: PACKAGING (the object arrives through the
 * application's own library directory), the BRIDGE HANDSHAKE against the
 * compiled-in authorities, the PUBLIC ENDPOINT path and its main-thread
 * independence with the oracle's own controls, and the engine's PROMPTNESS on
 * an idle endpoint. Every observation is named in
 * `tools/check-android-runtime.sh`, so the gate fails when one stops existing
 * rather than reporting a smaller, still-green count; the list of names is
 * pinned there and in the declaration gate, not here.
 *
 * They are deliberately NOT network tests. The endpoint cases construct a
 * root and request its close; no relay is contacted, nothing is published or
 * subscribed, and the loopback authority they name is never expected to
 * answer.
 */
@RunWith(AndroidJUnit4::class)
class AndroidRuntimeAcceptanceTest {

    /**
     * A loopback authority with nothing behind it.
     *
     * Construction is the fact under test, so the address only has to be
     * well-formed: it is never resolved off the device and nothing is expected
     * to accept a connection on it.
     */
    private val loopbackUrl = "https://127.0.0.1:4433/moq"

    /**
     * The JNI object arrives through ANDROID PACKAGING, not a host path.
     *
     * The claim has two halves and needs both. `System.loadLibrary` succeeding
     * says something loaded, but not from where -- so the first half locates
     * the file by its platform-mapped name inside the directory the package
     * manager owns for this application, which is the only place an APK's
     * `jniLibs` payload lands. The second half is that the product's ONLY
     * load entry point, the one that resolves by name and has no file-path
     * overload, reaches a completed handshake over it.
     *
     * This module instruments itself, so the application whose library
     * directory is inspected is the instrumentation APK: the AAR's payload is
     * merged into it exactly as it would be into a consumer's APK. That is the
     * packaging route under test, and it is stated here rather than implied.
     */
    @Test
    fun packagedImageResolvesFromTheApplicationLibraryDirectory() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val libraryDir = context.applicationInfo.nativeLibraryDir
        val mapped = System.mapLibraryName(NativeBridgeOwner.LIBRARY_NAME)
        val packaged = File(libraryDir, mapped)

        assertTrue(
            "$mapped is not present in the application's own library directory $libraryDir",
            packaged.isFile,
        )
        assertTrue("$packaged is empty", packaged.length() > 0L)

        val outcome = NativeBridgeOwner.initialize()
        assertTrue(
            "the packaged-name load did not reach a ready bridge: $outcome",
            outcome is BridgeInit.Ready,
        )
    }

    /**
     * The private ABI/feature handshake, decided on the device.
     *
     * The Kotlin side carries a compiled-in epoch and a required feature mask;
     * the object in the APK carries its own. The handshake is only evidence if
     * the observed values are compared with those authorities rather than with
     * themselves, so both are asserted explicitly, and the feature check is a
     * SUBSET test: a newer native side may advertise capabilities this build
     * has never heard of, and that must not read as a failure.
     */
    @Test
    fun bridgeHandshakeAgreesWithTheCompiledAuthority() {
        val outcome = NativeBridgeOwner.initialize()
        assertTrue("the bridge is not ready: $outcome", outcome is BridgeInit.Ready)
        val ready = outcome as BridgeInit.Ready

        assertEquals(
            "the packaged object was built to a different private ABI epoch",
            NativeBridge.ABI_EPOCH,
            ready.abiEpoch,
        )
        assertEquals(
            "a required capability is absent from the packaged object",
            NativeBridge.FEATURES_REQUIRED,
            ready.features and NativeBridge.FEATURES_REQUIRED,
        )
        assertTrue(
            "the diagnostic LibMoQ version is not positive: ${ready.libmoqVersion}",
            ready.libmoqVersion > 0,
        )
    }

    /**
     * PUBLIC construction plus a close REQUEST complete on device WITHOUT the
     * main thread. Exactly that: `open()` and the non-suspending `close()`.
     * Snapshot and the joined close are separate observations below.
     *
     * [MainLooperHold] states the fact directly: the main looper is held for
     * the whole operation, the operation has its own deadline strictly inside
     * the hold, and only `Independent` -- completed while still held, holder
     * never expired -- is accepted. A dependency on the main thread is
     * classified as `MainThreadDependent`, and the control test below proves
     * the oracle actually produces that classification for work that needs
     * the looper, so this row cannot pass by a timeout that quietly released
     * the forbidden dependency.
     */
    @Test
    fun endpointConstructAndCloseCompletesOffTheMainThread() {
        val verdict =
            MainLooperHold.run(
                workDeadlineMillis = WORK_DEADLINE_MILLIS,
                holdTimeoutMillis = HOLD_TIMEOUT_MILLIS,
                recoveryMillis = RECOVERY_MILLIS,
            ) {
                runBlocking {
                    withContext(Dispatchers.Default) {
                        assertNotEquals(
                            "the endpoint work ran on the main thread",
                            Looper.getMainLooper().thread,
                            Thread.currentThread(),
                        )
                        // Construct, then REQUEST the close. The seed record
                        // open() waits for is already a round trip through
                        // the engine's service thread. Joining is judged by
                        // its own observation below, so this row's verdict is
                        // about main-thread independence and nothing else.
                        val endpoint = MoqEndpoint.open(EndpointConfig(url = loopbackUrl))
                        endpoint.close()
                    }
                }
            }
        assertTrue(
            "public open() and the close() request must complete while the main looper is held; verdict: $verdict",
            verdict is MainLooperHold.Verdict.Independent,
        )
    }

    /**
     * Joining the close of a CONNECTING endpoint returns promptly.
     *
     * `closeAndJoin()` requests the close and waits for the engine to retire.
     * The request wakes the backend so a worker parked in its native wait
     * acts on it at once; a join that returns only when the transport
     * handshake bound elapses means the wake never returned that wait. The
     * deadline sits far below the handshake bound and the wait slice, so
     * "eventually" cannot pass as "promptly". On the deadline path the close
     * is still requested, so no endpoint outlives the row un-asked.
     */
    @Test
    fun closeAndJoinOnAConnectingEndpointReturnsPromptly() {
        runBlocking {
            withContext(Dispatchers.Default) {
                val endpoint = MoqEndpoint.open(EndpointConfig(url = loopbackUrl))
                val started = System.nanoTime()
                val joined = withTimeoutOrNull(PROMPT_DEADLINE_MILLIS) { endpoint.closeAndJoin(); true }
                val elapsedMillis = (System.nanoTime() - started) / 1_000_000
                if (joined == null) endpoint.close()
                assertTrue(
                    "closeAndJoin() on a connecting endpoint did not return within ${PROMPT_DEADLINE_MILLIS}ms " +
                        "(waited ${elapsedMillis}ms); the close request must return the worker's native wait",
                    joined != null,
                )
            }
        }
    }

    /**
     * THE CLEANUP CONTROL for the oracle.
     *
     * Work that ignores its interrupt for longer than the reaping bound must
     * be refused by name, never reported as the expected dependent verdict:
     * a worker that outlives the oracle would contaminate whatever observation
     * runs next. The stubborn worker is deliberately released by this test
     * afterwards and joined within a bound, so the control itself leaves no
     * thread behind.
     */
    @Test
    fun aWorkerThatSurvivesCleanupIsRefusedAsAHarnessFailure() {
        val letGo = CountDownLatch(1)
        val worker = AtomicReference<Thread?>(null)
        val verdict =
            MainLooperHold.run(
                workDeadlineMillis = CONTROL_DEADLINE_MILLIS,
                holdTimeoutMillis = CONTROL_HOLD_MILLIS,
                recoveryMillis = STUBBORN_REAP_MILLIS,
            ) {
                worker.set(Thread.currentThread())
                // Ignore every interrupt until released: each interrupted
                // wait is swallowed and the wait resumed, well past the reap
                // bound. Only the latch releases it -- the test counts it
                // down in its finally -- and the limit is a last-resort
                // ceiling so a broken test cannot pin this thread forever.
                var released = false
                while (!released) {
                    try {
                        released = letGo.await(STUBBORN_LIMIT_MILLIS, TimeUnit.MILLISECONDS)
                        if (!released) break // the ceiling: give up rather than spin
                    } catch (ignored: InterruptedException) {
                        // deliberately stubborn
                    }
                }
            }
        try {
            assertTrue(
                "a worker that survives cleanup must be refused by the worker-survived diagnostic; verdict: $verdict",
                verdict is MainLooperHold.Verdict.HarnessFailure &&
                    verdict.reason.contains(MainLooperHold.WORKER_SURVIVED),
            )
        } finally {
            letGo.countDown()
            worker.get()?.join(STUBBORN_JOIN_MILLIS)
            assertEquals("the stubborn worker must have exited once released", false, worker.get()?.isAlive)
        }
    }

    /**
     * A snapshot on an idle endpoint returns promptly.
     *
     * `snapshot()` is one queued job on the engine worker, and submission
     * wakes the backend so a worker parked in its native wait claims it at
     * once. That is the contract; this row is the device's answer. The
     * deadline is deliberately far below both the transport handshake bound
     * and the worker's wait slice, so a snapshot that is only delivered when
     * the endpoint next changes state -- a wake that never returned the wait
     * -- fails here by name instead of passing after the handshake times out.
     * The endpoint is always closed afterwards, on the deadline path too.
     */
    @Test
    fun snapshotOnAnIdleEndpointReturnsPromptly() {
        runBlocking {
            withContext(Dispatchers.Default) {
                val endpoint = MoqEndpoint.open(EndpointConfig(url = loopbackUrl))
                try {
                    val started = System.nanoTime()
                    val snapshot = withTimeoutOrNull(PROMPT_DEADLINE_MILLIS) { endpoint.snapshot() }
                    val elapsedMillis = (System.nanoTime() - started) / 1_000_000
                    assertTrue(
                        "snapshot() on an idle endpoint did not return within ${PROMPT_DEADLINE_MILLIS}ms " +
                            "(waited ${elapsedMillis}ms); a submission wake must return the worker's native wait",
                        snapshot != null,
                    )
                } finally {
                    // Requested, not joined: the join is judged by its own row.
                    endpoint.close()
                }
            }
        }
    }

    /**
     * THE NEGATIVE CONTROL for the oracle above.
     *
     * Work that genuinely needs the main looper -- it posts to the looper and
     * waits for that post to run -- must be classified as dependent. This is
     * the row that gives the previous one its meaning: an oracle that
     * released the hold before judging completion, or that ignored the
     * holder's expiry, would call this work independent, and this test would
     * fail by name. Its deadline is deliberately short, so the expected
     * timeout stays local to this control and never lengthens the suite.
     */
    @Test
    fun workThatNeedsTheMainLooperIsClassifiedAsDependent() {
        val verdict =
            MainLooperHold.run(
                workDeadlineMillis = CONTROL_DEADLINE_MILLIS,
                holdTimeoutMillis = CONTROL_HOLD_MILLIS,
                recoveryMillis = RECOVERY_MILLIS,
            ) {
                val ran = CountDownLatch(1)
                Handler(Looper.getMainLooper()).post { ran.countDown() }
                // Bounded, and longer than the work deadline: the oracle's
                // deadline must be what ends this, not the work giving up.
                ran.await(CONTROL_HOLD_MILLIS, TimeUnit.MILLISECONDS)
            }
        assertTrue(
            "main-looper-dependent work must be classified as dependent; verdict: $verdict",
            verdict is MainLooperHold.Verdict.MainThreadDependent,
        )
    }

    private companion object {
        /** The endpoint work's own deadline. Generous for a cold emulator. */
        const val WORK_DEADLINE_MILLIS = 30_000L

        /**
         * How long an idle-endpoint snapshot may take. Far below the 30 s
         * transport handshake bound and the worker's 60 s wait slice, so a
         * snapshot delivered only on the next state change cannot pass.
         */
        const val PROMPT_DEADLINE_MILLIS = 5_000L

        /** The holder's emergency release, strictly after the work deadline. */
        const val HOLD_TIMEOUT_MILLIS = 45_000L

        /** The control's deadline: short, because its timeout is the expected outcome. */
        const val CONTROL_DEADLINE_MILLIS = 2_000L
        const val CONTROL_HOLD_MILLIS = 5_000L

        /** How long release, the post-release looper proof and the worker reap may take. */
        const val RECOVERY_MILLIS = 10_000L

        /** The cleanup control's reap bound, and how long its stubborn worker may hold out. */
        const val STUBBORN_REAP_MILLIS = 1_000L
        const val STUBBORN_LIMIT_MILLIS = 60_000L
        const val STUBBORN_JOIN_MILLIS = 10_000L
    }
}
