package org.openmoq.moq5.engine

import kotlinx.coroutines.test.runTest
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * One row per PRIVATE-REFUSAL branch of the bridge: wake, wait, snapshot, stop
 * and post-stop destroy.
 *
 * A private registry refusal -- `STALE`, `KIND`, `THREAD`, `STATE` -- is the
 * bridge's own vocabulary, not a native endpoint result. It says the token this
 * engine holds is no longer one the bridge will honour, so it can mean neither
 * "the endpoint reported this" nor "reclamation happened". Every row therefore
 * asserts the same three facts:
 *
 *  * the refusal is carried EXACTLY -- the op name and the private code -- and
 *    is never normalised into a native result, a stop refusal, or a wait state;
 *  * the native ownership is QUARANTINED: no later backend call, no retry, and
 *    in particular NO destroy, which would free an object the bridge just said
 *    this engine cannot address;
 *  * every waiter, already-registered or late, learns that one stable failure.
 *
 * The engine's `close()` is the observation point for the last fact because it
 * is the only call that can report reclamation, and reporting success here
 * would be the exact lie this tranche exists to prevent.
 */
internal class EndpointEngineBridgeRefusalTest : EngineFixture() {
    private companion object {
        /** `MOQ5_ERR_REGISTRY_THREAD` -- the authority-thread refusal. */
        const val THREAD = -1005

        /** `MOQ5_ERR_REGISTRY_STALE` -- the generation refusal. */
        const val STALE = -1002
    }

    /**
     * Drives one refusal to quarantine and asserts the three facts above.
     *
     * [arm] installs the refusal; [drive] performs whatever operation reaches
     * it. Both are given the backend and the engine so a row can choose its own
     * route without a second copy of the assertions.
     */
    private suspend fun assertQuarantines(
        expectedOp: String,
        expectedCode: Int,
        arm: (ScriptedEndpointBackend) -> Unit,
        drive: suspend (EndpointEngine, ScriptedEndpointBackend) -> Unit,
    ) {
        val (e, b) = newEngine()
        // Armed before the engine is quarantined so an assertion failure ahead
        // of the refusal still gets ordinary cleanup.
        arm(b)
        markQuarantined(e)
        // The root must exist before the refusal is driven: a close that wins
        // the race against construction is the SEPARATE cancellation window,
        // and it legitimately makes no backend call at all.
        assertTrue(
            b.awaitCondition("the worker publishes the root") { it.serviceThread != null },
            "the worker never reached the endpoint",
        )
        drive(e, b)

        // The refusal can be latched by the worker rather than by the driving
        // call, so the latch is awaited under a deadlock ceiling. No assertion
        // depends on how long it takes -- only on it happening at all.
        val failure = awaitQuarantine(e)
        assertIs<BridgeContractException>(
            failure,
            "the refusal must be carried as a bridge-contract failure, not as a " +
                "stop refusal or a native result",
        )
        assertEquals(expectedOp, failure.failure.op, "the refused op must be carried exactly")
        assertEquals(expectedCode, failure.failure.code, "the private code must be carried exactly")

        assertEquals(
            0,
            b.observed().destroyCount,
            "a bridge refusal must NEVER be followed by destroy: the bridge just refused the " +
                "very token the destroy would address",
        )

        // Every waiter, including a late one, learns the SAME failure object --
        // not a fresh exception, not success, and not a hang.
        repeat(2) { attempt ->
            val thrown =
                try {
                    e.close()
                    null
                } catch (t: Throwable) {
                    t
                }
            assertTrue(
                thrown === failure,
                "close() #$attempt must rethrow the one retained failure, saw $thrown",
            )
        }

        // Retirement is latched by the close above, and it must NOT have added
        // a teardown call: the quarantine holds through retirement itself.
        assertTrue(e.isBackendRetired, "close() must retire the engine")
        assertEquals(
            0,
            b.observed().destroyCount,
            "retiring a quarantined engine must still make no destroy call",
        )
    }

    /** The quarantine latch, awaited under a ceiling. Null if it never comes. */
    private fun awaitQuarantine(e: EndpointEngine): Exception? {
        val deadline = System.nanoTime() + ScriptedEndpointBackend.CEILING_MILLIS * 1_000_000
        while (System.nanoTime() < deadline) {
            e.quarantineFailure?.let { return it }
            Thread.onSpinWait()
        }
        return e.quarantineFailure
    }

    /**
     * A quarantined engine ACCEPTS NO LATER WORK.
     *
     * The quarantine says the bridge will not honour this engine's token any
     * more. A body submitted after that is, once the real native backend
     * exists, overwhelmingly likely to be native or registry work -- so
     * accepting it and running it would be exactly the post-refusal native call
     * the quarantine exists to prevent.
     *
     * The three submission shapes report this differently because their
     * signatures differ, and each is pinned separately:
     *
     *  * `post()` and `postAndWait()` answer with the REFUSED_SHUTDOWN value;
     *  * `perform()` has no such value, so it fails -- and it must fail with the
     *    retained bridge failure, NOT with `EngineClosedException`, which would
     *    tell the caller the engine shut down normally.
     *
     * Deliberately NOT asserted here: a body accepted BEFORE the quarantine.
     * Those drain under the existing shutdown rule, and changing that is a
     * separate policy question rather than something this row decides.
     */
    @Test
    fun `a quarantined engine refuses every later submission and runs no body`() =
        runTest {
            val (e, b) = newEngine()
            b.wakeResult = BackendOpResult.Bridge(BridgeFailure("wake", THREAD))
            markQuarantined(e)
            assertTrue(
                b.awaitCondition("the worker publishes the root") { it.serviceThread != null },
                "the worker never reached the endpoint",
            )

            // Drives the refused wake, which is what latches the quarantine.
            e.post { }
            val failure = awaitQuarantine(e)
            assertIs<BridgeContractException>(failure, "the quarantine must be latched first")

            val ran = AtomicBoolean(false)

            assertEquals(
                SubmitOutcome.REFUSED_SHUTDOWN,
                e.post { ran.set(true) },
                "post() must refuse once the engine is quarantined",
            )
            assertEquals(
                SubmitOutcome.REFUSED_SHUTDOWN,
                e.postAndWait { ran.set(true) },
                "postAndWait() must refuse once the engine is quarantined",
            )
            val thrown =
                try {
                    e.perform { ran.set(true) }
                    null
                } catch (t: Throwable) {
                    t
                }
            assertTrue(
                thrown === failure,
                "perform() must fail with the retained bridge failure, saw $thrown",
            )

            assertEquals(false, ran.get(), "no refused body may run")
            assertEquals(
                0,
                b.observed().destroyCount,
                "a refused submission must not trigger a teardown either",
            )

            // The close contract is unchanged by the refusals above.
            val closeFailure =
                try {
                    e.close()
                    null
                } catch (t: Throwable) {
                    t
                }
            assertTrue(closeFailure === failure, "close() must still report the same failure")
            assertEquals(0, b.observed().destroyCount, "no destroy may be attempted")
        }

    @Test
    fun `a refused wake quarantines the endpoint and never destroys it`() =
        runTest {
            assertQuarantines(
                expectedOp = "wake",
                expectedCode = THREAD,
                arm = { it.wakeResult = BackendOpResult.Bridge(BridgeFailure("wake", THREAD)) },
                drive = { e, _ ->
                    // post() raises the engine level and the backend's, so the
                    // refusal is reached on the SUBMITTING thread -- the one
                    // path that quarantines from outside the worker.
                    assertEquals(SubmitOutcome.ACCEPTED, e.post { })
                },
            )
        }

    @Test
    fun `a wait result outside the contract quarantines rather than inventing a state`() =
        runTest {
            assertQuarantines(
                expectedOp = "wait",
                expectedCode = STALE,
                arm = { it.waitBridgeFailure = BridgeFailure("wait", STALE) },
                drive = { _, b ->
                    assertTrue(
                        b.awaitCondition("a refused wait is answered") {
                            it.waitReturns.any { r -> r is BackendWaitResult.Bridge }
                        },
                        "the worker never reached the refused wait",
                    )
                },
            )
        }

    @Test
    fun `a refused snapshot fails the caller and quarantines`() =
        runTest {
            var thrown: Throwable? = null
            assertQuarantines(
                expectedOp = "snapshot",
                expectedCode = STALE,
                arm = {
                    it.snapshotResult = BackendSnapshotResult.Bridge(BridgeFailure("snapshot", STALE))
                },
                drive = { e, _ ->
                    thrown =
                        try {
                            e.snapshot()
                            null
                        } catch (t: Throwable) {
                            t
                        }
                },
            )
            assertIs<BridgeContractException>(
                thrown,
                "the snapshot caller must learn the refusal, never a fabricated record",
            )
        }

    @Test
    fun `a refused stop is not reported as a native stop refusal`() =
        runTest {
            assertQuarantines(
                expectedOp = "stop",
                expectedCode = THREAD,
                arm = { it.stopBridgeFailure = BridgeFailure("stop", THREAD) },
                drive = { e, _ ->
                    // The first close drives retirement, which is where stop is
                    // called. It throws the refusal, which the shared
                    // assertions then re-observe on their own closes.
                    try {
                        e.close()
                    } catch (_: Throwable) {
                    }
                },
            )
        }

    @Test
    fun `a refused post-stop destroy is quarantined, not retried`() =
        runTest {
            val (e, b) = newEngine()
            b.destroyResult = BackendOpResult.Bridge(BridgeFailure("destroy", STALE))
            markQuarantined(e)
            // The root must exist before the close: a close that wins the race
            // against construction is the separate cancellation window, and it
            // legitimately reaches neither stop nor destroy.
            assertTrue(
                b.awaitCondition("the worker publishes the root") { it.serviceThread != null },
                "the worker never reached the endpoint",
            )

            val first =
                try {
                    e.close()
                    null
                } catch (t: Throwable) {
                    t
                }
            assertIs<BridgeContractException>(
                first,
                "a refused destroy is a bridge-contract failure, not a stop refusal",
            )
            assertEquals("destroy", first.failure.op)
            assertEquals(STALE, first.failure.code)

            val observed = b.observed()
            assertEquals(1, observed.stopCount, "the stop that preceded it must have run exactly once")
            assertEquals(1, observed.destroyCount, "the refused destroy must NOT be retried")

            val second =
                try {
                    e.close()
                    null
                } catch (t: Throwable) {
                    t
                }
            assertTrue(second === first, "a later close must learn the same stable failure")
            assertEquals(
                1,
                b.observed().destroyCount,
                "a later close must make no further backend call",
            )
        }
}
