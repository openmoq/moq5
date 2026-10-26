package org.openmoq.moq5.engine

import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.openmoq.moq5.MoqResultCode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Ownership and waiter truthfulness after a REFUSED native stop
 * (chats/libmoq/1244 B, corrected by 1246 F1/F2).
 *
 * `moq_endpoint_stop()` returns `moq_result_t`, and for a valid endpoint it can
 * answer `MOQ_ERR_WRONG_STATE` -- live attachments
 * (`service/src/endpoint.c:1871`) or a call made from inside `on_pump`
 * (`:1877`). A refused stop changes nothing: the endpoint is still live and
 * still owned by the service tier.
 *
 * `EndpointBackend.stop()` returns that answer as a `BackendStopResult`, and on
 * a refusal the engine QUARANTINES the endpoint: it does not destroy, does not
 * retry, makes no later backend call, and resolves every close waiter --
 * already registered, late, or repeated -- by throwing the one stable
 * `StopRefusedException` it retained.
 *
 * Ownership and truthfulness are asserted independently, so a partial fix stays
 * visible. The scripted refusal is the BACKEND's own record of what the C tier
 * answered, which is what lets the test check the code independently of the
 * engine's handling of it.
 *
 * `close()` returns `Unit` but is a suspending function and CAN throw, which is
 * how a waiter learns that reclamation did not happen.
 *
 * `StopRefusedException` is INTERNAL by design: its mapping onto a public
 * failure hierarchy is a later API ruling. What is settled here, and asserted,
 * is that every waiter receives that same stable typed failure carrying the
 * exact native code -- never a normal return, never a hang, and never a fresh
 * or generic exception.
 */
internal class EndpointEngineStopRefusalTest : EngineFixture() {

    private companion object {
        /**
         * `MOQ_ERR_WRONG_STATE` (`core/include/moq/types.h:29`). Defined here as
         * the fixture's own value rather than as a second public constant table.
         */
        val WRONG_STATE = MoqResultCode(-5)
    }


    @Test
    fun `a refused native stop neither destroys the endpoint nor reports success to any waiter`() =
        runTest {
            val (e, b) = newEngine()
            val ledger = ledger()

            // Pin the service thread inside an accepted job so it cannot reach
            // retire() until both close waiters are provably registered.
            val hold = gate("refusal-hold")
            assertEquals(
                SubmitOutcome.ACCEPTED,
                e.post { ledger.started("first"); hold.await(); ledger.completed("first") },
            )
            assertTrue(
                ledger.await("the pinned job starts") { it.started.contains("first") },
                "the pinned job never started",
            )
            assertEquals(SubmitOutcome.ACCEPTED, e.post { ledger.completed("second") })

            // Both closers register before the service thread is released. The
            // boundary is "close() reached its first suspension", not "the
            // helper is about to call close()", so each is launched
            // UNDISPATCHED and opens its gate only after that suspension --
            // the established pattern, not a sleep or scheduler luck.
            val entered1 = gate("close-1-entered")
            val closer1 =
                callBlocking("refusal-close-1") {
                    coroutineScope {
                        val child = launch(start = CoroutineStart.UNDISPATCHED) { e.close() }
                        entered1.open()
                        child.join()
                    }
                }
            assertTrue(entered1.await(), "the first close never reached its first suspension")

            val entered2 = gate("close-2-entered")
            val closer2 =
                callBlocking("refusal-close-2") {
                    coroutineScope {
                        val child = launch(start = CoroutineStart.UNDISPATCHED) { e.close() }
                        entered2.open()
                        child.join()
                    }
                }
            assertTrue(entered2.await(), "the second close never reached its first suspension")

            // Both waiters are registered, so the deliberately quarantined
            // terminal path is now the expected outcome: cleanup must audit
            // this engine's thread and backend but must not demand a later
            // successful close() or retry native teardown. Armed here, not
            // earlier, so any failure before this point still gets ordinary
            // cleanup.
            markQuarantined(e)

            b.refuseStopWith(WRONG_STATE)
            hold.open()

            assertTrue(
                b.awaitCondition("the engine attempts stop") { it.stopCount == 1 },
                "the engine never attempted stop",
            )

            // Consume each caller exactly once: this test owns the verdict
            // about success-vs-failure, so cleanup must not judge it again by
            // its own default rule. A caller that never returned is still
            // reported by cleanup -- consumption cannot erase containment.
            val o1 = closer1.consume()
            val o2 = closer2.consume()
            val s = b.observed()

            // Every named oracle is collected, so one failing diagnostic cannot
            // hide the others: these are INDEPENDENT defects and a partial fix
            // must remain visible.
            val problems = mutableListOf<String>()
            fun check(ok: Boolean, message: String) { if (!ok) problems += message }

            // The scenario's precondition: the backend really did answer with a
            // refusal. Everything below is conditioned on it, so removing the
            // scripted refusal collapses this test to this one check rather
            // than flagging behaviour that would then be correct.
            check(s.stopCount == 1, "stop must be attempted exactly once (was ${s.stopCount})")
            check(
                s.stopRefusedCode == WRONG_STATE,
                "the backend must have witnessed the exact native refusal code " +
                    "(was ${s.stopRefusedCode})",
            )

            // Delivery, as ONE check per waiter that names exactly how that
            // waiter failed. After a witnessed refusal the only valid generic
            // outcome is a DELIVERED failure, and its exact type, code and
            // object identity ARE compared below. Only the mapping onto a
            // public failure hierarchy is a later ruling. A normal return and
            // a bounded non-return are different defects and are never
            // reported as each other.
            val seen = mutableListOf<StopRefusedException>()
            fun requireDelivered(which: String, o: CallerOutcome) {
                when (o) {
                    is CallerOutcome.NormalReturn -> {
                        problems +=
                            "the $which close waiter reported success although the " +
                                "native stop was refused"
                        return
                    }
                    is CallerOutcome.NotReturned -> {
                        problems += "the $which close waiter never returned"
                        return
                    }
                    is CallerOutcome.Threw -> Unit
                }
                // The throwable is EXACTLY StopRefusedException. The helper
                // runs the call under runBlocking and rethrows what it caught,
                // so nothing wraps it; a generic throwable here is a defect,
                // not a coroutine artefact.
                val err = (o as CallerOutcome.Threw).error
                if (err !is StopRefusedException) {
                    problems +=
                        "the $which close waiter failed with ${err::class.simpleName}, " +
                            "expected StopRefusedException"
                    return
                }
                if (err.code != WRONG_STATE) {
                    problems +=
                        "the $which close waiter received code ${err.code}, " +
                            "expected $WRONG_STATE"
                }
                seen += err
            }

            if (s.stopRefusedCode != null) {
                requireDelivered("first", o1)
                requireDelivered("second", o2)

                // A LATE close -- registered only after quarantine -- resolves
                // with the same stable failure and makes no backend call.
                val callsBeforeLate = s.callOrder
                val late = callBlocking("refusal-close-late") { e.close() }
                requireDelivered("late", late.consume())
                check(
                    b.observed().callOrder == callsBeforeLate,
                    "a late close must make no backend call (was ${b.observed().callOrder})",
                )

                // Repeats are stable: same failure, still no backend call.
                val again = callBlocking("refusal-close-again") { e.close() }
                requireDelivered("repeated", again.consume())
                check(
                    b.observed().callOrder == callsBeforeLate,
                    "a repeated close must make no backend call",
                )

                // ONE stable failure object, retained by the engine and handed
                // to every waiter. A fresh exception per waiter -- even one
                // carrying the right code -- is not a stable failure.
                check(
                    seen.size == 4,
                    "all four waiters must deliver a StopRefusedException (got ${seen.size})",
                )
                val engineFailure = e.quarantineFailure
                check(
                    engineFailure != null,
                    "the engine must retain the refusal it quarantined on",
                )
                for ((i, err) in seen.withIndex()) {
                    check(
                        err === engineFailure,
                        "waiter $i received a DIFFERENT exception object; the " +
                            "engine must hand every waiter the same stable failure",
                    )
                }

                // The service worker terminated.
                val worker = b.observed().serviceThread
                check(worker != null, "no service thread was ever observed")
                if (worker != null) {
                    worker.join(ScriptedEndpointBackend.CEILING_MILLIS)
                    check(
                        !worker.isAlive,
                        "the service worker must terminate after a refused stop",
                    )
                }

                // 3. Ownership: a refused stop leaves the endpoint live.
                check(
                    s.destroyCount == 0,
                    "a refused stop leaves the endpoint LIVE: destroy must not be called " +
                        "(destroyCount ${s.destroyCount})",
                )
                check(
                    s.callOrder == listOf("stop-refused"),
                    "no native call may follow a refused stop (was ${s.callOrder})",
                )
                check(
                    e.isBackendRetired,
                    "a quarantined engine must report that no backend call may follow",
                )
            }
            check(
                ledger.view().completed == listOf("first", "second"),
                "accepted jobs were lost or reordered around the stop attempt " +
                    "(was ${ledger.view().completed})",
            )
            check(
                e.post { ledger.completed("late") } == SubmitOutcome.REFUSED_SHUTDOWN,
                "a submission after shutdown must be refused, not published as a wake",
            )
            check(s.violations.isEmpty(), "backend violations: ${s.violations}")

            assertTrue(
                problems.isEmpty(),
                problems.joinToString(prefix = "\n  - ", separator = "\n  - "),
            )
        }
}
