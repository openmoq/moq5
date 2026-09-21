"""The sender's bounded wait: Sender.wait(timeout_us) -> WaitResult.

These rows were written RED, against a method that did not exist, and each
failed by a NAMED missing behaviour rather than skipping. The method exists
now and they pass. The gate is kept: a missing or inert name is still a named
failure, the substrate rows still drive the ACTUAL native mirror on their own,
the owned-child envelope still bounds every concurrent scenario, and the
controls still prove this file's own machinery discriminates.

Scope: ONE bounded wait. Not a completion barrier, not a drain, not a flush,
and not a promise about a later write. Waiting writes nothing, ends nothing,
clears no latch and destroys no owner.

The contract, re-derived from THIS worktree:

  service/include/moq/media_sender.h:660-683
      Block until the WRITE LEVEL holds, the endpoint wakes, or the timeout
      elapses. The level is ready AND the active send queue has headroom for
      at least one more object under the configured object AND byte bounds.
      Level-triggered: a level that already holds returns MOQ_OK immediately,
      before and after the underlying endpoint wait.
      MOQ_OK means "attempt (another) write now". It does NOT promise the next
      write succeeds: another thread may fill the queue, one object may exceed
      the byte budget, and a drop-policy sender still reports per-object
      outcomes. Results: OK, DONE (timeout, level not holding), INTERRUPTED
      (latch), CLOSED (terminal), INVAL (NULL).
      PRIORITY: the latch, then terminal, WIN over the level -- because
      write() refuses those same states, so OK would be a false "write now".

  service/src/media_sender.c:4823-4867, the branches as written
      1. NULL sender                          -> INVAL
      2. interrupt latch                      -> INTERRUPTED
      3. sender fatal OR endpoint closed      -> CLOSED
      4. the level holds                      -> OK, with no endpoint wait
      5. otherwise moq_endpoint_wait(ep, timeout_us):
         * its INTERRUPTED or DONE are returned AS IS
         * then the latch is rechecked        -> INTERRUPTED
         * then fatal/closed is rechecked     -> CLOSED
         * then the level is rechecked        -> OK
         * otherwise `rc < 0 ? rc : MOQ_OK`

Two consequences are pinned as such, because neither is obvious:

  * Step 5's last line means an endpoint WAKE returns OK even though the level
    does NOT hold. OK is therefore advisory: "look again", not "there is
    demand" and not "your write will be accepted".
  * A negative endpoint result propagates only in the FINAL branch, and only
    once the latch, terminal and level rechecks have all declined: those three
    still WIN over it. When it does reach the caller it keeps its signed code,
    so a code this binding has never seen can arrive here.

This is NOT receiver wait. The receiver's polls stay legal while latched and
keep draining queued items after terminal; the sender's latch and terminal
WIN over its level. Nothing here copies the receiver's
queued-data-before-terminal ordering.

Proposed surface: `Sender.wait(timeout_us: int) -> WaitResult`, reusing the
EXISTING public timeout domain and the SHARED slicer (`_endpoint._sliced_wait`)
that Endpoint.wait and Receiver.wait already use -- one monotonic deadline,
bounded native slices, the remainder rounded up, an exhausted budget still
making exactly one observation. No sender-specific clock, unit or second
slicer. WOKEN/TIMED_OUT/INTERRUPTED/CLOSED map to the existing WaitResult;
any other signed native code becomes MoqError with operation "wait" and must
never escape as an enum ValueError.
"""

import contextlib
import gc
import io
import os
import select
import sys
import threading
import time
import unittest
from unittest import mock

import moq5
from moq5 import _endpoint, _native

import test_foundation as foundation
from test_sender import reap_child

NS = (b"svc", b"demo")

OK = 0
DONE = 1
INTERRUPTED = -13
CLOSED = -4
INVAL = -2
UNKNOWN_CODE = -31415

WAIT_SLICE_US = foundation.WAIT_SLICE_US
INT64_MAX = (1 << 63) - 1

BRIDGE = ("sender_wait",)


def assert_routed_slice(case, expected_slice):
    """The routing fact, in ONE place: exactly one native call, carrying the
    DECLARED remaining slice, on this sender. Both routing rows use this, and
    so does the control that perturbs the delivered value."""
    counts = _native._test_sender_wait_counts()
    case.assertEqual(counts["calls"], 1,
                     "the sender's own wait did not run exactly once")
    case.assertEqual(counts["last_timeout_us"], expected_slice,
                     "the declared remaining slice was not the one that "
                     "reached native")
    case.assertEqual(counts["last_sender"], 0,
                     "another sender was waited on")


def missing_surface() -> list[str]:
    return [] if callable(getattr(moq5.Sender, "wait", None)) else ["wait"]


class WaitFixtureBase(unittest.TestCase):
    def setUp(self):
        gc.collect()
        self.protocols = []
        _native._test_reset()
        _native._test_sender_reset()
        _native._test_send_track_reset()
        _native._test_sender_wait_reset()

    def protocol(self, act):
        """Every protocol this test creates is registered, so teardown can
        refuse to reset shared fixture state under a surviving worker."""
        created = WaitBlockingProtocol(self, act)
        self.protocols.append(created)
        return created

    def close_if_settled(self, protocol, sender=None, endpoint=None):
        """The ownership rule, in ONE place.

        A native owner is destroyed only when the worker that may still be
        using it has settled. When it has not, nothing is closed and the
        harness failure is raised instead -- a surviving worker must never
        race a destroyed owner.
        """
        if protocol is not None and not protocol.settled:
            raise HarnessFailure(
                "a worker did not settle: refusing to destroy its owner")
        try:
            if sender is not None:
                sender.close()
        finally:
            if endpoint is not None:
                endpoint.close()

    def tearDown(self):
        unsettled = [p for p in getattr(self, "protocols", [])
                     if p.worker is not None and not p.settled]
        if unsettled:
            # do NOT reset shared fixture state under a live worker
            raise HarnessFailure(
                f"{len(unsettled)} worker(s) never settled: the shared "
                "fixture is not reset while they may still be running")
        _native._test_sender_wait_reset()
        _native._test_wait_result(DONE)

    def connect(self):
        return moq5.Endpoint.connect(moq5.EndpointConfig(url="moqt://fixture.invalid"))

    def attach(self, endpoint):
        return moq5.Sender.attach(
            endpoint,
            moq5.SenderConfig(namespace=NS,
                              backpressure=moq5.Backpressure.DROP_GROUP))


# ---------------------------------------------------------------- substrate --

class WaitSubstrateTests(WaitFixtureBase):
    """The native mirror, driven directly, without the bridge.

    These execute the SOURCE's priority order against the fixture. They are
    evidence about the mirror's faithfulness to the branches quoted above,
    NOT about the real service's queue.
    """

    def test_a_a_holding_level_returns_ok_without_an_endpoint_wait(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                _native._test_wait_result(DONE)      # the endpoint would time out
                _native._test_sender_wait_level(True)
                self.assertEqual(_native._test_sender_wait_simulate(5_000), OK)
                counts = _native._test_sender_wait_counts()
                self.assertEqual(counts["calls"], 1)
                self.assertEqual(counts["last_timeout_us"], 5_000,
                                 "the timeout was not passed through")
            finally:
                sender.close()

    def test_b_the_latch_and_terminal_win_over_the_level(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                _native._test_sender_wait_level(True)     # the level HOLDS
                endpoint.set_interrupted(True)
                try:
                    self.assertEqual(
                        _native._test_sender_wait_simulate(0), INTERRUPTED,
                        "the level beat the interrupt latch")
                finally:
                    endpoint.set_interrupted(False)
                _native._test_sender_state(True, True, 7)     # fatal
                self.assertEqual(_native._test_sender_wait_simulate(0), CLOSED,
                                 "the level beat terminal state")
                _native._test_sender_state(True, False, 0)
                self.assertEqual(_native._test_sender_wait_simulate(0), OK)
            finally:
                sender.close()

    def test_c_the_endpoint_outcomes_pass_through(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                _native._test_sender_wait_level(False)
                for scripted, expected, why in (
                        (DONE, DONE, "a timeout became something else"),
                        (OK, OK, "an endpoint wake did not report OK"),
                        (UNKNOWN_CODE, UNKNOWN_CODE,
                         "a negative endpoint result was normalised")):
                    with self.subTest(scripted=scripted):
                        _native._test_wait_result(scripted)
                        self.assertEqual(
                            _native._test_sender_wait_simulate(1_000),
                            expected, why)
            finally:
                _native._test_wait_result(DONE)
                sender.close()

    def test_d_an_endpoint_wake_returns_ok_without_the_level(self):
        # The source's last line: a non-negative endpoint result becomes OK
        # even though the level does not hold. OK is advisory.
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                _native._test_sender_wait_level(False)
                _native._test_wait_result(OK)            # the endpoint WOKE
                self.assertEqual(_native._test_sender_wait_simulate(1_000), OK)
            finally:
                _native._test_wait_result(DONE)
                sender.close()

    def test_e_a_null_owner_is_inval(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                self.assertEqual(_native._test_sender_wait_simulate(0, 1), INVAL)
                self.assertEqual(
                    _native._test_sender_wait_counts()["last_sender"], -1)
            finally:
                sender.close()

    def test_f_the_level_can_start_holding_at_a_later_call(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                _native._test_wait_result(DONE)
                _native._test_sender_wait_level(False, 3)   # holds from call 3
                self.assertEqual(_native._test_sender_wait_simulate(10), DONE)
                self.assertEqual(_native._test_sender_wait_simulate(10), DONE)
                self.assertEqual(_native._test_sender_wait_simulate(10), OK,
                                 "the level flip was not observed")
                counts = _native._test_sender_wait_counts()
                self.assertEqual(counts["calls"], 3)
                self.assertEqual(counts["total_timeout_us"], 30)
            finally:
                sender.close()

    def test_g_reset_clears_the_script_and_the_inventory(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                before_resets = _native._test_sender_wait_counts()["resets"]
                _native._test_sender_wait_level(True)
                _native._test_sender_wait_simulate(1)
                self.assertGreater(
                    _native._test_sender_wait_counts()["calls"], 0)
                _native._test_sender_wait_reset()
                counts = _native._test_sender_wait_counts()
                self.assertEqual(
                    {name: counts[name] for name in
                     ("calls", "last_timeout_us", "total_timeout_us",
                      "last_sender")},
                    {"calls": 0, "last_timeout_us": 0,
                     "total_timeout_us": 0, "last_sender": -1})
                self.assertGreater(counts["resets"], before_resets,
                                   "the reset entry was not recorded")
            finally:
                sender.close()


# ------------------------------------------------------------- behavioural --

class WaitEnvelopeControlTests(unittest.TestCase):
    """The owned-child ENVELOPE itself. Never evidence about the product.

    Each row drives the real `in_owned_child` with a deliberately malformed
    child and requires the parent to refuse it. The diagnostics these induce
    are captured by the envelope under test, so they are controlled negative
    evidence rather than stray output in this run.
    """

    def refuses(self, body, expected_text):
        with self.assertRaises(AssertionError) as raised:
            in_owned_child(self, body)
        message = str(raised.exception)
        del raised
        self.assertIn(expected_text, message,
                      f"the envelope refused for the wrong reason: {message}")

    def test_a_a_quiet_valid_completion_is_accepted(self):
        self.assertEqual(in_owned_child(self, lambda: 0), 0)
        self.assertEqual(in_owned_child(self, lambda: 13), 13,
                         "a declared failure verdict was not returned")

    def test_b_a_missing_verdict_is_refused(self):
        self.refuses(lambda: None, "must be a non-bool int")

    def test_c_a_wrapping_status_is_refused(self):
        # 256 would truncate to 0 in a POSIX exit status
        self.refuses(lambda: 256, "outside the declared domain")
        self.refuses(lambda: 512, "outside the declared domain")

    def test_d_an_out_of_domain_or_boolean_verdict_is_refused(self):
        self.refuses(lambda: 7, "outside the declared domain")
        self.refuses(lambda: True, "must be a non-bool int")

    def test_e_a_premature_exit_zero_is_refused(self):
        def premature():
            os._exit(0)              # before any verdict is written
        self.refuses(premature, "exactly one verdict")

    def test_f_a_diagnostic_at_exit_zero_is_refused(self):
        def noisy():
            os.write(2, b"an induced diagnostic\n")
            return 0
        self.refuses(noisy, "unplanned diagnostic")

    def test_g_a_second_receipt_line_is_refused(self):
        # what a post-decision finalizer would produce
        def doubled():
            child_receipt(0)
            return 0
        self.refuses(doubled, "exactly one verdict")

    def test_g2_a_buffered_python_diagnostic_is_refused(self):
        def buffered():
            print("an induced buffered diagnostic")
            return 0
        self.refuses(buffered, "unplanned diagnostic")

    def test_g3_a_non_integer_verdict_is_refused_at_the_source(self):
        self.refuses(lambda: "0", "refused at the source")
        self.refuses(lambda: 3.0, "refused at the source")

    def test_g4_a_parent_collection_failure_still_reaps_its_child(self):
        """An injected collection error must not outlive its child.

        The child's pid is captured here, the failure is injected into the
        parent's own read, and the child must already be reaped when the
        helper raises -- an outer cleanup would be a safety net, not proof.
        """
        pids = []
        real_fork = os.fork

        def recording_fork():
            pid = real_fork()
            if pid:
                pids.append(pid)
            return pid

        def sleeper():
            # shorter than the settlement bound, so the child is REAPED
            # rather than killed: this row is about reaping, not killing
            time.sleep(1.0)
            return 0

        with mock.patch.object(os, "fork", recording_fork), \
                mock.patch.object(select, "select",
                                  side_effect=OSError("injected")):
            with self.assertRaises(OSError):
                in_owned_child(self, sleeper, timeout=3.0)
        self.assertEqual(len(pids), 1, "the probe did not observe one child")
        with self.assertRaises(ChildProcessError,
                               msg="the child was still reapable: it was not "
                                   "settled by the helper"):
            os.waitpid(pids[0], os.WNOHANG)

    def test_g5_a_valid_receipt_cannot_rescue_a_failed_child(self):
        """A receipt is not an envelope completion.

        The child writes a well-formed verdict and THEN exits nonzero. The
        parent must refuse by the exit-status diagnostic, and must already
        have reaped that child.
        """
        pids = []
        real_fork = os.fork

        def recording_fork():
            pid = real_fork()
            if pid:
                pids.append(pid)
            return pid

        def receipt_then_fail():
            child_receipt(0)
            os._exit(23)

        with mock.patch.object(os, "fork", recording_fork):
            self.refuses(receipt_then_fail, "did not complete its envelope")
        self.assertEqual(len(pids), 1)
        with self.assertRaises(ChildProcessError,
                               msg="the refused child was not reaped"):
            os.waitpid(pids[0], os.WNOHANG)

    def test_g6_a_killed_child_is_refused_even_with_a_receipt(self):
        def receipt_then_signal():
            child_receipt(0)
            os.kill(os.getpid(), 9)
        self.refuses(receipt_then_signal, "did not complete its envelope")

    def test_g7_both_failures_survive_when_collection_and_cleanup_fail(self):
        """The collection failure is never replaced, and the cleanup failure
        is never lost. The child is really reaped before the injected
        post-reap error is raised."""
        real_reap = reap_child

        def reap_then_raise(pid, timeout=10.0):
            real_reap(pid, timeout)          # genuinely settle it first
            raise RuntimeError("induced cleanup failure AFTER real reap")

        with mock.patch.object(select, "select",
                               side_effect=OSError("primary collection")), \
                mock.patch.object(sys.modules[__name__], "reap_child",
                                  reap_then_raise):
            with self.assertRaises(BaseExceptionGroup) as raised:
                in_owned_child(self, lambda: 0, timeout=2.0)
            kinds = [type(error).__name__ for error in
                     raised.exception.exceptions]
            del raised
        self.assertEqual(kinds, ["OSError", "RuntimeError"],
                         f"both failures did not survive together: {kinds}")

    def test_g8_each_failure_alone_is_reported_as_itself(self):
        real_reap = reap_child

        def reap_then_raise(pid, timeout=10.0):
            real_reap(pid, timeout)
            raise RuntimeError("induced cleanup failure AFTER real reap")

        # cleanup alone
        with mock.patch.object(sys.modules[__name__], "reap_child",
                               reap_then_raise):
            with self.assertRaises(RuntimeError):
                in_owned_child(self, lambda: 0, timeout=2.0)
        # collection alone
        with mock.patch.object(select, "select",
                               side_effect=OSError("primary collection")):
            with self.assertRaises(OSError):
                in_owned_child(self, lambda: 0, timeout=2.0)

    def test_h_a_child_that_never_returns_is_killed_and_refused(self):
        def stuck():
            time.sleep(30.0)
            return 0
        with self.assertRaises(AssertionError) as raised:
            in_owned_child(self, stuck, timeout=1.0)
        message = str(raised.exception)
        del raised
        self.assertTrue("had to be killed" in message
                        or "exactly one verdict" in message,
                        f"a stuck child was not refused: {message}")


class WaitGatedBase(WaitFixtureBase):
    """Never skips. A missing or inert surface is a NAMED failure, asserted
    before the row acquires an endpoint."""

    def require_surface(self):
        # Kept after GREEN: an inert or removed name must still fail by name.
        missing = missing_surface()
        if missing:
            self.fail("the sender wait surface is not implemented: "
                      f"moq5.Sender is missing {', '.join(missing)}")

    def scripted(self, results, **kwargs):
        """The SHARED scripted clock and native wait the accepted foundation
        rows use, aimed at the sender's own bridge."""
        script = foundation.ScriptedWait(results, **kwargs)
        return script, (
            mock.patch.object(_endpoint, "_monotonic_ns", script.clock),
            mock.patch.object(_native, "sender_wait", script.native_wait,
                              create=True),
        )


class WaitResultTests(WaitGatedBase):
    def test_a_every_agreed_code_maps_to_its_outcome(self):
        self.require_surface()
        for code, expected in ((OK, moq5.WaitResult.WOKEN),
                               (DONE, moq5.WaitResult.TIMED_OUT),
                               (INTERRUPTED, moq5.WaitResult.INTERRUPTED),
                               (CLOSED, moq5.WaitResult.CLOSED)):
            with self.subTest(code=code):
                script, patches = self.scripted([code])
                with self.connect() as endpoint:
                    sender = self.attach(endpoint)
                    try:
                        with patches[0], patches[1]:
                            self.assertIs(sender.wait(0), expected,
                                          "the exact outcome was not returned")
                        self.assertEqual(len(script.slices), 1,
                                         "one native observation per call")
                    finally:
                        sender.close()

    def test_b_an_unknown_signed_code_is_a_moq_error_not_a_value_error(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                _native._test_sender_wait_level(False)
                _native._test_wait_result(UNKNOWN_CODE)
                try:
                    with self.assertRaises(moq5.MoqError) as raised:
                        sender.wait(1_000)
                    code, operation = (raised.exception.code,
                                       raised.exception.operation)
                    del raised
                finally:
                    _native._test_wait_result(DONE)
                self.assertEqual(code, UNKNOWN_CODE,
                                 "the signed code was normalised")
                self.assertEqual(operation, "wait",
                                 "the operation was mislabelled")
            finally:
                sender.close()

    def test_c_closed_is_a_result_on_a_live_sender(self):
        self.require_surface()
        # A terminal sender is not a destroyed Python owner: the first is a
        # RESULT, the second is refused before the service is entered.
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            _native._test_sender_state(True, True, 9)        # live but fatal
            self.assertIs(sender.wait(0), moq5.WaitResult.CLOSED)
            self.assertIs(sender.closed, False,
                          "a CLOSED result closed the Python owner")
            sender.close()
            before = _native._test_sender_wait_counts()
            with self.assertRaises(RuntimeError):
                sender.wait(0)
            self.assertEqual(_native._test_sender_wait_counts()["calls"],
                             before["calls"],
                             "a destroyed owner entered the service")
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()

    def test_d_no_outcome_becomes_eof_or_completion(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                script, patches = self.scripted([CLOSED])
                with patches[0], patches[1]:
                    outcome = sender.wait(0)
                self.assertIs(outcome, moq5.WaitResult.CLOSED)
                self.assertNotEqual(outcome, moq5.WaitResult.TIMED_OUT,
                                    "CLOSED was reported as a timeout")
                # and the sender is not ended, drained or completed by it
                self.assertIs(sender.closed, False)
                self.assertNotIn("end_track", _native._test_sender_log())
            finally:
                sender.close()


class WaitBudgetTests(WaitGatedBase):
    """The SHARED slicer's contract, observed through the sender's method."""

    def test_a_one_budget_is_sliced_and_never_reset(self):
        self.require_surface()
        # A one-second budget where each native call consumes its whole slice:
        # four full slices, and the budget is never restarted. This is the
        # SHARED slicer's established shape, observed through the sender.
        script, patches = self.scripted([DONE] * 4,
                                        advance_ns_per_call=250_000_000)
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                with patches[0], patches[1]:
                    self.assertIs(sender.wait(WAIT_SLICE_US * 4),
                                  moq5.WaitResult.TIMED_OUT)
                self.assertEqual(script.slices, [WAIT_SLICE_US] * 4,
                                 "the budget was not sliced to the bound")
                self.assertEqual(sum(script.slices), WAIT_SLICE_US * 4,
                                 "the total exceeded one budget")
            finally:
                sender.close()

    def test_b_elapsed_time_between_slices_comes_out_of_the_budget(self):
        self.require_surface()
        # Each native call takes 300 ms of wall time against a one-second
        # budget: the remainder shrinks by what actually elapsed, and the last
        # slice is the exact remainder.
        script, patches = self.scripted([DONE] * 4,
                                        advance_ns_per_call=300_000_000)
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                with patches[0], patches[1]:
                    self.assertIs(sender.wait(1_000_000),
                                  moq5.WaitResult.TIMED_OUT)
                self.assertEqual(len(script.slices), 4,
                                 "the budget was not spent in bounded slices")
                self.assertEqual(script.slices,
                                 [250_000, 250_000, 250_000, 100_000],
                                 "elapsed overhead did not reduce the "
                                 "remainder, or the remainder was wrong")
            finally:
                sender.close()

    def test_b2_a_sub_microsecond_remainder_rounds_up(self):
        self.require_surface()
        # 1.5 us left must become a 2 us native wait, never 1: a truncated
        # slice would return before the caller's deadline.
        script, patches = self.scripted([DONE, DONE],
                                        advance_ns_per_call=250_000_500)
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                with patches[0], patches[1]:
                    self.assertIs(sender.wait(250_002),
                                  moq5.WaitResult.TIMED_OUT)
                self.assertEqual(script.slices, [250_000, 2],
                                 "the remainder was not rounded up")
            finally:
                sender.close()

    def test_c_the_whole_budget_is_never_one_native_call(self):
        self.require_surface()
        script, patches = self.scripted([DONE] * 4,
                                        advance_ns_per_call=250_000_000)
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                with patches[0], patches[1]:
                    sender.wait(WAIT_SLICE_US * 4)
                self.assertGreater(len(script.slices), 1,
                                   "the whole budget went to one native call")
                self.assertLessEqual(max(script.slices), WAIT_SLICE_US,
                                     "a slice exceeded the bound")
            finally:
                sender.close()

    def test_d_every_non_timeout_outcome_returns_from_its_slice(self):
        self.require_surface()
        for code, expected in ((OK, moq5.WaitResult.WOKEN),
                               (INTERRUPTED, moq5.WaitResult.INTERRUPTED),
                               (CLOSED, moq5.WaitResult.CLOSED)):
            with self.subTest(code=code):
                script, patches = self.scripted([DONE, code, DONE, DONE],
                                                advance_ns_per_call=250_000_000)
                with self.connect() as endpoint:
                    sender = self.attach(endpoint)
                    try:
                        with patches[0], patches[1]:
                            self.assertIs(sender.wait(WAIT_SLICE_US * 4),
                                          expected)
                        self.assertEqual(len(script.slices), 2,
                                         "the wait continued past a decided "
                                         "outcome")
                    finally:
                        sender.close()

    def test_e_a_zero_budget_still_makes_one_observation(self):
        self.require_surface()
        script, patches = self.scripted([DONE])
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                with patches[0], patches[1]:
                    self.assertIs(sender.wait(0), moq5.WaitResult.TIMED_OUT)
                self.assertEqual(script.slices, [0],
                                 "a zero budget did not make exactly one "
                                 "zero-timeout observation")
            finally:
                sender.close()

    def test_f_an_exhausted_budget_still_observes_its_outcome(self):
        self.require_surface()
        # An already-spent budget must not fabricate a timeout: the one
        # guaranteed observation still decides.
        for code, expected in ((OK, moq5.WaitResult.WOKEN),
                               (INTERRUPTED, moq5.WaitResult.INTERRUPTED),
                               (CLOSED, moq5.WaitResult.CLOSED)):
            with self.subTest(code=code):
                script, patches = self.scripted([code],
                                                advance_ns_per_read=10_000_000)
                with self.connect() as endpoint:
                    sender = self.attach(endpoint)
                    try:
                        with patches[0], patches[1]:
                            self.assertIs(sender.wait(1_000), expected)
                        self.assertEqual(len(script.slices), 1)
                    finally:
                        sender.close()

    def test_g_a_native_failure_is_not_retried(self):
        self.require_surface()
        error = _native.Error(UNKNOWN_CODE, "wait", "scripted")
        script, patches = self.scripted([error, DONE, DONE, DONE],
                                        advance_ns_per_call=250_000_000)
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                with patches[0], patches[1]:
                    with self.assertRaises(moq5.MoqError):
                        sender.wait(WAIT_SLICE_US * 3)
                self.assertEqual(len(script.slices), 1,
                                 "the wait was retried after a failure")
            finally:
                sender.close()

    def test_h_a_pending_python_signal_is_not_retried(self):
        self.require_surface()
        script, patches = self.scripted([KeyboardInterrupt(), DONE, DONE],
                                        advance_ns_per_call=250_000_000)
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                with patches[0], patches[1]:
                    with self.assertRaises(KeyboardInterrupt):
                        sender.wait(WAIT_SLICE_US * 3)
                self.assertEqual(len(script.slices), 1,
                                 "the wait was retried after an interrupt")
            finally:
                sender.close()

    def test_i_the_shared_slicer_is_the_one_used(self):
        self.require_surface()
        # Not a second implementation: the sender's wait must go through the
        # SAME slicer Endpoint.wait and Receiver.wait use.
        calls = []
        real = _endpoint._sliced_wait

        def spy(native_wait, handle, timeout_us):
            calls.append((native_wait, timeout_us))
            return real(native_wait, handle, timeout_us)

        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                _native._test_sender_wait_level(True)
                with mock.patch.object(_endpoint, "_sliced_wait", spy):
                    with mock.patch.object(moq5._sender, "_sliced_wait", spy,
                                           create=True):
                        sender.wait(1_000)
                self.assertEqual(len(calls), 1,
                                 "the sender did not use the shared slicer")
                self.assertIs(calls[0][0], _native.sender_wait,
                              "the slicer was aimed at the wrong native wait")
                self.assertEqual(calls[0][1], 1_000)
            finally:
                sender.close()


class WaitTimeoutDomainTests(WaitGatedBase):
    def test_a_the_public_domain_is_the_existing_one(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                _native._test_sender_wait_level(True)
                before = _native._test_sender_wait_counts()["calls"]
                for bad in (-1, INT64_MAX + 1, 1 << 64):
                    with self.subTest(bad=bad):
                        with self.assertRaises(ValueError):
                            sender.wait(bad)
                for bad in (True, False, 1.0, "1", None):
                    with self.subTest(bad=repr(bad)):
                        with self.assertRaises(TypeError):
                            sender.wait(bad)
                self.assertEqual(_native._test_sender_wait_counts()["calls"],
                                 before,
                                 "a refused timeout entered the service")
                # the boundaries themselves are accepted, without waiting:
                # the level holds, so the first observation returns at once
                self.assertIs(sender.wait(0), moq5.WaitResult.WOKEN)
                self.assertIs(sender.wait(INT64_MAX), moq5.WaitResult.WOKEN)
            finally:
                sender.close()

    def test_a2_the_private_bridge_passes_an_unsliced_timeout_unchanged(self):
        """The bridge's OWN contract, called directly.

        The public method slices, which is a different fact proved by the
        routing rows. What this row owns is that the bridge passes the
        timeout it is GIVEN straight to the service: no clamp to the public
        slice cap, no narrowing, no rounding. The write level is already true,
        so each call returns at once and nothing depends on elapsed time.
        """
        self.require_surface()
        missing = tuple(n for n in BRIDGE if not hasattr(_native, n))
        self.assertEqual(missing, (),
                         "the wait bridge is not implemented: _native is "
                         f"missing {', '.join(missing)}")
        endpoint = self.connect()
        sender = self.attach(endpoint)
        self.addCleanup(endpoint.close)
        self.addCleanup(sender.close)
        handle = getattr(sender, "_Sender__handle")
        for name, timeout in (("zero", 0),
                              ("one microsecond", 1),
                              ("just above the public slice cap",
                               WAIT_SLICE_US + 1),
                              ("above 32 bits", (1 << 32) + 17),
                              ("the domain maximum", INT64_MAX)):
            with self.subTest(case=name, timeout=timeout):
                # per-case evidence: nothing carried over from the last one
                _native._test_sender_wait_reset()
                _native._test_sender_wait_level(True)
                self.assertEqual(_native.sender_wait(handle, timeout), OK,
                                 "the bridge did not report the holding level")
                counts = _native._test_sender_wait_counts()
                self.assertEqual(counts["calls"], 1,
                                 "the bridge did not make exactly one call")
                self.assertEqual(counts["last_timeout_us"], timeout,
                                 "the bridge altered the timeout it was given")
                self.assertEqual(counts["last_sender"], 0,
                                 "another sender was waited on")

    def test_b_the_private_bridge_validates_its_own_timeout(self):
        self.require_surface()
        missing = tuple(n for n in BRIDGE if not hasattr(_native, n))
        self.assertEqual(missing, (),
                         "the wait bridge is not implemented: _native is "
                         f"missing {', '.join(missing)}")
        endpoint = self.connect()
        sender = self.attach(endpoint)
        self.addCleanup(endpoint.close)
        self.addCleanup(sender.close)
        handle = getattr(sender, "_Sender__handle")
        before = _native._test_sender_wait_counts()["calls"]
        for bad in (-1, INT64_MAX + 1, True, 1.0, "1", None):
            with self.subTest(bad=repr(bad)):
                with self.assertRaises((TypeError, ValueError)):
                    _native.sender_wait(handle, bad)
        self.assertEqual(_native._test_sender_wait_counts()["calls"], before,
                         "a refused timeout entered the service")


class WaitGuardTests(WaitGatedBase):
    def test_a_wrong_capsules_and_arities_are_refused(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        self.addCleanup(endpoint.close)
        self.addCleanup(sender.close)
        endpoint_handle = getattr(endpoint, "_Endpoint__handle")
        before = _native._test_sender_wait_counts()["calls"]
        with self.assertRaises((ValueError, TypeError)):
            _native.sender_wait(endpoint_handle, 0)
        with self.assertRaises(TypeError):
            _native.sender_wait(getattr(sender, "_Sender__handle"))
        with self.assertRaises(TypeError):
            _native.sender_wait()
        self.assertEqual(_native._test_sender_wait_counts()["calls"], before,
                         "a wrong capsule reached the service")

    def test_b_the_owner_thread_and_pid_guards_precede_service_entry(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            _native._test_sender_wait_level(True)
            before = _native._test_sender_wait_counts()["calls"]
            refused = []

            def other():
                try:
                    sender.wait(0)
                except RuntimeError as error:
                    refused.append(error)

            worker = threading.Thread(target=other, daemon=True)
            worker.start()
            worker.join(5.0)
            self.assertFalse(worker.is_alive(), "the guard worker never returned")
            self.assertEqual(len(refused), 1, "a foreign thread was admitted")
            self.assertEqual(_native._test_sender_wait_counts()["calls"],
                             before, "a foreign thread entered the service")

            if hasattr(os, "fork"):
                child = os.fork()
                if child == 0:
                    status = 0
                    try:
                        sender.wait(0)
                        status = 3              # the inherited handle worked
                    except RuntimeError:
                        pass
                    except BaseException:
                        status = 9
                    if (_native._test_sender_wait_counts()["calls"] != before):
                        status = 4
                    os._exit(status)
                self.assertEqual(reap_child(child), 0,
                                 "the fork guard child did not exit cleanly")
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()

    def test_c_the_exact_sender_and_declared_slice_reach_native(self):
        self.require_surface()
        # The slicer legitimately subtracts elapsed time before its FIRST
        # native call, so a real clock makes "the same number goes down"
        # false by a microsecond or two. The row therefore declares the clock
        # and asserts the DECLARED remaining slice.
        #
        # This is the PUBLIC sliced routing contract. That the BRIDGE passes
        # an unsliced timeout through unchanged is a different fact, owned by
        # WaitTimeoutDomainTests.
        # test_a2_the_private_bridge_passes_an_unsliced_timeout_unchanged.
        elapsed_ns = 2_000                       # declared overhead
        budget_us = 4_321
        # the slicer's own arithmetic: the REMAINDER in nanoseconds, rounded
        # UP to microseconds, never the untouched budget
        expected_slice = -(-(budget_us * 1_000 - elapsed_ns) // 1_000)
        clock = mock.Mock(side_effect=[0, elapsed_ns, elapsed_ns])
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                _native._test_sender_wait_reset()
                _native._test_sender_wait_level(True)
                with mock.patch.object(_endpoint, "_monotonic_ns", clock):
                    sender.wait(budget_us)
                assert_routed_slice(self, expected_slice)
            finally:
                sender.close()

    def test_c2_legal_elapsed_overhead_is_not_a_defect(self):
        self.require_surface()
        # A permanent positive control for the row above: overhead between
        # the deadline and the first native call is LEGAL, and the row must
        # accept it rather than demanding the untouched budget.
        for elapsed_ns in (0, 1, 999, 1_000, 2_000, 250_000):
            with self.subTest(elapsed_ns=elapsed_ns):
                budget_us = 4_321
                expected = -(-(budget_us * 1_000 - elapsed_ns) // 1_000)
                clock = mock.Mock(side_effect=[0, elapsed_ns, elapsed_ns])
                with self.connect() as endpoint:
                    sender = self.attach(endpoint)
                    try:
                        _native._test_sender_wait_reset()
                        _native._test_sender_wait_level(True)
                        with mock.patch.object(_endpoint, "_monotonic_ns",
                                               clock):
                            sender.wait(budget_us)
                        assert_routed_slice(self, expected)
                    finally:
                        sender.close()

    def test_d_waiting_has_no_lifecycle_effect(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            track = sender.add_track(moq5.SendTrackConfig(
                name=b"v", media_type=moq5.MediaType.VIDEO,
                packaging=moq5.Packaging.RAW, codec=b"av01", bitrate=1))
            _native._test_sender_wait_level(True)
            endpoint.set_interrupted(True)
            try:
                self.assertIs(sender.wait(0), moq5.WaitResult.INTERRUPTED)
                # the latch is RETAINED: waiting never clears it
                self.assertIs(sender.wait(0), moq5.WaitResult.INTERRUPTED,
                              "the wait cleared the interrupt latch")
            finally:
                endpoint.set_interrupted(False)
            operations = tuple(op for op in _native._test_sender_log()
                               if op in ("write", "end_track", "remove_track",
                                         "sender_destroy"))
            self.assertEqual(operations, (),
                             "a wait performed a lifecycle operation")
            self.assertIs(sender.closed, False)
            self.assertIs(endpoint.closed, False)
            self.assertIs(track.removed, False)
            self.assertIs(sender.wait(0), moq5.WaitResult.WOKEN,
                          "the owner was left unusable")
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()


CHILD_MEANING = {
    10: "the worker never settled",
    11: "the worker itself failed",
    12: "a bounded fixture barrier expired: harness failure",
    13: "the action did not happen inside the held call",
    14: "the declared signal never arrived",
    15: "the wait did not run exactly once",
    16: "the outcome did not follow the action performed inside the call",
    17: "an owner was destroyed or the fixture reset under a live worker",
    18: "the primary failure was lost",
    29: "an unexpected exception escaped the child",
}


CHILD_STATUSES = frozenset({0}) | frozenset(CHILD_MEANING)

_RECEIPT_FD = None            # the child's private receipt pipe, while forked
_CHILD_BASELINE = None        # the fingerprint when this child started


def child_observations():
    """A fingerprint of EXACTLY three observed operations.

    Sender destroy entries (a close or a destroy moves the first), sender
    attach entries (the second), and wait-fixture reset ENTRIES (the third --
    counted where the reset begins, so an attempted reset is visible even if
    the rest of that call were refused). It is not a general audit: it is a
    record of those three operations, written into the receipt so the parent
    sees what happened in the decision-to-exit interval instead of inferring
    it from some other object's lifetime.
    """
    counts = _native._test_sender_counts()
    observed = (counts["destroy_entries"], counts["attach_entries"],
                _native._test_sender_wait_counts()["resets"])
    if _CHILD_BASELINE is None:
        return observed
    # RELATIVE to this child's own start: the reset counter is cumulative for
    # the whole process, so only the child's own deltas are meaningful.
    return tuple(now - then for now, then in zip(observed, _CHILD_BASELINE))


def child_receipt(status):
    """Write the child's ONE verdict line, with its observations.

    The verdict is validated HERE, by the named type rule, before anything is
    serialised: a string, a bool or an out-of-domain value never becomes a
    well-formed receipt.
    """
    if _RECEIPT_FD is None:
        raise HarnessFailure("no receipt pipe in this process")
    if isinstance(status, bool) or not isinstance(status, int):
        raise HarnessFailure(
            f"the child verdict must be a non-bool int, not {type(status).__name__}")
    if status not in CHILD_STATUSES:
        raise HarnessFailure(
            f"the child verdict {status} is outside the declared domain")
    destroys, attaches, resets = child_observations()
    os.write(_RECEIPT_FD,
             f"verdict {status} {destroys} {attaches} {resets}\n".encode())


def child_fail_now(status):
    """Report a verdict and leave IMMEDIATELY.

    Used where continuing is unsafe -- an unsettled worker, above all. The
    observations are taken HERE, at the decision, and the process exits before
    any frame unwinds: no close, no capsule destructor, no fixture reset and
    no interpreter finalization can run afterwards.
    """
    try:
        child_receipt(status)
    finally:
        os._exit(0)


def in_owned_child(case, body, timeout=20.0):
    """Run one concurrency scenario in an OWNED child, against a RECEIPT.

    Everything the scenario owns -- endpoint, sender and every worker thread
    -- is created INSIDE the child, so nothing it starts can outlive it in the
    shared test runner and no failure path of its own can reset this process's
    fixture or destroy an owner here.

    The verdict travels on its own pipe, not in the exit status: a premature
    exit, a missing verdict, a value outside the declared domain, or a status
    that would wrap through POSIX truncation is a HARNESS failure, never a
    pass. The child's stdout and stderr are captured -- Python-level writes
    included, because the child makes them write-through -- and must be EMPTY.

    The child is reaped on EVERY path: collection errors, validation errors
    and the timeout alike. Acquisition failures release what was already
    acquired. The collection failure is preserved alongside any cleanup
    failure.
    """
    # Flush the PARENT's own buffers before forking, so nothing of ours is
    # inherited and then attributed to the child.
    sys.stdout.flush()
    sys.stderr.flush()

    receipt_r = receipt_w = output_r = output_w = None
    child = None
    receipt, output = b"", b""
    exit_code = None
    try:
        receipt_r, receipt_w = os.pipe()
        output_r, output_w = os.pipe()
        child = os.fork()
        if child == 0:
            global _RECEIPT_FD, _CHILD_BASELINE
            try:
                os.close(receipt_r)
                os.close(output_r)
                os.dup2(output_w, 1)
                os.dup2(output_w, 2)
                os.close(output_w)
                # Python-level writes must reach the captured descriptors,
                # established BEFORE the scenario runs. Reconfiguring the text
                # layer is not enough: the BUFFERED binary layer would still
                # hold the bytes, and os._exit discards them. Unbuffered
                # writers over the duplicated descriptors are.
                sys.stdout = io.TextIOWrapper(open(1, "wb", buffering=0),
                                              write_through=True)
                sys.stderr = io.TextIOWrapper(open(2, "wb", buffering=0),
                                              write_through=True)
                _RECEIPT_FD = receipt_w
                _CHILD_BASELINE = child_observations()
                try:
                    status = body()
                except BaseException:
                    status = 29
                try:
                    child_receipt(status)
                except HarnessFailure as invalid:
                    # A verdict that breaks the declared type rule is a
                    # HARNESS failure, not a status: it is reported as such
                    # and the parent refuses it by name.
                    os.write(_RECEIPT_FD, f"invalid {invalid}\n".encode())
            except BaseException:
                try:
                    child_receipt(29)
                except BaseException:
                    pass
            finally:
                os._exit(0)

        os.close(receipt_w)
        receipt_w = None
        os.close(output_w)
        output_w = None
        deadline = time.monotonic() + timeout
        open_fds = {receipt_r: "receipt", output_r: "output"}
        while open_fds:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            ready, _, _ = select.select(list(open_fds), [], [], remaining)
            if not ready:
                break
            for fd in ready:
                chunk = os.read(fd, 4096)
                if not chunk:
                    del open_fds[fd]
                elif fd == receipt_r:
                    receipt += chunk
                else:
                    output += chunk
    finally:
        # settle the child and release only what this call acquired, whatever
        # went wrong above
        cleanup_error = None
        if child:
            try:
                exit_code = reap_child(child, 5.0)
                if exit_code is None:
                    cleanup_error = HarnessFailure(
                        "the owned child had to be killed after its bound")
            except BaseException as error:      # noqa: BLE001
                cleanup_error = error
        for fd in (receipt_r, receipt_w, output_r, output_w):
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
        if cleanup_error is not None:
            primary = sys.exc_info()[1]
            if primary is None:
                raise cleanup_error
            # BOTH failures survive: the collection failure is never replaced
            # by the cleanup failure, and the cleanup failure is never lost.
            raise BaseExceptionGroup(
                "the child's collection failed and so did its settlement",
                [primary, cleanup_error]) from None

    # The receipt never overrides the envelope: a child that wrote a valid
    # verdict and then failed or was killed is still a harness failure.
    case.assertEqual(exit_code, 0,
                     "the owned child did not complete its envelope "
                     f"(exit status {exit_code})")
    case.assertEqual(output, b"",
                     "the owned child emitted an unplanned diagnostic: "
                     f"{output!r}")
    lines = receipt.decode(errors="replace").splitlines()
    case.assertEqual(len(lines), 1,
                     f"the child did not return exactly one verdict: {lines}")
    fields = lines[0].split()
    if fields and fields[0] != "verdict":
        case.fail(f"the child's verdict was refused at the source: {lines[0]!r}")
    case.assertEqual(len(fields), 5,
                     f"malformed child receipt: {lines[0]!r}")
    case.assertEqual(fields[0], "verdict",
                     f"the child's verdict was refused at the source: "
                     f"{lines[0]!r}")
    for field in fields[1:]:
        case.assertTrue(field.lstrip("-").isdigit(),
                        f"the child's receipt is not integral: {lines[0]!r}")
    status = int(fields[1])
    case.assertIn(status, CHILD_STATUSES,
                  "the child returned a status outside the declared domain: "
                  f"{status}")
    case.child_observations = (int(fields[2]), int(fields[3]),
                               int(fields[4]))
    return status


def assert_child_ok(case, outcome, what):
    case.assertEqual(
        outcome, 0,
        f"{what}: {CHILD_MEANING.get(outcome, 'unknown child status')} "
        f"(status {outcome})")


class HarnessFailure(AssertionError):
    """The fixture, not the product, failed. Never evidence about either."""


class WaitBlockingProtocol:
    """A worker action provably inside ONE native call.

    The old protocol observed a STICKY entered flag, which admitted a late
    action: a slice could time out, the worker could act between slices, and
    the NEXT call would return the expected outcome. That is a false pass, and
    it is now impossible here.

    The fixture's OPT-IN hold parks the qualified call inside itself until the
    declared action releases it, so the interval is the call. The fixture
    records WHICH call was parked when the action ran; success is bound to
    that same call, its recorded outcome, and the worker's own release. A
    cleanup release can never be credited as the action, because cleanup does
    not record an action call.

    Settlement is enforced in __exit__, before any owner teardown, and
    `settled` says whether the owner may be destroyed at all.
    """

    def __init__(self, case, act):
        self.case = case
        self.act = act
        self.failures = []
        self.held = []
        self.acted_on = []
        self.released = []
        self.worker = None
        self.settled = False
        self.join_timeout = 10.0      # a bound, never a receipt
        # the disposition controls exercise the RAISING path deliberately
        self.quarantine_disabled = False

    def __enter__(self):
        _native._test_sender_wait_hold(True)

        def body():
            try:
                index = _native._test_sender_wait_held(5.0)
                self.held.append(index)
                if index < 0:
                    raise HarnessFailure("no call was ever parked")
                self.act()                      # strictly INSIDE that call
                # note the action while the call is STILL parked, then release
                self.acted_on.append(_native._test_sender_wait_note_action())
                self.released.append(
                    _native._test_sender_wait_release_held())
            except BaseException as error:      # noqa: BLE001
                self.failures.append(error)
                # never leave the call parked on a worker failure
                _native._test_sender_wait_hold(False)

        self.worker = threading.Thread(target=body)
        self.worker.start()
        return self

    def __exit__(self, kind, value, traceback):
        # cleanup first: disarming releases anything still parked, and it does
        # NOT record an action call, so it cannot be mistaken for the worker
        _native._test_sender_wait_hold(False)
        _native._test_unblock()
        self.worker.join(self.join_timeout)
        alive = self.worker.is_alive()
        self.settled = not alive
        if alive:
            if _RECEIPT_FD is not None and not self.quarantine_disabled:
                # inside a child: end it here, before any frame unwinds
                child_fail_now(10)
            harness = HarnessFailure(
                "the owned worker did not settle: the owner must not be "
                "destroyed while it may still be in use")
            if value is None:
                raise harness
            # preserve the primary failure, and report the cleanup failure too
            raise BaseExceptionGroup(
                "the wait body failed and its worker did not settle",
                [value, harness]) from None
        return False

    def assert_acted_inside_one_call(self, expected_call=1):
        """Bind success to the SAME call the worker acted inside."""
        report = _native._test_sender_wait_hold_report()
        self.case.assertEqual(self.failures, [],
                              f"the worker failed: {self.failures}")
        self.case.assertFalse(report["timed_out"],
                              "the hold's own bound expired: harness failure")
        self.case.assertEqual(self.held, [expected_call],
                              "the worker did not park on the qualified call")
        self.case.assertEqual(self.acted_on, [expected_call],
                              "the action was not noted inside the held call")
        self.case.assertEqual(self.released, [True],
                              "the worker did not release the call it held")
        self.case.assertEqual(report["action_call"], expected_call,
                              "the action did not happen inside that call")
        self.case.assertEqual(
            _native._test_sender_wait_counts()["calls"], expected_call,
            "success was reached by a LATER native call, not the held one")
        self.case.assertFalse(_native._test_watchdog_fired(),
                              "the fixture watchdog released the wait: that is "
                              "a harness failure, not evidence")


class WaitSubstrateBoundaryTests(WaitFixtureBase):
    """The protocol itself, qualified against the SUBSTRATE driver.

    Every row that starts a worker runs in an OWNED CHILD: the endpoint, the
    sender and the worker are all created there, so a scenario can neither
    leave a thread in this runner nor reach this process's fixture globals.
    The parent only reaps and reads the status.

    `_test_sender_wait_simulate` releases the GIL exactly as the bridge must,
    so the held-interval, worker and settlement wiring the gated rows depend
    on is proved HERE. It is NOT product evidence: the product's own
    no-GIL-release discriminator is owed after GREEN.
    """

    # -- helpers used INSIDE the children ---------------------------------

    def held_call_in_background(self, timeout_us=5_000):
        """Arm the hold, start one native call, and wait until it is parked.

        Contract on failure: settle locally and re-raise, or -- if settlement
        fails -- end this isolated child at once. The caller is never handed a
        worker it must dispose of, and never receives a disposition for one it
        cannot see.
        """
        _native._test_sender_wait_hold(True)
        outcome = []
        worker = threading.Thread(
            target=lambda: outcome.append(
                _native._test_sender_wait_simulate(timeout_us)))
        worker.start()
        try:
            parked = _native._test_sender_wait_held(5.0)
        except BaseException:
            # Contract: settle locally and re-raise, or -- if settlement
            # fails -- end this isolated child at once. The caller never
            # receives a disposition for a worker it cannot see.
            _native._test_sender_wait_hold(False)
            worker.join(10.0)
            if worker.is_alive():
                child_fail_now(10)
            raise
        return worker, outcome, parked

    def settle_background(self, worker):
        """Join a background call and REQUIRE it to have settled.

        Inside a child, a worker that cannot settle ends the child AT THIS
        POINT: no owner frame unwinds, so no capsule destructor, no close and
        no interpreter finalization runs after the decision.
        """
        _native._test_sender_wait_hold(False)
        if worker is not None:
            worker.join(10.0)
            if worker.is_alive():
                child_fail_now(10)

    # -- scenarios, each executed inside its own child --------------------

    def acted_inside_child(self, action, scripted, expected_outcome,
                           patch_wake_to_noop=False):
        """One held call, one action performed inside it, one verdict."""
        def body():
            endpoint = self.connect()
            sender = self.attach(endpoint)
            protocol = None
            status = 0
            try:
                _native._test_sender_wait_level(False)
                _native._test_wait_result(scripted)
                protocol = WaitBlockingProtocol(self, lambda: action(endpoint))
                if patch_wake_to_noop:
                    patch = mock.patch.object(moq5.Endpoint, "wake",
                                              lambda self: None)
                else:
                    patch = contextlib.nullcontext()
                with patch:
                    with protocol:
                        outcome = _native._test_sender_wait_simulate(5_000)
                report = _native._test_sender_wait_hold_report()
                counts = _native._test_sender_wait_counts()
                if not protocol.settled:
                    status = 10
                elif protocol.failures:
                    status = 11
                elif report["timed_out"] or _native._test_watchdog_fired():
                    status = 12
                elif protocol.held != [1] or protocol.acted_on != [1]:
                    status = 13
                elif counts["calls"] != 1:
                    status = 15
                elif outcome != expected_outcome:
                    status = 16
            except BaseException:
                status = 29
            finally:
                # never destroy an owner under an unsettled worker
                if protocol is None or protocol.settled:
                    try:
                        sender.close()
                    finally:
                        endpoint.close()
                elif status == 0:
                    status = 10
            return status
        return in_owned_child(self, body)

    def test_a_a_worker_acts_inside_the_held_call(self):
        assert_child_ok(
            self,
            self.acted_inside_child(
                lambda endpoint: endpoint.set_interrupted(True),
                DONE, INTERRUPTED),
            "an interrupt performed inside the held call")

    def test_b_a_worker_wake_inside_the_held_call(self):
        # The no-wake outcome is a scripted DONE, so only a REAL wake can
        # produce OK: releasing the gate alone cannot.
        assert_child_ok(
            self,
            self.acted_inside_child(lambda endpoint: endpoint.wake(),
                                    DONE, OK),
            "a wake performed inside the held call")

    def test_b2_a_no_op_wake_is_refused_by_the_actual_wake_row(self):
        # The durable negative runs the ACTUAL row above, with Endpoint.wake
        # replaced by a no-op inside its child. The row's own assertion must
        # fail, with the outcome verdict naming the cause.
        original = WaitSubstrateBoundaryTests.acted_inside_child

        def no_op_wake(case, action, scripted, expected, **kwargs):
            return original(case, action, scripted, expected,
                            patch_wake_to_noop=True)

        with mock.patch.object(WaitSubstrateBoundaryTests,
                               "acted_inside_child", no_op_wake):
            with self.assertRaises(AssertionError) as raised:
                self.test_b_a_worker_wake_inside_the_held_call()
        message = str(raised.exception)
        del raised
        self.assertIn("the outcome did not follow the action", message,
                      "a no-op wake was not refused by the row's own outcome "
                      f"verdict: {message}")

    def test_c_a_late_worker_is_refused_by_the_same_assertions(self):
        # The action happens only AFTER the held call returned: nothing was
        # parked when it happened, and the same assertion path must refuse it.
        def body():
            endpoint = self.connect()
            sender = self.attach(endpoint)
            worker = None
            status = 0
            try:
                _native._test_sender_wait_level(False)
                _native._test_wait_result(DONE)
                worker, outcome, parked = self.held_call_in_background()
                if parked != 1:
                    return 13
                _native._test_sender_wait_release_held()   # release, no action
                worker.join(10.0)
                if worker.is_alive():
                    return 10
                if outcome != [DONE]:
                    return 16
                endpoint.set_interrupted(True)             # the LATE action
                noted = _native._test_sender_wait_note_action()
                if noted != -1:
                    return 13
                protocol = WaitBlockingProtocol(self, lambda: None)
                protocol.held, protocol.acted_on = [1], [noted]
                protocol.released = [True]
                try:
                    protocol.assert_acted_inside_one_call()
                except AssertionError:
                    status = 0                  # refused, as it must be
                else:
                    status = 17                 # a late action was accepted
            except BaseException:
                status = 29
            finally:
                endpoint.set_interrupted(False)
                try:
                    self.settle_background(worker)
                except HarnessFailure:
                    return 10
                sender.close()
                endpoint.close()
            return status
        assert_child_ok(self, in_owned_child(self, body),
                        "a late action must be refused")

    def test_d_a_cleanup_release_is_not_credited_as_the_action(self):
        def body():
            endpoint = self.connect()
            sender = self.attach(endpoint)
            worker = None
            status = 0
            try:
                _native._test_sender_wait_level(False)
                _native._test_wait_result(DONE)
                worker, outcome, parked = self.held_call_in_background()
                if parked != 1:
                    return 13
                _native._test_sender_wait_hold(False)      # CLEANUP only
                worker.join(10.0)
                if worker.is_alive():
                    return 10
                if _native._test_sender_wait_hold_report()["action_call"] != -1:
                    return 17                  # cleanup recorded as an action
                protocol = WaitBlockingProtocol(self, lambda: None)
                protocol.held, protocol.acted_on = [1], []
                protocol.released = []
                try:
                    protocol.assert_acted_inside_one_call()
                except AssertionError:
                    status = 0
                else:
                    status = 17
            except BaseException:
                status = 29
            finally:
                try:
                    self.settle_background(worker)
                except HarnessFailure:
                    return 10
                sender.close()
                endpoint.close()
            return status
        assert_child_ok(self, in_owned_child(self, body),
                        "a cleanup release must not be credited as the action")

    def unsettled_quarantine_child(self):
        """One child whose worker cannot settle, reporting what it observed.

        The receipt carries the native fixture's fingerprint taken AT THE
        EXIT, inside `child_fail_now`. The body records the same fingerprint
        just before the decision, so the parent can compare the two and see
        whether anything closed, destroyed or reset in between -- rather than
        inferring it. A finalization observer is kept as well: if any frame
        unwound it would add a second receipt line and the envelope would
        refuse the receipt.
        """
        def body():
            endpoint = self.connect()
            sender = self.attach(endpoint)
            stuck = threading.Event()

            class Observer:
                def __del__(self):
                    try:
                        os.write(_RECEIPT_FD, b"verdict 99 0 0 0\n")
                    except BaseException:
                        pass

            observer = Observer()             # noqa: F841 -- held by the frame
            protocol = WaitBlockingProtocol(self, lambda: None)
            protocol.worker = threading.Thread(target=lambda: stuck.wait(30.0))
            protocol.join_timeout = 0.5
            protocol.worker.start()
            self.decision_fingerprint = child_observations()
            protocol.__exit__(None, None, None)   # must not return
            stuck.set()                           # unreachable if it holds
            return 17
        return body

    def test_e_an_unsettled_worker_ends_the_child_before_any_teardown(self):
        """The quarantine, OBSERVED at the decision-to-exit boundary."""
        # The child attaches exactly one sender, closes nothing and resets
        # nothing, so at the decision its fingerprint is (0 destroys, 1
        # attach, 0 reset entries). The receipt
        # carries the fingerprint taken AT THE EXIT: equality is the evidence
        # that nothing closed, destroyed or reset in between.
        outcome = in_owned_child(self, self.unsettled_quarantine_child())
        self.assertEqual(outcome, 10,
                         "an unsettled worker did not end the child at the "
                         f"decision (status {outcome})")
        self.assertEqual(self.child_observations, (0, 1, 0),
                         "an observed operation ran after the unsettled "
                         "decision -- a sender destroy, a sender attach or a "
                         f"wait-fixture reset: {self.child_observations}")

    def test_e0_a_close_at_the_decision_fails_the_same_oracle(self):
        """The perturbation: an explicit close exactly at the decision.

        `child_fail_now` is wrapped so the forbidden operation happens in the
        decision-to-exit interval. The same oracle must refuse it -- and the
        worker here only waits on an Event, so no concurrently used native
        object is destroyed to make the point.
        """
        original = child_fail_now

        def closing_fail_now(status):
            for candidate in gc.get_objects():
                if isinstance(candidate, moq5.Sender):
                    try:
                        candidate.close()
                    except BaseException:
                        pass
            original(status)

        with mock.patch.object(sys.modules[__name__], "child_fail_now",
                               closing_fail_now):
            outcome = in_owned_child(self, self.unsettled_quarantine_child())
        self.assertEqual(outcome, 10,
                         "the perturbed child did not still report the "
                         "unsettled verdict")
        self.assertNotEqual(
            self.child_observations, (0, 1, 0),
            "an explicit close at the decision was not observed by the "
            "oracle: the no-teardown claim would be empty")
        self.assertEqual(
            self.child_observations, (1, 1, 0),
            "the forbidden close was observed, but not as exactly one destroy "
            f"{self.child_observations}")

    def test_e0b_a_fixture_reset_at_the_decision_fails_the_same_oracle(self):
        """The reset perturbation, observed at its own ENTRY.

        `_test_sender_wait_reset` moves no sender attach or destroy counter,
        so the fingerprint now carries the wait fixture's own reset-entry
        count. A reset performed exactly at the decision must be refused by
        the same oracle.
        """
        original = child_fail_now

        def resetting_fail_now(status):
            _native._test_sender_wait_reset()    # the forbidden operation
            original(status)

        with mock.patch.object(sys.modules[__name__], "child_fail_now",
                               resetting_fail_now):
            outcome = in_owned_child(self, self.unsettled_quarantine_child())
        self.assertEqual(outcome, 10,
                         "the perturbed child did not still report the "
                         "unsettled verdict")
        self.assertNotEqual(
            self.child_observations, (0, 1, 0),
            "a fixture reset at the decision was not observed by the oracle")
        self.assertEqual(
            self.child_observations, (0, 1, 1),
            "the forbidden reset was observed, but not as exactly one reset "
            f"entry {self.child_observations}")

    def test_e1b_the_unsettled_disposition_itself_refuses_everything(self):
        """The same verdict, with the quarantine deliberately disabled, so
        the disposition logic can be observed: __exit__ raises, `settled`
        stays false, the shared rule refuses, and nothing is destroyed."""
        def body():
            endpoint = self.connect()
            sender = self.attach(endpoint)
            before = _native._test_sender_counts()["destroy_entries"]
            stuck = threading.Event()
            protocol = WaitBlockingProtocol(self, lambda: None)
            protocol.quarantine_disabled = True
            protocol.worker = threading.Thread(target=lambda: stuck.wait(30.0))
            protocol.join_timeout = 0.5
            protocol.worker.start()
            status = 0
            try:
                try:
                    protocol.__exit__(None, None, None)
                except HarnessFailure:
                    pass
                else:
                    status = 17
                if status == 0 and protocol.settled:
                    status = 17
                if status == 0:
                    try:
                        self.close_if_settled(protocol, sender, endpoint)
                    except HarnessFailure:
                        pass
                    else:
                        status = 17
                if (status == 0 and
                        _native._test_sender_counts()["destroy_entries"]
                        != before):
                    status = 17
            except BaseException:
                status = 29
            finally:
                stuck.set()
                protocol.worker.join(10.0)
                if protocol.worker.is_alive():
                    child_fail_now(10)
            return status
        assert_child_ok(self, in_owned_child(self, body),
                        "an unsettled disposition must refuse every step")

    def test_e2_a_failing_body_keeps_both_failures_and_touches_nothing(self):
        # Body failure PLUS an unsettled verdict, through the real caller
        # path: nothing destroyed, no fixture reset, and BOTH failures kept.
        def body():
            endpoint = self.connect()
            sender = self.attach(endpoint)
            before = _native._test_sender_counts()["destroy_entries"]
            protocol = WaitBlockingProtocol(self, lambda: None)
            protocol.worker = threading.Thread(target=lambda: None)
            protocol.worker.start()
            protocol.worker.join(5.0)
            protocol.settled = False           # the injected verdict
            status = 0
            try:
                try:
                    try:
                        raise ZeroDivisionError("the body failed")
                    finally:
                        self.close_if_settled(protocol, sender, endpoint)
                except HarnessFailure as harness:
                    if not isinstance(harness.__context__, ZeroDivisionError):
                        status = 18            # the primary failure was lost
                except ZeroDivisionError:
                    status = 17                # the rule did not refuse
                if (status == 0 and
                        _native._test_sender_counts()["destroy_entries"]
                        != before):
                    status = 17
                if status == 0 and (sender.closed or endpoint.closed):
                    status = 17
                # and the honest verdict disposes of both owners
                protocol.settled = True
                self.close_if_settled(protocol, sender, endpoint)
                if status == 0 and not (sender.closed and endpoint.closed):
                    status = 17
            except BaseException:
                status = 29
            return status
        assert_child_ok(self, in_owned_child(self, body),
                        "a failing body with an unsettled verdict")

    def test_e3_an_entry_observation_failure_settles_before_it_raises(self):
        # When observing the parked state raises, the helper settles its
        # worker and re-raises; nothing is handed back, and no thread is left.
        def body():
            endpoint = self.connect()
            sender = self.attach(endpoint)
            status = 0
            try:
                _native._test_sender_wait_level(False)
                _native._test_wait_result(DONE)
                with mock.patch.object(
                        _native, "_test_sender_wait_held",
                        side_effect=RuntimeError("entry observation failed")):
                    try:
                        self.held_call_in_background()
                    except RuntimeError:
                        pass
                    else:
                        status = 17        # the failure was swallowed
                if status == 0 and threading.active_count() != 1:
                    # a surviving worker: end the child before any teardown
                    child_fail_now(10)
            except BaseException:
                status = 29
            if status == 0:
                # the worker DID settle, so ordinary cleanup is correct here
                try:
                    sender.close()
                finally:
                    endpoint.close()
            return status
        assert_child_ok(self, in_owned_child(self, body),
                        "an entry-observation failure must settle before it raises")

    def test_f_a_following_test_runs_safely_in_the_untouched_parent(self):
        # The point of the envelope: after the failure scenarios above, this
        # process's fixture is untouched and a following test runs normally.
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                _native._test_sender_wait_level(True)
                self.assertEqual(_native._test_sender_wait_simulate(0), OK)
                self.assertEqual(
                    _native._test_sender_wait_counts()["calls"], 1,
                    "the parent's fixture was disturbed by a child")
                self.assertEqual(threading.active_count(), 1,
                                 "a child's worker survived into the parent")
            finally:
                sender.close()

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only contract")
    def test_g_a_raising_signal_inside_the_held_call(self):
        # Already isolated: ONE child, no nesting.
        child = os.fork()
        if child == 0:
            status = 0
            try:
                import signal as signal_module

                received = []

                def handler(*args):
                    received.append(True)
                    raise KeyboardInterrupt

                signal_module.signal(signal_module.SIGUSR1, handler)
                endpoint = self.connect()
                sender = self.attach(endpoint)
                protocol = None
                try:
                    _native._test_sender_wait_level(False)
                    _native._test_wait_result(DONE)
                    protocol = WaitBlockingProtocol(
                        self, lambda: os.kill(os.getpid(),
                                              signal_module.SIGUSR1))
                    interrupted = False
                    try:
                        with protocol:
                            _native._test_sender_wait_simulate(5_000)
                    except KeyboardInterrupt:
                        interrupted = True
                    report = _native._test_sender_wait_hold_report()
                    calls = _native._test_sender_wait_counts()["calls"]
                    if not protocol.settled:
                        status = 10
                    elif protocol.failures:
                        status = 11
                    elif report["timed_out"] or _native._test_watchdog_fired():
                        status = 12
                    elif report["action_call"] != 1:
                        status = 13
                    elif not received:
                        status = 14
                    elif calls != 1:
                        status = 15
                    elif not interrupted:
                        status = 16
                finally:
                    if protocol is None or protocol.settled:
                        sender.close()
                        endpoint.close()
                    elif status == 0:
                        status = 10
            except BaseException:
                status = 29
            os._exit(status)
        outcome = reap_child(child)
        self.assertIsNotNone(outcome, "the signal child had to be killed")
        assert_child_ok(self, outcome,
                        "a raising signal delivered inside the held call")


def one_slice_clock(budget_us):
    """A declared clock: ONE positive slice, and then the budget is spent.

    The deadline read and the first remaining-time read both see zero elapsed,
    so the first native call receives the full budget as its timeout -- not a
    zero poll. Every later read sees the whole budget spent, so the public
    path makes EXACTLY one native call whatever that call returns: a perturbed
    action cannot become a retry loop, and the recorded native timeout is the
    positive slice.
    """
    spent = budget_us * 1_000
    return mock.Mock(side_effect=[0, 0] + [spent] * 8)


class WaitNativeBoundaryTests(WaitGatedBase):
    """What only the REAL bridge can show. RED until it exists.

    The surface check runs HERE, so a missing method is a named failure in
    this process. Everything concurrent then runs in an OWNED CHILD -- owner
    and workers created there -- so no scenario can leave a thread or a
    disturbed fixture behind in the shared runner.
    """

    def public_action_child(self, action, scripted, expected, *,
                            raising_signal=False):
        def body():
            endpoint = self.connect()
            sender = self.attach(endpoint)
            protocol = None
            status = 0
            try:
                if raising_signal:
                    import signal as signal_module
                    received = []

                    def handler(*args):
                        received.append(True)
                        raise KeyboardInterrupt

                    signal_module.signal(signal_module.SIGUSR1, handler)
                else:
                    received = [True]

                _native._test_sender_wait_level(False)
                _native._test_wait_result(scripted)
                protocol = WaitBlockingProtocol(self, lambda: action(endpoint))
                interrupted = False
                outcome = None
                try:
                    with mock.patch.object(_endpoint, "_monotonic_ns",
                                           one_slice_clock(WAIT_SLICE_US)):
                        with protocol:
                            outcome = sender.wait(WAIT_SLICE_US)
                except KeyboardInterrupt:
                    interrupted = True
                report = _native._test_sender_wait_hold_report()
                counts = _native._test_sender_wait_counts()
                if not protocol.settled:
                    status = 10
                elif protocol.failures:
                    status = 11
                elif report["timed_out"] or _native._test_watchdog_fired():
                    status = 12
                elif protocol.held != [1] or report["action_call"] != 1:
                    status = 13
                elif not received:
                    status = 14
                elif counts["calls"] != 1:
                    status = 15
                elif counts["last_timeout_us"] != WAIT_SLICE_US:
                    status = 15     # the declared slice, not a zero poll
                elif raising_signal and not interrupted:
                    status = 16
                elif not raising_signal and outcome is not expected:
                    status = 16
            except BaseException:
                status = 29
            finally:
                if protocol is None or protocol.settled:
                    try:
                        sender.close()
                    finally:
                        endpoint.close()
                elif status == 0:
                    status = 10
            return status
        return in_owned_child(self, body)

    def test_a_another_thread_runs_inside_the_blocked_native_call(self):
        self.require_surface()
        assert_child_ok(
            self,
            self.public_action_child(
                lambda endpoint: endpoint.set_interrupted(True),
                DONE, moq5.WaitResult.INTERRUPTED),
            "an interrupt performed inside the blocked native call")

    def test_a2_a_wake_inside_the_held_call_is_its_own_fact(self):
        self.require_surface()
        assert_child_ok(
            self,
            self.public_action_child(lambda endpoint: endpoint.wake(),
                                     DONE, moq5.WaitResult.WOKEN),
            "a wake performed inside the blocked native call")

    def test_b_a_cross_thread_interrupt_is_retained_until_cleared(self):
        self.require_surface()

        def body():
            endpoint = self.connect()
            sender = self.attach(endpoint)
            protocol = None
            status = 0
            try:
                _native._test_sender_wait_level(False)
                _native._test_wait_result(DONE)
                protocol = WaitBlockingProtocol(
                    self, lambda: endpoint.set_interrupted(True))
                with mock.patch.object(_endpoint, "_monotonic_ns",
                                       one_slice_clock(WAIT_SLICE_US)):
                    with protocol:
                        first = sender.wait(WAIT_SLICE_US)
                report = _native._test_sender_wait_hold_report()
                if not protocol.settled:
                    status = 10
                elif protocol.failures:
                    status = 11
                elif report["timed_out"]:
                    status = 12
                elif report["action_call"] != 1:
                    status = 13
                elif first is not moq5.WaitResult.INTERRUPTED:
                    status = 16
                elif sender.wait(0) is not moq5.WaitResult.INTERRUPTED:
                    status = 16         # the latch was not RETAINED
                else:
                    endpoint.set_interrupted(False)
                    _native._test_sender_wait_level(True)
                    if sender.wait(0) is not moq5.WaitResult.WOKEN:
                        status = 16     # the latch was not cleared by its owner
            except BaseException:
                status = 29
            finally:
                endpoint.set_interrupted(False)
                if protocol is None or protocol.settled:
                    try:
                        sender.close()
                    finally:
                        endpoint.close()
                elif status == 0:
                    status = 10
            return status
        assert_child_ok(self, in_owned_child(self, body),
                        "a cross-thread interrupt and its retention")

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only contract")
    def test_c_a_raising_signal_inside_the_held_call_propagates(self):
        self.require_surface()
        assert_child_ok(
            self,
            self.public_action_child(
                lambda endpoint: os.kill(os.getpid(), __import__("signal").SIGUSR1),
                DONE, None, raising_signal=True),
            "a raising signal delivered inside the blocked native call")


# ------------------------------------------------------------------ control --

PUBLIC_GATED_CLASSES = ("WaitResultTests", "WaitBudgetTests",
                        "WaitTimeoutDomainTests", "WaitGuardTests",
                        "WaitNativeBoundaryTests")


class WaitControlTests(unittest.TestCase):
    """This file's own machinery. Never evidence about the product."""

    # Rows about the NATIVE entry point, which a Python stand-in cannot model.
    EXCLUDED = {
        "WaitTimeoutDomainTests.test_a2_the_private_bridge_passes_an_unsliced_timeout_unchanged",
        "WaitTimeoutDomainTests.test_b_the_private_bridge_validates_its_own_timeout",
        "WaitGuardTests.test_a_wrong_capsules_and_arities_are_refused",
        "WaitGuardTests.test_b_the_owner_thread_and_pid_guards_precede_service_entry",
        "WaitNativeBoundaryTests.test_a_another_thread_runs_inside_the_blocked_native_call",
        "WaitNativeBoundaryTests.test_a2_a_wake_inside_the_held_call_is_its_own_fact",
        "WaitNativeBoundaryTests.test_b_a_cross_thread_interrupt_is_retained_until_cleared",
        "WaitNativeBoundaryTests.test_c_a_raising_signal_inside_the_held_call_propagates",
        "WaitBudgetTests.test_i_the_shared_slicer_is_the_one_used",
    }

    def test_a_inert_names_are_refused_by_every_gated_row(self):
        import test_send_wait as module

        saved = getattr(moq5.Sender, "wait", None)
        try:
            moq5.Sender.wait = object()
            loader = unittest.defaultTestLoader
            suite = unittest.TestSuite(
                loader.loadTestsFromTestCase(getattr(module, c))
                for c in PUBLIC_GATED_CLASSES)
            result = unittest.TestResult()
            suite.run(result)
        finally:
            if saved is None:
                if hasattr(moq5.Sender, "wait"):
                    del moq5.Sender.wait
            else:
                moq5.Sender.wait = saved
        self.assertGreater(result.testsRun, 0)
        self.assertEqual(result.skipped, [], "a gated row skipped")
        self.assertEqual(result.errors, [],
                         "a gated row raised an arbitrary error instead of "
                         "refusing by a named assertion")
        failed = {getattr(case, "test_case", case).id()
                  for case, _ in result.failures}
        self.assertEqual(len(failed), result.testsRun,
                         "an inert surface satisfied a behavioural row")

    @staticmethod
    def stand_in(*, whole_budget=False, retry_after_failure=False,
                 fabricate_timeout=False, latch_cleared=False):
        """This file's adapter: the SHARED slicer over the native mirror.

        The bridge does not exist yet, so the native call is reached through
        the substrate driver -- the same `moq_media_sender_wait` mirror, minus
        the capsule. The declared result mapping is modelled here: the four
        agreed codes become WaitResult, anything else becomes a MoqError with
        the operation "wait". Perturbations are OPT-IN, so one adapter serves
        the positive control and every discriminator.
        """
        agreed = {int(value) for value in moq5.WaitResult}

        def native(handle, timeout_us):
            # A row that scripts the native wait installs `_native.sender_wait`
            # for the duration; the stand-in must use THAT, exactly as the
            # product bridge would, or a scripted clock would never advance.
            # Otherwise it falls back to the substrate driver, which is the
            # same `moq_media_sender_wait` mirror minus the capsule.
            scripted = getattr(_native, "sender_wait", None)
            if scripted is not None:
                # through the binding's own translation, so a native error
                # becomes a MoqError exactly as the bridge would deliver it
                return _endpoint._call(scripted, handle, timeout_us)
            code = _native._test_sender_wait_simulate(timeout_us)
            if code not in agreed:
                raise moq5.MoqError(code, "wait", "scripted")
            return code

        def wait(sender, timeout_us):
            if sender.closed:
                raise RuntimeError("sender is closed")
            handle = getattr(sender, "_Sender__handle")
            if fabricate_timeout and timeout_us == 0:
                return moq5.WaitResult.TIMED_OUT
            if whole_budget:
                from moq5._values import _bounded_integer, _INT64_MAX
                _bounded_integer(timeout_us, "timeout_us", _INT64_MAX)
                return moq5.WaitResult(native(handle, timeout_us))
            if retry_after_failure:
                try:
                    return _endpoint._sliced_wait(native, handle, timeout_us)
                except BaseException:
                    return _endpoint._sliced_wait(native, handle, timeout_us)
            outcome = _endpoint._sliced_wait(native, handle, timeout_us)
            if latch_cleared and outcome is moq5.WaitResult.INTERRUPTED:
                sender.endpoint.set_interrupted(False)
            return outcome
        return wait

    def run_bodies(self, wait, excluded):
        import test_send_wait as module

        saved = getattr(moq5.Sender, "wait", None)
        try:
            moq5.Sender.wait = wait
            loader = unittest.defaultTestLoader
            names = []
            for class_name in PUBLIC_GATED_CLASSES:
                for test in loader.getTestCaseNames(getattr(module, class_name)):
                    if f"{class_name}.{test}" in excluded:
                        continue
                    names.append(f"{class_name}.{test}")
            suite = loader.loadTestsFromNames(names, module)
            result = unittest.TestResult()
            suite.run(result)
        finally:
            if saved is None:
                if hasattr(moq5.Sender, "wait"):
                    del moq5.Sender.wait
            else:
                moq5.Sender.wait = saved
        return result

    @property
    def ALL_ROWS(self):
        import test_send_wait as module

        loader = unittest.defaultTestLoader
        return {f"{class_name}.{test}"
                for class_name in PUBLIC_GATED_CLASSES
                for test in loader.getTestCaseNames(getattr(module, class_name))}

    def expected_body_count(self):
        import test_send_wait as module

        loader = unittest.defaultTestLoader
        return sum(
            1
            for class_name in PUBLIC_GATED_CLASSES
            for test in loader.getTestCaseNames(getattr(module, class_name))
            if f"{class_name}.{test}" not in self.EXCLUDED)

    def assert_semantic_rejection(self, result, defect, expected_rows):
        """A defect must be caught by a NAMED assertion in the rows that own
        it -- not by an arbitrary exception, a skip, or a body that never ran."""
        self.assertEqual(result.testsRun, self.expected_body_count(),
                         f"the bodies did not all run for {defect}")
        self.assertEqual(
            [f"{case.id()}: {text}" for case, text in result.errors], [],
            f"{defect} raised an arbitrary error instead of failing an "
            "assertion")
        self.assertEqual(result.skipped, [], f"a body skipped for {defect}")
        failed = {case.id().rsplit(".", 1)[-1].split(" (", 1)[0]
                  for case, _ in result.failures}
        self.assertNotEqual(failed, set(), f"the rows accepted {defect}")
        self.assertTrue(
            failed & expected_rows,
            f"{defect} was caught, but not by {sorted(expected_rows)}; "
            f"the failing rows were {sorted(failed)}")

    def test_b_the_gated_bodies_pass_against_a_scoped_stand_in(self):
        result = self.run_bodies(self.stand_in(), self.EXCLUDED)
        self.assertEqual(
            [f"{case.id()}: {error}" for case, error in
             result.failures + result.errors], [],
            "a gated body failed against the declared contract")
        self.assertEqual(result.skipped, [], "a gated body skipped")
        self.assertEqual(result.testsRun, self.expected_body_count(),
                         "not every intended body ran")

    def test_c_the_bodies_reject_each_named_defect(self):
        for name, wait, rows in (
                ("the whole budget in one native call",
                 self.stand_in(whole_budget=True),
                 {"test_a_one_budget_is_sliced_and_never_reset",
                  "test_c_the_whole_budget_is_never_one_native_call"}),
                ("a retry after a native failure",
                 self.stand_in(retry_after_failure=True),
                 {"test_g_a_native_failure_is_not_retried",
                  "test_h_a_pending_python_signal_is_not_retried"}),
                ("a fabricated zero-budget timeout",
                 self.stand_in(fabricate_timeout=True),
                 {"test_f_an_exhausted_budget_still_observes_its_outcome",
                  "test_a_every_agreed_code_maps_to_its_outcome",
                  "test_c_closed_is_a_result_on_a_live_sender",
                  "test_a_the_public_domain_is_the_existing_one",
                  "test_c_the_exact_sender_and_timeout_reach_native",
                  "test_d_waiting_has_no_lifecycle_effect"}),
                ("a latch cleared by waiting",
                 self.stand_in(latch_cleared=True),
                 {"test_d_waiting_has_no_lifecycle_effect"})):
            with self.subTest(defect=name):
                result = self.run_bodies(wait, self.EXCLUDED)
                self.assert_semantic_rejection(result, name, rows)

    def test_d_an_arbitrary_exception_is_not_a_semantic_kill(self):
        def boom(sender, timeout_us):
            raise LookupError("unrelated")

        result = self.run_bodies(boom, self.EXCLUDED)
        self.assertGreater(len(result.errors), 0,
                           "the errors-only stand-in did not error")
        with self.assertRaises(AssertionError):
            self.assert_semantic_rejection(
                result, "an unrelated exception",
                {"test_a_one_budget_is_sliced_and_never_reset"})

    def test_e_a_wrong_delivered_timeout_fails_the_actual_routing_row(self):
        """The perturbation runs the ACTUAL routing row, not a local copy.

        An adapter that delivers a value one microsecond off must make
        `test_c_the_exact_sender_and_declared_slice_reach_native` fail by its
        own named assertion -- with no errors and no skips. Deleting that
        row's assertion would make this control fail too, which is the point.
        """
        import test_send_wait as module

        row = ("WaitGuardTests."
               "test_c_the_exact_sender_and_declared_slice_reach_native")
        honest = self.stand_in()

        def skewed(sender, timeout_us):
            return honest(sender, timeout_us + 1)      # one microsecond off

        for name, wait, expect_failure in (("honest", honest, False),
                                           ("skewed", skewed, True)):
            with self.subTest(adapter=name):
                result = self.run_bodies(wait, self.EXCLUDED - {row}
                                         | {n for n in self.ALL_ROWS
                                            if n != row})
                self.assertEqual(result.testsRun, 1,
                                 "the actual routing row did not run")
                self.assertEqual(
                    [f"{case.id()}: {text}" for case, text in result.errors],
                    [], "the perturbation raised an arbitrary error")
                self.assertEqual(result.skipped, [], "the row skipped")
                if expect_failure:
                    self.assertEqual(len(result.failures), 1,
                                     "a wrong delivered timeout was accepted")
                    self.assertIn("the declared remaining slice",
                                  result.failures[0][1],
                                  "the failure was not the row's own named "
                                  "assertion")
                else:
                    self.assertEqual(result.failures, [],
                                     "the honest adapter failed the row")

    def test_e2_a_no_op_wake_is_refused_by_the_actual_gated_wake_row(self):
        """The gated wake row, run against the stand-in, with a no-op wake.

        The honest adapter must pass it and the no-op wake must fail it by the
        row's own outcome verdict -- so the row cannot be satisfied by merely
        releasing the held call.
        """
        import test_send_wait as module

        row = module.WaitNativeBoundaryTests(
            "test_a2_a_wake_inside_the_held_call_is_its_own_fact")
        saved = getattr(moq5.Sender, "wait", None)
        try:
            moq5.Sender.wait = self.stand_in()
            result = unittest.TestResult()
            row.run(result)
            self.assertEqual(result.failures + result.errors, [],
                             "the honest adapter failed the gated wake row")
            with mock.patch.object(moq5.Endpoint, "wake", lambda self: None):
                perturbed = unittest.TestResult()
                module.WaitNativeBoundaryTests(
                    "test_a2_a_wake_inside_the_held_call_is_its_own_fact"
                ).run(perturbed)
            self.assertEqual(perturbed.errors, [],
                             "the perturbation raised an arbitrary error")
            self.assertEqual(len(perturbed.failures), 1,
                             "a no-op wake satisfied the gated wake row")
            self.assertIn("the outcome did not follow the action",
                          perturbed.failures[0][1],
                          "the failure was not the row's own outcome verdict")
        finally:
            if saved is None:
                if hasattr(moq5.Sender, "wait"):
                    del moq5.Sender.wait
            else:
                moq5.Sender.wait = saved

    def test_f_the_bridge_is_present_and_refuses_wrong_arguments(self):
        # This row was the RED gate's "the bridge is absent" assertion. The
        # bridge exists now, so it pins presence, callability and that a
        # wrong-shaped call is refused rather than crashing.
        missing = tuple(n for n in BRIDGE if not hasattr(_native, n))
        self.assertEqual(missing, (), f"the wait bridge lost {missing}")
        for name in BRIDGE:
            self.assertTrue(callable(getattr(_native, name)),
                            f"_native.{name} is not callable")
        with self.assertRaises(TypeError):
            _native.sender_wait()
        with self.assertRaises(TypeError):
            _native.sender_wait(object())
        with self.assertRaises((TypeError, ValueError)):
            _native.sender_wait(object(), object())
