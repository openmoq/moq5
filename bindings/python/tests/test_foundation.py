"""Public Python contract, against the same bridge with a test-only C provider."""

import gc
import json
import os
import subprocess
import sys
import textwrap
import threading
import unittest
import warnings
from pathlib import Path
from unittest import mock

import moq5
from moq5 import _endpoint, _native

# The fixture's own failure identity: never a production moq_result_t.
FIXTURE_WATCHDOG = -1999
# An unexpected pthread failure inside the fixture: named, never MOQ_DONE.
FIXTURE_PTHREAD = -1998
# The reviewed SDK's offer capacity (service/src/endpoint_internal.h:13), which
# the fixture mirrors so the fake cannot certify what the SDK refuses.
SDK_MAX_VERSIONS = 8
WAIT_SLICE_US = 250_000


class ScriptedWait:
    """A scripted monotonic clock and native wait, so the PUBLIC wait path can be
    observed slice by slice: exact slice limits, elapsed overhead between native
    calls, the final remainder, and each native outcome -- without waiting."""

    def __init__(self, results, advance_ns_per_call=0, advance_ns_per_read=0):
        self.results = list(results)
        self.advance_call = advance_ns_per_call
        self.advance_read = advance_ns_per_read
        self.now_ns = 1_000_000_000_000
        self.slices = []
        self.reads = 0

    def clock(self):
        self.reads += 1
        self.now_ns += self.advance_read
        return self.now_ns

    def native_wait(self, handle, timeout_us):
        self.slices.append(timeout_us)
        self.now_ns += self.advance_call
        if not self.results:
            raise AssertionError("scripted native wait called more times than scripted")
        result = self.results.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result

    def patched(self):
        return (
            mock.patch.object(_endpoint, "_monotonic_ns", self.clock),
            mock.patch.object(_native, "wait", self.native_wait),
        )


class FoundationTests(unittest.TestCase):
    def setUp(self):
        gc.collect()
        _native._test_reset()

    def connect(self, **kwargs):
        return moq5.Endpoint.connect(moq5.EndpointConfig(url="moqt://fixture.invalid", **kwargs))

    def test_version_and_testing_identity(self):
        info = moq5.build_info()
        self.assertEqual(info["compiled_version"], "0.1.0")
        self.assertEqual(info["runtime_version"], "0.1.0")
        self.assertEqual(info["python_abi"], "cp312-abi3")
        self.assertTrue(info["test_backend"])

    def test_sized_config_exact_fields_and_secure_default(self):
        with self.connect(versions=(18, 16), sni="peer.invalid", ca_file="/a path/ca.pem",
                          protocol=moq5.Protocol.WEBTRANSPORT, backend=moq5.Backend.PICOQUIC,
                          wt_profile=moq5.WtProfile.CURRENT,
                          wt_path="/live", handshake_timeout_us=2**40) as endpoint:
            observed = _native._test_config()
            self.assertEqual(observed["url"], b"moqt://fixture.invalid")
            self.assertEqual(observed["versions"], (18, 16))
            self.assertEqual(observed["sni"], b"peer.invalid")
            self.assertEqual(observed["ca_file"], b"/a path/ca.pem")
            self.assertEqual(observed["wt_path"], b"/live")
            self.assertEqual(observed["handshake_timeout_us"], 2**40)
            self.assertFalse(observed["insecure_skip_verify"])
            self.assertTrue(observed["full_size"])
            self.assertEqual(observed["protocol"], 2)
            self.assertEqual(observed["backend"], 1)
            self.assertEqual(observed["wt_profile"], 1)
            self.assertEqual(observed["version_policy"], 1)
            self.assertFalse(endpoint.closed)
        self.assertTrue(endpoint.closed)
        self.assertEqual(_native._test_counts(), (1, 1, 1))

    def test_automatic_and_exact_version_policy(self):
        for versions, policy in (((), 0), ((18,), 2)):
            with self.connect(versions=versions):
                self.assertEqual(_native._test_config()["version_policy"], policy)
                self.assertEqual(_native._test_config()["versions"], versions)
        self.assertEqual(_native._test_counts(), (2, 2, 2))

    def test_config_owns_an_immutable_offer_copy(self):
        offered = [18, 16]
        config = moq5.EndpointConfig(url="moqt://fixture.invalid", versions=offered)
        offered[:] = [16]
        self.assertEqual(config.versions, (18, 16))
        with moq5.Endpoint.connect(config):
            self.assertEqual(_native._test_config()["versions"], (18, 16))

    def test_closed_owner_is_idempotent_and_not_reusable(self):
        endpoint = self.connect()
        endpoint.close()
        endpoint.close()
        self.assertEqual(_native._test_counts(), (1, 1, 1))
        for call in (lambda: endpoint.state, lambda: endpoint.terminal,
                     lambda: endpoint.negotiated_version, lambda: endpoint.wait(0),
                     endpoint.wake, lambda: endpoint.set_interrupted(True)):
            with self.subTest(call=call):
                with self.assertRaises(RuntimeError):
                    call()

    def test_native_error_preserves_code_and_operation(self):
        _native._test_connect_result(-14)
        with self.assertRaises(moq5.MoqError) as caught:
            self.connect()
        self.assertEqual(caught.exception.code, -14)
        self.assertEqual(caught.exception.operation, "connect")
        self.assertEqual(_native._test_counts(), (0, 0, 0))

    def test_refused_stop_retains_owner_without_destroy(self):
        endpoint = self.connect()
        try:
            _native._test_stop_result(-5)
            with self.assertRaises(moq5.MoqError) as caught:
                endpoint.close()
            self.assertEqual((caught.exception.code, caught.exception.operation), (-5, "close"))
            self.assertFalse(endpoint.closed)
            self.assertEqual(endpoint.state, moq5.EndpointState.CONNECTING)
            self.assertEqual(_native._test_counts(), (1, 1, 0))
        finally:
            _native._test_stop_result(0)
            endpoint.close()
        self.assertEqual(_native._test_counts(), (1, 2, 1))

    def test_close_in_progress_is_not_closed_and_cannot_be_woken(self):
        endpoint = self.connect()
        _native._test_block_stop()
        observed = []
        failures = []

        def other():
            try:
                _native._test_stop_entered()
                observed.append(endpoint.closed)
                for call in (endpoint.wake, lambda: endpoint.set_interrupted(True)):
                    try:
                        call()
                    except RuntimeError as refusal:
                        observed.append(str(refusal))
            except BaseException as error:
                failures.append(error)
            finally:
                _native._test_unblock()

        worker = threading.Thread(target=other)
        worker.start()
        try:
            endpoint.close()
        finally:
            _native._test_unblock()
            worker.join(5)
            endpoint.close()
        self.assertFalse(worker.is_alive())
        self.assertEqual(failures, [])
        # Close in progress is NOT closed: the refusal must say so, twice, while
        # closed reads False; only after release does it read "closed".
        self.assertEqual(observed, [False, "endpoint close is in progress", "endpoint close is in progress"])
        self.assertTrue(endpoint.closed)
        with self.assertRaises(RuntimeError) as released:
            endpoint.wake()
        self.assertEqual(str(released.exception), "endpoint is closed")
        self.assertEqual(_native._test_counts(), (1, 1, 1))

    def test_terminal_detail_is_not_narrowed_or_signed(self):
        with self.connect() as endpoint:
            _native._test_terminal(3, 2**64 - 67843)
            terminal = endpoint.terminal
            self.assertEqual(terminal.reason, 3)
            self.assertEqual(terminal.detail_code, 2**64 - 67843)
            _native._test_terminal(197, 2**64 - 1)
            self.assertEqual(endpoint.terminal.reason, 197)
            self.assertEqual(endpoint.terminal.detail_code, 2**64 - 1)

    def test_snapshot_preserves_unknown_state_and_version(self):
        with self.connect() as endpoint:
            self.assertEqual(endpoint.state, moq5.EndpointState.CONNECTING)
            self.assertEqual(endpoint.negotiated_version, 0)
            _native._test_snapshot(197, 2**32 - 1)
            self.assertEqual(endpoint.state, 197)
            self.assertEqual(endpoint.negotiated_version, 2**32 - 1)

    def test_incomplete_terminal_snapshot_is_not_adopted(self):
        with self.connect() as endpoint:
            _native._test_terminal_size(4)
            with self.assertRaises(moq5.MoqError) as caught:
                _ = endpoint.terminal
            self.assertEqual((caught.exception.code, caught.exception.operation), (-11, "terminal"))

    def test_wait_outcomes_are_not_exceptions_or_eof_conflations(self):
        with self.connect() as endpoint:
            for code, expected in ((0, moq5.WaitResult.WOKEN), (1, moq5.WaitResult.TIMED_OUT),
                                   (-13, moq5.WaitResult.INTERRUPTED), (-4, moq5.WaitResult.CLOSED)):
                _native._test_wait_result(code)
                self.assertEqual(endpoint.wait(0), expected)
            _native._test_wait_result(-99)
            with self.assertRaises(moq5.MoqError) as caught:
                endpoint.wait(0)
            self.assertEqual(caught.exception.code, -99)

    def test_wait_and_interrupt_do_not_coerce_bad_values(self):
        with self.connect() as endpoint:
            for value in (True, -1, 2**63, 0.5, "0"):
                with self.subTest(value=value):
                    with self.assertRaises((TypeError, ValueError)):
                        endpoint.wait(value)
            for value in (0, 1, None, "true"):
                with self.subTest(value=value):
                    with self.assertRaises(TypeError):
                        endpoint.set_interrupted(value)
            # INT64_MAX is accepted without actually waiting: the scripted clock
            # exhausts the deadline after ONE bounded slice. (Against the fake's
            # instant TIMED_OUT a real INT64_MAX wait would otherwise spin.)
            script = ScriptedWait([1], advance_ns_per_call=(2**63) * 1_000)
            with script.patched()[0], script.patched()[1]:
                self.assertEqual(endpoint.wait(2**63 - 1), moq5.WaitResult.TIMED_OUT)
            self.assertEqual(script.slices, [WAIT_SLICE_US])

    def test_wait_releases_gil_and_cross_thread_interrupt_is_retained(self):
        with self.connect() as endpoint:
            _native._test_block_wait()
            failures = []

            def interrupter():
                try:
                    _native._test_wait_entered()
                    endpoint.set_interrupted(True)
                except BaseException as error:
                    failures.append(error)

            worker = threading.Thread(target=interrupter)
            worker.start()
            try:
                self.assertEqual(endpoint.wait(1_000_000), moq5.WaitResult.INTERRUPTED)
                self.assertEqual(endpoint.wait(0), moq5.WaitResult.INTERRUPTED)
                endpoint.set_interrupted(False)
                self.assertEqual(endpoint.wait(0), moq5.WaitResult.TIMED_OUT)
            finally:
                _native._test_unblock()
                worker.join(5)
            self.assertFalse(worker.is_alive(), "owned interrupter did not settle")
            self.assertEqual(failures, [])

    def test_ordinary_calls_reject_a_foreign_thread_before_native_effect(self):
        with self.connect() as endpoint:
            results = []

            def other():
                for call in (lambda: endpoint.state, lambda: endpoint.wait(0), endpoint.close):
                    try:
                        call()
                    except RuntimeError:
                        results.append("refused")

            thread = threading.Thread(target=other)
            thread.start()
            thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(results, ["refused"] * 3)
            self.assertFalse(endpoint.closed)
            self.assertEqual(_native._test_counts(), (1, 0, 0))

    def test_bad_values_have_no_native_effect(self):
        for value in (True, -1, 2**63, 1.25, "5"):
            with self.subTest(value=value):
                with self.assertRaises((TypeError, ValueError)):
                    self.connect(handshake_timeout_us=value)
        for field in ("url", "ca_file", "sni", "wt_path"):
            kwargs = {"url": "moqt://fixture.invalid", field: "before\0after"}
            with self.subTest(field=field):
                with self.assertRaises((TypeError, ValueError)):
                    moq5.Endpoint.connect(moq5.EndpointConfig(**kwargs))
        self.assertEqual(_native._test_counts(), (0, 0, 0))

    def test_private_boundary_validates_before_effects_too(self):
        args = [b"moqt://fixture.invalid", 0, 0, (18,), b"", b"", b"", 0, 0]
        for index, value in ((0, b"a\0b"), (1, True), (3, (True,)), (8, 2**64)):
            bad = args.copy()
            bad[index] = value
            with self.subTest(index=index):
                with self.assertRaises((TypeError, ValueError)):
                    _native.connect(*bad)
        self.assertEqual(_native._test_counts(), (0, 0, 0))

    def test_forgotten_close_warns_and_releases_exactly_once(self):
        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter("always", ResourceWarning)
            endpoint = self.connect()
            del endpoint
            gc.collect()
        self.assertEqual(len(seen), 1)
        self.assertIs(seen[0].category, ResourceWarning)
        self.assertEqual(_native._test_counts(), (1, 1, 1))

    def test_foreign_thread_finalization_releases_unattached_owner(self):
        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter("always", ResourceWarning)
            owners = [self.connect()]

            def release():
                endpoint = owners.pop()
                del endpoint

            worker = threading.Thread(target=release)
            worker.start()
            worker.join(5)
            self.assertFalse(worker.is_alive())
        self.assertEqual([warning.category for warning in seen], [ResourceWarning])
        self.assertEqual(_native._test_counts(), (1, 1, 1))

    def test_with_body_failure_is_preserved(self):
        error = LookupError("application failure")
        with self.assertRaises(LookupError) as caught:
            with self.connect():
                raise error
        self.assertIs(caught.exception, error)
        self.assertEqual(_native._test_counts(), (1, 1, 1))

    def test_with_body_and_cleanup_failure_preserves_both(self):
        endpoint = self.connect()
        original = KeyboardInterrupt("body interrupted")
        try:
            _native._test_stop_result(-5)
            with self.assertRaises(BaseExceptionGroup) as caught:
                with endpoint:
                    raise original
            self.assertEqual(len(caught.exception.exceptions), 2)
            self.assertIs(caught.exception.exceptions[0], original)
            cleanup = caught.exception.exceptions[1]
            self.assertIsInstance(cleanup, moq5.MoqError)
            self.assertEqual((cleanup.code, cleanup.operation), (-5, "close"))
            self.assertFalse(endpoint.closed)
            self.assertEqual(_native._test_counts(), (1, 1, 0))
        finally:
            _native._test_stop_result(0)
            endpoint.close()
        self.assertEqual(_native._test_counts(), (1, 2, 1))

    # ---- F1: the public wait is a sequence of bounded native waits -----------

    def scripted(self, script):
        clock, native = script.patched()
        clock.start()
        native.start()
        self.addCleanup(native.stop)
        self.addCleanup(clock.stop)

    def test_wait_is_sliced_to_the_bound_and_never_resets_its_budget(self):
        with self.connect() as endpoint:
            # 1 s budget, 250 ms per native call, no overhead: four full slices.
            script = ScriptedWait([1, 1, 1, 1], advance_ns_per_call=250_000_000)
            self.scripted(script)
            self.assertEqual(endpoint.wait(1_000_000), moq5.WaitResult.TIMED_OUT)
            self.assertEqual(script.slices, [WAIT_SLICE_US] * 4)

    def test_wait_accounts_for_elapsed_overhead_between_native_calls(self):
        with self.connect() as endpoint:
            # 1 s budget, each native call takes 300 ms of wall time: the
            # remaining budget shrinks by what actually elapsed, and the last
            # slice is the exact remainder -- a timed-out slice does not
            # restart the budget and does not return early on its own.
            script = ScriptedWait([1, 1, 1, 1], advance_ns_per_call=300_000_000)
            self.scripted(script)
            self.assertEqual(endpoint.wait(1_000_000), moq5.WaitResult.TIMED_OUT)
            self.assertEqual(script.slices, [250_000, 250_000, 250_000, 100_000])

    def test_wait_rounds_a_sub_microsecond_remainder_up(self):
        with self.connect() as endpoint:
            # Budget 250,002 us; the first slice takes 250,000.5 us of wall time,
            # leaving 1.5 us, which must round UP to a 2 us native wait -- never
            # down to 1, which would return before the caller's deadline.
            script = ScriptedWait([1, 1], advance_ns_per_call=250_000_500)
            self.scripted(script)
            self.assertEqual(endpoint.wait(250_002), moq5.WaitResult.TIMED_OUT)
            self.assertEqual(script.slices, [250_000, 2])

    def test_wait_zero_is_exactly_one_nonblocking_native_poll(self):
        with self.connect() as endpoint:
            script = ScriptedWait([1])
            self.scripted(script)
            self.assertEqual(endpoint.wait(0), moq5.WaitResult.TIMED_OUT)
            self.assertEqual(script.slices, [0])

    def test_wait_returns_every_non_timeout_outcome_from_the_first_slice(self):
        for code, expected in ((0, moq5.WaitResult.WOKEN), (-13, moq5.WaitResult.INTERRUPTED),
                               (-4, moq5.WaitResult.CLOSED)):
            with self.subTest(code=code), self.connect() as endpoint:
                script = ScriptedWait([code, 1, 1, 1, 1])
                self.scripted(script)
                self.assertEqual(endpoint.wait(1_000_000), expected)
                self.assertEqual(script.slices, [WAIT_SLICE_US], "returned on the first slice, not swallowed")

    def test_wait_propagates_a_native_failure_and_a_pending_interrupt_from_any_slice(self):
        with self.connect() as endpoint:
            script = ScriptedWait([1, _native.Error(-99, "wait", "internal error")],
                                  advance_ns_per_call=250_000_000)
            self.scripted(script)
            with self.assertRaises(moq5.MoqError) as caught:
                endpoint.wait(1_000_000)
            self.assertEqual((caught.exception.code, caught.exception.operation), (-99, "wait"))
            self.assertEqual(script.slices, [WAIT_SLICE_US] * 2)
        with self.connect() as endpoint:
            script = ScriptedWait([1, KeyboardInterrupt()], advance_ns_per_call=250_000_000)
            self.scripted(script)
            with self.assertRaises(KeyboardInterrupt):
                endpoint.wait(1_000_000)
            self.assertEqual(script.slices, [WAIT_SLICE_US] * 2)

    def test_wait_never_passes_the_whole_timeout_to_one_native_call(self):
        # The named oracle a single unbounded native call must fail.
        with self.connect() as endpoint:
            script = ScriptedWait([1, 1, 1, 1], advance_ns_per_call=250_000_000)
            self.scripted(script)
            endpoint.wait(1_000_000)
            self.assertTrue(all(us <= WAIT_SLICE_US for us in script.slices),
                            f"a native wait exceeded the bound: {script.slices}")
            self.assertNotEqual(script.slices, [1_000_000], "the whole timeout reached one native call")

    # ---- isolated children: explicit policy, no unplanned diagnostics --------

    CHILD_PRELUDE = textwrap.dedent(
        """
        import gc, json, os, signal, sys, threading, time, traceback, unittest
        sys.path.insert(0, sys.argv[1])
        import moq5
        from moq5 import _native
        _native._test_reset()
        # Copies SCALARS only: the object being finalized must never be retained.
        seen = []
        def hook(unraisable):
            seen.append({
                "type": unraisable.exc_type.__name__ if unraisable.exc_type else None,
                "message": str(unraisable.exc_value) if unraisable.exc_value else None,
                "object_is_none": unraisable.object is None,
            })
        sys.unraisablehook = hook
        def connect():
            return moq5.Endpoint.connect(moq5.EndpointConfig(url="moqt://fixture.invalid"))
        """
    )

    def run_child(self, body, *, timeout=15):
        """Run BODY under -I -B -W error -X faulthandler; return CompletedProcess.

        Policy is passed explicitly: a child does not inherit -W error, and -I
        ignores PYTHONWARNINGS. dyld strips DYLD_INSERT_LIBRARIES, so a sanitizer
        lane names its runtime in MOQ5_TEST_CHILD_DYLD_INSERT_LIBRARIES (test-only)."""
        package = str(Path(moq5.__file__).resolve().parent.parent)
        env = os.environ.copy()
        insert = env.pop("MOQ5_TEST_CHILD_DYLD_INSERT_LIBRARIES", None)
        if insert:
            env["DYLD_INSERT_LIBRARIES"] = insert
        return subprocess.run(
            [sys.executable, "-I", "-B", "-W", "error", "-X", "faulthandler", "-c",
             self.CHILD_PRELUDE + textwrap.dedent(body), package],
            capture_output=True, text=True, timeout=timeout, check=False, env=env,
        )

    def assert_child(self, completed, *, returncode=0):
        """A successful child is silent: any stderr is an unplanned diagnostic,
        and its ENTIRE stdout is the one JSON report -- a leading diagnostic or
        trailing extra output is rejected, never discarded."""
        self.assertEqual(completed.returncode, returncode, completed.stdout + completed.stderr)
        self.assertEqual(completed.stderr, "", f"unplanned child diagnostics: {completed.stderr}")
        return self.parse_report(completed.stdout)

    def parse_report(self, stdout):
        try:
            report = json.loads(stdout)
        except json.JSONDecodeError as error:
            raise AssertionError(f"child stdout is not exactly one JSON report: {stdout!r}") from error
        self.assertIsInstance(report, dict, f"child report is not an object: {stdout!r}")
        return report

    SIGINT_CHILD = """
        endpoint = connect()
        _native._test_block_wait()
        def deliver():
            _native._test_wait_entered()
            os.kill(os.getpid(), signal.SIGINT)
        worker = threading.Thread(target=deliver)
        worker.start()
        started = time.monotonic()
        try:
            result = endpoint.wait(20_000_000)
            outcome = "returned " + result.name
        except KeyboardInterrupt:
            outcome = "KeyboardInterrupt"
        worker.join(5)
        endpoint.close()
        print(json.dumps({"outcome": outcome, "worker_alive": worker.is_alive(),
                          "watchdog_fired": _native._test_watchdog_fired(),
                          "counts": _native._test_counts(), "seen": seen}))
    """

    def check_signal_child(self, body):
        report = self.assert_child(self.run_child(body))
        self.assertEqual(report["outcome"], "KeyboardInterrupt", report)
        self.assertFalse(report["watchdog_fired"], "the fixture watchdog must not have released the wait")
        self.assertFalse(report["worker_alive"])
        self.assertEqual(report["counts"], [1, 1, 1])
        self.assertEqual(report["seen"], [], "no unraisable diagnostic in a clean child")

    def test_pending_sigint_interrupts_a_blocked_public_wait_in_a_subprocess(self):
        # Real signal, compiled bridge, isolated child, explicit -W error. The
        # fake honours each slice; nobody unblocks it and nobody sets the
        # native latch. The discriminator is the fixture's own word, not time.
        self.check_signal_child(self.SIGINT_CHILD)

    def run_child_recorded(self, body):
        """The child result itself, so a negative control can prove what the
        child actually did before crediting the gate's refusal."""
        return self.run_child(body)

    def test_a_child_warning_fails_the_signal_gate(self):
        # The prefix is composed onto the NORMALIZED child source so it is the
        # first statement of a valid program, not an indentation error.
        body = "import warnings; warnings.warn('UNPLANNED_CHILD_WARNING', RuntimeWarning)\n" + textwrap.dedent(self.SIGINT_CHILD)
        completed = self.run_child_recorded(body)
        # What the child did: -W error turned the named warning into a failure.
        self.assertEqual(completed.returncode, 1, completed.stdout + completed.stderr)
        self.assertIn("RuntimeWarning: UNPLANNED_CHILD_WARNING", completed.stderr)
        self.assertNotIn("IndentationError", completed.stderr)
        self.assertNotIn("SyntaxError", completed.stderr)
        self.assertEqual(completed.stdout, "", "the warning stopped the child before any report")
        # Only then: the gate refuses it.
        with self.assertRaises(AssertionError):
            self.assert_child(completed)

    def test_a_child_stderr_diagnostic_at_rc_zero_fails_the_signal_gate(self):
        body = "print('UNPLANNED_CHILD_DIAGNOSTIC', file=sys.stderr)\n" + textwrap.dedent(self.SIGINT_CHILD)
        completed = self.run_child_recorded(body)
        # What the child did: it succeeded, produced the otherwise-valid signal
        # report, AND wrote exactly the marker to stderr.
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(completed.stderr, "UNPLANNED_CHILD_DIAGNOSTIC\n")
        report = self.parse_report(completed.stdout)
        self.assertEqual(report["outcome"], "KeyboardInterrupt", "the report is otherwise valid")
        # Only then: the gate refuses it on the stderr diagnostic alone.
        with self.assertRaises(AssertionError) as refused:
            self.assert_child(completed)
        self.assertIn("unplanned child diagnostics", str(refused.exception))

    def test_a_leading_diagnostic_on_stdout_is_not_discarded(self):
        body = "print('LEADING_STDOUT_DIAGNOSTIC')\n" + textwrap.dedent(self.SIGINT_CHILD)
        completed = self.run_child_recorded(body)
        self.assertEqual(completed.returncode, 0)
        self.assertEqual(completed.stderr, "")
        with self.assertRaises(AssertionError) as refused:
            self.assert_child(completed)
        self.assertIn("not exactly one JSON report", str(refused.exception))

    # ---- F5: finalization never exposes the dying capsule ----------------------

    def test_finalizer_survives_a_leak_inside_a_failing_assert_raises(self):
        report = self.assert_child(self.run_child("""
            import io
            class T(unittest.TestCase):
                def test_leak(self):
                    with self.assertRaises(moq5.MoqError):
                        connect()   # succeeds: the temporary is finalized as the assertion fails
            stream = io.StringIO()
            result = unittest.TextTestRunner(stream=stream, verbosity=0).run(
                unittest.defaultTestLoader.loadTestsFromTestCase(T))
            verdict = (len(result.failures), len(result.errors),
                       "AssertionError: MoqError not raised" in result.failures[0][1] if result.failures else None)
            # The runner's own resources are settled BEFORE the verdict, so the
            # inventory below is complete, not a snapshot taken too early.
            del result
            stream.close()
            gc.collect()
            print(json.dumps({"failures": verdict[0], "errors": verdict[1], "formatted": verdict[2],
                              "seen": seen, "counts": _native._test_counts()}))
        """))
        self.assertEqual((report["failures"], report["errors"]), (1, 0), "exactly the intended inner failure")
        self.assertTrue(report["formatted"], "the inner failure's traceback was formatted normally")
        self.assertEqual(report["counts"], [1, 1, 1], "the leaked owner was stopped and destroyed once")
        self.assertEqual(report["seen"], [{"type": "ResourceWarning",
                                           "message": "unclosed moq5.Endpoint; use close() or a context manager",
                                           "object_is_none": True}],
                         "the diagnostic occurs exactly once, with NO dying object as its context")

    def test_forgotten_close_under_warnings_as_error_counts_once(self):
        report = self.assert_child(self.run_child("""
            endpoint = connect()
            del endpoint
            gc.collect()
            print(json.dumps({"seen": seen, "counts": _native._test_counts()}))
        """))
        self.assertEqual(report["counts"], [1, 1, 1])
        self.assertEqual([(e["type"], e["object_is_none"]) for e in report["seen"]], [("ResourceWarning", True)])

    def test_body_exception_identity_and_traceback_survive_finalization(self):
        report = self.assert_child(self.run_child("""
            declared = LookupError("application failure")
            replacement = LookupError("application failure")   # same type and message, a different object
            def body():
                endpoint = connect()
                try:
                    raise declared
                finally:
                    del endpoint   # released DURING propagation: the finalizer runs under the in-flight exception
            try:
                body()
            except LookupError as caught:
                counts_when_caught = _native._test_counts()
                formatted = "".join(traceback.format_exception(caught))
                ident = {"is_declared": caught is declared, "replacement_is_declared": replacement is declared,
                         "type": type(caught).__name__, "message": str(caught),
                         "formatted": "application failure" in formatted and "in body" in formatted}
            print(json.dumps({"ident": ident, "counts_when_caught": counts_when_caught,
                              "seen": seen, "counts": _native._test_counts()}))
        """))
        self.assertTrue(report["ident"]["is_declared"], "the caught exception IS the declared object")
        self.assertFalse(report["ident"]["replacement_is_declared"], "a same-type, same-message object is not it")
        self.assertEqual((report["ident"]["type"], report["ident"]["message"]), ("LookupError", "application failure"))
        self.assertTrue(report["ident"]["formatted"])
        self.assertEqual(report["counts_when_caught"], [1, 1, 1], "cleanup ran during propagation, before the handler observed it")
        self.assertEqual([(e["type"], e["object_is_none"]) for e in report["seen"]], [("ResourceWarning", True)])
        self.assertEqual(report["counts"], [1, 1, 1])

    def test_refused_stop_diagnostic_in_finalizer_never_destroys(self):
        report = self.assert_child(self.run_child("""
            _native._test_stop_result(-5)
            endpoint = connect()
            del endpoint
            gc.collect()
            print(json.dumps({"seen": seen, "counts": _native._test_counts()}))
        """))
        self.assertEqual(report["counts"], [1, 1, 0], "a refused stop is reported and nothing is destroyed")
        self.assertEqual(report["seen"], [{"type": "RuntimeError",
                                           "message": "native endpoint stop refused during finalization; not destroyed",
                                           "object_is_none": True}])

    # ---- expired-first: every valid public wait makes one native observation ---

    def expired(self, endpoint, timeout_us=1):
        with mock.patch.object(_endpoint, "_monotonic_ns", side_effect=(0, 2_000)):
            return endpoint.wait(timeout_us)

    def test_expired_budget_still_refuses_a_closed_handle(self):
        endpoint = self.connect()
        endpoint.close()
        with self.assertRaises(RuntimeError) as caught:
            self.expired(endpoint)
        self.assertEqual(str(caught.exception), "endpoint is closed")

    def test_expired_budget_still_refuses_a_foreign_owner(self):
        with self.connect() as endpoint:
            results = []
            worker = threading.Thread(target=lambda: results.append(
                type(self.assert_raises_from_thread(endpoint)).__name__))
            worker.start()
            worker.join(5)
            self.assertFalse(worker.is_alive())
            self.assertEqual(results, ["RuntimeError"])

    def assert_raises_from_thread(self, endpoint):
        try:
            self.expired(endpoint)
        except BaseException as error:
            return error
        return None

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only negative contract")
    def test_expired_budget_still_refuses_a_forked_handle(self):
        endpoint = self.connect()
        try:
            child = os.fork()
            if child == 0:
                try:
                    self.expired(endpoint)
                except RuntimeError:
                    os._exit(0)
                os._exit(3)
            _, status = os.waitpid(child, 0)
            self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        finally:
            endpoint.close()

    def test_expired_budget_still_observes_interrupt_and_exact_outcomes(self):
        with self.connect() as endpoint:
            endpoint.set_interrupted(True)
            self.assertEqual(endpoint.wait(0), moq5.WaitResult.INTERRUPTED, "control")
            self.assertEqual(self.expired(endpoint), moq5.WaitResult.INTERRUPTED)
            endpoint.set_interrupted(False)
            for code, expected in ((0, moq5.WaitResult.WOKEN), (-4, moq5.WaitResult.CLOSED)):
                _native._test_wait_result(code)
                self.assertEqual(self.expired(endpoint), expected)
            _native._test_wait_result(-99)
            with self.assertRaises(moq5.MoqError) as caught:
                self.expired(endpoint)
            self.assertEqual(caught.exception.code, -99)

    def test_expired_budget_is_one_zero_poll_and_an_ordinary_timeout(self):
        with self.connect() as endpoint:
            real = _native.wait
            calls = []

            def observed(handle, timeout_us):
                calls.append(timeout_us)
                return real(handle, timeout_us)

            with mock.patch.object(_native, "wait", observed):
                self.assertEqual(self.expired(endpoint), moq5.WaitResult.TIMED_OUT)
            self.assertEqual(calls, [0], "an already-expired budget still makes exactly one native poll")

    # ---- fixture: only ETIMEDOUT is an ordinary timeout ---------------------

    def test_an_unexpected_pthread_failure_is_a_named_fixture_failure(self):
        with self.connect() as endpoint:
            _native._test_block_wait()
            _native._test_force_pthread_error(22)   # EINVAL, as the fixture would see it
            with self.assertRaises(moq5.MoqError) as caught:
                endpoint.wait(1_000)
            self.assertEqual((caught.exception.code, caught.exception.operation), (FIXTURE_PTHREAD, "wait"))
            self.assertEqual(_native._test_pthread_error(), 22, "the actual error code is recorded")
            self.assertFalse(_native._test_watchdog_fired())
            _native._test_unblock()

    # ---- F2: fixture breakdown is never an injected native failure ------------

    def test_fixture_watchdog_is_its_own_named_failure(self):
        with self.connect() as endpoint:
            self.assertFalse(_native._test_watchdog_fired())
            _native._test_block_wait()
            _native._test_force_watchdog()
            with self.assertRaises(moq5.MoqError) as caught:
                endpoint.wait(1_000_000)
            self.assertEqual((caught.exception.code, caught.exception.operation), (FIXTURE_WATCHDOG, "wait"))
            self.assertTrue(_native._test_watchdog_fired(), "the fixture says its watchdog fired")
            _native._test_unblock()
        # And a forced stop watchdog is the same named identity, distinct from
        # any injected moq_result_t.
        endpoint = self.connect()
        try:
            _native._test_block_stop()
            _native._test_force_watchdog()
            with self.assertRaises(moq5.MoqError) as caught:
                endpoint.close()
            self.assertEqual((caught.exception.code, caught.exception.operation), (FIXTURE_WATCHDOG, "close"))
            self.assertFalse(endpoint.closed)
        finally:
            _native._test_unblock()
            endpoint.close()

    def test_injected_native_failure_is_not_the_fixture_watchdog(self):
        with self.connect() as endpoint:
            _native._test_wait_result(-99)
            with self.assertRaises(moq5.MoqError) as caught:
                endpoint.wait(0)
            self.assertEqual(caught.exception.code, -99)
            self.assertNotEqual(caught.exception.code, FIXTURE_WATCHDOG)
            self.assertFalse(_native._test_watchdog_fired())
            self.assertIsInstance(caught.exception.__cause__, _native.Error)

    # ---- F3: the fixture mirrors the SDK's offer capacity -------------------

    def test_offer_capacity_boundary_and_no_effect_refusal(self):
        with self.connect(versions=tuple(range(1, SDK_MAX_VERSIONS + 1))):
            self.assertEqual(len(_native._test_config()["versions"]), SDK_MAX_VERSIONS)
        self.assertEqual(_native._test_counts(), (1, 1, 1))
        _native._test_reset()
        try:
            accepted = self.connect(versions=tuple(range(1, SDK_MAX_VERSIONS + 2)))
        except moq5.MoqError as caught:
            self.assertEqual((caught.code, caught.operation), (-2, "connect"))
        else:
            # Close rather than leak: a leaked owner would finalize under
            # -W error, which is a separate finding, not this row's.
            accepted.close()
            self.fail(f"{SDK_MAX_VERSIONS + 1} offers were accepted; the SDK refuses them")
        self.assertEqual(_native._test_counts(), (0, 0, 0), "a refused offer has no native effect")

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only negative contract")
    def test_forked_handle_refused_without_native_cleanup(self):
        endpoint = self.connect()
        try:
            child = os.fork()
            if child == 0:
                try:
                    endpoint.close()
                except RuntimeError:
                    pass
                else:
                    os._exit(3)
                # Unlike _exit alone, this exercises the capsule's PID guard.
                with warnings.catch_warnings(record=True) as seen:
                    warnings.simplefilter("always", ResourceWarning)
                    del endpoint
                    gc.collect()
                if seen or _native._test_counts() != (1, 0, 0):
                    os._exit(4)
                os._exit(0)
            _, status = os.waitpid(child, 0)
            self.assertEqual(os.waitstatus_to_exitcode(status), 0)
            self.assertEqual(_native._test_counts(), (1, 0, 0))
        finally:
            endpoint.close()


if __name__ == "__main__":
    unittest.main()
