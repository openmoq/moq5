"""The player's pre-publisher phases, its supervisor-owned log sidecar, and the receipts that alone
settle it.

Offline only: owned stub children under the real supervisor, a test-owned install/prepare, and a
stand-in for the player's logcat stream that prints hand-written receipt lines (the sample's grammar,
not device output). Nothing here reaches adb.
"""
import sys
import threading
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import player_receipts                                       # noqa: E402
import run_android_media as m                                # noqa: E402
from test_route_dump import ONE_SUB, TWO_SUBS                # noqa: E402
from test_supervisor_route import (CLOSED, FAST, PRESENTED_FAIL, PRESENTED_PASS, Run, forwarded, ordering_violations,  # noqa: E402
                                   pairs)

EXITS = dict(m.OFFLINE_EXITS)


def names(rec):
    return [c.get("name") for c in rec["children"]]


class Receipts(unittest.TestCase):
    def test_the_sample_lines_parse_exactly(self):
        kind, fields = player_receipts.parse_line(PRESENTED_PASS)
        self.assertEqual(kind, "presented")
        self.assertEqual(list(fields), list(player_receipts.PRESENTED))
        self.assertEqual((fields["pass"], fields["released"], fields["indices"], fields["refusal"]), (True, 60, [0, 59], "none"))
        self.assertIsNone(player_receipts.presented_rejection(fields, 60))
        self.assertEqual(player_receipts.parse_line(CLOSED),
                         ("observer-closed", {"lateAfterFreeze": 0, "ignoredForeign": 0, "retiredAfterFreeze": 0}))
        self.assertIsNone(player_receipts.parse_line("--------- beginning of main"))

    def test_every_departure_is_named(self):
        cases = (
            ("keys out of order", PRESENTED_PASS.replace("latched=60 distinctTs=60", "distinctTs=60 latched=60")),
            ("a missing key", PRESENTED_PASS.replace(" preVideo=0", "")),
            ("a doubled space", PRESENTED_PASS.replace(" ordinal", "  ordinal")),
            ("a bool that is not one", PRESENTED_PASS.replace("pass=true", "pass=yes")),
            ("a negative count", CLOSED.replace("ignoredForeign=0", "ignoredForeign=-1")),
            ("indices with a space", PRESENTED_PASS.replace("[0,59]", "[0, 59]")),
            ("an extra key", CLOSED + " extra=1"),
            ("another log line", "MoqPlayer: something else"),
        )
        for label, line in cases:
            with self.subTest(label):
                with self.assertRaises(player_receipts.ReceiptError):
                    player_receipts.parse_line(line)

    def test_a_presented_receipt_that_does_not_accept_is_named(self):
        _, fields = player_receipts.parse_line(PRESENTED_FAIL)
        self.assertEqual(player_receipts.presented_rejection(fields, 60),
                         "pass=False, expected True; refusal='released-mismatch', expected 'none'")
        _, fields = player_receipts.parse_line(PRESENTED_PASS.replace("ordinal=1", "ordinal=2"))
        self.assertEqual(player_receipts.presented_rejection(fields, 60), "ordinal=2, expected 1")


def presented(**changes):
    """PRESENTED_PASS with the named fields replaced (values written as the sample writes them)."""
    line = PRESENTED_PASS
    for key, value in changes.items():
        _, fields = player_receipts.parse_line(line)
        old = f"{key}=[{','.join(map(str, fields[key]))}]" if key == "indices" else \
            f"{key}={str(fields[key]).lower() if isinstance(fields[key], bool) else fields[key]}"
        assert line.count(old) == 1, old
        line = line.replace(old, f"{key}={value}")
    return line


class ReceiptEvidence(unittest.TestCase):
    """A `presented:` receipt is judged by the evidence it reports, not by its own `pass`: every
    condition the recorder's freeze requires for a pass must visibly hold in the reported fields."""

    CONTRADICTION = dict(latched=0, distinctTs=0, indices="[]", leaseId=0)
    cases = (
        ("no picture evidence at all (pass=true)", CONTRADICTION,
         "leaseId=0, expected >= 1; indices=[], expected at least 2; indices=[] do not end at the final index 59; "
         "distinctTs=0, expected >= 2"),
        ("an unowned lease", dict(leaseId=0), "leaseId=0, expected >= 1"),
        ("one index", dict(indices="[59]"), "indices=[59], expected at least 2"),
        ("the final index missing", dict(indices="[0,58]"), "indices=[0, 58] do not end at the final index 59"),
        ("an index outside the run", dict(indices="[0,59,60]"), "indices=[0, 59, 60] do not end at the final index 59"),
        ("indices not strictly increasing", dict(indices="[0,59,59]"), "indices=[0, 59, 59] are not strictly increasing"),
        ("one timestamp", dict(distinctTs=1), "distinctTs=1, expected >= 2"),
        ("too few latches", dict(latched=1), "latched=1, expected >= preVideo 0 + 60 valid"),
        ("latches not covering pre-video", dict(preVideo=5, latched=6), "latched=6, expected >= preVideo 5 + 60 valid"),
        ("a geometry refusal count", dict(syncInvalid=1), "syncInvalid=1, expected 0"),
        ("a capture refusal count", dict(captureRefusals=1), "captureRefusals=1, expected 0"),
        ("unreadable after pre-video", dict(unreadable=3, preVideo=2, latched=64), "unreadable=3 exceeds preVideo=2"),
        ("more latches than the recorder holds", dict(latched=513), "latched=513 exceeds the recorder's capacity 512"),
    )

    def test_each_evidence_class_is_judged_independently_of_pass(self):
        for label, changes, rejection in self.cases:
            with self.subTest(label):
                _, fields = player_receipts.parse_line(presented(**changes))
                self.assertIs(fields["pass"], True)
                self.assertEqual(player_receipts.presented_rejection(fields, 60), rejection)

    def test_an_over_capacity_pass_never_settles_the_player(self):
        rec = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS], receipts=(presented(latched=513), CLOSED)).supervise()
        violation = "player log: presented receipt does not accept: latched=513 exceeds the recorder's capacity 512"
        self.assertEqual(rec["primary_error"], f"done: refused: {violation}")
        self.assertEqual(rec["player_log_violations"][0], violation)
        self.assertNotIn("player_settled", [event for _t, event in rec["facts"]])
        self.assertNotIn(("control", "done"), pairs(rec))

    def test_a_self_contradictory_pass_never_settles_the_player(self):
        receipt = presented(**self.CONTRADICTION)
        rec = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS], receipts=(receipt, CLOSED)).supervise()
        violation = f"player log: presented receipt does not accept: {self.cases[0][2]}"
        self.assertEqual(rec["primary_error"], f"done: refused: {violation}")
        self.assertEqual(rec["player_log_violations"][0], violation)
        self.assertNotIn("player_settled", [event for _t, event in rec["facts"]])
        self.assertNotIn(("control", "done"), pairs(rec))
        self.assertIs(rec["verdict"]["ok"], False)


class Phases(unittest.TestCase):
    """Install precedes the relay; prepare and the sidecar follow relay readiness; every failure is
    named before the publisher exists, with the children actually spawned (and only those) settled."""

    def check_failed_before_publisher(self, rec, primary, children):
        self.assertEqual(rec["primary_error"], primary)
        self.assertEqual(names(rec), children)
        self.assertTrue(all(c.get("readers_settled") and c.get("exit") is not None for c in rec["children"]))
        self.assertNotIn("publisher", names(rec))
        self.assertEqual(forwarded(rec), [])
        self.assertIs(rec["threads_settled"], True)
        self.assertIs(rec["verdict"]["ok"], False)
        self.assertIn(f"primary: {primary}", rec["verdict"]["failures"])
        self.assertIn(f"child inventory {sorted(children)} != {sorted(EXITS)}", rec["verdict"]["failures"])
        self.assertIn(("control", f"stopped: {primary}"), pairs(rec))

    def test_install_failure_precedes_every_child(self):
        run = Run(self, [ONE_SUB, TWO_SUBS], install="fails")
        rec = run.supervise()
        self.check_failed_before_publisher(rec, "player install: org.openmoq.moq5.samples.player present before install", [])
        self.assertEqual(run.phase_calls, [("install", FAST.player_install)])
        self.assertEqual([w for k, w in pairs(rec) if k == "player_install"],
                         ["begin", "failed: org.openmoq.moq5.samples.player present before install"])

    def test_an_install_that_raises_is_named(self):
        rec = Run(self, [ONE_SUB, TWO_SUBS], install="raises").supervise()
        self.check_failed_before_publisher(rec, "player install: raised OSError('adb: no devices/emulators found')", [])

    def test_prepare_failure_follows_the_relay_and_precedes_the_publisher(self):
        run = Run(self, [ONE_SUB, TWO_SUBS], prepare="fails")
        rec = run.supervise()
        self.check_failed_before_publisher(rec, f"player prepare: Start not enabled within {FAST.player_prepare}s", ["relay"])
        self.assertEqual(run.phase_calls, [("install", FAST.player_install), ("prepare", FAST.player_prepare)])

    def test_a_prepare_that_raises_is_named(self):
        rec = Run(self, [ONE_SUB, TWO_SUBS], prepare="raises").supervise()
        self.check_failed_before_publisher(rec, "player prepare: raised TimeoutError('uiautomator dump timed out')", ["relay"])

    def test_a_prepare_that_overruns_its_bound_is_named(self):
        import dataclasses
        tight = dataclasses.replace(FAST, player_prepare=0.2)
        rec = Run(self, [ONE_SUB, TWO_SUBS], prepare="overruns").supervise(tight)
        self.assertTrue((rec["primary_error"] or "").startswith("player prepare: took "), rec["primary_error"])
        self.assertTrue(rec["primary_error"].endswith("s, over its 0.2s bound"), rec["primary_error"])
        self.assertEqual(names(rec), ["relay"])

    def test_a_sidecar_that_cannot_start_is_named_before_the_publisher(self):
        rec = Run(self, [ONE_SUB, TWO_SUBS], log="spawn-fails").supervise()
        self.assertTrue((rec["primary_error"] or "").startswith("player log: spawn failed: FileNotFoundError("), rec["primary_error"])
        self.assertEqual(names(rec), ["relay"])
        self.assertNotIn("publisher", names(rec))


class Sidecar(unittest.TestCase):
    """The player-log child is owned, watched from its start, and every fault is a causal refusal."""

    def test_the_full_run_settles_only_on_both_receipts(self):
        rec = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS]).supervise()
        self.assertIsNone(rec["primary_error"])
        self.assertEqual(forwarded(rec), list(m.CONTROLS))
        self.assertEqual(rec["verdict"], {"ok": True, "failures": []})
        self.assertIn("player-log", names(rec))
        self.assertEqual(ordering_violations(rec["sequence"]), [])
        p = pairs(rec)
        order = [p.index(x) for x in (("player_install", "done"), ("player_prepare", "done"), ("player_log", "started"),
                                      ("player", "player_stopped"), ("player_log", "presented"),
                                      ("player_log", "observer-closed"), ("player", "player_settled"), ("control", "done"))]
        self.assertEqual(order, sorted(order), p)
        self.assertLess(p.index(("player_log", "started")), p.index(("host", "go")))

    def test_a_probe_cannot_settle_the_player(self):
        rec = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS], player="reports-settled").supervise()
        self.assertEqual(rec["primary_error"],
                         "go: refused: the player probe reported player_settled; only its receipts settle the player")
        self.assertNotIn("player_settled", [event for _t, event in rec["facts"]])    # the gate never recorded it
        self.assertEqual(forwarded(rec), [])

    def test_a_sidecar_that_exits_early_is_a_causal_refusal(self):
        rec = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS], log="exit-early").supervise()
        self.assertEqual(rec["primary_error"], "go: refused: player log exited (rc 3) before the run stopped")
        self.assertEqual(forwarded(rec), [])

    def test_a_missing_close_receipt_leaves_the_player_unsettled(self):
        rec = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS], receipts=(PRESENTED_PASS,)).supervise()
        self.assertEqual(rec["primary_error"], f"done: barrier(s) player_settled not observed within {FAST.done}s of publisher_complete")
        self.assertIn(("player_log", "presented"), pairs(rec))
        self.assertNotIn(("player", "player_settled"), pairs(rec))

    def test_the_ui_line_alone_never_settles(self):
        rec = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS], receipts=()).supervise()
        self.assertEqual(rec["primary_error"], f"done: barrier(s) player_settled not observed within {FAST.done}s of publisher_complete")
        self.assertIn(("player", "player_stopped"), pairs(rec))

    def test_receipt_faults_are_named_and_fail_the_verdict(self):
        malformed = f"player log: presented keys are ['pass'], expected {list(player_receipts.PRESENTED)}"
        cases = (   # label, receipts, the recorded violation, whether it must precede `done`
            ("a failing presentation", (PRESENTED_FAIL, CLOSED),
             "player log: presented receipt does not accept: pass=False, expected True; "
             "refusal='released-mismatch', expected 'none'", True),
            ("presented twice", (PRESENTED_PASS, PRESENTED_PASS, CLOSED), "player log: a second presented receipt", True),
            ("closed first", (CLOSED, PRESENTED_PASS), "player log: observer-closed before presented", True),
            ("a malformed line", ("presented: pass=true",), malformed, True),
            ("closed twice", (PRESENTED_PASS, CLOSED, CLOSED), "player log: a second observer-closed receipt", False),
        )
        for label, receipts, violation, before_done in cases:
            with self.subTest(label):
                rec = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS], receipts=receipts).supervise()
                self.assertEqual(rec["player_log_violations"], [violation])
                self.assertIn("check player_log_exact is False", rec["verdict"]["failures"])
                if before_done:
                    self.assertEqual(rec["primary_error"], f"done: refused: {violation}")
                    self.assertNotIn(("control", "done"), pairs(rec))

    def test_a_parser_callback_failure_is_named_not_swallowed(self):
        run = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS])

        def explode(line):
            if line.startswith("presented:"):
                raise RuntimeError("callback fault")
        run.actors.hooks["player_log_line"] = explode
        rec = run.supervise()
        violation = f"player log: RuntimeError('callback fault') on {PRESENTED_PASS!r}"
        self.assertEqual(rec["primary_error"], f"done: refused: {violation}")
        # the presented receipt was never judged, so the close that follows is refused in turn
        self.assertEqual(rec["player_log_violations"], [violation, "player log: observer-closed before presented"])
        player_log = next(c for c in rec["children"] if c.get("name") == "player-log")
        self.assertEqual(player_log["reader_errors"], [])

    def test_a_stuck_sidecar_reader_is_reported_at_settlement(self):
        run, held, release = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS]), threading.Event(), threading.Event()

        def stick(line):
            if line == CLOSED:
                held.set()
                release.wait(8 * FAST.settle)               # released by this test once supervise() has returned
        run.actors.hooks["player_log_line"] = stick
        self.addCleanup(release.set)
        rec = run.supervise()
        release.set()
        self.assertTrue(held.is_set())
        player_log = next(c for c in rec["children"] if c.get("name") == "player-log")
        self.assertIs(player_log["readers_settled"], False)
        self.assertIs(rec["checks"]["all_children_settled"], False)
        self.assertIn("player-log: readers not settled", rec["verdict"]["failures"])

    def test_a_sidecar_settle_failure_is_reported(self):
        class FailingSettle(m.Child):
            def settle(self, timeout, expected_rc=0, signal_first=None):
                if self.name == "player-log":
                    super().settle(timeout, expected_rc, signal_first)
                    raise OSError("settle fault")
                return super().settle(timeout, expected_rc, signal_first)
        original = m.Child
        m.Child = FailingSettle
        self.addCleanup(setattr, m, "Child", original)
        rec = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS]).supervise()
        entry = next(c for c in rec["children"] if c.get("name") == "player-log")
        self.assertEqual(entry.get("settle_error"), "OSError('settle fault')")
        self.assertIn("player-log: settle error OSError('settle fault')", rec["verdict"]["failures"])


class SidecarExit(unittest.TestCase):
    """The real receipt stream is `adb logcat`, which does not handle SIGTERM: it ends at the signal,
    status -15. The offline stubs handle it and exit 0. Each declaration must match its process."""

    def settled(self, program, signal_first=True):
        """Runs `program`, which prints `armed` once its SIGTERM disposition is in place, and settles it
        with SIGTERM only after that line (or after it exits by itself)."""
        import signal as sig
        armed = threading.Event()
        child = m.Child("player-log", [sys.executable, "-I", "-c", program], {"PATH": "/usr/bin:/bin"}, str(HERE),
                        on_line=lambda line: armed.set() if line == "armed" else None)
        self.addCleanup(lambda: child.proc.poll() is None and child.proc.kill())
        if signal_first:
            self.assertTrue(armed.wait(5), "the child never reported its signal disposition")
        else:
            child.proc.wait(5)
        return child.settle(5, signal_first=sig.SIGTERM)

    def verdict_failures(self, entry, declared):
        failures = m.final_verdict({"children": [entry]}, {"player-log": declared})["failures"]
        return [f for f in failures if f.startswith("player-log:")]

    def test_an_unhandled_sigterm_ends_at_minus_15_and_is_declared_so_for_the_real_stream(self):
        entry = self.settled("import time; print('armed', flush=True); time.sleep(60)")
        self.assertEqual((entry["how"], entry["exit"]), ("SIGTERM", -15))
        self.assertEqual(m.ADB_LOGCAT_EXIT, ("SIGTERM", -15))
        self.assertEqual(m.LIVE_EXITS["player-log"], m.ADB_LOGCAT_EXIT)
        self.assertEqual(self.verdict_failures(entry, m.ADB_LOGCAT_EXIT), [])
        self.assertEqual(self.verdict_failures(entry, ("SIGTERM", 0)), ["player-log: SIGTERM rc -15, expected SIGTERM rc 0"])

    def test_a_handled_sigterm_exits_zero_as_the_offline_stubs_declare(self):
        entry = self.settled("import signal, sys, time; signal.signal(signal.SIGTERM, lambda *_: sys.exit(0)); "
                             "print('armed', flush=True); time.sleep(60)")
        self.assertEqual((entry["how"], entry["exit"]), ("SIGTERM", 0))
        self.assertEqual(m.OFFLINE_EXITS["player-log"], ("SIGTERM", 0))
        self.assertEqual(m.REAL_HOST_EXITS["player-log"], ("SIGTERM", 0))
        self.assertEqual(self.verdict_failures(entry, m.ADB_LOGCAT_EXIT), ["player-log: SIGTERM rc 0, expected SIGTERM rc -15"])

    def test_a_stream_that_ends_by_itself_is_not_a_signal_exit(self):
        entry = self.settled("pass", signal_first=False)
        self.assertEqual((entry["how"], entry["exit"]), ("exited", 0))
        self.assertEqual(self.verdict_failures(entry, m.ADB_LOGCAT_EXIT), ["player-log: exited rc 0, expected SIGTERM rc -15"])

    def test_the_supervisor_judges_an_unhandled_stream_by_the_live_declaration(self):
        live = dict(m.OFFLINE_EXITS, **{"player-log": m.ADB_LOGCAT_EXIT})
        rec = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS], log="unhandled-term", exits=live).supervise()
        self.assertEqual(rec["verdict"], {"ok": True, "failures": []})
        entry = next(c for c in rec["children"] if c.get("name") == "player-log")
        self.assertEqual((entry["how"], entry["exit"]), ("SIGTERM", -15))
        rec = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS], log="unhandled-term").supervise()       # the stub declaration
        self.assertIn("player-log: SIGTERM rc -15, expected SIGTERM rc 0", rec["verdict"]["failures"])


if __name__ == "__main__":
    unittest.main()
