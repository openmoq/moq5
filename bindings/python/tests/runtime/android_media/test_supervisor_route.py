"""The supervisor's `go` handoff: host ACTIVE, the relay route baseline, the supervisor-initiated
player start, the post-start route observation, and only then `go`.

Every run is offline: owned stub children (relay, publisher, host) under the real supervisor, a
stub relay that answers each SIGUSR1 with the next scripted route block on stderr, and a test-owned
player start and player report. Order is judged from the supervisor's ordered inventory
`rec["sequence"]`, never from elapsed time. Nothing here identifies a peer: a subscription is a
binding cookie, and the handoff is an attribution by run construction (see route_dump).
"""
import dataclasses
import json
import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import live_actors                                           # noqa: E402
import route_dump                                            # noqa: E402
import run_android_media as m                                # noqa: E402
from test_route_dump import EMPTY, NAME, NS, ONE_SUB, TWO_SUBS, live_dump, sub_line  # noqa: E402

FAST = m.Deadlines(relay_ready=3, publisher_ready=3, go_window=3, ack_all=1.5, ended_observed=1.5, done=1.5,
                   shutdown=2, settle=3)

# The offline relay stub's readiness records, plus a SIGUSR1 handler serving scripted route blocks.
ROUTE_RELAY_STUB = r'''
import json, os, signal, sys, time
records, blocks, log = json.loads(sys.argv[1]), json.load(open(sys.argv[2])), sys.argv[3]
# "N|release|ack": on request N touch `release`, then hold the response until `ack` exists (bounded)
hold = sys.argv[4].split("|") if len(sys.argv) > 4 and sys.argv[4] else None
served = [0]
def on_usr1(*_):
    with open(log, "a") as f:
        f.write("usr1\n")
    if hold and served[0] + 1 == int(hold[0]):
        open(hold[1], "a").close()
        deadline = time.monotonic() + 5
        while not os.path.exists(hold[2]):
            if time.monotonic() > deadline:
                with open(log, "a") as f:
                    f.write("ack-timeout\n")
                break
            time.sleep(0.01)
    block = blocks[min(served[0], len(blocks) - 1)]
    served[0] += 1
    sys.stderr.write("\n===== MOQ5 Relay routes =====\n" + "".join(l + "\n" for l in block) + "\n")
    sys.stderr.flush()
def on_term(*_):
    print(json.dumps({"schema": "RELAY_STOP_V1"}), flush=True)
    sys.exit(0)
signal.signal(signal.SIGUSR1, on_usr1)
signal.signal(signal.SIGTERM, on_term)
for r in records:
    print(json.dumps(r, separators=(",", ":")), flush=True)
while True:
    time.sleep(0.02)
'''


# The handoff chain, in order, each exactly once when `go` is forwarded. `player established` and
# `route fact` both follow the start dispatch and precede `go`; no order between them is required.
CHAIN = (("host", "go"), ("route", "baseline ready"), ("player_start", "begin"), ("player_start", "admit"),
         ("player_start", "admitted"), ("player_start", "dispatched"),
         ("player_start", "started"), ("route", "after start ready"), ("route", "fact"), ("control", "go"))
ONCE = CHAIN + (("player", "player_established"),)


# The player's receipts, as the sample writes them (hand-written here, not device output).
PRESENTED_PASS = ("presented: pass=true released=60 latched=60 distinctTs=60 indices=[0,59] preVideo=0 unreadable=0 "
                  "syncInvalid=0 complementInvalid=0 captureRefusals=0 ordered=true settled=true ordinal=1 leaseId=1 "
                  "refusal=none freezeReason=settled")
PRESENTED_FAIL = PRESENTED_PASS.replace("pass=true", "pass=false").replace("refusal=none", "refusal=released-mismatch")
CLOSED = "observer-closed: lateAfterFreeze=0 ignoredForeign=0 retiredAfterFreeze=0"

# A stand-in for the player's logcat stream: once the player has reported its terminal line (the
# trigger file), it prints the scripted receipts, then runs until the supervisor stops it.
PLAYER_LOG_STUB = r'''
import json, os, signal, sys, time
trigger, lines, mode = sys.argv[1], json.loads(sys.argv[2]), sys.argv[3]
if mode != "unhandled-term":                        # "unhandled-term" leaves SIGTERM fatal, as adb logcat does
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
if mode == "exit-early":
    sys.exit(3)
print("--------- beginning of main", flush=True)
while not os.path.exists(trigger):
    time.sleep(0.01)
for line in lines:
    print(line, flush=True)
while True:
    time.sleep(0.02)
'''


def ordering_violations(sequence):
    """Named violations of the handoff over an ordered inventory of {kind, what} records. Kinds:
    host (a host control word, or "exit observed rc N"), route ("request", "<phase> <verdict>" per
    judged dump, "refused: <reason>", or "fact"), player_start ("begin", "dispatched", "started",
    "failed: …"), player (a player fact), control (a forwarded control, or "stopped: <primary>").
    A judged "… refused" or a "refused: …" route event is terminal. When `go` was forwarded the
    whole chain must be present, once each, in order, and no route refusal may precede it."""
    items = [(e["kind"], e["what"]) for e in sequence]
    at = {}
    out = []
    for i, it in enumerate(items):
        if it in ONCE:
            if it in at:
                out.append(f"{it[0]} {it[1]!r} occurs more than once")
            at.setdefault(it, i)
    failed = [i for i, it in enumerate(items) if it[0] == "player_start" and it[1].startswith("failed")]
    if failed and ("player_start", "started") in at:
        out.append("the player start both failed and started")
    if len(failed) > 1:
        out.append("the player start failed more than once")
    judged = [i for i, it in enumerate(items) if it[0] == "route" and it[1] != "fact" and it[1] != "request"
              and not it[1].startswith("refused: ")]
    terminal = [i for i, it in enumerate(items) if it[0] == "route" and (it[1].endswith(" refused") or it[1].startswith("refused: "))]
    if terminal:
        # A terminal refusal ends the route work: only the gate's own refusal note may follow it.
        t = terminal[0]
        later = [it for it in items[t + 1:] if it[0] == "route" and not it[1].startswith("refused: ")]
        if later:
            out.append(f"route {later[0][1]!r} after a terminal {items[t][1]!r}")
    after = [i for i, it in enumerate(items) if it[0] == "route" and it[1].startswith("after start ")]
    host_go, start, dispatch, started = at.get(CHAIN[0]), at.get(CHAIN[2]), at.get(CHAIN[3]), at.get(CHAIN[4])
    established = at.get(("player", "player_established"))
    fact, go = at.get(("route", "fact")), at.get(("control", "go"))
    if judged and (host_go is None or judged[0] < host_go):
        out.append("a route dump was judged before the host reported ACTIVE")
    if start is not None and (at.get(CHAIN[1]) is None or start < at[CHAIN[1]]):
        out.append("the player start began before the route baseline was recorded")
    if dispatch is not None and (start is None or dispatch < start):
        out.append("the start action was dispatched before the start began")
    if after and (started is None or after[0] < started):
        out.append("an after-start dump was judged before the player start completed")
    if established is not None and (dispatch is None or established < dispatch):
        out.append("the player reported established before its start was dispatched")
    if fact is not None and (at.get(CHAIN[5]) is None or fact < at[CHAIN[5]]):
        out.append("route_two_subscriptions was set without a preceding after-start ready dump")
    if go is not None:
        missing = [f"{k} {w!r}" for k, w in CHAIN[:-1] + (("player", "player_established"),) if (k, w) not in at]
        if missing:
            out.append("go was forwarded without " + ", ".join(missing))
        present = [at[c] for c in CHAIN if c in at]
        if present != sorted(present):
            out.append("go was forwarded with the handoff chain out of order: " +
                       ", ".join(f"{k} {w!r}" for k, w in sorted((c for c in CHAIN if c in at), key=lambda c: at[c])))
        if established is not None and established > go:
            out.append("go was forwarded before player_established")
        if failed:
            out.append("go was forwarded after a failed player start")
        if terminal:
            out.append(f"go was forwarded after a terminal route refusal {items[terminal[0]][1]!r}")
    return out


class Run:
    """One supervised offline run with the staged route handoff. `player` picks the start/report
    schedule; every step of it waits on an owned event, never on elapsed time."""

    def __init__(self, test, blocks, host=("none",), exits=None, player="normal", hold_on=None, install=None,
                 prepare=None, receipts=(PRESENTED_PASS, CLOSED), log="normal"):
        tmp = tempfile.TemporaryDirectory()
        test.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        for name, src in (("relay", ROUTE_RELAY_STUB), ("publisher", m._PUBLISHER_STUB), ("host", m._HOST_STUB),
                          ("player_log", PLAYER_LOG_STUB)):
            (self.dir / f"{name}.py").write_text(src)
        (self.dir / "blocks.json").write_text(json.dumps(blocks))
        self.requests = self.dir / "requests.log"
        self.dispatched, self.reported, self.host_gone = threading.Event(), threading.Event(), threading.Event()
        self.player, self.start_calls = player, []
        self.install, self.prepare, self.receipts, self.log, self.phase_calls = install, prepare, list(receipts), log, []
        self.stopped_trigger = self.dir / "player reported stopped"
        py = sys.executable
        self.ack = self.dir / "host exit acknowledged"
        hold = f"{hold_on[0]}|{self.dir / hold_on[1]}|{self.ack}" if hold_on else ""
        self.actors = m.Actors(
            relay_argv=[py, "-I", self.dir / "relay.py", json.dumps(m.RELAY_STUB_RECORDS["ready"]),
                        self.dir / "blocks.json", self.requests, hold],
            publisher_argv=[py, "-I", self.dir / "publisher.py", "60", "normal"],
            host_argv=[py, "-I", self.dir / "host.py", *host],
            player_probe=self.player_probe,
            route_barrier=self.route_barrier,
            player_start=self.player_start,
            player_install=self.player_install,
            player_prepare=self.player_prepare,
            player_log_argv=self.player_log_argv,
            relay_identity=m.OFFLINE_IDENTITY,
            expected_exits=exits or m.OFFLINE_EXITS,
            env={"PATH": "/usr/bin:/bin"},
            hooks={"host_exit": self.host_exit, "admitting": self.admitting})

    def player_install(self, stop, budget_s):
        """The fresh install: None, or a failure reason; `install` picks a failure."""
        self.phase_calls.append(("install", budget_s))
        if self.install == "raises":
            raise OSError("adb: no devices/emulators found")
        return "org.openmoq.moq5.samples.player present before install" if self.install == "fails" else None

    def player_prepare(self, stop, budget_s):
        """Launch and configuration before the publisher: None, or a failure reason."""
        self.phase_calls.append(("prepare", budget_s))
        if self.prepare == "raises":
            raise TimeoutError("uiautomator dump timed out")
        if self.prepare == "overruns":
            stop.wait(2 * budget_s)                           # returns success, but after its budget
        return f"Start not enabled within {budget_s}s" if self.prepare == "fails" else None

    def player_log_argv(self):
        if self.log == "spawn-fails":
            return [str(self.dir / "no such logcat")]
        return [sys.executable, "-I", self.dir / "player_log.py", self.stopped_trigger, json.dumps(self.receipts), self.log]

    def host_exit(self, rc):
        """The supervisor's host watcher observed the exit: acknowledge it to the relay stub."""
        self.ack.touch()
        self.host_gone.set()

    def route_barrier(self, host_active, player_started, record):
        if self.player == "route-factory-raises":
            raise ValueError("route factory fault")
        probe, baseline_recorded = route_dump.route_barrier(NS, NAME, host_active, player_started, interval_s=0.05,
                                                            record=record)
        if self.player == "route-probe-raises":
            def faulty_probe(gate, relay, stop):
                raise RuntimeError("route probe fault")
            return faulty_probe, baseline_recorded
        if self.player != "reports-early":
            return probe, baseline_recorded

        def after_the_early_report(gate, relay, stop):         # the report precedes any route work
            if self._wait(self.reported, stop):
                probe(gate, relay, stop)
        return after_the_early_report, baseline_recorded

    def admitting(self, relay):
        """The supervisor is about to judge the relay for admission: `relay-exits-before-admission`
        ends the retained relay here, after the route baseline and before the check, and waits for it."""
        self.relay = relay
        if self.player in ("relay-exits-before-admission", "readmits-after-refusal") and relay.proc.poll() is None:
            relay.proc.terminate()
            relay.proc.wait(FAST.settle)

    def player_start(self, stop, dispatched, admit):
        """The supervisor-initiated start: `admit()` once, then `dispatched()` just before the final
        start action (the device sequence's Start tap), then None once started or a failure reason."""
        self.start_calls.append("start")
        if self.player == "start-raises":
            raise OSError("adb: device offline")                  # a runner fault, before any start action
        if self.player == "start-fails":
            return "am start: Error type 3"                   # fails before any start action
        if self.player == "readmits-after-relay-exit":
            # A faulty actor: earns admission, then asks again once the relay has gone, ignores that
            # refusal, and dispatches anyway.
            if admit() is not None:
                return "the first admission was refused (test schedule)"
            self.relay.proc.terminate()
            self.relay.proc.wait(FAST.settle)
            self.second_admission = admit()
        elif self.player == "readmits-after-refusal":
            # A faulty actor: refused because the relay has gone, it asks again and dispatches anyway.
            self.first_admission = admit()
            self.second_admission = admit()
        elif self.player != "skips-admission":
            refused = admit()
            if refused is not None:
                return f"before the tap: {refused}"
        dispatched()
        self.dispatched.set()
        if self.player in ("in-flight", "fails-after-report"):
            if not self._wait(self.reported, stop, FAST.settle):
                return "the player did not report (test schedule)"
        if self.player == "fails-after-report":
            return "the Start node did not become Stop"
        if self.player == "host-exits-during-handoff":
            (self.dir / "host may exit").touch()
            if not self.host_gone.wait(FAST.settle):
                return "the host did not exit (test schedule)"
        return None

    def player_probe(self, gate, stop):
        """The player's reports: `established` once the start action was dispatched (or at once,
        for a report before any start), and its terminal `stopped:` line after `ended-observed`."""
        if self.player == "player-probe-raises":
            raise RuntimeError("player probe fault")
        if self.player != "reports-early" and not self._wait(self.dispatched, stop):
            return
        gate.fact("player_established")
        if self.player == "reports-settled":
            gate.fact("player_settled")                       # a probe may not settle the player itself
        self.reported.set()
        while not stop.is_set() and "ended-observed" not in gate.sent:
            stop.wait(0.02)
        if not stop.is_set():
            gate.fact("player_stopped")                       # the UI terminal line; settlement needs the receipts
            self.stopped_trigger.touch()

    @staticmethod
    def _wait(event, stop, bound=None):
        """True once `event` is set; False if `stop` is set first or, given a bound, after it."""
        polls = None if bound is None else int(bound / 0.02)
        while not event.is_set():
            if stop.is_set() or polls == 0:
                return False
            event.wait(0.02)
            polls = None if polls is None else polls - 1
        return True

    def supervise(self, deadlines=FAST):
        return m.supervise(self.dir, self.actors, deadlines, 60)


def forwarded(rec):
    return [c["line"] for c in rec["controls_forwarded"]]


def pairs(rec):
    return [(e["kind"], e["what"]) for e in rec["sequence"]]


class Handoff(unittest.TestCase):
    def check(self, rec, primary, forwarded_controls=(), violations=()):
        if primary is None:
            self.assertIsNone(rec["primary_error"])
        elif primary.endswith("…"):
            self.assertTrue((rec["primary_error"] or "").startswith(primary[:-1]), rec["primary_error"])
        else:
            self.assertEqual(rec["primary_error"], primary)
        self.assertEqual(forwarded(rec), list(forwarded_controls))
        self.assertEqual(ordering_violations(rec["sequence"]), list(violations), pairs(rec))
        self.assertTrue(rec["checks"]["all_children_settled"])

    def starts(self, rec):
        return [w for k, w in pairs(rec) if k == "player_start"]

    def test_go_forwards_only_after_the_staged_route_transition(self):
        run = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS])
        rec = run.supervise()
        self.check(rec, None, m.CONTROLS)
        self.assertEqual(self.starts(rec), ["begin", "admit", "admitted", "dispatched", "started"])
        self.assertEqual(run.start_calls, ["start"])
        self.assertEqual(rec["verdict"], {"ok": True, "failures": []})

    def test_a_report_while_the_start_is_in_flight_is_accepted(self):
        run = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS], player="in-flight")
        rec = run.supervise()
        self.check(rec, None, m.CONTROLS)
        p = pairs(rec)
        self.assertLess(p.index(("player_start", "dispatched")), p.index(("player", "player_established")))
        self.assertLess(p.index(("player", "player_established")), p.index(("player_start", "started")))

    def test_a_report_before_the_start_was_dispatched_is_refused(self):
        run = Run(self, [ONE_SUB, TWO_SUBS], player="reports-early")
        rec = run.supervise()
        self.check(rec, "go: refused: player reported player_established before its start was dispatched",
                   violations=["the player reported established before its start was dispatched"])
        handoff = [it for it in pairs(rec) if it[0] in ("player", "player_start", "route")]
        self.assertEqual(handoff[0], ("player", "player_established"))
        self.assertEqual((run.start_calls, self.starts(rec)), ([], []))

    def test_a_start_that_fails_after_a_report_does_not_forward_go(self):
        run = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS], player="fails-after-report")
        rec = run.supervise()
        self.check(rec, "go: refused: player start failed: the Start node did not become Stop")
        self.assertEqual(self.starts(rec), ["begin", "admit", "admitted", "dispatched", "failed: the Start node did not become Stop"])
        p = pairs(rec)
        self.assertLess(p.index(("player", "player_established")), p.index(("player_start", "failed: the Start node did not become Stop")))
        self.assertNotIn(("route", "fact"), p)

    def test_a_start_that_fails_before_dispatch_is_named(self):
        run = Run(self, [ONE_SUB, TWO_SUBS], player="start-fails")
        rec = run.supervise()
        self.check(rec, "go: refused: player start failed: am start: Error type 3")
        self.assertEqual(self.starts(rec), ["begin", "failed: am start: Error type 3"])

    def test_a_start_that_raises_is_a_named_refusal_and_its_worker_settles(self):
        uncaught = []
        previous = threading.excepthook
        threading.excepthook = lambda args: uncaught.append(f"{args.thread.name}: {args.exc_value!r}")
        self.addCleanup(setattr, threading, "excepthook", previous)
        run = Run(self, [ONE_SUB, TWO_SUBS], player="start-raises")
        rec = run.supervise()
        self.check(rec, "go: refused: player start failed: raised OSError('adb: device offline')")
        self.assertEqual(self.starts(rec), ["begin", "failed: raised OSError('adb: device offline')"])
        self.assertEqual(uncaught, [])
        self.assertIs(rec["threads_settled"], True)

    def uncaught(self):
        """Records every exception that escapes a thread for the rest of this test."""
        caught = []
        previous = threading.excepthook
        threading.excepthook = lambda args: caught.append(f"{args.thread.name}: {args.exc_value!r}")
        self.addCleanup(setattr, threading, "excepthook", previous)
        return caught

    def test_a_player_probe_that_raises_is_a_named_refusal(self):
        caught = self.uncaught()
        rec = Run(self, [ONE_SUB, TWO_SUBS], player="player-probe-raises").supervise()
        self.check(rec, "go: refused: player probe raised RuntimeError('player probe fault')")
        self.assertEqual(caught, [])
        self.assertIs(rec["threads_settled"], True)

    def test_a_route_probe_that_raises_is_a_named_refusal(self):
        caught = self.uncaught()
        rec = Run(self, [ONE_SUB, TWO_SUBS], player="route-probe-raises").supervise()
        self.check(rec, "go: refused: route probe raised RuntimeError('route probe fault')")
        self.assertIn(("route", "refused: route probe raised RuntimeError('route probe fault')"), pairs(rec))
        self.assertEqual(caught, [])
        self.assertIs(rec["threads_settled"], True)

    def test_a_route_factory_that_raises_is_a_named_harness_failure(self):
        run = Run(self, [ONE_SUB, TWO_SUBS], player="route-factory-raises")
        try:
            rec = run.supervise()
        except Exception as e:                           # the fault must not escape the supervisor
            self.fail(f"supervise() raised {e!r}")
        primary = "harness: ValueError('route factory fault')"
        self.assertEqual(rec["primary_error"], primary)
        self.assertEqual(pairs(rec), [("control", f"stopped: {primary}")])
        self.assertIs(rec["verdict"]["ok"], False)
        self.assertEqual(rec["children"], [])            # no child was launched
        self.assertIs(rec["threads_settled"], True)
        self.assertEqual((run.start_calls, forwarded(rec), rec["route_dumps"]), ([], [], []))

    def test_a_relay_gone_by_admission_is_refused_before_dispatch(self):
        run = Run(self, [ONE_SUB, TWO_SUBS], player="relay-exits-before-admission")
        rec = run.supervise()
        reason = "relay exited (rc 0) before the Start dispatch"          # the stub relay ends cleanly on SIGTERM
        self.assertEqual(rec["primary_error"], f"go: refused: player start failed: before the tap: {reason}")
        self.assertEqual(self.starts(rec), ["begin", "admit", f"admit refused: {reason}", f"failed: before the tap: {reason}"])
        p = pairs(rec)
        self.assertLess(p.index(("route", "baseline ready")), p.index(("player_start", "admit")))
        self.assertNotIn(("player_start", "dispatched"), p)
        self.assertFalse(run.dispatched.is_set())
        self.assertEqual(forwarded(rec), [])
        self.assertIs(rec["threads_settled"], True)

    def test_a_dispatch_without_admission_is_refused(self):
        run = Run(self, [ONE_SUB, TWO_SUBS], player="skips-admission")
        rec = run.supervise()
        self.assertEqual(rec["primary_error"], "go: refused: player start failed: raised "
                         "RuntimeError('the start was dispatched without admission')")
        self.assertEqual(self.starts(rec), ["begin", "dispatched without admission",
                                            "failed: raised RuntimeError('the start was dispatched without admission')"])
        self.assertFalse(run.dispatched.is_set())
        self.assertEqual(forwarded(rec), [])

    def test_a_refused_repeat_admission_leaves_nothing_to_dispatch(self):
        run = Run(self, [ONE_SUB, TWO_SUBS], player="readmits-after-relay-exit")
        rec = run.supervise()
        self.assertFalse(run.dispatched.is_set(), "a refused repeat admission left the dispatch open")
        self.assertEqual(run.second_admission, "admission was already asked once")
        self.assertEqual(rec["primary_error"], "go: refused: player start failed: raised "
                         "RuntimeError('the start was dispatched without admission')")
        self.assertEqual(self.starts(rec), ["begin", "admit", "admitted", "admit", "admit refused: admission was already asked once",
                                            "dispatched without admission",
                                            "failed: raised RuntimeError('the start was dispatched without admission')"])
        self.assertFalse(run.dispatched.is_set())
        self.assertEqual(forwarded(rec), [])
        self.assertIs(rec["threads_settled"], True)

    def test_admission_is_asked_once_even_after_a_refusal(self):
        run = Run(self, [ONE_SUB, TWO_SUBS], player="readmits-after-refusal")
        rec = run.supervise()
        self.assertFalse(run.dispatched.is_set())
        self.assertEqual((run.first_admission, run.second_admission),
                         ("relay exited (rc 0) before the Start dispatch", "admission was already asked once"))
        self.assertEqual(rec["primary_error"], "go: refused: player start failed: raised "
                         "RuntimeError('the start was dispatched without admission')")
        self.assertEqual(forwarded(rec), [])

    def test_established_without_a_new_subscription_does_not_forward_go(self):
        rec = Run(self, [ONE_SUB]).supervise()
        self.check(rec, f"go: barrier(s) route_two_subscriptions not observed within {FAST.go_window}s of publisher_ready")
        self.assertIn(("player", "player_established"), pairs(rec))
        self.assertNotIn(("route", "fact"), pairs(rec))

    def test_no_baseline_means_no_player_start(self):
        run = Run(self, [EMPTY])
        rec = run.supervise()
        self.check(rec, f"go: barrier(s) player_established, route_two_subscriptions not observed "
                        f"within {FAST.go_window}s of publisher_ready")
        self.assertEqual((run.start_calls, self.starts(rec)), ([], []))

    def test_two_subscriptions_at_the_baseline_are_refused(self):
        run = Run(self, [TWO_SUBS])
        rec = run.supervise()
        self.check(rec, 'go: refused: relay route dump (baseline): live/cam1 "video" has 2 '
                        "subscriptions before the player start; the baseline is one")
        self.assertEqual(run.start_calls, [])

    def test_host_exit_while_the_player_starts_is_named(self):
        rec = Run(self, [ONE_SUB, TWO_SUBS], host=("exit-after-go", "exit-on-file", "host may exit"),
                  exits=m.REAL_HOST_EXITS, player="host-exits-during-handoff").supervise()
        self.check(rec, "go: refused: host-receiver exited (rc 0) during the route handoff")

    def test_host_exit_after_the_start_before_the_route_result_is_named(self):
        # The relay's second request is the first after-start dump: it releases the host's exit and holds
        # its response until the supervisor's host watcher has observed that exit and acknowledged it.
        run = Run(self, [ONE_SUB], host=("exit-after-go", "exit-on-file", "host may exit"), exits=m.REAL_HOST_EXITS,
                  hold_on=(2, "host may exit"))
        rec = run.supervise()
        self.assertNotIn("ack-timeout", run.requests.read_text().split(),
                         "the relay held the after-start response 5s and the supervisor never acknowledged the host exit")
        primary = "go: refused: host-receiver exited (rc 0) during the route handoff"
        self.check(rec, primary)
        p = pairs(rec)
        started = p.index(("player_start", "started"))
        request = next(i for i, it in enumerate(p) if it == ("route", "request") and i > started)
        observed = p.index(("host", "exit observed rc 0"))
        stopped = p.index(("control", f"stopped: {primary}"))
        self.assertLess(started, request)
        self.assertLess(request, observed)
        self.assertLess(observed, stopped)
        self.assertNotIn(("route", "fact"), p)
        self.assertNotIn(("control", "go"), p)

    def test_a_malformed_dump_is_refused(self):
        run = Run(self, [ONE_SUB[:7] + ONE_SUB[8:]])
        rec = run.supervise()
        self.check(rec, "go: refused: relay route dump line 8: …")
        self.assertEqual(run.start_calls, [])

    def test_the_supervisor_has_no_fabricated_route_probe(self):
        self.assertNotIn("route_probe", m.Actors.__dataclass_fields__)


class Oracle(unittest.TestCase):
    """The ordering oracle on hand-written inventories: the full chain passes; omissions,
    reorderings, duplicates and contradictions fail by name."""

    STAGED = (("host", "go"), ("route", "baseline waiting"), ("route", "baseline ready"), ("player_start", "begin"),
              ("player_start", "admit"), ("player_start", "admitted"), ("player_start", "dispatched"), ("player", "player_established"), ("player_start", "started"),
              ("route", "after start waiting"), ("route", "after start ready"), ("route", "fact"), ("control", "go"))

    def seq(self, *pairs_):
        return [{"t": i, "kind": k, "what": w} for i, (k, w) in enumerate(pairs_)]

    def without(self, *drop):
        return self.seq(*(p for p in self.STAGED if p not in drop))

    def test_the_staged_inventory_passes(self):
        self.assertEqual(ordering_violations(self.seq(*self.STAGED)), [])

    def test_a_judged_refusal_and_its_gate_note_end_the_route_legally(self):
        self.assertEqual(ordering_violations(self.seq(
            ("host", "go"), ("route", "request"), ("route", "baseline refused"),
            ("route", "refused: relay route dump (baseline): two subscriptions"), ("control", "stopped: go: refused: x"))), [])

    def test_repeated_waiting_and_requests_are_legal(self):
        staged = list(self.STAGED)
        at = staged.index(("route", "after start waiting"))
        staged[at:at] = [("route", "request"), ("route", "after start waiting"), ("route", "request")]
        self.assertEqual(ordering_violations(self.seq(("route", "request"), *staged)), [])

    def test_established_and_the_route_fact_may_come_in_either_order(self):
        staged = list(self.STAGED)
        staged.remove(("player", "player_established"))
        staged.insert(staged.index(("route", "fact")) + 1, ("player", "player_established"))
        self.assertEqual(ordering_violations(self.seq(*staged)), [])

    def test_violations_are_named(self):
        cases = (
            ("no baseline and no start begin",
             self.seq(("host", "go"), ("player_start", "started"), ("player", "player_established"),
                      ("route", "after start ready"), ("route", "fact"), ("control", "go")),
             "go was forwarded without route 'baseline ready', player_start 'begin', player_start 'admit', "
             "player_start 'admitted', player_start 'dispatched'"),
            ("no admission", self.without(("player_start", "admit"), ("player_start", "admitted")),
             "go was forwarded without player_start 'admit', player_start 'admitted'"),
            ("no dispatch", self.without(("player_start", "dispatched")),
             "go was forwarded without player_start 'dispatched'"),
            ("no route fact", self.without(("route", "fact")), "go was forwarded without route 'fact'"),
            ("no established", self.without(("player", "player_established")),
             "go was forwarded without player 'player_established'"),
            ("the host go twice", self.seq(*self.STAGED[:1], *self.STAGED), "host 'go' occurs more than once"),
            ("go forwarded twice", self.seq(*self.STAGED, ("control", "go")), "control 'go' occurs more than once"),
            ("started twice", self.seq(*self.STAGED[:7], ("player_start", "started"), *self.STAGED[7:]),
             "player_start 'started' occurs more than once"),
            ("failed and started", self.seq(*self.STAGED[:6], ("player_start", "failed: x"), *self.STAGED[6:]),
             "the player start both failed and started"),
            ("baseline after begin", self.seq(("host", "go"), ("player_start", "begin"), ("route", "baseline ready"),
                                              *self.STAGED[4:]),
             "the player start began before the route baseline was recorded"),
            ("the route fact before go's chain, with go before it",
             self.seq(*self.STAGED[:9], ("control", "go"), ("route", "fact")), "control 'go'"),
            ("established before dispatch", self.seq(*self.STAGED[:4], ("player", "player_established"),
                                                     ("player_start", "dispatched"), *self.STAGED[6:]),
             "the player reported established before its start was dispatched"),
            ("a baseline refusal, then ready and go",
             self.seq(*self.STAGED[:2], ("route", "baseline refused"), *self.STAGED[2:]),
             "go was forwarded after a terminal route refusal 'baseline refused'"),
            ("a baseline refusal, then ready", self.seq(("host", "go"), ("route", "baseline refused"), ("route", "baseline ready")),
             "route 'baseline ready' after a terminal 'baseline refused'"),
            ("an after-start refusal, then ready and go",
             self.seq(*self.STAGED[:10], ("route", "after start refused"), *self.STAGED[10:]),
             "route 'after start ready' after a terminal 'after start refused'"),
            ("a parse refusal, then go", self.seq(*self.STAGED[:12], ("route", "refused: relay route dump line 8: x"),
                                                  ("control", "go")),
             "go was forwarded after a terminal route refusal 'refused: relay route dump line 8: x'"),
            ("an after-start refusal, then the fact (no go)",
             self.seq(*self.STAGED[:11], ("route", "after start refused"), ("route", "fact")),
             "route 'fact' after a terminal 'after start refused'"),
            ("a baseline refusal, then another request", self.seq(("host", "go"), ("route", "request"),
                                                                  ("route", "baseline refused"), ("route", "request")),
             "route 'request' after a terminal 'baseline refused'"),
            ("a parse refusal, then a waiting dump", self.seq(("host", "go"), ("route", "request"),
                                                             ("route", "refused: relay route dump line 8: x"),
                                                             ("route", "baseline waiting")),
             "route 'baseline waiting' after a terminal 'refused: relay route dump line 8: x'"),
        )
        for label, inventory, needle in cases:
            with self.subTest(label):
                v = ordering_violations(inventory)
                self.assertTrue(any(needle in x for x in v), v)


# The relay stand-in for the loader controls: the route stub, prefixed by what the pinned relay's
# loader adds under DYLD_PRINT_LIBRARIES -- one image line per `provider lines` entry, under this
# process's pid (or, in `loader mode` "foreign", another pid's). The real dyld prints the stand-in
# interpreter's own images as well, for whichever child the variable reaches.
LOADER_PRELUDE = r'''
import os as _os, sys as _sys
_here = _os.path.dirname(_os.path.abspath(__file__))
if _os.environ.get("DYLD_PRINT_LIBRARIES") == "1":
    _pid = _os.getpid() + (1 if open(_os.path.join(_here, "loader mode")).read() == "foreign" else 0)
    for _path in open(_os.path.join(_here, "provider lines")).read().splitlines():
        _sys.stderr.write(f"dyld[{_pid}]: <1C84AFF8-23AE-30A6-B804-9BE34CB0A3FD> {_path}\n")
    if open(_os.path.join(_here, "loader mode")).read() == "warn":
        _sys.stderr.write("warning: relay capacity rounded up\n")
    _sys.stderr.flush()
'''


class RelayLoaderEvidence(unittest.TestCase):
    """The composed run's relay alone receives the loader's image list, and the run's final verdict
    judges that relay's OWN settled record: its loader lines under its pid, naming the declared
    provider. Other children keep the shared environment; nothing earlier stands in for this run."""

    def run_with(self, relay_env=None, judge="provider", lines=None, mode="normal"):
        run = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS])
        provider = run.dir / "msquic build" / "libmsquic.2.6.0.dylib"
        provider.parent.mkdir()
        provider.write_bytes(b"the declared provider\n")
        (run.dir / "provider lines").write_text(f"{provider}\n" if lines is None else lines(run.dir, provider))
        (run.dir / "loader mode").write_text(mode)
        (run.dir / "relay_loader.py").write_text(LOADER_PRELUDE + ROUTE_RELAY_STUB)
        self.base_env = dict(run.actors.env)
        self.judged = []
        evidence = live_actors.relay_evidence_judge(provider) if judge == "provider" else judge

        def recording(settled):
            self.judged.append(settled)
            return evidence(settled)
        run.actors = dataclasses.replace(
            run.actors, relay_argv=[sys.executable, "-I", run.dir / "relay_loader.py", *run.actors.relay_argv[3:]],
            relay_env=relay_env, relay_evidence=recording if evidence is not None else None)
        rec = run.supervise()
        self.children = {c["name"]: c for c in rec["children"]}
        return run, rec

    def loader_pids(self, name):
        return {line.split("]", 1)[0] for line in self.children[name]["stderr"].splitlines() if line.startswith("dyld[")}

    def test_only_the_relay_receives_the_image_list_and_its_own_evidence_passes(self):
        run, rec = self.run_with(relay_env={"DYLD_PRINT_LIBRARIES": "1"})
        self.assertEqual(rec["verdict"], {"ok": True, "failures": []}, json.dumps(rec["verdict"], indent=1))
        relay = self.children["relay"]
        self.assertEqual(self.loader_pids("relay"), {f"dyld[{relay['pid']}"})
        for name in ("publisher", "host-receiver", "player-log"):
            self.assertEqual(self.loader_pids(name), set(), f"{name} received the relay's loader variable")
        self.assertEqual([j["pid"] for j in self.judged], [relay["pid"]])              # this run's relay, once
        self.assertIsNone(rec["relay_evidence"])
        self.assertEqual(run.actors.env, self.base_env)                                  # the shared base, untouched
        self.assertNotIn("DYLD_PRINT_LIBRARIES", run.actors.env)
        self.assertNotIn("DYLD_PRINT_LIBRARIES", os.environ)

    def test_without_the_override_no_child_receives_it_and_nothing_is_judged(self):
        _run, rec = self.run_with(judge=None)
        self.assertEqual(rec["verdict"], {"ok": True, "failures": []})
        self.assertNotIn("relay_evidence", rec)
        for name in self.children:
            self.assertEqual(self.loader_pids(name), set(), name)

    def assert_refused(self, rec, needle):
        self.assertFalse(rec["verdict"]["ok"])
        self.assertTrue(any(f.startswith("relay evidence: ") and needle in f for f in rec["verdict"]["failures"]),
                        rec["verdict"]["failures"])

    def test_missing_wrong_or_foreign_evidence_prevents_success(self):
        def elsewhere(d, provider):
            other = d / "another build" / provider.name
            other.parent.mkdir()
            other.write_bytes(provider.read_bytes())
            return f"{other}\n"
        for label, kw, needle in (
                ("no provider image", dict(lines=lambda d, p: ""), "0 libmsquic images loaded"),
                ("the same name from another build", dict(lines=elsewhere), "loaded "),
                ("another process's loader lines", dict(mode="foreign"), "expected only the serve process"),
                ("no image list at all: an earlier receipt cannot stand in", dict(relay_env=None),
                 "loader lines from processes []")):
            with self.subTest(label):
                _run, rec = self.run_with(**dict(dict(relay_env={"DYLD_PRINT_LIBRARIES": "1"}), **kw))
                self.assert_refused(rec, needle)

    def test_a_run_whose_relay_never_ran_has_no_evidence(self):
        run = Run(self, [ONE_SUB], install="fails")                          # refused before the relay's spawn
        run.actors = dataclasses.replace(run.actors, relay_env={"DYLD_PRINT_LIBRARIES": "1"},
                                         relay_evidence=lambda settled: None)
        rec = run.supervise()
        self.assertEqual(rec["relay_evidence"], "no settled record of this run's relay (0 relay records)")
        self.assertIn("relay evidence: no settled record of this run's relay (0 relay records)", rec["verdict"]["failures"])

    def test_a_judge_that_raises_is_a_named_failure(self):
        def raises(settled):
            raise RuntimeError("judge fault")
        _run, rec = self.run_with(relay_env={"DYLD_PRINT_LIBRARIES": "1"}, judge=raises)
        self.assert_refused(rec, "RuntimeError('judge fault')")

    def test_warnings_stay_fatal_beside_good_evidence(self):
        _run, rec = self.run_with(relay_env={"DYLD_PRINT_LIBRARIES": "1"}, mode="warn")
        self.assertIsNone(rec["relay_evidence"])
        self.assertFalse(rec["verdict"]["ok"])
        self.assertIn("unplanned diagnostic: relay [stderr]: warning: relay capacity rounded up", rec["verdict"]["failures"])


class RelaySnapshot(unittest.TestCase):
    """At the run's retirement boundary, before the relay is settled, the actors' relay snapshot is
    taken of the still-running relay; a snapshot that fails is recorded and retirement goes on."""

    def run_with(self, snapshot):
        run = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS])
        run.actors = dataclasses.replace(run.actors, relay_snapshot=snapshot)
        return run.supervise()

    def test_the_snapshot_sees_the_live_relay_before_its_settlement(self):
        seen = []

        def snapshot(relay):
            seen.append((relay.name, relay.alive(), relay.settled))
            return {"route": "taken"}
        rec = self.run_with(snapshot)
        self.assertEqual(seen, [("relay", True, None)])
        self.assertEqual(rec["relay_snapshot"], {"route": "taken"})
        self.assertEqual(rec["verdict"], {"ok": True, "failures": []})

    def test_a_snapshot_that_raises_is_recorded_and_retirement_goes_on(self):
        def snapshot(relay):
            raise OSError("admin refused")
        rec = self.run_with(snapshot)
        self.assertEqual(rec["relay_snapshot"], {"error": "OSError('admin refused')"})
        self.assertTrue(all(c.get("exit") is not None for c in rec["children"]))

    def test_the_live_snapshot_reads_the_admin_endpoint_and_one_final_route_block(self):
        import http.server
        import socket
        served = []

        class Admin(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                served.append(self.path)
                body = f"body of {self.path}".encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass
        server = http.server.HTTPServer(("127.0.0.1", 0), Admin)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        self.addCleanup(worker.join, 5)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        port = server.server_address[1]
        take = live_actors.relay_boundary_snapshot(port)
        rec = self.run_with(take)
        snap = rec["relay_snapshot"]
        self.assertEqual(sorted(served), ["/api/v1/shards", "/metrics"])
        self.assertEqual(snap["admin"]["/metrics"], {"status": 200, "body": "body of /metrics"})
        self.assertEqual(snap["route"]["lines"], TWO_SUBS)                      # the rig's last scripted block
        track, = snap["route"]["summary"]["tracks"]
        self.assertEqual(len(track["subscriptions"]), 2)
        self.assertTrue(all("frontier" in sub for sub in track["subscriptions"]))


if __name__ == "__main__":
    unittest.main()
