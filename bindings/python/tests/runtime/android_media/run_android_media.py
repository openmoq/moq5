"""Supervisor for the Android real-media acceptance run: generated H.264 ->
runtime publisher -> our moq5-relay -> (host exact receiver, Android player).

    python -I run_android_media.py --offline-controls     stub children only; no relay, adb or network
    python -I run_android_media.py --live --authorized ... one owned device/media run (live_entry.py):
                                                           every input declared; refuses without them

`--offline-controls` runs stub children only, and a stub's `CTRL ack-all` is
not evidence that any wire object matched.

The supervisor directly owns the relay, the publisher and the host receiver
and settles every one of them on every path. The publisher starts only after
the relay's RELAY_READY_V1 record for the declared raw listener (listener,
host, port, ALPN set and lane count all equal). The publisher's four positive
controls are forwarded only when EVERY barrier the protocol names holds:

    go              host receiver ACTIVE  and  player established  and  the staged route handoff (below)
    ack-all         host receiver matched every expected object exactly
    ended-observed  host receiver drained TRACK_ENDED
    done            host receiver completion receipt  and  player settled terminal line

Each control's deadline is absolute: its budget runs from the publisher event
that starts the phase (`ready`, the last of the ordered rc-0 writes 0..59,
`end_track`, `complete`), stamped by the supervisor's reader when observed.
That event must itself be stamped within the budget of the previous control's
forwarding, and a control is refused once its deadline has passed even if
every barrier holds. Relay readiness, publisher readiness and the publisher's
shutdown record are likewise judged by their reader stamps against absolute
deadlines. A write stream that is out of order, repeated, failed, or early
refuses every later control. A missed deadline fails by name and is never
retried. The publisher's `complete` result (the completion
request) and its later `shutdown` result (teardown, drain, stop) are judged
separately, and the run passes only through `final_verdict`. Relay aggregate
metrics are liveness evidence only.

The `go` handoff is staged (route_dump): once the host reports ACTIVE, a relay
route dump must show the declared track routed from its announce with exactly
one subscription, the host's; only then does the supervisor start the player,
and a dump requested after that start must show exactly that subscription and
one new one. Immediately before the player's final start action, admission
requires the retained relay child still running; without it the dispatch stays
closed. The player's reports are admitted from the moment its final start
action is dispatched; a start that fails or raises refuses `go`, and no report
stands in for the post-start dump. A host exit observed before `go` is released
refuses `go`. Every step is entered, in order, in the run's `sequence`.
"""
import io
import json
import math
import os
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
from runtime_support import EXPECTED_ALPN, Child, collect_diagnostics          # noqa: E402
import player_receipts                                                        # noqa: E402

CONTROLS = ("go", "ack-all", "ended-observed", "done")
HOST_FACT_CONTROL = {"host_active": "go", "host_all_matched": "ack-all", "host_ended": "ended-observed",
                     "host_complete": "done"}
DEDICATED_SERIAL = "emulator-5580"


@dataclass
class Deadlines:
    """Phase budgets in seconds, each measured from its named starting event.
    Offline controls pass smaller ones."""
    player_install: float = 60.0      # from the run's start: the player package absent, installed, present
    relay_ready: float = 30.0         # from relay spawn
    player_prepare: float = 30.0      # from relay readiness: launch and configure the player, before the publisher
    publisher_ready: float = 20.0     # from publisher spawn
    go_window: float = 25.0           # from publisher `ready`
    ack_all: float = 10.0             # from the last `wrote`
    ended_observed: float = 10.0      # from `end_track`
    done: float = 25.0                # from `complete`
    shutdown: float = 10.0            # from `done` forwarded
    settle: float = 5.0


class PhaseFailure(RuntimeError):
    pass


class MonotonicClock:
    """The supervisor's time source. `mark` names a supervisor step; only a
    test clock acts on it."""

    def now(self):
        return time.monotonic()

    def wait(self, cv, timeout):
        cv.wait(timeout)

    def mark(self, _label):
        pass


class SkewClock(MonotonicClock):
    """Monotonic time plus a jump applied at a named supervisor step, so an
    offline control can place a step late without sleeping."""

    def __init__(self, jumps):
        self.jumps, self.offset = dict(jumps), 0.0

    def now(self):
        return time.monotonic() + self.offset

    def mark(self, label):
        self.offset += self.jumps.pop(label, 0.0)


# ------------------------------------------------------------------ stamped events
class StampedEvent:
    """A one-shot event stamped with the clock when first set, from the
    reader thread that observed it. The clock step `stamp:<name>` is marked
    under the event's lock, so a test clock can move time at exactly the
    observation without a waiter seeing the interval."""

    def __init__(self, clock, name="event"):
        self.clock, self.name = clock, name
        self.cv, self.t = threading.Condition(threading.RLock()), None

    def set(self):
        with self.cv:
            if self.t is None:
                self.clock.mark("stamp:" + self.name)
                self.t = self.clock.now()
            self.cv.notify_all()

    def is_set(self):
        return self.t is not None


def await_stamped(ev, deadline, clock, alive=None):
    """'on-time' or 'late' by the event's stamp against `deadline` (however
    late the waiter wakes); 'exited' if `alive` turns false first; 'absent'
    once the deadline is reached without it."""
    with ev.cv:
        while True:
            if ev.t is not None:
                return "on-time" if ev.t <= deadline else "late"
            if alive is not None and not alive():
                return "exited"
            now = clock.now()
            if now >= deadline:
                return "absent"
            left = deadline - now
            clock.wait(ev.cv, min(left, 0.05) if alive is not None else left)


class WriteLedger:
    """The publisher's `wrote` records as an anchor source: indices 0..n-1,
    each exactly once, in order, rc 0, only after `go` was released. The
    last one completes the stream exactly once; any defect is an error and
    the stream never completes. This judges the publisher's own receipts
    only; the received media is compared against the objects file elsewhere."""

    def __init__(self, n):
        self.n, self.next, self.done, self.error = n, 0, False, None

    def admit(self, ev, released):
        if self.error is not None:
            return "error"
        idx, rc = ev.get("index"), ev.get("rc")
        if not released:
            self.error = f"wrote index {idx!r} before go"
        elif self.done:
            self.error = f"wrote index {idx!r} after the last object"
        elif type(idx) is not int or idx != self.next:
            self.error = f"wrote index {idx!r} where {self.next} was next"
        elif type(rc) is not int or rc != 0:
            self.error = f"wrote index {idx} rc {rc!r}"
        if self.error is not None:
            return "error"
        self.next += 1
        if self.next == self.n:
            self.done = True
            return "complete"
        return None


# ------------------------------------------------------------------ device actor (argv only)
class Device:
    """Builds every adb command for the ONE dedicated target. The serial must
    equal DEDICATED_SERIAL exactly; there is no other selection path.
    Execution goes through the injected runner, so offline controls can prove
    nothing reaches adb."""

    def __init__(self, serial, runner, adb="adb"):
        if serial != DEDICATED_SERIAL:
            raise ValueError(f"refusing target {serial!r}: only {DEDICATED_SERIAL} is allowed")
        self.serial, self.runner, self.adb = serial, runner, adb

    def argv(self, *args):
        return [self.adb, "-s", self.serial, *args]

    def commands(self):
        """Every command shape this actor can issue, for inspection."""
        return {
            "avd_name": self.argv("emu", "avd", "name"),
            "boot_completed": self.argv("shell", "getprop", "sys.boot_completed"),
            "sdk": self.argv("shell", "getprop", "ro.build.version.sdk"),
            "abi": self.argv("shell", "getprop", "ro.product.cpu.abilist"),
            "packages": self.argv("shell", "pm", "list", "packages"),
            "install": self.argv("install", "<apk>"),
            # The test CA: staged unprivileged, then every step on it is its own `su 0` command, so adbd
            # itself is never restarted (the dedicated image is userdebug; see device_lane).
            "ca_stage": self.argv("push", "<local ca>", "/data/local/tmp/moq5-test-ca.pem"),
            "privileged": self.argv("shell", "su", "0", "<argv...>"),
            "privileged_read": self.argv("exec-out", "su", "0", "cat", "<path>"),
            # The launch extras arm the sample's opt-in presentation observer (ordinary launches never
            # observe) and preset its three fields, so nothing is typed into them.
            "start": self.argv("shell", "am", "start", "-W", "-n", "org.openmoq.moq5.samples.player/.PlayerActivity",
                               "--ez", "org.openmoq.moq5.samples.player.OBSERVE_PRESENTATION", "true",
                               "--es", "org.openmoq.moq5.samples.player.RELAY_URL", "<url>",
                               "--es", "org.openmoq.moq5.samples.player.NAMESPACE", "<namespace>",
                               "--es", "org.openmoq.moq5.samples.player.OBJECT_BUDGET", "<objects>"),
            "package": self.argv("shell", "pm", "list", "packages", "org.openmoq.moq5.samples.player"),
            "pidof": self.argv("shell", "pidof", "org.openmoq.moq5.samples.player"),
            "ui_dump": self.argv("exec-out", "uiautomator", "dump", "/dev/tty"),
            "tap": self.argv("shell", "input", "tap", "<x>", "<y>"),
            "player_log": self.argv("logcat", "--pid=<pid>", "-v", "raw", "-s", "MoqPlayer:I", "MoqPlayerObserver:I"),
            "uninstall": self.argv("uninstall", "org.openmoq.moq5.samples.player"),
            "stop_emulator": self.argv("emu", "kill"),
        }

    def run(self, name, *args, timeout=30.0):
        return self.runner(name, self.argv(*args), timeout)


# The dedicated emulator's own shutdown, after the run's `emu kill`: these two warnings, as observed
# (the Netsim port varies by run), each at most once. Nothing else, and nothing before the kill.
EMULATOR_SHUTDOWN_WARNINGS = (
    re.compile(re.escape("WARNING      | Emulator client has not yet been configured.. Call configure me first!")),
    re.compile(r"WARNING      \| Netsim Wifi localhost:[0-9]+ is gone due to CANCELLED"))
_EMULATOR_DIAGNOSTIC = re.compile(r"\b(WARNING|ERROR|FATAL)\b")


def emulator_log_failures(text, kill_offset):
    """The emulator log's diagnostics judged against the declared shutdown: `text` is the whole log
    up to the emulator's exit, decoded; `kill_offset` is an index into that same `text` (characters,
    not bytes): its length when the run issued `emu kill`, captured at that moment on the same text.
    Returns the failures, named; empty when the log is clean."""
    failures, seen = [], set()
    for line in text[:kill_offset].splitlines():
        if _EMULATOR_DIAGNOSTIC.search(line):
            failures.append(f"before the emulator kill: {line}")
    for line in text[kill_offset:].splitlines():
        if not _EMULATOR_DIAGNOSTIC.search(line):
            continue
        which = next((i for i, form in enumerate(EMULATOR_SHUTDOWN_WARNINGS) if form.fullmatch(line)), None)
        if which is None:
            failures.append(f"after the emulator kill, undeclared: {line}")
        elif which in seen:
            failures.append(f"after the emulator kill, declared more than once: {line}")
        seen.add(which)
    return failures


# `adb version`, as the dedicated SDK's adb prints it (observed): four lines, nothing on stderr.
_ADB_VERSION_LINES = (
    ("Android Debug Bridge version <n>.<n>.<n>", re.compile(r"Android Debug Bridge version ([0-9]+\.[0-9]+\.[0-9]+)")),
    ("Version <n>.<n>.<n>-<build>", re.compile(r"Version ([0-9]+\.[0-9]+\.[0-9]+-[0-9]+)")),
    (None, None),                                          # "Installed as <the declared adb>", exactly
    ("Running on <host>", re.compile(r"Running on \S.*")))


def adb_version_receipt(r, adb):
    """({"bridge", "version"}, []) when `r` (a command result) is the local `adb version` receipt of
    the declared `adb`; otherwise (None, the departures named). It says nothing about any server."""
    if r.get("timeout"):
        return None, ["timed out"]
    wrong = [] if r.get("rc") == 0 else [f"rc {r.get('rc')}, expected 0"]
    err = (r.get("stderr") or "").splitlines()
    if err:
        wrong.append(f"stderr {err!r}, expected none")
    lines = (r.get("stdout") or "").splitlines()
    if len(lines) != len(_ADB_VERSION_LINES):
        return None, wrong + [f"{len(lines)} lines, expected {len(_ADB_VERSION_LINES)}"]
    found = []
    for n, (line, (shape, pattern)) in enumerate(zip(lines, _ADB_VERSION_LINES), 1):
        if pattern is None:
            want = f"Installed as {adb}"
            if line != want:
                wrong.append(f"line {n} is {line!r}, expected {want!r}")
            continue
        match = pattern.fullmatch(line)
        if match is None:
            wrong.append(f"line {n} is {line!r}, expected {shape!r}")
        elif match.groups():
            found.append(match.group(1))
    if wrong:
        return None, wrong
    return {"bridge": found[0], "version": found[1]}, []


# ------------------------------------------------------------------ relay readiness
@dataclass(frozen=True)
class RelayIdentity:
    """The listener the run configured. Readiness is this listener's record,
    never a token found in some line."""
    listener: str
    host: str
    port: int
    alpn_set: str
    lanes: int

    def fields(self):
        return {"schema": "RELAY_READY_V1", "listener": self.listener, "host": self.host, "port": self.port,
                "alpn_set": self.alpn_set, "lanes": self.lanes}

    def describe(self):
        return f"{self.listener} {self.host}:{self.port} {self.alpn_set} lanes {self.lanes}"


def relay_ready_matches(rec, identity):
    """Every identity field present with the same JSON type and value."""
    return isinstance(rec, dict) and all(type(rec.get(k)) is type(v) and rec.get(k) == v
                                         for k, v in identity.fields().items())


def relay_ready_record(line, identity):
    """The parsed record when `line` is one JSON object that is the declared
    listener's RELAY_READY_V1; otherwise None."""
    if not line.startswith("{"):
        return None
    try:
        rec = json.loads(line)
    except ValueError:
        return None
    return rec if relay_ready_matches(rec, identity) else None


# ------------------------------------------------------------------ the control gate
class ControlGate:
    """Holds timestamped facts and forwards each control exactly once, in
    order. A control's deadline is its anchor fact's time plus its budget;
    the anchor's recorded time must lie within the budget of the previous
    control's forwarding. A refusal (a defective publisher write stream)
    refuses every later control. Facts arrive from reader threads."""

    BARRIERS = {"go": ("host_active", "player_established", "route_two_subscriptions"),
                "ack-all": ("host_all_matched",), "ended-observed": ("host_ended",),
                "done": ("host_complete", "player_settled")}
    ANCHORS = {"go": "publisher_ready", "ack-all": "publisher_wrote_all",
               "ended-observed": "publisher_end_track", "done": "publisher_complete"}

    def __init__(self, forward, clock=None):
        self.forward, self.clock = forward, clock or MonotonicClock()
        self.cv = threading.Condition(threading.RLock())
        self.facts, self.times, self.sent, self.basis, self.log = {}, {}, [], {}, []
        self.released, self.refusals = [], []          # refusals: (first refused control index, reason), in arrival order

    def fact(self, name, value=True):
        with self.cv:
            now = self.clock.now()
            if name not in self.facts:
                self.facts[name], self.times[name] = value, now
            self.log.append((round(now, 6), name))
            self.cv.notify_all()

    def refuse(self, reason, from_control=None):
        """Refuse every control from `from_control` on (default: every control not yet sent).
        An earlier control is unaffected, and a waiter for it is not ended by this refusal.
        Every refusal is kept and logged; none replaces or narrows another (see _refusal)."""
        with self.cv:
            scope = CONTROLS.index(from_control) if from_control is not None else 0
            if (scope, reason) not in self.refusals:
                self.refusals.append((scope, reason))
                self.log.append((round(self.clock.now(), 6), "refused: " + reason))
            self.cv.notify_all()

    def _refusal(self, control):
        """The reason `control` is refused, or None: of the refusals that reach it, the one with
        the earliest scope decides, and the first observed within that scope."""
        here = CONTROLS.index(control)
        reaching = [(scope, i, reason) for i, (scope, reason) in enumerate(self.refusals) if here >= scope]
        return min(reaching)[2] if reaching else None

    def _refuses(self, control):
        return self._refusal(control) is not None

    def is_released(self, control):
        with self.cv:
            return control in self.released

    def _await(self, names, deadline, control):
        """Missing facts once `deadline` is reached or a refusal of `control`
        lands; [] as soon as all hold."""
        while True:
            missing = [n for n in names if not self.facts.get(n)]
            now = self.clock.now()
            if self._refuses(control) or not missing or now >= deadline:
                return missing
            self.clock.wait(self.cv, deadline - now)

    def _check_refused(self, control):
        reason = self._refusal(control)
        if reason is not None:
            raise PhaseFailure(f"{control}: refused: {reason}")

    def send(self, control, budget):
        with self.cv:
            expected = CONTROLS[len(self.sent)] if len(self.sent) < len(CONTROLS) else None
            if control != expected:
                raise PhaseFailure(f"control order: {control} requested but {expected} is next")
            self._check_refused(control)
            anchor = self.ANCHORS[control]
            prev = self.sent[-1] if self.sent else None
            anchor_by = self.basis[prev]["sent_t"] + budget if prev else self.clock.now()
            missing = self._await((anchor,), anchor_by, control)
            self._check_refused(control)
            if missing:
                raise PhaseFailure(f"{control}: anchor {anchor} not observed within {budget}s of {prev or 'the gate'}")
            anchor_t = self.times[anchor]
            if prev is not None and anchor_t > anchor_by:
                raise PhaseFailure(f"{control}: anchor {anchor} observed {anchor_t - anchor_by:.3f}s after its {budget}s window from {prev}")
            deadline = anchor_t + budget
            missing = self._await(self.BARRIERS[control], deadline, control)
            self._check_refused(control)
            if missing:
                raise PhaseFailure(f"{control}: barrier(s) {', '.join(missing)} not observed within {budget}s of {anchor}")
            now = self.clock.now()
            if now > deadline:
                raise PhaseFailure(f"{control}: deadline {budget}s from {anchor} passed {now - deadline:.3f}s before forwarding")
            sent_t = now
            self.released.append(control)
            if not self.forward(control):
                raise PhaseFailure(f"{control}: forwarding to the publisher failed")
            self.sent.append(control)
            self.basis[control] = {"anchor": anchor, "anchor_t": anchor_t, "budget": budget, "deadline": deadline,
                                   "sent_t": sent_t}


# ------------------------------------------------------------------ publisher events
AV_LIFECYCLE = ["connected", "ready", "demand", "demand_audio", "end_track", "end_track_audio", "complete", "stats",
                "demand_log", "demand_log_audio", "shutdown"]


def _merged_in_pts_order(writes):
    """Each write at or after the one before it in pts, video first when two share a pts."""
    for a, b in zip(writes, writes[1:]):
        if b.get("pts_us") < a.get("pts_us"):
            return False
        if b.get("pts_us") == a.get("pts_us") and a.get("event") == "wrote_audio" and b.get("event") == "wrote":
            return False
    return True


def publisher_checks(events, n_objects, n_audio=None):
    """Judged from the publisher's JSON event records. Expected media comes
    from the objects file elsewhere; these checks only judge the publisher's
    own lifecycle, with the completion request and the teardown separate.
    With `n_audio` (the av-diagnostic) the same keys judge both tracks: the
    exact two-track lifecycle, each ledger's writes admitted in order with its
    own indices, the whole stream merged by pts (video first on ties), and each
    track ended once. Without it the video-only judgment is unchanged."""
    by, names = {}, [e.get("event") for e in events]
    for e in events:
        by.setdefault(e.get("event"), e)
    lifecycle = [n for n in names if n not in (("wrote",) if n_audio is None else ("wrote", "wrote_audio"))]
    wrote = [e for e in events if e.get("event") == "wrote"]
    video_ok = len(wrote) == n_objects and all(w.get("rc") == 0 for w in wrote) \
        and [w.get("index") for w in wrote] == list(range(n_objects))
    if n_audio is None:
        lifecycle_ok, writes_ok = lifecycle == ["connected", "ready", "demand", "end_track", "complete", "stats", "demand_log", "shutdown"], video_ok
        end_ok = (by.get("end_track") or {}).get("rc") == 0
    else:
        audio = [e for e in events if e.get("event") == "wrote_audio"]
        audio_ok = len(audio) == n_audio and all(w.get("rc") == 0 and w.get("track") == "audio" for w in audio) \
            and [w.get("index") for w in audio] == list(range(n_audio))
        merged = [e for e in events if e.get("event") in ("wrote", "wrote_audio")]
        lifecycle_ok = lifecycle == AV_LIFECYCLE and (by.get("demand_audio") or {}).get("track") == "audio"
        writes_ok = video_ok and audio_ok and _merged_in_pts_order(merged)
        end_ok = (by.get("end_track") or {}).get("rc") == 0 and (by.get("end_track_audio") or {}).get("rc") == 0 \
            and (by.get("end_track_audio") or {}).get("track") == "audio"
    return {
        "publisher_lifecycle_exact": lifecycle_ok,
        "publisher_writes_admitted": writes_ok,
        "publisher_end_track_rc_ok": end_ok,
        "publisher_complete_rc_ok": (by.get("complete") or {}).get("rc") == 0,
        "publisher_shutdown_ok": (by.get("shutdown") or {}).get("drain_rc") == 0 and (by.get("shutdown") or {}).get("stop_rc") == 0
            and (by.get("shutdown") or {}).get("fatal") == 0 and (by.get("shutdown") or {}).get("exit") == 0,
    }


# ------------------------------------------------------------------ the final verdict
REQUIRED_CHECKS = ("publisher_lifecycle_exact", "publisher_writes_admitted", "publisher_end_track_rc_ok",
                   "publisher_complete_rc_ok", "publisher_shutdown_ok", "controls_forwarded_in_order",
                   "relay_ready_identity", "all_children_settled", "host_stdout_exact", "player_log_exact")


def relay_evidence_refusal(children, judge):
    """None when this run's one relay settled and `judge` accepts its settled record; otherwise the
    refusal. Only the record of the relay this run owned is judged; nothing else stands in for it."""
    settled = [c for c in children if c.get("name") == "relay"]
    if len(settled) != 1 or settled[0].get("settle_error") or settled[0].get("exit") is None:
        return f"no settled record of this run's relay ({len(settled)} relay records)"
    try:
        return judge(settled[0])
    except Exception as e:
        return f"the relay evidence judge raised {e!r}"


def final_verdict(rec, expected_exits):
    """The only pass/fail of a supervised run. Every required check must be
    literally True (a missing one fails), the settled children must be exactly
    the owned inventory, each ending the declared way (an expected SIGTERM is
    distinct from exiting by itself), with readers settled and no reader,
    write or settle error, probe threads settled, and no unplanned diagnostic
    line from any child."""
    failures = []
    if rec.get("primary_error") is not None:
        failures.append(f"primary: {rec['primary_error']}")
    checks = rec.get("checks") or {}
    for k in REQUIRED_CHECKS:
        if checks.get(k) is not True:
            failures.append(f"check {k} is {checks.get(k)!r}")
    if rec.get("threads_settled") is not True:
        failures.append("probe threads not settled")
    if rec.get("relay_evidence") is not None:
        failures.append(f"relay evidence: {rec['relay_evidence']}")
    failures.extend(f"interrupted during {i['where']}" for i in rec.get("interruptions") or [])
    if (rec.get("player_diagnostic") or {}).get("spawn_error"):
        failures.append(f"player diagnostic: spawn failed: {rec['player_diagnostic']['spawn_error']}")
    failures.extend(f"retained: {r['name']} pid {r['pid']}: {r['why']}" for r in rec.get("retained") or [])
    children = rec.get("children") or []
    names = sorted(str(c.get("name")) for c in children)
    if names != sorted(expected_exits):
        failures.append(f"child inventory {names} != {sorted(expected_exits)}")
    for c in children:
        name, want = c.get("name"), expected_exits.get(c.get("name"))
        if want is not None and (c.get("how"), c.get("exit")) != want:
            failures.append(f"{name}: {c.get('how')} rc {c.get('exit')}, expected {want[0]} rc {want[1]}")
        if c.get("readers_settled") is not True:
            failures.append(f"{name}: readers not settled")
        for key in ("reader_errors", "write_errors"):
            if c.get(key):
                failures.append(f"{name}: {key.replace('_', ' ')} {c[key]}")
        if c.get("settle_error"):
            failures.append(f"{name}: settle error {c['settle_error']}")
        failures.extend(f"unplanned diagnostic: {d}" for d in collect_diagnostics(c))
    return {"ok": not failures, "failures": failures}


# ------------------------------------------------------------------ one supervised run
@dataclass
class Actors:
    relay_argv: list
    publisher_argv: list
    host_argv: list
    player_probe: object          # callable(gate, stop_event) run on its own thread; reports player facts
    route_barrier: object         # callable(host_active, player_started, record) -> (probe(gate, relay, stop), baseline_recorded)
    player_start: object          # callable(stop_event, dispatched, admit) -> None once started, or a failure reason;
                                  # it calls admit() once (None, or a refusal it must return) after its own checks,
                                  # then dispatched() just before its final start action (the Start tap);
                                  # dispatched() raises unless that one admit() said yes (a repeat revokes it)
    player_install: object        # callable(stop_event, budget_s) -> None, or a failure reason; before the relay
    player_prepare: object        # callable(stop_event, budget_s) -> None, or a failure reason; before the publisher
    player_log_argv: object       # callable() -> argv of the player's receipt stream, an owned child ("player-log")
    relay_identity: RelayIdentity
    expected_exits: dict          # child name -> (settle `how`, exit status)
    env: dict = field(default_factory=lambda: {"PATH": "/usr/bin:/bin"})
    relay_env: dict = None        # variables the relay child alone receives, laid over a copy of `env`
    relay_evidence: object = None  # callable(the relay's settled record) -> None, or the refusal; when set, the
                                   # final verdict requires this run's own relay to have settled and passed it
    player_diag_argv: object = None  # callable() -> argv of the player's diagnostic log stream, an owned child
                                     # ("player-diag") whose bytes go to a file and never to any judge
    player_diag_bound: int = None    # that file's byte bound (None: PLAYER_DIAG_BOUND)
    audio_objects: int = None        # the av-diagnostic's declared audio object count; None: video-only
    relay_snapshot: object = None    # callable(the owned relay child) -> record, taken at retirement before the
                                     # relay is settled; evidence only
    # Offline scheduling points, keyed by name; each is called on the thread that reaches it:
    #   host_line(what)          host stdout reader, before the control's fact is recorded
    #   player_log_line(line)    player-log stdout reader, before the line is judged
    #   pub_event(event)         publisher stdout reader, before a JSON event is recorded
    #   host_exit(rc)            host watcher, when it first observes the host process exit
    #   host_exit_draining()     host watcher, before it waits for the host's stdout reader to drain
    #   host_exit_classified()   host watcher, after deciding whether the exit left a fact missing
    #   host_exit_drain_refused()  host watcher, after refusing because the host's output did not drain
    #   admitting(relay)         handoff thread, inside admit(), before the relay is judged
    #   stopping(primary)        supervisor, once the run has stopped, before the probe threads are joined
    #   joining(thread)          supervisor, before each probe thread's bounded join
    #   before_settle()          supervisor, after the run and its probes, before any child is settled
    #   forwarded(control)       supervisor, after a control was written to the publisher
    hooks: dict = field(default_factory=dict)


PLAYER_DIAG_BOUND = 16 * 1024 * 1024
PLAYER_DIAG_FILE = "player-diagnostic.log"


class DiagnosticChild(Child):
    """A directly owned child whose stdout is copied, exact bytes, into `path` (created exclusively) up to
    `bound` bytes; what comes after the bound is counted, not kept. None of it reaches a judge, a fact or
    the warning rule. Stderr is read as `Child` reads it; settlement is `Child.settle`."""

    def __init__(self, name, argv, env, cwd, path, bound):
        self.name, self.argv = name, [str(a) for a in argv]
        self.stdout_lines, self.stderr_lines, self.reader_errors, self.write_errors = [], [], [], []
        self.on_line, self.path, self.bound, self.kept, self.dropped = None, Path(path), bound, 0, 0
        self.sink = open(self.path, "xb")
        try:
            self.proc = subprocess.Popen(self.argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd=cwd)
        except BaseException:
            self.sink.close()
            raise
        self.readers = [threading.Thread(target=self._copy, args=(self.proc.stdout,), daemon=True),
                        threading.Thread(target=self._reader, args=(io.TextIOWrapper(self.proc.stderr), self.stderr_lines, False),
                                         daemon=True)]
        for t in self.readers:
            t.start()
        self.settled = None

    def _copy(self, pipe):
        try:
            for chunk in iter(lambda: pipe.read1(65536), b""):
                keep = chunk[:max(self.bound - self.kept, 0)]
                if keep:
                    self.sink.write(keep)
                    self.kept += len(keep)
                self.dropped += len(chunk) - len(keep)
        except Exception as e:
            self.reader_errors.append(f"{self.name}: {e!r}")
        finally:
            pipe.close()
            self.sink.close()


class RawStdoutChild(Child):
    """A directly owned child whose stdout reaches `on_line` as the exact bytes of each line, its LF
    included and nothing decoded or translated, so a CR, a missing final LF or a non-ASCII byte stays
    visible to the caller. Stderr is read as `Child` reads it; settlement is `Child.settle`."""

    def __init__(self, name, argv, env, cwd, on_line):
        self.name, self.argv = name, [str(a) for a in argv]
        self.stdout_lines, self.stderr_lines, self.reader_errors, self.write_errors = [], [], [], []
        self.on_line = on_line
        self.proc = subprocess.Popen(self.argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd=cwd)
        self.readers = [threading.Thread(target=self._raw_reader, args=(self.proc.stdout,), daemon=True),
                        threading.Thread(target=self._reader, args=(io.TextIOWrapper(self.proc.stderr), self.stderr_lines, False),
                                         daemon=True)]
        for t in self.readers:
            t.start()
        self.settled = None

    def _raw_reader(self, pipe):
        try:
            for raw in iter(pipe.readline, b""):
                self.stdout_lines.append(raw.decode("ascii", "backslashreplace").removesuffix("\n"))
                self.on_line(raw)
        except Exception as e:
            self.reader_errors.append(f"{self.name}: {e!r}")
        finally:
            pipe.close()


def supervise(run_dir, actors, deadlines, n_objects, clock=None):
    run_dir = Path(run_dir)
    clock = clock or MonotonicClock()
    identity = actors.relay_identity
    rec = {"primary_error": None, "children": [], "controls_forwarded": [], "publisher_events": [], "facts": [],
           "relay_ready_record": None, "relay_ready_other": [], "deadline_basis": {}, "event_stamps": {},
           "write_stream_error": None, "write_anchor_index": None, "checks": {}, "threads_settled": None,
           "host_stdout": [], "host_stdout_violations": [], "sequence": [], "route_dumps": [], "player_log_violations": [],
           "interruptions": [], "retained": [], "verdict": None}
    children, threads, stop = [], [], threading.Event()
    diag = relay = None
    relay_ready = StampedEvent(clock, "relay_ready")
    pub_ready = StampedEvent(clock, "publisher_ready")
    pub_shutdown = StampedEvent(clock, "publisher_shutdown")
    ledger = WriteLedger(n_objects)
    pub = None
    host_active, player_started, start_dispatched = threading.Event(), threading.Event(), threading.Event()

    def note(kind, what):
        rec["sequence"].append({"t": round(clock.now(), 6), "kind": kind, "what": what})

    def interrupted(where, e):
        """An operator interruption: recorded (the first one is the run's primary failure when nothing
        failed before it), and the retirement it arrived in goes on."""
        rec["interruptions"].append({"where": where, "what": repr(e)})
        if rec["primary_error"] is None:
            rec["primary_error"] = f"interrupted during {where}: {e!r}"
        note("control", f"interrupted during {where}")

    def retire(where, call):
        """`call` -- one bounded retirement step -- attempted again once if an interruption cuts it
        short; True once it completed, False after a second interruption."""
        for _attempt in range(2):
            try:
                call()
                return True
            except KeyboardInterrupt as e:
                interrupted(where, e)
        return False

    def settle_owned(c, sig):
        """The child's settled record: its bounded settlement, attempted again once if an interruption
        cuts it short. A child not proven reaped is named in `retained`, never recorded as exited."""
        for _attempt in range(2):
            try:
                return c.settle(deadlines.settle, signal_first=sig)
            except KeyboardInterrupt as e:
                interrupted(f"settling {c.name}", e)
            except Exception as e:
                rec["retained"].append({"name": c.name, "pid": c.proc.pid, "why": f"settle raised {e!r}; not proven reaped"})
                return {"name": c.name, "pid": c.proc.pid, "settle_error": repr(e), "settled": False}
        why = "settlement interrupted twice; not proven reaped"
        rec["retained"].append({"name": c.name, "pid": c.proc.pid, "why": why})
        return {"name": c.name, "pid": c.proc.pid, "settle_error": why, "settled": False}

    def forward(control):
        ok = pub is not None and pub.write_line(control)
        if ok:
            note("control", control)
        if ok and hook("forwarded"):
            hook("forwarded")(control)
        rec["controls_forwarded"].append({"line": control, "forwarded": ok, "t": round(clock.now(), 6)})
        return ok

    gate = ControlGate(forward, clock)

    def relay_line(line):
        if relay_ready.is_set():
            return
        match = relay_ready_record(line, identity)
        if match is not None:
            rec["relay_ready_record"] = match
            relay_ready.set()
        elif "RELAY_READY_V1" in line:
            rec["relay_ready_other"].append(line)

    def pub_line(line):
        if not line.startswith("{"):
            return
        try:
            ev = json.loads(line)
        except ValueError:
            return
        if hook("pub_event"):
            hook("pub_event")(ev)
        rec["publisher_events"].append(ev)
        name = ev.get("event")
        if name == "ready":
            gate.fact("publisher_ready")
            pub_ready.set()
        elif name == "wrote":
            outcome = ledger.admit(ev, released=gate.is_released("go"))
            if outcome == "complete":
                rec["write_anchor_index"] = ev.get("index")
                gate.fact("publisher_wrote_all")
            elif outcome == "error":
                gate.refuse(f"publisher write stream: {ledger.error}")
        elif name == "end_track":
            gate.fact("publisher_end_track")
        elif name == "complete":
            gate.fact("publisher_complete")
        elif name == "shutdown":
            pub_shutdown.set()

    hook = actors.hooks.get

    def host_line(raw):
        # The host's stdout is exactly the four control lines, byte-exact -- ASCII text and one LF --
        # once each, in order. Any other line is a violation recorded for the verdict and refused at
        # once, even after `done`, when no later control could observe the refusal.
        n = len(rec["host_stdout"]) + 1
        rec["host_stdout"].append(raw)
        expected = f"CTRL {CONTROLS[n - 1]}\n".encode() if n <= len(CONTROLS) else None
        if raw != expected:
            line = raw[:-1].decode("ascii") if raw.endswith(b"\n") and raw.isascii() else None
            if not raw.endswith(b"\n"):
                reason = f"host-receiver stdout line {n} is not LF-terminated: {raw!r}"
            elif b"\r" in raw:
                reason = f"host-receiver stdout line {n} contains CR: {raw!r}"
            elif line is None:
                reason = f"host-receiver stdout line {n} is not ASCII: {raw!r}"
            elif not line.startswith("CTRL "):
                reason = f"host-receiver wrote a non-control line to stdout: {line!r}"
            elif expected is None:
                reason = f"host-receiver control line {n} is {line!r}, after 'CTRL done'"
            else:
                reason = f"host-receiver control line {n} is {line!r}, expected {expected[:-1].decode()!r}"
            rec["host_stdout_violations"].append(reason)
            gate.refuse(reason)
            return
        what = CONTROLS[n - 1]
        if hook("host_line"):
            hook("host_line")(what)
        gate.fact({"go": "host_active", "ack-all": "host_all_matched", "ended-observed": "host_ended",
                   "done": "host_complete"}[what])
        note("host", what)
        if what == "go":
            host_active.set()

    def judge(what, ev, deadline, budget, since, child=None):
        """A stamped wait with every outcome named."""
        outcome = await_stamped(ev, deadline, clock, alive=child.alive if child is not None else None)
        rec["event_stamps"][what] = {"stamp": ev.t, "deadline": deadline, "outcome": outcome}
        if outcome == "exited":
            raise PhaseFailure(f"{child.name} exited before {what}: rc {child.proc.wait(deadlines.settle)}")
        if outcome == "absent":
            raise PhaseFailure(f"{what} not observed within {budget}s{since}")
        if outcome == "late":
            raise PhaseFailure(f"{what} observed {ev.t - deadline:.3f}s after its {budget}s window{since}")

    class Inventory(list):
        """The route probe's judged dumps, each also entered in the ordered inventory."""
        def append(self, r):
            super().append(r)
            note("route", f"{r['phase']} {r['verdict']}")

    class RouteGate:
        def fact(self, name):
            note("route", "fact")
            gate.fact(name)

        def refuse(self, reason):
            note("route", f"refused: {reason}")
            gate.refuse(reason)

    class RelayView:
        """The relay as the route probe sees it: its stderr, its liveness, and its signals, each
        dump request entered in the inventory before it is sent."""
        stderr_lines = property(lambda self: relay.stderr_lines)

        def __init__(self):
            self.proc = self

        def alive(self):
            return relay.alive()

        def send_signal(self, sig):
            note("route", "request")
            relay.proc.send_signal(sig)

    class PlayerGate:
        """The player's reports: none may precede the dispatch of the player's final start action."""
        def __getattr__(self, name):
            return getattr(gate, name)

        def fact(self, name):
            note("player", name)
            if name == "player_settled":
                gate.refuse("the player probe reported player_settled; only its receipts settle the player")
                return
            if not start_dispatched.is_set():
                gate.refuse(f"player reported {name} before its start was dispatched")
                return
            gate.fact(name)
            settle_check()

    receipts, settle_lock = {"presented": 0, "presented_ok": False, "closed": 0}, threading.Lock()

    def settle_check():
        """`player_settled` holds only with the player's own terminal line and both receipts, once."""
        with settle_lock:
            ready = (gate.facts.get("player_stopped") and receipts["presented_ok"] and receipts["closed"] == 1
                     and receipts["presented"] == 1 and not gate.facts.get("player_settled"))
            if ready:
                note("player", "player_settled")
                gate.fact("player_settled")

    def player_log_violation(reason):
        rec["player_log_violations"].append(reason)
        note("player_log", f"refused: {reason}")
        gate.refuse(reason)

    def player_log_line(line):
        """Each line of the player's receipt stream, judged strictly. A fault here is named and refused:
        the child's reader would otherwise swallow it."""
        try:
            if hook("player_log_line"):
                hook("player_log_line")(line)
            try:
                parsed = player_receipts.parse_line(line)
            except player_receipts.ReceiptError as e:
                player_log_violation(f"player log: {e}")
                return
            if parsed is None:
                return
            kind, fields = parsed
            with settle_lock:
                if kind == "presented":
                    receipts["presented"] += 1
                    if receipts["presented"] > 1:
                        reason = "player log: a second presented receipt"
                    else:
                        rejection = player_receipts.presented_rejection(fields, n_objects)
                        reason = f"player log: presented receipt does not accept: {rejection}" if rejection else None
                        receipts["presented_ok"] = rejection is None
                else:
                    receipts["closed"] += 1
                    reason = ("player log: observer-closed before presented" if not receipts["presented"]
                              else "player log: a second observer-closed receipt" if receipts["closed"] > 1 else None)
            if reason:
                player_log_violation(reason)
                return
            note("player_log", kind)
            settle_check()
        except Exception as e:
            player_log_violation(f"player log: {e!r} on {line!r}")

    def phase(kind, what, fn, budget):
        """A player preparation step before the publisher: bounded by `budget`, named on failure."""
        note(kind, "begin")
        began = clock.now()
        try:
            failure = fn(stop, budget)
        except Exception as e:
            failure = f"raised {e!r}"
        if failure is None and clock.now() - began > budget:
            failure = f"took {clock.now() - began:.3f}s, over its {budget}s bound"
        if failure is not None:
            note(kind, f"failed: {failure}")
            raise PhaseFailure(f"{what}: {failure}")
        note(kind, "done")

    def player_log_watch(child, stop):
        """The player's receipt stream runs until the supervisor stops it; any earlier exit is named."""
        while not stop.is_set():
            rc = child.proc.poll()
            if rc is not None:
                note("player_log", f"exited rc {rc}")
                gate.refuse(f"player log exited (rc {rc}) before the run stopped")
                return
            stop.wait(0.02)

    def guarded(what, target, refuse):
        """`target` for a probe thread: an ordinary exception it raises becomes a named refusal of
        every control not yet sent, instead of an uncaught thread exception and a later timeout."""
        def run(*args):
            try:
                target(*args)
            except Exception as e:
                refuse(f"{what} raised {e!r}")
        run.__name__ = getattr(target, "__name__", what)
        return run

    rec["route_dumps"] = Inventory()

    try:
        # The route barrier is an actor's callback: a fault building it is a named harness failure.
        route_probe, baseline_recorded = actors.route_barrier(host_active, player_started, rec["route_dumps"])
        phase("player_install", "player install", actors.player_install, deadlines.player_install)
        start = clock.now()
        relay_env = dict(actors.env, **actors.relay_env) if actors.relay_env else actors.env
        relay = Child("relay", actors.relay_argv, relay_env, str(run_dir), on_line=relay_line)
        children.append(relay)
        judge(f"relay readiness ({identity.describe()})", relay_ready, start + deadlines.relay_ready,
              deadlines.relay_ready, "", relay)
        phase("player_prepare", "player prepare", actors.player_prepare, deadlines.player_prepare)
        try:
            player_log = Child("player-log", actors.player_log_argv(), actors.env, str(run_dir), on_line=player_log_line)
        except Exception as e:
            note("player_log", f"spawn failed: {e!r}")
            raise PhaseFailure(f"player log: spawn failed: {e!r}") from None
        children.append(player_log)
        note("player_log", "started")
        if actors.player_diag_argv is not None:
            # Evidence only: a failure to start it is named, and the run goes on without it.
            bound = actors.player_diag_bound or PLAYER_DIAG_BOUND
            path = run_dir / PLAYER_DIAG_FILE
            try:
                diag = DiagnosticChild("player-diag", actors.player_diag_argv(), actors.env, str(run_dir), path, bound)
            except Exception as e:
                rec["player_diagnostic"] = {"spawn_error": repr(e)}
                note("player_diag", f"spawn failed: {e!r}")
            else:
                children.append(diag)
                rec["player_diagnostic"] = {"path": str(path), "bound": bound}
                note("player_diag", "started")
        watcher = threading.Thread(target=player_log_watch, args=(player_log, stop), daemon=True)
        watcher.start()
        threads.append(watcher)
        start = clock.now()
        pub = Child("publisher", actors.publisher_argv, actors.env, str(run_dir), stdin=True, on_line=pub_line)
        children.append(pub)
        judge("ready", pub_ready, start + deadlines.publisher_ready, deadlines.publisher_ready, "", pub)
        host = RawStdoutChild("host-receiver", actors.host_argv, actors.env, str(run_dir), on_line=host_line)
        children.append(host)
        clock.mark("host_spawned")

        def host_watch(gate, stop):
            """A host that exits before its last promised fact refuses every later control by name.
            Exit can precede the reader's callback for a line already in the pipe, so the missing
            fact is named only after the host's stdout reader has reached EOF and settled; that wait
            is bounded and holds no gate lock. A fact still lands at its own reader time, so its
            control's deadline judges it as before."""
            while not stop.is_set():
                rc = host.proc.poll()
                if rc is not None:
                    note("host", f"exit observed rc {rc}")
                    if hook("host_exit"):
                        hook("host_exit")(rc)
                    # The rule is an observed exit before `go` is released: `go` is released under the gate's
                    # lock, so this check and a release cannot interleave. It does not make an exit this
                    # watcher has not yet observed impossible while `go` is being forwarded.
                    with gate.cv:
                        in_handoff = host_active.is_set() and not gate.is_released("go")
                    if in_handoff:
                        gate.refuse(f"host-receiver exited (rc {rc}) during the route handoff")
                    if hook("host_exit_draining"):
                        hook("host_exit_draining")()
                    stdout_reader = host.readers[0]
                    stdout_reader.join(deadlines.settle)
                    if stdout_reader.is_alive():
                        gate.refuse(f"host-receiver exited (rc {rc}) and its output did not drain within {deadlines.settle}s")
                        if hook("host_exit_drain_refused"):
                            hook("host_exit_drain_refused")()
                        return
                    with gate.cv:
                        missing = [f for f in ("host_active", "host_all_matched", "host_ended", "host_complete")
                                   if not gate.facts.get(f)]
                    if missing:
                        # Only the control whose fact is missing, and those after it, are refused.
                        gate.refuse(f"host-receiver exited (rc {rc}) before {missing[0]}",
                                    from_control=HOST_FACT_CONTROL[missing[0]])
                    if hook("host_exit_classified"):
                        hook("host_exit_classified")()
                    return
                stop.wait(0.02)
        def handoff(gate, stop):
            """Starts the player once the route baseline holds and the host is live. A report is
            admissible from the start dispatch on; the route's after-start dump waits for success."""
            while not baseline_recorded.is_set():
                if stop.is_set():
                    return
                baseline_recorded.wait(0.02)
            if host.proc.poll() is not None:
                return                                        # the host watcher names the handoff exit

            decision = None                       # admission's one decision: None until asked, then "admitted" or "refused"

            def admit():
                """The last check before the start action: the retained relay child is still running.
                None admits; otherwise the reason, and dispatched() stays closed. Admission is asked
                once: a second request is refused and revokes any earlier admission."""
                nonlocal decision
                note("player_start", "admit")
                if decision is not None:
                    decision = "refused"
                    reason = "admission was already asked once"
                    note("player_start", f"admit refused: {reason}")
                    return reason
                if hook("admitting"):
                    hook("admitting")(relay)
                rc = relay.proc.poll()
                if rc is not None:
                    decision = "refused"
                    reason = f"relay exited (rc {rc}) before the Start dispatch"
                    note("player_start", f"admit refused: {reason}")
                    return reason
                decision = "admitted"
                note("player_start", "admitted")
                return None

            def dispatched():
                if decision != "admitted":
                    note("player_start", "dispatched without admission")
                    raise RuntimeError("the start was dispatched without admission")
                note("player_start", "dispatched")
                start_dispatched.set()
            note("player_start", "begin")
            try:
                failure = actors.player_start(stop, dispatched, admit)
            except Exception as e:                    # a start that raises is a named refusal, not a lost thread
                failure = f"raised {e!r}"
            if failure is not None:
                note("player_start", f"failed: {failure}")
                gate.refuse(f"player start failed: {failure}")
                return
            note("player_start", "started")
            player_started.set()

        player_gate, route_gate = PlayerGate(), RouteGate()
        player_probe = guarded("player probe", actors.player_probe, gate.refuse)
        route_probe = guarded("route probe", route_probe, route_gate.refuse)
        for fn, args in ((player_probe, (player_gate, stop)), (route_probe, (route_gate, RelayView(), stop)),
                         (handoff, (gate, stop)), (host_watch, (gate, stop))):
            t = threading.Thread(target=fn, args=args, daemon=True)
            t.start()
            threads.append(t)
        gate.send("go", deadlines.go_window)
        gate.send("ack-all", deadlines.ack_all)
        gate.send("ended-observed", deadlines.ended_observed)
        gate.send("done", deadlines.done)
        judge("publisher shutdown record", pub_shutdown, gate.basis["done"]["sent_t"] + deadlines.shutdown,
              deadlines.shutdown, " from done")
    except PhaseFailure as e:
        rec["primary_error"] = str(e)
        note("control", f"stopped: {e}")
    except Exception as e:                     # an unexpected harness fault is still a named failure
        rec["primary_error"] = f"harness: {e!r}"
        note("control", f"stopped: {rec['primary_error']}")
    except KeyboardInterrupt as e:             # the operator's interruption: recorded, and retirement goes on
        interrupted("supervision", e)
    finally:
        # Retirement owns every thread and child whatever interrupts it: each step is bounded, and one
        # an interruption cuts short is attempted once more (the current child included).
        stop.set()
        if hook("stopping"):
            retire("stopping", lambda: hook("stopping")(rec["primary_error"]))
        for t in threads:
            def join(t=t):
                if hook("joining"):
                    hook("joining")(t)
                t.join(deadlines.settle)
            retire("probe-thread retirement", join)
        rec["threads_settled"] = not any(t.is_alive() for t in threads)
        if hook("before_settle"):
            retire("before settle", hook("before_settle"))
        if actors.relay_snapshot is not None and relay is not None and relay.alive():
            def snapshot():
                try:
                    rec["relay_snapshot"] = actors.relay_snapshot(relay)
                except Exception as e:                 # evidence only: retirement goes on
                    rec["relay_snapshot"] = {"error": repr(e)}
            retire("relay snapshot", snapshot)
        for c in reversed(children):
            # Signal first only a child the run declares is ended by SIGTERM; one declared to exit
            # by itself is awaited (bounded), and a SIGTERM after that bound is a named failure.
            sig = signal.SIGTERM if actors.expected_exits.get(c.name, ("exited", 0))[0] == "SIGTERM" else None
            rec["children"].append(settle_owned(c, sig))
        rec["facts"], rec["deadline_basis"] = list(gate.log), dict(gate.basis)
        rec["write_stream_error"] = ledger.error
        if diag is not None:
            rec["player_diagnostic"].update(bytes_kept=diag.kept, bytes_dropped=diag.dropped,
                                            complete=diag.dropped == 0 and not diag.reader_errors and diag.settled is not None
                                            and diag.settled.get("readers_settled") is True)
    if actors.relay_evidence is not None:
        rec["relay_evidence"] = relay_evidence_refusal(rec["children"], actors.relay_evidence)
    rec["checks"] = publisher_checks(rec["publisher_events"], n_objects, actors.audio_objects)
    rec["checks"]["controls_forwarded_in_order"] = [c["line"] for c in rec["controls_forwarded"]] == list(CONTROLS) \
        and all(c["forwarded"] for c in rec["controls_forwarded"])
    rec["checks"]["relay_ready_identity"] = relay_ready_matches(rec["relay_ready_record"], identity)
    rec["checks"]["host_stdout_exact"] = not rec["host_stdout_violations"]
    rec["checks"]["player_log_exact"] = not rec["player_log_violations"]
    rec["checks"]["all_children_settled"] = bool(rec["children"]) and all(
        c.get("exit") is not None and c.get("readers_settled") for c in rec["children"])
    rec["verdict"] = final_verdict(rec, actors.expected_exits)
    return rec


# ------------------------------------------------------------------ offline controls (stub children)
_STUB_PRELUDE = (
    "import socket, sys, time, json\n"
    "def _trip(*a, **k):\n"
    "    raise SystemExit('stub attempted network use')\n"
    "socket.socket.connect = _trip\n"
    "socket.socket.bind = _trip\n"
)

@dataclass(frozen=True)
class HostIdentity:
    """Everything the real host receiver child is told, declared by the run: the installed moq5
    package root, the loopback raw listener URL, the CA file and SNI it verifies against, the
    objects file its expectation is read from, and its two bounds."""
    package: Path
    url: str
    ca_file: Path
    sni: str
    objects: Path
    establish_s: float
    deadline_s: float


HOST_RECEIVER = HERE / "host_receiver.py"

# The real host child exits by itself after `done`; the offline stub sleeps and is SIGTERMed.
# Offline runs pair the real host receiver's exit with the offline player-log stub, which handles SIGTERM.
REAL_HOST_EXITS = {"relay": ("SIGTERM", 0), "publisher": ("exited", 0), "host-receiver": ("exited", 0),
                   "player-log": ("SIGTERM", 0)}
# The real receipt stream is `adb logcat`, which does not handle SIGTERM: it ends at the signal, and
# `Child.settle` reports status -15 (observed on the dedicated emulator). Declared as such, never
# normalized to a success status.
ADB_LOGCAT_EXIT = ("SIGTERM", -15)
LIVE_EXITS = {**REAL_HOST_EXITS, "player-log": ADB_LOGCAT_EXIT}


def host_argv(identity, python=sys.executable):
    """The host child's exact argv for `identity`, after checking every declared input exists and
    the URL is a loopback raw listener. Raises ValueError naming the first bad input. The URL and
    bound rules are the child's own (host_receiver), so the two cannot disagree."""
    import host_receiver
    i = identity
    try:
        if not (Path(i.package) / "moq5" / "__init__.py").is_file():
            raise host_receiver.Refused(f"package: no moq5 package under {str(i.package)!r}")
        host_receiver.check_url(i.url)
        if not Path(i.ca_file).is_file():
            raise host_receiver.Refused(f"ca_file: no CA file at {str(i.ca_file)!r}")
        if not i.sni:
            raise host_receiver.Refused("sni: empty")
        if not Path(i.objects).is_file():
            raise host_receiver.Refused(f"objects: no objects file at {str(i.objects)!r}")
        for name in ("establish_s", "deadline_s"):
            value = getattr(i, name)
            if not (isinstance(value, (int, float)) and math.isfinite(value) and value > 0):
                raise host_receiver.Refused(f"{name}: {value!r} is not a finite positive number of seconds")
        host_receiver.check_bounds(float(i.establish_s), float(i.deadline_s))
    except host_receiver.Refused as refused:
        raise ValueError(str(refused)) from None
    return [str(python), "-I", "-B", str(HOST_RECEIVER), "--package", str(i.package), "--url", i.url, "--ca", str(i.ca_file),
            "--sni", i.sni, "--objects", str(i.objects), "--establish-s", str(float(i.establish_s)),
            "--deadline-s", str(float(i.deadline_s))]


OFFLINE_IDENTITY = RelayIdentity(listener="raw", host="127.0.0.1", port=4443, alpn_set=EXPECTED_ALPN, lanes=1)
OFFLINE_EXITS = {"relay": ("SIGTERM", 0), "publisher": ("exited", 0), "host-receiver": ("SIGTERM", 0),
                 "player-log": ("SIGTERM", 0)}

_RAW_READY = {"schema": "RELAY_READY_V1", "elapsed_us": 0, "listener": "raw", "host": "127.0.0.1", "port": 4443,
              "alpn_set": EXPECTED_ALPN, "lanes": 1}
_SIGNALS_READY = {"schema": "RELAY_READY_V1", "elapsed_us": 0, "listener": "signals", "sigusr1": "metrics+routes",
                  "sigusr2": "trace", "sink": "stderr"}
RELAY_STUB_RECORDS = {
    "ready": [_SIGNALS_READY, _RAW_READY],
    "silent": [],
    "signals-only": [_SIGNALS_READY],
    "wrong-listener": [dict(_RAW_READY, listener="webtransport", path="/moq", profile="current")],
    "wrong-port": [dict(_RAW_READY, port=4444)],
    "echoed-token": [{"schema": "RELAY_RUN_CONFIG_V1", "elapsed_us": 0, "awaits": "RELAY_READY_V1"}],
}

_RELAY_STUB = _STUB_PRELUDE + (
    "import signal\n"
    "records = json.loads(sys.argv[2])[sys.argv[1]]\n"
    "def _stop(*a):\n"
    "    print(json.dumps({'schema': 'RELAY_STOP_V1'}), flush=True); sys.exit(0)\n"
    "signal.signal(signal.SIGTERM, _stop)\n"
    "blocks, served = json.loads(sys.argv[3]), [0]\n"
    "def _routes(*a):\n"
    "    block = blocks[min(served[0], len(blocks) - 1)]; served[0] += 1\n"
    "    sys.stderr.write('\\n===== MOQ5 Relay routes =====\\n' + ''.join(l + '\\n' for l in block) + '\\n'); sys.stderr.flush()\n"
    "signal.signal(signal.SIGUSR1, _routes)\n"
    "for r in records:\n"
    "    print(json.dumps(r, separators=(',', ':')), flush=True)\n"
    "time.sleep(60)\n"
)

_PUBLISHER_STUB = _STUB_PRELUDE + (
    "n = int(sys.argv[1]); mode = sys.argv[2]\n"
    "def emit(o): print(json.dumps(o), flush=True)\n"
    "if mode == 'early-exit':\n"
    "    sys.stderr.write('publisher: fatal\\n'); sys.exit(3)\n"
    "emit({'event': 'connected'})\n"
    "if mode == 'early-write': emit({'event': 'wrote', 'index': 0, 'rc': 0, 'attempts': 1})\n"
    "emit({'event': 'ready'})\n"
    "def want(word):\n"
    "    line = sys.stdin.readline()\n"
    "    if line.strip() != word:\n"
    "        emit({'event': 'shutdown', 'drain_rc': 0, 'stop_rc': 0, 'fatal': 0, 'exit': 4}); sys.exit(4)\n"
    "want('go'); emit({'event': 'demand', 'active': 1, 'query': 1})\n"
    "order = {'dup-write': list(range(n - 1)) + [n - 2, n - 1],\n"
    "         'skip-write': [i for i in range(n) if i != 7]}.get(mode, list(range(n)))\n"
    "for i in order: emit({'event': 'wrote', 'index': i, 'rc': -11 if mode == 'bad-rc-write' and i == 30 else 0, 'attempts': 1})\n"
    "want('ack-all'); emit({'event': 'end_track', 'rc': 0, 'attempts': 1})\n"
    "want('ended-observed'); emit({'event': 'complete', 'rc': 0})\n"
    "want('done'); emit({'event': 'stats', 'rc': 0}); emit({'event': 'demand_log', 'dropped': 0, 'entries': []})\n"
    "drain = -8 if mode == 'bad-drain' else 0\n"
    "emit({'event': 'shutdown', 'drain_rc': drain, 'stop_rc': 0, 'fatal': 0, 'exit': 5 if drain else 0})\n"
    "sys.exit(5 if drain else 0)\n"
)

_HOST_STUB = _STUB_PRELUDE + (
    "import signal\n"
    "signal.signal(signal.SIGTERM, lambda *a: sys.exit(0))\n"
    "withhold = sys.argv[1]\n"
    "exit_after = withhold[len('exit-after-'):] if withhold.startswith('exit-after-') else None\n"
    "if len(sys.argv) > 2 and sys.argv[2] == 'warn':\n"
    "    sys.stderr.write('warning: stub diagnostic\\n'); sys.stderr.flush()\n"
    "if len(sys.argv) > 2 and sys.argv[2] == 'noise':\n"
    "    print('debug: receiver attached', flush=True)\n"
    "variant = sys.argv[2] if len(sys.argv) > 2 else ''\n"
    "release = sys.argv[3] if len(sys.argv) > 3 and sys.argv[2] == 'exit-on-file' else None\n"
    "RAW_GO = {'crlf': b'CTRL go\\r\\n', 'undecodable': b'CTRL go\\xff\\n', 'non-ascii': 'CTRL g\\u00f6\\n'.encode()}\n"
    "for word in ('go', 'ack-all', 'ended-observed', 'done'):\n"
    "    if word == withhold: time.sleep(60)\n"
    "    time.sleep(0.05)\n"
    "    raw = RAW_GO.get(variant) if word == 'go' else (b'CTRL done' if variant == 'no-final-lf' and word == 'done' else None)\n"
    "    if raw is None: print('CTRL ' + word + (' ' if variant == 'bad-ctrl' and word == 'go' else ''), flush=True)\n"
    "    else: sys.stdout.flush(); sys.stdout.buffer.write(raw); sys.stdout.buffer.flush()\n"
    "    if variant == 'no-final-lf' and word == 'done': sys.exit(0)\n"
    "    if variant == 'dup-go' and word == 'go': print('CTRL go', flush=True)\n"
    "    if variant in ('noise-after-done', 'extra-after-done') and word == 'done':\n"
    "        import os\n"
    "        deadline = time.monotonic() + 10\n"
    "        while not os.path.exists(sys.argv[3]) and time.monotonic() < deadline: time.sleep(0.01)\n"
    "        if variant == 'extra-after-done': sys.stdout.buffer.write(b'extra'); sys.stdout.buffer.flush()\n"
    "        else: print('debug: after done', flush=True)\n"
    "        sys.exit(0)\n"
    "    if word == exit_after:\n"
    "        import os\n"
    "        deadline = time.monotonic() + 10\n"
    "        while release and not os.path.exists(release) and time.monotonic() < deadline: time.sleep(0.01)\n"
    "        sys.exit(0)\n"
    "time.sleep(60)\n"
)


def _offline_route_dump(*subscribers):
    """A route block for the declared track, announced and routed from binding 1."""
    import host_receiver
    ns = "/".join(p.decode() for p in host_receiver.NAMESPACE)
    return (["epochs: node=1 shard=0 route=1", "announces:", f"  {ns} -> binding 1", "namespace_subscriptions:", "  (none)",
             "tracks:", f'  {ns} "{host_receiver.TRACK.decode()}" state=active upstream_binding=1',
             "    log: groups=0 records=0 bytes=0 oldest=null newest=null eot=0", "    subs:"]
            + [f"      binding={b} state=active start=0/0 end=- lag_groups=0 frontier=null skip_pending=0 "
               "start_before_retention=0" for b in subscribers])


# The player's receipt stream offline: the two receipts, as the sample writes them for a passing run,
# printed once the player has reported its terminal line (the trigger file).
_OFFLINE_RECEIPTS = [
    "presented: pass=true released=60 latched=60 distinctTs=60 indices=[0,59] preVideo=0 unreadable=0 syncInvalid=0 "
    "complementInvalid=0 captureRefusals=0 ordered=true settled=true ordinal=1 leaseId=1 refusal=none freezeReason=settled",
    "observer-closed: lateAfterFreeze=0 ignoredForeign=0 retiredAfterFreeze=0"]
_PLAYER_LOG_STUB = _STUB_PRELUDE + (
    "import os, signal\n"
    "signal.signal(signal.SIGTERM, lambda *a: sys.exit(0))\n"
    "trigger, lines = sys.argv[1], json.loads(sys.argv[2])\n"
    "while not os.path.exists(trigger): time.sleep(0.01)\n"
    "for line in lines: print(line, flush=True)\n"
    "time.sleep(60)\n"
)


def _probe(run_dir, player_log_stub, established=True, route=True, settled=True):
    """Offline stand-ins: the relay's scripted route blocks, the player's install, preparation and
    start, its reports (which begin only once the supervisor has started it), and its receipt stream,
    which follows its terminal line."""
    import host_receiver
    import route_dump
    started = threading.Event()
    blocks = [_offline_route_dump(2), _offline_route_dump(2, 3)] if route else [_offline_route_dump(2)]

    def player(gate, stop):
        while not started.is_set():
            if stop.is_set():
                return
            started.wait(0.02)
        if established:
            gate.fact("player_established")
        if settled:
            while not stop.is_set() and "ended-observed" not in gate.sent:
                stop.wait(0.02)
            if not stop.is_set():
                gate.fact("player_stopped")
                trigger.touch()

    def player_start(stop, dispatched, admit):
        refused = admit()
        if refused is not None:
            return refused
        dispatched()
        started.set()

    def ready(stop, budget_s):
        return None

    trigger = Path(run_dir) / "player reported stopped"

    def player_log_argv():
        return [sys.executable, "-I", str(player_log_stub), str(trigger), json.dumps(_OFFLINE_RECEIPTS)]

    def route_barrier(host_active, player_started, record):
        return route_dump.route_barrier(host_receiver.NAMESPACE, host_receiver.TRACK, host_active, player_started,
                                        interval_s=0.05, record=record)
    return player, route_barrier, player_start, ready, ready, player_log_argv, blocks


def _in_order(events, *expected):
    """True when every expected event occurred exactly once, in this order; others may interleave."""
    return all(events.count(e) == 1 for e in expected) and [events.index(e) for e in expected] == \
        sorted(events.index(e) for e in expected)


def _stuck_reader_schedule(gone, bound):
    """The owned schedule for a host whose stdout reader is stuck on its last line. The host exits
    only once `ended-observed` was forwarded (the `gone` file). The publisher's `complete`, which
    starts `done`'s budget, is held until the host watcher has entered its drain wait, so `done`
    cannot fail with the watcher unarmed. `done`'s own deadline fails first; only then does the
    watcher begin its bounded drain wait, which refuses; that decision releases the reader, and the
    run joins its threads only after it. Every hold is bounded by `bound` and records its outcome."""
    order = []
    released, decided, stopped, entered = (threading.Event() for _ in range(4))

    def stick(what):
        if what == "done":
            order.append("reader held")
            order.append("reader released" if released.wait(bound) else "reader hold expired")

    def on_forwarded(control):
        if control == "ended-observed":
            order.append("host exit released")
            gone.touch()

    def on_pub_event(ev):
        if ev.get("event") == "complete":
            order.append("complete waits for the watcher")
            order.append("watcher armed" if entered.wait(bound) else "complete: the watcher never entered its drain wait")

    def draining():
        order.append("watcher entered")
        entered.set()
        order.append("watcher resumed" if stopped.wait(bound) else "watcher: the run never stopped")

    def drain_refused():
        order.append("drain refused")
        decided.set()
        released.set()

    def on_stopping(primary):
        order.append("stopping")
        stopped.set()
        order.append("joins after the drain decision" if decided.wait(bound) else "joins without a drain decision")
    return ({"host_line": stick, "forwarded": on_forwarded, "pub_event": on_pub_event, "host_exit_draining": draining,
             "host_exit_drain_refused": drain_refused, "stopping": on_stopping}, order)


def _stuck_reader_order_ok(order):
    """The owned precedences of `_stuck_reader_schedule`; events on different threads that the
    schedule does not order may interleave."""
    return (_in_order(order, "host exit released", "watcher entered", "watcher armed", "stopping", "watcher resumed",
                      "drain refused", "joins after the drain decision")
            and _in_order(order, "complete waits for the watcher", "watcher armed")
            and _in_order(order, "reader held", "drain refused", "reader released"))


def _offline_rig(base):
    """The offline controls' shared setup under `base`: the stub children, the adb tripwire, the fast
    deadlines, `run` (one supervised run of the stubs) and `hold_line` (a host-reader hold)."""
    base = Path(base)
    py = sys.executable
    stubs = {}
    for name, src in (("relay", _RELAY_STUB), ("publisher", _PUBLISHER_STUB), ("host", _HOST_STUB),
                      ("player_log", _PLAYER_LOG_STUB)):
        p = base / f"{name}_stub.py"
        p.write_text(src)
        stubs[name] = p
    tripwire_dir = base / "tripwire-bin"
    tripwire_dir.mkdir()
    tripwire_log = base / "adb-tripwire.log"
    (tripwire_dir / "adb").write_text(f"#!/bin/sh\necho \"$@\" >> '{tripwire_log}'\nexit 97\n")
    os.chmod(tripwire_dir / "adb", 0o755)
    env = {"PATH": f"{tripwire_dir}:/usr/bin:/bin"}
    fast = Deadlines(relay_ready=3, publisher_ready=3, go_window=1.5, ack_all=1.5, ended_observed=1.5, done=1.5,
                     shutdown=2, settle=3)
    n = 60
    records = json.dumps(RELAY_STUB_RECORDS)

    def run(label, relay_mode="ready", pub_mode="normal", withhold="none", host_extra=(), clock=None, hooks=None,
            exits=OFFLINE_EXITS, **probe):
        d = base / label
        d.mkdir()
        player, route_barrier, player_start, install, prepare, log_argv, blocks = _probe(d, stubs["player_log"], **probe)
        actors = Actors(relay_argv=[py, "-I", str(stubs["relay"]), relay_mode, records, json.dumps(blocks)],
                        publisher_argv=[py, "-I", str(stubs["publisher"]), str(n), pub_mode],
                        host_argv=[py, "-I", str(stubs["host"]), withhold, *host_extra], player_probe=player,
                        route_barrier=route_barrier, player_start=player_start, player_install=install,
                        player_prepare=prepare, player_log_argv=log_argv, relay_identity=OFFLINE_IDENTITY,
                        expected_exits=exits, env=env, hooks=hooks or {})
        return supervise(d, actors, fast, n, clock=clock)

    def hold_line(word):
        """Holds the host stdout reader on `word` -- after the line was read, before its fact is
        recorded -- until the watcher has observed the exit and then either classified it or begun
        waiting for the reader to drain. Every wait is bounded; the record says what happened."""
        exited, proceed, classified = threading.Event(), threading.Event(), threading.Event()
        state = {"held": False, "released_by": None, "classified": classified}

        def on_line(what):
            if what == word:
                state["held"] = True
                ok = exited.wait(fast.settle) and proceed.wait(fast.settle)
                state["released_by"] = state.get("signal") if ok else "timeout"
        def on_exit(rc):
            exited.set()

        def on(signal):
            def mark():
                state.setdefault("signal", signal)
                proceed.set()
            return mark
        def on_classified():
            classified.set()
            on("classified")()
        return {"host_line": on_line, "host_exit": on_exit, "host_exit_classified": on_classified,
                "host_exit_draining": on("draining")}, state

    return {"base": base, "tripwire_log": tripwire_log, "fast": fast, "n": n, "run": run, "hold_line": hold_line}


def _ack_all_line_in_pipe(rig, label, gated=True, handoff_hold=None):
    """The host exits after writing `ack-all` while its reader still holds that line: the exit must be
    classified as before `host_ended`, with `go` and `ack-all` forwarded. With `gated`, the host may
    exit only once `go` has been forwarded, so its exit never falls inside the route handoff.
    `handoff_hold` holds the handoff inside the player's admission: "until-exit" until the watcher has
    judged an observed host exit (a schedule barrier), or a number of seconds. Returns the run and
    its judged record."""
    fast, run, hold_line = rig["fast"], rig["run"], rig["hold_line"]
    hooks, held = hold_line("ack-all")

    def after_classification(ev):
        # The publisher's first write waits (bounded) until the watcher has classified the exit, so
        # `ack-all` cannot be forwarded before the refusal exists: only its scope decides the result.
        if ev.get("event") == "wrote" and ev.get("index") == 0:
            held["classified"].wait(fast.settle)
    hooks["pub_event"] = after_classification
    host_extra = ()
    if gated:
        go_forwarded = rig["base"] / f"{label}: go was forwarded"
        host_extra = ("exit-on-file", str(go_forwarded))
        hooks["forwarded"] = lambda control: go_forwarded.touch() if control == "go" else None
    if handoff_hold == "until-exit":
        entered, judged = threading.Event(), threading.Event()
        on_exit, on_draining = hooks["host_exit"], hooks["host_exit_draining"]

        def admitting(relay):
            entered.set()
            judged.wait(fast.settle)

        def host_exit(rc):                                 # the handoff is under way when the exit is judged
            entered.wait(fast.settle)
            on_exit(rc)

        def draining():
            on_draining()
            judged.set()
        hooks.update(admitting=admitting, host_exit=host_exit, host_exit_draining=draining)
    elif handoff_hold is not None:
        hooks["admitting"] = lambda relay: time.sleep(handoff_hold)
    r = run(label, withhold="exit-after-ack-all", host_extra=host_extra, hooks=hooks)
    piped = r["primary_error"] or ""
    return r, {"primary": piped, "forwarded": [c["line"] for c in r["controls_forwarded"]],
               "held": {k: v for k, v in held.items() if k != "classified"}, "classified_first": held["classified"].is_set(),
               "ok": held["held"] and held["released_by"] in ("classified", "draining")
                     and piped == "ended-observed: refused: host-receiver exited (rc 0) before host_ended"
                     and [c["line"] for c in r["controls_forwarded"]] == ["go", "ack-all"]}



def offline_controls(base):
    base = Path(base)
    rig = _offline_rig(base)
    tripwire_log, fast, n, run, hold_line = (rig[k] for k in ("tripwire_log", "fast", "n", "run", "hold_line"))
    results, all_runs, negative_runs = {}, [], {}
    r = positive = run("positive")
    all_runs.append(r)
    results["positive_control_order"] = {"primary": r["primary_error"], "forwarded": r["controls_forwarded"],
                                         "ready_record": r["relay_ready_record"], "verdict": r["verdict"],
                                         "ok": r["primary_error"] is None and r["checks"]["controls_forwarded_in_order"]
                                         and r["relay_ready_record"] == _RAW_READY and r["verdict"] == {"ok": True, "failures": []}
                                         and r["write_stream_error"] is None and r["write_anchor_index"] == n - 1
                                         and [f for _t, f in r["facts"]].count("publisher_wrote_all") == 1
                                         and all(v["outcome"] == "on-time" for v in r["event_stamps"].values())
                                         and len(r["event_stamps"]) == 3}
    basis = positive["deadline_basis"]
    budgets = {"go": fast.go_window, "ack-all": fast.ack_all, "ended-observed": fast.ended_observed, "done": fast.done}
    results["phase_deadlines_anchored_to_events"] = {
        "basis": basis,
        "ok": sorted(basis) == sorted(CONTROLS) and all(
            basis[c]["anchor"] == ControlGate.ANCHORS[c] and basis[c]["budget"] == budgets[c]
            and basis[c]["deadline"] == basis[c]["anchor_t"] + budgets[c] and basis[c]["sent_t"] <= basis[c]["deadline"]
            for c in CONTROLS)}
    for label, kw, needle in (
            ("go_window_timeout_named", {"established": False}, "go: barrier(s) player_established not observed"),
            ("go_route_barrier_timeout_named", {"route": False}, "go: barrier(s) route_two_subscriptions not observed"),
            ("ack_all_timeout_named", {"withhold": "ack-all"}, "ack-all: barrier(s) host_all_matched not observed"),
            ("ended_observed_timeout_named", {"withhold": "ended-observed"}, "ended-observed: barrier(s) host_ended not observed"),
            ("done_timeout_named", {"settled": False}, "done: barrier(s) player_settled not observed"),
            ("publisher_early_exit_named", {"pub_mode": "early-exit"}, "publisher exited before ready: rc 3"),
            ("relay_not_ready_named", {"relay_mode": "silent"}, "relay readiness (raw 127.0.0.1:4443")):
        r = run(label.replace("_named", ""), **kw)
        all_runs.append(r)
        negative_runs[label] = r
        results[label] = {"primary": r["primary_error"], "forwarded": [c["line"] for c in r["controls_forwarded"]],
                          "ok": needle in (r["primary_error"] or "") and not r["checks"]["controls_forwarded_in_order"]}
    refused = {}
    for mode in ("signals-only", "wrong-listener", "wrong-port", "echoed-token"):
        r = run("relay_" + mode, relay_mode=mode)
        all_runs.append(r)
        negative_runs["relay_" + mode] = r
        refused[mode] = {"primary": r["primary_error"], "children": [c.get("name") for c in r["children"]],
                         "ready_record": r["relay_ready_record"], "seen": r["relay_ready_other"]}
    results["relay_ready_refuses_non_matching_records"] = {
        "runs": refused,
        "ok": all(v["children"] == ["relay"] and v["ready_record"] is None
                  and (v["primary"] or "").startswith("relay readiness (raw 127.0.0.1:4443") for v in refused.values())}
    r = run("late_go", clock=SkewClock({"host_spawned": fast.go_window + 1.0}))
    all_runs.append(r)
    negative_runs["late_go"] = r
    late = r["primary_error"] or ""
    results["late_go_refused_by_absolute_deadline"] = {       # refused whether or not the probes' facts landed first
        "primary": r["primary_error"], "forwarded": [c["line"] for c in r["controls_forwarded"]],
        "ok": r["controls_forwarded"] == [] and late.startswith("go: ")
              and (f"within {fast.go_window}s of publisher_ready" in late or f"{fast.go_window}s from publisher_ready passed" in late)}
    late_runs = {}
    for key, mark, budget, prefix, n_sent in (
            ("relay_ready_late_refused", "stamp:relay_ready", fast.relay_ready, "relay readiness (raw 127.0.0.1:4443", 0),
            ("publisher_ready_late_refused", "stamp:publisher_ready", fast.publisher_ready, "ready", 0),
            ("shutdown_late_refused", "stamp:publisher_shutdown", fast.shutdown, "publisher shutdown record", 4)):
        r = run(key.replace("_refused", ""), clock=SkewClock({mark: budget + 1.0}))
        all_runs.append(r)
        negative_runs[key] = r
        p = r["primary_error"] or ""
        results[key] = {"primary": p, "stamps": r["event_stamps"], "forwarded": [c["line"] for c in r["controls_forwarded"]],
                        "ok": p.startswith(prefix) and f"after its {budget}s window" in p
                              and len(r["controls_forwarded"]) == n_sent}
    writes = {}
    for mode, reason, n_sent in (("early-write", "go: refused: publisher write stream: wrote index 0 before go", 0),
                                 ("dup-write", "ack-all: refused: publisher write stream: wrote index 58 where 59 was next", 1),
                                 ("skip-write", "ack-all: refused: publisher write stream: wrote index 8 where 7 was next", 1),
                                 ("bad-rc-write", "ack-all: refused: publisher write stream: wrote index 30 rc -11", 1)):
        r = run("writes_" + mode, pub_mode=mode)
        all_runs.append(r)
        negative_runs["writes_" + mode] = r
        writes[mode] = {"primary": r["primary_error"], "forwarded": [c["line"] for c in r["controls_forwarded"]],
                        "anchor_index": r["write_anchor_index"],
                        "ok": r["primary_error"] == reason and len(r["controls_forwarded"]) == n_sent
                              and "ack-all" not in [c["line"] for c in r["controls_forwarded"]]
                              and r["write_anchor_index"] is None}
    results["write_stream_defects_refuse_ack_all"] = {"runs": writes, "ok": all(v["ok"] for v in writes.values())}
    gone_after_go = base / "go was forwarded"
    r = run("host_exit_after_go", withhold="exit-after-go", host_extra=("exit-on-file", str(gone_after_go)),
            hooks={"forwarded": lambda control: gone_after_go.touch() if control == "go" else None})
    all_runs.append(r)
    negative_runs["host_exit_after_go"] = r
    early = r["primary_error"] or ""
    results["host_exit_before_control_named"] = {
        "primary": early, "forwarded": [c["line"] for c in r["controls_forwarded"]],
        "ok": early.startswith("ack-all: ") and "host-receiver exited" in early
              and [c["line"] for c in r["controls_forwarded"]] == ["go"]}
    hooks, held = hold_line("done")
    r = run("host_final_line_in_pipe", withhold="exit-after-done", hooks=hooks,
            exits={**OFFLINE_EXITS, "host-receiver": ("exited", 0)})
    all_runs.append(r)
    results["host_final_line_in_pipe_not_refused"] = {
        "primary": r["primary_error"], "forwarded": [c["line"] for c in r["controls_forwarded"]],
        "held": {k: v for k, v in held.items() if k != "classified"}, "verdict": r["verdict"],
        "ok": held["held"] and held["released_by"] in ("classified", "draining") and r["primary_error"] is None
              and [c["line"] for c in r["controls_forwarded"]] == list(CONTROLS) and r["verdict"]["ok"] is True}
    r, results["host_exit_with_ack_all_line_in_pipe_names_host_ended"] = _ack_all_line_in_pipe(
        rig, "host_exit_ack_all_line_in_pipe")
    all_runs.append(r)
    negative_runs["host_exit_ack_all_line_in_pipe"] = r
    gone = base / "ended-observed was forwarded, stuck reader"
    hooks, order = _stuck_reader_schedule(gone, 4 * fast.settle)
    r = run("host_exit_stuck_reader", withhold="exit-after-done", host_extra=("exit-on-file", str(gone)), hooks=hooks)
    all_runs.append(r)
    negative_runs["host_exit_stuck_reader"] = r
    jammed = r["primary_error"] or ""
    drain = f"refused: host-receiver exited (rc 0) and its output did not drain within {fast.settle}s"
    host = next((c for c in r["children"] if c.get("name") == "host-receiver"), {})
    results["host_exit_with_a_stuck_reader_is_bounded_and_named"] = {
        "primary": jammed, "forwarded": [c["line"] for c in r["controls_forwarded"]], "order": order,
        "ok": jammed == f"done: barrier(s) host_complete not observed within {fast.done}s of publisher_complete"
              and [c["line"] for c in r["controls_forwarded"]] == ["go", "ack-all", "ended-observed"]
              and any(event == drain for _t, event in r["facts"])
              and _stuck_reader_order_ok(order)
              and r["threads_settled"] is True and host.get("readers_settled") is True
              and r["checks"]["all_children_settled"] is True}
    r = run("host_stdout_noise", host_extra=("noise",))
    all_runs.append(r)
    negative_runs["host_stdout_noise"] = r
    noisy = r["primary_error"] or ""
    results["host_stdout_is_controls_only"] = {
        "primary": noisy, "forwarded": [c["line"] for c in r["controls_forwarded"]],
        "ok": noisy == "go: refused: host-receiver wrote a non-control line to stdout: 'debug: receiver attached'"
              and r["controls_forwarded"] == []}
    inventory = {}
    after_done, host_gone = base / "done was forwarded", threading.Event()
    for variant, key, extra, hooks in (
            ("dup-go", "duplicate", (), None), ("bad-ctrl", "malformed", (), None),
            # the stub writes its extra line only once `done` has been forwarded -- no later send can
            # see it -- then exits by itself; settlement waits for that exit, so the line is read
            ("noise-after-done", "after_done", (str(after_done),),
             {"forwarded": lambda control: after_done.touch() if control == "done" else None,
              "host_exit": lambda rc: host_gone.set(), "before_settle": lambda: host_gone.wait(fast.settle)})):
        r = run("host_stdout_" + key, host_extra=(variant, *extra), hooks=hooks,
                exits=REAL_HOST_EXITS if key == "after_done" else OFFLINE_EXITS)
        all_runs.append(r)
        negative_runs["host_stdout_" + key] = r
        inventory[key] = {"primary": r["primary_error"], "forwarded": [c["line"] for c in r["controls_forwarded"]],
                          "verdict": r["verdict"]}
    dup, bad, late = inventory["duplicate"], inventory["malformed"], inventory["after_done"]
    results["host_stdout_is_an_exact_control_inventory"] = {
        "runs": inventory,
        "ok": (dup["primary"] or "").endswith("refused: host-receiver control line 2 is 'CTRL go', expected 'CTRL ack-all'")
              and "ack-all" not in dup["forwarded"]
              and bad["primary"] == "go: refused: host-receiver control line 1 is 'CTRL go ', expected 'CTRL go'"
              and bad["forwarded"] == []
              and late["forwarded"] == list(CONTROLS) and late["primary"] is None
              and late["verdict"]["failures"] == ["check host_stdout_exact is False"]
              and all(v["verdict"]["ok"] is False for v in inventory.values())}
    # The host's stdout is judged as raw bytes: each line is its ASCII control text and one LF. A CR,
    # a missing LF, a non-ASCII or undecodable byte, or any byte after `CTRL done` is refused by name.
    raw_runs, extra_forwarded, extra_gone = {}, base / "done was forwarded before extra data", threading.Event()
    for variant, key, extra, exits, hooks in (
            ("crlf", "crlf", (), OFFLINE_EXITS, None),
            ("no-final-lf", "no_final_lf", (), REAL_HOST_EXITS, None),
            ("undecodable", "undecodable", (), OFFLINE_EXITS, None),
            ("non-ascii", "non_ascii", (), OFFLINE_EXITS, None),
            ("extra-after-done", "extra_after_done", (str(extra_forwarded),), REAL_HOST_EXITS,
             {"forwarded": lambda control: extra_forwarded.touch() if control == "done" else None,
              "host_exit": lambda rc: extra_gone.set(), "before_settle": lambda: extra_gone.wait(fast.settle)})):
        r = run("host_stdout_raw_" + key, host_extra=(variant, *extra), hooks=hooks, exits=exits)
        all_runs.append(r)
        negative_runs["host_stdout_raw_" + key] = r
        raw_runs[key] = {"primary": r["primary_error"], "forwarded": [c["line"] for c in r["controls_forwarded"]],
                         "violations": r["host_stdout_violations"], "verdict": r["verdict"]}
    refused_at_go = {"crlf": f"host-receiver stdout line 1 contains CR: {b'CTRL go' + bytes([13, 10])!r}",
                     "undecodable": f"host-receiver stdout line 1 is not ASCII: {b'CTRL go' + bytes([0xff, 10])!r}",
                     "non_ascii": f"host-receiver stdout line 1 is not ASCII: {('CTRL g' + chr(0xf6) + chr(10)).encode()!r}"}
    unterminated = f"host-receiver stdout line 4 is not LF-terminated: {b'CTRL done'!r}"
    trailing = f"host-receiver stdout line 5 is not LF-terminated: {b'extra'!r}"
    results["host_stdout_is_raw_ascii_with_one_lf"] = {
        "runs": raw_runs,
        "ok": all(raw_runs[k]["primary"] == "go: refused: " + reason and raw_runs[k]["forwarded"] == []
                  and raw_runs[k]["violations"] == [reason] for k, reason in refused_at_go.items())
              and raw_runs["no_final_lf"]["primary"] == "done: refused: " + unterminated
              and raw_runs["no_final_lf"]["forwarded"] == ["go", "ack-all", "ended-observed"]
              and raw_runs["no_final_lf"]["violations"] == [unterminated]
              and raw_runs["extra_after_done"]["primary"] is None
              and raw_runs["extra_after_done"]["forwarded"] == list(CONTROLS)
              and raw_runs["extra_after_done"]["violations"] == [trailing]
              and raw_runs["extra_after_done"]["verdict"]["failures"] == ["check host_stdout_exact is False"]
              and all(v["verdict"]["ok"] is False for v in raw_runs.values())}
    # R1: a real actor that exits by itself after `done` -- only once settlement has begun -- must
    # be awaited, not signalled, when the run declares it ("exited", 0).
    release = base / "host may exit"
    r = run("host_exits_after_done_real", withhold="exit-after-done", host_extra=("exit-on-file", str(release)),
            exits=REAL_HOST_EXITS, hooks={"before_settle": release.touch})
    all_runs.append(r)
    host = next((c for c in r["children"] if c.get("name") == "host-receiver"), {})
    real = {"primary": r["primary_error"], "verdict": r["verdict"], "how": host.get("how"), "exit": host.get("exit")}
    # R2: the same self-exit under the stub's SIGTERM declaration is a named wrong settlement. The host
    # has already exited (observed by the watcher) before settlement, so the outcome cannot race.
    exited = threading.Event()
    r = run("host_exits_after_done_stub_declared", withhold="exit-after-done", exits=OFFLINE_EXITS,
            hooks={"host_exit": lambda rc: exited.set(), "before_settle": lambda: exited.wait(fast.settle)})
    all_runs.append(r)
    negative_runs["host_exits_after_done_stub_declared"] = r
    stub = {"primary": r["primary_error"], "verdict": r["verdict"]}
    results["real_actor_exit_after_done_is_declared"] = {
        "real": real, "stub": stub,
        "ok": real["primary"] is None and real["verdict"] == {"ok": True, "failures": []}
              and (real["how"], real["exit"]) == ("exited", 0)
              and stub["verdict"]["failures"] == ["host-receiver: exited rc 0, expected SIGTERM rc 0"]
              and OFFLINE_EXITS["host-receiver"] == ("SIGTERM", 0)}
    gone_after_ack_all = base / "ack-all was forwarded"
    r = run("host_exit_after_ack_all", withhold="exit-after-ack-all", host_extra=("exit-on-file", str(gone_after_ack_all)),
            hooks={"forwarded": lambda control: gone_after_ack_all.touch() if control == "ack-all" else None})
    all_runs.append(r)
    negative_runs["host_exit_after_ack_all"] = r
    later = r["primary_error"] or ""
    results["host_exit_after_ack_all_named"] = {
        "primary": later, "forwarded": [c["line"] for c in r["controls_forwarded"]],
        "ok": later.startswith("ended-observed: ") and "host-receiver exited" in later
              and [c["line"] for c in r["controls_forwarded"]] == ["go", "ack-all"]}
    r = run("bad_drain", pub_mode="bad-drain")
    all_runs.append(r)
    negative_runs["bad_drain"] = r
    results["complete_rc_separate_from_shutdown"] = {
        "complete_rc_ok": r["checks"]["publisher_complete_rc_ok"], "shutdown_ok": r["checks"]["publisher_shutdown_ok"],
        "ok": r["checks"]["publisher_complete_rc_ok"] is True and r["checks"]["publisher_shutdown_ok"] is False}
    results["verdict_fails_on_bad_drain"] = {
        "primary": r["primary_error"], "verdict": r["verdict"],
        "ok": r["primary_error"] is None and r["checks"]["publisher_complete_rc_ok"] is True
              and r["verdict"]["ok"] is False and "check publisher_shutdown_ok is False" in r["verdict"]["failures"]
              and "publisher: exited rc 5, expected exited rc 0" in r["verdict"]["failures"]}
    r = run("host_warning", host_extra=("warn",))
    all_runs.append(r)
    negative_runs["host_warning"] = r
    results["verdict_fails_on_unplanned_diagnostic"] = {
        "primary": r["primary_error"], "verdict": r["verdict"],
        "ok": r["primary_error"] is None and r["verdict"]["ok"] is False
              and r["verdict"]["failures"] == ["unplanned diagnostic: host-receiver [stderr]: warning: stub diagnostic"]}
    results["verdict_fails_on_every_negative_path"] = {
        "verdicts": {k: v["verdict"]["ok"] for k, v in negative_runs.items()},
        "ok": len(negative_runs) == 35 and all(v["verdict"]["ok"] is False for v in negative_runs.values())}
    results["children_settled_on_every_path"] = {
        "per_run": [(x["primary_error"], [c.get("how") for c in x["children"]]) for x in all_runs],
        "ok": all(x["checks"]["all_children_settled"] and x["threads_settled"] for x in all_runs)}
    results["stub_adb_tripwire_not_reached"] = {"ok": not tripwire_log.exists()}
    calls = []
    dev = Device(DEDICATED_SERIAL, lambda name, argv, timeout: calls.append(argv) or {"rc": 0})
    for spec in dev.commands().values():
        dev.run("probe", *spec[3:])
    results["device_argv_never_targets_shared_emulator"] = {
        "ok": bool(calls) and all(a[:3] == ["adb", "-s", DEDICATED_SERIAL] and "emulator-5554" not in a for a in calls)}
    refused_serials, runner_calls = [], []
    candidates = ("emulator-5554", "emulator-5560", "emulator-5582", "emulator-55800", "emulator-5580 ", "0123456789ABCDEF", "")
    for bad in candidates:
        try:
            Device(bad, lambda *a: runner_calls.append(a))
        except ValueError:
            refused_serials.append(bad)
    results["device_only_dedicated_serial"] = {
        "refused": refused_serials,
        "ok": refused_serials == list(candidates) and runner_calls == []}
    return results


def publisher_track_selftests(publisher_bin):
    """The runtime publisher's declared track configuration, offline: the
    `--track-config-selftest` mode resolves exactly what the publishing path
    would declare and prints it without connecting anywhere."""
    cases = [
        ("defaults_unchanged", [], 0, {"codec": "avc1.42E01E", "width": 320, "height": 240, "framerate_millis": 24000, "declared": False}),
        ("declared_from_sps", ["--track", "avc1.42C01E", "320x240", "24000"], 0,
         {"codec": "avc1.42C01E", "width": 320, "height": 240, "framerate_millis": 24000, "declared": True}),
        ("codec_bad_character", ["--track", "avc1.42C01E;", "320x240", "24000"], 2, "publisher: invalid track codec"),
        ("codec_empty", ["--track", "", "320x240", "24000"], 2, "publisher: invalid track codec"),
        ("size_malformed", ["--track", "avc1.42C01E", "320-240", "24000"], 2, "publisher: invalid track size"),
        ("size_zero", ["--track", "avc1.42C01E", "0x240", "24000"], 2, "publisher: invalid track size"),
        ("framerate_zero", ["--track", "avc1.42C01E", "320x240", "0"], 2, "publisher: invalid track framerate"),
        ("framerate_not_number", ["--track", "avc1.42C01E", "320x240", "24k"], 2, "publisher: invalid track framerate"),
        ("track_missing_values", ["--track", "avc1.42C01E"], 2, "usage"),
    ]
    results = {}
    for name, extra, want_rc, want in cases:
        r = subprocess.run([str(publisher_bin), "--track-config-selftest", *extra], capture_output=True, text=True, timeout=10)
        entry = {"rc": r.returncode, "stdout": r.stdout.strip(), "stderr": r.stderr.strip()}
        if want_rc == 0:
            try:
                got = json.loads(r.stdout.strip().splitlines()[-1]) if r.stdout.strip() else {}
            except ValueError:
                got = {}
            entry["ok"] = r.returncode == 0 and r.stderr == "" and got.get("event") == "track_config" \
                and all(got.get(k) == v for k, v in want.items()) and got.get("timescale") == 90000 and got.get("bitrate") == 400000
        else:
            entry["ok"] = r.returncode == want_rc and want in r.stderr and r.stdout == ""
        results[name] = entry
    return results


def main(argv):
    if argv[1:2] == ["--publisher-track-selftests"]:
        results = publisher_track_selftests(Path(argv[2]))
        print(json.dumps(results, indent=1))
        return 0 if all(v["ok"] for v in results.values()) else 1
    if argv[1:2] == ["--offline-controls"]:
        with tempfile.TemporaryDirectory() as tmp:
            results = offline_controls(Path(tmp))
        print(json.dumps(results, indent=1, default=str))
        return 0 if all(v.get("ok") for v in results.values()) else 1
    if argv[1:2] == ["--live"]:
        import live_entry
        return live_entry.main(argv[2:])
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
