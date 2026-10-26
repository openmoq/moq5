"""The Android player as a supervised actor: install, prepare, the one Start tap, and the host-observed
UI facts, all through the dedicated `Device`'s injected command runner.

What the sample exposes (`bindings/kotlin/samples/player`, source-derived):
  - `org.openmoq.moq5.samples.player/.PlayerActivity`, built in code with no view ids: the labels
    "Relay URL", "Namespace" and "Objects to receive", each followed by an EditText (defaults
    `moqt://10.0.2.2:4443`, `live/cam1`, `120`), one Button (`Start`/`Stop`, enabled once the preview
    surface exists), and one log TextView after the button.
  - The launch's string extras RELAY_URL, NAMESPACE and OBJECT_BUDGET preset the three fields
    (`PlayerLaunch.kt`); the launch intent is the Activity's own, so a recreated Activity starts from
    the same values. Nothing is typed into a field.
  - A tap on an enabled `Start` is the final start action; a second tap would cancel.
  - The log TextView shows `established: <negotiatedVersion>`, `stopped: <end>; N objects, N decoder
    inputs`, failures as `<SimpleName>: <message>`, and the observer receipts (also in logcat, where the
    supervisor's own sidecar judges them). `Started` clears it.

Everything here is judged structurally (Python's XML parser on `uiautomator dump`, exact line text), and
every wait is bounded by its phase's absolute deadline. Whether a device's dump, keyboard input, `am
start -W` output, `pidof` and `logcat` behave as these parsers assume is a device-side gate.
"""
import re
import threading
import time
import xml.etree.ElementTree as ET
from urllib.parse import urlsplit

PACKAGE = "org.openmoq.moq5.samples.player"
COMPONENT = f"{PACKAGE}/.PlayerActivity"
OBSERVE_EXTRA = f"{PACKAGE}.OBSERVE_PRESENTATION"
TRACE_EXTRA = f"{PACKAGE}.DIAGNOSTIC_TRACE"         # the player's diagnostic breadcrumbs; off unless named
STATS_ACTION = f"{PACKAGE}.DIAGNOSTIC_STATS"        # a traced player's one recorded receiver-stats request
TRACE_TAG = "MoqPlayerDiag"                             # the player's trace breadcrumb tag
LISTENER_ARMED = "diagnostics-listener armed thread=moq5-player-diagnostics"
DEFAULT_URL, NAMESPACE, DEFAULT_BUDGET = "moqt://10.0.2.2:4443", "live/cam1", "120"
# The sample's own default port stays its ordinary default; a supervised player never targets it.
DEFAULT_PORT = urlsplit(DEFAULT_URL).port
RELAY_URL_EXTRA = f"{PACKAGE}.RELAY_URL"
NAMESPACE_EXTRA = f"{PACKAGE}.NAMESPACE"
OBJECT_BUDGET_EXTRA = f"{PACKAGE}.OBJECT_BUDGET"
# An extra's value crosses the device shell as one word: only characters that shell never splits,
# expands or quotes.
EXTRA_VALUE = re.compile(r"[A-Za-z0-9:/._-]+")
LABELS = ("Relay URL", "Namespace", "Objects to receive")
ESTABLISHED = "established: MoqVersion.Draft18"
COMMAND_S = 10.0              # the longest any one command may take, and never beyond its phase's deadline
POLL_S = 0.1                  # between successive dumps while waiting for a UI state
# The Start action's own bound, inside the 25 s `go` window: a pidof, one precheck dump, the tap and up to
# two confirmation dumps. A warm `uiautomator dump` takes about 2 s on the dedicated API-36 emulator; a
# freshly booted one has been seen to hold a post-tap confirmation dump past 6 s. The bound is a chosen
# harness tolerance, not a measured latency ceiling, and every command inside it keeps COMMAND_S.
HANDOFF_S = 20.0
# `uiautomator dump /dev/tty` writes this after the XML (the platform's own spelling).
DUMP_TRAILER = "UI hierchary dumped to: /dev/tty"
_BOUNDS = re.compile(r"\[(\d+),(\d+)\]\[(\d+),(\d+)\]")


class DeviceError(RuntimeError):
    pass


class DeadlineError(DeviceError):
    """A command not started because its phase's deadline had passed."""


def stopped_line(n_objects):
    return f"stopped: ObjectLimit; {n_objects} objects, {n_objects} decoder inputs"


class Screen:
    """One `uiautomator dump` of the player, parsed structurally. Raises DeviceError naming any
    departure: malformed XML, a foreign package, a missing or repeated label, field, button or log."""

    def __init__(self, text):
        body = text.rstrip("\n")
        if body.endswith(DUMP_TRAILER):
            body = body[:-len(DUMP_TRAILER)]
        start, end = body.find("<?xml"), body.rfind("</hierarchy>")
        if start != 0 or end < 0 or body[end + len("</hierarchy>"):].strip():
            raise DeviceError(f"ui dump is not one XML hierarchy: {text[:80]!r}")
        try:
            root = ET.fromstring(body)
        except ET.ParseError as e:
            raise DeviceError(f"ui dump does not parse: {e}") from None
        nodes = [n for n in root.iter("node")]
        mine = [n for n in nodes if n.get("package") == PACKAGE]
        if not mine:
            raise DeviceError(f"ui dump has no {PACKAGE} node")
        buttons = [i for i, n in enumerate(mine) if n.get("class") == "android.widget.Button"]
        if len(buttons) != 1:
            raise DeviceError(f"ui dump has {len(buttons)} buttons, expected 1")
        self.button = mine[buttons[0]]
        # The log is the button's next sibling and the last child of their parent (PlayerActivity.kt adds
        # the three pairs, the button, then the log to one LinearLayout). Nodes elsewhere in the dump --
        # the platform's action bar and its title among them -- are not candidates.
        parent = {child: node for node in root.iter("node") for child in node}
        content = parent.get(self.button)
        siblings = list(content) if content is not None else [self.button]
        after = siblings[siblings.index(self.button) + 1:]
        if not after:
            raise DeviceError("the button's parent has no child after it; expected the log TextView")
        if after[0].get("class") != "android.widget.TextView":
            raise DeviceError(f"the child after the button is {after[0].get('class')}, not the log TextView")
        if len(after) != 1:
            raise DeviceError(f"the button's parent has {len(after)} children after it, expected only the log")
        self.log = after[0].get("text", "")
        # The sample builds exactly three label/field pairs, in this order, each field right after its
        # label (PlayerActivity.kt); with no view ids, that structure is the only widget identity.
        self.fields, where = {}, {}
        for label in LABELS:
            at = [i for i, n in enumerate(mine) if n.get("class") == "android.widget.TextView" and n.get("text") == label]
            if len(at) != 1:
                raise DeviceError(f"ui dump has {len(at)} {label!r} labels, expected 1")
            where[label] = at[0]
        order = sorted(LABELS, key=where.get)
        if order != list(LABELS):
            raise DeviceError(f"the labels are {order}, expected {list(LABELS)}")
        edits = [n for n in mine if n.get("class") == "android.widget.EditText"]
        if len(edits) != len(LABELS):
            raise DeviceError(f"ui dump has {len(edits)} input fields, expected {len(LABELS)}")
        for label in LABELS:
            after = mine[where[label] + 1] if where[label] + 1 < len(mine) else None
            if after is None or after.get("class") != "android.widget.EditText":
                raise DeviceError(f"the node after {label!r} is {None if after is None else after.get('class')}, not its field")
            self.fields[label] = after
        pair_nodes = [mine[where[label]] for label in LABELS] + list(self.fields.values())
        if any(parent.get(n) is not content for n in pair_nodes):
            raise DeviceError("the label/field pairs and the button are not in one content parent")

    @staticmethod
    def centre(node):
        m = _BOUNDS.fullmatch(node.get("bounds", ""))
        if not m:
            raise DeviceError(f"node bounds {node.get('bounds')!r} are not [x1,y1][x2,y2]")
        x1, y1, x2, y2 = map(int, m.groups())
        if x2 <= x1 or y2 <= y1:
            raise DeviceError(f"node bounds {node.get('bounds')!r} are empty")
        return (x1 + x2) // 2, (y1 + y2) // 2

    def start_ready(self):
        """None when the button is an enabled `Start`; otherwise what it is instead."""
        text, enabled = self.button.get("text"), self.button.get("enabled")
        if text == "Stop":
            return "the button already reads Stop"
        if text != "Start":
            return f"the button reads {text!r}"
        return None if enabled == "true" else "Start is not enabled"

    def field(self, label):
        return self.fields[label].get("text", "")


class DevicePlayer:
    """The player actor over a `Device`. The supervisor calls `install` (before the relay), `prepare`
    (before the publisher), `start` (the handoff's one tap), runs `observe` on a thread, and spawns
    `log_argv()` as its owned receipt sidecar."""

    def __init__(self, device, apk, n_objects, url, clock=time.monotonic, observe=True, trace=False):
        self.device, self.apk, self.n_objects, self.clock = device, str(apk), n_objects, clock
        self.observing, self.tracing = observe, trace       # the launch's observer and trace switches
        self.inputs = dict(zip(LABELS, (url, NAMESPACE, str(n_objects))))
        for label, value in self.inputs.items():
            if not isinstance(value, str) or not EXTRA_VALUE.fullmatch(value):
                raise ValueError(f"{label!r}: {value!r} is not a launch extra value ({EXTRA_VALUE.pattern})")
        try:
            port = urlsplit(url).port
        except ValueError:
            port = None
        if port is None:
            raise ValueError(f"'Relay URL': {url!r} names no port")
        if port == DEFAULT_PORT:
            raise ValueError(f"'Relay URL': {url!r} targets port {DEFAULT_PORT}, the sample's default destination")
        self.pid = None
        # One `uiautomator dump` at a time: preparation, the Start path and the observer all dump, and
        # serializing them keeps their use of the device's UiAutomation from overlapping.
        self._dumping = threading.Lock()

    # -- commands, each bounded by the phase deadline ------------------------------------------
    def _run(self, name, deadline, *args):
        remaining = deadline - self.clock()
        if remaining <= 0:
            raise DeadlineError(f"{name}: the phase deadline passed before it could run")
        r = self.device.run(name, *args, timeout=min(COMMAND_S, remaining))
        if r.get("timeout"):
            raise DeviceError(f"{name}: timed out")
        if r.get("rc") != 0:
            raise DeviceError(f"{name}: rc {r.get('rc')}: {(r.get('stdout') or '') + (r.get('stderr') or '')!r}")
        return r.get("stdout") or ""

    def _packages(self, deadline):
        out = self._run("package", deadline, "shell", "pm", "list", "packages", PACKAGE)
        return [line for line in out.splitlines() if line == f"package:{PACKAGE}"]

    def _pid(self, deadline):
        out = self._run("pidof", deadline, "shell", "pidof", PACKAGE).split()
        if len(out) != 1 or not out[0].isdigit():
            raise DeviceError(f"pidof {PACKAGE}: {out!r}, expected exactly one pid")
        return int(out[0])

    def _screen(self, deadline):
        """One dump, parsed. Waiting for another dump in flight spends this caller's own deadline, and
        the remaining time is judged again once the device is granted (`_run`), so a dump granted
        after the deadline is never sent. The device is released whatever the dump or its parse does."""
        wait = deadline - self.clock()
        if wait <= 0:
            raise DeadlineError("ui_dump: the phase deadline passed before it could run")
        if not self._dumping.acquire(timeout=wait):
            raise DeadlineError("ui_dump: the phase deadline passed while another UI dump was in flight")
        try:
            return Screen(self._run("ui_dump", deadline, "exec-out", "uiautomator", "dump", "/dev/tty"))
        finally:
            self._dumping.release()

    def _inputs_differ(self, screen):
        """None when the three fields read exactly this run's inputs; otherwise the first difference."""
        for label, want in self.inputs.items():
            if screen.field(label) != want:
                return f"{label!r} reads {screen.field(label)!r}, expected {want!r}"
        return None

    def launch_args(self):
        """`am start` for this run: the observer switch (armed unless this is an observer-off run), the
        diagnostic trace switch when it is on, and the three fields preset by launch extras."""
        trace = ("--ez", TRACE_EXTRA, "true") if self.tracing else ()
        return ("shell", "am", "start", "-W", "-n", COMPONENT, "--ez", OBSERVE_EXTRA, "true" if self.observing else "false",
                *trace, "--es", RELAY_URL_EXTRA, self.inputs["Relay URL"], "--es", NAMESPACE_EXTRA, self.inputs["Namespace"],
                "--es", OBJECT_BUDGET_EXTRA, self.inputs["Objects to receive"])

    def _await_screen(self, deadline, stop, ready, what, seen):
        """Dumps until `ready(screen)` is None, bounded by the phase deadline. A state first seen by a
        dump that returned after the deadline is not an in-budget result."""
        last = None
        while True:
            try:
                screen = self._screen(deadline)
            except DeadlineError:
                if last is None:
                    raise
                raise DeviceError(f"{what}: {last}") from None    # the deadline passed during the wait
            last = ready(screen)
            if last is None:
                if self.clock() > deadline:
                    raise DeviceError(f"{what}: {seen} seen only after the deadline")
                return screen
            if self.clock() + POLL_S >= deadline or stop.is_set():
                raise DeviceError(f"{what}: {last}")
            stop.wait(POLL_S)

    # -- phases ---------------------------------------------------------------------------------
    def install(self, stop, budget_s):
        deadline = self.clock() + budget_s
        try:
            if self._packages(deadline):
                return f"{PACKAGE} present before install"
            out = self._run("install", deadline, "install", self.apk)
            if out.strip().splitlines()[-1:] != ["Success"]:
                return f"install did not report Success: {out!r}"
            if len(self._packages(deadline)) != 1:
                return f"{PACKAGE} not present after install"
        except DeviceError as e:
            return str(e)
        return None

    def prepare(self, stop, budget_s):
        deadline = self.clock() + budget_s
        try:
            out = self._run("start", deadline, *self.launch_args())
            launch = dict(line.split(": ", 1) for line in out.splitlines() if ": " in line)
            for key, want in (("Status", "ok"), ("LaunchState", "COLD"), ("Activity", COMPONENT)):
                if launch.get(key) != want:
                    return f"launch {key} is {launch.get(key)!r}, expected {want!r}"
            self.pid = self._pid(deadline)
            screen = self._await_screen(deadline, stop, Screen.start_ready, "Start never became ready", "Start")
            differ = self._inputs_differ(screen)
            if differ is not None:
                return f"the launch did not preset the inputs: {differ}"
            if self._pid(deadline) != self.pid:
                return "the player process changed during preparation"
        except DeviceError as e:
            return str(e)
        return None

    def start(self, stop, dispatched, admit):
        """The one Start action: a fresh check of the same process, then ONE dump that must show this
        run's three inputs exactly and an enabled Start, whose bounds are the ones tapped; then the
        supervisor's `admit()`, once; then `dispatched()` immediately before exactly one tap, then
        `Stop` confirmed on a screen still showing those inputs. The Activity reads its fields at the tap, so the check and the tap
        target come from the same dump. Nothing is dispatched or tapped if the check fails; a failed
        tap or a missing `Stop` is never retried."""
        deadline = self.clock() + HANDOFF_S
        try:
            pid = self._pid(deadline)
            if pid != self.pid:
                return f"the player process changed: {self.pid} became {pid}"
            screen = self._screen(deadline)
            not_ready = screen.start_ready()
            if not_ready is not None:
                return f"Start is not ready: {not_ready}"
            differ = self._inputs_differ(screen)
            if differ is not None:
                return f"before the tap: {differ}"
            x, y = Screen.centre(screen.button)
        except DeviceError as e:
            return f"before the tap: {e}"
        # Nothing may signal a start that cannot be tapped in time: the last checks are here, right
        # before the dispatch and its one tap, and again after admission, which runs on this bound.
        if stop.is_set():
            return "before the tap: the run stopped"
        if self.clock() >= deadline:
            return "before the tap: the handoff deadline passed"
        refused = admit()
        if refused is not None:
            return f"before the tap: {refused}"
        if stop.is_set():
            return "before the tap: the run stopped"
        if self.clock() >= deadline:
            return "before the tap: the handoff deadline passed"
        dispatched()
        try:
            self._run("tap", deadline, "shell", "input", "tap", str(x), str(y))
        except DeviceError as e:
            return f"the Start tap failed: {e}"
        if self.clock() > deadline:
            return "start not confirmed: the tap's result arrived after the handoff deadline"
        try:
            def stop_shown(screen):
                text = screen.button.get("text")
                return None if text == "Stop" else f"the button reads {text!r}"
            screen = self._await_screen(deadline, stop, stop_shown, "start not confirmed", "Stop")
        except DeviceError as e:
            return str(e)
        differ = self._inputs_differ(screen)
        if differ is not None:
            return f"after the tap: {differ}"
        return None

    def observe(self, gate, stop):
        """The host-observed UI facts: `player_established` on the exact established line and
        `player_stopped` on the exact terminal line. The log may only grow; the process must stay the
        prepared one; any other session line is refused by name. Settlement is not this reader's."""
        seen, reported = [], set()
        while not stop.is_set():
            deadline = self.clock() + COMMAND_S
            try:
                if self._pid(deadline) != self.pid:
                    gate.refuse("player process changed while observing")
                    return
                lines = [line for line in self._screen(deadline).log.split("\n") if line]
            except DeviceError as e:
                gate.refuse(f"player observation: {e}")
                return
            if lines[:len(seen)] != seen:
                gate.refuse(f"player log changed under the observer: {seen!r} became {lines!r}")
                return
            for line in lines[len(seen):]:
                if line.startswith(("presented: ", "observer-closed: ")):
                    continue                              # receipts: the sidecar judges them from logcat
                if line == ESTABLISHED and "established" not in reported:
                    reported.add("established")
                    gate.fact("player_established")
                elif line == stopped_line(self.n_objects) and "stopped" not in reported:
                    reported.add("stopped")
                    gate.fact("player_stopped")
                else:
                    gate.refuse(f"player reported {line!r}")
                    return
            seen = lines
            stop.wait(POLL_S)

    def stats_request_argv(self):
        """The one diagnostic stats request a traced player answers with a breadcrumb: a broadcast to
        this package only. Not passive: the player's stats read runs on its endpoint's service worker."""
        return self.device.argv("shell", "am", "broadcast", "-a", STATS_ACTION, "-p", PACKAGE)

    def listener_armed_argv(self):
        """A one-shot read of this prepared process's trace breadcrumbs, from which a traced player's
        diagnostic listener is shown armed on its own thread before any stats request is sent."""
        if self.pid is None:
            raise DeviceError("the player was not prepared: no pid for its breadcrumbs")
        return self.device.argv("logcat", "-d", f"--pid={self.pid}", "-v", "threadtime", "-s", f"{TRACE_TAG}:I")

    def diagnostic_argv(self):
        """A diagnostic stream of this prepared process's whole log: every tag and level, with time, pid and
        tid (threadtime), under an explicit filter. Evidence only, never a receipt source."""
        if self.pid is None:
            raise DeviceError("the player was not prepared: no pid for its diagnostic stream")
        return self.device.argv("logcat", f"--pid={self.pid}", "-v", "threadtime", "*:V")

    def log_argv(self):
        """The receipt sidecar: this prepared process's receipts only."""
        if self.pid is None:
            raise DeviceError("the player was not prepared: no pid for its receipt stream")
        return self.device.argv("logcat", f"--pid={self.pid}", "-v", "raw", "-s", "MoqPlayer:I", "MoqPlayerObserver:I")
