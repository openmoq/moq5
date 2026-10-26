"""The device player actor over a scripted command runner; nothing reaches adb.

`FakeDevice` answers each named `Device` command from a small state machine of the sample's screen.
Its `uiautomator dump` XML and its `am start -W`, `pm`, `install` and `pidof` outputs are hand-written
from the sample's source and the platform's documented formats; they are not device captures. Every
runner call is recorded, with the `dispatched()` marker in the same list, so order and argv are exact.
"""
import json
import sys
import tempfile
import threading
import time
import traceback
import unittest
import unittest.mock
from pathlib import Path
from xml.sax.saxutils import quoteattr

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import device_player as dp                                   # noqa: E402
import route_dump                                            # noqa: E402
import run_android_media as m                                # noqa: E402
from test_route_dump import NAME, NS, ONE_SUB, TWO_SUBS      # noqa: E402
from test_supervisor_route import CLOSED, FAST, PLAYER_LOG_STUB, PRESENTED_PASS, ROUTE_RELAY_STUB, forwarded, pairs  # noqa: E402

SERIAL = m.DEDICATED_SERIAL
APK = "/builds/player debug.apk"
PLAYER_URL = "moqt://10.0.2.2:46443"          # the run's declared relay, as the emulator reaches it
INPUTS = {"Relay URL": PLAYER_URL, "Namespace": dp.NAMESPACE, "Objects to receive": "60"}


def adb(*args):
    return ["adb", "-s", SERIAL, *args]


class FakeDevice:
    """The runner for `Device`: one scripted player screen. Attributes steer each scenario."""

    def __init__(self):
        self.calls, self.lock = [], threading.Lock()
        self.installed, self.pids = False, [4242]
        self.launch = ("Starting: Intent { cmp=org.openmoq.moq5.samples.player/.PlayerActivity (has extras) }\n"
                       "Status: ok\nLaunchState: COLD\nActivity: org.openmoq.moq5.samples.player/.PlayerActivity\n"
                       "TotalTime: 812\nWaitTime: 815\nComplete\n")
        self.install_out = "Performing Streamed Install\nSuccess\n"
        self.fields = {"Relay URL": dp.DEFAULT_URL, "Namespace": dp.NAMESPACE, "Objects to receive": dp.DEFAULT_BUDGET}
        self.honour_extras = True                 # the sample presets its fields from the launch's string extras
        self.enabled_after = 1                    # dumps before Start becomes enabled
        self.dumps = 0
        self.button, self.button_bounds = "Start", "[40,1500][1040,1620]"
        self.log = []
        self.stopped_when = None                  # a file: its existence adds the terminal line
        self.after_tap = [dp.ESTABLISHED]         # log lines the Start tap produces
        self.confirm = True                       # whether the tap turns the button into Stop
        self.fail = {}                            # command name -> rc for its next call
        self.extra_button = self.extra_log = self.twice_label = False
        self.layout = "normal"                    # normal | extra_budget | missing_namespace | misplaced | labels_swapped |
        #                                           log_missing | wrong_sibling | log_elsewhere | pairs_elsewhere
        self.action_bar = True                    # the platform's title branch, dumped after the content (API 36)
        self.after = []                           # one-shot (command name, callable) run as that command returns

    def __call__(self, name, argv, timeout):
        with self.lock:
            self.calls.append((name, list(argv), timeout))
            if name in self.fail:
                return {"rc": self.fail.pop(name), "stdout": "", "stderr": "error: fault", "timeout": False}
            out = {"rc": 0, "stdout": getattr(self, "_" + name)(argv), "stderr": "", "timeout": False}
            for i, (when, fn) in enumerate(self.after):
                if when == name:
                    del self.after[i]
                    fn()
                    break
            return out

    def names(self):
        return [c[0] for c in self.calls]

    # -- commands --------------------------------------------------------------------------------
    def _package(self, argv):
        return f"package:{dp.PACKAGE}\n" if self.installed else ""

    def _install(self, argv):
        self.installed = self.installed or "Success" in self.install_out
        return self.install_out

    def _start(self, argv):
        # PlayerLaunch.kt: each `--es` extra the sample knows presets its field; the rest keep defaults.
        by_extra = {dp.RELAY_URL_EXTRA: "Relay URL", dp.NAMESPACE_EXTRA: "Namespace", dp.OBJECT_BUDGET_EXTRA: "Objects to receive"}
        for i, word in enumerate(argv):
            if word == "--es" and self.honour_extras and argv[i + 1] in by_extra:
                self.fields[by_extra[argv[i + 1]]] = argv[i + 2]
        return self.launch

    def _pidof(self, argv):
        pid = self.pids[0] if len(self.pids) == 1 else self.pids.pop(0)
        return f"{pid}\n" if isinstance(pid, int) else pid

    def _tap(self, argv):
        x, y = int(argv[-2]), int(argv[-1])
        if self._inside(self.button_bounds, x, y) and self.button == "Start":
            self.log = list(self.after_tap)
            if self.confirm:
                self.button = "Stop"
        return ""

    def _ui_dump(self, argv):
        self.dumps += 1
        log = list(self.log)
        if self.stopped_when is not None and Path(self.stopped_when).exists():
            log.append(dp.stopped_line(60))
        return self.xml(enabled=self.dumps > self.enabled_after, log=log) + dp.DUMP_TRAILER

    # -- the screen ------------------------------------------------------------------------------
    @staticmethod
    def _inside(bounds, x, y):
        x1, y1, x2, y2 = map(int, bounds.replace("][", ",").strip("[]").split(","))
        return x1 <= x < x2 and y1 <= y < y2

    @staticmethod
    def _field_bounds(label):
        top = 300 + 250 * dp.LABELS.index(label)
        return f"[40,{top + 80}][1040,{top + 200}]"

    def xml(self, enabled, log):
        """The dump's shape as captured on API 36: the Activity's content (a TextureView, then a
        ScrollView holding the content LinearLayout of the sample's eight views) followed by the
        platform's action-bar branch, whose title TextView carries the app label."""
        def node(cls, text="", bounds="[0,0][1080,2400]", enabled_="true", children=None):
            head = (f'<node index="0" text={quoteattr(text)} resource-id="" class="{cls}" package="{dp.PACKAGE}" '
                    f'content-desc="" checkable="false" checked="false" clickable="false" enabled="{enabled_}" '
                    f'focusable="false" focused="false" scrollable="false" long-clickable="false" password="false" '
                    f'selected="false" bounds="{bounds}"')
            return head + (" />" if children is None else ">" + "".join(children) + "</node>")
        parts = []
        labels = list(dp.LABELS)
        if self.layout == "labels_swapped":
            labels[0], labels[1] = labels[1], labels[0]
        for label in labels:
            top = 300 + 250 * dp.LABELS.index(label)
            parts.append(node("android.widget.TextView", label, f"[40,{top}][1040,{top + 70}]"))
            if label == "Relay URL" and self.twice_label:
                parts.append(node("android.widget.TextView", label, f"[40,{top}][1040,{top + 70}]"))
            if label == "Relay URL" and self.layout == "misplaced":
                continue                              # its field is written after the next label instead
            if label == "Namespace" and self.layout == "missing_namespace":
                continue
            if label == "Objects to receive" and self.layout == "extra_budget":
                parts.append(node("android.widget.EditText", self.fields[label], f"[40,{top + 80}][1040,{top + 90}]"))
            parts.append(node("android.widget.EditText", self.fields[label], self._field_bounds(label)))
            if label == "Namespace" and self.layout == "misplaced":
                parts.append(node("android.widget.EditText", self.fields["Relay URL"], self._field_bounds("Relay URL")))
        parts.append(node("android.widget.Button", self.button, self.button_bounds, "true" if enabled else "false"))
        if self.extra_button:
            parts.append(node("android.widget.Button", "Start", "[40,1700][1040,1800]"))
        if self.layout == "wrong_sibling":
            parts.append(node("android.widget.ImageView", "", "[40,1630][1040,1640]"))
        the_log = node("android.widget.TextView", "\n".join(log), "[40,1640][1040,2300]")
        if self.layout not in ("log_missing", "log_elsewhere"):
            parts.append(the_log)
        if self.extra_log:
            parts.append(node("android.widget.TextView", "", "[40,2300][1040,2380]"))
        pairs = []
        if self.layout == "pairs_elsewhere":                  # the three pairs in a parent of their own
            cut = next(i for i, part in enumerate(parts) if 'class="android.widget.Button"' in part)
            pairs, parts = [node("android.widget.LinearLayout", children=parts[:cut])], parts[cut:]
        content = node("android.widget.LinearLayout", bounds="[24,604][1056,1336]", children=parts)
        scroll = node("android.widget.ScrollView", bounds="[24,604][1056,2376]", children=pairs + [content])
        activity = node("android.widget.LinearLayout", children=[node("android.view.TextureView", bounds="[24,24][1056,604]"),
                                                                scroll] + ([the_log] if self.layout == "log_elsewhere" else []))
        branches = [activity]
        if self.action_bar:
            title = node("android.widget.TextView", "MoQ Player Sample", "[42,166][510,237]")
            branches.append(node("android.widget.FrameLayout", bounds="[0,0][1080,275]",
                                 children=[node("android.view.ViewGroup", bounds="[0,128][1080,275]", children=[title])]))
        return ("<?xml version='1.0' encoding='UTF-8' standalone='yes' ?><hierarchy rotation=\"0\">"
                + node("android.widget.FrameLayout", children=branches) + "</hierarchy>")


def player(fake, clock=time.monotonic):
    return dp.DevicePlayer(m.Device(SERIAL, fake), APK, 60, PLAYER_URL, clock=clock)


def prepared(fake):
    p = player(fake)
    assert p.install(threading.Event(), 60) is None
    assert p.prepare(threading.Event(), 30) is None
    return p


class Install(unittest.TestCase):
    def test_absent_install_present(self):
        fake = FakeDevice()
        self.assertIsNone(player(fake).install(threading.Event(), 60))
        self.assertEqual([c[1] for c in fake.calls], [
            adb("shell", "pm", "list", "packages", dp.PACKAGE), adb("install", APK), adb("shell", "pm", "list", "packages", dp.PACKAGE)])

    def test_a_package_present_before_install_is_refused(self):
        fake = FakeDevice()
        fake.installed = True
        self.assertEqual(player(fake).install(threading.Event(), 60), f"{dp.PACKAGE} present before install")
        self.assertEqual(fake.names(), ["package"])

    def test_an_install_that_does_not_succeed_is_named(self):
        fake = FakeDevice()
        fake.install_out = "Performing Streamed Install\nFailure [INSTALL_FAILED_INSUFFICIENT_STORAGE]\n"
        self.assertEqual(player(fake).install(threading.Event(), 60),
                         f"install did not report Success: {fake.install_out!r}")

    def test_an_install_that_leaves_no_package_is_named(self):
        fake = FakeDevice()
        fake._install = lambda argv: "Success\n"               # reports success, installs nothing
        self.assertEqual(player(fake).install(threading.Event(), 60), f"{dp.PACKAGE} not present after install")


class Prepare(unittest.TestCase):
    def test_the_exact_sequence(self):
        fake = FakeDevice()
        p = player(fake)
        self.assertIsNone(p.prepare(threading.Event(), 30))
        self.assertEqual(p.pid, 4242)
        self.assertEqual(fake.names(), ["start", "pidof", "ui_dump", "ui_dump", "pidof"])      # nothing is typed or tapped
        argv = {c[0]: c[1] for c in fake.calls}
        self.assertEqual(argv["start"], adb("shell", "am", "start", "-W", "-n", dp.COMPONENT, "--ez", dp.OBSERVE_EXTRA, "true",
                                            "--es", "org.openmoq.moq5.samples.player.RELAY_URL", PLAYER_URL,
                                            "--es", "org.openmoq.moq5.samples.player.NAMESPACE", "live/cam1",
                                            "--es", "org.openmoq.moq5.samples.player.OBJECT_BUDGET", "60"))
        self.assertEqual(fake.fields, INPUTS)
        self.assertTrue(all(t <= 30 for _n, _a, t in fake.calls))

    def test_the_launch_shape_is_the_devices_declared_start(self):
        declared = m.Device(SERIAL, FakeDevice()).commands()["start"]
        got = adb(*player(FakeDevice()).launch_args())
        self.assertEqual(len(got), len(declared))
        self.assertEqual([w for w, d in zip(got, declared) if not d.startswith("<")],
                         [d for d in declared if not d.startswith("<")])

    def test_launch_departures_are_named(self):
        cases = (
            ("an error at rc 0", "Error: Activity class {org.openmoq.moq5.samples.player/.PlayerActivity} does not exist.\n",
             "launch Status is None, expected 'ok'"),
            ("a warm start", FakeDevice().launch.replace("COLD", "WARM"), "launch LaunchState is 'WARM', expected 'COLD'"),
            ("another activity", FakeDevice().launch.replace("Activity: org.openmoq.moq5.samples.player/.PlayerActivity",
                                                             "Activity: com.android.launcher3/.Launcher"),
             "launch Activity is 'com.android.launcher3/.Launcher', expected 'org.openmoq.moq5.samples.player/.PlayerActivity'"),
        )
        for label, out, reason in cases:
            with self.subTest(label):
                fake = FakeDevice()
                fake.launch = out
                self.assertEqual(player(fake).prepare(threading.Event(), 30), reason)
                self.assertNotIn("tap", fake.names())

    def test_more_than_one_pid_is_named(self):
        fake = FakeDevice()
        fake.pids = ["4242 4243\n"]
        self.assertEqual(player(fake).prepare(threading.Event(), 30),
                         f"pidof {dp.PACKAGE}: ['4242', '4243'], expected exactly one pid")

    def test_the_whole_phase_is_bounded_by_its_deadline(self):
        fake = FakeDevice()
        fake.enabled_after = 10 ** 6                              # Start never becomes enabled
        began = time.monotonic()
        reason = player(fake).prepare(threading.Event(), 0.35)
        self.assertEqual(reason, "Start never became ready: Start is not enabled")
        self.assertLess(time.monotonic() - began, 0.35 + 0.25)
        self.assertTrue(all(t <= 0.35 for _n, _a, t in fake.calls), fake.calls)
        self.assertNotIn("tap", fake.names())

    def test_a_launch_that_did_not_preset_the_inputs_is_named(self):
        fake = FakeDevice()
        fake.honour_extras = False                              # a sample that ignores the extras: defaults stay
        self.assertEqual(player(fake).prepare(threading.Event(), 30), "the launch did not preset the inputs: "
                         "'Relay URL' reads 'moqt://10.0.2.2:4443', expected 'moqt://10.0.2.2:46443'")
        self.assertNotIn("tap", fake.names())

    def test_each_field_that_does_not_read_back_is_named(self):
        for field, value in (("Relay URL", "moqt://10.0.2.2:4644"), ("Namespace", "live/cam2"), ("Objects to receive", "600")):
            with self.subTest(field):
                fake = FakeDevice()
                fake.after.append(("start", lambda f=fake, k=field, v=value: f.fields.__setitem__(k, v)))
                self.assertEqual(player(fake).prepare(threading.Event(), 30), "the launch did not preset the inputs: "
                                 f"{field!r} reads {value!r}, expected {INPUTS[field]!r}")
                self.assertNotIn("tap", fake.names())

    def test_the_samples_default_destination_is_refused_before_any_command(self):
        # The sample keeps 4443 as its ordinary default; a supervised player never targets it.
        for url in (dp.DEFAULT_URL, "moqt://127.0.0.1:4443", "moqt://10.0.2.2:4443/"):
            with self.subTest(url=url):
                fake = FakeDevice()
                with self.assertRaises(ValueError) as caught:
                    dp.DevicePlayer(m.Device(SERIAL, fake), APK, 60, url)
                self.assertTrue(str(caught.exception).startswith("'Relay URL': ") and "port 4443" in str(caught.exception),
                                str(caught.exception))
                self.assertEqual(fake.calls, [])

    def test_a_relay_url_without_a_valid_port_is_refused_before_any_command(self):
        for url in ("moqt://10.0.2.2", "moqt://10.0.2.2:99999", "moqt://10.0.2.2:port"):
            with self.subTest(url=url):
                fake = FakeDevice()
                with self.assertRaises(ValueError) as caught:
                    dp.DevicePlayer(m.Device(SERIAL, fake), APK, 60, url)
                self.assertEqual(str(caught.exception), f"'Relay URL': {url!r} names no port")
                self.assertEqual(fake.calls, [])

    def test_an_input_the_device_shell_would_reinterpret_is_refused_before_any_command(self):
        for url in ("moqt://10.0.2.2:46443 ", "moqt://10.0.2.2:46443;id", "moqt://10.0.2.2:$PORT", "moqt://[::1]:46443", ""):
            with self.subTest(url=url):
                fake = FakeDevice()
                with self.assertRaises(ValueError) as caught:
                    dp.DevicePlayer(m.Device(SERIAL, fake), APK, 60, url)
                self.assertTrue(str(caught.exception).startswith("'Relay URL': "), str(caught.exception))
                self.assertEqual(fake.calls, [])

    def test_screen_ambiguity_is_named(self):
        for label, attr, reason in (("two buttons", "extra_button", "ui dump has 2 buttons, expected 1"),
                                    ("two labels", "twice_label", "ui dump has 2 'Relay URL' labels, expected 1"),
                                    ("two logs", "extra_log", "the button's parent has 2 children after it, expected only the log")):
            with self.subTest(label):
                fake = FakeDevice()
                setattr(fake, attr, True)
                self.assertEqual(player(fake).prepare(threading.Event(), 30), reason)

    def test_a_malformed_dump_is_named(self):
        fake = FakeDevice()
        fake._ui_dump = lambda argv: "<?xml version='1.0'?><hierarchy><node></hierarchy>" + dp.DUMP_TRAILER
        self.assertTrue(player(fake).prepare(threading.Event(), 30).startswith("ui dump does not parse: "))


class LogIdentity(unittest.TestCase):
    """The log is the button's immediate TextView sibling in the content parent, and its last child."""

    def test_the_platform_title_branch_is_not_the_log(self):
        fake = FakeDevice()                                   # action_bar is on, as captured on API 36
        self.assertIsNone(player(fake).prepare(threading.Event(), 30))

    def test_departures_from_button_then_log_are_named(self):
        for layout, reason in (
                ("log_missing", "the button's parent has no child after it; expected the log TextView"),
                ("wrong_sibling", "the child after the button is android.widget.ImageView, not the log TextView"),
                ("log_elsewhere", "the button's parent has no child after it; expected the log TextView")):
            with self.subTest(layout):
                fake = FakeDevice()
                fake.layout = layout
                self.assertEqual(player(fake).prepare(threading.Event(), 30), reason)
                self.assertNotIn("tap", fake.names())

    def test_the_pairs_must_share_the_buttons_parent(self):
        fake = FakeDevice()
        fake.layout = "pairs_elsewhere"
        self.assertEqual(player(fake).prepare(threading.Event(), 30),
                         "the label/field pairs and the button are not in one content parent")
        self.assertNotIn("tap", fake.names())


class FakeClock:
    """Time that moves only when a test moves it; `script` holds readings to return next, in order."""

    def __init__(self):
        self.t, self.script = 0.0, []

    def __call__(self):
        if self.script:
            self.t = self.script.pop(0)
        return self.t


class Structure(unittest.TestCase):
    """The three label/field pairs the sample builds, in its order, each field right after its label."""

    def test_departures_from_the_three_pairs_are_refused_before_any_tap(self):
        for layout, reason in (
                ("extra_budget", "ui dump has 4 input fields, expected 3"),
                ("missing_namespace", "ui dump has 2 input fields, expected 3"),
                ("misplaced", "the node after 'Relay URL' is android.widget.TextView, not its field"),
                ("labels_swapped", "the labels are ['Namespace', 'Relay URL', 'Objects to receive'], "
                                   "expected ['Relay URL', 'Namespace', 'Objects to receive']")):
            with self.subTest(layout):
                fake = FakeDevice()
                fake.layout = layout
                self.assertEqual(player(fake).prepare(threading.Event(), 30), reason)
                self.assertNotIn("tap", fake.names())


class Start(unittest.TestCase):
    def marked(self, fake):
        return lambda: fake.calls.append(("dispatched", [], None))

    def admit(self, fake, refusal=None, then=None):
        """The supervisor's pre-dispatch guard, recorded in the call order; `then` runs inside it."""
        def admit():
            fake.calls.append(("admit", [], None))
            if then is not None:
                then()
            return refusal
        return admit

    def test_one_tap_at_the_current_bounds_right_after_dispatch(self):
        fake = FakeDevice()
        p = prepared(fake)
        fake.button_bounds = "[100,1800][500,1900]"               # moved since preparation
        before = len(fake.calls)
        self.assertIsNone(p.start(threading.Event(), self.marked(fake), self.admit(fake)))
        names = fake.names()[before:]
        self.assertEqual(names[:5], ["pidof", "ui_dump", "admit", "dispatched", "tap"])
        self.assertEqual(fake.calls[before + 4][1], adb("shell", "input", "tap", "300", "1850"))
        self.assertEqual(names.count("tap"), 1)
        self.assertEqual(names.count("admit"), 1)
        self.assertEqual(names[5:], ["ui_dump"])                  # Stop confirmed
        self.assertTrue(all(t <= dp.HANDOFF_S for _n, _a, t in fake.calls[before:] if t is not None))

    def assert_refused_before_dispatch(self, fake, p, reason):
        before = len(fake.calls)
        self.assertEqual(p.start(threading.Event(), self.marked(fake), self.admit(fake)), reason)
        self.assertNotIn("dispatched", fake.names()[before:])
        self.assertNotIn("tap", fake.names()[before:])
        self.assertNotIn("admit", fake.names()[before:])          # the guard is asked only when all else holds

    def test_a_changed_process_is_refused_before_dispatch(self):
        fake = FakeDevice()
        p = prepared(fake)
        fake.pids = [5151]
        self.assert_refused_before_dispatch(fake, p, "the player process changed: 4242 became 5151")

    def test_a_button_already_reading_stop_is_refused_before_dispatch(self):
        fake = FakeDevice()
        p = prepared(fake)
        fake.button = "Stop"
        self.assert_refused_before_dispatch(fake, p, "Start is not ready: the button already reads Stop")

    def test_a_failed_precheck_dump_is_refused_before_dispatch(self):
        fake = FakeDevice()
        p = prepared(fake)
        fake.fail["ui_dump"] = 1
        self.assert_refused_before_dispatch(fake, p, "before the tap: ui_dump: rc 1: 'error: fault'")

    def test_an_input_that_changed_since_preparation_is_refused_before_dispatch(self):
        # The Activity reads its fields at the tap; a recreated Activity built from defaults shows 4443 again.
        for field, value in (("Relay URL", dp.DEFAULT_URL), ("Relay URL", "moqt://10.0.2.2:46444"),
                             ("Namespace", "live/cam2"), ("Objects to receive", dp.DEFAULT_BUDGET)):
            with self.subTest(field=field, value=value):
                fake = FakeDevice()
                p = prepared(fake)
                fake.fields[field] = value
                self.assert_refused_before_dispatch(fake, p, f"before the tap: {field!r} reads {value!r}, expected {INPUTS[field]!r}")

    def test_the_inputs_and_the_tapped_bounds_come_from_one_dump(self):
        # The precheck dump shows the default URL with Start at new bounds; nothing earlier can stand in for it.
        fake = FakeDevice()
        p = prepared(fake)
        fake.button_bounds = "[100,1800][500,1900]"
        fake.after.append(("pidof", lambda: fake.fields.__setitem__("Relay URL", dp.DEFAULT_URL)))
        self.assert_refused_before_dispatch(fake, p, f"before the tap: 'Relay URL' reads {dp.DEFAULT_URL!r}, expected {PLAYER_URL!r}")

    def test_inputs_that_change_by_the_stop_confirmation_are_named(self):
        fake = FakeDevice()
        p = prepared(fake)
        fake.after.append(("tap", lambda: fake.fields.__setitem__("Relay URL", dp.DEFAULT_URL)))
        before = len(fake.calls)
        self.assertEqual(p.start(threading.Event(), self.marked(fake), self.admit(fake)),
                         f"after the tap: 'Relay URL' reads {dp.DEFAULT_URL!r}, expected {PLAYER_URL!r}")
        self.assertEqual(fake.names()[before:].count("tap"), 1)

    def test_a_refused_admission_never_dispatches(self):
        fake = FakeDevice()
        p = prepared(fake)
        before = len(fake.calls)
        self.assertEqual(p.start(threading.Event(), self.marked(fake), self.admit(fake, "relay exited (rc 1) before the Start dispatch")),
                         "before the tap: relay exited (rc 1) before the Start dispatch")
        self.assertEqual(fake.names()[before:], ["pidof", "ui_dump", "admit"])

    def test_a_failed_tap_is_never_retried(self):
        fake = FakeDevice()
        p = prepared(fake)
        fake.fail["tap"] = 255
        before = len(fake.calls)
        self.assertEqual(p.start(threading.Event(), self.marked(fake), self.admit(fake)), "the Start tap failed: tap: rc 255: 'error: fault'")
        self.assertEqual(fake.names()[before:], ["pidof", "ui_dump", "admit", "dispatched", "tap"])

    def test_a_missing_stop_is_bounded_and_never_retried(self):
        fake = FakeDevice()
        p = prepared(fake)
        fake.confirm = False
        before, began = len(fake.calls), time.monotonic()
        self.assertEqual(p.start(threading.Event(), self.marked(fake), self.admit(fake)), "start not confirmed: the button reads 'Start'")
        self.assertLess(time.monotonic() - began, dp.HANDOFF_S + 0.5)
        self.assertEqual(fake.names()[before:].count("tap"), 1)

    def clocked(self):
        fake, clock = FakeDevice(), FakeClock()
        p = dp.DevicePlayer(m.Device(SERIAL, fake), APK, 60, PLAYER_URL, clock=clock)
        assert p.install(threading.Event(), 60) is None and p.prepare(threading.Event(), 30) is None
        return fake, clock, p

    def test_a_deadline_reached_by_the_precheck_never_dispatches(self):
        fake, clock, p = self.clocked()
        before = len(fake.calls)
        fake.after.append(("ui_dump", lambda: setattr(clock, "t", clock.t + dp.HANDOFF_S)))
        self.assertEqual(p.start(threading.Event(), self.marked(fake), self.admit(fake)), "before the tap: the handoff deadline passed")
        self.assertEqual(fake.names()[before:], ["pidof", "ui_dump"])

    def test_an_admission_that_outlasts_the_handoff_never_dispatches(self):
        fake, clock, p = self.clocked()
        before = len(fake.calls)
        admit = self.admit(fake, then=lambda: setattr(clock, "t", clock.t + dp.HANDOFF_S))
        self.assertEqual(p.start(threading.Event(), self.marked(fake), admit), "before the tap: the handoff deadline passed")
        self.assertEqual(fake.names()[before:], ["pidof", "ui_dump", "admit"])

    def test_a_stop_during_admission_never_dispatches(self):
        fake, clock, p = self.clocked()
        stop, before = threading.Event(), len(fake.calls)
        self.assertEqual(p.start(stop, self.marked(fake), self.admit(fake, then=stop.set)), "before the tap: the run stopped")
        self.assertEqual(fake.names()[before:], ["pidof", "ui_dump", "admit"])

    def test_a_stop_before_dispatch_never_dispatches(self):
        fake, clock, p = self.clocked()
        stop, before = threading.Event(), len(fake.calls)
        fake.after.append(("ui_dump", stop.set))
        self.assertEqual(p.start(stop, self.marked(fake), self.admit(fake)), "before the tap: the run stopped")
        self.assertEqual(fake.names()[before:], ["pidof", "ui_dump"])

    def test_a_stop_confirmed_only_after_the_deadline_is_not_a_success(self):
        fake, clock, p = self.clocked()
        before = len(fake.calls)
        fake.after.append(("tap", lambda: setattr(clock, "t", clock.t + dp.HANDOFF_S + 1)))
        self.assertEqual(p.start(threading.Event(), self.marked(fake), self.admit(fake)), "start not confirmed: the tap's result arrived after the handoff deadline")
        self.assertEqual(fake.names()[before:], ["pidof", "ui_dump", "admit", "dispatched", "tap"])

    def test_a_stop_seen_by_a_dump_that_returns_late_is_not_a_success(self):
        fake, clock, p = self.clocked()
        before = len(fake.calls)
        fake.after.append(("tap", lambda: fake.after.append(("ui_dump", lambda: setattr(clock, "t", clock.t + dp.HANDOFF_S + 1)))))
        self.assertEqual(p.start(threading.Event(), self.marked(fake), self.admit(fake)), "start not confirmed: Stop seen only after the deadline")
        self.assertEqual(fake.names()[before:], ["pidof", "ui_dump", "admit", "dispatched", "tap", "ui_dump"])

    def test_a_deadline_crossed_while_waiting_between_dumps_names_the_last_state(self):
        fake, clock, p = self.clocked()
        fake.confirm = False
        before = len(fake.calls)
        # the handoff starts at t=0 (deadline HANDOFF_S); after the first confirmation dump, the poll
        # check still sees room for another dump, and the deadline passes during the wait before it
        fake.after.append(("tap", lambda: fake.after.append(
            ("ui_dump", lambda: clock.script.extend([dp.HANDOFF_S - 2 * dp.POLL_S, dp.HANDOFF_S + 0.01])))))
        self.assertEqual(p.start(threading.Event(), self.marked(fake), self.admit(fake)), "start not confirmed: the button reads 'Start'")
        self.assertEqual(fake.names()[before:].count("tap"), 1)

    def test_the_handoff_bound_covers_the_measured_device_latency(self):
        # Per-command latencies measured on the dedicated API-36 emulator (warm uiautomator dump, pidof),
        # plus an allowance for the Start tap, which was not measured; `Stop` shows on the second
        # confirmation dump. The whole handoff must fit its bound with the deadline checks unchanged.
        latency = {"pidof": 0.06, "ui_dump": 2.30, "tap": 0.20}
        fake, clock, p = self.clocked()
        runner = fake.__call__
        def timed(name, argv, timeout):
            out = runner(name, argv, timeout)
            clock.t += latency.get(name, 0.0)
            return out
        p.device.runner = timed
        fake.confirm = False
        fake.after.append(("tap", lambda: fake.after.append(("ui_dump", lambda: setattr(fake, "button", "Stop")))))
        before, began = len(fake.calls), clock.t
        self.assertIsNone(p.start(threading.Event(), self.marked(fake), self.admit(fake)))
        self.assertEqual(fake.names()[before:], ["pidof", "ui_dump", "admit", "dispatched", "tap", "ui_dump", "ui_dump"])
        self.assertLessEqual(clock.t - began, dp.HANDOFF_S)
        self.assertEqual(dp.HANDOFF_S, 20.0)

    def test_a_slow_post_tap_confirmation_fits_the_handoff_tolerance(self):
        # A synthetic boundary vector, not a device measurement: the pre-tap work takes 3.3 s in all,
        # the tap 0.2 s and the ONE post-tap confirmation dump 8 s (each call inside COMMAND_S), and
        # that dump shows Stop with the inputs unchanged. The whole handoff is 11.5 s: refused by a
        # 10 s bound at the confirmation, accepted by the 20 s tolerance the bound actually declares.
        latencies = {"pidof": [0.06], "ui_dump": [3.04, 8.0], "admit": [0.2], "tap": [0.2]}
        for bound, accepted in ((10.0, False), (20.0, True)):
            with self.subTest(bound=bound):
                fake, clock, p = self.clocked()
                runner, queue = fake.__call__, {k: list(v) for k, v in latencies.items()}
                def timed(name, argv, timeout):
                    self.assertLessEqual(timeout, dp.COMMAND_S)
                    out = runner(name, argv, timeout)
                    clock.t += queue[name].pop(0) if queue.get(name) else 0.0
                    return out
                p.device.runner = timed
                admit = self.admit(fake, then=lambda: setattr(clock, "t", clock.t + queue["admit"].pop(0)))
                before, began = len(fake.calls), clock.t
                with unittest.mock.patch.object(dp, "HANDOFF_S", bound):
                    outcome = p.start(threading.Event(), self.marked(fake), admit)
                self.assertEqual(fake.names()[before:], ["pidof", "ui_dump", "admit", "dispatched", "tap", "ui_dump"])
                self.assertAlmostEqual(11.5, clock.t - began)
                if accepted:
                    self.assertIsNone(outcome)
                else:
                    self.assertTrue(outcome is not None and outcome.startswith("start not confirmed"), outcome)

    def test_the_handoff_bound_sits_inside_the_go_window(self):
        self.assertLess(dp.HANDOFF_S, m.Deadlines().go_window)


class FakeGate:
    def __init__(self):
        self.facts, self.refusals, self.changed = [], [], threading.Event()

    def fact(self, name):
        self.facts.append(name)
        self.changed.set()

    def refuse(self, reason):
        self.refusals.append(reason)
        self.changed.set()


class Observe(unittest.TestCase):
    def observe(self, fake, p, until):
        gate, stop = FakeGate(), threading.Event()
        t = threading.Thread(target=p.observe, args=(gate, stop), daemon=True)
        t.start()
        deadline = time.monotonic() + 5
        while not until(gate) and time.monotonic() < deadline:
            gate.changed.wait(0.05)
            gate.changed.clear()
        stop.set()
        t.join(5)
        self.assertFalse(t.is_alive(), "observer did not return")
        return gate

    def started(self, fake, log):
        p = prepared(fake)
        fake.log, fake.button = list(log), "Stop"
        return p

    def test_the_exact_lines_become_facts_and_receipts_are_left_to_the_sidecar(self):
        fake = FakeDevice()
        with tempfile.TemporaryDirectory() as tmp:
            fake.stopped_when = Path(tmp) / "stopped"
            p = self.started(fake, [dp.ESTABLISHED])
            gate = FakeGate()
            gate_thread_stop = threading.Event()
            t = threading.Thread(target=p.observe, args=(gate, gate_thread_stop), daemon=True)
            t.start()
            deadline = time.monotonic() + 5
            while "player_established" not in gate.facts and time.monotonic() < deadline:
                time.sleep(0.02)
            fake.log.append(PRESENTED_PASS)
            fake.stopped_when.touch()
            while "player_stopped" not in gate.facts and time.monotonic() < deadline:
                time.sleep(0.02)
            gate_thread_stop.set()
            t.join(5)
        self.assertEqual((gate.facts, gate.refusals), (["player_established", "player_stopped"], []))

    def test_departures_are_refused_by_name(self):
        cases = (
            ("another version", ["established: MoqVersion.Draft16"], None, "player reported 'established: MoqVersion.Draft16'"),
            ("no version", ["established: null"], None, "player reported 'established: null'"),
            ("a failure", [dp.ESTABLISHED, "PlaybackTerminatedException: the receiver terminated"], None,
             "player reported 'PlaybackTerminatedException: the receiver terminated'"),
            ("another terminal", [dp.ESTABLISHED, "stopped: ReceiverEnded; 59 objects, 59 decoder inputs"], None,
             "player reported 'stopped: ReceiverEnded; 59 objects, 59 decoder inputs'"),
        )
        for label, log, _unused, reason in cases:
            with self.subTest(label):
                fake = FakeDevice()
                gate = self.observe(fake, self.started(fake, log), lambda g: g.refusals)
                self.assertEqual(gate.refusals, [reason])

    def test_a_cleared_log_is_refused(self):
        fake = FakeDevice()
        p = self.started(fake, [dp.ESTABLISHED])
        seen = threading.Event()

        def until(gate):
            if gate.facts and not seen.is_set():
                seen.set()
                fake.log = []                                 # the session's log cleared under the observer
            return gate.refusals
        gate = self.observe(fake, p, until)
        self.assertEqual(gate.refusals, ["player log changed under the observer: ['established: MoqVersion.Draft18'] became []"])

    def test_a_changed_process_is_refused(self):
        fake = FakeDevice()
        p = self.started(fake, [])
        fake.pids = [5151]
        gate = self.observe(fake, p, lambda g: g.refusals)
        self.assertEqual(gate.refusals, ["player process changed while observing"])

    def test_an_ambiguous_log_target_is_refused(self):
        fake = FakeDevice()
        p = self.started(fake, [dp.ESTABLISHED])
        fake.extra_log = True
        gate = self.observe(fake, p, lambda g: g.refusals)
        self.assertEqual(gate.refusals, ["player observation: the button's parent has 2 children after it, expected only the log"])


class Sidecar(unittest.TestCase):
    def test_the_receipt_stream_is_this_process_on_the_dedicated_serial(self):
        fake = FakeDevice()
        self.assertEqual(prepared(fake).log_argv(),
                         adb("logcat", "--pid=4242", "-v", "raw", "-s", "MoqPlayer:I", "MoqPlayerObserver:I"))

    def test_no_stream_without_a_prepared_process(self):
        with self.assertRaises(dp.DeviceError):
            player(FakeDevice()).log_argv()


class Supervised(unittest.TestCase):
    """The actor under the real supervisor, with owned stub children and the scripted device. The
    sidecar argv is derived from the actor, checked, then replaced by the offline receipt stub."""

    def run_supervised(self, receipts):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        d = Path(tmp.name)
        for name, src in (("relay", ROUTE_RELAY_STUB), ("publisher", m._PUBLISHER_STUB), ("host", m._HOST_STUB),
                          ("player_log", PLAYER_LOG_STUB)):
            (d / f"{name}.py").write_text(src)
        (d / "blocks.json").write_text(json.dumps([ONE_SUB, ONE_SUB, TWO_SUBS]))
        trigger = d / "ended-observed was forwarded"
        fake = FakeDevice()
        fake.stopped_when = trigger
        actor = player(fake)
        derived = []

        def log_argv():
            derived.append(actor.log_argv())
            return [sys.executable, "-I", d / "player_log.py", trigger, json.dumps(list(receipts)), "normal"]
        py = sys.executable
        actors = m.Actors(
            relay_argv=[py, "-I", d / "relay.py", json.dumps(m.RELAY_STUB_RECORDS["ready"]), d / "blocks.json",
                        d / "requests.log", ""],
            publisher_argv=[py, "-I", d / "publisher.py", "60", "normal"], host_argv=[py, "-I", d / "host.py", "none"],
            player_probe=actor.observe,
            route_barrier=lambda ha, ps, record: route_dump.route_barrier(NS, NAME, ha, ps, interval_s=0.05, record=record),
            player_start=actor.start, player_install=actor.install, player_prepare=actor.prepare, player_log_argv=log_argv,
            relay_identity=m.OFFLINE_IDENTITY, expected_exits=m.OFFLINE_EXITS, env={"PATH": "/usr/bin:/bin"},
            hooks={"forwarded": lambda control: trigger.touch() if control == "ended-observed" else None})
        rec = m.supervise(d, actors, FAST, 60)
        return rec, fake, derived

    def test_the_actor_settles_only_through_its_receipts(self):
        rec, fake, derived = self.run_supervised((PRESENTED_PASS, CLOSED))
        self.assertEqual(rec["verdict"], {"ok": True, "failures": []})
        self.assertEqual(forwarded(rec), list(m.CONTROLS))
        self.assertEqual(derived, [adb("logcat", "--pid=4242", "-v", "raw", "-s", "MoqPlayer:I", "MoqPlayerObserver:I")])
        p = pairs(rec)
        for earlier, later in ((("player_install", "done"), ("player_prepare", "done")),
                               (("player_prepare", "done"), ("player_log", "started")),
                               (("player_start", "admitted"), ("player_start", "dispatched")),
                               (("player_start", "dispatched"), ("player", "player_established")),
                               (("player", "player_stopped"), ("player", "player_settled"))):
            self.assertLess(p.index(earlier), p.index(later))
        self.assertEqual(fake.names().count("tap"), 1)            # the one Start; nothing is typed
        self.assertTrue(all(c[1][:3] == ["adb", "-s", SERIAL] for c in fake.calls if c[1]))
        self.assertNotIn("uninstall", fake.names())
        self.assertIs(rec["threads_settled"], True)

    def test_the_ui_lines_alone_never_settle(self):
        rec, _fake, _derived = self.run_supervised(())
        self.assertEqual(rec["primary_error"], f"done: barrier(s) player_settled not observed within {FAST.done}s of publisher_complete")
        self.assertIn(("player", "player_stopped"), pairs(rec))


class ContendedDevice(FakeDevice):
    """The fake device WITHOUT FakeDevice's whole-call serialization for UI dumps, as a real device
    behaves: each dump is in flight until it returns, `in_flight_max` records the most at once, and a
    dump that starts while another is in flight is answered `Killed` (this fake's model of an overlapping
    UiAutomation use). `hold()` keeps the next dump in flight until `release()`; every hold is bounded so
    a candidate that never releases cannot hang the suite."""

    def __init__(self):
        super().__init__()
        self.flight, self.in_flight, self.in_flight_max = threading.Lock(), 0, 0
        self.overlapped, self.held, self.released, self.holding = threading.Event(), threading.Event(), threading.Event(), False
        self.dump_threads, self.raise_on_dump = [], None

    def hold(self):
        self.holding, self.released = True, threading.Event()
        self.held.clear()

    def release(self):
        self.released.set()

    def __call__(self, name, argv, timeout):
        if name != "ui_dump":
            return super().__call__(name, argv, timeout)
        with self.flight:
            self.in_flight += 1
            self.in_flight_max = max(self.in_flight_max, self.in_flight)
            overlapping, hold = self.in_flight > 1, self.holding
            self.holding = False
            self.dump_threads.append(threading.current_thread().name)
        try:
            if overlapping:
                self.overlapped.set()
                with self.lock:
                    self.calls.append((name, list(argv), timeout))
                return {"rc": 0, "stdout": "Killed \n", "stderr": "", "timeout": False}
            if hold:
                self.held.set()
                self.released.wait(5.0)
            if self.raise_on_dump is not None:
                fault, self.raise_on_dump = self.raise_on_dump, None
                with self.lock:
                    self.calls.append((name, list(argv), timeout))
                raise fault
            return super().__call__(name, argv, timeout)
        finally:
            with self.flight:
                self.in_flight -= 1


class WitnessLock:
    """Delegates to the player's own dump lock, and witnesses contention: an `acquire` attempted while
    another thread holds it records the attempting thread in `contenders` and sets `contended`. The
    real lock still decides who proceeds; this only observes that the boundary was actually reached."""

    def __init__(self, real):
        self.real, self.contended, self.contenders = real, threading.Event(), []

    def acquire(self, *args, **kwargs):
        if self.real.locked():
            self.contenders.append(threading.current_thread().name)
            self.contended.set()
        return self.real.acquire(*args, **kwargs)

    def release(self):
        self.real.release()

    def locked(self):
        return self.real.locked()


class Gate:
    def __init__(self):
        self.facts, self.refusals = [], []

    def fact(self, name):
        self.facts.append(name)

    def refuse(self, reason):
        self.refusals.append(reason)


class ThreadClock:
    """`time.monotonic` for every thread but the one named `scripted`, which reads `script` in order
    (then keeps the last reading): the Start path's deadlines move only as the test scripts them."""

    def __init__(self, scripted, script):
        self.scripted, self.script, self.last = scripted, list(script), None

    def __call__(self):
        if threading.current_thread().name != self.scripted:
            return time.monotonic()
        if self.script:
            self.last = self.script.pop(0)
        return self.last


class DumpSerialization(unittest.TestCase):
    """The player's UI dumps -- preparation, the observer and the Start path -- go through one
    instance-owned boundary: at most one in flight, waiting on the caller's own deadline."""

    def setUp(self):
        self.fake = ContendedDevice()
        self.stop = threading.Event()
        self.workers = []
        self.addCleanup(self._retire)

    def _retire(self):
        self.stop.set()
        self.fake.release()
        for t in self.workers:
            t.join(10.0)
        self.assertEqual([t.name for t in self.workers if t.is_alive()], [], "a test worker was left running")

    def worker(self, name, fn, *args):
        """`fn(*args)` on an owned thread: its return value in out["result"], or the exception it raised,
        with its traceback, in out["error"] -- never a loose thread traceback, never a silent None."""
        out = {}

        def run():
            try:
                out["result"] = fn(*args)
            except BaseException:
                out["error"] = traceback.format_exc()
        t = threading.Thread(target=run, name=name, daemon=True)
        self.workers.append(t)
        t.start()
        return t, out

    def prepared(self, clock=time.monotonic):
        p = dp.DevicePlayer(m.Device(SERIAL, self.fake), APK, 60, PLAYER_URL, clock=clock)
        self.assertIsNone(p.install(threading.Event(), 60))
        self.assertIsNone(p.prepare(threading.Event(), 30))
        return p

    def witnessed(self, p):
        """The player's dump lock, wrapped so the rows can see a contender reach it."""
        p._dumping = WitnessLock(p._dumping)
        return p._dumping

    def await_contention(self, witness, contender):
        """Returns once `contender` has attempted the dump boundary while the held dump is in flight.
        Fails -- never releasing the holder first -- if instead a second dump overlapped the held one,
        or if no contender arrived within the bound: a serial run must not stand in for contention."""
        for _ in range(200):
            if self.fake.overlapped.is_set():
                self.fail(f"a second UI dump overlapped the held one (threads {self.fake.dump_threads})")
            if witness.contended.is_set():
                self.assertEqual(witness.contenders, [contender])
                return
            witness.contended.wait(0.025)
        self.fail(f"no contention witness: {contender} never attempted the dump boundary while it was held")

    def completed(self, out):
        self.assertNotIn("error", out, out.get("error"))
        self.assertIn("result", out, "the worker returned nothing")
        return out["result"]

    def admitted(self):
        return lambda: self.fake.calls.append(("admit", [], None)) and None

    def test_the_observer_and_the_start_path_never_dump_at_once_and_both_complete(self):
        p = self.prepared()
        witness = self.witnessed(p)
        gate = Gate()
        self.fake.hold()
        observer, _ = self.worker("observer", p.observe, gate, self.stop)
        self.assertTrue(self.fake.held.wait(5.0), "the observer's dump never started")
        starter, started = self.worker("start", p.start, self.stop, lambda: self.fake.calls.append(("dispatched", [], None)),
                                       self.admitted())
        self.await_contention(witness, "start")
        self.fake.release()
        starter.join(10.0)
        self.assertFalse(starter.is_alive())
        self.assertIsNone(self.completed(started))
        self.assertEqual(self.fake.in_flight_max, 1, "two UI dumps were in flight at once")
        names = self.fake.names()
        self.assertEqual(names.count("tap"), 1)
        self.assertEqual(names.count("dispatched"), 1)
        for _ in range(50):                                            # the observer sees the tap's line (bounded)
            if "player_established" in gate.facts:
                break
            self.stop.wait(0.05)
        self.assertEqual((gate.facts, gate.refusals), (["player_established"], []))
        self.stop.set()
        observer.join(10.0)
        self.assertFalse(observer.is_alive())

    def test_a_start_whose_deadline_expires_while_waiting_is_refused_and_never_reaches_the_device(self):
        # The Start thread's readings: its deadline (0 + HANDOFF_S), pidof's check, then the wait's
        # start with 0.3 s left; the held observer dump outlasts that.
        p = self.prepared(clock=ThreadClock("start", [0.0, 0.0, dp.HANDOFF_S - 0.3, dp.HANDOFF_S + 1.0]))
        witness = self.witnessed(p)
        gate = Gate()
        self.fake.hold()
        observer, _ = self.worker("observer", p.observe, gate, self.stop)
        self.assertTrue(self.fake.held.wait(5.0))
        before = len(self.fake.calls)
        starter, started = self.worker("start", p.start, self.stop, lambda: self.fake.calls.append(("dispatched", [], None)),
                                       self.admitted())
        starter.join(10.0)
        self.assertFalse(starter.is_alive())
        self.assertEqual((self.fake.in_flight, self.fake.released.is_set()), (1, False),
                         "the Start path gave up only after the held dump ended: its wait was not its deadline")
        self.assertEqual(witness.contenders, ["start"])                  # it waited at the boundary, then gave up
        result = self.completed(started)
        self.assertTrue((result or "").startswith("before the tap: ui_dump: "), result)
        self.assertIn("deadline", result)
        after = self.fake.names()[before:]
        for forbidden in ("ui_dump", "admit", "dispatched", "tap"):
            self.assertNotIn(forbidden, after, f"the expired Start path reached {forbidden}")
        self.assertNotIn("start", self.fake.dump_threads)
        self.fake.release()

    def test_a_dump_granted_after_the_deadline_is_not_run(self):
        # The Start thread waits within its budget, is granted the device once the held dump returns,
        # and by then its deadline has passed: the dump must not be sent.
        p = self.prepared(clock=ThreadClock("start", [0.0, 0.0, 0.0, dp.HANDOFF_S + 1.0]))
        witness = self.witnessed(p)
        gate = Gate()
        self.fake.hold()
        observer, _ = self.worker("observer", p.observe, gate, self.stop)
        self.assertTrue(self.fake.held.wait(5.0))
        before = len(self.fake.calls)
        starter, started = self.worker("start", p.start, self.stop, lambda: self.fake.calls.append(("dispatched", [], None)),
                                       self.admitted())
        self.await_contention(witness, "start")
        self.stop.set()                                                    # the observer takes no further dump
        self.fake.release()
        starter.join(10.0)
        self.assertFalse(starter.is_alive())
        result = self.completed(started)
        self.assertTrue((result or "").startswith("before the tap: ui_dump: "), result)
        self.assertEqual(self.fake.in_flight_max, 1)
        self.assertNotIn("start", self.fake.dump_threads)
        for forbidden in ("admit", "dispatched", "tap"):
            self.assertNotIn(forbidden, self.fake.names()[before:])

    def test_a_failed_malformed_or_interrupted_dump_releases_the_device(self):
        p = self.prepared()
        real = self.fake._ui_dump

        def malformed():                                              # one truncated dump, then the real screen
            self.fake._ui_dump = lambda argv: (delattr(self.fake, "_ui_dump"), "<hierarchy><node")[1]
        for label, rig, error in (
                ("rc 1", lambda: self.fake.fail.__setitem__("ui_dump", 1), dp.DeviceError),
                ("malformed XML", malformed, dp.DeviceError),
                ("interrupted", lambda: setattr(self.fake, "raise_on_dump", KeyboardInterrupt("operator")), KeyboardInterrupt)):
            with self.subTest(label):
                taps = self.fake.names().count("tap")
                rig()
                with self.assertRaises(error):                              # never normalized into a screen
                    p._screen(time.monotonic() + 5.0)
                self.assertEqual(self.fake._ui_dump, real)
                # a later caller within its budget is not blocked by the failed one
                t, out = self.worker(f"later {label}", p._screen, time.monotonic() + 2.0)
                t.join(5.0)
                self.assertFalse(t.is_alive())
                self.assertIsInstance(self.completed(out), dp.Screen)
                self.assertEqual(self.fake.names().count("tap"), taps)

class LaunchModes(unittest.TestCase):
    def test_the_observer_off_diagnostic_launch_names_both_switches(self):
        p = dp.DevicePlayer(m.Device(SERIAL, FakeDevice()), APK, 60, PLAYER_URL, observe=False, trace=True)
        args = p.launch_args()
        self.assertEqual(args[args.index(dp.OBSERVE_EXTRA) - 1:args.index(dp.OBSERVE_EXTRA) + 2], ("--ez", dp.OBSERVE_EXTRA, "false"))
        self.assertEqual(args[args.index(dp.TRACE_EXTRA) - 1:args.index(dp.TRACE_EXTRA) + 2], ("--ez", dp.TRACE_EXTRA, "true"))
        self.assertEqual(dp.TRACE_EXTRA, "org.openmoq.moq5.samples.player.DIAGNOSTIC_TRACE")

    def test_the_default_launch_is_unchanged(self):
        args = dp.DevicePlayer(m.Device(SERIAL, FakeDevice()), APK, 60, PLAYER_URL).launch_args()
        self.assertEqual(args[5:9], (dp.COMPONENT, "--ez", dp.OBSERVE_EXTRA, "true"))
        self.assertNotIn(dp.TRACE_EXTRA, args)


if __name__ == "__main__":
    unittest.main()
