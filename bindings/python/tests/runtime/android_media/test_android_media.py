"""Offline tests for the Android real-media acceptance harness.

    python -I bindings/python/tests/runtime/android_media/test_android_media.py

Nothing here may start a relay, invoke adb, or open a network connection: the
suite installs a tripwire on socket connect/bind for its own process and runs
every stub child with the same tripwire. The media generator test invokes the
local ffmpeg/ffprobe only.
"""
import base64
import json
import os
import socket
import struct
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

_real_connect, _real_bind = socket.socket.connect, socket.socket.bind


def _tripwire(*_a, **_k):
    raise AssertionError("network use in an offline test")


socket.socket.connect = _tripwire
socket.socket.bind = _tripwire


class MarkerVectors(unittest.TestCase):
    def test_vectors_file_is_exactly_what_the_builder_writes(self):
        r = subprocess.run([sys.executable, "-I", str(HERE / "build_vectors.py"), "--check"])
        self.assertEqual(r.returncode, 0, "marker_vectors.json drifted from build_vectors.py")

    def test_reader_matches_every_declared_vector(self):
        import media_marker as marker
        doc = json.loads((HERE / "marker_vectors.json").read_text())
        names = set()
        for v in doc["vectors"]:
            names.add(v["name"])
            got = marker.classify(base64.b64decode(v["luma_b64"]), v["width"], v["height"])
            self.assertEqual((got.outcome, got.index), (v["expected"]["outcome"], v["expected"]["index"]), v["name"])
        # the required kinds are present, not merely whatever the builder happens to emit
        for required in ("index_1_one_bit", "sync_row_data_swapped", "complement_bit2_equals_data", "blank_black",
                         "shifted_one_cell_right", "shifted_down_off_rows", "margin_exact_high", "margin_short_high"):
            self.assertIn(required, names)

    def test_capture_refusals(self):
        import media_marker as marker
        self.assertEqual(marker.classify(None, 64, 36).outcome, "capture-null")
        self.assertEqual(marker.classify(b"\x00" * 10, 64, 36).outcome, "capture-size")
        self.assertEqual(marker.classify(b"", 0, 0).outcome, "capture-empty")


class ObjectsOracle(unittest.TestCase):
    def ledger(self, tmp):
        import media_oracle
        payloads = [bytes([0, 0, 0, 1, 9, 0x10]) + bytes([i]) * (3 + i) for i in range(4)]
        path = Path(tmp) / "objects.bin"
        with open(path, "wb") as f:
            for i, p in enumerate(payloads):
                f.write(struct.pack("<IBQ", len(p), 1 if i in (0, 2) else 0, i * 41666))
                f.write(p)
        return media_oracle.load_objects(path), payloads

    def test_identical_passes_and_mutations_fail_by_name(self):
        import media_oracle
        with tempfile.TemporaryDirectory() as tmp:
            expected, payloads = self.ledger(tmp)
            self.assertEqual(len(expected), 4)
            got = [media_oracle.Received(p, i in (0, 2), i * 41666) for i, p in enumerate(payloads)]
            self.assertEqual(media_oracle.compare(expected, got), [])
            self.assertIn("count 0 != expected 4", media_oracle.compare(expected, []))
            wrong = list(got); wrong[2] = media_oracle.Received(payloads[2][:-1] + b"\xff", True, 2 * 41666)
            self.assertTrue(any(p.startswith("object 2: payload differs") for p in media_oracle.compare(expected, wrong)))
            swapped = [got[1], got[0]] + got[2:]
            self.assertTrue(any(p.startswith("object 0: payload differs") for p in media_oracle.compare(expected, swapped)))
            key = list(got); key[1] = media_oracle.Received(payloads[1], True, 41666)
            self.assertIn("object 1: keyframe True != False", media_oracle.compare(expected, key))
            pts = list(got); pts[3] = media_oracle.Received(payloads[3], False, 1)
            self.assertIn("object 3: pts_us 1 != 124998", media_oracle.compare(expected, pts))
            extra = got + [media_oracle.Received(b"x", False, 5)]
            self.assertIn("count 5 != expected 4", media_oracle.compare(expected, extra))

    def test_malformed_objects_file_refused(self):
        import media_oracle
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "bad.bin"
            p.write_bytes(struct.pack("<IBQ", 10, 1, 0) + b"short")
            with self.assertRaises(ValueError):
                media_oracle.load_objects(p)

    def test_expected_data_never_comes_from_publisher_receipts(self):
        import media_oracle
        self.assertFalse(hasattr(media_oracle, "from_publisher_events"))
        import inspect
        self.assertNotIn("wrote", inspect.getsource(media_oracle))


class Generator(unittest.TestCase):
    def test_generate_twice_identical_and_structurally_valid(self):
        import make_input
        with tempfile.TemporaryDirectory() as tmp:
            receipt = make_input.generate(Path(tmp))
            self.assertTrue(receipt["ok"], json.dumps(receipt["checks"], indent=1))
            c = receipt["checks"]
            for name in ("two_runs_identical", "au_concatenation", "ffprobe_sizes_match", "ffprobe_keyframes_match_idr",
                         "idr_positions_declared", "sps_codec_matches", "au_count_declared", "marker_decoded_exact",
                         "negative_controls_rejected", "objects_file_matches_ledger"):
                self.assertTrue(c[name], name)
            self.assertEqual(receipt["codec"], "avc1.42C01E")
            self.assertTrue((Path(tmp) / "objects.bin").exists() and (Path(tmp) / "expected.tsv").exists())
            self.assertFalse(any(p.suffix == ".h264" for p in HERE.iterdir()), "no media binary may live in the source tree")


class SupervisorControls(unittest.TestCase):
    def test_offline_controls_all_pass(self):
        import run_android_media
        with tempfile.TemporaryDirectory() as tmp:
            results = run_android_media.offline_controls(Path(tmp))
        failed = {k: v for k, v in results.items() if not v.get("ok")}
        self.assertEqual(failed, {}, json.dumps(failed, indent=1, default=str))
        for required in ("positive_control_order", "go_window_timeout_named", "ack_all_timeout_named",
                         "ended_observed_timeout_named", "done_timeout_named", "publisher_early_exit_named",
                         "relay_not_ready_named", "children_settled_on_every_path", "complete_rc_separate_from_shutdown",
                         "stub_adb_tripwire_not_reached", "device_argv_never_targets_shared_emulator",
                         "device_only_dedicated_serial", "relay_ready_refuses_non_matching_records",
                         "late_go_refused_by_absolute_deadline", "phase_deadlines_anchored_to_events",
                         "verdict_fails_on_bad_drain", "verdict_fails_on_unplanned_diagnostic",
                         "verdict_fails_on_every_negative_path", "relay_ready_late_refused",
                         "publisher_ready_late_refused", "shutdown_late_refused",
                         "write_stream_defects_refuse_ack_all", "host_exit_before_control_named",
                         "host_exit_after_ack_all_named", "host_final_line_in_pipe_not_refused",
                         "host_exit_with_ack_all_line_in_pipe_names_host_ended",
                         "host_exit_with_a_stuck_reader_is_bounded_and_named", "host_stdout_is_controls_only",
                         "real_actor_exit_after_done_is_declared", "host_stdout_is_an_exact_control_inventory",
                         "host_stdout_is_raw_ascii_with_one_lf"):
            self.assertIn(required, results)


class RawHostStdout(unittest.TestCase):
    """Only the host child's stdout is read as raw bytes; its stderr and settlement are `Child`'s."""

    PROGRAM = ("import sys; sys.stdout.buffer.write(b'CTRL go\\r\\nCTRL \\xff\\nCTRL done'); sys.stdout.flush(); "
               "sys.stderr.write('note one\\r\\nnote two\\n')")

    def settle(self, child):
        record = child.settle(10)
        self.assertTrue(record["readers_settled"])
        return record

    def test_raw_child_delivers_exact_line_bytes_and_keeps_the_child_contract(self):
        import run_android_media as m
        from runtime_support import Child
        raw_lines = []
        argv = [sys.executable, "-I", "-c", self.PROGRAM]
        raw = self.settle(m.RawStdoutChild("host-receiver", argv, {}, str(HERE), on_line=raw_lines.append))
        text = self.settle(Child("relay", argv, {}, str(HERE), on_line=lambda line: None))
        self.assertEqual(raw_lines, [b"CTRL go\r\n", b"CTRL \xff\n", b"CTRL done"])
        self.assertEqual(sorted(raw), sorted(text))
        self.assertEqual((raw["rc"], raw["how"], raw["reader_errors"]), (0, "exited", []))
        self.assertEqual(raw["stderr"], text["stderr"])
        self.assertEqual(raw["stderr"], "note one\nnote two")
        self.assertEqual(raw["stdout"], "CTRL go\r\nCTRL \\xff\nCTRL done")

    def test_relay_and_publisher_keep_the_shared_text_reader(self):
        import inspect
        import run_android_media as m
        source = inspect.getsource(m.supervise)
        self.assertIn('Child("relay", ', source)
        self.assertIn('Child("publisher", ', source)
        self.assertIn('RawStdoutChild("host-receiver", ', source)
        self.assertEqual(source.count("RawStdoutChild("), 1)


class AckAllLineInPipe(unittest.TestCase):
    """The offline control whose host exits with `ack-all` still in its reader. Its exit must never
    fall inside the route handoff, however long the handoff takes."""

    IN_HANDOFF = "go: refused: host-receiver exited (rc 0) during the route handoff"
    BEFORE_ENDED = "ended-observed: refused: host-receiver exited (rc 0) before host_ended"

    def arm(self, **kw):
        import run_android_media as m
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        _run, record = m._ack_all_line_in_pipe(m._offline_rig(Path(tmp.name)), "arm", **kw)
        return record

    def test_an_ungated_exit_can_fall_inside_the_handoff(self):
        # The schedule without the gate: held open until the watcher has judged the exit, it takes the
        # handoff branch -- the failure seen once in the full offline controls.
        record = self.arm(gated=False, handoff_hold="until-exit")
        self.assertEqual((record["primary"], record["forwarded"], record["ok"]), (self.IN_HANDOFF, [], False))

    def test_an_ungated_exit_falls_inside_a_long_handoff(self):
        record = self.arm(gated=False, handoff_hold=0.5)
        self.assertEqual((record["primary"], record["forwarded"], record["ok"]), (self.IN_HANDOFF, [], False))

    def test_the_gated_arm_names_host_ended_however_long_the_handoff(self):
        for hold in (None, 0.5):
            with self.subTest(hold=hold):
                record = self.arm(handoff_hold=hold)
                self.assertEqual((record["primary"], record["forwarded"]), (self.BEFORE_ENDED, ["go", "ack-all"]))
                self.assertTrue(record["held"]["held"])
                self.assertIn(record["held"]["released_by"], ("classified", "draining"))
                self.assertIs(record["ok"], True)


class EmulatorShutdownDiagnostics(unittest.TestCase):
    """The dedicated emulator's log: no diagnostic before the run's own `emu kill`; after it, only the
    two declared shutdown warnings, each at most once, exactly as observed."""

    BOOT = "INFO         | Boot completed in 19485 ms\nINFO         | Increasing screen off timeout\n"
    CONFIGURE = "WARNING      | Emulator client has not yet been configured.. Call configure me first!\n"
    NETSIM = "WARNING      | Netsim Wifi localhost:55923 is gone due to CANCELLED\n"
    AFTER = "INFO         | Wait for emulator (pid 90036) 20 seconds to shutdown gracefully before kill\n"

    def judge(self, before, after):
        import run_android_media as m
        return m.emulator_log_failures(before + after, len(before))

    def test_the_observed_shutdown_passes(self):
        self.assertEqual(self.judge(self.BOOT, self.AFTER + self.CONFIGURE + "INFO         | removeAll\n" + self.NETSIM), [])
        self.assertEqual(self.judge(self.BOOT, self.AFTER + self.NETSIM.replace("55923", "61284")), [])
        self.assertEqual(self.judge(self.BOOT, self.AFTER), [])          # a quieter shutdown is not a failure

    def test_every_departure_is_named(self):
        cases = (
            ("a declared warning before the kill", self.BOOT + self.CONFIGURE, self.AFTER,
             ["before the emulator kill: " + self.CONFIGURE.strip()]),
            ("a duplicate", self.BOOT, self.CONFIGURE + self.CONFIGURE,
             ["after the emulator kill, declared more than once: " + self.CONFIGURE.strip()]),
            ("changed text", self.BOOT, self.NETSIM.replace("CANCELLED", "UNAVAILABLE"),
             ["after the emulator kill, undeclared: " + self.NETSIM.replace("CANCELLED", "UNAVAILABLE").strip()]),
            ("another warning", self.BOOT, "WARNING      | Failed to process .ini file\n",
             ["after the emulator kill, undeclared: WARNING      | Failed to process .ini file"]),
            ("an error after the kill", self.BOOT, "ERROR        | Unable to connect to adb daemon on port: 5037\n",
             ["after the emulator kill, undeclared: ERROR        | Unable to connect to adb daemon on port: 5037"]),
            ("a fatal before the kill", self.BOOT + "FATAL        | Missing emulator engine program\n", "",
             ["before the emulator kill: FATAL        | Missing emulator engine program"]),
            ("a warning with a non-numeric port", self.BOOT, self.NETSIM.replace("55923", "port"),
             ["after the emulator kill, undeclared: " + self.NETSIM.replace("55923", "port").strip()]),
        )
        for label, before, after, want in cases:
            with self.subTest(label):
                self.assertEqual(self.judge(before, after), want)


class AdbVersion(unittest.TestCase):
    """The local `adb version` receipt, as observed with the dedicated SDK's adb."""

    ADB = "/Users/jekyll/Library/Android/sdk/platform-tools/adb"
    OBSERVED = ("Android Debug Bridge version 1.0.41\nVersion 37.0.0-14910828\n"
                "Installed as /Users/jekyll/Library/Android/sdk/platform-tools/adb\nRunning on Darwin 24.6.0 (arm64)\n")

    def judge(self, stdout, stderr="", rc=0, adb=ADB):
        import run_android_media as m
        return m.adb_version_receipt({"rc": rc, "stdout": stdout, "stderr": stderr, "timeout": False}, adb)

    def test_the_observed_receipt_names_the_declared_adb(self):
        self.assertEqual(self.judge(self.OBSERVED), ({"bridge": "1.0.41", "version": "37.0.0-14910828"}, []))

    def test_every_departure_is_named(self):
        o = self.OBSERVED
        cases = (
            ("another adb", o, "", 0, "/opt/sdk/platform-tools/adb",
             ["line 3 is 'Installed as /Users/jekyll/Library/Android/sdk/platform-tools/adb', "
              "expected 'Installed as /opt/sdk/platform-tools/adb'"]),
            ("stderr", o, "* daemon not running; starting now at tcp:5037\n", 0, self.ADB,
             ["stderr ['* daemon not running; starting now at tcp:5037'], expected none"]),
            ("rc", o, "", 1, self.ADB, ["rc 1, expected 0"]),
            ("a missing line", o.replace("Running on Darwin 24.6.0 (arm64)\n", ""), "", 0, self.ADB,
             ["3 lines, expected 4"]),
            ("an extra line", o + "warning: unexpected\n", "", 0, self.ADB, ["5 lines, expected 4"]),
            ("a changed version shape", o.replace("37.0.0-14910828", "37.0.0"), "", 0, self.ADB,
             ["line 2 is 'Version 37.0.0', expected 'Version <n>.<n>.<n>-<build>'"]),
            ("a changed heading", o.replace("Android Debug Bridge", "Android Bridge"), "", 0, self.ADB,
             ["line 1 is 'Android Bridge version 1.0.41', expected 'Android Debug Bridge version <n>.<n>.<n>'"]),
        )
        for label, stdout, stderr, rc, adb, want in cases:
            with self.subTest(label):
                self.assertEqual(self.judge(stdout, stderr, rc, adb), (None, want))

    def test_a_timeout_is_named(self):
        import run_android_media as m
        self.assertEqual(m.adb_version_receipt({"rc": None, "stdout": "", "stderr": "", "timeout": True}, self.ADB),
                         (None, ["timed out"]))


class DeviceStart(unittest.TestCase):
    def test_supervised_start_arms_the_observer_and_presets_the_inputs(self):
        import run_android_media as m
        commands = m.Device(m.DEDICATED_SERIAL, lambda *a: None).commands()
        self.assertEqual(commands["start"], ["adb", "-s", m.DEDICATED_SERIAL, "shell", "am", "start", "-W", "-n",
                                             "org.openmoq.moq5.samples.player/.PlayerActivity",
                                             "--ez", "org.openmoq.moq5.samples.player.OBSERVE_PRESENTATION", "true",
                                             "--es", "org.openmoq.moq5.samples.player.RELAY_URL", "<url>",
                                             "--es", "org.openmoq.moq5.samples.player.NAMESPACE", "<namespace>",
                                             "--es", "org.openmoq.moq5.samples.player.OBJECT_BUDGET", "<objects>"])
        self.assertFalse({"keyevent", "text"} & set(commands), "nothing is typed into the player's fields")


class FakeClock:
    """Deterministic time for the control gate: a wait advances to the next
    scheduled fact (applying it) or by the whole timeout, never sleeping."""

    def __init__(self, t=0.0):
        self.t, self.schedule = t, []

    def now(self):
        return self.t

    def at(self, t, fn):
        self.schedule.append((t, fn))
        self.schedule.sort(key=lambda x: x[0])

    def wait(self, cv, timeout):
        if self.schedule and self.schedule[0][0] <= self.t + timeout:
            at, fn = self.schedule.pop(0)
            self.t = max(self.t, at)
            fn()
        else:
            self.t += timeout

    def mark(self, _label):
        pass


GO_FACTS = ("host_active", "player_established", "route_two_subscriptions")


class DedicatedTarget(unittest.TestCase):
    def test_only_the_dedicated_serial_is_accepted(self):
        import run_android_media as m
        calls = []
        runner = lambda *a: calls.append(a)          # noqa: E731
        for bad in ("emulator-5560", "emulator-5582", "emulator-5554", "emulator-5580 ", "emulator-55800", "", "5580"):
            with self.assertRaises(ValueError, msg=bad):
                m.Device(bad, runner)
        self.assertEqual(calls, [], "a refused target must never reach the runner")
        self.assertEqual(m.Device("emulator-5580", runner).argv("x")[:3], ["adb", "-s", "emulator-5580"])


class RelayReadiness(unittest.TestCase):
    def identity(self):
        import run_android_media as m
        return m.RelayIdentity(listener="raw", host="127.0.0.1", port=4443, alpn_set="moqt-18+moqt-16", lanes=1)

    def test_only_the_declared_raw_listener_record_matches(self):
        import run_android_media as m
        good = {"schema": "RELAY_READY_V1", "elapsed_us": 7, "listener": "raw", "host": "127.0.0.1", "port": 4443,
                "alpn_set": "moqt-18+moqt-16", "lanes": 1}
        self.assertEqual(m.relay_ready_record(json.dumps(good), self.identity()), good)
        refused = {
            "signals": {"schema": "RELAY_READY_V1", "elapsed_us": 0, "listener": "signals", "sigusr1": "metrics+routes",
                        "sigusr2": "trace", "sink": "stderr"},
            "webtransport_same_endpoint": dict(good, listener="webtransport", path="/moq", profile="current"),
            "admin_same_endpoint": {"schema": "RELAY_READY_V1", "listener": "admin", "host": "127.0.0.1", "port": 4443},
            "wrong_port": dict(good, port=4444),
            "wrong_host": dict(good, host="0.0.0.0"),
            "wrong_alpn": dict(good, alpn_set="moqt-16"),
            "wrong_lanes": dict(good, lanes=2),
            "port_as_string": dict(good, port="4443"),
            "lanes_as_bool": dict(good, lanes=True),
            "other_schema_echoing_token": {"schema": "RELAY_RUN_CONFIG_V1", "awaits": "RELAY_READY_V1"},
        }
        for name, rec in refused.items():
            self.assertIsNone(m.relay_ready_record(json.dumps(rec), self.identity()), name)
        for name, line in (("not_json", 'diag: "RELAY_READY_V1" listener raw 127.0.0.1 4443'),
                           ("truncated", json.dumps(good)[:-1]), ("array", json.dumps([good])),
                           ("leading_space", " " + json.dumps(good))):
            self.assertIsNone(m.relay_ready_record(line, self.identity()), name)


class PhaseClocks(unittest.TestCase):
    def gate(self, clock):
        import run_android_media as m
        sent = []
        return m.ControlGate(lambda c: sent.append(c) or True, clock=clock), sent

    def test_anchors_are_the_named_publisher_events(self):
        import run_android_media as m
        self.assertEqual(m.ControlGate.ANCHORS, {"go": "publisher_ready", "ack-all": "publisher_wrote_all",
                                                 "ended-observed": "publisher_end_track", "done": "publisher_complete"})

    def test_late_go_is_refused_even_when_every_fact_holds(self):
        import run_android_media as m
        clock = FakeClock(100.0)
        g, sent = self.gate(clock)
        g.fact("publisher_ready")
        for f in GO_FACTS:
            g.fact(f)
        clock.t = 130.0
        with self.assertRaises(m.PhaseFailure) as cm:
            g.send("go", 25.0)
        self.assertIn("go: deadline 25.0s from publisher_ready passed", str(cm.exception))
        self.assertEqual(sent, [])

    def test_go_at_exactly_the_deadline_is_forwarded(self):
        clock = FakeClock(100.0)
        g, sent = self.gate(clock)
        g.fact("publisher_ready")
        for f in GO_FACTS:
            g.fact(f)
        clock.t = 125.0
        g.send("go", 25.0)
        self.assertEqual(sent, ["go"])

    def _after_go(self, clock):
        g, sent = self.gate(clock)
        g.fact("publisher_ready")
        for f in GO_FACTS:
            g.fact(f)
        g.send("go", 25.0)
        return g, sent

    def test_delayed_publisher_phase_gets_its_full_budget_from_its_event(self):
        clock = FakeClock(0.0)
        g, sent = self._after_go(clock)
        clock.at(8.0, lambda: g.fact("publisher_wrote_all"))
        clock.at(15.0, lambda: g.fact("host_all_matched"))
        g.send("ack-all", 10.0)
        self.assertEqual(sent, ["go", "ack-all"])
        b = g.basis["ack-all"]
        self.assertEqual((b["anchor"], b["anchor_t"], b["deadline"], b["sent_t"]), ("publisher_wrote_all", 8.0, 18.0, 15.0))

    def test_barrier_after_the_anchored_deadline_is_named(self):
        import run_android_media as m
        clock = FakeClock(0.0)
        g, sent = self._after_go(clock)
        clock.at(8.0, lambda: g.fact("publisher_wrote_all"))
        clock.at(18.5, lambda: g.fact("host_all_matched"))
        with self.assertRaises(m.PhaseFailure) as cm:
            g.send("ack-all", 10.0)
        self.assertEqual(str(cm.exception), "ack-all: barrier(s) host_all_matched not observed within 10.0s of publisher_wrote_all")
        self.assertEqual(sent, ["go"])

    def test_missing_anchor_is_named_and_bounded_from_the_previous_control(self):
        import run_android_media as m
        clock = FakeClock(0.0)
        g, sent = self._after_go(clock)
        g.fact("host_all_matched")
        with self.assertRaises(m.PhaseFailure) as cm:
            g.send("ack-all", 10.0)
        self.assertEqual(str(cm.exception), "ack-all: anchor publisher_wrote_all not observed within 10.0s of go")
        self.assertEqual(clock.t, 10.0)
        self.assertEqual(sent, ["go"])

    def test_out_of_order_control_refused(self):
        import run_android_media as m
        g, sent = self.gate(FakeClock(0.0))
        with self.assertRaises(m.PhaseFailure):
            g.send("ack-all", 10.0)
        self.assertEqual(sent, [])


LEGS = (("ack-all", "publisher_wrote_all", ("host_all_matched",)),
        ("ended-observed", "publisher_end_track", ("host_ended",)),
        ("done", "publisher_complete", ("host_complete", "player_settled")))


class AnchorWindows(unittest.TestCase):
    """Each post-go anchor must be RECORDED within its budget of the previous
    control's forwarding, even when the leg's barrier is already true."""

    def through(self, clock, upto):
        import run_android_media as m
        sent = []
        g = m.ControlGate(lambda c: sent.append(c) or True, clock=clock)
        g.fact("publisher_ready")
        for f in GO_FACTS:
            g.fact(f)
        g.send("go", 25.0)
        for control, anchor, barrier in LEGS[:upto]:
            g.fact(anchor)
            for b in barrier:
                g.fact(b)
            g.send(control, 10.0)
        return g, sent

    def test_late_anchor_refused_on_every_post_go_leg(self):
        import run_android_media as m
        for i, (control, anchor, barrier) in enumerate(LEGS):
            clock = FakeClock(0.0)
            g, sent = self.through(clock, i)
            prev = g.sent[-1]
            for b in barrier:
                g.fact(b)
            clock.t = g.basis[prev]["sent_t"] + 11.0
            g.fact(anchor)
            with self.assertRaises(m.PhaseFailure, msg=control) as cm:
                g.send(control, 10.0)
            self.assertEqual(str(cm.exception),
                             f"{control}: anchor {anchor} observed 1.000s after its 10.0s window from {prev}")
            self.assertNotIn(control, sent)

    def test_late_anchor_arriving_while_waiting_refused(self):
        import run_android_media as m
        clock = FakeClock(0.0)
        g, sent = self.through(clock, 0)
        g.fact("host_all_matched")
        clock.at(10.5, lambda: g.fact("publisher_wrote_all"))
        with self.assertRaises(m.PhaseFailure) as cm:
            g.send("ack-all", 10.0)
        self.assertEqual(str(cm.exception), "ack-all: anchor publisher_wrote_all not observed within 10.0s of go")
        self.assertEqual(sent, ["go"])

    def test_anchor_at_exactly_the_window_end_accepted(self):
        clock = FakeClock(0.0)
        g, sent = self.through(clock, 0)
        clock.at(10.0, lambda: g.fact("publisher_wrote_all"))
        clock.at(12.0, lambda: g.fact("host_all_matched"))
        g.send("ack-all", 10.0)
        self.assertEqual((sent, g.basis["ack-all"]["deadline"]), (["go", "ack-all"], 20.0))


class StampedWaits(unittest.TestCase):
    """Relay-ready, publisher-ready and shutdown waits judge the time the
    reader OBSERVED the event, not the time the waiter wakes."""

    def event(self, clock):
        import run_android_media as m
        return m.StampedEvent(clock)

    def test_event_already_set_late_is_late(self):
        import run_android_media as m
        clock = FakeClock(11.0)
        ev = self.event(clock)
        ev.set()
        self.assertEqual(ev.t, 11.0)
        self.assertEqual(m.await_stamped(ev, 10.0, clock), "late")

    def test_event_set_on_the_deadline_is_on_time_even_if_the_waiter_wakes_later(self):
        import run_android_media as m
        clock = FakeClock(10.0)
        ev = self.event(clock)
        ev.set()
        clock.t = 14.0
        self.assertEqual(m.await_stamped(ev, 10.0, clock), "on-time")

    def test_event_arriving_while_waiting(self):
        import run_android_media as m
        for at, want in ((9.5, "on-time"), (10.0, "on-time"), (10.5, "absent")):
            clock = FakeClock(0.0)
            ev = self.event(clock)
            clock.at(at, ev.set)
            self.assertEqual(m.await_stamped(ev, 10.0, clock), want, at)

    def test_first_stamp_is_kept(self):
        clock = FakeClock(3.0)
        ev = self.event(clock)
        ev.set()
        clock.t = 20.0
        ev.set()
        self.assertEqual(ev.t, 3.0)

    def test_child_exit_first_is_named(self):
        import run_android_media as m
        clock = FakeClock(0.0)
        self.assertEqual(m.await_stamped(self.event(clock), 10.0, clock, alive=lambda: False), "exited")


def wrote(i, rc=0):
    return {"event": "wrote", "index": i, "rc": rc, "attempts": 1}


class WriteLedgerAnchor(unittest.TestCase):
    def ledger(self):
        import run_android_media as m
        return m.WriteLedger(60)

    def test_valid_stream_anchors_exactly_once_at_its_last_write(self):
        led = self.ledger()
        results = [led.admit(wrote(i), released=True) for i in range(60)]
        self.assertEqual(results, [None] * 59 + ["complete"])
        self.assertIsNone(led.error)

    def test_defective_streams_never_anchor(self):
        cases = {
            "duplicate": [wrote(i) for i in range(59)] + [wrote(58)] + [wrote(59)],
            "skipped": [wrote(i) for i in range(60) if i != 7],
            "non_ok": [wrote(i, rc=-11 if i == 30 else 0) for i in range(60)],
            "missing_index": [wrote(i) for i in range(59)] + [{"event": "wrote", "rc": 0}],
            "index_as_string": [wrote(i) for i in range(59)] + [dict(wrote(59), index="59")],
            "extra_after_last": [wrote(i) for i in range(60)] + [wrote(60)],
        }
        for name, stream in cases.items():
            led = self.ledger()
            got = [led.admit(e, released=True) for e in stream]
            if name == "extra_after_last":
                self.assertEqual(got.count("complete"), 1, name)
            else:
                self.assertNotIn("complete", got, name)
            self.assertIsNotNone(led.error, name)

    def test_write_before_go_is_an_error(self):
        led = self.ledger()
        self.assertEqual(led.admit(wrote(0), released=False), "error")
        self.assertIn("before go", led.error)
        self.assertNotIn("complete", [led.admit(wrote(i), released=True) for i in range(1, 60)])

    def test_poisoned_gate_refuses_the_next_control_even_with_barriers_true(self):
        import run_android_media as m
        clock = FakeClock(0.0)
        sent = []
        g = m.ControlGate(lambda c: sent.append(c) or True, clock=clock)
        g.fact("publisher_ready")
        for f in GO_FACTS:
            g.fact(f)
        g.send("go", 25.0)
        g.fact("host_all_matched")
        g.refuse("publisher write stream: index 58 repeated")
        with self.assertRaises(m.PhaseFailure) as cm:
            g.send("ack-all", 10.0)
        self.assertEqual(str(cm.exception), "ack-all: refused: publisher write stream: index 58 repeated")
        self.assertEqual(sent, ["go"])

    def test_refusal_while_waiting_ends_the_wait_at_once(self):
        import run_android_media as m
        clock = FakeClock(0.0)
        sent = []
        g = m.ControlGate(lambda c: sent.append(c) or True, clock=clock)
        g.fact("publisher_ready")
        for f in GO_FACTS:
            g.fact(f)
        g.send("go", 25.0)
        clock.at(3.0, lambda: g.refuse("publisher write stream: wrote index 30 rc -11"))
        with self.assertRaises(m.PhaseFailure) as cm:
            g.send("ack-all", 10.0)
        self.assertEqual(str(cm.exception), "ack-all: refused: publisher write stream: wrote index 30 rc -11")
        self.assertEqual((clock.t, sent), (3.0, ["go"]))


class HostArgv(unittest.TestCase):
    """The supervisor's host child argv, from the run's declared identity only."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        base = Path(self.tmp.name)
        (base / "installed pkg" / "moq5").mkdir(parents=True)
        (base / "installed pkg" / "moq5" / "__init__.py").write_text("")
        (base / "run dir").mkdir()
        (base / "run dir" / "objects file.bin").write_bytes(b"")
        (base / "ca dir").mkdir()
        (base / "ca dir" / "ca cert.pem").write_text("pem")
        self.base = base

    def identity(self, **change):
        import run_android_media as m
        fields = dict(package=self.base / "installed pkg", url="moqt://127.0.0.1:4443", ca_file=self.base / "ca dir" / "ca cert.pem",
                      sni="localhost", objects=self.base / "run dir" / "objects file.bin", establish_s=10.0, deadline_s=60.0)
        fields.update(change)
        return m.HostIdentity(**fields)

    def test_the_argv_is_exactly_the_declared_identity_with_paths_intact(self):
        import run_android_media as m
        argv = m.host_argv(self.identity(), python="/usr/bin/python3")
        self.assertEqual(argv, ["/usr/bin/python3", "-I", "-B", str(m.HOST_RECEIVER),
                                "--package", str(self.base / "installed pkg"), "--url", "moqt://127.0.0.1:4443",
                                "--ca", str(self.base / "ca dir" / "ca cert.pem"), "--sni", "localhost",
                                "--objects", str(self.base / "run dir" / "objects file.bin"),
                                "--establish-s", "10.0", "--deadline-s", "60.0"])

    def test_wrong_or_missing_inputs_are_refused_by_name(self):
        import run_android_media as m
        for change, needle in ((dict(package=self.base / "nope"), "package"),
                               (dict(ca_file=self.base / "nope.pem"), "ca_file"),
                               (dict(objects=self.base / "nope.bin"), "objects"),
                               (dict(url="https://127.0.0.1:4443/moq"), "url"),
                               (dict(url="moqt://10.0.2.2:4443"), "url"),
                               (dict(sni=""), "sni"),
                               (dict(deadline_s=0), "deadline_s"),
                               (dict(deadline_s=float("inf")), "deadline_s"),
                               (dict(deadline_s=float("nan")), "deadline_s"),
                               (dict(establish_s=float("inf")), "establish_s"),
                               (dict(establish_s=300.0, deadline_s=300.0), "545"),
                               (dict(url="moqt://127.0.0.1:0"), "url"),
                               (dict(url="moqt://127.0.0.1:65536"), "url"),
                               (dict(url="moqt://127.0.0.1:99999"), "url"),
                               (dict(url="moqt://127.0.0.1:4443/x"), "url"),
                               (dict(url="moqt://u@127.0.0.1:4443"), "url")):
            with self.assertRaises(ValueError, msg=needle) as cm:
                m.host_argv(self.identity(**change))
            self.assertIn(needle, str(cm.exception))

    def test_the_real_actor_and_the_offline_stub_declare_different_host_exits(self):
        import run_android_media as m
        self.assertEqual(m.REAL_HOST_EXITS["host-receiver"], ("exited", 0))
        self.assertEqual(m.OFFLINE_EXITS["host-receiver"], ("SIGTERM", 0))


class RefusalPrecedence(unittest.TestCase):
    """Several refusals: for each control the applicable refusal with the earliest control scope
    decides, first-wins within a scope, and no later refusal weakens an earlier one."""

    HOST = "host-receiver exited (rc 0) before host_ended"
    WRITES = "publisher write stream: wrote index 60 after the last object"

    def after_go(self, clock):
        import run_android_media as m
        sent = []
        g = m.ControlGate(lambda c: sent.append(c) or True, clock=clock)
        g.fact("publisher_ready")
        for f in GO_FACTS:
            g.fact(f)
        g.send("go", 25.0)
        return g, sent

    def ack_all_ready(self, g):
        g.fact("publisher_wrote_all")
        g.fact("host_all_matched")

    def test_a_global_write_refusal_after_a_scoped_host_refusal_still_refuses_ack_all(self):
        import run_android_media as m
        g, sent = self.after_go(FakeClock(0.0))
        g.refuse(self.HOST, from_control="ended-observed")
        g.refuse(self.WRITES)
        self.ack_all_ready(g)
        with self.assertRaises(m.PhaseFailure) as cm:
            g.send("ack-all", 10.0)
        self.assertEqual(str(cm.exception), "ack-all: refused: " + self.WRITES)
        self.assertEqual(sent, ["go"], "never forwarded")

    def test_a_global_write_refusal_wakes_a_waiting_ack_all_promptly_past_a_scoped_one(self):
        import run_android_media as m
        clock = FakeClock(0.0)
        g, sent = self.after_go(clock)
        clock.at(1.0, lambda: g.refuse(self.HOST, from_control="ended-observed"))
        clock.at(2.0, lambda: g.refuse(self.WRITES))
        with self.assertRaises(m.PhaseFailure) as cm:
            g.send("ack-all", 10.0)
        self.assertEqual(str(cm.exception), "ack-all: refused: " + self.WRITES)
        self.assertEqual(clock.t, 2.0, "ended when the applicable refusal landed, not at the window's end")
        self.assertEqual(sent, ["go"])

    def test_a_scoped_host_refusal_after_a_global_one_does_not_weaken_it(self):
        import run_android_media as m
        g, sent = self.after_go(FakeClock(0.0))
        g.refuse(self.WRITES)
        g.refuse(self.HOST, from_control="ended-observed")
        self.ack_all_ready(g)
        with self.assertRaises(m.PhaseFailure) as cm:
            g.send("ack-all", 10.0)
        self.assertEqual(str(cm.exception), "ack-all: refused: " + self.WRITES)
        self.assertEqual(sent, ["go"])

    def test_a_scoped_host_refusal_alone_lets_ack_all_through_and_refuses_ended_observed(self):
        import run_android_media as m
        g, sent = self.after_go(FakeClock(0.0))
        g.refuse(self.HOST, from_control="ended-observed")
        self.ack_all_ready(g)
        g.send("ack-all", 10.0)
        self.assertEqual(sent, ["go", "ack-all"])
        g.fact("publisher_end_track")
        g.fact("host_ended")
        with self.assertRaises(m.PhaseFailure) as cm:
            g.send("ended-observed", 10.0)
        self.assertEqual(str(cm.exception), "ended-observed: refused: " + self.HOST)

    def test_precedence_is_earliest_scope_then_first_observed(self):
        g, _ = self.after_go(FakeClock(0.0))
        g.refuse(self.WRITES)
        g.refuse(self.HOST, from_control="ended-observed")
        self.assertEqual(g._refusal("ended-observed"), self.WRITES, "a later, narrower refusal never replaces an earlier, wider one")
        self.assertEqual(g._refusal("done"), self.WRITES)
        g, _ = self.after_go(FakeClock(0.0))
        g.refuse(self.HOST, from_control="ended-observed")
        g.refuse("host-receiver exited (rc 1) before host_ended", from_control="ended-observed")
        self.assertEqual(g._refusal("ended-observed"), self.HOST, "first observed within one scope")
        g.refuse(self.WRITES)
        self.assertEqual(g._refusal("ended-observed"), self.WRITES, "a wider refusal takes the narrower control too")

    def test_every_observed_refusal_is_logged(self):
        g, _sent = self.after_go(FakeClock(0.0))
        g.refuse(self.HOST, from_control="ended-observed")
        g.refuse(self.WRITES)
        logged = [event for _t, event in g.log if event.startswith("refused: ")]
        self.assertEqual(logged, ["refused: " + self.HOST, "refused: " + self.WRITES])


def good_run():
    """A synthetic supervised-run record that satisfies the final verdict."""
    checks = {k: True for k in ("publisher_lifecycle_exact", "publisher_writes_admitted", "publisher_end_track_rc_ok",
                                "publisher_complete_rc_ok", "publisher_shutdown_ok", "controls_forwarded_in_order",
                                "relay_ready_identity", "all_children_settled", "host_stdout_exact", "player_log_exact")}

    def child(name, how):
        return {"name": name, "exit": 0, "how": how, "readers_settled": True, "reader_errors": [], "write_errors": [],
                "stdout": "", "stderr": ""}
    return {"primary_error": None, "checks": checks, "threads_settled": True,
            "children": [child("host-receiver", "SIGTERM"), child("publisher", "exited"), child("relay", "SIGTERM")]}


EXPECTED_EXITS = {"relay": ("SIGTERM", 0), "publisher": ("exited", 0), "host-receiver": ("SIGTERM", 0)}


class FinalVerdict(unittest.TestCase):
    def verdict(self, rec):
        import run_android_media as m
        return m.final_verdict(rec, EXPECTED_EXITS)

    def test_good_run_passes(self):
        self.assertEqual(self.verdict(good_run()), {"ok": True, "failures": []})

    def assertFailsWith(self, rec, needle):
        v = self.verdict(rec)
        self.assertFalse(v["ok"])
        self.assertTrue(any(needle in f for f in v["failures"]), (needle, v["failures"]))

    def test_complete_ok_with_drain_failure_fails_overall(self):
        r = good_run()
        r["checks"]["publisher_shutdown_ok"] = False
        self.assertTrue(r["checks"]["publisher_complete_rc_ok"])
        self.assertFailsWith(r, "check publisher_shutdown_ok")

    def test_same_count_wrong_child_inventory_fails(self):
        r = good_run()
        r["children"][0] = dict(r["children"][2])          # three records, two named relay, none host-receiver
        self.assertFailsWith(r, "child inventory")

    def test_reader_failure_with_every_child_present_fails(self):
        r = good_run()
        r["children"][1]["reader_errors"] = ["publisher: ValueError()"]
        self.assertFailsWith(r, "publisher: reader errors")

    def test_unexpected_exit_distinct_from_expected_sigterm(self):
        r = good_run()
        r["children"][2].update(how="exited", exit=0)       # the relay went away by itself
        self.assertFailsWith(r, "relay: exited rc 0, expected SIGTERM rc 0")
        r = good_run()
        r["children"][1].update(how="SIGTERM_after_timeout", exit=-15)
        self.assertFailsWith(r, "publisher: SIGTERM_after_timeout rc -15, expected exited rc 0")

    def test_probe_thread_left_alive_fails(self):
        r = good_run()
        r["threads_settled"] = False
        self.assertFailsWith(r, "probe threads not settled")

    def test_unplanned_diagnostic_fails(self):
        r = good_run()
        r["children"][0]["stderr"] = "warning: something"
        self.assertFailsWith(r, "unplanned diagnostic")

    def test_empty_or_missing_facts_never_pass(self):
        self.assertFailsWith({}, "check publisher_complete_rc_ok")
        r = good_run()
        del r["checks"]["publisher_writes_admitted"]
        self.assertFailsWith(r, "check publisher_writes_admitted")
        r = good_run()
        r["children"] = []
        self.assertFailsWith(r, "child inventory")
        r = good_run()
        r["primary_error"] = "go: something"
        self.assertFailsWith(r, "primary: go: something")


if __name__ == "__main__":
    unittest.main(verbosity=2)
