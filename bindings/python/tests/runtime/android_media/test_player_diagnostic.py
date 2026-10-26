"""The player's diagnostic log stream: an owned `logcat` child for exactly the prepared player's pid, every
tag and level, whose stdout goes byte-exact to a bounded file in the run directory. It is evidence only:
nothing it prints reaches a judge, so it can never satisfy a receipt fact or `player_settled`, and its
content is not held to the warning rule (logcat's own stderr still is). It is owned and retired like the
receipt sidecar, a failed start is named, and output beyond its bound is counted and reported.

Offline: the real supervisor with the route rig's owned stub children; the diagnostic child is a stub
that replays scripted bytes, and the fixture holds its own handle to every child.
"""
import dataclasses
import json
import signal
import sys
import threading
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import device_player as dp                                   # noqa: E402
import device_lane                                           # noqa: E402
import live_actors                                           # noqa: E402
import run_android_media as m                                # noqa: E402
from test_device_player import FakeDevice, prepared, SERIAL  # noqa: E402
from test_supervisor_interrupt import Owned                  # noqa: E402
from test_supervisor_route import CLOSED, ONE_SUB, PRESENTED_PASS, TWO_SUBS, Run  # noqa: E402

# Replays the bytes of argv[1] on stdout, then runs until the supervisor's SIGTERM.
DIAG_STUB = r'''
import signal, sys, time
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
sys.stdout.buffer.write(open(sys.argv[1], "rb").read())
sys.stdout.buffer.flush()
while True:
    time.sleep(0.02)
'''
# What a real `logcat -v threadtime` shows beside the receipts: time, pid, tid, level, tag, and text,
# a receipt's own text and a warning line included, plus a byte that is not ASCII.
DIAG_BYTES = ("09-28 14:47:31.200  4242  4260 I MoqPlayer: " + PRESENTED_PASS + "\n"
              "09-28 14:47:31.201  4242  4260 I MoqPlayerObserver: " + CLOSED + "\n"
              "09-28 14:47:31.300  4242  4271 W MediaCodec: warning: codec output stalled\n").encode() + b"\xff\n"


class DiagnosticArgv(unittest.TestCase):
    def test_the_stream_is_exactly_the_prepared_process_every_tag_and_level(self):
        fake = FakeDevice()
        p = prepared(fake)
        self.assertEqual(p.diagnostic_argv(), ["adb", "-s", SERIAL, "logcat", f"--pid={p.pid}", "-v", "threadtime", "*:V"])

    def test_no_stream_without_a_prepared_process(self):
        p = dp.DevicePlayer(m.Device(SERIAL, FakeDevice()), "/builds/p.apk", 60, "moqt://10.0.2.2:46443")
        with self.assertRaises(dp.DeviceError):
            p.diagnostic_argv()

    def test_the_live_composition_wires_it_and_expects_its_ending(self):
        from test_live_actors import Fixture
        fx = Fixture(self)
        lane = device_lane.EmulatorLane("/opt/sdk/emulator/emulator", str(fx.adb), fx.root / "emulator.log",
                                        runner=lambda *a, **k: None)
        actors, _n = live_actors.compose(fx.live_run(), lane, "/builds/player.apk")
        self.assertEqual(actors.player_diag_argv, actors.player_start.player.diagnostic_argv)
        self.assertEqual(actors.expected_exits, {**m.LIVE_EXITS, "player-diag": m.ADB_LOGCAT_EXIT})


class DiagnosticStream(unittest.TestCase):
    def supervise(self, receipts=(PRESENTED_PASS, CLOSED), argv=None, bound=None, plan=None, diag=DIAG_BYTES):
        self.owned = Owned(self, plan)
        run = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS], receipts=receipts)
        (run.dir / "diag.py").write_text(DIAG_STUB)
        (run.dir / "diag.bin").write_bytes(diag)
        self.path = run.dir / "player-diagnostic.log"
        fields = dict(player_diag_argv=argv or (lambda: [sys.executable, "-I", run.dir / "diag.py", run.dir / "diag.bin"]),
                      expected_exits={**run.actors.expected_exits, "player-diag": ("SIGTERM", 0)})
        if bound is not None:
            fields["player_diag_bound"] = bound
        run.actors = dataclasses.replace(run.actors, **fields)
        rec = run.supervise()
        self.assertEqual(self.owned.alive(), [], "a child was left running")
        return rec

    def diag_child(self, rec):
        return next(c for c in rec["children"] if c["name"] == "player-diag")

    def test_a_normal_run_keeps_the_exact_bytes_and_its_content_judges_nothing(self):
        rec = self.supervise()
        self.assertEqual(rec["verdict"], {"ok": True, "failures": []}, json.dumps(rec["verdict"], indent=1))
        self.assertEqual(self.path.read_bytes(), DIAG_BYTES)
        c = self.diag_child(rec)
        self.assertEqual((c["how"], c["exit"], c["readers_settled"], c["stdout"]), ("SIGTERM", 0, True, ""))
        self.assertEqual(rec["player_diagnostic"], {"path": str(self.path), "bound": m.PLAYER_DIAG_BOUND,
                                                    "bytes_kept": len(DIAG_BYTES), "bytes_dropped": 0, "complete": True})

    def test_diagnostic_lines_cannot_settle_the_player(self):
        rec = self.supervise(receipts=())                        # the receipt stream says nothing; the diagnostic does
        self.assertFalse(rec["verdict"]["ok"])
        self.assertTrue((rec["primary_error"] or "").startswith("done: barrier(s) player_settled not observed"), rec["primary_error"])
        self.assertNotIn("player_settled", [f[1] for f in rec["facts"]])
        self.assertEqual(self.path.read_bytes(), DIAG_BYTES)

    def test_output_beyond_the_bound_is_counted_and_reported(self):
        rec = self.supervise(bound=64)
        self.assertEqual(self.path.read_bytes(), DIAG_BYTES[:64])
        d = rec["player_diagnostic"]
        self.assertEqual((d["bytes_kept"], d["bytes_dropped"], d["complete"]), (64, len(DIAG_BYTES) - 64, False))

    def test_a_stream_that_cannot_start_is_named_and_leaves_nothing_running(self):
        rec = self.supervise(argv=lambda: ["/nonexistent/adb", "logcat"])
        self.assertIn("spawn_error", rec["player_diagnostic"])
        self.assertFalse(rec["verdict"]["ok"])
        self.assertTrue(any(f.startswith("player diagnostic: spawn failed") for f in rec["verdict"]["failures"]),
                        rec["verdict"]["failures"])
        self.assertNotIn("player-diag", [c["name"] for c in rec["children"]])

    def test_an_interrupted_retirement_still_settles_it(self):
        rec = self.supervise(plan={"player-diag": 1})
        self.assertEqual(rec["interruptions"][0]["where"], "settling player-diag")
        c = self.diag_child(rec)
        self.assertEqual((c["how"], c["exit"], c["readers_settled"]), ("SIGTERM", 0, True))
        self.assertEqual(rec["retained"], [])


if __name__ == "__main__":
    unittest.main()
