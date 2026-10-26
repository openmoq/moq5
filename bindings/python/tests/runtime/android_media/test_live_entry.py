"""The finite live entry, driven through `run_android_media.main(["…", "--live", …])` with every effect
injected: credentials, media, preflight, the emulator lane and supervision are stand-ins that record
into one ordered ledger, so the order of every step and the absence of every forbidden later step are
observed. Nothing here starts an emulator, runs adb, opens a listener or sends a packet.
"""
import contextlib
import hashlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import device_player                                         # noqa: E402
import live_actors                                           # noqa: E402
import live_entry                                            # noqa: E402
import run_android_media                                     # noqa: E402

DIGEST_A = "a" * 64


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class Ledger(list):
    def names(self):
        return [e[0] for e in self]


class FakeLane:
    """The lane's contract as the entry uses it: start/place_ca/remove_ca/restore/stop, `device.adb`,
    `ca` (the placement record the composition's trust gate reads) and `baseline`/`proc` state."""

    def __init__(self, ledger, adb, script, run_ca):
        self.ledger, self.script, self.run_ca = ledger, script, run_ca
        self.device = SimpleNamespace(adb=str(adb))
        self.proc, self.baseline, self.ca = None, None, None

    def _say(self, name):
        self.ledger.append((f"lane.{name}",))
        outcome = self.script.get(name)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def start(self):
        self.proc = "spawned" if self.script.get("start_spawns", True) else None
        out = self._say("start")
        if out is None:
            self.baseline = {"package:android"}
        return out

    def place_ca(self, ca_pem):
        out = self._say("place_ca")
        if out is None:
            import device_lane
            import ssl
            der = hashlib.sha256(ssl.PEM_cert_to_DER_cert(Path(ca_pem).read_text())).hexdigest()
            self.ca = {"placed": True, "ca_der_sha256": der, "ca_created": True, "stage_created": False,
                       "store_created": True,
                       "store_before": device_lane.StoreInventory(existed=False, entries=(), parseable=True)}
        return out

    def remove_ca(self):
        if self.ca is not None:
            self.ca["placed"] = False
        out = self._say("remove_ca")
        if out is None:
            self.ca = None
        return out

    def restore(self):
        return self._say("restore")

    def stop(self):
        out = self._say("stop")
        was = self.proc
        self.proc = None
        return out if out is not None else {"how": "exited" if was else None, "exit": 0 if was else None,
                                            "kill_offset": 0 if was else None, "failures": []}


class Rig:
    """One temporary workspace: real declared input files (absolute, hashed), an absent output
    directory, and injected effects scripted by `script`."""

    def __init__(self, test, script=None):
        tmp = tempfile.TemporaryDirectory()
        test.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()
        self.ledger, self.script = Ledger(), dict(script or {})
        files = {}
        for name in ("relay", "provider.dylib", "publisher", "python3", "adb", "emulator", "player.apk", "ffmpeg", "ffprobe"):
            path = self.root / "inputs" / name
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(f"declared {name}\n".encode())
            os.chmod(path, 0o755)
            files[name] = path
        (self.root / "inputs" / "site" / "moq5").mkdir(parents=True)
        (self.root / "inputs" / "site" / "moq5" / "__init__.py").write_text("")
        self.files, self.out = files, self.root / "runs" / "one"
        self.out.parent.mkdir()
        self.lane = None

    def argv(self, **drop_or_change):
        f = self.files
        args = {"--out": self.out, "--relay": f["relay"], "--relay-sha256": sha256(f["relay"]),
                "--provider": f["provider.dylib"], "--provider-sha256": sha256(f["provider.dylib"]),
                "--publisher": f["publisher"], "--package": self.root / "inputs" / "site", "--python": f["python3"],
                "--adb": f["adb"], "--emulator": f["emulator"], "--apk": f["player.apk"],
                "--apk-sha256": sha256(f["player.apk"]), "--ffmpeg": f["ffmpeg"], "--ffprobe": f["ffprobe"],
                "--port": "46443", "--admin-port": "46444"}
        for k, v in drop_or_change.items():
            key = "--" + k.replace("_", "-")
            if v is None:
                args.pop(key)
            else:
                args[key] = v
        argv = ["run_android_media.py", "--live", "--authorized"]
        for k, v in args.items():
            argv += [k, str(v)]
        return argv

    def deps(self):
        ledger, script = self.ledger, self.script

        def say(name, default=None):
            ledger.append((name,))
            outcome = script.get(name, default)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

        def credentials(run_dir):
            ledger.append(("credentials",))
            if script.get("credentials"):
                return None, script["credentials"]
            return live_actors.fresh_credentials(run_dir)

        def media(run_dir, ffmpeg, ffprobe):
            ledger.append(("media", str(ffmpeg), str(ffprobe)))
            if script.get("media"):
                return {"ok": False, "error": script["media"]}
            (Path(run_dir) / "objects.bin").write_bytes(b"")
            return {"ok": True, "checks": {}}

        def preflight(run):
            say("preflight")
            reason = script.get("preflight_reason")
            return {"ok": reason is None, "checks": [{"name": "relay_provider", "ok": reason is None, "reason": reason}],
                    "open_obligations": []}

        def lane_factory(emulator, adb, log_path):
            ledger.append(("lane.create", str(emulator), str(adb), str(log_path)))
            run_ca = None
            self.lane = FakeLane(ledger, adb, script.get("lane", {}), run_ca)
            return self.lane

        def stacks(actors, out):
            ledger.append(("stacks",))
            outcome = script.get("stacks", {"java": {"rc": 0}, "native": {"rc": 0}})
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

        def final_ui(actors, out):
            ledger.append(("final_ui",))
            outcome = script.get("final_ui", {"path": str(out / "final-ui.xml"), "rc": 0})
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

        def supervise(run_dir, actors, deadlines, n_objects):
            ledger.append(("supervise", str(run_dir), n_objects))
            self.actors = actors
            # the composition's own gates, as the supervisor would meet them: with the CA withdrawn, the
            # composed install refuses before reaching the device; with it placed, this run's CA admits
            placed, self.lane.ca = self.lane.ca, None
            refused = actors.player_install(None, 1)
            self.lane.ca = placed
            ledger.append(("install_gated", refused.startswith("trust prerequisite: ")))
            ledger.append(("trust_gate", live_actors.trust_prerequisite(self.lane, self.run_of(run_dir))))
            ledger.append(("relay_gate", actors.relay_env, actors.relay_evidence is not None))
            outcome = script.get("supervise", {"ok": True, "failures": []})
            if isinstance(outcome, BaseException):
                raise outcome
            if callable(outcome):                                            # the real supervisor, offline
                return outcome(run_dir, actors, deadlines, n_objects)
            return {"verdict": outcome, "children": [], "primary_error": None}

        def audio_media(run_dir, ffmpeg, ffprobe):
            ledger.append(("audio_media", str(ffmpeg), str(ffprobe)))
            if script.get("audio_media"):
                return {"ok": False, "error": script["audio_media"]}
            import test_av_diagnostic
            test_av_diagnostic.write_audio_fixture(Path(run_dir))
            return {"ok": True, "checks": {}}

        def audio_check(run):
            ledger.append(("audio_check",))
            reason = script.get("audio_check")
            return {"argv": ["publisher", "--audio-config-selftest"]}, reason

        return live_entry.Deps(credentials=credentials, media=media, preflight=preflight, lane=lane_factory,
                               supervise=supervise, open_receipt=script.get("open_receipt", live_entry.open_receipt),
                               final_ui=final_ui, stacks=stacks, audio_media=audio_media, audio_check=audio_check)

    def run_of(self, run_dir):
        f = self.files
        return live_actors.LiveRun(run_dir=Path(run_dir), relay_binary=f["relay"], publisher_binary=f["publisher"],
                                   package=self.root / "inputs" / "site", admin_port=46444, port=46443, adb=f["adb"],
                                   relay_sha256=sha256(f["relay"]), provider=f["provider.dylib"],
                                   provider_sha256=sha256(f["provider.dylib"]), python=str(f["python3"]))

    def main(self, argv=None):
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), contextlib.redirect_stdout(io.StringIO()):
            with live_entry.injected(self.deps()):
                rc = run_android_media.main(argv or self.argv())
        self.stderr = stderr.getvalue()
        return rc

    def receipt(self):
        return json.loads((self.out / "live-receipt.json").read_text())


class Admission(unittest.TestCase):
    """Refusals before any effect: nothing created, nothing called."""

    def assert_refused_before_effects(self, rig, rc, needle):
        self.assertEqual(rc, live_entry.EXIT_REFUSED, rig.stderr)
        self.assertIn(needle, rig.stderr)
        self.assertEqual(rig.ledger, [])
        self.assertFalse(rig.out.exists())

    def test_without_authorization_nothing_runs(self):
        rig = Rig(self)
        argv = [a for a in rig.argv() if a != "--authorized"]
        self.assert_refused_before_effects(rig, rig.main(argv), "not released")

    def test_each_missing_or_invalid_input_refuses(self):
        rig = Rig(self)
        cases = [({"relay": None}, "--relay"), ({"apk": None}, "--apk"), ({"ffmpeg": None}, "--ffmpeg"),
                 ({"relay": "relative/moq5-relay"}, "--relay: must be an absolute path"),
                 ({"publisher": rig.root / "absent"}, "--publisher: not a file"),
                 ({"package": rig.root / "inputs"}, "--package: no moq5 package"),
                 ({"relay_sha256": "ABC"}, "--relay-sha256: must be a lowercase hex sha256"),
                 ({"apk_sha256": DIGEST_A}, "--apk-sha256: "),
                 ({"port": "4443"}, "--port"), ({"admin_port": "4443"}, "--admin-port"),
                 ({"admin_port": "46443"}, "--admin-port: must differ from --port"),
                 ({"port": "http"}, "--port"), ({"out": "relative/run"}, "--out: must be an absolute path"),
                 ({"out": rig.root / "no parent" / "run"}, "--out: parent directory")]
        for change, needle in cases:
            with self.subTest(change=change):
                rig.ledger.clear()
                self.assert_refused_before_effects(rig, rig.main(rig.argv(**change)), needle)

    def test_an_unknown_argument_refuses(self):
        rig = Rig(self)
        self.assert_refused_before_effects(rig, rig.main(rig.argv() + ["--serial", "emulator-5554"]),
                                           "unrecognized arguments")

    def test_an_existing_output_is_refused_and_left_unmodified(self):
        rig = Rig(self)
        rig.out.mkdir()
        (rig.out / "earlier.json").write_text("an earlier run's receipt\n")
        before = sorted((p.name, p.read_bytes()) for p in rig.out.iterdir())
        rc = rig.main()
        self.assertEqual(rc, live_entry.EXIT_REFUSED)
        self.assertIn("--out: already exists", rig.stderr)
        self.assertEqual(rig.ledger, [])
        self.assertEqual(sorted((p.name, p.read_bytes()) for p in rig.out.iterdir()), before)


class ExclusiveOutput(unittest.TestCase):
    def test_an_output_that_appears_after_admission_is_refused_and_left_unmodified(self):
        rig = Rig(self)
        inputs = live_entry.parse(rig.argv()[2:])
        rig.out.mkdir()                                                      # appears between the check and the run
        (rig.out / "someone else's").write_text("kept\n")
        rec = live_entry.run(inputs, rig.deps())
        self.assertEqual((rec["outcome"]["first_failure"] or {}).get("step"), "out", rec["outcome"])
        self.assertEqual(rig.ledger, [])
        self.assertEqual([p.name for p in rig.out.iterdir()], ["someone else's"])


class Lifecycle(unittest.TestCase):
    CLEANUP = ["lane.remove_ca", "lane.restore", "lane.stop"]

    def names(self, rig):
        return [n for n in rig.ledger.names() if n not in ("install_gated", "trust_gate", "relay_gate")]

    def test_complete_ordered_success(self):
        rig = Rig(self)
        rc = rig.main()
        self.assertEqual(rc, live_entry.EXIT_OK, rig.stderr)
        self.assertEqual(self.names(rig), ["credentials", "media", "preflight", "lane.create", "lane.start",
                                           "lane.place_ca", "supervise", *self.CLEANUP])
        media = next(e for e in rig.ledger if e[0] == "media")
        self.assertEqual(media[1:], (str(rig.files["ffmpeg"]), str(rig.files["ffprobe"])))
        self.assertIn(("install_gated", True), rig.ledger)
        self.assertIn(("trust_gate", None), rig.ledger)                      # this run's placed CA admits
        self.assertIn(("relay_gate", live_actors.RELAY_LOADER_ENV, True), rig.ledger)
        r = rig.receipt()
        self.assertEqual((r["outcome"]["exit"], r["outcome"]["first_failure"], r["outcome"]["cleanup_failures"]),
                         (0, None, []))
        self.assertEqual(r["media_verdict"], {"ok": True, "failures": []})
        self.assertEqual(r["inputs"]["apk_sha256"], sha256(rig.files["player.apk"]))
        self.assertEqual(oct(os.stat(rig.out / "live-receipt.json").st_mode & 0o777), "0o444")
        self.assertEqual(oct(os.stat(rig.out).st_mode & 0o777), "0o700")

    def assert_failed(self, rig, rc, exit_code, step, forbidden, cleanup=CLEANUP):
        self.assertEqual(rc, exit_code, rig.stderr)
        names = self.names(rig)
        for name in forbidden:
            self.assertNotIn(name, names, f"{name} ran after a refusal: {names}")
        self.assertEqual([n for n in names if n in self.CLEANUP], list(cleanup))
        r = rig.receipt()
        self.assertEqual(r["outcome"]["first_failure"]["step"], step, r["outcome"])
        self.assertEqual(r["outcome"]["exit"], exit_code)
        return r

    def test_preflight_failure_stops_before_any_lane(self):
        rig = Rig(self, {"preflight_reason": "relay provider: 0 libmsquic images loaded"})
        r = self.assert_failed(rig, rig.main(), live_entry.EXIT_SETUP, "preflight",
                               ["lane.create", "lane.start", "supervise"], cleanup=[])
        self.assertIsNone(r["media_verdict"])                             # no media was started: no verdict
        self.assertEqual(r["media"], "not started")

    def test_credential_or_media_failure_stops_before_preflight(self):
        for script, step in (({"credentials": "credentials: openssl rc 1"}, "credentials"),
                             ({"media": "ffmpeg rc 1"}, "media")):
            with self.subTest(step):
                rig = Rig(self, script)
                self.assert_failed(rig, rig.main(), live_entry.EXIT_SETUP, step,
                                   ["preflight", "lane.create", "supervise"], cleanup=[])

    def test_a_partial_start_still_reaches_stop(self):
        rig = Rig(self, {"lane": {"start": "boot not completed within 180s"}})
        r = self.assert_failed(rig, rig.main(), live_entry.EXIT_SETUP, "lane.start",
                               ["lane.place_ca", "supervise"], cleanup=["lane.remove_ca", "lane.stop"])
        self.assertEqual(r["cleanup"][-1]["step"], "lane.stop")

    def test_a_start_that_raises_still_reaches_stop(self):
        rig = Rig(self, {"lane": {"start": OSError("spawn failed")}})
        self.assert_failed(rig, rig.main(), live_entry.EXIT_SETUP, "lane.start", ["lane.place_ca", "supervise"],
                           cleanup=["lane.remove_ca", "lane.stop"])

    def test_a_ca_refusal_stops_before_supervision(self):
        rig = Rig(self, {"lane": {"place_ca": "store already held ['aabbccdd.0']"}})
        self.assert_failed(rig, rig.main(), live_entry.EXIT_SETUP, "lane.place_ca", ["supervise"])

    def test_a_failed_media_verdict_is_not_success_and_cleans_up(self):
        rig = Rig(self, {"supervise": {"ok": False, "failures": ["primary: player start failed: am start: Error"]}})
        r = self.assert_failed(rig, rig.main(), live_entry.EXIT_MEDIA, "media_verdict", [])
        self.assertFalse(r["media_verdict"]["ok"])

    def test_a_retained_supervisor_resource_is_never_success(self):
        retained = [{"name": "relay", "pid": 4242, "why": "settle raised OSError('x'); not proven reaped"}]
        rig = Rig(self, {"supervise": lambda *a: {"verdict": {"ok": True, "failures": []}, "children": [],
                                                  "retained": retained}})
        rc = rig.main()
        self.assertEqual(rc, live_entry.EXIT_CLEANUP, rig.stderr)
        self.assertEqual(rig.receipt()["outcome"]["retained"], retained)

    def test_a_failed_verdict_keeps_the_final_ui_before_any_cleanup(self):
        rig = Rig(self, {"supervise": {"ok": False, "failures": ["primary: done: barrier(s) player_settled not observed"]}})
        rc = rig.main()
        self.assertEqual(rc, live_entry.EXIT_MEDIA)
        names = self.names(rig)
        self.assertEqual(names[names.index("supervise"):], ["supervise", "final_ui", "stacks", *self.CLEANUP])
        self.assertEqual(rig.receipt()["final_ui"], {"path": str(rig.out / "final-ui.xml"), "rc": 0})

    def test_the_observer_off_diagnostic_mode_is_recorded_and_never_passes(self):
        rig = Rig(self)
        rc = rig.main(rig.argv(player_mode="observer-off-diagnostic"))       # the fake verdict is ok
        self.assertEqual(rc, live_entry.EXIT_MEDIA, rig.stderr)
        r = rig.receipt()
        self.assertEqual(r["outcome"]["first_failure"]["step"], "player_mode")
        self.assertIn("OBSERVER-OFF DIAGNOSTIC", r["outcome"]["first_failure"]["reason"])
        self.assertEqual(r["player"]["mode"], "observer-off-diagnostic")
        args = r["player"]["launch_args"]
        self.assertEqual(args[args.index(device_player.OBSERVE_EXTRA) + 1], "false")
        self.assertEqual(args[args.index(device_player.TRACE_EXTRA) + 1], "true")
        self.assertEqual([n for n in self.names(rig) if n in self.CLEANUP], self.CLEANUP)

    def test_the_av_diagnostic_prepares_and_validates_audio_before_any_lane_and_never_passes(self):
        rig = Rig(self)
        rc = rig.main(rig.argv(player_mode="av-diagnostic"))                 # the fake verdict is ok
        self.assertEqual(rc, live_entry.EXIT_MEDIA, rig.stderr)
        self.assertEqual(self.names(rig), ["credentials", "media", "audio_media", "audio_check", "preflight", "lane.create",
                                           "lane.start", "lane.place_ca", "supervise", *self.CLEANUP])
        audio = next(e for e in rig.ledger if e[0] == "audio_media")
        self.assertEqual(audio[1:], (str(rig.files["ffmpeg"]), str(rig.files["ffprobe"])))
        r = rig.receipt()
        self.assertEqual(r["outcome"]["first_failure"]["step"], "player_mode")
        self.assertEqual((r["player"]["mode"], r["audio_input"]["ok"]), ("av-diagnostic", True))
        args = r["player"]["launch_args"]
        self.assertEqual(args[args.index(device_player.OBSERVE_EXTRA) + 1], "false")
        self.assertEqual(args[args.index(device_player.OBJECT_BUDGET_EXTRA) + 1], "178")
        self.assertEqual(rig.actors.audio_objects, 118)
        self.assertIn("--audio", rig.actors.publisher_argv)

    def test_an_audio_fixture_or_publisher_refusal_stops_before_any_lane(self):
        for script, step in (({"audio_media": "ffmpeg warning"}, "audio_media"),
                             ({"audio_check": "audio publisher selftest: rc 2"}, "audio_publisher")):
            with self.subTest(step):
                rig = Rig(self, script)
                self.assert_failed(rig, rig.main(rig.argv(player_mode="av-diagnostic")), live_entry.EXIT_SETUP, step,
                                   ["preflight", "lane.create", "supervise"], cleanup=[])

    def test_video_modes_never_make_or_check_audio(self):
        for mode in ("acceptance", "observer-off-diagnostic"):
            with self.subTest(mode):
                rig = Rig(self)
                rig.main(rig.argv(player_mode=mode))
                self.assertFalse({"audio_media", "audio_check"} & set(self.names(rig)))

    def test_the_default_mode_is_acceptance_and_an_unknown_mode_refuses(self):
        rig = Rig(self)
        self.assertEqual(rig.main(), live_entry.EXIT_OK)
        self.assertEqual(rig.receipt()["player"]["mode"], "acceptance")
        rig2 = Rig(self)
        self.assertEqual(rig2.main(rig2.argv(player_mode="something")), live_entry.EXIT_REFUSED)
        self.assertEqual(rig2.ledger, [])

    def test_a_passing_verdict_takes_no_final_ui(self):
        rig = Rig(self)
        self.assertEqual(rig.main(), live_entry.EXIT_OK)
        self.assertNotIn("final_ui", rig.ledger.names())
        self.assertNotIn("stacks", rig.ledger.names())
        self.assertIsNone(rig.receipt()["final_ui"])
        self.assertIsNone(rig.receipt()["stacks"])

    def test_a_final_ui_capture_that_fails_is_recorded_and_skips_no_cleanup(self):
        rig = Rig(self, {"supervise": {"ok": False, "failures": ["x"]}, "final_ui": OSError("adb offline")})
        self.assertEqual(rig.main(), live_entry.EXIT_MEDIA)
        self.assertEqual([n for n in self.names(rig) if n in self.CLEANUP], self.CLEANUP)
        self.assertEqual(rig.receipt()["final_ui"], {"error": "OSError('adb offline')"})

    def test_a_stack_capture_that_fails_is_recorded_and_skips_no_cleanup(self):
        rig = Rig(self, {"supervise": {"ok": False, "failures": ["x"]}, "stacks": RuntimeError("debuggerd")})
        self.assertEqual(rig.main(), live_entry.EXIT_MEDIA)
        self.assertEqual([n for n in self.names(rig) if n in self.CLEANUP], self.CLEANUP)
        self.assertEqual(rig.receipt()["stacks"], {"error": "RuntimeError('debuggerd')"})

    def test_supervision_that_raises_is_named_and_cleans_up(self):
        rig = Rig(self, {"supervise": RuntimeError("supervisor fault")})
        r = self.assert_failed(rig, rig.main(), live_entry.EXIT_SETUP, "supervise", [])
        self.assertIn("supervisor fault", r["outcome"]["first_failure"]["reason"])

    def test_cleanup_failures_never_exit_zero_and_do_not_skip_later_steps(self):
        for label, lane, failing in (
                ("CA removal refused", {"remove_ca": "CA removal incomplete: su 0 id -u: rc 1"}, "lane.remove_ca"),
                ("restore raises", {"restore": RuntimeError("adb offline")}, "lane.restore"),
                ("stop names a failure", {"stop": {"how": "SIGKILL", "exit": -9, "kill_offset": 1,
                                                   "failures": ["the emulator did not exit within 60s of emu kill"]}},
                 "lane.stop")):
            with self.subTest(label):
                rig = Rig(self, {"lane": lane})
                rc = rig.main()
                self.assertEqual(rc, live_entry.EXIT_CLEANUP, rig.stderr)
                self.assertEqual([n for n in self.names(rig) if n in self.CLEANUP], self.CLEANUP)
                r = rig.receipt()
                self.assertIsNone(r["outcome"]["first_failure"])
                self.assertEqual(r["media_verdict"], {"ok": True, "failures": []})   # media passed; still not success
                self.assertEqual([c["step"] for c in r["outcome"]["cleanup_failures"]], [failing])

    def test_an_interruption_is_cleaned_up_and_named(self):
        rig = Rig(self, {"supervise": KeyboardInterrupt()})
        r = self.assert_failed(rig, rig.main(), live_entry.EXIT_INTERRUPTED, "supervise", [])
        self.assertIn("interrupted", r["outcome"]["first_failure"]["reason"])

    def test_an_interruption_during_cleanup_does_not_skip_later_steps(self):
        rig = Rig(self, {"lane": {"remove_ca": KeyboardInterrupt()}})
        try:
            rc = rig.main()
        except KeyboardInterrupt:
            self.fail("an interruption in cleanup escaped the entry before its later steps")
        self.assertEqual(rc, live_entry.EXIT_CLEANUP)
        self.assertEqual([n for n in self.names(rig) if n in self.CLEANUP], self.CLEANUP)

    def test_a_receipt_that_cannot_be_written_is_a_failure(self):
        def refuse(path):
            raise OSError("disk full")
        rig = Rig(self, {"open_receipt": refuse})
        rc = rig.main()
        self.assertEqual(rc, live_entry.EXIT_RECEIPT)
        self.assertIn("receipt: OSError('disk full')", rig.stderr)
        self.assertEqual([n for n in self.names(rig) if n in self.CLEANUP], self.CLEANUP)

    def test_a_partially_written_receipt_is_not_left_as_one(self):
        class Broken(io.StringIO):
            def write(self, s):
                raise OSError("short write")

        def half(path):
            Path(path).write_text("{")                                  # created, then the write fails
            return Broken()
        rig = Rig(self, {"open_receipt": half})
        rc = rig.main()
        self.assertEqual(rc, live_entry.EXIT_RECEIPT)
        self.assertFalse((rig.out / "live-receipt.json").exists())
        self.assertTrue((rig.out / "live-receipt.json.incomplete").exists())   # kept for attribution, never as a receipt


class RealSupervisorInterruption(unittest.TestCase):
    """The entry with the REAL supervisor: the route rig's owned offline stub children stand in for the
    composed actors, and only the device lane is faked. An interruption must reach the receipt as the
    run's interruption, with every supervisor-owned child settled and the lane cleaned up after."""

    def real_supervisor(self, plan=None, hooks=None):
        import run_android_media as m
        from test_supervisor_interrupt import Owned
        from test_supervisor_route import FAST, ONE_SUB, TWO_SUBS, Run
        self.owned = Owned(self, plan)

        def supervise(run_dir, actors, deadlines, n_objects):
            run = Run(self, [ONE_SUB, ONE_SUB, TWO_SUBS])
            run.actors.hooks.update(hooks or {})
            return m.supervise(run.dir, run.actors, FAST, 60)
        return supervise

    def assert_interrupted(self, rig, rc, where):
        self.assertEqual(rc, live_entry.EXIT_INTERRUPTED, rig.stderr)
        names = rig.ledger.names()
        self.assertEqual(names[names.index("supervise") + 1:][-3:], ["lane.remove_ca", "lane.restore", "lane.stop"])
        r = rig.receipt()
        first = r["outcome"]["first_failure"]
        self.assertEqual(first["step"], "supervise")
        self.assertTrue(first["reason"].startswith(f"interrupted during {where}"), first["reason"])
        children = r["supervision"]["children"]
        self.assertEqual(sorted(c["name"] for c in children), ["host-receiver", "player-log", "publisher", "relay"])
        self.assertTrue(all(c.get("exit") is not None and c.get("readers_settled") is True for c in children), children)
        self.assertEqual(r["supervision"]["retained"], [])
        self.assertFalse(r["media_verdict"]["ok"])
        self.assertEqual(self.owned.alive(), [])

    def test_an_interruption_while_settling_a_child_is_the_runs_interruption(self):
        rig = Rig(self, {"supervise": None})
        rig.script["supervise"] = self.real_supervisor(plan={"host-receiver": 1})
        self.assert_interrupted(rig, rig.main(), "settling host-receiver")
        self.assertEqual(self.owned.settle_calls[:2], ["host-receiver", "host-receiver"])

    def test_a_real_sigterm_during_supervision_is_converted_and_the_callers_handler_restored(self):
        import signal
        before = signal.getsignal(signal.SIGTERM)
        sent = []

        def terminate(control):
            if control == "ack-all" and not sent:
                sent.append(control)
                os.kill(os.getpid(), signal.SIGTERM)                          # delivered on this, the supervisor's thread
        rig = Rig(self, {"supervise": None})
        rig.script["supervise"] = self.real_supervisor(hooks={"forwarded": terminate})
        self.assert_interrupted(rig, rig.main(), "supervision")
        self.assertIn("SIGTERM", rig.receipt()["outcome"]["first_failure"]["reason"])
        self.assertIs(signal.getsignal(signal.SIGTERM), before)


class StackCapture(unittest.TestCase):
    """The stack snapshot's own boundaries: the exact prepared player, revalidated immediately before
    each process-directed command; refusals run nothing further; outputs kept exclusively."""
    PID = 4242

    def capture(self, answers):
        import device_player
        import run_android_media as m
        calls = []

        def runner(name, argv, timeout):
            calls.append((name, argv[3:], timeout))
            got = answers(name, argv)
            return {"rc": 0, "stdout": "", "stderr": "", "timeout": False, **(got or {})}
        player = device_player.DevicePlayer(m.Device(m.DEDICATED_SERIAL, runner), "/b/p.apk", 60, "moqt://10.0.2.2:46443")
        player.pid = self.PID
        actors = SimpleNamespace(player_start=SimpleNamespace(player=player))
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            rec = live_entry.capture_stacks(actors, out)
            files = {p.name: p.read_text() for p in out.iterdir()}
        return rec, calls, files

    def owned(self, name, argv):
        if name.endswith("pidof"):
            return {"stdout": f"{self.PID}\n"}
        if name.endswith("cmdline"):
            return {"stdout": "org.openmoq.moq5.samples.player\x00"}
        return {"stdout": f"stacks of {argv[-1]} via {' '.join(argv[3:7])}\n"}

    def test_both_stacks_of_exactly_the_prepared_player_through_su(self):
        rec, calls, files = self.capture(self.owned)
        self.assertEqual([c[1] for c in calls], [
            ["shell", "pidof", "org.openmoq.moq5.samples.player"], ["shell", "cat", f"/proc/{self.PID}/cmdline"],
            ["shell", "su", "0", "debuggerd", "-j", str(self.PID)],
            ["shell", "pidof", "org.openmoq.moq5.samples.player"], ["shell", "cat", f"/proc/{self.PID}/cmdline"],
            ["shell", "su", "0", "debuggerd", str(self.PID)]])
        self.assertTrue(all(c[2] == live_entry.STACK_S for c in calls if "debuggerd" in c[1]))
        self.assertEqual(sorted(files), ["stacks-java.txt", "stacks-native.txt"])
        self.assertEqual((rec["java"]["rc"], rec["native"]["rc"]), (0, 0))

    def test_another_process_or_package_is_refused_before_any_debuggerd(self):
        for label, bad in (("another pid", {"pidof": {"stdout": "4243\n"}}), ("two pids", {"pidof": {"stdout": "4242 4243\n"}}),
                           ("another package", {"cmdline": {"stdout": "com.android.systemui\x00"}})):
            with self.subTest(label):
                def answers(name, argv, bad=bad):
                    for suffix, got in bad.items():
                        if name.endswith(suffix):
                            return got
                    return self.owned(name, argv)
                rec, calls, files = self.capture(answers)
                self.assertFalse(any("debuggerd" in c[1] for c in calls), calls)
                self.assertIn("refused", rec["java"])
                self.assertEqual(files, {})

    def test_a_process_gone_after_the_java_stacks_is_not_targeted_again(self):
        seen = []

        def answers(name, argv):
            if name.endswith("pidof"):
                seen.append(name)
                return {"stdout": f"{self.PID}\n"} if len(seen) == 1 else {"rc": 1, "stdout": ""}
            return self.owned(name, argv)
        rec, calls, files = self.capture(answers)
        self.assertEqual(sum("debuggerd" in c[1] for c in calls), 1)
        self.assertIn("refused", rec["native"])
        self.assertEqual(sorted(files), ["stacks-java.txt"])


class DeclaredMediaTools(unittest.TestCase):
    def test_the_media_is_made_by_the_declared_tools_not_by_path(self):
        import make_input
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            log = root / "ran"
            tool = root / "declared ffmpeg"
            tool.write_text(f"#!/bin/sh\necho \"$0\" >> '{log}'\nexit 7\n")
            os.chmod(tool, 0o755)
            receipt = make_input.generate(root / "run", ffmpeg=tool, ffprobe=tool)
            self.assertEqual((receipt["ffmpeg"], receipt["ffprobe"]), (str(tool), str(tool)))
            self.assertIs(receipt["ok"], False)                               # the stand-in refuses to encode
            self.assertEqual(log.read_text().splitlines(), [str(tool), str(tool)])   # -version, then generation a


if __name__ == "__main__":
    unittest.main()
