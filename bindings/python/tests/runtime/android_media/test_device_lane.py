"""The dedicated emulator's lifecycle, offline: a scripted adb runner and a fake emulator process whose
log is a real file. Nothing here starts an emulator or runs adb."""
import json
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import device_lane as lane                                   # noqa: E402
import device_player as dp                                   # noqa: E402
import run_android_media as m                                # noqa: E402

ADB = "/opt/sdk/platform-tools/adb"
EMULATOR = "/opt/sdk/emulator/emulator"
VERSION = ("Android Debug Bridge version 1.0.41\nVersion 37.0.0-14910828\n"
           f"Installed as {ADB}\nRunning on Darwin 24.6.0 (arm64)\n")
BOOT_LOG = "INFO         | Boot completed in 19485 ms\n"
KILL_LOG = ("INFO         | Wait for emulator (pid 90036) 20 seconds to shutdown gracefully before kill\n"
            "WARNING      | Emulator client has not yet been configured.. Call configure me first!\n"
            "INFO         | removeAll\n"
            "WARNING      | Netsim Wifi localhost:55923 is gone due to CANCELLED\n")
PACKAGES = "package:com.android.settings\npackage:com.android.shell\n"


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def wait(self, s):
        self.t += s


class FakeEmulator:
    """The spawned emulator: writes its boot log at once; `emu kill` makes it write its shutdown log and
    exit, unless `exits_on_kill` is False."""

    def __init__(self, log, boot_log=BOOT_LOG, kill_log=KILL_LOG, exits_on_kill=True, exit_rc=0):
        self.log, self.kill_log, self.exits_on_kill, self.exit_rc = log, kill_log, exits_on_kill, exit_rc
        self.returncode, self.killed, self.pid = None, False, 90036
        log.write(boot_log)
        log.flush()

    def on_emu_kill(self):
        self.log.write(self.kill_log)
        self.log.flush()
        if self.exits_on_kill:
            self.returncode = self.exit_rc

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        if self.returncode is None:
            import subprocess
            raise subprocess.TimeoutExpired("emulator", timeout)
        return self.returncode

    def kill(self):
        self.killed, self.returncode = True, -9


STORE = "/data/misc/user/0/cacerts-added"
STAGE = "/data/local/tmp/moq5-test-ca.pem"
LABEL = "u:object_r:misc_user_data_file:s0"
PREFIX = [ADB, "-s", "emulator-5580"]


class FakeDeviceFS:
    """The device side of the CA commands, interpreted from each call's argv alone. Privilege is the
    argv's own `su 0` prefix, per command: `shell su 0 <argv>` runs as uid 0, a plain `shell <argv>` as
    the shell uid 2000. /data/misc needs uid 0. `su` is "ok", "missing", "denied", or "uid:<text>" (the
    stdout of `su 0 id -u`). `ls -lZ` prints the shape observed on the dedicated emulator."""
    NAMES = {"su_uid", "ca_store", "stage_check", "push", "mkdir", "chown_store", "cp", "chown_ca", "chmod_ca",
             "ca_read", "ca_stat", "rm_ca", "rm_stage", "rmdir_store", "ca_gone", "store_gone"}

    def __init__(self):
        self.dirs, self.files, self.su, self.log = set(), {}, "ok", []

    def ok(self, out=""):
        return {"rc": 0, "stdout": out, "stderr": "", "timeout": False}

    def err(self, msg, rc=1):
        return {"rc": rc, "stdout": "", "stderr": msg + "\n", "timeout": False}

    def children(self, d):
        return sorted(q.rsplit("/", 1)[1] for q in self.files if q.rsplit("/", 1)[0] == d)

    def run(self, argv):
        assert argv[:3] == PREFIX, argv                       # every device call names the dedicated serial
        self.log.append(list(argv))
        verb, rest = argv[3], argv[4:]
        if verb == "push":
            self.files[rest[1]] = [Path(rest[0]).read_bytes(), "shell", "666"]
            return self.ok(f"{rest[0]}: 1 file pushed, 0 skipped.\n")
        assert verb in ("shell", "exec-out"), argv
        uid = 2000
        if rest[:2] == ["su", "0"]:
            if self.su == "missing":
                return self.err("/system/bin/sh: su: inaccessible or not found", 127)
            if self.su == "denied":
                return self.err("su: permission denied")
            uid, rest = 0, rest[2:]
        if rest == ["id", "-u"]:
            return self.ok((self.su[4:] if self.su.startswith("uid:") and uid == 0 else str(uid)) + "\n")
        path = rest[-1]
        if path.startswith("/data/misc") and uid != 0:
            return self.err(f"{rest[0]}: {path}: Permission denied")
        if rest[:1] == ["ls"]:
            if rest[1:2] == ["-lZ"]:
                data, owner, mode = self.files[path]
                perms = {"644": "-rw-r--r--", "666": "-rw-rw-rw-"}[mode]
                return self.ok(f"{perms} 1 {owner} {owner} {LABEL}  {len(data)} 2026-09-25 12:22 {path}\n")
            if path in self.dirs:
                return self.ok("".join(f"{n}\n" for n in ([".", ".."] if "-1a" in rest else []) + self.children(path)))
            if path in self.files:
                return self.ok(path + "\n")
            return self.err(f"ls: {path}: No such file or directory")
        if rest[0] == "mkdir":
            self.dirs.add(path)
            return self.ok()
        if rest[0] == "chown":
            if path in self.files:
                self.files[path][1] = rest[1].split(":")[0]           # owner:group, the same name here
            return self.ok()
        if rest[0] == "chmod":
            self.files[path][2] = rest[1]
            return self.ok()
        if rest[0] == "cp":
            self.files[path] = list(self.files[rest[1]])
            return self.ok()
        if rest[0] == "cat":
            return self.ok(self.files[path][0].decode())
        if rest[:2] == ["rm", "-f"]:
            self.files.pop(path, None)
            return self.ok()
        if rest[0] == "rmdir":
            if self.children(path):
                return self.err(f"rmdir: {path}: Directory not empty")
            self.dirs.discard(path)
            return self.ok()
        raise AssertionError(argv)


class Rig:
    def __init__(self, test, **emu):
        tmp = tempfile.TemporaryDirectory()
        test.addCleanup(tmp.cleanup)
        self.dir, self.clock, self.calls, self.emu_kw = Path(tmp.name), Clock(), [], emu
        self.out = {"adb_version": VERSION, "devices": "List of devices attached\nemulator-5554\tdevice\n\n",
                    "avd_name": "moq5-runtime-2392\nOK\n", "sdk": "36\n", "abi": "arm64-v8a\n",
                    "packages": PACKAGES, "stop_emulator": "OK: killing emulator, bye bye\nOK\n", "uninstall": "Success\n",
                    "package": ""}
        self.rc, self.raises, self.boots_after, self.emulator = {}, {}, 2, None
        self.device = FakeDeviceFS()
        self.effect_then_fail, self.noop, self.timeouts, self.fail_nth, self.seen = set(), set(), set(), {}, {}
        self.out_from = {}                       # name -> (n, stdout): that stdout from the n-th call on
        self.result = {}                         # name -> a whole result returned instead, with no effect
        self.lane = lane.EmulatorLane(EMULATOR, ADB, self.dir / "emulator.log", spawn=self.spawn, runner=self.run,
                                      clock=self.clock, wait=self.clock.wait)
        test.addCleanup(self.lane.stop)                   # owed after any spawn; nothing once stopped

    def spawn(self, argv, stdout, stderr):
        self.calls.append(("spawn", list(argv)))
        self.emulator = FakeEmulator(stdout, **self.emu_kw)
        return self.emulator

    def run(self, name, argv, timeout):
        self.calls.append((name, list(argv)))
        if name in self.raises:
            raise self.raises[name]
        self.seen[name] = self.seen.get(name, 0) + 1
        if name in self.timeouts:
            return {"rc": None, "stdout": "", "stderr": "", "timeout": True}
        if self.fail_nth.get(name) == self.seen[name]:          # only that call fails, inconclusively
            return {"rc": 1, "stdout": "", "stderr": f"{name}: I/O error\n", "timeout": False}
        if name in self.noop:                                    # reports success, changes nothing
            return {"rc": 0, "stdout": "", "stderr": "", "timeout": False}
        if name == "boot_completed":
            self.boots_after -= 1
            return {"rc": 0, "stdout": "1\n" if self.boots_after <= 0 else "\n", "stderr": "", "timeout": False}
        if name == "stop_emulator" and self.emulator is not None:
            self.emulator.on_emu_kill()
        if name == "ca_hash":
            return {"rc": 0, "stdout": self.out.get("ca_hash", "d56d3822\n"), "stderr": "", "timeout": False}
        if name in FakeDeviceFS.NAMES:
            if name in self.result:                              # a whole result, with no effect
                return dict(self.result[name])
            if name in self.rc:                                  # a failed command takes no effect
                return {"rc": self.rc[name], "stdout": "", "stderr": f"{name}: fault\n", "timeout": False}
            r = self.device.run(argv)
            if name in self.out_from and self.seen[name] >= self.out_from[name][0]:
                r = dict(r, stdout=self.out_from[name][1])
            if name in self.effect_then_fail:                    # ... unless it took effect, then failed
                return {"rc": 1, "stdout": "", "stderr": f"{name}: fault after effect\n", "timeout": False}
            if name in self.out:
                r = dict(r, stdout=self.out[name])
            return r
        return {"rc": self.rc.get(name, 0), "stdout": self.out.get(name, ""), "stderr": "", "timeout": False}

    def names(self):
        return [c[0] for c in self.calls]


class Start(unittest.TestCase):
    def test_the_exact_sequence(self):
        rig = Rig(self)
        self.assertIsNone(rig.lane.start())
        self.assertEqual(rig.names(), ["adb_version", "devices", "spawn", "boot_completed", "boot_completed",
                                       "avd_name", "sdk", "abi", "packages"])
        argv = dict(rig.calls)
        self.assertEqual(argv["adb_version"], [ADB, "version"])
        self.assertEqual(argv["devices"], [ADB, "devices"])
        self.assertEqual(argv["spawn"], [EMULATOR, "-avd", "moq5-runtime-2392", "-port", "5580", "-no-window", "-no-audio",
                                         "-no-boot-anim", "-no-snapshot", "-gpu", "swiftshader_indirect", "-memory", "2048"])
        self.assertEqual(argv["avd_name"], [ADB, "-s", "emulator-5580", "emu", "avd", "name"])
        self.assertEqual(rig.lane.baseline, {"package:com.android.settings", "package:com.android.shell"})

    def test_refusals_before_the_emulator_is_spawned(self):
        for label, name, value, reason in (
                ("another adb", "adb_version", VERSION.replace(ADB, "/usr/local/bin/adb"),
                 f"adb version: line 3 is 'Installed as /usr/local/bin/adb', expected 'Installed as {ADB}'"),
                ("the serial already present", "devices", "List of devices attached\nemulator-5580\tdevice\n\n",
                 "emulator-5580 is already attached; this run owns only an emulator it starts")):
            with self.subTest(label):
                rig = Rig(self)
                rig.out[name] = value
                self.assertEqual(rig.lane.start(), reason)
                self.assertNotIn("spawn", rig.names())

    def test_boot_is_bounded(self):
        rig = Rig(self)
        rig.boots_after = 10 ** 6
        self.assertEqual(rig.lane.start(), f"boot not completed within {lane.BOOT_S:g}s")
        self.assertLessEqual(rig.clock.t, lane.BOOT_S + lane.POLL_S)

    def test_an_emulator_that_exits_during_boot_is_named(self):
        rig = Rig(self, exit_rc=1)
        rig.boots_after = 10 ** 6
        original = rig.spawn

        def spawn(argv, stdout, stderr):
            proc = original(argv, stdout, stderr)
            proc.returncode = 1
            return proc
        rig.lane.spawn = spawn
        self.assertEqual(rig.lane.start(), "the emulator exited during boot: rc 1")

    def test_identity_departures_are_named(self):
        for name, value, reason in (
                ("avd_name", "moq5_api36_arm64\nOK\n", "avd is 'moq5_api36_arm64', expected 'moq5-runtime-2392'"),
                ("sdk", "35\n", "sdk is '35', expected '36'"),
                ("abi", "x86_64\n", "abi is 'x86_64', expected 'arm64-v8a'")):
            with self.subTest(name):
                rig = Rig(self)
                rig.out[name] = value
                self.assertEqual(rig.lane.start(), f"identity: {reason}")
                self.assertNotIn("packages", rig.names())


class Ownership(unittest.TestCase):
    """One lane owns at most one emulator, and never loses it."""

    def test_a_second_start_is_refused_and_the_first_emulator_stays_owned(self):
        for label, boots_after in (("after a completed start", 2), ("while the first is still booting", 10 ** 6)):
            with self.subTest(label):
                rig = Rig(self)
                rig.boots_after = boots_after
                rig.lane.start()
                first, log = rig.lane.proc, rig.lane.log
                before = len(rig.calls)
                self.assertEqual(rig.lane.start(), "this lane already owns a started emulator; stop() it first")
                self.assertEqual(rig.calls[before:], [])                        # no adb, no spawn
                self.assertIs(rig.lane.proc, first)
                self.assertIs(rig.lane.log, log)
                receipt = rig.lane.stop()
                self.assertEqual((receipt["how"], receipt["exit"]), ("exited", 0))
                self.assertTrue(log.closed)
                self.assertEqual(rig.names().count("spawn"), 1)

    def test_restore_without_a_baseline_runs_nothing(self):
        rig = Rig(self)
        self.assertEqual(rig.lane.restore(), "no started emulator to restore")
        self.assertEqual(rig.calls, [])
        rig.out["sdk"] = "35\n"
        self.assertEqual(rig.lane.start(), "identity: sdk is '35', expected '36'")
        before = len(rig.calls)
        self.assertEqual(rig.lane.restore(), "no package baseline: start() did not complete")
        self.assertEqual(rig.calls[before:], [])

    def test_restore_after_stop_or_a_failed_restart_runs_nothing(self):
        rig = Rig(self)
        assert rig.lane.start() is None
        rig.lane.stop()
        before = len(rig.calls)
        self.assertEqual(rig.lane.restore(), "no started emulator to restore")
        self.assertEqual(rig.calls[before:], [])
        rig.out["abi"] = "x86_64\n"                                       # the restart fails its identity
        self.assertEqual(rig.lane.start(), "identity: abi is 'x86_64', expected 'arm64-v8a'")
        before = len(rig.calls)
        self.assertEqual(rig.lane.restore(), "no package baseline: start() did not complete")   # never the first run's
        self.assertEqual(rig.calls[before:], [])

    def test_a_kill_command_that_raises_still_retires_the_emulator_and_its_log(self):
        rig = Rig(self)
        assert rig.lane.start() is None
        log = rig.lane.log
        rig.raises["stop_emulator"] = OSError("adb: No such file or directory")
        receipt = rig.lane.stop()
        self.assertIn("emu kill: raised OSError('adb: No such file or directory')", receipt["failures"])
        self.assertEqual((receipt["how"], receipt["exit"]), ("SIGKILL", -9))
        self.assertTrue(rig.emulator.killed)
        self.assertTrue(log.closed)
        self.assertIsNone(rig.lane.proc)
        self.assertEqual(rig.lane.stop(), {"how": None, "exit": None, "kill_offset": None, "failures": []})


class Stop(unittest.TestCase):
    def started(self, **emu):
        rig = Rig(self, **emu)
        assert rig.lane.start() is None
        return rig

    def test_a_clean_shutdown_judges_the_log_from_the_owned_kill(self):
        rig = self.started()
        receipt = rig.lane.stop()
        self.assertEqual(receipt["failures"], [])
        self.assertEqual((receipt["how"], receipt["exit"]), ("exited", 0))
        self.assertEqual(receipt["kill_offset"], len(BOOT_LOG))           # captured on the judged text, at the kill
        self.assertEqual(rig.names()[-1], "stop_emulator")

    def test_a_diagnostic_before_the_kill_fails(self):
        rig = self.started(boot_log=BOOT_LOG + "WARNING      | Emulator client has not yet been configured.. Call configure me first!\n")
        self.assertEqual(rig.lane.stop()["failures"],
                         ["before the emulator kill: WARNING      | Emulator client has not yet been configured.. Call configure me first!"])

    def test_an_undeclared_line_after_the_kill_fails(self):
        rig = self.started(kill_log=KILL_LOG + "ERROR        | qemu: unexpected exit\n")
        self.assertEqual(rig.lane.stop()["failures"], ["after the emulator kill, undeclared: ERROR        | qemu: unexpected exit"])

    def test_an_emulator_that_does_not_exit_is_killed_and_named(self):
        rig = self.started(exits_on_kill=False)
        receipt = rig.lane.stop()
        self.assertTrue(rig.emulator.killed)
        self.assertEqual((receipt["how"], receipt["exit"]), ("SIGKILL", -9))
        self.assertIn(f"the emulator did not exit within {lane.SHUTDOWN_S:g}s of emu kill", receipt["failures"])

    def test_a_failed_kill_command_is_named(self):
        rig = self.started()
        rig.rc["stop_emulator"] = 1
        rig.emulator.exits_on_kill = False
        receipt = rig.lane.stop()
        self.assertIn("emu kill: rc 1", receipt["failures"])
        self.assertTrue(rig.emulator.killed)

    def test_a_second_stop_runs_nothing(self):
        rig = self.started()
        rig.lane.stop()
        before = len(rig.calls)
        self.assertEqual(rig.lane.stop(), {"how": None, "exit": None, "kill_offset": None, "failures": []})
        self.assertEqual(len(rig.calls), before)

    def test_a_spawn_that_raises_leaves_no_open_log(self):
        rig = Rig(self)

        def spawn(argv, stdout, stderr):
            raise OSError("emulator: not found")
        rig.lane.spawn = spawn
        with self.assertRaises(OSError):
            rig.lane.start()
        self.assertIsNone(rig.lane.log)

    def test_stop_without_a_start_runs_nothing(self):
        rig = Rig(self)
        self.assertEqual(rig.lane.stop(), {"how": None, "exit": None, "kill_offset": None, "failures": []})
        self.assertEqual(rig.calls, [])


class KillOffset(unittest.TestCase):
    def test_the_offset_indexes_the_final_text_even_mid_character(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "emulator.log"
            the_lane = lane.EmulatorLane(EMULATOR, ADB, log)
            log.write_bytes("INFO | caf".encode() + "\u00e9".encode()[:1])       # the kill lands inside "é"
            offset = len(the_lane._log_text(log.stat().st_size))
            log.write_bytes("INFO | caf\u00e9\nWARNING      | Netsim Wifi localhost:1 is gone due to CANCELLED\n".encode())
            text = the_lane._log_text()
            self.assertEqual(text[:offset], "INFO | caf")
            self.assertEqual(m.emulator_log_failures(text, offset), [])


class Restore(unittest.TestCase):
    def test_the_player_is_removed_and_the_package_set_restored(self):
        rig = Rig(self)
        assert rig.lane.start() is None
        rig.out["package"] = f"package:{dp.PACKAGE}\n"
        rig.out["packages"] = "package:com.android.shell\npackage:com.android.settings\n"      # another order
        self.assertIsNone(rig.lane.restore())
        self.assertIn(("uninstall", [ADB, "-s", "emulator-5580", "uninstall", dp.PACKAGE]), rig.calls)

    def test_a_changed_package_set_is_named(self):
        rig = Rig(self)
        assert rig.lane.start() is None
        rig.out["packages"] = PACKAGES + "package:com.example.left.behind\n"
        self.assertEqual(rig.lane.restore(), "packages differ from the baseline: added ['package:com.example.left.behind'], removed []")

    def test_a_failed_uninstall_is_named(self):
        rig = Rig(self)
        assert rig.lane.start() is None
        rig.out["package"], rig.out["uninstall"] = f"package:{dp.PACKAGE}\n", "Failure [DELETE_FAILED_INTERNAL_ERROR]\n"
        self.assertEqual(rig.lane.restore(), "uninstall did not report Success: 'Failure [DELETE_FAILED_INTERNAL_ERROR]\\n'")


class Ca(unittest.TestCase):
    """The test CA as an owned, reversible user certificate on the dedicated emulator, each privileged
    step its own `su 0` command: adbd itself is never restarted."""

    PATH = f"{STORE}/d56d3822.0"

    def started(self):
        rig = Rig(self)
        assert rig.lane.start() is None
        rig.pem = rig.dir / "ca.pem"
        rig.pem.write_bytes(b"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n")
        return rig

    def assert_command_contract(self, rig):
        """Every device call names the dedicated serial; no call restarts adbd; every call on the store
        or the staging file is a `su 0` command; removals target only this lane's exact paths."""
        for name, argv in rig.calls:
            if argv[:1] == [ADB] and argv[1:2] != ["version"] and argv[1:2] != ["devices"]:
                self.assertEqual(argv[:3], PREFIX, name)
            self.assertNotIn(argv[3:4], (["root"], ["unroot"]), name)
            self.assertFalse(any(w in ("kill-server", "start-server") for w in argv), name)
            if name in FakeDeviceFS.NAMES - {"push"}:
                self.assertEqual(argv[4:6], ["su", "0"], name)
            if name in ("rm_ca", "rm_stage", "rmdir_store"):
                self.assertIn(argv[-1], (self.PATH, STAGE, STORE), name)

    def test_clean_placement_and_removal(self):
        rig = self.started()
        self.assertIsNone(rig.lane.place_ca(rig.pem))
        fs = rig.device
        self.assertEqual(fs.files[self.PATH], [rig.pem.read_bytes(), "system", "644"])
        self.assertNotIn(STAGE, fs.files)
        self.assertIn(STORE, fs.dirs)
        self.assertEqual(rig.lane.ca["path"], self.PATH)
        self.assertIsNone(rig.lane.remove_ca())
        self.assertEqual((fs.files, fs.dirs), ({}, set()))
        self.assertIsNone(rig.lane.ca)
        self.assertIsNone(rig.lane.remove_ca())                            # nothing owned: nothing runs
        self.assert_command_contract(rig)
        read = [a for n, a in rig.calls if n == "ca_read"]
        self.assertEqual(read, [PREFIX + ["exec-out", "su", "0", "cat", self.PATH]])   # raw transport, privileged

    def test_the_privilege_receipt_comes_first_and_is_command_local(self):
        rig = self.started()
        before = len(rig.calls)
        assert rig.lane.place_ca(rig.pem) is None
        device = [(n, a) for n, a in rig.calls[before:] if a[:1] == [ADB]]
        self.assertEqual(device[0], ("su_uid", PREFIX + ["shell", "su", "0", "id", "-u"]))
        self.assertFalse(hasattr(rig.lane, "rooted_by_lane"))              # no adbd privilege state is kept

    def test_refusals_before_any_change(self):
        rig = Rig(self)
        pem = rig.dir / "ca.pem"
        pem.write_bytes(b"x")
        self.assertEqual(rig.lane.place_ca(pem), "no started, identified emulator: start() did not complete")
        self.assertEqual(rig.calls, [])
        rig = self.started()
        assert rig.lane.place_ca(rig.pem) is None
        before = len(rig.calls)
        self.assertEqual(rig.lane.place_ca(rig.pem), "this lane already owns a placed CA; remove_ca() it first")
        self.assertEqual(rig.calls[before:], [])

    def test_a_pre_existing_store_is_kept_with_its_other_certificates(self):
        rig = self.started()
        fs = rig.device
        fs.dirs.add(STORE)
        fs.files[f"{STORE}/aabbccdd.0"] = [b"other", "system", "644"]
        self.assertIsNone(rig.lane.place_ca(rig.pem))
        self.assertIs(rig.lane.ca["store_created"], False)
        self.assertIsNone(rig.lane.remove_ca())
        self.assertEqual(fs.dirs, {STORE})
        self.assertEqual(fs.files, {f"{STORE}/aabbccdd.0": [b"other", "system", "644"]})
        self.assertNotIn("rmdir_store", rig.names())
        self.assert_command_contract(rig)

    def test_an_occupied_slot_or_staging_path_is_never_overwritten(self):
        for label, occupy, reason in (
                ("the same hash, slot 0", f"{STORE}/d56d3822.0",
                 f"{STORE}/d56d3822.0 already exists; this lane never overwrites a user certificate"),
                ("the same hash, another slot", f"{STORE}/d56d3822.1",
                 f"{STORE}/d56d3822.1 already exists; this lane never overwrites a user certificate"),
                ("the staging path", STAGE, f"{STAGE} already exists; this lane never overwrites it")):
            with self.subTest(label):
                rig = self.started()
                fs = rig.device
                fs.dirs.add(STORE)
                fs.files[occupy] = [b"theirs", "system", "644"]
                self.assertEqual(rig.lane.place_ca(rig.pem), reason)
                self.assertEqual(fs.files, {occupy: [b"theirs", "system", "644"]})
                self.assertIsNone(rig.lane.ca)
                self.assertFalse(any(n in ("push", "mkdir", "cp", "rm_ca", "rm_stage", "rmdir_store") for n in rig.names()))

    def test_a_failure_at_each_change_is_undone_and_named(self):
        for step in ("push", "mkdir", "chown_store", "cp", "chown_ca", "chmod_ca", "ca_read", "ca_stat"):
            for how in ("rc", "raises", "timeout"):
                with self.subTest(step=step, how=how):
                    rig = self.started()
                    if how == "rc":
                        rig.rc[step] = 1
                    elif how == "raises":
                        rig.raises[step] = OSError(f"{step}: fault")
                    else:
                        rig.timeouts.add(step)
                    reason = rig.lane.place_ca(rig.pem)
                    self.assertIsNotNone(reason, "a failed placement was reported as placed")
                    self.assertTrue(reason.startswith(f"CA placement failed at {step}: "), reason)
                    fs = rig.device
                    self.assertEqual((fs.files, fs.dirs), ({}, set()), reason)
                    self.assertIsNone(rig.lane.ca)
                    self.assert_command_contract(rig)

    def test_a_change_that_took_effect_before_failing_is_still_undone(self):
        for step in ("push", "mkdir", "cp"):
            with self.subTest(step):
                rig = self.started()
                rig.effect_then_fail.add(step)
                reason = rig.lane.place_ca(rig.pem)
                self.assertIsNotNone(reason, "a failed placement was reported as placed")
                self.assertTrue(reason.startswith(f"CA placement failed at {step}: rc 1"), reason)
                self.assertEqual((rig.device.files, rig.device.dirs), ({}, set()))
                self.assertIsNone(rig.lane.ca)

    def test_a_read_back_that_differs_is_not_a_placement(self):
        for step, value in (("ca_read", "-----BEGIN CERTIFICATE-----\nOTHER\n-----END CERTIFICATE-----\n"),
                            ("ca_stat", f"-rw-rw-rw- 1 shell shell {LABEL}  9 2026-09-25 12:22 {STORE}/d56d3822.0\n"),
                            ("ca_stat", f"-rw-r--r-- 1 system system u:object_r:shell_data_file:s0  61 2026-09-25 12:22 {STORE}/d56d3822.0\n")):
            with self.subTest(step=step, value=value[:20]):
                rig = self.started()
                rig.out[step] = value
                reason = rig.lane.place_ca(rig.pem)
                self.assertIsNotNone(reason, "a failed placement was reported as placed")
                self.assertTrue(reason.startswith(f"CA placement failed at {step}: "), reason)
                self.assertEqual((rig.device.files, rig.device.dirs), ({}, set()))
                self.assertIsNone(rig.lane.ca)

    def test_incomplete_cleanup_keeps_the_obligation(self):
        rig = self.started()
        assert rig.lane.place_ca(rig.pem) is None
        rig.rc["rm_ca"] = 1
        reason = rig.lane.remove_ca()
        self.assertTrue(reason.startswith("CA removal incomplete: "), reason)
        self.assertIn("rm_ca", reason)
        self.assertIsNotNone(rig.lane.ca)
        self.assertEqual(rig.lane.restore(), "a placed CA is still owned; remove_ca() it first")
        del rig.rc["rm_ca"]
        self.assertIsNone(rig.lane.remove_ca())
        self.assertEqual((rig.device.files, rig.device.dirs), ({}, set()))

    def test_restore_refuses_while_a_ca_is_owned_and_stop_retires_it(self):
        rig = self.started()
        assert rig.lane.place_ca(rig.pem) is None
        before = len(rig.calls)
        self.assertEqual(rig.lane.restore(), "a placed CA is still owned; remove_ca() it first")
        self.assertEqual(rig.calls[before:], [])
        receipt = rig.lane.stop()
        self.assertEqual(receipt["failures"], [])
        self.assertEqual((rig.device.files, rig.device.dirs), ({}, set()))
        names = rig.names()
        self.assertLess(names.index("rm_ca"), names.index("stop_emulator"))
        self.assertIsNone(rig.lane.ca)

    def test_stop_reports_a_ca_it_could_not_retire(self):
        rig = self.started()
        assert rig.lane.place_ca(rig.pem) is None
        rig.raises["rm_ca"] = OSError("adb: device offline")
        receipt = rig.lane.stop()
        self.assertTrue(any(f.startswith("CA removal incomplete: ") and "rm_ca" in f for f in receipt["failures"]),
                        receipt["failures"])
        self.assertEqual((receipt["how"], receipt["exit"]), ("exited", 0))


class StoreInventoryRecord(unittest.TestCase):
    """What the user store held just before placement, kept as an immutable fact."""

    started = Ca.started

    def test_the_inventory_before_placement_is_recorded(self):
        rig = self.started()
        assert rig.lane.place_ca(rig.pem) is None
        self.assertEqual(rig.lane.ca["store_before"], lane.StoreInventory(existed=False, entries=(), parseable=True))
        self.assertIs(rig.lane.ca["placed"], True)
        with self.assertRaises(Exception):
            rig.lane.ca["store_before"].entries = ()                        # frozen
        rig = self.started()
        rig.device.dirs.add(STORE)
        rig.device.files[f"{STORE}/aabbccdd.0"] = [b"other", "system", "644"]
        assert rig.lane.place_ca(rig.pem) is None
        self.assertEqual(rig.lane.ca["store_before"], lane.StoreInventory(existed=True, entries=("aabbccdd.0",), parseable=True))

    def test_an_unexpected_listing_is_recorded_as_unparseable(self):
        rig = self.started()
        rig.device.dirs.add(STORE)
        rig.result["ca_store"] = {"rc": 0, "stdout": "garbage\n", "stderr": "", "timeout": False}
        assert rig.lane.place_ca(rig.pem) is None
        self.assertIs(rig.lane.ca["store_before"].parseable, False)

    def test_placed_is_set_only_by_a_complete_placement(self):
        rig = self.started()
        rig.noop.add("rm_stage")
        self.assertIsNotNone(rig.lane.place_ca(rig.pem))
        self.assertIs(rig.lane.ca["placed"], False)                         # the stage still owed
        rig = self.started()
        rig.rc["chmod_ca"] = 1
        self.assertIsNotNone(rig.lane.place_ca(rig.pem))
        self.assertIsNone(rig.lane.ca)


class Privilege(unittest.TestCase):
    """`su 0 id -u` must be a strict zero before any change; privilege is never assumed for later steps."""

    started = Ca.started

    def test_a_failed_privilege_receipt_refuses_before_any_change(self):
        for label, arrange, reason in (
                ("su missing", lambda r: setattr(r.device, "su", "missing"),
                 "su 0 id -u: su_uid: rc 127: '/system/bin/sh: su: inaccessible or not found'"),
                ("su denied", lambda r: setattr(r.device, "su", "denied"), "su 0 id -u: su_uid: rc 1: 'su: permission denied'"),
                ("a non-root uid", lambda r: setattr(r.device, "su", "uid:2000"), "su 0 id -u: uid 2000, expected 0"),
                ("an empty uid", lambda r: setattr(r.device, "su", "uid:"), "su 0 id -u: inconclusive receipt ''"),
                ("a malformed uid", lambda r: setattr(r.device, "su", "uid:uid=0(root)"),
                 "su 0 id -u: inconclusive receipt 'uid=0(root)'"),
                ("a signed uid", lambda r: setattr(r.device, "su", "uid:-0"), "su 0 id -u: inconclusive receipt '-0'"),
                ("a timeout", lambda r: r.timeouts.add("su_uid"), "su 0 id -u: su_uid: timed out"),
                ("a raise", lambda r: r.raises.__setitem__("su_uid", OSError("adb: device offline")),
                 "su 0 id -u: su_uid: raised OSError('adb: device offline')")):
            with self.subTest(label):
                rig = self.started()
                arrange(rig)
                self.assertEqual(rig.lane.place_ca(rig.pem), f"CA placement refused: {reason}")
                self.assertFalse(any(n in ("ca_store", "push", "mkdir", "cp") for n in rig.names()))
                self.assertEqual((rig.device.files, rig.device.dirs), ({}, set()))
                self.assertIsNone(rig.lane.ca)

    def test_a_failed_privilege_receipt_at_removal_keeps_the_obligation(self):
        rig = self.started()
        assert rig.lane.place_ca(rig.pem) is None
        rig.device.su = "denied"
        reason = rig.lane.remove_ca()
        self.assertEqual(reason, "CA removal incomplete: su 0 id -u: su_uid: rc 1: 'su: permission denied'")
        self.assertIn(Ca.PATH, rig.device.files)
        self.assertIsNotNone(rig.lane.ca)
        rig.device.su = "ok"
        self.assertIsNone(rig.lane.remove_ca())
        self.assertEqual((rig.device.files, rig.device.dirs), ({}, set()))

    def test_a_wrapper_failure_is_never_evidence_of_absence(self):
        # A failed su or adb whose message happens to contain "No such file or directory" says nothing
        # about the requested path: only `ls`'s own exact line for that path is absence.
        wrapper = {"rc": 1, "stdout": "", "stderr": "su: /system/xbin/su: No such file or directory\n", "timeout": False}
        rig = self.started()
        assert rig.lane.place_ca(rig.pem) is None
        rig.result["ca_gone"] = wrapper
        reason = rig.lane.remove_ca()
        self.assertIsNotNone(reason, "a wrapper failure was taken as proof the CA was gone")
        self.assertIn(Ca.PATH, reason)
        self.assertIsNotNone(rig.lane.ca)
        self.assertTrue(rig.lane.ca["ca_created"])
        del rig.result["ca_gone"]
        self.assertIsNone(rig.lane.remove_ca())
        self.assertEqual((rig.device.files, rig.device.dirs), ({}, set()))

    def test_a_wrapper_failure_at_the_store_check_is_not_an_absent_store(self):
        rig = self.started()
        rig.result["ca_store"] = {"rc": 1, "stdout": "", "stderr": "adb: error: No such file or directory\n", "timeout": False}
        reason = rig.lane.place_ca(rig.pem)
        self.assertIsNotNone(reason, "a wrapper failure was taken as an absent store")
        self.assertTrue(reason.startswith("CA placement failed at ca_store: rc 1"), reason)
        self.assertFalse(any(n in ("push", "mkdir", "cp") for n in rig.names()))
        self.assertIsNone(rig.lane.ca)

    def test_privileged_steps_that_fail_after_the_receipt_are_named_and_undone(self):
        for step in ("ca_read", "rm_stage"):
            with self.subTest(step):
                rig = self.started()
                rig.result[step] = {"rc": 1, "stdout": "", "stderr": "su: permission denied\n", "timeout": False}
                reason = rig.lane.place_ca(rig.pem)
                self.assertIsNotNone(reason)
                self.assertIn(step, reason)
                if step == "ca_read":
                    self.assertEqual((rig.device.files, rig.device.dirs), ({}, set()))
                    self.assertIsNone(rig.lane.ca)
                else:                                              # the stage could not be removed: still owned
                    self.assertTrue(reason.startswith("CA placement incomplete: "), reason)
                    self.assertTrue(rig.lane.ca["stage_created"])

    def test_remote_operands_are_plain_words(self):
        rig = self.started()
        rig.out["ca_hash"] = "d56d3822\n"
        with self.assertRaises(ValueError):
            rig.lane._su("probe", "ls", "/data/misc/user/0/cacerts-added; reboot")
        self.assertNotIn("probe", rig.names())


class CaInputs(unittest.TestCase):
    """The staging postcondition and the local CA inputs."""

    started = Ca.started

    def test_a_staging_file_that_survives_removal_is_not_a_placement(self):
        rig = self.started()
        rig.noop.add("rm_stage")
        reason = rig.lane.place_ca(rig.pem)
        self.assertIsNotNone(reason, "a remaining staging file was reported as a clean placement")
        self.assertEqual(reason, f"CA placement incomplete: {STAGE} is still present after rm_stage; "
                                 "the CA and the staging file stay owned")
        self.assertIs(rig.lane.ca["stage_created"], True)
        rig.noop.discard("rm_stage")
        self.assertIsNone(rig.lane.remove_ca())
        self.assertEqual((rig.device.files, rig.device.dirs), ({}, set()))

    def test_an_inconclusive_staging_check_fails_closed(self):
        rig = self.started()
        rig.fail_nth["stage_check"] = 2                           # the check after rm_stage
        reason = rig.lane.place_ca(rig.pem)
        self.assertIsNotNone(reason)
        self.assertTrue(reason.startswith("CA placement incomplete: stage_check: rc 1"), reason)
        self.assertIs(rig.lane.ca["stage_created"], True)
        self.assertIsNone(rig.lane.remove_ca())
        self.assertEqual((rig.device.files, rig.device.dirs), ({}, set()))

    def test_local_input_failures_are_refusals_before_any_adb(self):
        cases = (
            ("a missing CA file", lambda rig: rig.pem.unlink(), "CA file: FileNotFoundError"),
            ("an unreadable CA file", lambda rig: rig.pem.chmod(0), "CA file: PermissionError"),
            ("a raising hash process", lambda rig: rig.raises.__setitem__("ca_hash", OSError("openssl: not found")),
             "CA subject hash: raised OSError('openssl: not found')"),
            ("a hash timeout", lambda rig: rig.timeouts.add("ca_hash"), "CA subject hash: timed out"),
            ("a malformed hash", lambda rig: rig.out.__setitem__("ca_hash", "zzzz\n"), "CA subject hash: rc 0: 'zzzz'"))
        for label, damage, prefix in cases:
            with self.subTest(label):
                rig = self.started()
                self.addCleanup(lambda r=rig: r.pem.exists() and r.pem.chmod(0o600))
                damage(rig)
                before = len(rig.calls)
                reason = rig.lane.place_ca(rig.pem)
                self.assertIsNotNone(reason)
                self.assertTrue(reason.startswith(prefix), reason)
                self.assertEqual([n for n, a in rig.calls[before:] if a[:1] == [ADB]], [])
                self.assertIsNone(rig.lane.ca)


class LiveAdmission(unittest.TestCase):
    """The live composition's install and Start, over this lane: admitted only for a completed placement
    of this run's own CA, and never once its retirement has begun."""

    def setUp(self):
        import threading
        import live_actors
        from test_live_actors import Fixture
        self.threading, self.live_actors = threading, live_actors
        self.fx = Fixture(self)
        self.rig = Rig(self)
        assert self.rig.lane.start() is None
        self.run = self.fx.live_run(adb=Path(ADB))
        self.actors, _n = live_actors.compose(self.run, self.rig.lane, "/builds/player.apk")
        self.effects = []
        player = self.actors.player_install.player
        player.install = lambda stop, budget_s: self.effects.append("install") or None
        player.start = lambda stop, dispatched, admit: self.effects.append("start") or None

    def admitted(self):
        """(install's result, Start's result), each through the composed actors."""
        return (self.actors.player_install(self.threading.Event(), 60),
                self.actors.player_start(self.threading.Event(), lambda: None, lambda: None))

    def test_this_runs_placed_ca_is_admitted(self):
        assert self.rig.lane.place_ca(self.run.ca) is None
        self.assertEqual(self.admitted(), (None, None))
        self.assertEqual(self.effects, ["install", "start"])

    def test_a_partial_removal_revokes_admission_but_keeps_the_obligation(self):
        assert self.rig.lane.place_ca(self.run.ca) is None
        self.rig.result["rmdir_store"] = {"rc": 1, "stdout": "", "stderr": "rmdir: refusal\n", "timeout": False}
        self.assertTrue(self.rig.lane.remove_ca().startswith("CA removal incomplete: "))
        self.assertIsNotNone(self.rig.lane.ca)                             # the store is still owed
        for reason in self.admitted():
            self.assertIsNotNone(reason, "a CA whose removal had begun was still admitted")
            self.assertTrue(reason.startswith("trust prerequisite: "), reason)
        self.assertEqual(self.effects, [])
        del self.rig.result["rmdir_store"]
        self.assertIsNone(self.rig.lane.remove_ca())                        # later cleanup still completes
        self.assertEqual((self.rig.device.files, self.rig.device.dirs), ({}, set()))

    def test_a_refused_removal_still_revokes_admission(self):
        assert self.rig.lane.place_ca(self.run.ca) is None
        self.rig.device.su = "denied"
        self.assertTrue(self.rig.lane.remove_ca().startswith("CA removal incomplete: su 0 id -u"))
        self.assertIn(f"{STORE}/d56d3822.0", self.rig.device.files)       # nothing removed yet
        for reason in self.admitted():
            self.assertIsNotNone(reason, "a CA whose removal had begun was still admitted")
        self.assertEqual(self.effects, [])

    def test_another_runs_ca_is_not_admitted(self):
        from test_live_actors import Fixture
        other = Fixture(self)                                              # an independently generated CA
        self.rig.out["ca_hash"] = "d56d3822\n"
        assert self.rig.lane.place_ca(other.run_dir / "creds" / "ca.pem") is None
        for reason in self.admitted():
            self.assertIsNotNone(reason, "another run's CA was admitted")
            self.assertIn("is not this run's CA", reason)
        self.assertEqual(self.effects, [])

    def test_a_run_input_changed_between_install_and_start_is_judged_again(self):
        assert self.rig.lane.place_ca(self.run.ca) is None
        self.assertIsNone(self.actors.player_install(self.threading.Event(), 60))
        receipt = json.loads(self.run.credentials_receipt.read_text())
        self.run.credentials_receipt.write_text(json.dumps(dict(receipt, ca_der_sha256="0" * 64)))
        reason = self.actors.player_start(self.threading.Event(), lambda: None, lambda: None)
        self.assertIsNotNone(reason)
        self.assertIn("is not this run's CA", reason)
        self.assertEqual(self.effects, ["install"])


class Declarations(unittest.TestCase):
    def test_every_command_the_lane_issues_is_a_declared_device_shape(self):
        # A shape word "<name>" stands for one word; a final "<argv...>" for the rest of the argv.
        shapes = m.Device(m.DEDICATED_SERIAL, lambda *a: None, adb=ADB).commands()
        self.assertNotIn("root", shapes)
        self.assertNotIn("unroot", shapes)

        def fits(argv, shape):
            if shape[-1:] == ["<argv...>"]:
                return len(argv) >= len(shape) and fits(argv[:len(shape) - 1], shape[:-1])
            return len(argv) == len(shape) and all(w == v or (w.startswith("<") and w.endswith(">")) for w, v in zip(shape, argv))
        rig = Ca.started(self)
        assert rig.lane.place_ca(rig.pem) is None and rig.lane.remove_ca() is None
        assert rig.lane.restore() is None
        rig.lane.stop()
        issued = [(n, a) for n, a in rig.calls if a[:3] == PREFIX]
        self.assertTrue(issued)
        for name, argv in issued:
            self.assertTrue(any(fits(argv, shape) for shape in shapes.values()), f"{name}: {argv} is not a declared shape")


    def test_the_lane_targets_only_the_dedicated_serial(self):
        self.assertEqual(lane.SERIAL, m.DEDICATED_SERIAL)
        self.assertEqual(lane.AVD, "moq5-runtime-2392")
        self.assertIn("-port", lane.EMULATOR_ARGS)
        self.assertEqual(lane.EMULATOR_ARGS[lane.EMULATOR_ARGS.index("-port") + 1], "5580")


if __name__ == "__main__":
    unittest.main()
