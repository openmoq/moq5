"""The dedicated emulator's lifecycle for a supervised device run: start the owned `moq5-runtime-2392`
on `emulator-5580`, prove what it is, record its package baseline, restore it, and stop it.

    start()    the local `adb version` receipt of the declared adb, and `emulator-5580` absent, BEFORE
               anything is spawned; then the emulator (a directly owned child logging to a file), a
               bounded boot wait, the exact identity (AVD, SDK, ABI) and the package baseline (a set:
               `pm list packages` order is not stable across boots).
    place_ca() the run's test CA as a user certificate, `<subject_hash_old>.0` in the user store, owned by
               this lane: nothing existing is overwritten, every created path is recorded, the bytes and
               the owner/mode/label are read back, and any failure undoes what was created. The store's
               inventory just before placement is kept as an immutable `StoreInventory`. Privilege
               is per command (`su 0`, after a strict `su 0 id -u` receipt of 0); adbd itself is never
               restarted, so nothing the emulator runs over adb after boot is disturbed.
    remove_ca() removes exactly what place_ca() created (the store only if it created it and it is
               empty) and verifies each is gone; an incomplete removal keeps the obligation.
    restore()  the player package removed if present, then the package set equal to the baseline;
               refused while a placed CA is still owned.
    stop()     `emu kill`, the log offset captured at that moment on the same decoded text that is
               judged, a bounded exit (SIGKILL after the bound, named as a failure), and the log judged
               by `emulator_log_failures`: no diagnostic before the kill, only the declared shutdown
               warnings after it.

Every adb command names the dedicated serial through `Device`, except the two host-level reads
(`adb version`, which never contacts the server, and `adb devices`). Execution goes through the
injected runner and spawner, so the offline tests prove the sequence without an emulator.
"""
import hashlib
import re
import ssl
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import device_player
from run_android_media import DEDICATED_SERIAL, Device, adb_version_receipt, emulator_log_failures
from runtime_support import OPENSSL, cmd

SERIAL = DEDICATED_SERIAL
AVD = "moq5-runtime-2392"
EMULATOR_ARGS = ("-avd", AVD, "-port", "5580", "-no-window", "-no-audio", "-no-boot-anim", "-no-snapshot",
                 "-gpu", "swiftshader_indirect", "-memory", "2048")
IDENTITY = (("avd", AVD), ("sdk", "36"), ("abi", "arm64-v8a"))
COMMAND_S = 30.0              # any one adb command
BOOT_S = 180.0                # from spawn to sys.boot_completed=1
POLL_S = 1.0                  # between boot polls
SHUTDOWN_S = 60.0             # from `emu kill` to the emulator's exit
CA_STORE = "/data/misc/user/0/cacerts-added"
CA_STAGE = "/data/local/tmp/moq5-test-ca.pem"
CA_LABEL = "u:object_r:misc_user_data_file:s0"
_OPERAND = re.compile(r"[A-Za-z0-9/._:=-]+")      # a remote argument the device shell passes through as one word


@dataclass(frozen=True)
class StoreInventory:
    """The user CA store as `su 0 ls -1a` listed it just before placement: whether it existed, its
    entries other than `.` and `..`, and whether the listing had the expected shape at all."""
    existed: bool
    entries: tuple
    parseable: bool


def default_runner(name, argv, timeout):
    return cmd(argv, timeout=timeout)


class EmulatorLane:
    def __init__(self, emulator, adb, log_path, spawn=subprocess.Popen, runner=default_runner,
                 clock=time.monotonic, wait=time.sleep, openssl=OPENSSL):
        self.emulator, self.adb, self.log_path = str(emulator), str(adb), Path(log_path)
        self.spawn, self.runner, self.clock, self.wait, self.openssl = spawn, runner, clock, wait, str(openssl)
        self.device = Device(SERIAL, runner, adb=self.adb)
        self.proc, self.log, self.baseline, self.ca = None, None, None, None

    def _host(self, name, *args):
        return self.runner(name, [self.adb, *args], COMMAND_S)

    def _run(self, name, *args):
        r = self.device.run(name, *args, timeout=COMMAND_S)
        if r.get("timeout"):
            raise RuntimeError(f"{name}: timed out")
        if r.get("rc") != 0:
            raise RuntimeError(f"{name}: rc {r.get('rc')}: {(r.get('stdout') or '') + (r.get('stderr') or '')!r}")
        return r.get("stdout") or ""

    def _packages(self):
        return set(line for line in self._run("packages", "shell", "pm", "list", "packages").splitlines() if line)

    def start(self):
        """None once the dedicated emulator is up, identified and baselined; otherwise the reason. A
        refusal before the spawn leaves nothing running; after it, `stop()` is still owed. A lane owns
        at most one emulator: while it owns one, start() refuses without running anything."""
        if self.proc is not None:
            return "this lane already owns a started emulator; stop() it first"
        self.baseline = None
        _version, wrong = adb_version_receipt(self._host("adb_version", "version"), self.adb)
        if wrong:
            return "adb version: " + "; ".join(wrong)
        devices = self._host("devices", "devices")
        if devices.get("rc") != 0:
            return f"adb devices: rc {devices.get('rc')}"
        if any(line.split("\t")[0] == SERIAL for line in (devices.get("stdout") or "").splitlines()[1:]):
            return f"{SERIAL} is already attached; this run owns only an emulator it starts"
        self.log = open(self.log_path, "w")
        try:
            self.proc = self.spawn([self.emulator, *EMULATOR_ARGS], stdout=self.log, stderr=subprocess.STDOUT)
        except BaseException:
            self.log.close()
            self.log = None
            raise
        deadline = self.clock() + BOOT_S
        while True:
            if self.proc.poll() is not None:
                return f"the emulator exited during boot: rc {self.proc.poll()}"
            r = self.device.run("boot_completed", "shell", "getprop", "sys.boot_completed", timeout=COMMAND_S)
            if r.get("rc") == 0 and (r.get("stdout") or "").strip() == "1":
                break
            if self.clock() + POLL_S > deadline:
                return f"boot not completed within {BOOT_S:g}s"
            self.wait(POLL_S)
        try:
            got = {"avd": (self._run("avd_name", "emu", "avd", "name").splitlines() or [""])[0],
                   "sdk": self._run("sdk", "shell", "getprop", "ro.build.version.sdk").strip(),
                   "abi": self._run("abi", "shell", "getprop", "ro.product.cpu.abilist").strip()}
            for key, want in IDENTITY:
                if got[key] != want:
                    return f"identity: {key} is {got[key]!r}, expected {want!r}"
            self.baseline = self._packages()
        except RuntimeError as e:
            return str(e)
        return None

    def restore(self):
        """None once the player package is gone and the package set equals the baseline; otherwise the
        reason. Without an owned emulator and a baseline from its completed start() it runs nothing."""
        if self.proc is None:
            return "no started emulator to restore"
        if self.ca is not None:
            return "a placed CA is still owned; remove_ca() it first"
        if self.baseline is None:
            return "no package baseline: start() did not complete"
        try:
            if self._run("package", "shell", "pm", "list", "packages", device_player.PACKAGE).strip():
                out = self._run("uninstall", "uninstall", device_player.PACKAGE)
                if out.strip().splitlines()[-1:] != ["Success"]:
                    return f"uninstall did not report Success: {out!r}"
            now = self._packages()
        except RuntimeError as e:
            return str(e)
        if now != self.baseline:
            return (f"packages differ from the baseline: added {sorted(now - self.baseline)}, "
                    f"removed {sorted(self.baseline - now)}")
        return None

    # -- the test CA as an owned user certificate ------------------------------------------------
    def _step(self, name, *args):
        """(result, None) when the command ran with rc 0; otherwise (result or None, the failure named)."""
        try:
            r = self.device.run(name, *args, timeout=COMMAND_S)
        except Exception as e:
            return None, f"{name}: raised {e!r}"
        if r.get("timeout"):
            return r, f"{name}: timed out"
        if r.get("rc") != 0:
            return r, f"{name}: rc {r.get('rc')}: {((r.get('stderr') or '') + (r.get('stdout') or '')).strip()!r}"
        return r, None

    def _su(self, name, *argv, raw=False):
        """One privileged command, `su 0 <argv>`, through `adb shell` (or `adb exec-out` for a raw read).
        Every operand must be a plain word, so the device shell cannot reinterpret it."""
        bad = [a for a in argv if not _OPERAND.fullmatch(a)]
        if bad:
            raise ValueError(f"{name}: {bad[0]!r} is not a plain remote operand")
        return self._step(name, "exec-out" if raw else "shell", "su", "0", *argv)

    def _absent(self, name, path):
        """True when `ls` itself reports exactly that `path` does not exist, False when it lists it; a
        failure otherwise. A wrapper's message is never evidence about the path."""
        r, err = self._su(name, "ls", path)
        if err is None:
            return False, None
        if r is not None and r.get("rc") == 1 and not (r.get("stdout") or "") \
                and (r.get("stderr") or "").strip() == f"ls: {path}: No such file or directory":
            return True, None
        return None, err

    def _privilege(self):
        """None when `su 0 id -u` is exactly the uid 0; otherwise the refusal. Only an unsigned decimal
        uid is a fact. The receipt is for this moment only: every later step is its own `su 0`
        command and is judged by its own status."""
        r, err = self._su("su_uid", "id", "-u")
        if err:
            return f"su 0 id -u: {err}"
        out = (r.get("stdout") or "").strip()
        if not re.fullmatch(r"[0-9]+", out):
            return f"su 0 id -u: inconclusive receipt {out!r}"
        return None if int(out) == 0 else f"su 0 id -u: uid {out}, expected 0"

    def _retire(self, ca):
        """Removes what `ca` records as created and verifies each is gone. Returns the incomplete steps;
        `ca` keeps only what remains."""
        incomplete = []
        for key, name, args, check in (("ca_created", "rm_ca", ("rm", "-f", ca["path"]), ("ca_gone", ca["path"])),
                                       ("stage_created", "rm_stage", ("rm", "-f", ca["stage"]), ("stage_check", ca["stage"])),
                                       ("store_created", "rmdir_store", ("rmdir", CA_STORE), ("store_gone", CA_STORE))):
            if not ca[key]:
                continue
            gone, err = self._absent(*check)
            if gone:
                ca[key] = False
                continue
            _r, step_err = self._su(name, *args)
            gone, err = self._absent(*check)
            if gone:
                ca[key] = False
            else:
                incomplete.append(f"{check[1]}: {step_err or err or 'still present'}")
        return incomplete

    def place_ca(self, ca_pem):
        """None once the CA at `ca_pem` is a verified user certificate owned by this lane; otherwise the
        reason, with everything this call created removed (or the remainder still owned, and named)."""
        if self.proc is None or self.baseline is None:
            return "no started, identified emulator: start() did not complete"
        if self.ca is not None:
            return "this lane already owns a placed CA; remove_ca() it first"
        try:
            data = Path(ca_pem).read_bytes()
            der_sha256 = hashlib.sha256(ssl.PEM_cert_to_DER_cert(data.decode("ascii"))).hexdigest()
        except OSError as e:
            return f"CA file: {e!r}"
        except (UnicodeDecodeError, ValueError) as e:
            return f"CA file: not one PEM certificate: {e}"
        try:
            h = self.runner("ca_hash", [self.openssl, "x509", "-in", str(ca_pem), "-noout", "-subject_hash_old"], COMMAND_S)
        except Exception as e:
            return f"CA subject hash: raised {e!r}"
        if h.get("timeout"):
            return "CA subject hash: timed out"
        digest = (h.get("stdout") or "").strip()
        if h.get("rc") != 0 or not re.fullmatch(r"[0-9a-f]{8}", digest):
            return f"CA subject hash: rc {h.get('rc')}: {digest!r} {(h.get('stderr') or '').strip()!r}"
        ca = {"path": f"{CA_STORE}/{digest}.0", "stage": CA_STAGE, "ca_created": False, "stage_created": False,
              "store_created": False, "store_before": None, "placed": False,
              "ca_der_sha256": der_sha256}           # SHA-256 over the DER certificate, verified read back
        denied = self._privilege()
        if denied:
            return f"CA placement refused: {denied}"
        refusal, failure = None, None
        r, err = self._su("ca_store", "ls", "-1a", CA_STORE)                  # one name per line
        existed = err is None
        if err and not (r is not None and r.get("rc") == 1 and not (r.get("stdout") or "")
                        and (r.get("stderr") or "").strip() == f"ls: {CA_STORE}: No such file or directory"):
            failure = err
        if failure is None:
            listed = (r.get("stdout") or "").splitlines() if existed else [".", ".."]
            ca["store_before"] = StoreInventory(
                existed=existed, entries=tuple(listed[2:]),
                parseable=listed[:2] == [".", ".."] and all(re.fullmatch(r"[0-9a-f]{8}\.[0-9]+", n) for n in listed[2:]))
        if failure is None and existed:
            taken = [n for n in (r.get("stdout") or "").splitlines() if n.startswith(f"{digest}.")]
            if taken:
                refusal = f"{CA_STORE}/{sorted(taken)[0]} already exists; this lane never overwrites a user certificate"
        if failure is None and refusal is None:
            gone, err = self._absent("stage_check", CA_STAGE)
            if err:
                failure = err
            elif not gone:
                refusal = f"{CA_STAGE} already exists; this lane never overwrites it"
        if failure is None and refusal is None:
            steps = [("stage_created", "push", None, ("push", str(ca_pem), CA_STAGE))]    # local operand: plain adb
            if not existed:
                steps += [("store_created", "mkdir", True, ("mkdir", "-m", "755", CA_STORE)),
                          (None, "chown_store", True, ("chown", "system:system", CA_STORE))]
            steps += [("ca_created", "cp", True, ("cp", CA_STAGE, ca["path"])),
                      (None, "chown_ca", True, ("chown", "system:system", ca["path"])),
                      (None, "chmod_ca", True, ("chmod", "644", ca["path"]))]
            for key, name, privileged, args in steps:
                if key:
                    ca[key] = True                # attempted: it may exist even if the command failed
                _r, err = self._su(name, *args) if privileged else self._step(name, *args)
                if err:
                    failure = err
                    break
        if failure is None and refusal is None:
            r, err = self._su("ca_read", "cat", ca["path"], raw=True)
            if err:
                failure = err
            elif (r.get("stdout") or "").encode() != data:
                failure = "ca_read: the read-back differs from the CA's bytes"
        if failure is None and refusal is None:
            r, err = self._su("ca_stat", "ls", "-lZ", ca["path"])
            fields = (r.get("stdout") or "").split() if err is None else []
            want = ["-rw-r--r--", "system", "system", CA_LABEL, str(len(data))]
            if err:
                failure = err
            elif len(fields) != 9 or [fields[0], fields[2], fields[3], fields[4], fields[5]] != want or fields[8] != ca["path"]:
                failure = f"ca_stat: {(r.get('stdout') or '').strip()!r}, expected mode, owner, group, label, size {want}"
        if failure is None and refusal is None:
            _r, err = self._su("rm_stage", "rm", "-f", CA_STAGE)
            gone, check = self._absent("stage_check", CA_STAGE)
            ca["stage_created"] = gone is not True
            self.ca = ca
            if gone is not True:
                left = err or check or f"{CA_STAGE} is still present after rm_stage"
                return f"CA placement incomplete: {left}; the CA and the staging file stay owned"
            ca["placed"] = True                   # verified, and nothing but the CA itself remains
            return None
        incomplete = self._retire(ca)
        self.ca = ca if any(ca[k] for k in ("ca_created", "stage_created", "store_created")) else None
        head = refusal or f"CA placement failed at {failure}"
        return "; ".join([head] + ([f"cleanup incomplete: {'; '.join(incomplete)}"] if incomplete else []))

    def remove_ca(self):
        """None once nothing this lane placed remains (at once when nothing is owned); otherwise the
        incomplete steps, with the obligation kept."""
        if self.ca is None:
            return None
        self.ca["placed"] = False                 # retirement has begun: no admission from here on
        denied = self._privilege()
        if denied:
            return f"CA removal incomplete: {denied}"
        incomplete = self._retire(self.ca)
        if not any(self.ca[k] for k in ("ca_created", "stage_created", "store_created")):
            self.ca = None
        return "CA removal incomplete: " + "; ".join(incomplete) if incomplete else None

    def stop(self):
        """Stops the owned emulator and judges its log. Returns {how, exit, kill_offset, failures}. Once
        stopped, or when nothing was spawned, it runs nothing and returns an empty receipt."""
        receipt = {"how": None, "exit": None, "kill_offset": None, "failures": []}
        if self.proc is None:
            return receipt
        failures = receipt["failures"]
        if self.ca is not None:                   # the owned CA is removed while the emulator still runs
            unretired = self.remove_ca()
            if unretired is not None:
                failures.append(unretired)
        self.log.flush()
        receipt["kill_offset"] = len(self._log_text(self.log_path.stat().st_size))
        try:
            kill = self.device.run("stop_emulator", "emu", "kill", timeout=COMMAND_S)
        except Exception as e:                    # the process and log are still retired below
            kill = {"raised": e}
        if "raised" in kill:
            failures.append(f"emu kill: raised {kill['raised']!r}")
        elif kill.get("timeout") or kill.get("rc") != 0:
            failures.append(f"emu kill: rc {kill.get('rc')}" if not kill.get("timeout") else "emu kill: timed out")
        try:
            receipt["how"], receipt["exit"] = "exited", self.proc.wait(SHUTDOWN_S)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            receipt["how"], receipt["exit"] = "SIGKILL", self.proc.wait(SHUTDOWN_S)
            failures.append(f"the emulator did not exit within {SHUTDOWN_S:g}s of emu kill")
        self.log.close()
        self.proc, self.log = None, None
        failures.extend(emulator_log_failures(self._log_text(), receipt["kill_offset"]))
        return receipt

    def _log_text(self, size=None):
        """The log decoded (all of it when `size` is None). For the kill offset, the first `size` bytes
        less any incomplete UTF-8 sequence at their end, so the offset indexes the final decoded text
        exactly: that prefix decodes to the same characters there."""
        data = self.log_path.read_bytes()
        if size is None:
            return data.decode("utf-8", "replace")
        prefix = data[:size]
        for trim in range(4):
            try:
                return prefix[:len(prefix) - trim].decode("utf-8")
            except UnicodeDecodeError:
                continue
        return prefix.decode("utf-8", "replace")
