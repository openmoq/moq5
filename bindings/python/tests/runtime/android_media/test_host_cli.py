"""Subprocess tests for the host receiver child (`python -I host_receiver.py ...`).

    python -I bindings/python/tests/runtime/android_media/test_host_cli.py

The child runs the real CLI and client path against a fake *installed* moq5
package (fake_moq5_package.py) whose public shapes are pinned to the
published package source here. The fake refuses any socket inside the child,
so nothing can reach a network; this module changes nothing in the
interpreter that imports it. Every path used contains a space.
"""
import ast
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
MOQ5_SRC = HERE.parents[2] / "src" / "moq5"
CHILD = HERE / "host_receiver.py"

import fake_moq5_package as fake          # noqa: E402

CONTROLS = ["CTRL go", "CTRL ack-all", "CTRL ended-observed", "CTRL done"]


def write_objects(path, n=60, sync_every=24, step=41_666):
    """A well-formed objects file of `n` records (the run declares 60)."""
    with open(path, "wb") as f:
        for i in range(n):
            payload = bytes([0, 0, 0, 1, 9]) + i.to_bytes(2, "big") * (3 + i % 5)
            f.write(struct.pack("<IBQ", len(payload), 1 if i % sync_every == 0 else 0, i * step))
            f.write(payload)


class Run:
    """One child run: a fake package root, a run directory with the objects file and a CA file,
    all under directory names containing spaces."""

    def __init__(self, mode="happy"):
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        self.package = base / "installed pkg"
        self.run_dir = base / "run dir"
        self.run_dir.mkdir()
        self.objects = self.run_dir / "objects file.bin"
        write_objects(self.objects)
        self.ca = base / "ca dir" / "ca cert.pem"
        self.ca.parent.mkdir()
        self.ca.write_text("-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n")
        self.calls = fake.write_fake_package(self.package, {"mode": mode, "objects": str(self.objects)})
        self.url, self.sni = "moqt://127.0.0.1:4443", "localhost"

    def argv(self, **override):
        args = {"--package": str(self.package), "--url": self.url, "--ca": str(self.ca), "--sni": self.sni,
                "--objects": str(self.objects), "--establish-s": "2", "--deadline-s": "3"}
        args.update({k: v for k, v in override.items() if v is not None})
        drop = [k for k, v in override.items() if v is None]
        out = []
        for k, v in args.items():
            if k not in drop:
                out += [k, v]
        return out

    def run(self, *extra, **override):
        r = subprocess.run([sys.executable, "-I", "-B", "-W", "error::ResourceWarning", str(CHILD), *self.argv(**override), *extra],
                           capture_output=True, text=True, timeout=60, cwd=self.run_dir,
                           env={"PATH": "/usr/bin:/bin"})
        return r, r.stdout.splitlines(), fake.read_calls(self.calls)

    def close(self):
        self.tmp.cleanup()


def calls_of(calls, name):
    return [c for c in calls if c["call"] == name]


# ------------------------------------------------------------------ 0. the fake package is the published shape
class FakePackageShapes(unittest.TestCase):
    def published(self, name):
        return {n.name: n for n in ast.parse((MOQ5_SRC / name).read_text()).body if isinstance(n, ast.ClassDef)}

    def fake_classes(self):
        return {n.name: n for n in ast.parse(fake._SOURCE).body if isinstance(n, ast.ClassDef)}

    def fields(self, cls):
        return [s.target.id for s in cls.body if isinstance(s, ast.AnnAssign) and isinstance(s.target, ast.Name)]

    def members(self, cls):
        return [s.targets[0].id for s in cls.body if isinstance(s, ast.Assign) and isinstance(s.targets[0], ast.Name)]

    def methods(self, cls):
        return {f.name for f in cls.body if isinstance(f, ast.FunctionDef)}

    def test_configs_and_enums_match_the_published_package(self):
        pub, got = self.published("_values.py"), self.fake_classes()
        for name in ("EndpointConfig", "ReceiverConfig", "TrackEvent", "MediaObject", "ReceiverStats", "ReceiverTerminal"):
            self.assertEqual(self.fields(got[name]), self.fields(pub[name]), name)
        for name in ("Protocol", "Backend", "Version", "EndpointState", "TerminalReason", "OverflowPolicy", "TimeMode",
                     "WtProfile", "TrackEventKind", "TrackState", "PollOutcome", "ObjectStatus", "WaitResult", "Packaging", "MediaType"):
            self.assertEqual(self.members(got[name]), self.members(pub[name]), name)
        self.assertIn("flow_control", self.methods(pub["ReceiverConfig"]))

    def test_endpoint_and_receiver_entry_points_match(self):
        ep, rx, got = self.published("_endpoint.py")["Endpoint"], self.published("_receiver.py")["Receiver"], self.fake_classes()
        for used in ("connect", "wait", "close"):
            self.assertIn(used, self.methods(ep))
            self.assertIn(used, self.methods(got["Endpoint"]))
        for prop in ("state", "terminal"):
            self.assertIn(prop, self.methods(ep))
            self.assertIn(prop, self.methods(got["Endpoint"]))
        for used in ("attach", "poll_track", "poll_object", "wait", "subscribe", "track_state", "stats", "drained", "close", "terminal"):
            self.assertIn(used, self.methods(rx))
            self.assertIn(used, self.methods(got["Receiver"]))


# ------------------------------------------------------------------ 1-3. the child
class HostChild(unittest.TestCase):
    def setUp(self):
        self.runs = []

    def tearDown(self):
        for r in self.runs:
            r.close()

    def new(self, mode="happy"):
        r = Run(mode)
        self.runs.append(r)
        return r

    def assertRefusedBeforeConnect(self, r, result, calls, needle):
        rc, stdout = result.returncode, result.stdout
        self.assertNotEqual(rc, 0)
        self.assertEqual(stdout, "", "no control is ever written")
        self.assertIn(needle, result.stderr)
        self.assertEqual(calls_of(calls, "connect"), [], "refused before Endpoint.connect")

    def test_a_clean_run_writes_exactly_the_four_controls_and_exits_0(self):
        r = self.new()
        result, lines, calls = r.run()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(lines, CONTROLS, "stdout carries only the controls, once each, in order")
        self.assertNotIn("CTRL", result.stderr)
        self.assertEqual(Path(calls_of(calls, "import")[0]["file"]).resolve(), (r.package / "moq5" / "__init__.py").resolve(),
                         "moq5 came from the given package root")
        self.assertEqual(calls_of(calls, "connect"), [{
            "call": "connect", "url": "moqt://127.0.0.1:4443", "protocol": "RAW_QUIC", "backend": "PICOQUIC",
            "versions": ["DRAFT_18"], "sni": "localhost", "ca_file": str(r.ca), "wt_path": None, "handshake_timeout_us": 0}])
        self.assertEqual(calls_of(calls, "attach"), [{"call": "attach", "namespace": ["live", "cam1"], "overflow": "FLOW_CONTROL",
                                                      "auto_subscribe": False, "catalog_track": ""}])
        self.assertEqual(calls_of(calls, "subscribe"), [{"call": "subscribe", "track": "video"}])
        self.assertEqual([c["call"] for c in calls if c["call"].endswith("_close")], ["receiver_close", "endpoint_close"])

    def test_the_package_must_resolve_inside_the_given_root(self):
        r = self.new()
        other = self.new()
        swapped = Path(r.tmp.name) / "swapped pkg"
        swapped.mkdir()
        (swapped / "moq5").symlink_to(other.package / "moq5", target_is_directory=True)
        result, _lines, _calls = r.run(**{"--package": str(swapped)})
        self.assertRefusedBeforeConnect(r, result, fake.read_calls(other.calls), "package:")
        result, _lines, calls = r.run(**{"--package": str(Path(r.tmp.name) / "no such pkg")})
        self.assertRefusedBeforeConnect(r, result, calls, "package:")

    def test_the_objects_file_is_loaded_and_checked_before_connecting(self):
        r = self.new()
        blob = r.objects.read_bytes()
        r.objects.write_bytes(blob[:-3])
        result, _lines, calls = r.run()
        self.assertRefusedBeforeConnect(r, result, calls, "objects:")
        self.assertIn("runs past the end of the file", result.stderr, "the loader's own reason, not a substitute")
        result, _lines, calls = r.run(**{"--objects": str(r.run_dir / "missing.bin")})
        self.assertRefusedBeforeConnect(r, result, calls, "objects:")
        self.assertIn("No such file", result.stderr)

    def test_trust_inputs_are_explicit_and_checked(self):
        r = self.new()
        result, _lines, calls = r.run(**{"--ca": str(r.run_dir / "no ca.pem")})
        self.assertRefusedBeforeConnect(r, result, calls, "ca:")
        result, _lines, calls = r.run(**{"--sni": ""})
        self.assertRefusedBeforeConnect(r, result, calls, "sni:")
        result, _lines, calls = r.run(**{"--ca": None})
        self.assertRefusedBeforeConnect(r, result, calls, "--ca")
        result, _lines, calls = r.run("--insecure")
        self.assertRefusedBeforeConnect(r, result, calls, "--insecure")

    def test_only_the_declared_loopback_raw_listener_is_accepted(self):
        r = self.new()
        for url in ("https://127.0.0.1:4443/moq", "moqt://example.com:4443", "moqt://10.0.2.2:4443", "moqt://127.0.0.1"):
            result, _lines, calls = r.run(**{"--url": url})
            self.assertRefusedBeforeConnect(r, result, calls, "url:")

    def test_a_refused_attach_closes_the_acquired_endpoint(self):
        r = self.new("attach_refused")
        result, lines, calls = r.run()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(lines, [])
        self.assertIn("attach:", result.stderr)
        self.assertEqual([c["call"] for c in calls if c["call"].endswith("_close")], ["endpoint_close"])

    def test_an_endpoint_that_never_establishes_is_a_bounded_named_failure_that_still_closes_it(self):
        r = self.new("never_established")
        result, lines, calls = r.run(**{"--establish-s": "0.5"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(lines, [])
        self.assertIn("not established within 0.5s", result.stderr)
        self.assertEqual(calls_of(calls, "attach"), [], "no receiver on an unestablished endpoint")
        self.assertEqual([c["call"] for c in calls if c["call"].endswith("_close")], ["endpoint_close"])

    def test_package_output_never_reaches_the_control_stream(self):
        r = self.new("stdout_noise")
        result, lines, _calls = r.run()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(lines, CONTROLS)
        self.assertIn("fake moq5: connecting (noise on stdout)", result.stderr)

    def test_a_named_failure_stops_the_controls_exits_nonzero_and_still_closes(self):
        r = self.new("media_corrupt")
        result, lines, calls = r.run()
        self.assertEqual(result.returncode, 1)
        self.assertEqual(lines, ["CTRL go"], "no control after the failure")
        self.assertIn("ack-all:", result.stderr)
        self.assertIn("object 12: payload differs", result.stderr)
        self.assertEqual([c["call"] for c in calls if c["call"].endswith("_close")], ["receiver_close", "endpoint_close"])

    def test_a_fatal_receiver_is_named_with_its_code(self):
        r = self.new("fatal_after_go")
        result, lines, _calls = r.run()
        self.assertEqual(result.returncode, 1)
        self.assertEqual(lines, ["CTRL go"])
        self.assertIn("fatal_code 11", result.stderr)

    def test_the_objects_file_must_declare_exactly_the_recipes_60(self):
        r = self.new()
        for n in (59, 61):
            write_objects(r.objects, n=n)
            result, _lines, calls = r.run()
            self.assertRefusedBeforeConnect(r, result, calls, "objects:")
            self.assertIn(f"{n} records", result.stderr)
            self.assertIn("declares 60", result.stderr)

    def test_bounds_are_finite_positive_and_within_the_run_bound(self):
        r = self.new()
        for flag in ("--establish-s", "--deadline-s"):
            for bad in ("inf", "-inf", "nan", "abc", "0", "-1", "600"):
                result, _lines, calls = r.run(**{flag: bad})
                self.assertRefusedBeforeConnect(r, result, calls, flag.lstrip("-") + ":")
        result, _lines, calls = r.run(**{"--establish-s": "300", "--deadline-s": "300"})
        self.assertRefusedBeforeConnect(r, result, calls, "545")

    def test_only_a_bare_loopback_host_and_a_valid_port_are_accepted(self):
        r = self.new()
        for url in ("moqt://127.0.0.1:0", "moqt://127.0.0.1:65536", "moqt://127.0.0.1:99999", "moqt://127.0.0.1:4443/x",
                    "moqt://127.0.0.1:4443?a=1", "moqt://127.0.0.1:4443#f", "moqt://u@127.0.0.1:4443", "moqt://127.0.0.1:44x3"):
            result, _lines, calls = r.run(**{"--url": url})
            self.assertRefusedBeforeConnect(r, result, calls, "url:")
        for url in ("moqt://localhost:4443", "moqt://[::1]:4443", "moqt://127.0.0.1:65535", "moqt://127.0.0.1:1"):
            ok = self.new()
            result, lines, calls = ok.run(**{"--url": url})
            self.assertEqual((result.returncode, lines), (0, CONTROLS), url + ": " + result.stderr)
            self.assertEqual(calls_of(calls, "connect")[0]["url"], url)

    def test_a_never_active_track_is_bounded_by_the_explicit_deadline(self):
        r = self.new("never_active")
        result, lines, _calls = r.run(**{"--deadline-s": "0.5"})
        self.assertEqual(result.returncode, 1)
        self.assertEqual(lines, [])
        self.assertIn("go:", result.stderr)


class ImportIsolation(unittest.TestCase):
    def test_importing_this_module_changes_nothing_in_the_importing_interpreter(self):
        probe = ("import socket, sys; sys.path.insert(0, sys.argv[1]); "
                 "before = (socket.socket.connect, socket.socket.bind); import test_host_cli; "
                 "after = (socket.socket.connect, socket.socket.bind); "
                 "sys.exit(0 if before == after else 1)")
        r = subprocess.run([sys.executable, "-I", "-B", "-c", probe, str(HERE)], capture_output=True, text=True, timeout=60)
        self.assertEqual(r.returncode, 0, "importing test_host_cli must not patch socket: " + r.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
