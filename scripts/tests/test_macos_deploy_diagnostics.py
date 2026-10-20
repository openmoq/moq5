#!/usr/bin/env python3
"""Fake-command checks only; no configure, compiler, provider, or network use."""

import contextlib
import io
import os
from pathlib import Path
import re
import runpy
import subprocess
import tempfile
import unittest
from unittest.mock import patch


SCRIPTS = Path(__file__).resolve().parents[1]


class DiagnosticTests(unittest.TestCase):
    def test_diagnostic_markers_not_test_names(self):
        lane = str(SCRIPTS / "check_macos_deploy_lane.sh")
        clean = (
            '\u25c7 Test "Reasons and error domains are independent, preserving future values and zero" started.\n',
            '\u2714 Test "Reasons and error domains are independent, preserving future values and zero" passed after 0.205 seconds.\n',
            "All tests passed; error handling exercised.\n",
        )
        diagnostics = (
            "file.swift:12:3: error: injected\n",
            "file.c:12:3: fatal error: injected\n",
            "file.c:12:3: warning: injected\n",
            "ld: warning: injected\n",
            "WARNING injected\n",
            "CMake Error at CMakeLists.txt:12 (message):\n  injected\n",
            "CMake Error: injected\n",
            "CMake Warning (dev) at CMakeLists.txt:12 (message):\n  injected\n",
            "error: injected\n",
            "file.c(12): error C2065: injected\n",
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "build.log"
            for line, expected in [(s, 0) for s in clean] + [(s, 1) for s in diagnostics]:
                with self.subTest(line=line):
                    path.write_text(line)
                    result = subprocess.run(
                        ["bash", lane, "diagnostic-logs", str(path)],
                        capture_output=True, text=True, timeout=5,
                    )
                    self.assertEqual(result.returncode, expected, result.stderr)
                    if expected:
                        self.assertIn("diagnostic match", result.stderr)
                    else:
                        self.assertEqual(result.stderr, "")
            missing = subprocess.run(
                ["bash", lane, "diagnostic-logs", str(path.with_name("absent.log"))],
                capture_output=True, text=True, timeout=5,
            )
            self.assertEqual(missing.returncode, 2)
            self.assertIn("diagnostic scanner status 2", missing.stderr)

    def test_workflow_pipeline_status(self):
        workflow = (SCRIPTS.parent / ".github/workflows/ci.yml").read_text()
        job = workflow.split("\n  swift-service-binding:\n", 1)[1].split("\n  swift-service-ios:\n", 1)[0]
        self.assertIn("    defaults:\n      run:\n        shell: bash\n", job)
        self.assertIn("run: python3 scripts/tests/test_macos_deploy_diagnostics.py", job)
        with tempfile.TemporaryDirectory() as tmp:
            # GitHub's unspecified Unix shell versus explicit shell: bash.
            # No diagnostic keyword can substitute for the producer status.
            for flags, expected in ((["-e"], 0), (["-e", "-o", "pipefail"], 17)):
                result = subprocess.run(
                    ["bash", "--noprofile", "--norc", *flags, "-c",
                     '(printf "producer stopped\\n"; exit 17) | tee "$1"',
                     "test", str(Path(tmp) / "pipeline.log")],
                    capture_output=True, text=True, timeout=5,
                )
                self.assertEqual(result.returncode, expected)
                self.assertEqual(result.stdout, "producer stopped\n")
                self.assertEqual(result.stderr, "")

    def test_probe_log_scanner(self):
        lane = str(SCRIPTS / "check_macos_deploy_lane.sh")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logs = root / "CMakeFiles"
            logs.mkdir()

            def run(mode, path, env=None):
                return subprocess.run(["bash", lane, mode, str(path)],
                                      capture_output=True, text=True, timeout=5, env=env)

            missing = run("probe-logs", logs)
            self.assertEqual(missing.returncode, 1)
            self.assertIn("no CMake probe logs", missing.stderr)
            # Either CMake generation is sufficient; absent alternatives are
            # legitimate. Expected failed feature probes are not warnings.
            for name in ("CMakeConfigureLog.yaml", "CMakeOutput.log", "CMakeError.log"):
                probe = logs / name
                probe.write_text("error: expected failed feature probe\n")
                result = run("probe-logs", logs)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stderr, "")
                probe.write_text("warning: injected probe\n")
                result = run("probe-logs", logs)
                self.assertEqual(result.returncode, 1)
                self.assertIn("warning: injected probe", result.stderr)
                probe.unlink()

            probe = logs / "CMakeConfigureLog.yaml"
            probe.write_text("clean probe\n")
            fake_bin = root / "bin"
            fake_bin.mkdir()
            fake_grep = fake_bin / "grep"
            fake_grep.write_text("#!/bin/sh\nprintf 'scanner unavailable\\n' >&2\nexit 2\n")
            fake_grep.chmod(0o755)
            env = dict(os.environ, PATH=str(fake_bin) + os.pathsep + os.environ["PATH"])
            for mode, path in (("probe-logs", logs), ("diagnostic-logs", probe)):
                result = run(mode, path, env)
                self.assertEqual(result.returncode, 2)
                self.assertIn("scanner unavailable", result.stderr)
                self.assertIn("diagnostic scanner status 2", result.stderr)
            # Original conditional conflated grep error 2 with clean status 1.
            old = subprocess.run(["bash", "-e", "-c", 'if grep pattern "$1"; then exit 1; fi',
                                  "test", str(probe)], env=env,
                                 capture_output=True, text=True, timeout=5)
            self.assertEqual(old.returncode, 0)
            self.assertEqual(old.stderr, "scanner unavailable\n")
            # Original optional-log loop also accepted an entirely missing set.
            old_missing = subprocess.run(
                ["bash", "-e", "-c", 'for p in "$1"/absent; do if [ -f "$p" ] && grep warning "$p"; then exit 1; fi; done',
                 "test", str(logs)], capture_output=True, text=True, timeout=5)
            self.assertEqual(old_missing.returncode, 0)
            self.assertEqual(old_missing.stderr, "")

    def test_shell_capture_and_status(self):
        rows = (
            ("printf 'install progress\\n'; printf 'progress\\n' >&2", 0, 0),
            ("printf 'warning: injected\\n'", 1, 0),
            ("printf 'nm: warning: injected\\n' >&2", 1, 0),
            ("printf 'failure detail\\n' >&2; exit 17", 17, 17),
        )
        for name in ("build_macos_openssl.sh", "check_macos_deploy_lane.sh"):
            source = (SCRIPTS / name).read_text()
            match = re.search(r"^run_logged\(\) \{\n.*?^\}", source, re.M | re.S)
            self.assertIsNotNone(match)
            script_rows = rows
            if name == "check_macos_deploy_lane.sh":
                script_rows += (
                    ("printf 'All tests passed; error handling exercised.\\n'", 0, 0),
                    ("printf 'error: injected\\n' >&2", 1, 0),
                    ("printf 'CMake Error at CMakeLists.txt:12 (message):\\n'", 1, 0),
                    ("printf 'file.c(12): error C2065: injected\\n'", 1, 0),
                )
            for command, expected, command_status in script_rows:
                with self.subTest(script=name, command=command), tempfile.TemporaryDirectory() as tmp:
                    base = Path(tmp) / "probe"
                    arg = "probe" if name == "build_macos_openssl.sh" else str(base)
                    script = (
                        'set -euo pipefail\nroot=$1\n'
                        'log() { printf "%s\\n" "$*" >&2; }\n'
                        'die() { log "$*"; exit 1; }\n'
                        + match.group() + '\n'
                        # Exercise the conditional-call context too: errexit
                        # must not be responsible for status propagation.
                        + 'run_logged "$2" bash -c "$3" || exit $?\n'
                    )
                    result = subprocess.run(
                        ["bash", "-c", script, "test", tmp, arg, command],
                        capture_output=True, text=True, timeout=5,
                    )
                    self.assertEqual(result.returncode, expected, result.stderr)
                    self.assertEqual(base.with_suffix(".status").read_text(), f"{command_status}\n")
                    stdout = base.with_suffix(".stdout") if name == "build_macos_openssl.sh" else base
                    for stream in (stdout, base.with_suffix(".stderr")):
                        self.assertIn(stream.read_text(), result.stderr)

    def test_ar_diagnostics(self):
        audit = runpy.run_path(str(SCRIPTS / "macho_deploy_audit.py"))
        for status, stderr, expected in (
            (0, "", 1),
            (0, "ar: warning: injected\n", None),
            (17, "failure detail\n", None),
        ):
            with self.subTest(status=status, stderr=stderr):
                result = subprocess.CompletedProcess(["ar"], status, "member.o\n", stderr)
                capture = io.StringIO()
                with patch.object(subprocess, "run", return_value=result) as run:
                    with contextlib.redirect_stderr(capture):
                        self.assertEqual(audit["ar_t_count"]("fixture.a"), expected)
                run.assert_called_once_with(["ar", "-t", "fixture.a"], capture_output=True, text=True)
                self.assertIn(f"status={status}", capture.getvalue())
                self.assertIn(result.stdout, capture.getvalue())
                self.assertIn(stderr, capture.getvalue())


if __name__ == "__main__":
    unittest.main()
