"""Negative controls for installed_smoke.py: the installed-artifact gate must
refuse a wheel installation whose receiver module is missing, unrecorded, or
substituted.

Each control installs the wheel into its own fresh venv with pip (offline,
no dependencies), mutates exactly one declared receiver entry of that
installation, and runs the smoke with -I -W error from a directory that is
not a source tree. An untouched installation must pass first. Every command
is bounded and leaves a raw receipt (argv, rc, stdout, stderr, timeout) in
<workdir>/receipts.jsonl; any warning diagnostic on either stream fails the
control that produced it, even alongside the expected exit code and text.
No socket is opened. `--self-test` runs the offline tests of the
classification and mutation-path rules against stubs and scratch files.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from dataclasses import asdict, dataclass
from pathlib import Path

COMMAND_TIMEOUT_S = 300.0
WARNING = re.compile(r"warning", re.IGNORECASE)
PASSED = "smoke passed."
RECEIVER_ENTRY = "moq5/_receiver.py"
SENDER_ENTRY = "moq5/_sender.py"
VALUES_ENTRY = "moq5/_values.py"


class ControlError(Exception):
    """A control or its setup did not produce exactly the required evidence."""


@dataclass(frozen=True)
class Receipt:
    label: str
    argv: tuple[str, ...]
    cwd: str
    timeout_s: float
    rc: int | None
    stdout: str
    stderr: str
    seconds: float


def _run(argv: tuple[str, ...], cwd: Path, timeout_s: float) -> tuple[int | None, str, str]:
    """The one subprocess path. A timeout kills the child and is reported as
    rc None with the partial streams as text."""
    try:
        completed = subprocess.run(
            argv, cwd=cwd, capture_output=True, text=True, timeout=timeout_s, check=False
        )
    except subprocess.TimeoutExpired as expired:
        return None, _text(expired.stdout), _text(expired.stderr)
    return completed.returncode, completed.stdout, completed.stderr


def _text(stream: object) -> str:
    if stream is None:
        return ""
    if isinstance(stream, bytes):
        return stream.decode("utf-8", "replace")
    return str(stream)


class Recorder:
    def __init__(self, path: Path | None) -> None:
        self.path = path
        self.receipts: list[Receipt] = []

    def command(self, label: str, argv: list[str], cwd: Path,
                timeout_s: float = COMMAND_TIMEOUT_S) -> Receipt:
        started = time.monotonic()
        rc, out, err = _run(tuple(argv), cwd, timeout_s)
        receipt = Receipt(label, tuple(argv), str(cwd), timeout_s, rc, out, err,
                          round(time.monotonic() - started, 3))
        self.receipts.append(receipt)
        if self.path is not None:
            with self.path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(asdict(receipt)) + "\n")
        return receipt


def diagnostics(receipt: Receipt) -> list[str]:
    """Every warning line on either stream; the streams are never filtered."""
    return [line for line in (receipt.stdout + "\n" + receipt.stderr).splitlines()
            if WARNING.search(line)]


def check_setup(receipt: Receipt) -> None:
    if receipt.rc != 0:
        raise ControlError(f"{receipt.label}: rc={receipt.rc}, stderr={receipt.stderr.strip()!r}")
    if diagnostics(receipt):
        raise ControlError(f"{receipt.label}: warning diagnostic: {diagnostics(receipt)!r}")


def check_positive(receipt: Receipt) -> None:
    if receipt.rc != 0:
        raise ControlError(f"{receipt.label}: rc={receipt.rc}, stderr={receipt.stderr.strip()!r}")
    if receipt.stderr.strip():
        raise ControlError(f"{receipt.label}: stderr not empty: {receipt.stderr.strip()!r}")
    if PASSED not in receipt.stdout:
        raise ControlError(f"{receipt.label}: no {PASSED!r} marker")
    if diagnostics(receipt):
        raise ControlError(f"{receipt.label}: warning diagnostic: {diagnostics(receipt)!r}")


def check_refusal(receipt: Receipt, expected_rc: int, expected_text: str) -> None:
    if receipt.rc != expected_rc:
        raise ControlError(f"{receipt.label}: rc={receipt.rc}, expected {expected_rc}")
    if expected_text not in receipt.stderr:
        raise ControlError(f"{receipt.label}: required diagnostic missing: {expected_text!r}")
    if PASSED in receipt.stdout:
        raise ControlError(f"{receipt.label}: refused install still reports {PASSED!r}")
    if diagnostics(receipt):
        raise ControlError(f"{receipt.label}: warning diagnostic: {diagnostics(receipt)!r}")


def make_venv(recorder: Recorder, base_python: Path, wheel: Path, venv: Path, cwd: Path) -> Path:
    check_setup(recorder.command(
        "venv", [str(base_python), "-I", "-m", "venv", str(venv)], cwd))
    python = venv / "bin" / "python"
    check_setup(recorder.command(
        "pip-install",
        [str(python), "-I", "-m", "pip", "install", "--no-index", "--no-deps",
         "--no-cache-dir", "--disable-pip-version-check", str(wheel)],
        cwd,
    ))
    return python


def site_packages(recorder: Recorder, python: Path, venv: Path, cwd: Path) -> Path:
    """The venv's purelib directory: one nonempty absolute line from a
    successful lookup, resolving inside the venv this driver created."""
    receipt = recorder.command(
        "site-lookup",
        [str(python), "-I", "-c", "import sysconfig; print(sysconfig.get_paths()['purelib'])"],
        cwd,
    )
    check_setup(receipt)
    lines = receipt.stdout.splitlines()
    if len(lines) != 1 or not lines[0].strip():
        raise ControlError(f"site lookup produced {len(lines)} lines, expected one path")
    site = Path(lines[0].strip())
    if not site.is_absolute():
        raise ControlError(f"site lookup produced a relative path: {site}")
    root = venv.resolve(strict=True)
    resolved = site.resolve()
    if not resolved.is_relative_to(root) or not resolved.is_dir():
        raise ControlError(f"site path {site} is not a directory inside {root}")
    return resolved


def mutation_target(site: Path, venv: Path, relative: str) -> Path:
    """An existing regular file of THIS installation, addressed without any
    symlink and resolving inside the driver-created venv."""
    root = venv.resolve(strict=True)
    base = site.resolve(strict=True)
    if not base.is_relative_to(root):
        raise ControlError(f"site {base} is outside the installation {root}")
    parts = Path(relative).parts
    if Path(relative).is_absolute() or ".." in parts or not parts:
        raise ControlError(f"mutation target escapes the installation: {relative}")
    path = base
    for part in parts:
        path = path / part
        if path.is_symlink():
            raise ControlError(f"mutation target passes through a symlink: {path}")
    if not path.is_file():
        raise ControlError(f"mutation target is not a regular file: {path}")
    resolved = path.resolve(strict=True)
    if not resolved.is_relative_to(root):
        raise ControlError(f"mutation target escapes the installation: {resolved}")
    return resolved


def record_path(site: Path, venv: Path) -> Path:
    records = sorted(site.glob("moq5-*.dist-info/RECORD"))
    if len(records) != 1:
        raise ControlError(f"expected one moq5 RECORD, found {len(records)}")
    return mutation_target(site, venv, str(records[0].relative_to(site)))


def omit_module(site: Path, venv: Path) -> None:
    mutation_target(site, venv, RECEIVER_ENTRY).unlink()


def omit_record(site: Path, venv: Path) -> None:
    path = record_path(site, venv)
    with path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.reader(stream))
    matching = [row for row in rows if row and row[0] == RECEIVER_ENTRY]
    if len(matching) != 1:
        raise ControlError(f"RECORD lists {RECEIVER_ENTRY} {len(matching)} times, expected once")
    kept = [row for row in rows if row is not matching[0]]
    with path.open("w", newline="", encoding="utf-8") as stream:
        csv.writer(stream, lineterminator="\n").writerows(kept)


def substitute_module(site: Path, venv: Path) -> None:
    path = mutation_target(site, venv, RECEIVER_ENTRY)
    path.write_text(path.read_text(encoding="utf-8") + "\nSUBSTITUTED = True\n", encoding="utf-8")


def omit_sender_module(site: Path, venv: Path) -> None:
    mutation_target(site, venv, SENDER_ENTRY).unlink()


def substitute_sender_module(site: Path, venv: Path) -> None:
    path = mutation_target(site, venv, SENDER_ENTRY)
    path.write_text(path.read_text(encoding="utf-8") + "\nSUBSTITUTED = True\n",
                    encoding="utf-8")


def wrong_scalar_constant(site: Path, venv: Path) -> None:
    """Change the declared scalar export to a wrong value. The artifact hash
    check sees the edited file first, which is the point: a scalar cannot be
    swapped without the artifact gate noticing."""
    path = mutation_target(site, venv, VALUES_ENTRY)
    text = path.read_text(encoding="utf-8")
    marker = "CATALOG_REFRESH_DISABLED = _UINT64_MAX"
    if marker not in text:
        raise ControlError("the declared scalar export moved")
    path.write_text(text.replace(marker, "CATALOG_REFRESH_DISABLED = 1"),
                    encoding="utf-8")


CONTROLS = (
    ("receiver_module_omitted", omit_module, 1, "No module named 'moq5._receiver'"),
    ("receiver_record_omitted", omit_record, 1, f"missing installed file: {RECEIVER_ENTRY}"),
    ("receiver_module_substituted", substitute_module, 1,
     f"installed file hash mismatch: {RECEIVER_ENTRY}"),
    ("sender_module_omitted", omit_sender_module, 1, "No module named 'moq5._sender'"),
    ("sender_module_substituted", substitute_sender_module, 1,
     f"installed file hash mismatch: {SENDER_ENTRY}"),
    ("wrong_scalar_constant", wrong_scalar_constant, 1,
     f"installed file hash mismatch: {VALUES_ENTRY}"),
)


# A direct check of the SCALAR rule itself. The integrity negative above edits
# the file and is caught by the RECORD hash, which says nothing about the
# type/value/origin assertion. This one leaves file provenance intact and
# changes only the exported value in memory, so the scalar assertion is what
# must refuse.
SCALAR_PROBE = """\
import sys
sys.path.insert(0, {smoke_dir!r})
import installed_smoke as smoke
import moq5, moq5._values

moq5._values.CATALOG_REFRESH_DISABLED = 1
moq5.CATALOG_REFRESH_DISABLED = 1
try:
    smoke.check_public_origin()
except AssertionError as error:
    text = str(error)
    if "CATALOG_REFRESH_DISABLED is 1, not" in text:
        print("refused:", text, file=sys.stderr)
        raise SystemExit(0)
    print("wrong diagnostic:", text, file=sys.stderr)
    raise SystemExit(2)
print("the wrong scalar was accepted", file=sys.stderr)
raise SystemExit(3)
"""


# A direct check of the DECLARED public-name rule for the write surface. The
# integrity controls edit files and are caught by the RECORD hash, which says
# nothing about the export declaration. This one leaves provenance intact and
# removes only an exported name in memory, so the declaration is what refuses.
WRITE_EXPORT_PROBE = """\
import sys
sys.path.insert(0, {smoke_dir!r})
import installed_smoke as smoke
import moq5

del moq5.WriteOutcome
moq5.__all__ = [n for n in moq5.__all__ if n != "WriteOutcome"]
try:
    smoke.check_public_origin()
except AssertionError as error:
    text = str(error)
    if "WriteOutcome" in text:
        print("refused:", text, file=sys.stderr)
        raise SystemExit(0)
    print("wrong diagnostic:", text, file=sys.stderr)
    raise SystemExit(2)
print("a missing public write name was accepted", file=sys.stderr)
raise SystemExit(3)
"""


def smoke_argv(python: Path, smoke_script: Path, artifact: Path, args: argparse.Namespace) -> list[str]:
    argv = [str(python), "-I", "-W", "error", str(smoke_script),
            "--artifact", str(artifact),
            "--expected-package-version", args.expected_package_version,
            "--expected-sdk-version", args.expected_sdk_version]
    if args.skip_backendless_checks:
        argv.append("--skip-backendless-checks")
    return argv


def run_controls(args: argparse.Namespace) -> int:
    wheel = args.wheel.resolve(strict=True)
    smoke_script = Path(__file__).resolve().with_name("installed_smoke.py")
    workdir = args.workdir.resolve()
    workdir.mkdir(parents=True, exist_ok=False)
    cwd = workdir / "cwd"
    cwd.mkdir()
    smoke_copy = workdir / "installed_smoke.py"
    shutil.copy2(smoke_script, smoke_copy)
    recorder = Recorder(workdir / "receipts.jsonl")
    outcomes: list[tuple[str, str]] = []

    def attempt(name: str, action) -> None:
        try:
            action()
        except ControlError as error:
            outcomes.append((name, f"FAIL {error}"))
        else:
            outcomes.append((name, "ok"))

    def positive() -> None:
        venv = workdir / "venv-positive"
        python = make_venv(recorder, args.python, wheel, venv, cwd)
        check_positive(recorder.command("smoke:positive", smoke_argv(python, smoke_copy, wheel, args), cwd))

    attempt("positive", positive)

    def scalar_rule() -> None:
        venv = workdir / "venv-scalar_rule"
        python = make_venv(recorder, args.python, wheel, venv, cwd)
        probe = workdir / "scalar_probe.py"
        probe.write_text(SCALAR_PROBE.format(smoke_dir=str(workdir)),
                         encoding="utf-8")
        receipt = recorder.command(
            "smoke:scalar_rule",
            [str(python), "-I", "-W", "error", str(probe)], cwd)
        check_refusal(receipt, 0, "CATALOG_REFRESH_DISABLED is 1, not")

    attempt("scalar_rule", scalar_rule)

    def write_export_rule() -> None:
        venv = workdir / "venv-write_export_rule"
        python = make_venv(recorder, args.python, wheel, venv, cwd)
        probe = workdir / "write_export_probe.py"
        probe.write_text(WRITE_EXPORT_PROBE.format(smoke_dir=str(workdir)),
                         encoding="utf-8")
        receipt = recorder.command(
            "smoke:write_export_rule",
            [str(python), "-I", "-W", "error", str(probe)], cwd)
        check_refusal(receipt, 0, "WriteOutcome")

    attempt("write_export_rule", write_export_rule)
    for name, mutate, expected_rc, expected_text in CONTROLS:
        def control(name=name, mutate=mutate, expected_rc=expected_rc, expected_text=expected_text) -> None:
            venv = workdir / f"venv-{name}"
            python = make_venv(recorder, args.python, wheel, venv, cwd)
            mutate(site_packages(recorder, python, venv, cwd), venv)
            receipt = recorder.command(f"smoke:{name}", smoke_argv(python, smoke_copy, wheel, args), cwd)
            check_refusal(receipt, expected_rc, expected_text)
        attempt(name, control)

    for name, outcome in outcomes:
        print(f"{name}: {outcome}")
    print(f"receipts: {len(recorder.receipts)} commands in {recorder.path}")
    return 1 if any(outcome != "ok" for _, outcome in outcomes) else 0


# --- offline self-tests ------------------------------------------------------

def _receipt(label: str = "x", rc: int | None = 0, stdout: str = "", stderr: str = "") -> Receipt:
    return Receipt(label, ("stub",), ".", 1.0, rc, stdout, stderr, 0.0)


class ClassificationTests(unittest.TestCase):
    def test_clean_setup_positive_and_refusal_pass(self) -> None:
        check_setup(_receipt(rc=0, stdout="Successfully installed moq5\n"))
        check_positive(_receipt(rc=0, stdout=f"Installed x: {PASSED}\n"))
        check_refusal(_receipt(rc=1, stderr="AssertionError: missing installed file: moq5/_receiver.py\n"),
                      1, "missing installed file: moq5/_receiver.py")

    def test_positive_stdout_warning_fails_despite_marker_and_rc_zero(self) -> None:
        receipt = _receipt(rc=0, stdout=f"DeprecationWarning: x\n{PASSED}\n")
        with self.assertRaisesRegex(ControlError, "warning diagnostic"):
            check_positive(receipt)

    def test_positive_requires_marker_and_empty_stderr(self) -> None:
        with self.assertRaisesRegex(ControlError, "marker"):
            check_positive(_receipt(rc=0, stdout="done\n"))
        with self.assertRaisesRegex(ControlError, "stderr not empty"):
            check_positive(_receipt(rc=0, stdout=PASSED, stderr="note\n"))
        with self.assertRaisesRegex(ControlError, "rc=None"):
            check_positive(_receipt(rc=None, stdout=PASSED))

    def test_refusal_stderr_warning_fails_despite_expected_text_and_rc(self) -> None:
        receipt = _receipt(rc=1, stderr="RuntimeWarning: y\nAssertionError: missing installed file: moq5/_receiver.py\n")
        with self.assertRaisesRegex(ControlError, "warning diagnostic"):
            check_refusal(receipt, 1, "missing installed file: moq5/_receiver.py")

    def test_refusal_requires_rc_text_and_no_pass_marker(self) -> None:
        with self.assertRaisesRegex(ControlError, "rc=0"):
            check_refusal(_receipt(rc=0, stderr="missing installed file: moq5/_receiver.py"), 1, "missing installed file: moq5/_receiver.py")
        with self.assertRaisesRegex(ControlError, "required diagnostic missing"):
            check_refusal(_receipt(rc=1, stderr="other"), 1, "missing installed file: moq5/_receiver.py")
        with self.assertRaisesRegex(ControlError, "still reports"):
            check_refusal(_receipt(rc=1, stdout=PASSED, stderr="missing installed file: moq5/_receiver.py"), 1, "missing installed file: moq5/_receiver.py")

    def test_setup_warning_fails_and_setup_rc_is_checked(self) -> None:
        with self.assertRaisesRegex(ControlError, "warning diagnostic"):
            check_setup(_receipt(rc=0, stderr="WARNING: pip is old\n"))
        with self.assertRaisesRegex(ControlError, "rc=1"):
            check_setup(_receipt(rc=1, stderr="boom\n"))

    def test_receipts_keep_argv_rc_streams_and_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            recorder = Recorder(Path(scratch) / "receipts.jsonl")
            receipt = recorder.command(
                "echo", [sys.executable, "-I", "-c", "import sys; print('out'); print('err', file=sys.stderr)"],
                Path(scratch), timeout_s=30.0)
            self.assertEqual((receipt.rc, receipt.stdout, receipt.stderr, receipt.timeout_s), (0, "out\n", "err\n", 30.0))
            stored = json.loads((Path(scratch) / "receipts.jsonl").read_text().splitlines()[0])
            self.assertEqual(stored["argv"], list(receipt.argv))
            self.assertEqual(stored["rc"], 0)

    def test_timed_out_command_is_rc_none_with_partial_text(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            recorder = Recorder(None)
            receipt = recorder.command(
                "sleep", [sys.executable, "-I", "-c", "import sys, time; print('partial', flush=True); time.sleep(30)"],
                Path(scratch), timeout_s=0.5)
            self.assertIsNone(receipt.rc)
            self.assertIn("partial", receipt.stdout)
            with self.assertRaisesRegex(ControlError, "rc=None"):
                check_setup(receipt)


class StubbedLookup:
    """Replaces the subprocess path for one test with declared outputs."""

    def __init__(self, rc: int | None, stdout: str, stderr: str = "") -> None:
        self.result = (rc, stdout, stderr)

    def __enter__(self) -> "StubbedLookup":
        self.saved = globals()["_run"]
        globals()["_run"] = lambda argv, cwd, timeout_s: self.result
        return self

    def __exit__(self, *exc: object) -> None:
        globals()["_run"] = self.saved


class MutationPathTests(unittest.TestCase):
    def setUp(self) -> None:
        self.scratch = Path(tempfile.mkdtemp(prefix="smoke-controls-"))
        self.venv = self.scratch / "venv"
        self.site = self.venv / "lib" / "python3.12" / "site-packages"
        (self.site / "moq5").mkdir(parents=True)
        (self.site / "moq5" / "_receiver.py").write_text("RECEIVER = 1\n")
        self.dist = self.site / "moq5-0.1.0.dev0.dist-info"
        self.dist.mkdir()
        (self.dist / "RECORD").write_text(
            "moq5/__init__.py,sha256=aaa,10\nmoq5/_receiver.py,sha256=bbb,20\n"
            "moq5-0.1.0.dev0.dist-info/RECORD,,\n")
        self.outside = self.scratch / "outside"
        self.outside.mkdir()
        (self.outside / "victim.py").write_text("VICTIM = 1\n")
        self.recorder = Recorder(None)

    def tearDown(self) -> None:
        shutil.rmtree(self.scratch)

    def lookup(self) -> Path:
        return site_packages(self.recorder, self.venv / "bin" / "python", self.venv, self.scratch)

    def test_owned_site_lookup_and_controls_work(self) -> None:
        with StubbedLookup(0, f"{self.site}\n"):
            self.assertEqual(self.lookup(), self.site.resolve())
        omit_record(self.site, self.venv)
        with (self.dist / "RECORD").open(newline="") as stream:
            rows = list(csv.reader(stream))
        self.assertEqual([row[0] for row in rows], ["moq5/__init__.py", "moq5-0.1.0.dev0.dist-info/RECORD"])
        substitute_module(self.site, self.venv)
        self.assertTrue((self.site / "moq5" / "_receiver.py").read_text().endswith("SUBSTITUTED = True\n"))
        omit_module(self.site, self.venv)
        self.assertFalse((self.site / "moq5" / "_receiver.py").exists())

    def test_failed_empty_relative_multiline_or_foreign_lookup_is_refused(self) -> None:
        for rc, stdout, stderr, message in (
            (1, "", "Traceback\n", "rc=1"),
            (0, "", "", "expected one path"),
            (0, "lib/site-packages\n", "", "relative path"),
            (0, f"{self.site}\n{self.site}\n", "", "expected one path"),
            (0, f"{self.outside}\n", "", "not a directory inside"),
            (0, f"{self.site}\n", "warning: odd\n", "warning diagnostic"),
            (None, "", "", "rc=None"),
        ):
            with StubbedLookup(rc, stdout, stderr), self.assertRaisesRegex(ControlError, message):
                self.lookup()

    def test_escaping_symlink_or_outside_target_is_refused_before_mutation(self) -> None:
        (self.site / "moq5" / "_receiver.py").unlink()
        (self.site / "moq5" / "_receiver.py").symlink_to(self.outside / "victim.py")
        for action in (omit_module, substitute_module):
            with self.assertRaisesRegex(ControlError, "symlink"):
                action(self.site, self.venv)
        self.assertEqual((self.outside / "victim.py").read_text(), "VICTIM = 1\n")
        for relative in ("../../../../outside/victim.py", str(self.outside / "victim.py"), ""):
            with self.assertRaisesRegex(ControlError, "escapes"):
                mutation_target(self.site, self.venv, relative)
        with self.assertRaisesRegex(ControlError, "outside the installation"):
            mutation_target(self.outside, self.venv, "victim.py")
        self.assertEqual((self.outside / "victim.py").read_text(), "VICTIM = 1\n")

    def test_symlinked_record_directory_is_refused(self) -> None:
        shutil.move(str(self.dist), str(self.outside / "dist-info"))
        self.dist.symlink_to(self.outside / "dist-info")
        with self.assertRaisesRegex(ControlError, "symlink"):
            omit_record(self.site, self.venv)
        self.assertIn("moq5/_receiver.py,sha256=bbb,20", (self.outside / "dist-info" / "RECORD").read_text())

    def test_record_must_list_receiver_exactly_once(self) -> None:
        (self.dist / "RECORD").write_text("moq5/__init__.py,sha256=aaa,10\n")
        with self.assertRaisesRegex(ControlError, "0 times"):
            omit_record(self.site, self.venv)
        (self.dist / "RECORD").write_text("moq5/_receiver.py,sha256=b,1\nmoq5/_receiver.py,sha256=c,2\n")
        with self.assertRaisesRegex(ControlError, "2 times"):
            omit_record(self.site, self.venv)
        self.assertIn("sha256=c", (self.dist / "RECORD").read_text())

    def test_quoted_record_rows_survive_csv_round_trip(self) -> None:
        (self.dist / "RECORD").write_text(
            '"moq5/odd,name.py",sha256=q,5\nmoq5/_receiver.py,sha256=bbb,20\n')
        omit_record(self.site, self.venv)
        self.assertEqual((self.dist / "RECORD").read_text(), '"moq5/odd,name.py",sha256=q,5\n')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="run the offline driver tests only")
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--python", type=Path, help="base CPython 3.12")
    parser.add_argument("--workdir", type=Path, help="fresh directory, not a source tree")
    parser.add_argument("--expected-package-version")
    parser.add_argument("--expected-sdk-version")
    parser.add_argument("--skip-backendless-checks", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        result = unittest.main(module=__name__, argv=[sys.argv[0], "-v"], exit=False).result
        return 0 if result.wasSuccessful() and result.testsRun else 1
    missing = [name for name in ("wheel", "python", "workdir", "expected_package_version", "expected_sdk_version")
               if getattr(args, name) is None]
    if missing:
        parser.error("missing: " + ", ".join(missing))
    return run_controls(args)


if __name__ == "__main__":
    sys.exit(main())
