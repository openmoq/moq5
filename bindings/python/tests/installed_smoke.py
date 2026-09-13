"""Check an installed foundation artifact against a real, adapter-free SDK.

Run with CPython 3.12 -I -W error in a fresh venv, outside the source tree.
This checks installation and configuration refusals, not live handshakes.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.metadata as metadata
import json
import re
import subprocess
import sys
from email.parser import Parser
from pathlib import Path
from urllib.parse import unquote, urlsplit


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def check_artifact(
    artifact: Path, expected_version: str, package_file: Path, native_file: Path
) -> None:
    distribution = metadata.distribution("moq5")
    require(distribution.metadata["Name"] == "moq5", "wrong distribution name")
    require(distribution.version == expected_version, "wrong package version")
    direct_url = json.loads(distribution.read_text("direct_url.json") or "{}")
    origin = urlsplit(direct_url.get("url", ""))
    require(origin.scheme == "file" and not origin.netloc, "not a local artifact")
    require(Path(unquote(origin.path)).resolve() == artifact, "wrong artifact path")
    with artifact.open("rb") as stream:
        artifact_hash = hashlib.file_digest(stream, "sha256").hexdigest()
    recorded_hash = direct_url.get("archive_info", {}).get("hashes", {}).get("sha256")
    require(recorded_hash == artifact_hash, "installed artifact SHA-256 mismatch")

    wheel = Parser().parsestr(distribution.read_text("WHEEL") or "")
    tags = wheel.get_all("Tag", [])
    require(bool(tags), "missing wheel compatibility tags")
    require(
        all(tag.startswith("cp312-abi3-") and not tag.endswith("-any") for tag in tags),
        f"unexpected wheel tags: {tags}",
    )
    require(wheel.get("Root-Is-Purelib") == "false", "native wheel marked pure")
    files = {str(entry): entry for entry in distribution.files or ()}
    require(bool(files), "missing installed file records")
    require(
        not any(
            marker in name
            for name in files
            for marker in ("fake_service", "test-package", "_native_fixture")
        ),
        "private fixture files were installed",
    )
    prefix = Path(sys.prefix).resolve()
    for relative in (
        "moq5/__init__.py",
        "moq5/_endpoint.py",
        "moq5/_values.py",
        "moq5/py.typed",
        "moq5/_native.abi3.so",
    ):
        require(relative in files, f"missing installed file: {relative}")
        entry = files[relative]
        installed = Path(distribution.locate_file(entry)).resolve()
        require(installed.is_relative_to(prefix), f"file outside venv: {installed}")
        require(entry.hash is not None, f"missing file hash: {relative}")
        require(entry.hash.mode == "sha256", f"unexpected hash algorithm: {relative}")
        with installed.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").digest()
        encoded = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
        require(
            encoded == entry.hash.value, f"installed file hash mismatch: {relative}"
        )
    require(
        package_file == Path(distribution.locate_file("moq5/__init__.py")).resolve(),
        "moq5 imported from outside its installed distribution",
    )
    require(
        native_file == Path(distribution.locate_file("moq5/_native.abi3.so")).resolve(),
        "native extension imported from outside its installed distribution",
    )


def check_loader_paths(native_file: Path) -> None:
    if sys.platform == "darwin":
        command = ["otool", "-l", str(native_file)]
        pattern = r"\bcmd LC_RPATH\b"
    elif sys.platform.startswith("linux"):
        command = ["readelf", "-d", str(native_file)]
        pattern = r"\((?:RPATH|RUNPATH)\)"
    else:
        raise RuntimeError("this foundation smoke covers Linux and macOS only")
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    require(not result.stderr.strip(), f"loader inspection diagnostic: {result.stderr}")
    require(
        re.search(pattern, result.stdout) is None, "artifact contains a loader RPATH"
    )


def expect_native_error(url: str, expected_code: int, versions: tuple[int, ...] = ()) -> None:
    from moq5 import Endpoint, EndpointConfig, MoqError, _native

    try:
        with Endpoint.connect(EndpointConfig(url=url, versions=versions)):
            pass
    except MoqError as error:
        require(type(error.code) is int, "native result lost its integer type")
        require(error.code == expected_code, f"wrong native result: {error}")
        require(type(error.operation) is str, "native operation is not a string")
        require(error.operation == "connect", "failure did not come from connect")
        require(isinstance(error.__cause__, _native.Error), "native cause was lost")
        require(error.__cause__.args[0] == expected_code, "native code was changed")
    else:
        raise AssertionError(f"expected native connect refusal for {url}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--expected-package-version", required=True)
    parser.add_argument("--expected-sdk-version", required=True)
    parser.add_argument(
        "--skip-backendless-checks",
        action="store_true",
        help="skip valid-URL refusal assertions for an external SDK with adapters",
    )
    args = parser.parse_args()
    require(sys.implementation.name == "cpython", "CPython is required")
    require(sys.version_info[:2] == (3, 12), "this smoke targets CPython 3.12")
    require(sys.flags.isolated == 1, "run with -I")
    require("error" in sys.warnoptions, "run with -W error")
    require(sys.prefix != sys.base_prefix, "run in a fresh virtual environment")
    artifact = args.artifact.resolve(strict=True)
    require(artifact.is_file(), "artifact must be a wheel, sdist, or source archive")
    require(not (Path.cwd() / "pyproject.toml").exists(), "run outside the source tree")

    import moq5
    from moq5 import Terminal, TerminalReason, WaitResult, _native

    package_file = Path(moq5.__file__).resolve()
    native_file = Path(_native.__file__).resolve()
    check_artifact(artifact, args.expected_package_version, package_file, native_file)
    check_loader_paths(native_file)
    info = moq5.build_info()
    require(
        set(info)
        == {"compiled_version", "runtime_version", "python_abi", "test_backend"},
        "unexpected build identity fields",
    )
    for field in ("compiled_version", "runtime_version"):
        require(type(info[field]) is str, f"{field} is not a string")
        require(info[field] == args.expected_sdk_version, f"wrong SDK {field}")
    require(info["python_abi"] == "cp312-abi3", "wrong Python ABI identity")
    require(info["test_backend"] is False, "private test provider is active")
    require(
        not any(name.startswith("_test") for name in dir(_native)), "test hooks leaked"
    )
    require(
        {member.name: member.value for member in WaitResult}
        == {"WOKEN": 0, "TIMED_OUT": 1, "INTERRUPTED": -13, "CLOSED": -4},
        "wait outcome mapping changed",
    )

    terminal = Terminal(TerminalReason.TRANSPORT, (1 << 64) - 1)
    require(
        terminal.reason is TerminalReason.TRANSPORT, "known terminal reason changed"
    )
    require(type(terminal.detail_code) is int, "terminal detail is not an integer")
    require(terminal.detail_code == (1 << 64) - 1, "terminal detail lost raw bits")
    unknown = Terminal(0x7FFFFFFF, 0)
    require(
        type(unknown.reason) is int and unknown.reason == 0x7FFFFFFF,
        "unknown terminal reason was not preserved",
    )
    for value, expected_error in (
        (True, TypeError),
        (1.0, TypeError),
        ("1", TypeError),
        (-1, ValueError),
        (1 << 64, ValueError),
    ):
        try:
            Terminal(TerminalReason.NONE, value)
        except expected_error:
            pass
        else:
            raise AssertionError(f"invalid terminal detail accepted: {value!r}")

    # Invalid syntax reaches the real C resolver on every SDK, without a socket.
    expect_native_error("unknown-scheme://127.0.0.1:9", -2)
    # Native offer-count and support limits govern validation, not the Python
    # layer: nine offers exceed the SDK's capacity and are refused with INVAL
    # during resolution, BEFORE any backend is selected or a socket exists --
    # on every SDK, with or without adapters. The fixture cannot certify this.
    expect_native_error("moqt://127.0.0.1:9", -2, versions=(16,) * 9)
    if args.skip_backendless_checks:
        print("Backend-absence assertions omitted; no valid-URL connection attempted.")
    else:
        expect_native_error("moqt://127.0.0.1:9", -14)
        expect_native_error("https://127.0.0.1:443/moq", -14)
        # Eight offers is the SDK's capacity: accepted by resolution, then
        # refused by backend absence. Adapter-free SDKs only; never dialled.
        expect_native_error("moqt://127.0.0.1:9", -14, versions=(16,) * 8)
    print(
        f"Installed {artifact.name}: moq5={args.expected_package_version}, "
        f"LibMoQ VERSION={args.expected_sdk_version}, cp312-abi3; smoke passed."
    )


if __name__ == "__main__":
    main()
