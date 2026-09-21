"""Check an installed endpoint+receiver artifact against a real, adapter-free SDK.

Run with CPython 3.12 -I -W error in a fresh venv, outside the source tree.
This checks installation, the installed origin of every public name, and
configuration refusals; it opens no socket and attaches no receiver.
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
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


INSTALLED_FILES = (
    "moq5/__init__.py",
    "moq5/_endpoint.py",
    "moq5/_receiver.py",
    "moq5/_send_track.py",
    "moq5/_sender.py",
    "moq5/_values.py",
    "moq5/py.typed",
    "moq5/_native.abi3.so",
)

# The DECLARED public surface, not a count measured from the product. A name
# added or removed without updating this fails the gate by name.
PUBLIC_NAMES = frozenset({
    "Backend", "Backpressure", "BindingError", "BuildInfo",
    "CATALOG_REFRESH_DISABLED", "Cenc", "CmafInit", "CmafSample", "CodecKind",
    "Endpoint", "EndpointConfig", "EndpointState", "EventLost", "FatalOrigin",
    "MediaObject", "MediaTemplate", "MediaType", "MoqError", "ObjectLost",
    "ObjectStatus", "OverflowPolicy", "Packaging", "ParseDrop",
    "ParseDropClass", "PollOutcome", "Protocol", "Receiver", "ReceiverConfig",
    "ReceiverFatal", "ReceiverStats", "ReceiverTerminal", "SapType",
    "SenderStats",
    "SendObject", "SendTrack", "SendTrackConfig", "Sender", "SenderConfig",
    "SenderTerminal", "StartMode",
    "Terminal", "TerminalReason", "TimeMode", "Track", "TrackDescription",
    "TrackEvent", "TrackEventKind", "TrackNameBusy", "TrackState", "Version",
    "VodState", "WaitResult", "WriteOutcome", "WtProfile", "build_info",
})

# Public names that are plain values rather than types or functions. Each is
# checked by exact type and value, and by the module that exports it, instead
# of relaxing the origin rule for anything without a __module__.
PUBLIC_SCALARS = {
    "CATALOG_REFRESH_DISABLED": (int, 2 ** 64 - 1, "moq5._values"),
}


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
    for relative in INSTALLED_FILES:
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


def check_public_origin() -> None:
    """Every public name resolves to an object defined in a file of the
    installed distribution, and the receiver module is the installed one."""
    import moq5

    distribution = metadata.distribution("moq5")
    installed = {
        Path(distribution.locate_file(relative)).resolve() for relative in INSTALLED_FILES
    }
    exported = set(moq5.__all__)
    require(len(set(moq5.__all__)) == len(moq5.__all__), "duplicate public name")
    require(exported == PUBLIC_NAMES,
            "public surface changed: "
            f"added {sorted(exported - PUBLIC_NAMES)}, "
            f"missing {sorted(PUBLIC_NAMES - exported)}")
    for name in sorted(exported):
        value = getattr(moq5, name)
        if name in PUBLIC_SCALARS:
            kind, expected, owner = PUBLIC_SCALARS[name]
            require(type(value) is kind,
                    f"{name} is {type(value).__name__}, not {kind.__name__}")
            require(value == expected, f"{name} is {value!r}, not {expected!r}")
            require(getattr(sys.modules[owner], name, None) == expected,
                    f"{name} is not exported by {owner}")
            origin = Path(sys.modules[owner].__file__).resolve()
            require(origin in installed,
                    f"{name} comes from outside the artifact: {origin}")
            continue
        module_name = getattr(value, "__module__", None)
        require(
            type(module_name) is str and module_name.startswith("moq5."),
            f"{name} is not defined by a moq5 module: {module_name!r}",
        )
        origin = Path(sys.modules[module_name].__file__).resolve()
        require(origin in installed, f"{name} comes from outside the artifact: {origin}")
    receiver_module = sys.modules["moq5._receiver"]
    require(
        Path(receiver_module.__file__).resolve()
        == Path(distribution.locate_file("moq5/_receiver.py")).resolve(),
        "receiver module imported from outside its installed distribution",
    )
    for name in ("Receiver",):
        require(getattr(moq5, name).__module__ == "moq5._receiver", f"{name} moved")
    for name in ("Track", "TrackEvent", "MediaObject", "ReceiverConfig", "ObjectStatus"):
        require(getattr(moq5, name).__module__ == "moq5._values", f"{name} moved")
    for name in ("BindingError", "EventLost", "ObjectLost", "MoqError",
                 "TrackNameBusy"):
        require(getattr(moq5, name).__module__ == "moq5._endpoint", f"{name} moved")
    for module_file, names in (("moq5/_sender.py", ("Sender",)),
                               ("moq5/_send_track.py", ("SendTrack",))):
        module_name = "moq5." + Path(module_file).stem
        module = sys.modules[module_name]
        require(
            Path(module.__file__).resolve()
            == Path(distribution.locate_file(module_file)).resolve(),
            f"{module_name} imported from outside its installed distribution",
        )
        for name in names:
            require(getattr(moq5, name).__module__ == module_name, f"{name} moved")
    for name in ("SenderConfig", "SendTrackConfig", "SenderTerminal"):
        require(getattr(moq5, name).__module__ == "moq5._values", f"{name} moved")


def check_receiver_values() -> None:
    """Pure configuration checks of the receiver layer: no endpoint, no
    receiver attachment, no native media call."""
    from moq5 import (
        BindingError,
        EventLost,
        MoqError,
        ObjectLost,
        OverflowPolicy,
        PollOutcome,
        Receiver,
        ReceiverConfig,
        TimeMode,
        WaitResult,
        _native,
    )

    require(callable(getattr(_native, "receiver_attach", None)), "native receiver entry missing")
    require(callable(getattr(_native, "receiver_poll_object", None)), "native object poll missing")
    for refused in (lambda: Receiver(), lambda: Receiver.attach("endpoint", None)):
        try:
            refused()
        except TypeError:
            pass
        else:
            raise AssertionError("a Receiver was created without an endpoint")
    require(
        {member.name: member.value for member in PollOutcome}
        == {
            "EMPTY": WaitResult.TIMED_OUT.value,
            "INTERRUPTED": WaitResult.INTERRUPTED.value,
            "CLOSED": WaitResult.CLOSED.value,
        },
        "poll outcome mapping changed",
    )
    require(
        len({OverflowPolicy.DROP_TO_KEYFRAME, OverflowPolicy.DROP_GROUP, OverflowPolicy.FLOW_CONTROL}) == 3,
        "overflow policies are not distinct",
    )
    config = ReceiverConfig.live([b"ns", b"part"], catalog_track=b"catalog")
    require(config.namespace == (b"ns", b"part"), "namespace was not copied as bytes parts")
    require(config.overflow is OverflowPolicy.DROP_TO_KEYFRAME, "live preset overflow changed")
    require(
        ReceiverConfig.flow_control((b"ns",)).overflow is OverflowPolicy.FLOW_CONTROL,
        "flow-control preset overflow changed",
    )
    require(config.time_mode is TimeMode.RAW, "default time mode changed")
    require((config.max_objects, config.max_bytes, config.max_track_events) == (0, 0, 0), "defaults changed")
    for fields, expected_error in (
        ({"namespace": ()}, ValueError),
        ({"namespace": (b"",)}, ValueError),
        ({"namespace": ("text",)}, TypeError),
        ({"namespace": (b"ns",), "catalog_track": "catalog"}, TypeError),
        ({"namespace": (b"ns",), "auto_subscribe": 1}, TypeError),
        ({"namespace": (b"ns",), "max_objects": 2_863_311_530}, ValueError),
        ({"namespace": (b"ns",), "max_bytes": 1 << 64}, ValueError),
        ({"namespace": (b"ns",), "max_track_events": -1}, ValueError),
    ):
        try:
            ReceiverConfig(overflow=OverflowPolicy.DROP_GROUP, **fields)
        except expected_error:
            pass
        else:
            raise AssertionError(f"invalid receiver configuration accepted: {fields!r}")
    try:
        setattr(config, "max_objects", 1)
    except (AttributeError, TypeError):
        pass
    else:
        raise AssertionError("receiver configuration is mutable")
    require(issubclass(BindingError, RuntimeError), "bridge error hierarchy changed")
    require(
        issubclass(EventLost, BindingError) and issubclass(ObjectLost, BindingError),
        "lost-record errors are not bridge errors",
    )
    require(not issubclass(BindingError, MoqError), "bridge errors carry no native code")
    lost = ObjectLost("payload", 7, 0, None)
    require(
        (lost.stage, lost.presentation_time_us, lost.status, lost.packaging) == ("payload", 7, 0, None),
        "object loss context changed",
    )


def check_sender_values() -> None:
    """Pure configuration checks of the sender and track layer: no endpoint,
    no attachment, no native call. Nothing here publishes anything."""
    from moq5 import (
        Backpressure,
        CATALOG_REFRESH_DISABLED,
        MediaType,
        Packaging,
        SendTrack,
        SendTrackConfig,
        Sender,
        SenderConfig,
        TrackNameBusy,
        MoqError,
    )

    require(issubclass(TrackNameBusy, MoqError), "TrackNameBusy is not a MoqError")
    require(CATALOG_REFRESH_DISABLED == 2 ** 64 - 1, "wrong refresh sentinel")
    require(type(CATALOG_REFRESH_DISABLED) is int, "the sentinel is not an int")

    config = SenderConfig(namespace=(b"svc", b"demo"),
                          backpressure=Backpressure.DROP_GROUP)
    require(config.namespace == (b"svc", b"demo"), "namespace not copied exactly")
    require(config.validate_cmaf is True, "unexpected validate_cmaf default")

    track = SendTrackConfig(name=b"v\x001", media_type=MediaType.VIDEO,
                            packaging=Packaging.RAW, codec=b"av01",
                            bitrate=2 ** 32 + 1)
    require(track.name == b"v\x001", "the track name lost bytes")
    require(track.bitrate == 2 ** 32 + 1, "a wide bitrate was narrowed")
    require(track.is_live is True, "unexpected is_live default")
    require(track.track_duration_ms is None, "unexpected duration default")

    for bad in (dict(codec=b""), dict(bitrate=0), dict(name=b""),
                dict(is_live=True, track_duration_ms=1)):
        fields = dict(name=b"v", media_type=MediaType.VIDEO,
                      packaging=Packaging.RAW, codec=b"av01", bitrate=1)
        fields.update(bad)
        try:
            SendTrackConfig(**fields)
        except ValueError:
            continue
        raise AssertionError(f"a malformed track configuration was accepted: {bad}")

    for cls, method in ((Sender, "attach"), (Sender, "add_track"),
                        (Sender, "remove_track"), (Sender, "write"),
                        (Sender, "end_track"), (Sender, "stats"),
                        (Sender, "subscriptions"), (Sender, "has_subscriber"),
                        (Sender, "has_media_subscriber"), (Sender, "wait"),
                        (Sender, "request_complete")):
        require(callable(getattr(cls, method, None)), f"{method} is missing")
    # the sender's method contract, as far as it can be checked without an
    # endpoint: end_track takes one track and is not a completion barrier
    import inspect

    signature = inspect.signature(Sender.end_track)
    require(list(signature.parameters) == ["self", "track"],
            f"unexpected end_track signature: {signature}")
    signature = inspect.signature(Sender.request_complete)
    require(list(signature.parameters) == ["self"],
            f"unexpected request_complete signature: {signature}")
    for absent in ("finish", "complete", "completed", "drain", "demand"):
        require(not hasattr(Sender, absent),
                f"the sender grew an unreviewed {absent}")

    # the write value surface, checked without any endpoint or native call
    from moq5 import SapType, SendObject, WriteOutcome

    require(WriteOutcome.ACCEPTED == 0 and WriteOutcome.WOULD_BLOCK == -8
            and WriteOutcome.INTERRUPTED == -13 and WriteOutcome.CLOSED == -4,
            "the write outcome codes are not the native ones")
    require(not hasattr(SapType, "UNKNOWN"),
            "the internal SAP sentinel is offered publicly")
    require(tuple(int(v) for v in SapType) == (0, 1, 2, 3),
            "unexpected SAP type values")

    object_ = SendObject(payload=b"\x00fr\xff", properties=b"",
                         capture_time_us=0, sap_type=SapType.NONE)
    require(object_.payload == b"\x00fr\xff", "the payload lost bytes")
    require(object_.properties == b"", "a present empty block became absent")
    require(object_.capture_time_us == 0, "an explicit zero became absence")
    require(object_.sap_type is SapType.NONE, "the declared SAP was dropped")
    empty = SendObject(payload=b"")
    require(empty.payload == b"" and empty.properties is None,
            "an empty payload or absent properties was not preserved")
    require(SendObject(payload=b"p").capture_time_us is None,
            "an absent capture time was defaulted to a value")
    for bad in (dict(payload=bytearray(b"p")), dict(payload="p"),
                dict(payload=memoryview(b"p")),
                dict(payload=b"p", properties=bytearray(b"q")),
                dict(payload=b"p", is_sync=1)):
        try:
            SendObject(**bad)
        except TypeError:
            continue
        raise AssertionError(f"a wrongly typed send object was accepted: {bad}")
    try:
        SendObject()
    except TypeError:
        pass
    else:
        raise AssertionError("a send object without a payload was accepted")
    try:
        SendObject(payload=b"p", capture_time_us=2 ** 64)
    except ValueError:
        pass
    else:
        raise AssertionError("an out-of-range capture time was accepted")
    try:
        object_.payload = b"other"
    except AttributeError:
        pass
    else:
        raise AssertionError("the send object is not frozen")

    # the statistics snapshot's shape and its None-versus-zero contract,
    # checked without an endpoint or a native call
    from moq5 import SenderStats

    require(list(inspect.signature(Sender.stats).parameters) == ["self"],
            "unexpected stats signature")
    for method, parameters in (("wait", ["self", "timeout_us"]),
                               ("subscriptions", ["self", "track"]),
                               ("has_subscriber", ["self", "track"]),
                               ("has_media_subscriber", ["self"])):
        signature = inspect.signature(getattr(Sender, method))
        require(list(signature.parameters) == parameters,
                f"unexpected {method} signature: {signature}")
    names = [f.name for f in dataclasses.fields(SenderStats)]
    require(names == ["objects_written", "objects_sent", "objects_queued",
                      "bytes_queued", "objects_dropped", "groups_dropped",
                      "keyframes_dropped", "groups_abandoned",
                      "backpressure_stalls", "last_error",
                      "sap_records_evicted"],
            f"unexpected SenderStats fields: {names}")
    absent = SenderStats(*([0] * 10), None)
    present = SenderStats(*([0] * 10), 0)
    require(absent.sap_records_evicted is None,
            "absence was not preserved")
    require(present.sap_records_evicted == 0
            and present.sap_records_evicted is not None,
            "a present zero was collapsed into absence")
    require(absent != present,
            "absence and a present zero compare equal")
    require(not hasattr(absent, "__dict__"),
            "SenderStats carries a mutable __dict__")
    try:
        absent.objects_written = 1
    except AttributeError:
        pass
    else:
        raise AssertionError("SenderStats is not frozen")
    try:
        SendTrack()
    except TypeError:
        pass
    else:
        raise AssertionError("SendTrack was constructed directly")


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
    check_public_origin()
    check_receiver_values()
    check_sender_values()
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
