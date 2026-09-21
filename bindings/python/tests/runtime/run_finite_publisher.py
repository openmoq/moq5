"""Finite RAW/LOC publishing runtime fixture.

The mirror of run_finite_receiver.py: here the PUBLISHER is the production
`moq5` package driving the shipped `examples/publish_loop.py`, the peer is our
own `moq5-relay` on raw loopback QUIC, and the observer is a receiver child
that drives the PUBLIC Receiver directly with THIS fixture's stop policy --
the declared inventory plus that track's ENDED event. It does not run
`receive_loop.py`, whose completion policy needs a VOD conversion or an
isComplete catalog that a publisher which never requests completion does not
produce, and it never requests completion to make some other policy return.

Declared data lives in `fixture.py` and is written before any process starts.
Synthetic carrier bytes, not playable media; no decoder, no camera, no
FFmpeg. It opens sockets, so it is NOT part of the deterministic suite and
runs only by explicit invocation.

What a positive run proves: those exact payload bytes, identities and mapped
object metadata reached a real receiver through our own relay, in order, and
that the track's ENDED event was observed. What it does NOT prove: the CAUSE
of that ENDED (the facade includes rejection in it), a wire END_OF_TRACK
discriminant, a flush, a remote acknowledgment, a graceful finish, or any
general completion API. The publisher's own record is a SUBMISSION receipt
and is kept distinct from the receiver's observations throughout.

Ownership: the supervisor owns the relay, the publisher and the receiver, and
settles every one of them. The publisher keeps its sender and endpoint alive
until the receiver's observations are complete -- `publish()` returning
"submitted" does NOT release them -- and closes only when the supervisor says
the observations are done.

    python -I bindings/python/tests/runtime/run_finite_publisher.py --oracle-red
    python -I bindings/python/tests/runtime/run_finite_publisher.py --child-controls
    env -i PATH=/usr/bin:/bin DYLD_FALLBACK_LIBRARY_PATH=<msquic dylib dir> \
      python -I bindings/python/tests/runtime/run_finite_publisher.py \
        --package <installed moq5 dir> --relay <moq5-relay> [--negative]
    python -I bindings/python/tests/runtime/run_finite_publisher.py --verdict-red <run dir>
"""

import datetime, importlib.util, json, os, re, select, sys, threading, time, traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import fixture                                              # noqa: E402
from runtime_support import (EXPECTED_ALPN, Harness, MSQUIC_DIR, ControlProtocol,  # noqa: E402
                             collect_diagnostics, free_udp_port, relay_checks)
import hashlib                                              # noqa: E402

EXAMPLE = HERE.parent.parent / "examples" / "publish_loop.py"
NS_TEXT = "/".join(p.decode() for p in fixture.NAMESPACE)
MSQUIC_IMAGE = MSQUIC_DIR + "/libmsquic.2.6.0.dylib"
OPENSSL3_IMAGES = ("/opt/homebrew/opt/openssl@3/lib/libssl.3.dylib",
                   "/opt/homebrew/opt/openssl@3/lib/libcrypto.3.dylib")
DROP_FIELDS = ("objects_dropped", "groups_dropped", "keyframes_dropped", "groups_abandoned")

# The receiver emits these, and nothing else, in a positive run.
EXPECTED_EVENTS = ["ADDED", "CATALOG_READY", "ENDED"]
# What the receiver may say to the publisher, in this order.
CONTROLS_POSITIVE = ["subscribed", "observed"]

CONNECT_S = 20.0
READY_S = 20.0
SUBSCRIBE_S = 25.0
OBSERVE_S = 30.0
NEGATIVE_WINDOW_S = 6.0
CLIENT_S = 90.0
PUBLISHER_EXIT_S = 30.0
READER_STOP_S = 5.0
REPORT_S = 2.5          # the control driver's bound on a child's report
REPORT_BYTES = 8192
CHILD_STOP_S = 10.0


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# The declared receiver-visible values, in ONE place. The child cross-checks
# these against the package's own enums and records the result, so a drift in
# either direction is a named failure rather than a silent agreement.
RAW_PACKAGING = 1          # moq5.Packaging.RAW
NORMAL_STATUS = 0          # moq5.ObjectStatus.NORMAL


def declared_images():
    """The declared inventory as receipt images: the comparison the child
    makes and the comparison the durable verdict recomputes are the same."""
    return [
        {"payload": payload.hex(), "length": len(payload), "keyframe": is_sync,
         "packaging": RAW_PACKAGING, "status": NORMAL_STATUS,
         "end_of_group": True, "datagram": False,
         "capture_time_us": fixture.PTS_US[i], "decode_time_us": fixture.PTS_US[i],
         "composition_offset_us": 0, "presentation_time_us": fixture.PTS_US[i],
         "config_generation": 0, "track_name": fixture.TRACK.decode()}
        for i, is_sync, payload in fixture.OBJECTS
    ]


def compare_images(declared, received):
    """Named mismatches between two inventories of receipt images."""
    problems = []
    if len(received) != len(declared):
        problems.append(f"received {len(received)} objects != declared {len(declared)}")
    for i, (want, got) in enumerate(zip(declared, received)):
        if not isinstance(got, dict):
            problems.append(f"object {i}: not an object image")
            continue
        for field, expected in want.items():
            if field not in got:
                problems.append(f"object {i}: {field} missing")
            elif got[field] != expected:
                problems.append(f"object {i}: {field} {got[field]!r} != {expected!r}")
    return problems


def declared_submissions(moq5):
    """The exact objects the publisher submits, declared before any process
    starts: the same bytes, sync flags and presentation times the receiver
    expects, written the way the C fixture publisher writes them."""
    return [
        moq5.SendObject(payload=payload, is_sync=is_sync, starts_group=is_sync,
                        presentation_time_us=fixture.PTS_US[i],
                        decode_time_us=fixture.PTS_US[i])
        for i, is_sync, payload in fixture.OBJECTS
    ]


def dyld_images():
    """The images THIS process has loaded, from the dynamic loader itself.

    Stdlib ctypes over dyld's own public API: no external tool, no elevated
    privileges, and no binding test seam. It is the process's own report of
    what it mapped, which is what provenance needs, and it is the primary
    provenance for this fixture.
    """
    import ctypes

    libc = ctypes.CDLL(None)
    libc._dyld_image_count.restype = ctypes.c_uint32
    libc._dyld_get_image_name.restype = ctypes.c_char_p
    libc._dyld_get_image_name.argtypes = [ctypes.c_uint32]
    names = []
    for index in range(libc._dyld_image_count()):
        name = libc._dyld_get_image_name(index)
        if name:
            names.append(name.decode(errors="replace"))
    return sorted(names)


def object_image(obj):
    """A JSON-able image of one received MediaObject, for the receipt. It
    carries the track ASSOCIATION as well, so the durable verdict can check
    that each object arrived on the declared track."""
    return {"track_name": obj.track.description.name.decode(),
            "payload": obj.payload.hex(), "length": len(obj.payload),
            "keyframe": obj.keyframe, "packaging": int(obj.packaging),
            "status": int(obj.status), "end_of_group": obj.end_of_group,
            "datagram": obj.datagram, "capture_time_us": obj.capture_time_us,
            "decode_time_us": obj.decode_time_us,
            "composition_offset_us": obj.composition_offset_us,
            "presentation_time_us": obj.presentation_time_us,
            "config_generation": obj.config_generation}


# A control line is a short declared word; anything longer is refused as
# overlong rather than truncated into something valid, and the whole protocol
# has a finite byte budget.
MAX_MESSAGE_BYTES = 64
MAX_PROTOCOL_BYTES = 4096


class ControlReader:
    """The publisher's stdin control reader: bounded descriptor reads with
    explicit line framing and no normalisation, OWNED rather than abandoned.

    It never uses a buffered readline: readability promises bytes, not a line,
    and a buffered read can hold the NEXT line where a descriptor poll cannot
    see it -- both of which lose or stall controls. Instead it reads the
    descriptor, frames on newlines, processes every complete line already
    read before polling again, and keeps a partial line until more bytes, EOF,
    the deadline or cancellation. Every outcome is named: a partial line at
    EOF, invalid encoding, an overlong line, a duplicate, an out-of-order
    control, an unknown word, and the protocol byte budget. Nothing invalid is
    silently stripped into something valid. The thread is stopped and JOINED
    on every exit path; a daemon thread the process outlives is not
    settlement, so this is not one.
    """

    def __init__(self, expected, fd=0):
        self.expected = list(expected)
        self.fd = fd
        self.lines = []
        self.malformed = []
        self.out_of_order = []
        self.duplicates = []
        self.invalid_encoding = []
        self.overlong = []
        self.partial_at_eof = None
        self.partial_held = None
        self.overflow = False
        self.eof = False
        self.error = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._run, name="control-reader")
        self._thread.start()

    def _accept(self, raw):
        if len(raw) > MAX_MESSAGE_BYTES:
            self.overlong.append(len(raw))
            return
        try:
            control = raw.decode("utf-8")
        except UnicodeDecodeError:
            self.invalid_encoding.append(raw.hex())
            return
        # The newline is the framing delimiter and the ONLY one: a control is
        # the exact declared word. Surrounding whitespace or a stray carriage
        # return is malformed input, not something to normalise away.
        if control not in self.expected:
            self.malformed.append(control)
            return
        if control in self.lines:
            self.duplicates.append(control)
            return
        if self.expected.index(control) != len(self.lines):
            self.out_of_order.append(control)
        self.lines.append(control)

    def _run(self):
        buffer, total = b"", 0
        try:
            while not self._stop.is_set():
                ready, _, _ = select.select([self.fd], [], [], 0.05)
                if not ready:
                    continue
                chunk = os.read(self.fd, 4096)
                if chunk == b"":
                    with self._lock:
                        self.eof = True
                        if buffer:
                            self.partial_at_eof = buffer.decode("utf-8", "replace")
                    return
                total += len(chunk)
                if total > MAX_PROTOCOL_BYTES:
                    with self._lock:
                        self.overflow = True
                    return
                buffer += chunk
                with self._lock:
                    while b"\n" in buffer:      # every complete line already read
                        raw, buffer = buffer.split(b"\n", 1)
                        self._accept(raw)
                    if len(buffer) > MAX_MESSAGE_BYTES:
                        self.overlong.append(len(buffer))
                        buffer = b""
        except BaseException as error:                      # noqa: BLE001
            self.error = f"{type(error).__name__}: {error}"
        finally:
            with self._lock:
                if buffer and self.partial_at_eof is None:
                    self.partial_held = buffer.decode("utf-8", "replace")

    def wait_for(self, name, deadline_s):
        end = time.monotonic() + deadline_s
        while time.monotonic() < end:
            with self._lock:
                if name in self.lines:
                    return True
            if self.error is not None:
                return False
            time.sleep(0.02)
        return False

    def settle(self, timeout_s):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout_s)
            joined = not self._thread.is_alive()
        else:
            joined = True
        with self._lock:
            return {"joined": joined, "lines": list(self.lines),
                    "malformed": list(self.malformed),
                    "out_of_order": list(self.out_of_order),
                    "duplicates": list(self.duplicates),
                    "invalid_encoding": list(self.invalid_encoding),
                    "overlong": list(self.overlong),
                    "partial_at_eof": self.partial_at_eof,
                    "partial_held": self.partial_held,
                    "overflow": self.overflow,
                    "eof": self.eof, "error": self.error}


# ------------------------------------------------------------------ children
def publisher_main(run, port, package, negative):
    """The publisher child: the installed package plus the SHIPPED example."""
    out = {"kind": "publisher", "harness_failures": [], "record": None,
           "controls_seen": [], "close_sequence": []}
    sys.path.insert(0, package)
    import moq5
    out["package"] = {"file": moq5.__file__, "build_info": dict(moq5.build_info())}
    spec = importlib.util.spec_from_file_location("publish_loop", EXAMPLE)
    example = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(example)
    out["example"] = {"path": str(EXAMPLE), "sha256": sha256_file(EXAMPLE)}
    out["enum_check"] = {"packaging_raw": int(moq5.Packaging.RAW),
                         "status_normal": int(moq5.ObjectStatus.NORMAL)}

    def emit(payload):
        sys.stdout.write(json.dumps(payload) + "\n")
        sys.stdout.flush()

    reader = ControlReader(CONTROLS_POSITIVE)
    reader.start()

    def await_control(name, deadline_s):
        if reader.wait_for(name, deadline_s):
            out["controls_seen"].append(name)
            return True
        return False

    creds = run / "creds"
    endpoint = sender = track = None
    try:
        endpoint = moq5.Endpoint.connect(moq5.EndpointConfig(
            url=f"moqt://127.0.0.1:{port}", backend=moq5.Backend.PICOQUIC,
            versions=(moq5.Version.DRAFT_18,), sni="localhost",
            ca_file=str(creds / "ca.pem")))
        t0 = time.monotonic()
        while endpoint.state != moq5.EndpointState.ESTABLISHED:
            if time.monotonic() - t0 > CONNECT_S or endpoint.terminal.reason != moq5.TerminalReason.NONE:
                raise RuntimeError(f"publisher endpoint not established: {endpoint.state} {endpoint.terminal}")
            endpoint.wait(250_000)
        out["publisher_endpoint"] = {"negotiated_version": endpoint.negotiated_version,
                                     "state": endpoint.state.name,
                                     "ms": round((time.monotonic() - t0) * 1000)}
        sender = moq5.Sender.attach(endpoint, moq5.SenderConfig(
            namespace=fixture.NAMESPACE,
            backpressure=moq5.Backpressure.RETURN_WOULD_BLOCK))
        track = sender.add_track(moq5.SendTrackConfig(
            name=fixture.TRACK, media_type=moq5.MediaType.VIDEO,
            packaging=moq5.Packaging.RAW, codec=fixture.CODEC,
            bitrate=fixture.BITRATE, timescale=fixture.TIMESCALE,
            width=fixture.WIDTH, height=fixture.HEIGHT,
            framerate_millis=fixture.FRAMERATE_MILLIS))
        t1 = time.monotonic()
        while not sender.ready:
            if time.monotonic() - t1 > READY_S:
                raise RuntimeError("the sender never became ready")
            sender.wait(250_000)
        out["ready_ms"] = round((time.monotonic() - t1) * 1000)
        emit({"event": "ready"})

        # The intended subscription is established BEFORE anything is
        # submitted: the receiver says it subscribed, and the service then
        # reports the demand itself. Aggregate demand is not readiness and
        # not per-track demand; this run has exactly one media track, which
        # is what makes the aggregate answer sufficient here.
        if not await_control("subscribed", SUBSCRIBE_S):
            raise RuntimeError("the receiver never reported its subscription")
        t2 = time.monotonic()
        while not sender.has_media_subscriber():
            if time.monotonic() - t2 > SUBSCRIBE_S:
                raise RuntimeError("the service never reported the subscriber")
            sender.wait(250_000)
        out["demand_ms"] = round((time.monotonic() - t2) * 1000)
        out["subscriptions"] = sender.subscriptions(track)
        out["loaded_images"] = dyld_images()

        plan = [] if negative else [(track, obj) for obj in declared_submissions(moq5)]
        dropped = []
        clock_zero = time.monotonic()

        def now_us():
            return int((time.monotonic() - clock_zero) * 1_000_000)

        record = example.publish(
            sender, plan, now_us, 30_000_000, moq5.WriteOutcome,
            moq5.WaitResult, moq5.MoqError, dropped.append, wait_us=250_000)
        out["record"] = {
            "stop": record["stop"], "submitted": record["submitted"],
            "attempts": record["attempts"], "pending": record["pending"],
            "would_block": record["would_block"],
            "ended": [t.name.decode() for t in record["ended"]],
            "ends_refused": [[t.name.decode(), code] for t, code in record["ends_refused"]],
            "interrupted": record["interrupted"], "dropped": record["dropped"],
            "terminal": {"closed": record["terminal"].closed,
                         "fatal": record["terminal"].fatal},
            "stats": {name: getattr(record["stats"], name)
                      for name in ("objects_written", "objects_sent", "objects_queued",
                                   "bytes_queued") + DROP_FIELDS},
        }
        out["drop_reports"] = dropped
        emit({"event": "submitted", "stop": record["stop"],
              "submitted": record["submitted"]})

        # The owners stay alive while observations are owed.
        if not await_control("observed", OBSERVE_S):
            raise RuntimeError("the receiver never reported its observations")
        snapshot = sender.stats()                       # ONE observation
        out["stats_before_close"] = {name: getattr(snapshot, name)
                                     for name in ("objects_written", "objects_sent",
                                                  "objects_queued") + DROP_FIELDS}
    except BaseException as error:                          # noqa: BLE001
        out["harness_failures"].append(f"{type(error).__name__}: {error}")
        out["traceback"] = traceback.format_exc()
    finally:
        # the control reader is OWNED: stopped, joined within a bound, and its
        # disposition recorded whether or not the scenario succeeded
        out["control_reader"] = reader.settle(READER_STOP_S)
        if not out["control_reader"]["joined"]:
            out["harness_failures"].append("the control reader did not settle")
        if out["control_reader"]["error"]:
            out["harness_failures"].append("control reader: " + out["control_reader"]["error"])
        if out["control_reader"]["out_of_order"]:
            out["harness_failures"].append(
                "controls arrived out of order: " + str(out["control_reader"]["lines"]))
        for name, owner in (("sender", sender), ("endpoint", endpoint)):
            if owner is None:
                continue
            try:
                owner.close()
                out["close_sequence"].append(name)
            except BaseException as error:                  # noqa: BLE001
                out["harness_failures"].append(f"close {name}: {error!r}")
        (run / "publisher.json").write_text(json.dumps(out, indent=1, default=str))
    return 0 if not out["harness_failures"] else 3


def receiver_main(run, port, package, negative):
    """The observer child: the PUBLIC Receiver, this fixture's stop policy."""
    out = {"kind": "receiver", "harness_failures": [], "events": [],
           "inventory": None, "problems": None, "close_sequence": []}
    sys.path.insert(0, package)
    import moq5
    out["package"] = {"file": moq5.__file__, "build_info": dict(moq5.build_info())}
    # the declared receiver-visible constants, checked against the package's
    # own enums so the one shared contract cannot drift unnoticed
    out["enum_check"] = {"packaging_raw": int(moq5.Packaging.RAW),
                         "status_normal": int(moq5.ObjectStatus.NORMAL)}

    def emit(line):
        sys.stdout.write("CTRL " + line + "\n")
        sys.stdout.flush()

    creds = run / "creds"
    endpoint = receiver = None
    received, wanted, ended = [], None, False
    try:
        endpoint = moq5.Endpoint.connect(moq5.EndpointConfig(
            url=f"moqt://127.0.0.1:{port}", backend=moq5.Backend.PICOQUIC,
            versions=(moq5.Version.DRAFT_18,), sni="localhost",
            ca_file=str(creds / "ca.pem")))
        t0 = time.monotonic()
        while endpoint.state != moq5.EndpointState.ESTABLISHED:
            if time.monotonic() - t0 > CONNECT_S or endpoint.terminal.reason != moq5.TerminalReason.NONE:
                raise RuntimeError(f"receiver endpoint not established: {endpoint.state} {endpoint.terminal}")
            endpoint.wait(250_000)
        out["receiver_endpoint"] = {"negotiated_version": endpoint.negotiated_version,
                                    "state": endpoint.state.name,
                                    "ms": round((time.monotonic() - t0) * 1000)}
        receiver = moq5.Receiver.attach(endpoint, moq5.ReceiverConfig.flow_control(
            fixture.NAMESPACE, auto_subscribe=False))
        out["loaded_images"] = dyld_images()
        deadline = time.monotonic() + OBSERVE_S + SUBSCRIBE_S
        negative_deadline = time.monotonic() + NEGATIVE_WINDOW_S
        declared = None
        while time.monotonic() < deadline:
            while True:                                     # every track event
                event = receiver.poll_track()
                if event is moq5.PollOutcome.EMPTY:
                    break
                if event is moq5.PollOutcome.CLOSED:
                    out["events"].append({"kind": "CLOSED", "track_name": None,
                                          "wanted": False})
                    break
                name = None
                if event.description is not None:
                    name = event.description.name.decode()
                elif event.track is not None and event.track.description is not None:
                    name = event.track.description.name.decode()
                out["events"].append({"kind": event.kind.name, "track_name": name,
                                      "wanted": event.track is wanted})
                if event.kind is moq5.TrackEventKind.ADDED and \
                        event.description.name == fixture.TRACK:
                    wanted = event.track
                    receiver.subscribe(wanted)
                    declared = fixture.declared_expectations(moq5, wanted)
                    negative_deadline = time.monotonic() + NEGATIVE_WINDOW_S
                    emit("subscribed")
                elif event.kind is moq5.TrackEventKind.ENDED and event.track is wanted:
                    ended = True
            while True:                                     # every media object
                obj = receiver.poll_object()
                if obj in (moq5.PollOutcome.EMPTY, moq5.PollOutcome.CLOSED,
                           moq5.PollOutcome.INTERRUPTED):
                    break
                received.append(obj)
            # THIS fixture's stop policy: the declared inventory and that
            # track's terminal observation. Nothing about VOD or isComplete.
            # The deliberate no-media run ends nothing, so it stops on its own
            # bounded window rather than waiting for an ENDED that a publisher
            # with an empty plan never requests.
            if negative:
                if wanted is not None and time.monotonic() > negative_deadline:
                    break
            elif ended and declared is not None and len(received) >= len(declared):
                break
            receiver.wait(250_000)
        out["inventory"] = [object_image(obj) for obj in received]
        out["ended_observed"] = ended
        out["subscribed"] = wanted is not None
        if declared is not None:
            # the child's own comparison, against the SAME declared contract
            # the durable verdict recomputes from the receipt
            out["problems"] = fixture.compare(declared, received)
            out["image_problems"] = compare_images(declared_images(), out["inventory"])
        else:
            out["problems"] = ["the declared track never appeared"]
            out["image_problems"] = ["the declared track never appeared"]
        out["stats"] = {"objects": len(received)}
        emit("observed")
    except BaseException as error:                          # noqa: BLE001
        out["harness_failures"].append(f"{type(error).__name__}: {error}")
        out["traceback"] = traceback.format_exc()
    finally:
        for name, owner in (("receiver", receiver), ("endpoint", endpoint)):
            if owner is None:
                continue
            try:
                owner.close()
                out["close_sequence"].append(name)
            except BaseException as error:                  # noqa: BLE001
                out["harness_failures"].append(f"close {name}: {error!r}")
        (run / "receiver.json").write_text(json.dumps(out, indent=1, default=str))
    return 0 if not out["harness_failures"] else 3


# ------------------------------------------------------------------- oracle
HEX = set("0123456789abcdefABCDEF")
MODES = ("positive", "negative")
SELECTED_ROLES = ("module", "example", "relay", "msquic", "libssl", "libcrypto")
OWNED_CHILDREN = ("relay", "publisher", "receiver")
OBJECT_FIELDS = ("track_name", "payload", "length", "keyframe", "packaging", "status",
                 "end_of_group", "datagram", "capture_time_us", "decode_time_us",
                 "composition_offset_us", "presentation_time_us", "config_generation")
# The exact event sequence each mode declares, as (kind, track) pairs.
DECLARED_EVENTS = {
    "positive": [("ADDED", fixture.TRACK.decode()), ("CATALOG_READY", None),
                 ("ENDED", fixture.TRACK.decode())],
    "negative": [("ADDED", fixture.TRACK.decode()), ("CATALOG_READY", None)],
}


def _count(value):
    """A count or enum here is a plain int. Python's bool is not one."""
    return isinstance(value, int) and not isinstance(value, bool)


def _typed(node, key, kind, where, problems, default=None):
    """One typed read. A missing or wrong-typed field is a NAMED problem, not
    a traceback and not a default that happens to look healthy."""
    if not isinstance(node, dict) or key not in node:
        problems.append(f"{where}.{key} missing")
        return default
    value = node[key]
    if kind is int:
        if not _count(value):
            problems.append(f"{where}.{key} is not a plain integer: {value!r}")
            return default
        return value
    if not isinstance(value, kind):
        problems.append(f"{where}.{key} is {type(value).__name__}, not "
                        f"{getattr(kind, '__name__', kind)}")
        return default
    return value


def _items(node, key, where, problems):
    """The VALIDATED list at node[key], or an empty one.

    A container that is not a list is named here and never descended into: a
    truthy wrong type must not be iterated just because `or []` let it
    through.
    """
    value = _typed(node, key, list, where, problems)
    return value if isinstance(value, list) else []


def _digest_ok(value):
    return isinstance(value, str) and len(value) == 64 and all(c in HEX for c in value)


def _hex_ok(value):
    if not isinstance(value, str):
        return False
    try:
        bytes.fromhex(value)
    except ValueError:
        return False
    return True


def structure_problems(rec):
    """Every field this verdict consumes, checked BEFORE anything that can
    throw. This is the finite shape of THIS receipt, audited once: not a
    generic schema, and not a subset test."""
    problems = []
    if not isinstance(rec, dict):
        return ["the receipt is not an object"]
    mode = _typed(rec, "mode", str, "receipt", problems)
    if mode is not None and mode not in MODES:
        problems.append(f"receipt.mode {mode!r} is not one of {MODES}")
    for key, kind in (("publisher_child", dict), ("receiver_child", dict),
                      ("publisher", dict), ("receiver", dict), ("relay", dict),
                      ("selected_images", dict), ("controls_forwarded", list),
                      ("controls_rejected", list), ("children", list),
                      ("commands", list), ("command_warnings", list),
                      ("harness_failures", list), ("cleanup_errors", list),
                      ("threads_settled", bool)):
        _typed(rec, key, kind, "receipt", problems)

    # the text lists this verdict concatenates
    for key in ("harness_failures", "cleanup_errors", "command_warnings",
                "controls_rejected"):
        for i, item in enumerate(_items(rec, key, "receipt", problems)):
            if not isinstance(item, str):
                problems.append(f"receipt.{key}[{i}] is not text: {item!r}")

    # the command records relay_checks reads
    for i, command in enumerate(_items(rec, "commands", "receipt", problems)):
        if not isinstance(command, dict):
            problems.append(f"commands[{i}] is not a command record")
            continue
        _typed(command, "name", str, f"commands[{i}]", problems)
        _typed(command, "rc", int, f"commands[{i}]", problems)
        _typed(command, "expected_rc", int, f"commands[{i}]", problems)
        _typed(command, "timeout", bool, f"commands[{i}]", problems)

    relay = rec.get("relay")
    if isinstance(relay, dict):
        _typed(relay, "port", int, "relay", problems)
        _typed(relay, "schemas", list, "relay", problems)
        _typed(relay, "readers_settled", bool, "relay", problems)
        _typed(relay, "reader_errors", list, "relay", problems)
        ready = _typed(relay, "ready_record", dict, "relay", problems)
        if isinstance(ready, dict):
            for key, kind in (("schema", str), ("listener", str), ("host", str),
                              ("port", int), ("alpn_set", str), ("lanes", int)):
                _typed(ready, key, kind, "relay.ready_record", problems)
        stop = _typed(relay, "stop", dict, "relay", problems)
        if isinstance(stop, dict):
            _typed(stop, "how", str, "relay.stop", problems)
            _typed(stop, "exit", int, "relay.stop", problems)

    # the forwarded control records, validated rather than filtered
    for i, control in enumerate(_items(rec, "controls_forwarded", "receipt", problems)):
        if not isinstance(control, dict):
            problems.append(f"controls_forwarded[{i}] is not a control record")
            continue
        _typed(control, "line", str, f"controls_forwarded[{i}]", problems)
        _typed(control, "forwarded", bool, f"controls_forwarded[{i}]", problems)

    selected = rec.get("selected_images")
    if isinstance(selected, dict):
        if tuple(sorted(selected)) != tuple(sorted(SELECTED_ROLES)):
            problems.append(f"selected image roles {sorted(selected)} != {sorted(SELECTED_ROLES)}")
        for role, entry in sorted(selected.items()):
            if not isinstance(entry, dict):
                problems.append(f"selected image {role}: not an image record")
                continue
            path = _typed(entry, "path", str, f"selected_images.{role}", problems)
            if isinstance(path, str) and not path.startswith("/"):
                problems.append(f"selected image {role}: path is not absolute")
            for key in ("sha256_before", "sha256_after"):
                if not _digest_ok(entry.get(key)):
                    problems.append(f"selected image {role}: {key} is not a sha256 digest")

    children = rec.get("children")
    if isinstance(children, list):
        names = []
        for i, child in enumerate(children):
            if not isinstance(child, dict):
                problems.append(f"children[{i}] is not a child record")
                continue
            name = _typed(child, "name", str, f"children[{i}]", problems)
            names.append(name)
            if child.get("settled") is not True:
                problems.append(f"child {name!r} was not settled")
            if not _count(child.get("exit")):
                problems.append(f"child {name!r} has no recorded exit status")
        if sorted(n for n in names if n) != sorted(OWNED_CHILDREN):
            problems.append(f"owned children {sorted(n for n in names if n)} != {sorted(OWNED_CHILDREN)}")

    for key in ("publisher", "receiver"):
        node = rec.get(key)
        if isinstance(node, dict):
            _typed(node, "exit", int, key, problems)
            for flag, kind in (("readers_settled", bool), ("timeout", bool)):
                _typed(node, flag, kind, key, problems)
            _typed(node, "reader_errors", list, key, problems)

    pub = rec.get("publisher_child") if isinstance(rec.get("publisher_child"), dict) else {}
    rx = rec.get("receiver_child") if isinstance(rec.get("receiver_child"), dict) else {}
    record = _typed(pub, "record", dict, "publisher_child", problems)
    if record is not None:
        for key, kind in (("stop", str), ("submitted", int), ("attempts", int),
                          ("would_block", int), ("ended", list), ("ends_refused", list),
                          ("interrupted", bool), ("dropped", dict), ("terminal", dict),
                          ("stats", dict)):
            _typed(record, key, kind, "record", problems)
        if "pending" not in record:
            problems.append("record.pending missing")
        elif record["pending"] is not None and not _count(record["pending"]):
            problems.append(f"record.pending is not an index: {record['pending']!r}")
        for where, node in (("record.dropped", record.get("dropped")),
                            ("stats_before_close", pub.get("stats_before_close"))):
            if not isinstance(node, dict):
                continue
            missing = [f for f in DROP_FIELDS if f not in node]
            if missing:
                problems.append(f"{where}: drop counters missing {missing}")
            for field in DROP_FIELDS:
                if field in node and not _count(node[field]):
                    problems.append(f"{where}.{field} is not a plain integer")
        terminal = record.get("terminal")
        if isinstance(terminal, dict):
            for key in ("closed", "fatal"):
                _typed(terminal, key, bool, "record.terminal", problems)
        stats = record.get("stats")
        if isinstance(stats, dict):
            for key in ("objects_written", "objects_sent", "objects_queued"):
                _typed(stats, key, int, "record.stats", problems)
        for key in ("submitted", "attempts", "would_block"):
            value = record.get(key)
            if _count(value) and value < 0:
                problems.append(f"record.{key} is negative: {value}")
    pre_close = pub.get("stats_before_close")
    if isinstance(pre_close, dict):
        for key in ("objects_written", "objects_sent", "objects_queued"):
            value = _typed(pre_close, key, int, "stats_before_close", problems)
            if _count(value) and value < 0:
                problems.append(f"stats_before_close.{key} is negative: {value}")
    for node, where in ((pub, "publisher_child"), (rx, "receiver_child")):
        for i, item in enumerate(_items(node, "harness_failures", where, problems)):
            if not isinstance(item, str):
                problems.append(f"{where}.harness_failures[{i}] is not text")
        for i, item in enumerate(_items(node, "close_sequence", where, problems)):
            if not isinstance(item, str):
                problems.append(f"{where}.close_sequence[{i}] is not a name")
        enum_check = _typed(node, "enum_check", dict, where, problems)
        if isinstance(enum_check, dict):
            for key in ("packaging_raw", "status_normal"):
                _typed(enum_check, key, int, f"{where}.enum_check", problems)
        images = _typed(node, "loaded_images", list, where, problems)
        if isinstance(images, list):
            if not images:
                problems.append(f"{where}.loaded_images is empty")
            for i, image in enumerate(images):
                if not isinstance(image, str) or not image.startswith("/"):
                    problems.append(f"{where}.loaded_images[{i}] is not an absolute path: {image!r}")
    for key in ("problems", "image_problems"):
        for i, item in enumerate(_items(rx, key, "receiver_child", problems)):
            if not isinstance(item, str):
                problems.append(f"receiver_child.{key}[{i}] is not text")
    for i, item in enumerate(_items(pub, "drop_reports", "publisher_child", problems)):
        if not isinstance(item, dict):
            problems.append(f"publisher_child.drop_reports[{i}] is not a delta")
    _typed(pub, "stats_before_close", dict, "publisher_child", problems)
    _typed(pub, "controls_seen", list, "publisher_child", problems)
    _typed(pub, "subscriptions", int, "publisher_child", problems)
    reader = _typed(pub, "control_reader", dict, "publisher_child", problems)
    if isinstance(reader, dict):
        for key in ("joined", "eof", "overflow"):
            _typed(reader, key, bool, "control_reader", problems)
        for key in ("lines", "malformed", "out_of_order", "duplicates",
                    "invalid_encoding", "overlong"):
            _typed(reader, key, list, "control_reader", problems)
        for key in ("partial_at_eof", "partial_held", "error"):
            if key not in reader:
                problems.append(f"control_reader.{key} missing")
            elif reader[key] is not None and not isinstance(reader[key], str):
                problems.append(f"control_reader.{key} is neither text nor absent")

    for key in ("subscribed", "ended_observed"):
        _typed(rx, key, bool, "receiver_child", problems)
    inventory = _typed(rx, "inventory", list, "receiver_child", problems)
    if isinstance(inventory, list):
        for i, image in enumerate(inventory):
            if not isinstance(image, dict):
                problems.append(f"receiver inventory[{i}] is not an object image")
                continue
            if tuple(sorted(image)) != tuple(sorted(OBJECT_FIELDS)):
                problems.append(f"inventory[{i}] fields {sorted(image)} != {sorted(OBJECT_FIELDS)}")
                continue
            if not _hex_ok(image["payload"]):
                problems.append(f"inventory[{i}].payload is not hexadecimal")
            for field in ("length", "packaging", "status", "capture_time_us",
                          "decode_time_us", "composition_offset_us",
                          "presentation_time_us", "config_generation"):
                if not _count(image[field]):
                    problems.append(f"inventory[{i}].{field} is not a plain integer: "
                                    f"{image[field]!r}")
            for field in ("keyframe", "end_of_group", "datagram"):
                if not isinstance(image[field], bool):
                    problems.append(f"inventory[{i}].{field} is not a boolean")
            if not isinstance(image["track_name"], str):
                problems.append(f"inventory[{i}].track_name is not a name")
    events = _typed(rx, "events", list, "receiver_child", problems)
    if isinstance(events, list):
        for i, event in enumerate(events):
            if not isinstance(event, dict) or tuple(sorted(event)) != ("kind", "track_name", "wanted"):
                problems.append(f"receiver events[{i}] is not an event image")
                continue
            if not isinstance(event["kind"], str):
                problems.append(f"events[{i}].kind is not a name")
            if event["track_name"] is not None and not isinstance(event["track_name"], str):
                problems.append(f"events[{i}].track_name is not a name")
            if not isinstance(event["wanted"], bool):
                problems.append(f"events[{i}].wanted is not a boolean")
    return problems


def provenance_problems(rec):
    """The recorded provenance, ENFORCED. Each real process must have mapped
    the module and the pinned provider image this receipt names, by their
    resolved paths, and every selected image must hash the same before and
    after. Unrelated system and interpreter images are none of this check's
    business; static incorporation is evidence of a different kind and is not
    demanded here as a DSO."""
    problems = []
    selected = rec.get("selected_images")
    if not isinstance(selected, dict) or not selected:
        problems.append("selected image identities missing")
        return problems
    for name, entry in sorted(selected.items()):
        if not isinstance(entry, dict):
            problems.append(f"selected image {name}: not an image record")
            continue
        before, after = entry.get("sha256_before"), entry.get("sha256_after")
        if not isinstance(before, str) or len(before) != 64:
            problems.append(f"selected image {name}: no recorded digest")
        elif before != after:
            problems.append(f"selected image {name}: digest changed during the run "
                            f"({before[:12]}… -> {str(after)[:12]}…)")
    def resolved(paths):
        out = set()
        for path in paths or []:
            try:
                out.add(os.path.realpath(path))
            except OSError:
                out.add(path)
        return out
    for child, key in (("publisher", "publisher_child"), ("receiver", "receiver_child")):
        node = rec.get(key) or {}
        images = node.get("loaded_images")
        if not isinstance(images, list) or not images:
            problems.append(f"{child}: no loader inventory recorded")
            continue
        mapped = resolved(images)
        for name in ("module", "msquic"):
            entry = selected.get(name) or {}
            path = entry.get("path")
            if not isinstance(path, str):
                problems.append(f"selected image {name}: no path recorded")
                continue
            if os.path.realpath(path) not in mapped:
                problems.append(f"{child} never mapped the selected {name}: {path}")
    return problems


def ownership_problems(rec):
    """Controls, close order and settlement, ENFORCED from the actual records."""
    problems = []
    negative = rec.get("mode") == "negative"
    pub = rec.get("publisher_child") or {}
    rx = rec.get("receiver_child") or {}
    expected_controls = list(CONTROLS_POSITIVE)
    forwarded = rec.get("controls_forwarded") or []
    if [c.get("line") for c in forwarded if isinstance(c, dict)] != expected_controls:
        problems.append(f"control sequence {[c.get('line') for c in forwarded if isinstance(c, dict)]}"
                        f" != {expected_controls}")
    for control in forwarded:
        if isinstance(control, dict) and control.get("forwarded") is not True:
            problems.append(f"control {control.get('line')!r} was not delivered")
    if rec.get("controls_rejected"):
        problems.append(f"controls rejected by the protocol: {rec['controls_rejected']}")
    if (pub.get("controls_seen") or []) != expected_controls:
        problems.append(f"the publisher observed {pub.get('controls_seen')} != {expected_controls}")
    reader = pub.get("control_reader") or {}
    if (reader.get("lines") or []) != expected_controls:
        problems.append(f"the control reader accepted {reader.get('lines')} != {expected_controls}")
    if (reader.get("lines") or []) != (pub.get("controls_seen") or []):
        problems.append("the control reader's lines and the publisher's observations disagree")
    for field in ("duplicates", "invalid_encoding", "overlong"):
        if reader.get(field):
            problems.append(f"control reader {field}: {reader[field]}")
    if reader.get("partial_at_eof") or reader.get("partial_held") or reader.get("overflow"):
        # a successful settlement leaves no remainder; a cancelled reader
        # legitimately reports one, which is a different verdict and is what
        # the reader's own control covers
        problems.append(f"control reader ended with unframed input: "
                        f"at_eof={reader.get('partial_at_eof')!r} "
                        f"held={reader.get('partial_held')!r} "
                        f"overflow={reader.get('overflow')}")
    if reader.get("joined") is not True:
        problems.append("the publisher's control reader was not joined")
    if reader.get("error"):
        problems.append("control reader error: " + str(reader["error"]))
    if reader.get("malformed"):
        problems.append(f"the publisher received malformed controls: {reader['malformed']}")
    if reader.get("out_of_order"):
        problems.append(f"controls arrived out of order: {reader['out_of_order']}")
    if (pub.get("close_sequence") or []) != ["sender", "endpoint"]:
        problems.append(f"publisher close order {pub.get('close_sequence')} != ['sender', 'endpoint']")
    if (rx.get("close_sequence") or []) != ["receiver", "endpoint"]:
        problems.append(f"receiver close order {rx.get('close_sequence')} != ['receiver', 'endpoint']")
    for name in ("publisher", "receiver"):
        info = rec.get(name) or {}
        if info.get("exit") != 0:
            problems.append(f"{name} process: exit {info.get('exit')!r} ({info.get('how')})")
        if info.get("timeout"):
            problems.append(f"{name} process: deadline expired ({info.get('how')})")
        if info.get("readers_settled") is not True:
            problems.append(f"{name} process: readers not settled")
        if info.get("reader_errors"):
            problems.append(f"{name} process: reader errors {info['reader_errors']}")
        if info.get("write_errors"):
            problems.append(f"{name} process: write errors {info['write_errors']}")
    summaries = {"publisher": rec.get("publisher") or {},
                 "receiver": rec.get("receiver") or {},
                 "relay": (rec.get("relay") or {}).get("stop") or {}}
    for child in rec.get("children") or []:
        if not isinstance(child, dict):
            problems.append("a child record is malformed")
            continue
        name = child.get("name")
        if not child.get("settled") or child.get("exit") is None:
            problems.append(f"child {name!r} was not settled and reaped")
        # an integer is not proof of success, and the two records must agree
        summary = summaries.get(name)
        if isinstance(summary, dict) and "exit" in summary:
            if child.get("exit") != summary.get("exit"):
                problems.append(f"child {name!r} exit {child.get('exit')!r} disagrees with "
                                f"its process record {summary.get('exit')!r}")
        if child.get("exit") not in (0, None):
            problems.append(f"child {name!r} exited {child.get('exit')!r}")
    if rec.get("threads_settled") is not True:
        problems.append("a child's readers were not settled")
    for enum_check, who in ((pub.get("enum_check"), "publisher"), (rx.get("enum_check"), "receiver")):
        if not isinstance(enum_check, dict):
            continue
        if enum_check.get("packaging_raw") != RAW_PACKAGING or \
                enum_check.get("status_normal") != NORMAL_STATUS:
            problems.append(f"{who}: the package's enums differ from the declared contract: {enum_check}")
    if negative:
        pass
    return problems


def verdict(rec):
    """The whole-run verdict. Every claim in this receipt is checked HERE, from
    the serialized evidence: a cached child opinion is never the oracle. The
    publisher's SUBMISSION receipt and the receiver's OBSERVATIONS stay apart."""
    problems = structure_problems(rec)
    if problems:
        return {"ok": False, "problems": problems, "class": "malformed receipt"}
    negative = rec.get("mode") == "negative"
    pub, rx = rec["publisher_child"], rec["receiver_child"]

    if rec.get("primary_error"):
        problems.append("supervisor error: " + str(rec["primary_error"]))
    for failure in rec.get("harness_failures", []):
        problems.append("harness: " + failure)
    for child, label in ((pub, "publisher"), (rx, "receiver")):
        for failure in child.get("harness_failures", []) or []:
            problems.append(f"{label} child: {failure}")
    for warning in rec.get("command_warnings", []):
        problems.append("unplanned diagnostic: " + str(warning)[:200])
    for error in rec.get("cleanup_errors", []) or []:
        problems.append("cleanup error: " + str(error)[:200])

    # the publisher's submission receipt
    record = pub["record"]
    expected_submitted = 0 if negative else fixture.N
    if record["stop"] != "submitted":
        problems.append(f"submission stopped at {record['stop']!r}")
    if record["submitted"] != expected_submitted:
        problems.append(f"submitted {record['submitted']} != declared {expected_submitted}")
    if record["pending"] is not None:
        problems.append(f"a submission was left pending at index {record['pending']}")
    if record["interrupted"]:
        problems.append("the submission was interrupted")
    if record["attempts"] != record["submitted"] + record["would_block"]:
        problems.append(f"attempts {record['attempts']} != submitted {record['submitted']} "
                        f"+ would_block {record['would_block']}")
    expected_ended = [] if negative else [fixture.TRACK.decode()]
    if record["ended"] != expected_ended:
        problems.append(f"ended tracks {record['ended']} != {expected_ended}")
    if record["ends_refused"]:
        problems.append(f"an end was refused: {record['ends_refused']}")
    if record["terminal"].get("fatal") or record["terminal"].get("closed"):
        problems.append(f"the sender was terminal at the end: {record['terminal']}")
    for field in DROP_FIELDS:
        if record["dropped"].get(field):
            problems.append(f"the service discarded media: {field}={record['dropped'][field]}")
        if (pub.get("stats_before_close") or {}).get(field):
            problems.append(f"a discard was observed before close: "
                            f"{field}={pub['stats_before_close'][field]}")
    if pub.get("drop_reports"):
        problems.append(f"drop reports during the run: {pub['drop_reports']}")
    if not negative:
        if record["stats"].get("objects_written") != fixture.N:
            problems.append(f"objects_written {record['stats'].get('objects_written')} != {fixture.N}")
        pre_close = pub.get("stats_before_close") or {}
        if pre_close.get("objects_written") != fixture.N:
            problems.append("the pre-close observation does not account for every written object")
        # the claim THIS receipt makes about the pre-close observation, which
        # is not a claim about the submission snapshot: that one may
        # legitimately still have media queued, and nothing here requires it
        # to be zero
        if pre_close.get("objects_sent") != fixture.N:
            problems.append(f"the pre-close observation reports {pre_close.get('objects_sent')!r} "
                            f"of {fixture.N} objects sent")
        if pre_close.get("objects_queued") != 0:
            problems.append(f"the pre-close observation still has "
                            f"{pre_close.get('objects_queued')!r} queued")
        if not isinstance(pub.get("subscriptions"), int) or pub["subscriptions"] < 1:
            problems.append(f"the service never reported a subscriber: {pub.get('subscriptions')!r}")

    # the receiver's independent observations, RECOMPUTED here
    if not rx.get("subscribed"):
        problems.append("the receiver never subscribed to the declared track")
    events = rx["events"]
    observed = [(e.get("kind"), e.get("track_name")) for e in events]
    declared_sequence = DECLARED_EVENTS["negative" if negative else "positive"]
    if observed != declared_sequence:
        problems.append(f"receiver events {observed} != declared {declared_sequence}")
    ended_events = [e for e in events if e.get("kind") == "ENDED"]
    if not negative:
        if len(ended_events) == 1 and not ended_events[0].get("wanted"):
            problems.append("the terminal event was for another track")
        if not rx.get("ended_observed"):
            problems.append("the receiver never observed the track's ENDED event")
    elif ended_events:
        problems.append("the no-media run observed a terminal event it never requested")
    inventory = rx["inventory"]
    if negative:
        if inventory:
            problems.append(f"the negative run delivered {len(inventory)} objects")
        problems.append("declared media missing: the negative run delivers none")
    else:
        problems.extend(compare_images(declared_images(), inventory))
        for problem in rx.get("problems") or []:
            problems.append("child comparison: " + problem)
        for problem in rx.get("image_problems") or []:
            problems.append("child image comparison: " + problem)

    problems.extend(provenance_problems(rec))
    problems.extend(ownership_problems(rec))
    for name, passed in relay_checks(rec).items():
        if not passed:
            problems.append("relay/process check failed: " + name)
    return {"ok": not problems, "problems": problems,
            "class": "rejected" if problems else "accepted"}


def healthy_receipt():
    """A receipt that is healthy in every checked respect, as the offline REDs'
    control. Built from the declared contract, never from a run."""
    module = "/p/moq5/_native.abi3.so"
    msquic = "/p/msquic/libmsquic.2.6.0.dylib"
    images = [module, msquic, "/usr/lib/libSystem.B.dylib",
              "/opt/homebrew/Caskroom/miniconda/base/lib/libssl.3.dylib"]
    return {
        "mode": "positive", "threads_settled": True, "harness_failures": [],
        "command_warnings": [], "primary_error": None, "cleanup_errors": [],
        "commands": [{"name": "openssl req ca", "rc": 0, "expected_rc": 0, "timeout": False}],
        "environment_notes": [],
        "relay": {"exit": 0, "how": "SIGTERM", "port": 45001,
                  "schemas": ["RELAY_READY_V1", "RELAY_STOP_V1"],
                  "readers_settled": True, "reader_errors": [],
                  "ready_record": {"schema": "RELAY_READY_V1", "listener": "raw",
                                   "host": "127.0.0.1", "port": 45001,
                                   "alpn_set": EXPECTED_ALPN, "lanes": 1},
                  "stop": {"exit": 0, "how": "SIGTERM"}},
        "publisher": {"exit": 0, "how": "exited", "timeout": False, "readers_settled": True,
                      "reader_errors": [], "write_errors": []},
        "receiver": {"exit": 0, "how": "exited", "timeout": False, "readers_settled": True,
                     "reader_errors": []},
        "children": [{"name": name, "exit": 0, "settled": True} for name in OWNED_CHILDREN],
        "controls_forwarded": [{"line": "subscribed", "forwarded": True},
                               {"line": "observed", "forwarded": True}],
        "controls_rejected": [],
        "selected_images": {role: {"path": path, "sha256_before": digest * 64,
                                   "sha256_after": digest * 64}
                            for role, path, digest in (
                                ("module", module, "a"), ("msquic", msquic, "b"),
                                ("relay", "/p/moq5-relay", "c"), ("example", "/p/publish_loop.py", "d"),
                                ("libssl", "/p/libssl.3.dylib", "e"),
                                ("libcrypto", "/p/libcrypto.3.dylib", "f"))},
        "publisher_images": list(images), "receiver_images": list(images),
        "environment_notes": [],
        "publisher_child": {
            "harness_failures": [], "drop_reports": [], "subscriptions": 1,
            "problems": [],
            "loaded_images": list(images),
            "enum_check": {"packaging_raw": RAW_PACKAGING, "status_normal": NORMAL_STATUS},
            "controls_seen": list(CONTROLS_POSITIVE),
            "control_reader": {"joined": True, "lines": list(CONTROLS_POSITIVE),
                               "malformed": [], "out_of_order": [], "duplicates": [],
                               "invalid_encoding": [], "overlong": [],
                               "partial_at_eof": None, "partial_held": None,
                               "overflow": False, "eof": False, "error": None},
            "close_sequence": ["sender", "endpoint"],
            "stats_before_close": {"objects_written": fixture.N, "objects_sent": fixture.N,
                                   "objects_queued": 0, **{f: 0 for f in DROP_FIELDS}},
            "record": {"stop": "submitted", "submitted": fixture.N, "attempts": fixture.N,
                       "pending": None, "would_block": 0,
                       "ended": [fixture.TRACK.decode()], "ends_refused": [],
                       "interrupted": False, "dropped": {f: 0 for f in DROP_FIELDS},
                       "terminal": {"closed": False, "fatal": False},
                       "stats": {"objects_written": fixture.N, "objects_sent": fixture.N,
                                 "objects_queued": 0, "bytes_queued": 0,
                                 **{f: 0 for f in DROP_FIELDS}}}},
        "receiver_child": {
            "harness_failures": [], "subscribed": True, "ended_observed": True,
            "loaded_images": list(images),
            "enum_check": {"packaging_raw": RAW_PACKAGING, "status_normal": NORMAL_STATUS},
            "close_sequence": ["receiver", "endpoint"],
            "events": [{"kind": "ADDED", "track_name": fixture.TRACK.decode(), "wanted": False},
                       {"kind": "CATALOG_READY", "track_name": None, "wanted": False},
                       {"kind": "ENDED", "track_name": fixture.TRACK.decode(), "wanted": True}],
            "inventory": declared_images(), "problems": [], "image_problems": []},
    }


def oracle_red():
    """Offline: the ACTUAL verdict must reject each named defect, by its own
    class, while the healthy control is accepted. Every case is a receipt that
    is healthy except for the defect it is named for."""
    import copy

    cases = {}
    control = verdict(healthy_receipt())
    cases["healthy"] = {"accepted": control["ok"], "class": control["class"],
                        "problems": control["problems"][:2]}

    def case(name, mutate, expect_class="rejected", expect_substring=None):
        rec = copy.deepcopy(healthy_receipt())
        mutate(rec)
        result = verdict(rec)
        matched = (not result["ok"] and result["class"] == expect_class and
                   (expect_substring is None or
                    any(expect_substring in p for p in result["problems"])))
        cases[name] = {"accepted": result["ok"], "class": result["class"],
                       "matched_expected_class": matched,
                       "problems": result["problems"][:2]}

    # --- the measured false passes of the previous verifier -----------------
    case("image inventories removed",
         lambda r: ([r.pop(k, None) for k in ("selected_images", "publisher_images",
                                              "receiver_images")],
                    r["publisher_child"].pop("loaded_images", None),
                    r["receiver_child"].pop("loaded_images", None)),
         "malformed receipt", "selected_images missing")
    case("module digest changed during the run",
         lambda r: r["selected_images"]["module"].update(sha256_after="0" * 64),
         expect_substring="digest changed during the run")
    case("module never mapped by a child",
         lambda r: r["publisher_child"].update(loaded_images=["/usr/lib/libSystem.B.dylib"]),
         expect_substring="never mapped the selected module")
    case("provider image substituted",
         lambda r: r["receiver_child"].update(loaded_images=[
             r["selected_images"]["module"]["path"],
             "/opt/homebrew/opt/libmsquic/lib/libmsquic.2.dylib"]),
         expect_substring="never mapped the selected msquic")
    case("receiver events emptied", lambda r: r["receiver_child"].update(events=[]),
         expect_substring="receiver events [] != declared")
    case("object metadata corrupted",
         lambda r: r["receiver_child"]["inventory"][0].update(packaging=999, decode_time_us=999999),
         expect_substring="object 0: packaging")
    case("object on another track",
         lambda r: r["receiver_child"]["inventory"][1].update(track_name="other"),
         expect_substring="object 1: track_name")
    case("terminal state at the end",
         lambda r: r["publisher_child"]["record"]["terminal"].update(fatal=True),
         expect_substring="terminal at the end")
    case("controls dropped and one rejected",
         lambda r: r.update(controls_forwarded=[], controls_rejected=["observed"]),
         expect_substring="control sequence")
    case("a discard before close",
         lambda r: r["publisher_child"]["stats_before_close"].update(objects_dropped=1),
         expect_substring="discard was observed before close")
    case("submission record emptied",
         lambda r: r["publisher_child"].update(record={}),
         "malformed receipt", "record.stop missing")
    case("payload not hexadecimal",
         lambda r: r["receiver_child"]["inventory"][0].update(payload="zz"),
         "malformed receipt", "not hexadecimal")

    # --- inventory, terminal and conservation --------------------------------
    case("empty inventory", lambda r: r["receiver_child"].update(inventory=[]),
         expect_substring="received 0 objects")
    case("truncated inventory", lambda r: r["receiver_child"]["inventory"].pop(),
         expect_substring="received 7 objects")
    case("duplicated object", lambda r: r["receiver_child"]["inventory"].append(
        dict(r["receiver_child"]["inventory"][0])), expect_substring="received 9 objects")

    def reorder(rec):
        inv = rec["receiver_child"]["inventory"]
        inv[0], inv[1] = inv[1], inv[0]
    case("reordered inventory", reorder, expect_substring="object 0: payload")
    case("duplicate terminal event",
         lambda r: r["receiver_child"]["events"].append(
             {"kind": "ENDED", "track_name": fixture.TRACK.decode(), "wanted": True}),
         expect_substring="!= declared")
    case("terminal event for another track",
         lambda r: r["receiver_child"]["events"].__setitem__(
             2, {"kind": "ENDED", "track_name": "other", "wanted": False}),
         expect_substring="the terminal event was for another track")
    case("missing terminal observation",
         lambda r: r["receiver_child"].update(ended_observed=False),
         expect_substring="never observed the track's ENDED")
    case("never subscribed", lambda r: r["receiver_child"].update(subscribed=False),
         expect_substring="never subscribed")
    case("no subscriber reported by the service",
         lambda r: r["publisher_child"].update(subscriptions=0),
         expect_substring="never reported a subscriber")
    case("attempts do not conserve",
         lambda r: r["publisher_child"]["record"].update(attempts=fixture.N + 2),
         expect_substring="attempts")
    case("submission stopped early",
         lambda r: r["publisher_child"]["record"].update(stop="deadline", submitted=3,
                                                         attempts=3, pending=3),
         expect_substring="submission stopped at")
    case("media discarded",
         lambda r: r["publisher_child"]["record"]["dropped"].update(objects_dropped=1),
         expect_substring="service discarded media")
    case("publisher failed despite receiver data",
         lambda r: r["publisher"].update(exit=3), expect_substring="publisher process: exit 3")

    # --- the measured receipt-boundary defects of 2014 -----------------------
    case("record.pending deleted",
         lambda r: r["publisher_child"]["record"].pop("pending"),
         "malformed receipt", "record.pending missing")
    case("a loaded image is not a path",
         lambda r: r["publisher_child"].update(loaded_images=[None]),
         "malformed receipt", "loaded_images[0] is not an absolute path")
    case("drop counters missing",
         lambda r: r["publisher_child"]["record"].update(dropped={}),
         "malformed receipt", "drop counters missing")
    case("no owned children recorded", lambda r: r.update(children=[]),
         "malformed receipt", "owned children [] !=")
    case("mode outside its domain", lambda r: r.update(mode="typo"),
         "malformed receipt", "is not one of")
    case("booleans where counts belong",
         lambda r: r["receiver_child"]["inventory"][0].update(packaging=True, status=False),
         "malformed receipt", "not a plain integer")
    case("an extra ADDED for another track",
         lambda r: r["receiver_child"]["events"].insert(
             1, {"kind": "ADDED", "track_name": "other", "wanted": False}),
         expect_substring="!= declared")
    case("the catalog event removed",
         lambda r: r["receiver_child"].update(events=[
             e for e in r["receiver_child"]["events"] if e["kind"] != "CATALOG_READY"]),
         expect_substring="!= declared")
    case("selected image roles trimmed",
         lambda r: r.update(selected_images={k: v for k, v in r["selected_images"].items()
                                             if k in ("module", "msquic")}),
         "malformed receipt", "selected image roles")
    case("the control reader accepted nothing",
         lambda r: r["publisher_child"]["control_reader"].update(lines=[]),
         expect_substring="the control reader accepted")
    case("a digest that is not hexadecimal",
         lambda r: r["selected_images"]["module"].update(sha256_before="z" * 64,
                                                         sha256_after="z" * 64),
         "malformed receipt", "not a sha256 digest")
    case("a duplicate owned child",
         lambda r: r["children"].append(dict(r["children"][1])),
         "malformed receipt", "owned children")
    case("a control reader diagnostic",
         lambda r: r["publisher_child"]["control_reader"].update(duplicates=["subscribed"]),
         expect_substring="control reader duplicates")
    case("unframed input at the end",
         lambda r: r["publisher_child"]["control_reader"].update(partial_at_eof="obse"),
         expect_substring="unframed input")

    # --- the measured receipt-boundary defects of 2016 -----------------------
    case("a command record is not a record", lambda r: r.update(commands=[None]),
         "malformed receipt", "commands[0] is not a command record")
    case("the relay stop record is not a record", lambda r: r["relay"].update(stop=1),
         "malformed receipt", "relay.stop is int")
    case("a harness failure is not text", lambda r: r.update(harness_failures=[None]),
         "malformed receipt", "harness_failures[0] is not text")
    case("an empty command record", lambda r: r.update(commands=[{}]),
         "malformed receipt", "commands[0].name missing")
    case("a child exit disagrees with its process record",
         lambda r: r["children"][1].update(exit=9),
         expect_substring="disagrees with its process record")
    case("a forwarded control is not a record",
         lambda r: r["controls_forwarded"].append(None),
         "malformed receipt", "controls_forwarded[2] is not a control record")
    case("the reader's error field is absent",
         lambda r: r["publisher_child"]["control_reader"].pop("error"),
         "malformed receipt", "control_reader.error missing")
    case("negative counts",
         lambda r: r["publisher_child"]["record"].update(attempts=-1, would_block=-9),
         "malformed receipt", "is negative")
    case("booleans in the pre-close counters",
         lambda r: r["publisher_child"]["stats_before_close"].update(
             objects_sent=True, objects_queued=False),
         "malformed receipt", "stats_before_close.objects_sent is not a plain integer")
    case("a pre-close counter is absent",
         lambda r: r["publisher_child"]["stats_before_close"].pop("objects_sent"),
         "malformed receipt", "stats_before_close.objects_sent missing")
    case("an unframed remainder at settlement",
         lambda r: r["publisher_child"]["control_reader"].update(partial_held="trailing"),
         expect_substring="unframed input")
    case("the pre-close observation still has media queued",
         lambda r: r["publisher_child"]["stats_before_close"].update(objects_queued=2),
         expect_substring="still has 2 queued")
    case("the pre-close observation is short of sent",
         lambda r: r["publisher_child"]["stats_before_close"].update(objects_sent=fixture.N - 1),
         expect_substring="of 8 objects sent")
    case("the relay ready record is malformed",
         lambda r: r["relay"]["ready_record"].update(port="45001"),
         "malformed receipt", "relay.ready_record.port")
    case("a receiver comparison problem is not text",
         lambda r: r["receiver_child"].update(problems=[None]),
         "malformed receipt", "receiver_child.problems[0] is not text")

    # --- wrong CONTAINERS (a truthy non-list), distinct from the wrong
    # --- ELEMENT cases above: `or []` would iterate these, so each path is
    # --- named here as well
    for path in ("harness_failures", "command_warnings", "cleanup_errors",
                 "commands", "controls_forwarded", "controls_rejected",
                 "publisher_child.harness_failures",
                 "publisher_child.close_sequence",
                 "receiver_child.harness_failures",
                 "receiver_child.close_sequence",
                 "receiver_child.problems", "receiver_child.image_problems",
                 "publisher_child.drop_reports", "children",
                 "publisher_child.loaded_images", "receiver_child.loaded_images",
                 "receiver_child.events", "receiver_child.inventory"):
        def wrong_container(rec, path=path):
            node, key = rec, path
            if "." in path:
                parent, key = path.split(".")
                node = rec[parent]
            node[key] = 1                              # truthy, and not a list
        case(f"a truthy non-list at {path}", wrong_container,
             "malformed receipt", f"{path.replace('.', '.')} is int, not list"
             if "." in path else f"receipt.{path} is int, not list")

    # --- ownership, controls and settlement ----------------------------------
    case("control reader never joined",
         lambda r: r["publisher_child"]["control_reader"].update(joined=False),
         expect_substring="control reader was not joined")
    case("control reader failed",
         lambda r: r["publisher_child"]["control_reader"].update(error="OSError: broken pipe"),
         expect_substring="control reader error")
    case("malformed control received",
         lambda r: r["publisher_child"]["control_reader"].update(malformed=["go"]),
         expect_substring="malformed controls")
    case("controls out of order",
         lambda r: r["publisher_child"]["control_reader"].update(out_of_order=["observed"]),
         expect_substring="out of order")
    case("publisher closed in the wrong order",
         lambda r: r["publisher_child"].update(close_sequence=["endpoint", "sender"]),
         expect_substring="publisher close order")
    case("a child was not reaped",
         lambda r: r["children"][1].update(exit=None, settled=False),
         "malformed receipt", "was not settled")
    case("reader errors in a child",
         lambda r: r["receiver"].update(reader_errors=["decode error"]),
         expect_substring="reader errors")
    case("threads not settled", lambda r: r.update(threads_settled=False),
         expect_substring="readers were not settled")
    case("the package's enums drifted",
         lambda r: r["receiver_child"]["enum_check"].update(packaging_raw=7),
         expect_substring="enums differ from the declared contract")
    case("unplanned diagnostic",
         lambda r: r["command_warnings"].append("publisher stderr: warning"),
         expect_substring="unplanned diagnostic")
    case("cleanup error", lambda r: r["cleanup_errors"].append("settle relay: boom"),
         expect_substring="cleanup error")

    ok = (cases["healthy"]["accepted"] is True and
          all(not case_["accepted"] and case_["matched_expected_class"]
              for name, case_ in cases.items() if name != "healthy"))
    print(json.dumps({"ok": ok, "cases": cases}, indent=1))
    return 0 if ok else 1


# -------------------------------------------------------------- supervisor
# The supplementary vmmap inventory is no longer invoked. Its historical
# records stay in the receipts that collected it; new runs rely on each
# child's OWN dyld report, which needs no external tool, no elevated
# privilege and no exception to this harness's diagnostic policy (every
# command it does run goes through Harness.run_cmd, whose zero-exit warnings
# are collected and fail the verdict).


def supervise(run, relay, package, negative, publisher_argv=None,
              receiver_argv=None, client_timeout=CLIENT_S):
    """The one supervisor: it owns the relay, the publisher and the receiver,
    and settles every one of them on every path. The offline ownership
    controls pass stub argvs and no relay."""
    h = Harness(run, relay)
    h.rec.update({"mode": "negative" if negative else "positive"})
    pub = rx = None
    try:
        if relay is not None:
            h.rec["credentials"] = h.make_credentials()
            port = free_udp_port()
            h.start_relay(port)
        else:
            port = 0
        env = {"PATH": "/usr/bin:/bin", "DYLD_FALLBACK_LIBRARY_PATH": MSQUIC_DIR}
        ready = threading.Event()
        pub_events = []

        def on_pub_line(line):
            if line.startswith("{"):
                try:
                    event = json.loads(line)
                except ValueError:
                    return
                pub_events.append(event)
                if event.get("event") == "ready":
                    ready.set()

        argv = publisher_argv or [sys.executable, "-I", "-W", "error",
                                  str(Path(__file__).resolve()), "--publisher",
                                  str(run), str(port), str(package),
                                  "negative" if negative else "positive"]
        pub = h.spawn("publisher", argv, env, stdin=True, on_line=on_pub_line)
        if not ready.wait(READY_S):
            if not pub.alive():
                rec = pub.settle(1)
                h.fail(f"publisher exited before ready: rc {rec['exit']}: {rec['stderr'][-300:]}")
            h.fail(f"publisher not ready within {READY_S}s")
        if relay is not None:
            selected = {"module": str(Path(package) / "moq5" / "_native.abi3.so"),
                        "example": str(EXAMPLE), "relay": str(Path(relay).resolve()),
                        "msquic": MSQUIC_IMAGE, "libssl": OPENSSL3_IMAGES[0],
                        "libcrypto": OPENSSL3_IMAGES[1]}
            h.rec["selected_images"] = {k: {"path": p, "sha256_before": sha256_file(p)}
                                        for k, p in selected.items()}
        forwarded = []
        protocol = ControlProtocol(CONTROLS_POSITIVE)

        subscribed = threading.Event()

        def on_rx_line(line):
            if line.startswith("CTRL "):
                control = line[5:]
                if protocol.admit(control):
                    forwarded.append({"line": control, "forwarded": pub.write_line(control),
                                      "t": round(time.monotonic(), 6)})
                    if control == "subscribed":
                        subscribed.set()

        rargv = receiver_argv or [sys.executable, "-I", "-W", "error",
                                  str(Path(__file__).resolve()), "--receiver",
                                  str(run), str(port), str(package),
                                  "negative" if negative else "positive"]
        rx = h.spawn("receiver", rargv, env, on_line=on_rx_line)
        if relay is not None:
            # the receiver reports its own loaded images once it has
            # subscribed; the supervisor waits for that control so a failure
            # to subscribe is a named harness failure rather than a silent
            # missing inventory
            if not subscribed.wait(SUBSCRIBE_S):
                if not rx.alive():
                    rec = rx.settle(1)
                    h.fail(f"receiver exited before subscribing: rc {rec['exit']}: {rec['stderr'][-300:]}")
                h.fail(f"the receiver did not subscribe within {SUBSCRIBE_S}s")
        rrec = rx.settle(client_timeout)
        h.rec["commands"].append(rrec)
        h.rec["command_warnings"].extend(collect_diagnostics(rrec))
        h.rec["receiver"] = {k: rrec[k] for k in ("pid", "exit", "how", "readers_settled",
                                                  "reader_errors", "timeout")}
        h.rec["controls_forwarded"] = forwarded
        h.rec["controls_rejected"] = protocol.rejected
        # The receiver has finished observing; only now may the publisher let
        # its owners go. It closes on its own, in its owning thread.
        prec = pub.settle(PUBLISHER_EXIT_S)
        h.rec["commands"].append(prec)
        h.rec["command_warnings"].extend(collect_diagnostics(prec))
        h.rec["publisher"] = {k: prec[k] for k in ("pid", "exit", "how", "readers_settled",
                                                   "reader_errors", "write_errors", "timeout", "stderr")}
        h.rec["publisher"]["events"] = pub_events
        if rrec["timeout"]:
            h.fail(f"receiver child: {rrec['how']} (deadline {client_timeout}s)")
        if rrec["exit"] != 0:
            h.fail(f"receiver child: rc {rrec['exit']}: {rrec['stderr'][-400:]}")
    except BaseException as error:                          # noqa: BLE001
        h.rec["primary_error"] = repr(error)
    finally:
        h.settle_all([(c, PUBLISHER_EXIT_S if c is pub else 5.0)
                      for c in (rx, pub) if c is not None])
        for key, child in (("receiver", rx), ("publisher", pub)):
            if child is not None and key not in h.rec and child.settled:
                h.rec[key] = {k: child.settled[k] for k in ("pid", "exit", "how", "readers_settled",
                                                            "reader_errors", "timeout", "stderr")}
                if key == "publisher":
                    h.rec[key]["events"] = pub_events
        try:
            if relay is not None and hasattr(h, "relay"):
                h.stop_relay()
        except BaseException as error:                      # noqa: BLE001
            h.rec["cleanup_errors"].append(repr(error))
        h.rec["threads_settled"] = all(c.settled and c.settled["readers_settled"] for c in h.children)
        for _key, value in (h.rec.get("selected_images") or {}).items():
            try:
                value["sha256_after"] = sha256_file(value["path"])
            except OSError as error:
                value["sha256_after"] = repr(error)
        h.rec["children"] = [{"name": c.name, "pid": c.proc.pid, "exit": c.proc.poll(),
                              "settled": bool(c.settled), "how": c.settled and c.settled["how"]}
                             for c in h.children]
        for key, name in (("publisher_child", "publisher.json"), ("receiver_child", "receiver.json")):
            path = run / name
            if not path.exists():
                h.rec[key] = {"harness_failures": [f"{name} was never written"]}
                continue
            try:
                h.rec[key] = json.loads(path.read_text())
            except BaseException as error:                  # noqa: BLE001
                # the receipt survives: the parse failure is recorded, the
                # primary error is not replaced, and the children are already
                # settled above
                h.rec["cleanup_errors"].append(f"{name}: {error!r}")
                h.rec[key] = {"harness_failures": [f"{name} could not be read: {error!r}"]}
        h.rec["verdict"] = verdict(h.rec) if relay is not None else None
        (run / "results.json").write_text(json.dumps(h.rec, indent=1, default=str))
    return h.rec


def child_controls():
    """Offline ownership controls through THIS supervisor, with stub children:
    no relay, no network, no real unrelated PID. Every owned handle must be
    settled by the supervisor itself and every error retained."""
    root = HERE.parent.parent.parent.parent / "build" / "py-publisher-fixture"
    root.mkdir(parents=True, exist_ok=True)
    base = root / ("child-controls-" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
    base.mkdir(mode=0o700)
    py = sys.executable
    ready_stub = [py, "-c", "import sys, time; print('{\"event\":\"ready\"}', flush=True); time.sleep(60)"]
    results = {}

    # 1. a receiver that never returns, with a live owned publisher
    (base / "receiver_timeout").mkdir()
    rec = supervise(base / "receiver_timeout", None, None, False,
                    publisher_argv=ready_stub,
                    receiver_argv=[py, "-c", "import time; time.sleep(60)"],
                    client_timeout=2.0)
    results["receiver_timeout"] = {
        "primary_error": rec["primary_error"],
        "children_settled": all(c["settled"] for c in rec["children"]),
        "all_reaped": all(c["exit"] is not None for c in rec["children"]),
        "threads_settled": rec["threads_settled"]}

    # 2. a publisher that never announces readiness
    (base / "no_ready").mkdir()
    rec = supervise(base / "no_ready", None, None, False,
                    publisher_argv=[py, "-c", "import time; time.sleep(60)"],
                    receiver_argv=[py, "-c", "pass"], client_timeout=2.0)
    results["publisher_never_ready"] = {
        "primary_error": rec["primary_error"],
        "children_settled": all(c["settled"] for c in rec["children"]),
        "all_reaped": all(c["exit"] is not None for c in rec["children"])}

    # 3. a publisher that exits before readiness
    (base / "early_exit").mkdir()
    rec = supervise(base / "early_exit", None, None, False,
                    publisher_argv=[py, "-c", "raise SystemExit(7)"],
                    receiver_argv=[py, "-c", "pass"], client_timeout=2.0)
    results["publisher_early_exit"] = {
        "primary_error": rec["primary_error"],
        "all_reaped": all(c["exit"] is not None for c in rec["children"])}

    # 4. a receiver that fails: its status is the verdict's, not a log marker
    (base / "receiver_failed").mkdir()
    rec = supervise(base / "receiver_failed", None, None, False,
                    publisher_argv=ready_stub,
                    receiver_argv=[py, "-c", "import sys; print('CTRL subscribed', flush=True); sys.exit(4)"],
                    client_timeout=5.0)
    results["receiver_failed"] = {
        "primary_error": rec["primary_error"],
        "receiver_exit": (rec.get("receiver") or {}).get("exit"),
        "all_reaped": all(c["exit"] is not None for c in rec["children"])}

    # 5. a malformed child receipt: the run record survives, the children are
    #    still reaped, and the primary attribution is retained
    (base / "malformed_receipt").mkdir()
    (base / "malformed_receipt" / "receiver.json").write_text("{not json")
    rec = supervise(base / "malformed_receipt", None, None, False,
                    publisher_argv=ready_stub,
                    receiver_argv=[py, "-c", "import sys; print('CTRL subscribed', flush=True); sys.exit(4)"],
                    client_timeout=5.0)
    results["malformed_receipt"] = {
        "primary_error": rec["primary_error"],
        "cleanup_errors": rec["cleanup_errors"],
        "receiver_child": rec.get("receiver_child"),
        "all_reaped": all(c["exit"] is not None for c in rec["children"]),
        "verdict_class": (verdict(rec) or {}).get("class")}

    ok = (results["malformed_receipt"]["all_reaped"]
          and results["malformed_receipt"]["primary_error"]
          and results["malformed_receipt"]["cleanup_errors"]
          and results["receiver_timeout"]["all_reaped"] and results["receiver_timeout"]["threads_settled"]
          and results["receiver_timeout"]["primary_error"]
          and results["publisher_never_ready"]["primary_error"]
          and results["publisher_never_ready"]["all_reaped"]
          and results["publisher_early_exit"]["primary_error"]
          and results["publisher_early_exit"]["all_reaped"]
          and results["receiver_failed"]["receiver_exit"] == 4
          and results["receiver_failed"]["primary_error"]
          and results["receiver_failed"]["all_reaped"])
    print(json.dumps({"ok": bool(ok), "dir": str(base), "results": results}, indent=1, default=str))
    return 0 if ok else 1


def reader_child(case):
    """Run the ACTUAL ControlReader against this process's stdin and report
    its settled disposition. Used only by the offline reader controls.

    "live" reports while the writer is still OPEN, which is what catches a
    reader that only catches up at EOF; the other cases report once the input
    has been consumed or the descriptor has closed.
    """
    reader = ControlReader(CONTROLS_POSITIVE)
    reader.start()
    if case == "silent":
        time.sleep(4.0)                                     # never reports
        reader.settle(READER_STOP_S)
        return 0
    if case == "partial_report":
        sys.stdout.write('{"joi')                           # no newline, stays alive
        sys.stdout.flush()
        time.sleep(4.0)
        reader.settle(READER_STOP_S)
        return 0
    if case == "live":
        time.sleep(0.8)
    elif case == "before_ready":
        time.sleep(0.2)                                     # nothing arrives
    else:
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            with reader._lock:                              # noqa: SLF001 (same module)
                done = reader.eof or reader.overflow or (
                    len(reader.lines) + len(reader.malformed) + len(reader.duplicates)
                    + len(reader.invalid_encoding) + len(reader.overlong) >= 2)
            if done:
                break
            time.sleep(0.02)
    print(json.dumps(reader.settle(READER_STOP_S)))
    return 0


def reader_controls():
    """The publisher's control reader, exercised offline through its REAL
    path in children this function owns and reaps. No network, no relay, no
    foreign PID, no unbounded thread.

    Two of these are the measured live-writer cases: a reader that frames on
    a buffered readline loses the second line of a single write, and stalls
    (never joins) on a partial line, because both wait for an EOF the writer
    has not sent.
    """
    import subprocess

    def read_report(proc, deadline_s=REPORT_S, limit=REPORT_BYTES):
        """The child's one-line report, read from the descriptor with a bound.

        A blocking readline cannot be bounded and would hang this gate on a
        child that stays alive without finishing its line, so the report is
        framed here the same way the reader frames its own input.
        """
        fd = proc.stdout.fileno()
        buffer = b""
        end = time.monotonic() + deadline_s
        while time.monotonic() < end:
            ready, _, _ = select.select([fd], [], [], 0.05)
            if not ready:
                continue
            chunk = os.read(fd, 4096)
            if chunk == b"":
                return buffer, ("the reader child closed its report unfinished"
                                if buffer else "the reader child wrote no report")
            buffer += chunk
            if len(buffer) > limit:
                return buffer[:limit], "the reader child's report exceeded its bound"
            if b"\n" in buffer:
                return buffer.split(b"\n", 1)[0], None
        return buffer, ("the reader child's report was incomplete within its bound"
                        if buffer else "the reader child did not report within its bound")

    def run_case(name, mode, writes, close_stdin):
        argv = [sys.executable, "-I", "-W", "error", str(Path(__file__).resolve()),
                "--reader-child", mode]
        proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        report, failure = {}, None
        try:
            for chunk in writes:
                proc.stdin.write(chunk)
                proc.stdin.flush()
                time.sleep(0.05)
            if close_stdin:
                proc.stdin.close()
            line, failure = read_report(proc)
            if failure is None:
                report = json.loads(line) if line.strip() else {}
        except BaseException as error:                      # noqa: BLE001
            failure = repr(error)
        finally:
            try:
                if not proc.stdin.closed:
                    proc.stdin.close()
            except BaseException:                           # noqa: BLE001
                pass
            try:
                proc.wait(timeout=CHILD_STOP_S)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=CHILD_STOP_S)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=CHILD_STOP_S)
                failure = failure or "the reader child had to be signalled"
            stderr = proc.stderr.read().decode(errors="replace").strip()
            proc.stdout.close()
            proc.stderr.close()
        report["rc"], report["stderr"], report["harness"] = proc.returncode, stderr, failure
        return report

    both = [b"subscribed\nobserved\n"]
    cases = {
        "both lines in one write, writer open": (
            run_case("live_both", "live", both, False),
            lambda r: r["joined"] and r["lines"] == list(CONTROLS_POSITIVE)
            and not r["eof"] and not r["partial_held"]),
        "partial line, writer open": (
            run_case("live_partial", "live", [b"sub"], False),
            lambda r: r["joined"] and r["lines"] == [] and r["partial_held"] == "sub"
            and not r["eof"]),
        "lines split across writes": (
            run_case("split", "until", [b"subs", b"cribed\nobse", b"rved\n"], True),
            lambda r: r["joined"] and r["lines"] == list(CONTROLS_POSITIVE)),
        "complete lines then EOF": (
            run_case("complete", "until", both, True),
            lambda r: r["joined"] and r["lines"] == list(CONTROLS_POSITIVE) and r["eof"]),
        "EOF with nothing": (
            run_case("eof", "until", [], True),
            lambda r: r["joined"] and r["eof"] and r["lines"] == []),
        "partial line at EOF": (
            run_case("partial_eof", "until", [b"sub"], True),
            lambda r: r["joined"] and r["eof"] and r["lines"] == []
            and r["partial_at_eof"] == "sub"),
        "unknown control": (
            run_case("malformed", "until", [b"go\nsubscribed\n"], True),
            lambda r: r["joined"] and r["malformed"] == ["go"] and r["lines"] == ["subscribed"]),
        "out of order": (
            run_case("out_of_order", "until", [b"observed\nsubscribed\n"], True),
            lambda r: r["joined"] and r["out_of_order"][:1] == ["observed"]
            and r["lines"] == ["observed", "subscribed"]),
        "duplicate control": (
            run_case("duplicate", "until", [b"subscribed\nsubscribed\n"], True),
            lambda r: r["joined"] and r["duplicates"] == ["subscribed"]
            and r["lines"] == ["subscribed"]),
        "invalid encoding": (
            run_case("encoding", "until", [b"\xff\xfe\nsubscribed\n"], True),
            lambda r: r["joined"] and r["invalid_encoding"] == ["fffe"]
            and r["lines"] == ["subscribed"]),
        "overlong line": (
            run_case("overlong", "until", [b"s" * (MAX_MESSAGE_BYTES + 8) + b"\n"], True),
            lambda r: r["joined"] and r["overlong"] and r["lines"] == []
            and not r["malformed"]),
        # the regression control for the framing correction: a newline is the
        # ONLY delimiter, so padded and carriage-returned words are malformed
        "whitespace is not normalised": (
            run_case("whitespace", "live", [b" subscribed \t\nobserved\r\n"], False),
            lambda r: r["joined"] and r["lines"] == []
            and r["malformed"] == [" subscribed \t", "observed\r"]
            and not r["out_of_order"] and not r["duplicates"]
            and not r["invalid_encoding"] and not r["overlong"]),
        "bounded stop holding an incomplete line": (
            run_case("stop_partial", "live", [b"subscribed\nobse"], False),
            lambda r: r["joined"] and r["lines"] == ["subscribed"]
            and r["partial_held"] == "obse"),
    }
    # a child that never finishes its report must be a NAMED failure and must
    # still be reaped within the bound: both are driven here, not hypothesised
    cases["no report at all"] = (
        run_case("silent", "silent", [], False),
        lambda r: r.get("harness") == "the reader child did not report within its bound"
        and r.get("rc") is not None)
    cases["a partial report, child alive"] = (
        run_case("partial_report", "partial_report", [], False),
        lambda r: r.get("harness") == "the reader child's report was incomplete within its bound"
        and r.get("rc") is not None)

    expected_failures = {"no report at all", "a partial report, child alive"}
    results = {}
    for name, (report, expectation) in cases.items():
        try:
            as_declared = bool(expectation(report))
        except BaseException as error:                      # noqa: BLE001
            as_declared, report = False, {**report, "expectation_error": repr(error)}
        if name not in expected_failures:
            as_declared = (as_declared and report.get("rc") == 0
                           and not report.get("stderr") and not report.get("harness"))
        results[name] = {"as_declared": bool(as_declared), "report": report}
    ok = all(v["as_declared"] for v in results.values())
    print(json.dumps({"ok": ok, "results": results}, indent=1))
    return 0 if ok else 1


def diagnostic_controls():
    """The harness's diagnostic policy, exercised through the ACTUAL command
    path it uses. The supplementary vmmap call is gone, so every command a run
    makes goes through Harness.run_cmd: a zero-exit command that prints a
    warning must still be collected as an unplanned diagnostic and must fail
    the verdict. Nothing is suppressed and no observed status is adopted as
    the expected one."""
    root = HERE.parent.parent.parent.parent / "build" / "py-publisher-fixture"
    root.mkdir(parents=True, exist_ok=True)
    base = root / ("diagnostic-controls-" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
    base.mkdir(mode=0o700)
    results = {}

    h = Harness(base, None)
    h.run_cmd("zero exit with a warning", ["/bin/sh", "-c",
                                           "echo 'tool: warning: review probe' >&2"])
    results["zero_exit_warning"] = {
        "command_warnings": list(h.rec["command_warnings"]),
        "collected": bool(h.rec["command_warnings"])}
    receipt = healthy_receipt()
    receipt["command_warnings"] = list(h.rec["command_warnings"])
    result = verdict(receipt)
    results["zero_exit_warning"]["verdict_rejects"] = not result["ok"]
    results["zero_exit_warning"]["problem"] = next(
        (p for p in result["problems"] if "unplanned diagnostic" in p), None)

    h2 = Harness(base, None)
    failed = None
    try:
        h2.run_cmd("unexpected failure", ["/bin/sh", "-c", "exit 9"])
    except BaseException as error:                          # noqa: BLE001
        failed = repr(error)
    results["unexpected_failure"] = {
        "raised": failed, "harness_failures": list(h2.rec["harness_failures"])}

    h3 = Harness(base, None)
    h3.run_cmd("clean command", ["/bin/sh", "-c", "exit 0"])
    results["clean_command"] = {"command_warnings": list(h3.rec["command_warnings"])}

    ok = (results["zero_exit_warning"]["collected"]
          and results["zero_exit_warning"]["verdict_rejects"]
          and results["zero_exit_warning"]["problem"]
          and results["unexpected_failure"]["raised"]
          and results["unexpected_failure"]["harness_failures"]
          and not results["clean_command"]["command_warnings"])
    print(json.dumps({"ok": bool(ok), "dir": str(base), "results": results}, indent=1))
    return 0 if ok else 1


def verdict_red(run_dir):
    """Post-run: the verdict must reject mutations of a REAL receipt."""
    import copy

    source = json.loads((Path(run_dir) / "results.json").read_text())
    base = verdict(source)
    cases = {"as recorded": (base["ok"], base["problems"][:3])}

    def case(name, mutate):
        rec = copy.deepcopy(source)
        mutate(rec)
        result = verdict(rec)
        cases[name] = (result["ok"], result["problems"][:3])

    case("one payload byte changed", lambda r: r["receiver_child"]["inventory"][0].update(
        payload=(bytes.fromhex(r["receiver_child"]["inventory"][0]["payload"])[:-1] + b"\xff").hex()))
    case("an object dropped", lambda r: r["receiver_child"]["inventory"].pop())
    case("terminal observation removed", lambda r: r["receiver_child"].update(ended_observed=False))
    case("submission stopped early", lambda r: r["publisher_child"]["record"].update(
        stop="deadline", submitted=1, pending=1))
    ok = base["ok"] and all(not accepted for name, (accepted, _) in cases.items()
                            if name != "as recorded")
    print(json.dumps({"ok": bool(ok), "cases": {k: {"accepted": v[0], "problems": v[1]}
                                                for k, v in cases.items()}}, indent=1))
    return 0 if ok else 1


def main(argv):
    if "--oracle-red" in argv:
        return oracle_red()
    if "--child-controls" in argv:
        return child_controls()
    if "--reader-controls" in argv:
        return reader_controls()
    if "--diagnostic-controls" in argv:
        return diagnostic_controls()
    if "--reader-child" in argv:
        return reader_child(argv[argv.index("--reader-child") + 1])
    if "--verdict-red" in argv:
        return verdict_red(argv[argv.index("--verdict-red") + 1])
    if "--publisher" in argv:
        i = argv.index("--publisher")
        return publisher_main(Path(argv[i + 1]), int(argv[i + 2]), argv[i + 3],
                              argv[i + 4] == "negative")
    if "--receiver" in argv:
        i = argv.index("--receiver")
        return receiver_main(Path(argv[i + 1]), int(argv[i + 2]), argv[i + 3],
                             argv[i + 4] == "negative")
    package = argv[argv.index("--package") + 1]
    relay = argv[argv.index("--relay") + 1]
    negative = "--negative" in argv
    root = HERE.parent.parent.parent.parent / "build" / "py-publisher-fixture"
    root.mkdir(parents=True, exist_ok=True)
    run = root / (("negative-" if negative else "positive-") +
                  datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
    run.mkdir(mode=0o700)
    rec = supervise(run, relay, package, negative)
    result = rec.get("verdict") or {"ok": False, "problems": ["no verdict"]}
    print(json.dumps({"run": str(run), "ok": result["ok"],
                      "problems": result["problems"][:8]}, indent=1, default=str))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
