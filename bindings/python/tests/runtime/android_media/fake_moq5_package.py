"""A fake *installed* moq5 package for the host CLI's subprocess tests.

`write_fake_package(root, scenario)` writes `<root>/moq5/__init__.py` exposing
the published public names the host child uses (their field lists and member
names are pinned to the real package source by the tests), a `scenario.json`
that scripts one run, and an empty `calls.jsonl` the fake appends to. Importing
it installs a socket tripwire, so a child using it cannot reach a network.

Scenario modes: happy, never_active, attach_refused, never_established,
stdout_noise, media_corrupt, fatal_after_go.
"""
import json
from pathlib import Path

_SOURCE = r'''
"""FAKE moq5 for offline host-child tests: published shapes, scripted behaviour."""
import json, os, socket, struct, sys, time
from dataclasses import dataclass, field
from enum import IntEnum

def _trip(*a, **k):
    raise RuntimeError("fake moq5: network use is refused offline")
socket.socket.connect = _trip
socket.socket.bind = _trip

_HERE = os.path.dirname(os.path.abspath(__file__))
_SCENARIO = json.load(open(os.path.join(_HERE, "scenario.json")))

def _log(**entry):
    with open(os.path.join(_HERE, "calls.jsonl"), "a") as f:
        f.write(json.dumps(entry) + "\n")

_log(call="import", file=os.path.abspath(__file__))


class Protocol(IntEnum):
    AUTO = 0
    RAW_QUIC = 1
    WEBTRANSPORT = 2

class Backend(IntEnum):
    AUTO = 0
    PICOQUIC = 1
    MVFST = 2
    PROXYGEN = 3
    MSQUIC = 4
    WTQUIC_NETWORK = 5
    WTQUIC_MSQUIC = 6

class WtProfile(IntEnum):
    BACKEND_DEFAULT = 0
    CURRENT = 1
    D13_14_COMPAT = 2

class Version(IntEnum):
    DRAFT_16 = 0xff000010
    DRAFT_18 = 0xff000012

class EndpointState(IntEnum):
    CONNECTING = 0
    ESTABLISHED = 1
    RECONNECTING = 2
    DRAINING = 3
    CLOSED = 4

class TerminalReason(IntEnum):
    NONE = 0
    CLEAN = 1
    PROTOCOL = 2
    TLS_CERTIFICATE = 3
    TLS = 4
    TRANSPORT = 5

class OverflowPolicy(IntEnum):
    DROP_TO_KEYFRAME = 0
    DROP_GROUP = 1
    FLOW_CONTROL = 2

class TimeMode(IntEnum):
    RAW = 0
    SHARED_EPOCH = 1

class TrackEventKind(IntEnum):
    ADDED = 1
    UPDATED = 2
    REMOVED = 3
    ENDED = 4
    CATALOG_READY = 5
    UPDATE_OK = 6
    PARSE_DROP = 7

class TrackState(IntEnum):
    DISCOVERED = 0
    PENDING = 1
    ACTIVE = 2
    PAUSED_APP = 3
    PAUSED_FLOW = 4
    ENDED = 5

class PollOutcome(IntEnum):
    EMPTY = 1
    INTERRUPTED = -2
    CLOSED = -3

class ObjectStatus(IntEnum):
    NORMAL = 0
    END_OF_GROUP = 3
    END_OF_TRACK = 4

class WaitResult(IntEnum):
    WOKEN = 0
    TIMED_OUT = 1
    INTERRUPTED = -2
    CLOSED = -3

class Packaging(IntEnum):
    RAW = 0
    CMAF = 1

class MediaType(IntEnum):
    VIDEO = 0
    AUDIO = 1


class MoqError(RuntimeError):
    def __init__(self, code, operation, message):
        super().__init__(message)
        self.code, self.operation, self.message = code, operation, message
    def __str__(self):
        return f"{self.operation}: {self.message} ({self.code})"


@dataclass(frozen=True)
class Terminal:
    reason: object = TerminalReason.NONE


@dataclass(frozen=True)
class EndpointConfig:
    url: str
    protocol: Protocol = Protocol.AUTO
    backend: Backend = Backend.AUTO
    versions: tuple = ()
    sni: object = None
    ca_file: object = None
    wt_path: object = None
    wt_profile: WtProfile = WtProfile.BACKEND_DEFAULT
    handshake_timeout_us: int = 0


@dataclass(frozen=True)
class ReceiverConfig:
    namespace: tuple
    overflow: OverflowPolicy
    catalog_track: bytes = b""
    auto_subscribe: bool = False
    time_mode: TimeMode = TimeMode.RAW
    max_objects: int = 0
    max_bytes: int = 0
    max_track_events: int = 0

    @classmethod
    def flow_control(cls, namespace, **fields):
        return cls(namespace=namespace, overflow=OverflowPolicy.FLOW_CONTROL, **fields)


@dataclass(frozen=True, slots=True)
class TrackEvent:
    kind: object
    track: object
    description: object
    largest: object = None
    expires_ms: object = None
    parse_drop: object = None


@dataclass(frozen=True, slots=True)
class MediaObject:
    track: object
    config_generation: int = 0
    packaging: object = Packaging.RAW
    status: object = ObjectStatus.NORMAL
    end_of_group: bool = False
    datagram: bool = False
    keyframe: bool = False
    capture_time_us: object = None
    decode_time_us: int = 0
    composition_offset_us: int = 0
    presentation_time_us: int = 0
    payload: bytes = b""
    fragment: bytes = b""
    mdat_offset: int = 0
    mdat_len: int = 0
    samples: tuple = ()
    identity: object = None

    @property
    def is_status_only(self):
        return self.status != ObjectStatus.NORMAL


@dataclass(frozen=True, slots=True)
class ReceiverStats:
    objects_received: int = 0
    objects_queued: int = 0
    bytes_queued: int = 0
    objects_dropped: int = 0
    groups_dropped: int = 0
    keyframes_dropped: int = 0
    parse_drops: int = 0
    overflow_events: int = 0
    pause_transitions: int = 0
    paused: bool = False
    catalog_drops: object = 0
    catalog_complete: object = False


@dataclass(frozen=True, slots=True)
class ReceiverTerminal:
    closed: bool = False
    fatal: bool = False
    fatal_code: int = 0
    endpoint: object = None
    origin: object = 0


class _Description:
    def __init__(self, name):
        self.name, self.media_type, self.packaging = name, MediaType.VIDEO, Packaging.RAW


class Track:
    def __init__(self, name):
        self.description = _Description(name)


def _enum_name(v):
    return v.name if hasattr(v, "name") else v


class Endpoint:
    def __new__(cls):
        raise TypeError("Use Endpoint.connect(EndpointConfig(...))")

    @classmethod
    def connect(cls, config):
        if not isinstance(config, EndpointConfig):
            raise TypeError("config must be EndpointConfig")
        mode = _SCENARIO["mode"]
        if mode == "stdout_noise":
            print("fake moq5: connecting (noise on stdout)")
        _log(call="connect", url=config.url, protocol=_enum_name(config.protocol), backend=_enum_name(config.backend),
             versions=[_enum_name(Version(v)) if v in list(Version) else v for v in config.versions], sni=config.sni,
             ca_file=config.ca_file, wt_path=config.wt_path, handshake_timeout_us=config.handshake_timeout_us)
        e = object.__new__(cls)
        e._state = EndpointState.CONNECTING
        e._waits = 0
        e.closed = False
        return e

    @property
    def state(self):
        if _SCENARIO["mode"] != "never_established" and self._waits >= 1:
            return EndpointState.ESTABLISHED
        return self._state

    @property
    def terminal(self):
        return Terminal()

    @property
    def negotiated_version(self):
        return int(Version.DRAFT_18)

    def wait(self, timeout_us):
        self._waits += 1
        time.sleep(min(timeout_us / 1e6, 0.02))
        return WaitResult.WOKEN

    def set_interrupted(self, interrupted):
        pass

    def close(self):
        _log(call="endpoint_close")
        self.closed = True


class Receiver:
    def __new__(cls):
        raise TypeError("Use Receiver.attach(endpoint, ReceiverConfig(...))")

    @classmethod
    def attach(cls, endpoint, config):
        if not isinstance(endpoint, Endpoint):
            raise TypeError("endpoint must be Endpoint")
        if not isinstance(config, ReceiverConfig):
            raise TypeError("config must be ReceiverConfig")
        _log(call="attach", namespace=[p.decode() for p in config.namespace], overflow=_enum_name(config.overflow),
             auto_subscribe=config.auto_subscribe, catalog_track=config.catalog_track.decode())
        if _SCENARIO["mode"] == "attach_refused":
            raise MoqError(-22, "attach", "receiver attach refused")
        r = object.__new__(cls)
        r._video = Track(b"video")
        r._events = [TrackEvent(TrackEventKind.ADDED, r._video, r._video.description),
                     TrackEvent(TrackEventKind.CATALOG_READY, None, None)]
        r._objects, r._waits, r._active = [], 0, False
        r._stats, r._terminal = ReceiverStats(), ReceiverTerminal()
        r._interrupted = False
        return r

    def _media(self):
        data = open(_SCENARIO["objects"], "rb").read()
        out, off, head = [], 0, struct.Struct("<IBQ")
        while off < len(data):
            n, sync, pts = head.unpack_from(data, off)
            off += head.size
            payload = data[off:off + n]
            off += n
            out.append(MediaObject(track=self._video, keyframe=bool(sync), presentation_time_us=pts, payload=payload))
        if _SCENARIO["mode"] == "media_corrupt":
            p = bytearray(out[12].payload); p[-1] ^= 0x01
            out[12] = MediaObject(track=self._video, keyframe=out[12].keyframe,
                                  presentation_time_us=out[12].presentation_time_us, payload=bytes(p))
        return out

    def poll_track(self):
        return self._events.pop(0) if self._events else (PollOutcome.CLOSED if self._terminal.closed else PollOutcome.EMPTY)

    def poll_object(self):
        if self._interrupted:
            return PollOutcome.INTERRUPTED
        if _SCENARIO["mode"] == "stdout_noise" and self._objects:
            print("fake moq5: object delivered (noise on stdout)")
        return self._objects.pop(0) if self._objects else (PollOutcome.CLOSED if self._terminal.closed else PollOutcome.EMPTY)

    def wait(self, timeout_us):
        self._waits += 1
        time.sleep(min(timeout_us / 1e6, 0.02))
        mode = _SCENARIO["mode"]
        if self._waits == 1 and mode != "never_active":
            self._active = True
        elif self._waits == 2 and self._active:
            if mode == "fatal_after_go":
                self._terminal = ReceiverTerminal(closed=False, fatal=True, fatal_code=11)
                self._interrupted = True
            else:
                self._objects.extend(self._media())
        elif self._waits == 3 and self._active:
            self._events.append(TrackEvent(TrackEventKind.ENDED, self._video, self._video.description))
        elif self._waits == 4 and self._active:
            self._events.append(TrackEvent(TrackEventKind.REMOVED, self._video, self._video.description))
            self._stats = ReceiverStats(catalog_complete=True)
        return WaitResult.WOKEN

    def subscribe(self, track, *, start=None, priority=None):
        _log(call="subscribe", track=track.description.name.decode())

    def unsubscribe(self, track):
        _log(call="unsubscribe")

    def track_state(self, track):
        return TrackState.ACTIVE if self._active else TrackState.PENDING

    def stats(self):
        return self._stats

    @property
    def terminal(self):
        return self._terminal

    def drained(self):
        return self._terminal.closed and not self._interrupted and not self._events and not self._objects

    def close(self):
        _log(call="receiver_close")
'''


def write_fake_package(root, scenario):
    """Writes the fake package under `root` (the --package directory). Returns the calls log path."""
    pkg = Path(root) / "moq5"
    pkg.mkdir(parents=True, exist_ok=True)
    (pkg / "__init__.py").write_text(_SOURCE)
    (pkg / "scenario.json").write_text(json.dumps(scenario))
    calls = pkg / "calls.jsonl"
    calls.write_text("")
    return calls


def read_calls(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]
