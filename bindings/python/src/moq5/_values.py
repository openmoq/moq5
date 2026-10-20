"""Checked values for the caller-driven endpoint foundation and receiver shell."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING, TypeVar, TypedDict

from . import _native

if TYPE_CHECKING:
    from ._receiver import Receiver

_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1
_INT64_MAX = (1 << 63) - 1
_Enum = TypeVar("_Enum", bound=IntEnum)
_constants = _native.constants


class Protocol(IntEnum):
    """Transport protocol; AUTO derives it from the URL scheme."""

    AUTO = _constants["TRANSPORT_PROTOCOL_AUTO"]
    RAW_QUIC = _constants["TRANSPORT_PROTOCOL_RAW_QUIC"]
    WEBTRANSPORT = _constants["TRANSPORT_PROTOCOL_WEBTRANSPORT"]


class Backend(IntEnum):
    """Native backend selection, subject to the installed SDK's capabilities."""

    AUTO = _constants["TRANSPORT_BACKEND_AUTO"]
    PICOQUIC = _constants["TRANSPORT_BACKEND_PICOQUIC"]
    MVFST = _constants["TRANSPORT_BACKEND_MVFST"]
    PROXYGEN = _constants["TRANSPORT_BACKEND_PROXYGEN"]
    MSQUIC = _constants["TRANSPORT_BACKEND_MSQUIC"]
    WTQUIC_NETWORK = _constants["TRANSPORT_BACKEND_WTQUIC_NETWORK"]
    WTQUIC_MSQUIC = _constants["TRANSPORT_BACKEND_WTQUIC_MSQUIC"]


class WtProfile(IntEnum):
    """WebTransport wire dialect, independent of the MoQ version offer."""

    BACKEND_DEFAULT = _constants["WT_PROFILE_BACKEND_DEFAULT"]
    CURRENT = _constants["WT_PROFILE_CURRENT"]
    D13_14_COMPAT = _constants["WT_PROFILE_D13_14_COMPAT"]


class UriMode(IntEnum):
    """How the endpoint reads the URL.

    STANDARD is opt-in and reads it as the MOQT URI scheme defines it (port 443 when
    omitted; the URL's own path, or "/", is the WebTransport path) and takes
    no wt_path override. LEGACY is the default and original reading: lowercase moqt, moq
    and https schemes, port 4433 for moqt and moq, the wt_path override and
    the "/moq" WebTransport default.
    """

    STANDARD = _constants["ENDPOINT_URI_STANDARD"]
    LEGACY = _constants["ENDPOINT_URI_LEGACY"]


class Version(IntEnum):
    """Named wire versions; availability is checked by the native service."""

    DRAFT_16 = _constants["VERSION_DRAFT_16"]
    DRAFT_18 = _constants["VERSION_DRAFT_18"]
    DRAFT_22 = _constants["VERSION_DRAFT_22"]


class EndpointState(IntEnum):
    """Known connection states, separate from native resource release."""

    CONNECTING = _constants["ENDPOINT_CONNECTING"]
    ESTABLISHED = _constants["ENDPOINT_ESTABLISHED"]
    RECONNECTING = _constants["ENDPOINT_RECONNECTING"]
    DRAINING = _constants["ENDPOINT_DRAINING"]
    CLOSED = _constants["ENDPOINT_CLOSED"]


class TerminalReason(IntEnum):
    """Known terminal classifications; NONE does not establish a clean close."""

    NONE = _constants["ENDPOINT_TERMINAL_NONE"]
    CLEAN = _constants["ENDPOINT_TERMINAL_CLEAN"]
    PROTOCOL = _constants["ENDPOINT_TERMINAL_PROTOCOL"]
    TLS_CERTIFICATE = _constants["ENDPOINT_TERMINAL_TLS_CERTIFICATE"]
    TLS = _constants["ENDPOINT_TERMINAL_TLS"]
    TRANSPORT = _constants["ENDPOINT_TERMINAL_TRANSPORT"]


class WaitResult(IntEnum):
    """Exact native wait outcomes. CLOSED alone does not imply clean EOF."""

    WOKEN = _constants["OK"]
    TIMED_OUT = _constants["DONE"]
    INTERRUPTED = _constants["ERR_INTERRUPTED"]
    CLOSED = _constants["ERR_CLOSED"]


class BuildInfo(TypedDict):
    """Compiled/runtime VERSION identities, Python ABI, and test fixture marker."""

    compiled_version: str
    runtime_version: str
    python_abi: str
    test_backend: bool


def _integer(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer, not bool or a converted value")
    return int(value)


def _bounded_integer(
    value: int, name: str, maximum: int, *, minimum: int = 0
) -> int:
    value = _integer(value, name)
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _known_or_int(enum_type: type[_Enum], value: int) -> _Enum | int:
    try:
        return enum_type(value)
    except ValueError:
        return value


def _text(value: str, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be str")
    if "\x00" in value:
        raise ValueError(f"{name} must not contain NUL")
    value.encode("utf-8")


@dataclass(frozen=True, slots=True, kw_only=True)
class EndpointConfig:
    """Immutable connection inputs. URL syntax and capabilities are checked in C.

    Text is UTF-8. None or an empty optional string selects the native default.
    A version tuple or list is copied in preference order; empty means AUTO.
    handshake_timeout_us bounds only the transport handshake, not MoQ SETUP.
    uri_mode defaults to LEGACY, preserving the wt_path override and native
    port/path defaults. Explicit STANDARD uses the URL's WebTransport path
    and refuses a non-empty wt_path here before any native effect.
    preferred_version (None for the library default) names the version an
    AUTO offer places first, so it is refused beside an explicit versions list.
    """

    url: str
    protocol: Protocol = Protocol.AUTO
    backend: Backend = Backend.AUTO
    versions: tuple[int, ...] = ()
    sni: str | None = None
    ca_file: str | None = None
    wt_path: str | None = None
    wt_profile: WtProfile = WtProfile.BACKEND_DEFAULT
    handshake_timeout_us: int = 0
    uri_mode: UriMode = UriMode.LEGACY
    preferred_version: int | None = None

    def __post_init__(self) -> None:
        _text(self.url, "url")
        for name in ("sni", "ca_file", "wt_path"):
            value = getattr(self, name)
            if value is not None:
                _text(value, name)
        object.__setattr__(
            self, "protocol", Protocol(_integer(self.protocol, "protocol"))
        )
        object.__setattr__(
            self, "backend", Backend(_integer(self.backend, "backend"))
        )
        object.__setattr__(
            self, "wt_profile", WtProfile(_integer(self.wt_profile, "wt_profile"))
        )
        if not isinstance(self.versions, (tuple, list)):
            raise TypeError("versions must be a tuple or list of integers")
        versions = tuple(
            _bounded_integer(value, f"versions[{index}]", _UINT32_MAX, minimum=1)
            for index, value in enumerate(self.versions)
        )
        object.__setattr__(self, "versions", versions)
        object.__setattr__(
            self,
            "handshake_timeout_us",
            _bounded_integer(
                self.handshake_timeout_us, "handshake_timeout_us", _INT64_MAX
            ),
        )
        object.__setattr__(
            self, "uri_mode", UriMode(_integer(self.uri_mode, "uri_mode"))
        )
        if self.uri_mode is UriMode.STANDARD and self.wt_path:
            raise ValueError(
                "wt_path is a UriMode.LEGACY override; under UriMode.STANDARD "
                "the url carries the WebTransport path"
            )
        if self.preferred_version is not None:
            preferred = _bounded_integer(
                self.preferred_version, "preferred_version", _UINT32_MAX, minimum=1
            )
            if versions:
                raise ValueError(
                    "preferred_version orders only the automatic offer; an "
                    "explicit versions list already states its order"
                )
            object.__setattr__(self, "preferred_version", preferred)


@dataclass(frozen=True, slots=True)
class Terminal:
    """Owned terminal snapshot; detail_code retains all 64 unsigned raw bits."""

    reason: int
    detail_code: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "reason",
            _known_or_int(TerminalReason, _integer(self.reason, "reason")),
        )
        object.__setattr__(
            self,
            "detail_code",
            _bounded_integer(self.detail_code, "detail_code", _UINT64_MAX),
        )


# -- receiver values --------------------------------------------------------

_MAX_NAMESPACE_PARTS = 32
# The largest FLOW_CONTROL object threshold whose ring (n + n/2 + 1) fits uint32.
_MAX_OBJECTS = 2_863_311_529
# The largest byte budget whose FLOW_CONTROL ceiling (n + n/2) fits uint64.
_MAX_BYTES = 12_297_829_382_473_034_410


class OverflowPolicy(IntEnum):
    """What the receiver does when its object queue is full. There is no UNSET
    member: the choice is required, as it is in C."""

    DROP_TO_KEYFRAME = _constants["MEDIA_OVERFLOW_DROP_TO_KEYFRAME"]
    DROP_GROUP = _constants["MEDIA_OVERFLOW_DROP_GROUP"]
    FLOW_CONTROL = _constants["MEDIA_OVERFLOW_FLOW_CONTROL"]


class TimeMode(IntEnum):
    """RAW passes wire timestamps through; SHARED_EPOCH rebases reported times."""

    RAW = _constants["MEDIA_TIME_RAW"]
    SHARED_EPOCH = _constants["MEDIA_TIME_SHARED_EPOCH"]


class StartMode(IntEnum):
    """Where a subscription joins: the live edge, or the next group boundary."""

    CURRENT = _constants["MEDIA_START_CURRENT"]
    NEXT_GROUP = _constants["MEDIA_START_NEXT_GROUP"]


class CompanionProfile(IntEnum):
    """Media dialect. AUTO (the default) follows the negotiated draft: the
    legacy dialect on draft 16/18, the draft-22 dialect on draft 22. An
    explicit dialect refuses the other drafts."""

    LEGACY_MSF01_LOC01 = _constants["MEDIA_COMPANION_LEGACY_MSF01_LOC01"]
    D22_MSF01_LOC04_PUBLIC = _constants["MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC"]
    AUTO = _constants["MEDIA_COMPANION_AUTO"]


class LocationFilterKind(IntEnum):
    NONE = _constants["LOCATION_FILTER_NONE"]
    RELATIVE_GROUP = _constants["LOCATION_FILTER_RELATIVE_GROUP"]
    ABSOLUTE_START = _constants["LOCATION_FILTER_ABSOLUTE_START"]
    GROUP_END = _constants["LOCATION_FILTER_GROUP_END"]
    OBJECT_END = _constants["LOCATION_FILTER_OBJECT_END"]
    NEXT_OBJECT = _constants["LOCATION_FILTER_NEXT_OBJECT"]


@dataclass(frozen=True, slots=True, kw_only=True)
class LocationFilter:
    """Semantic locations: absolute ends, inclusive end_object, full uint64.

    NONE explicitly removes a constraint; None at the call site omits it.
    RELATIVE_GROUP offset 0 selects next, 1 current, 2 preceding group.
    Inactive fields are ignored by native validation/resolution.
    """

    kind: LocationFilterKind = LocationFilterKind.NONE
    relative_group_offset: int = 0
    start_group: int = 0
    start_object: int = 0
    end_group: int = 0
    end_object: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", LocationFilterKind(_integer(self.kind, "kind")))
        for name in ("relative_group_offset", "start_group", "start_object", "end_group", "end_object"):
            object.__setattr__(self, name, _bounded_integer(getattr(self, name), name, _UINT64_MAX))


class FillGroupOrder(IntEnum):
    ASCENDING = _constants["GROUP_ORDER_ASCENDING"]
    DESCENDING = _constants["GROUP_ORDER_DESCENDING"]


@dataclass(frozen=True, slots=True, kw_only=True)
class FillConfig:
    """One initial fill request; an empty value still requests a child.

    None overrides inherit; timeout_ms=0 permits only immediately available
    objects, not a child-lifetime timer. Native service checks eligibility.
    """

    priority: int | None = None
    group_order: FillGroupOrder | None = None
    timeout_ms: int | None = None
    location: LocationFilter | None = None

    def __post_init__(self) -> None:
        if self.priority is not None:
            object.__setattr__(self, "priority", _bounded_integer(self.priority, "priority", 255))
        if self.group_order is not None:
            object.__setattr__(self, "group_order", FillGroupOrder(_integer(self.group_order, "group_order")))
        if self.timeout_ms is not None:
            object.__setattr__(self, "timeout_ms", _bounded_integer(self.timeout_ms, "timeout_ms", _UINT64_MAX))
        if self.location is not None and not isinstance(self.location, LocationFilter):
            raise TypeError("location must be LocationFilter or None")


class PollOutcome(IntEnum):
    """The non-item results of a poll, as exact native codes. CLOSED means the
    polled queue is empty AND the receiver is terminal, never aggregate EOF."""

    EMPTY = _constants["DONE"]
    INTERRUPTED = _constants["ERR_INTERRUPTED"]
    CLOSED = _constants["ERR_CLOSED"]


class TrackEventKind(IntEnum):
    """Known track-event kinds; unknown future kinds are preserved as integers."""

    ADDED = _constants["MEDIA_TRACK_ADDED"]
    UPDATED = _constants["MEDIA_TRACK_UPDATED"]
    REMOVED = _constants["MEDIA_TRACK_REMOVED"]
    ENDED = _constants["MEDIA_TRACK_ENDED"]
    CATALOG_READY = _constants["MEDIA_CATALOG_READY"]
    UPDATE_OK = _constants["MEDIA_TRACK_UPDATE_OK"]
    PARSE_DROP = _constants["MEDIA_TRACK_PARSE_DROP"]


class TrackState(IntEnum):
    """A track's delivery state; the two pause states are deliberately distinct."""

    DISCOVERED = _constants["MEDIA_TRACK_STATE_DISCOVERED"]
    PENDING = _constants["MEDIA_TRACK_STATE_PENDING"]
    ACTIVE = _constants["MEDIA_TRACK_STATE_ACTIVE"]
    PAUSED_APP = _constants["MEDIA_TRACK_STATE_PAUSED_APP"]
    PAUSED_FLOW = _constants["MEDIA_TRACK_STATE_PAUSED_FLOW"]
    ENDED = _constants["MEDIA_TRACK_STATE_ENDED"]


class ExtendedTrackEventKind(IntEnum):
    """Additive names without changing TrackEventKind's original members."""

    ADDED = TrackEventKind.ADDED
    UPDATED = TrackEventKind.UPDATED
    REMOVED = TrackEventKind.REMOVED
    ENDED = TrackEventKind.ENDED
    CATALOG_READY = TrackEventKind.CATALOG_READY
    UPDATE_OK = TrackEventKind.UPDATE_OK
    PARSE_DROP = TrackEventKind.PARSE_DROP
    LOCAL_FAILURE = _constants["MEDIA_TRACK_LOCAL_FAILURE"]
    FILL_TERMINAL = _constants["MEDIA_TRACK_FILL_TERMINAL"]
    DELIVERY_LOCAL_FAILURE = _constants["MEDIA_TRACK_DELIVERY_LOCAL_FAILURE"]
    UPDATE_LOCAL_FAILURE = _constants["MEDIA_TRACK_UPDATE_LOCAL_FAILURE"]


class ExtendedTrackState(IntEnum):
    DISCOVERED = TrackState.DISCOVERED
    PENDING = TrackState.PENDING
    ACTIVE = TrackState.ACTIVE
    PAUSED_APP = TrackState.PAUSED_APP
    PAUSED_FLOW = TrackState.PAUSED_FLOW
    ENDED = TrackState.ENDED
    FAILED = _constants["MEDIA_TRACK_STATE_FAILED"]


class FillReason(IntEnum):
    COMPLETE = _constants["FILL_COMPLETE"]
    NO_STREAM = _constants["FILL_NO_STREAM"]
    LOCAL_CANCELLED = _constants["FILL_LOCAL_CANCELLED"]
    PEER_CANCELLED_OR_RESET = _constants["FILL_PEER_CANCELLED_OR_RESET"]
    LOCAL_ABORTED = _constants["FILL_LOCAL_ABORTED"]
    PARENT_REJECTED = _constants["FILL_PARENT_REJECTED"]
    PARENT_TERMINATED = _constants["FILL_PARENT_TERMINATED"]


class FillErrorDomain(IntEnum):
    NONE = _constants["FILL_ERROR_NONE"]
    STREAM = _constants["FILL_ERROR_STREAM"]
    PARENT_REQUEST = _constants["FILL_ERROR_PARENT_REQUEST"]
    PARENT_DONE = _constants["FILL_ERROR_PARENT_DONE"]


@dataclass(frozen=True, slots=True)
class FillTerminal:
    """One child's terminal, not parent END_OF_TRACK. Partial fields are None;
    a zero error_code with STREAM domain is a real failure code, not success."""

    initiating_request_id: int | None
    reason: FillReason | int | None
    error_domain: FillErrorDomain | int | None
    error_code: int | None


class ReceiverFatal(IntEnum):
    """The receiver's OWN fatal codes, for documentation. A fatal_code equal to
    one of these does NOT prove the failure originated in the receiver: the
    same value can be an endpoint protocol code."""

    CATALOG_UNUSABLE = _constants["MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE"]
    EVENT_OVERFLOW = _constants["MEDIA_RECEIVER_FATAL_EVENT_OVERFLOW"]
    SETUP_FAILED = _constants["MEDIA_RECEIVER_FATAL_SETUP_FAILED"]
    CATALOG_REJECTED = _constants["MEDIA_RECEIVER_FATAL_CATALOG_REJECTED"]


class FatalOrigin(IntEnum):
    """Whether the fatal origin is known. The C surface cannot say whether the
    receiver or its endpoint failed, so a fatal receiver reports UNKNOWN."""

    NONE = 0
    UNKNOWN = 1


def _namespace(value: object) -> tuple[bytes, ...]:
    if not isinstance(value, (tuple, list)):
        raise TypeError("namespace must be a tuple or list of bytes parts")
    parts = tuple(value)
    if not 1 <= len(parts) <= _MAX_NAMESPACE_PARTS:
        raise ValueError(f"namespace must have 1 to {_MAX_NAMESPACE_PARTS} parts")
    for index, part in enumerate(parts):
        if type(part) is not bytes:
            raise TypeError(f"namespace[{index}] must be bytes")
        if not part:
            raise ValueError(f"namespace[{index}] must not be empty")
    return parts


@dataclass(frozen=True, slots=True, kw_only=True)
class ReceiverConfig:
    """Immutable receiver inputs, checked before any native call.

    `overflow` is required. Sizes of zero select the native defaults (256
    objects, 32 MiB, 64 track events). The bounds are the largest values whose
    FLOW_CONTROL ring and byte-ceiling arithmetic stay representable; the
    native constructor refuses anything beyond them as well. Namespace parts
    are binary and copied byte-exact.
    """

    namespace: tuple[bytes, ...]
    overflow: OverflowPolicy
    catalog_track: bytes = b""
    auto_subscribe: bool = False
    time_mode: TimeMode = TimeMode.RAW
    max_objects: int = 0
    max_bytes: int = 0
    max_track_events: int = 0
    companion_profile: CompanionProfile = CompanionProfile.AUTO

    def __post_init__(self) -> None:
        object.__setattr__(self, "namespace", _namespace(self.namespace))
        object.__setattr__(
            self, "overflow", OverflowPolicy(_integer(self.overflow, "overflow"))
        )
        if type(self.catalog_track) is not bytes:
            raise TypeError("catalog_track must be bytes")
        if type(self.auto_subscribe) is not bool:
            raise TypeError("auto_subscribe must be bool")
        object.__setattr__(
            self, "time_mode", TimeMode(_integer(self.time_mode, "time_mode"))
        )
        object.__setattr__(
            self, "companion_profile", CompanionProfile(_integer(self.companion_profile, "companion_profile"))
        )
        object.__setattr__(
            self, "max_objects", _bounded_integer(self.max_objects, "max_objects", _MAX_OBJECTS)
        )
        object.__setattr__(
            self, "max_bytes", _bounded_integer(self.max_bytes, "max_bytes", _MAX_BYTES)
        )
        object.__setattr__(
            self,
            "max_track_events",
            _bounded_integer(self.max_track_events, "max_track_events", _UINT32_MAX),
        )

    @classmethod
    def live(cls, namespace: tuple[bytes, ...], **fields: object) -> ReceiverConfig:
        """The C `cfg_init_live` preset: DROP_TO_KEYFRAME overflow."""
        return cls(namespace=namespace, overflow=OverflowPolicy.DROP_TO_KEYFRAME, **fields)  # type: ignore[arg-type]

    @classmethod
    def flow_control(cls, namespace: tuple[bytes, ...], **fields: object) -> ReceiverConfig:
        """The C `cfg_init_flow_control` preset: FLOW_CONTROL overflow."""
        return cls(namespace=namespace, overflow=OverflowPolicy.FLOW_CONTROL, **fields)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class ReceiverStats:
    """A copied stats snapshot. The v0 fields (through `paused`) are always
    present; the appended fields are None when the native stamped prefix does
    not cover them, never zero."""

    objects_received: int
    objects_queued: int
    bytes_queued: int
    objects_dropped: int
    groups_dropped: int
    keyframes_dropped: int
    parse_drops: int
    overflow_events: int
    pause_transitions: int
    paused: bool
    catalog_drops: int | None
    catalog_complete: bool | None
    tracks_malformed: int | None


@dataclass(frozen=True, slots=True)
class ReceiverTerminal:
    """Three receiver getters read in sequence plus the endpoint's own terminal
    snapshot; not one atomic observation. `fatal_code` is the raw effective
    uint64 (0 is not "no error"). `origin` is UNKNOWN whenever `fatal`: the C
    surface does not expose whether the receiver or the endpoint failed."""

    closed: bool
    fatal: bool
    fatal_code: int
    endpoint: Terminal
    origin: FatalOrigin

    def __post_init__(self) -> None:
        if type(self.closed) is not bool or type(self.fatal) is not bool:
            raise TypeError("closed and fatal must be bool")
        object.__setattr__(
            self, "fatal_code", _bounded_integer(self.fatal_code, "fatal_code", _UINT64_MAX)
        )
        if not isinstance(self.endpoint, Terminal):
            raise TypeError("endpoint must be Terminal")
        object.__setattr__(self, "origin", FatalOrigin(_integer(self.origin, "origin")))
        if (self.origin is FatalOrigin.UNKNOWN) != self.fatal:
            raise ValueError("origin must be UNKNOWN exactly when fatal")


# -- track events and descriptions -------------------------------------------


class Packaging(IntEnum):
    """Structured media packaging; unknown future values stay integers."""

    RAW = _constants["MEDIA_PACKAGING_RAW"]
    CMAF = _constants["MEDIA_PACKAGING_CMAF"]


class MediaType(IntEnum):
    VIDEO = _constants["MEDIA_TYPE_VIDEO"]
    AUDIO = _constants["MEDIA_TYPE_AUDIO"]


class CodecKind(IntEnum):
    UNKNOWN = _constants["CMAF_CODEC_UNKNOWN"]
    AVC = _constants["CMAF_CODEC_AVC"]
    HEVC = _constants["CMAF_CODEC_HEVC"]
    AV1 = _constants["CMAF_CODEC_AV1"]
    AAC = _constants["CMAF_CODEC_AAC"]
    OPUS = _constants["CMAF_CODEC_OPUS"]


class ParseDropClass(IntEnum):
    """Which receive path a coalesced PARSE_DROP came from."""

    MEDIA = _constants["MEDIA_PARSE_DROP_MEDIA"]
    SAP = _constants["MEDIA_PARSE_DROP_SAP"]
    MEDIA_TIMELINE = _constants["MEDIA_PARSE_DROP_MEDIA_TIMELINE"]


@dataclass(frozen=True, slots=True)
class Cenc:
    """CENC parameters copied from the init segment (libmoq does not decrypt).
    Present whenever the init reports CENC; each field is None when the
    native stamped init prefix does not cover it."""

    scheme: bytes | None
    default_is_protected: int | None
    default_per_sample_iv_size: int | None
    default_kid: bytes | None


@dataclass(frozen=True, slots=True)
class CmafInit:
    """Parsed decoder configuration, copied. Fields the native stamped prefix
    does not cover are None; `codec_config` is the decoder extradata."""

    codec_kind: CodecKind | int | None
    timescale: int | None
    width: int | None
    height: int | None
    samplerate: int | None
    channel_count: int | None
    codec_config: bytes | None
    track_id: int | None
    cenc: Cenc | None


@dataclass(frozen=True, slots=True)
class MediaTemplate:
    """MSF media-timeline template (eight uint64 values, copied)."""

    start_media_ms: int
    delta_media_ms: int
    start_group: int
    start_object: int
    delta_group: int
    delta_object: int
    start_wallclock_ms: int
    delta_wallclock_ms: int


@dataclass(frozen=True, slots=True)
class VodState:
    """The CURRENT live/VOD triple, read through the sized description copy
    under the receiver mutex at poll time -- present state, not the value at
    the moment the event was queued."""

    is_live: bool
    track_duration_ms: int | None


@dataclass(frozen=True, slots=True)
class TrackDescription:
    """An owned copy of a track's description. Every span and array is Python
    bytes; nothing borrows native memory, so a copy outlives the receiver.
    Optional fields the catalog did not carry, or the native stamped prefixes
    (outer, info, init) did not cover, are None -- never fabricated zeros."""

    name: bytes
    role: bytes | None
    codec: bytes | None
    lang: bytes | None
    label: bytes | None
    media_type: MediaType | int | None
    packaging: Packaging | int | None
    timescale: int | None
    transport_version: int | None
    init: CmafInit | None
    init_data: bytes
    width: int | None
    height: int | None
    samplerate: int | None
    channel_config: bytes | None
    framerate_millis: int | None
    bitrate: int | None
    max_grp_sap: int | None
    max_obj_sap: int | None
    content_protection_ref_ids: tuple[bytes, ...]
    packaging_text: bytes
    event_type: bytes | None
    mime_type: bytes | None
    depends: tuple[bytes, ...]
    template: MediaTemplate | None
    vod: VodState


@dataclass(frozen=True, slots=True)
class ParseDrop:
    """A coalesced parse-drop diagnostic snapshot for one track. `total` and
    `delta` are None when the native stamped event prefix does not cover
    them (per-field, like every other appended value)."""

    cls: ParseDropClass | int
    total: int | None
    delta: int | None


class Track:
    """An opaque track handle owned by exactly one Receiver.

    A Track keeps its receiver alive; the receiver only caches tracks weakly,
    so the handle table never forms a cycle. Two Tracks compare equal when they
    name the same native handle of the same receiver; identity holds while any
    reference exists. Nothing here dereferences native memory. `description`
    is the latest owned copy delivered by a poll (None until the first event
    for this handle); a copy a caller retains is never mutated.
    """

    __slots__ = ("_receiver", "_capsule", "_key", "_description", "__weakref__")

    def __init__(self, receiver: Receiver, capsule: object, key: int) -> None:
        self._receiver = receiver
        self._capsule = capsule
        self._key = key
        self._description: TrackDescription | None = None

    @property
    def receiver(self) -> Receiver:
        """The owning receiver."""
        return self._receiver

    @property
    def description(self) -> TrackDescription | None:
        """The latest CURRENT-state copy delivered by poll_track (immutable)."""
        return self._description

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Track):
            return NotImplemented
        return other._receiver is self._receiver and other._key == self._key

    def __hash__(self) -> int:
        return hash((id(self._receiver), self._key))

    def __repr__(self) -> str:
        return "<moq5.Track>"


class ObjectStatus(IntEnum):
    """Object status as the exact native code; unknown future values stay
    integers. Anything other than NORMAL is status-only (no media)."""

    NORMAL = _constants["OBJECT_NORMAL"]
    END_OF_GROUP = _constants["OBJECT_END_OF_GROUP"]
    END_OF_TRACK = _constants["OBJECT_END_OF_TRACK"]


@dataclass(frozen=True, slots=True)
class CmafSample:
    """One sample record of a CMAF fragment in the track's timescale TICKS,
    not microseconds: three unsigned 32-bit fields and a signed 32-bit
    composition offset, exactly as the native parser reported them."""

    duration: int
    size: int
    flags: int
    composition_offset: int


class MediaTimeDomain(IntEnum):
    NONE = _constants["MEDIA_TIME_DOMAIN_NONE"]
    UNIX_US = _constants["MEDIA_TIME_DOMAIN_UNIX_US"]
    APPLICATION_MEDIA = _constants["MEDIA_TIME_DOMAIN_APPLICATION_MEDIA"]


class DeliveryOrigin(IntEnum):
    LIVE = _constants["MEDIA_DELIVERY_LIVE"]
    FILL = _constants["MEDIA_DELIVERY_FILL"]


@dataclass(frozen=True, slots=True)
class LocMetadata:
    """Copied effective LOC fields. None is absent; b'' is present-empty.
    Raw ticks/scale/marking and the presence mask are never reinterpreted."""

    present: int
    timestamp: int | None
    timescale: int | None
    marking: bytes | None
    audio_level: int | None
    video_config: bytes | None
    audio_config: bytes | None


@dataclass(frozen=True, slots=True)
class MediaMetadata:
    """Native time-domain classification; time_us is not necessarily UNIX.
    None means no conversion/independence observation, never zero/false."""

    time_domain: MediaTimeDomain | int
    time_us: int | None
    independent: bool | None
    loc: LocMetadata | None


@dataclass(frozen=True, slots=True)
class MediaObject:
    """One dequeued media object, fully owned: every byte and sample is a
    Python copy made before the transferred native buffers were released, so
    the value survives receiver and endpoint close and may be read from any
    thread. Timestamps are unsigned 64-bit microseconds and
    `composition_offset_us` is signed; `capture_time_us` is None when the
    object carried none. RAW media is `payload` (fragment empty); CMAF media
    is the whole `fragment` with the mdat slice at `mdat_offset`/`mdat_len`
    and its `samples` (payload empty). Unknown `status`/`packaging` values
    are kept as integers and their bytes are not interpreted. Additive
    observations are keyword-only and do not change legacy equality/hash;
    compare their values explicitly when they matter."""

    track: Track
    config_generation: int
    packaging: Packaging | int
    status: ObjectStatus | int
    end_of_group: bool
    datagram: bool
    keyframe: bool
    capture_time_us: int | None
    decode_time_us: int
    composition_offset_us: int
    presentation_time_us: int
    payload: bytes
    fragment: bytes
    mdat_offset: int
    mdat_len: int
    samples: tuple[CmafSample, ...]
    group_id: int | None = field(default=None, compare=False, kw_only=True)
    subgroup_id: int | None = field(default=None, compare=False, kw_only=True)
    object_id: int | None = field(default=None, compare=False, kw_only=True)
    publisher_priority: int | None = field(default=None, compare=False, kw_only=True)
    has_subgroup: bool | None = field(default=None, compare=False, kw_only=True)
    metadata: MediaMetadata | None = field(default=None, compare=False, kw_only=True)
    object_properties: bytes | None = field(default=None, compare=False, kw_only=True)
    track_properties: bytes | None = field(default=None, compare=False, kw_only=True)
    delivery_origin: DeliveryOrigin | int | None = field(default=None, compare=False, kw_only=True)
    initiating_request_id: int | None = field(default=None, compare=False, kw_only=True)

    @property
    def is_status_only(self) -> bool:
        """Whether this object carries status rather than media: decided by
        `status`, never by byte length (a zero-length NORMAL object is media)."""
        return self.status != ObjectStatus.NORMAL


@dataclass(frozen=True, slots=True)
class TrackEvent:
    """One dequeued track event. `track`/`description` are None for
    CATALOG_READY; `largest`/`expires_ms` are UPDATE_OK observations present
    only when the native stamped prefix covers them; `parse_drop` is present
    for PARSE_DROP only. UPDATE_LOCAL_FAILURE carries the local UPDATE
    admission result, not a peer error. DELIVERY_LOCAL_FAILURE carries local_result and
    delivery_origin; only FILL has a delivery_initiating_request_id (zero is
    valid). Absent complete fields remain None. The keyword-only views
    preserve legacy equality/hash and positional construction/patterns."""

    kind: TrackEventKind | int
    track: Track | None
    description: TrackDescription | None
    largest: tuple[int, int] | None
    expires_ms: int | None
    parse_drop: ParseDrop | None
    local_result: int | None = field(default=None, compare=False, kw_only=True)
    fill_terminal: FillTerminal | None = field(default=None, compare=False, kw_only=True)
    delivery_origin: DeliveryOrigin | int | None = field(default=None, compare=False, kw_only=True)
    delivery_initiating_request_id: int | None = field(default=None, compare=False, kw_only=True)

    @property
    def kind_extended(self) -> ExtendedTrackEventKind | int:
        """Named new observations; kind retains its legacy enum/int behavior."""
        return _known_or_int(ExtendedTrackEventKind, self.kind)


# -- sender -------------------------------------------------------------------

#: `catalog_refresh_interval_us` sentinel: refresh explicitly DISABLED.
CATALOG_REFRESH_DISABLED = _UINT64_MAX


class Backpressure(IntEnum):
    """The send-side overflow policy, as the exact native codes.

    The choice is forced: the native UNSET (0) is not a member, so a
    configuration cannot be built without naming a policy.
    """

    DROP_TO_KEYFRAME = _constants["MEDIA_SEND_BP_DROP_TO_KEYFRAME"]
    DROP_GROUP = _constants["MEDIA_SEND_BP_DROP_GROUP"]
    BLOCK_TIMEOUT = _constants["MEDIA_SEND_BP_BLOCK_TIMEOUT"]
    RETURN_WOULD_BLOCK = _constants["MEDIA_SEND_BP_RETURN_WOULD_BLOCK"]


@dataclass(frozen=True, slots=True)
class SenderStats:
    """A copied stats snapshot, taken anew on every call.

    The v0 fields (through `last_error`) are always present. The appended
    `sap_records_evicted` is None when the native stamped prefix does not
    cover the WHOLE field, never zero; a stamped zero is a present zero.

    `objects_written`, `objects_sent`, `objects_queued`, `objects_dropped` and
    `bytes_queued` are ONE population: media objects accepted by write() and
    the bytes they hold. The private END_OF_TRACK marker end_track() queues is
    not media and appears in none of them, so `objects_queued == 0` means no
    media is pending -- it does NOT mean the lifecycle finished or that a
    terminal has been emitted. `objects_sent` means handed to the session, not
    delivered to a receiver.

    `groups_dropped` counts distinct (track, group) values that lost at least
    one queued object; `groups_abandoned` counts open wire subgroups driven
    through the RESET lifecycle. They are different facts, and one partially
    emitted group can contribute to both.

    `last_error` is the last non-OK write() return, 0 for none. It is DATA: a
    recorded historical failure is reported here, not raised.
    """

    objects_written: int
    objects_sent: int
    objects_queued: int
    bytes_queued: int
    objects_dropped: int
    groups_dropped: int
    keyframes_dropped: int
    groups_abandoned: int
    backpressure_stalls: int
    last_error: int
    sap_records_evicted: int | None


class SapType(IntEnum):
    """An app-declared CMAF stream access point type (CMSF-00 3.6.1).

    The exact type cannot be derived from CMAF sample flags alone, so the
    encoder declares it. The native internal sentinel UNKNOWN is deliberately
    not a member: the service rejects it, and offering it here would only
    build an object that cannot be written.
    """

    NONE = _constants["SAP_NONE"]
    TYPE_1 = _constants["SAP_TYPE_1"]
    TYPE_2 = _constants["SAP_TYPE_2"]
    TYPE_3 = _constants["SAP_TYPE_3"]


class WriteOutcome(IntEnum):
    """What the service did with one submitted object, as the native codes.

    ACCEPTED means the service took ownership of the object, NOT that it was
    delivered: under a drop policy an accepted object may still be evicted
    from the queue, and this surface carries no delivery receipt.

    WOULD_BLOCK is backpressure -- a full queue under RETURN_WOULD_BLOCK, a
    BLOCK_TIMEOUT that expired, a missing sync anchor, or a GOP larger than
    the bound under a drop policy. INTERRUPTED means the endpoint's interrupt
    latch was set. CLOSED means the sender or endpoint is terminal. None of
    the three took ownership, and none of them is retried here.
    """

    ACCEPTED = _constants["OK"]
    WOULD_BLOCK = _constants["ERR_WOULD_BLOCK"]
    INTERRUPTED = _constants["ERR_INTERRUPTED"]
    CLOSED = _constants["ERR_CLOSED"]


_SEND_LOC_FIELDS = (
    ("timestamp", _constants["LOC_METADATA_HAS_TIMESTAMP"], _UINT64_MAX),
    ("timescale", _constants["LOC_METADATA_HAS_TIMESCALE"], _UINT64_MAX),
    ("marking", _constants["LOC_METADATA_HAS_MARKING"], None),
    ("audio_level", _constants["LOC_METADATA_HAS_AUDIO_LEVEL"], 0xff),
    ("video_config", _constants["LOC_METADATA_HAS_VIDEO_CONFIG"], None),
    ("audio_config", _constants["LOC_METADATA_HAS_AUDIO_CONFIG"], None),
)
_SEND_LOC_MASK = sum(flag for _, flag, _ in _SEND_LOC_FIELDS)


def _loc_metadata_arguments(value: LocMetadata | None) -> tuple | None:
    """Validate outbound use only and retain immutable native-call inputs."""
    if value is None:
        return None
    if not isinstance(value, LocMetadata):
        raise TypeError("loc_metadata must be LocMetadata or None")
    present = _bounded_integer(value.present, "loc_metadata.present", _UINT32_MAX)
    if present & ~_SEND_LOC_MASK:
        raise ValueError("loc_metadata.present has unknown bits")
    arguments = [present]
    for field, flag, maximum in _SEND_LOC_FIELDS:
        item = getattr(value, field)
        name = f"loc_metadata.{field}"
        if bool(present & flag) != (item is not None):
            raise ValueError(f"{name} must agree with loc_metadata.present")
        if item is not None:
            if maximum is not None:
                item = _bounded_integer(item, name, maximum)
            else:
                if type(item) is not bytes:
                    raise TypeError(f"{name} must be immutable bytes")
                if field == "marking" and not 1 <= len(item) <= 4:
                    raise ValueError(f"{name} must contain 1 to 4 bytes")
        arguments.append(item)
    return tuple(arguments)


@dataclass(frozen=True, slots=True, kw_only=True)
class SendObject:
    """One media object to submit, checked before any native call.

    `payload` is required and is immutable bytes; b"" is a VALUE, passed as a
    present zero-length buffer, while omitting it is an error. `properties` is
    the CMAF property block, passed through byte-exact: None is ABSENCE and
    b"" is a present empty block, and the two are never collapsed. The service
    requires properties to be absent on a RAW track and owns the LOC block
    there; nothing is parsed, sniffed or generated here.

    `capture_time_us` and `sap_type` are optional: None is absence, and an
    explicit 0 is a declared value (SapType.NONE is "not a SAP", which is not
    the same as declaring nothing). `capture_time_us` is wall-clock
    microseconds since the Unix epoch, emitted verbatim as the legacy LOC-01
    Capture Timestamp on drafts 16 and 18. Its encodable range follows the
    negotiated codec and is enforced by the service, not clamped here.
    These legacy timing fields do not supply D22 RAW metadata.

    `loc_metadata` supplies explicit raw LOC inputs, not derived time or
    independence observations. Its present bits must agree with non-None
    values; zero and present-empty configs are preserved. Timestamp and scale
    remain uint64 values without clock conversion. D22 RAW timestamp/scale
    semantics and companion compatibility are checked by the native service;
    legacy RAW and CMAF ignore this metadata input.

    `decode_time_us` is advisory in v0: per track, write order IS decode order.
    """

    payload: bytes
    properties: bytes | None = None
    is_sync: bool = False
    starts_group: bool = False
    ends_group: bool = False
    decode_time_us: int = 0
    presentation_time_us: int = 0
    capture_time_us: int | None = None
    sap_type: SapType | None = None
    loc_metadata: LocMetadata | None = None

    def __post_init__(self) -> None:
        if type(self.payload) is not bytes:
            raise TypeError("payload must be immutable bytes")
        if self.properties is not None and type(self.properties) is not bytes:
            raise TypeError("properties must be immutable bytes or None")
        for field in ("is_sync", "starts_group", "ends_group"):
            if type(getattr(self, field)) is not bool:
                raise TypeError(f"{field} must be bool")
        for field in ("decode_time_us", "presentation_time_us"):
            object.__setattr__(
                self, field,
                _bounded_integer(getattr(self, field), field, _UINT64_MAX))
        if self.capture_time_us is not None:
            object.__setattr__(
                self, "capture_time_us",
                _bounded_integer(self.capture_time_us, "capture_time_us",
                                 _UINT64_MAX))
        if self.sap_type is not None:
            object.__setattr__(
                self, "sap_type",
                SapType(_integer(self.sap_type, "sap_type")))
        _loc_metadata_arguments(self.loc_metadata)


@dataclass(frozen=True, slots=True, kw_only=True)
class SenderConfig:
    """Immutable sender inputs, checked before any native call.

    `backpressure` is required. Sizes of zero select the native defaults, and
    `catalog_refresh_interval_us` keeps the native meanings: 0 is the library
    default, CATALOG_REFRESH_DISABLED turns refresh off, anything else is that
    interval. Namespace parts and the catalog name are binary and copied
    byte-exact. `companion_profile` selects the media dialect; its AUTO
    default follows the draft the endpoint negotiates.
    """

    namespace: tuple[bytes, ...]
    backpressure: Backpressure
    catalog_track: bytes = b""
    block_timeout_us: int = 0
    queue_max_objects: int = 0
    queue_max_bytes: int = 0
    pre_ready_max_objects: int = 0
    pre_ready_max_bytes: int = 0
    validate_cmaf: bool = True
    publish_tracks: bool = False
    drop_without_demand: bool = False
    catalog_refresh_interval_us: int = 0
    companion_profile: CompanionProfile = CompanionProfile.AUTO

    def __post_init__(self) -> None:
        object.__setattr__(self, "namespace", _namespace(self.namespace))
        object.__setattr__(
            self, "backpressure",
            Backpressure(_integer(self.backpressure, "backpressure")))
        object.__setattr__(
            self, "companion_profile",
            CompanionProfile(_integer(self.companion_profile, "companion_profile")))
        if type(self.catalog_track) is not bytes:
            raise TypeError("catalog_track must be bytes")
        for name in ("validate_cmaf", "publish_tracks", "drop_without_demand"):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"{name} must be bool")
        for name, maximum in (("block_timeout_us", _UINT64_MAX),
                              ("queue_max_objects", _UINT32_MAX),
                              ("queue_max_bytes", _UINT32_MAX),
                              ("pre_ready_max_objects", _UINT32_MAX),
                              ("pre_ready_max_bytes", _UINT32_MAX),
                              ("catalog_refresh_interval_us", _UINT64_MAX)):
            object.__setattr__(
                self, name, _bounded_integer(getattr(self, name), name, maximum))

    @classmethod
    def live(cls, namespace: tuple[bytes, ...], **fields: object) -> "SenderConfig":
        """The C `cfg_init_live` preset: DROP_TO_KEYFRAME (never block)."""
        return cls(namespace=namespace, backpressure=Backpressure.DROP_TO_KEYFRAME, **fields)  # type: ignore[arg-type]

    @classmethod
    def lossless(cls, namespace: tuple[bytes, ...], **fields: object) -> "SenderConfig":
        """The C `cfg_init_lossless` preset: BLOCK_TIMEOUT (the app decides)."""
        return cls(namespace=namespace, backpressure=Backpressure.BLOCK_TIMEOUT, **fields)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True, kw_only=True)
class SendTrackConfig:
    """Immutable track inputs, checked before any native call.

    Every span is bytes and is kept byte-exact, embedded NULs included; no
    value is sniffed, parsed or normalised. `timescale` 0 selects the native
    default. MSF-01 requires a codec (5.2.18) and a maximum bitrate (5.2.22)
    for audio and video, and an audio track additionally requires a sample
    rate (5.2.28) and a channel configuration (5.2.29); those are mirrored
    here so a malformed track never reaches the service. A live track must not
    declare a duration (5.2.35).

    Deliberately not settable from Python: the
    CMSF maximum SAP starting types, content-protection reference ids, the
    generated SAP and media timelines, and the CMSF alternate group. Every one
    of them is sent absent.
    """

    name: bytes
    media_type: MediaType
    packaging: Packaging
    codec: bytes
    bitrate: int
    timescale: int = 0
    init_data: bytes = b""
    role: bytes = b""
    lang: bytes = b""
    is_live: bool = True
    width: int = 0
    height: int = 0
    framerate_millis: int = 0
    samplerate: int = 0
    channel_config: bytes = b""
    track_duration_ms: int | None = None
    # The track's Track Properties (draft-22 section 10), stated on every
    # answer the sender gives for the track: opaque and in the negotiated
    # draft's encoding. A property whose type and value are both below 64
    # encodes identically on drafts 16, 18 and 22. A set the session refuses
    # makes the sender fatal.
    track_properties: bytes = b""
    # Advertise DYNAMIC_GROUPS = 1: subscribers may then ask for a new group,
    # which Sender.take_new_group_request reports.
    dynamic_groups: bool = False

    def __post_init__(self) -> None:
        for field in ("name", "codec", "init_data", "role", "lang",
                      "channel_config", "track_properties"):
            if type(getattr(self, field)) is not bytes:
                raise TypeError(f"{field} must be bytes")
        if type(self.is_live) is not bool:
            raise TypeError("is_live must be bool")
        if type(self.dynamic_groups) is not bool:
            raise TypeError("dynamic_groups must be bool")
        object.__setattr__(
            self, "media_type", MediaType(_integer(self.media_type, "media_type")))
        object.__setattr__(
            self, "packaging", Packaging(_integer(self.packaging, "packaging")))
        for field, maximum in (("bitrate", _UINT64_MAX),
                               ("framerate_millis", _UINT64_MAX),
                               ("timescale", _UINT32_MAX),
                               ("width", _UINT32_MAX),
                               ("height", _UINT32_MAX),
                               ("samplerate", _UINT32_MAX)):
            object.__setattr__(
                self, field, _bounded_integer(getattr(self, field), field, maximum))
        if self.track_duration_ms is not None:
            object.__setattr__(
                self, "track_duration_ms",
                _bounded_integer(self.track_duration_ms, "track_duration_ms",
                                 _UINT64_MAX))
        # value rules, mirrored from the service so a malformed track is
        # refused here rather than at the native boundary
        if not self.name:
            raise ValueError("name must not be empty")
        if not self.codec:
            raise ValueError("codec must not be empty (MSF-01 5.2.18)")
        if self.bitrate == 0:
            raise ValueError("bitrate must not be zero (MSF-01 5.2.22)")
        if self.media_type is MediaType.AUDIO:
            if self.samplerate == 0:
                raise ValueError("samplerate is required for audio "
                                 "(MSF-01 5.2.28)")
            if not self.channel_config:
                raise ValueError("channel_config is required for audio "
                                 "(MSF-01 5.2.29)")
        if self.is_live and self.track_duration_ms is not None:
            raise ValueError("a live track must not declare track_duration_ms "
                             "(MSF-01 5.2.35)")


@dataclass(frozen=True, slots=True)
class SenderTerminal:
    """Three sender getters read in sequence plus the endpoint's own terminal
    snapshot; not one atomic observation. `fatal` is true when the SENDER or
    its endpoint is fatal, and `fatal_code` is the sender's own code when it is
    sender-fatal, otherwise the endpoint's - the same fallback the C surface
    applies. `fatal_code` is the raw effective uint64 (0 is not "no error")."""

    closed: bool
    fatal: bool
    fatal_code: int
    endpoint: Terminal
