"""Checked values for the caller-driven endpoint foundation."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import TypeVar, TypedDict

from . import _native

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


class Version(IntEnum):
    """Named wire versions; availability is checked by the native service."""

    DRAFT_16 = _constants["VERSION_DRAFT_16"]
    DRAFT_18 = _constants["VERSION_DRAFT_18"]


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
