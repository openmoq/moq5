"""Synchronous endpoint ownership over the private CPython bridge."""

from __future__ import annotations

from collections.abc import Callable
from time import monotonic_ns as _monotonic_ns
from types import TracebackType
from typing import Literal, ParamSpec, Self, TypeVar, cast

from . import _native
from ._values import (
    BuildInfo,
    EndpointConfig,
    EndpointState,
    Terminal,
    WaitResult,
    _INT64_MAX,
    _bounded_integer,
    _known_or_int,
)

_Params = ParamSpec("_Params")
_Result = TypeVar("_Result")

# One public wait is a sequence of bounded native wait REQUESTS against one
# monotonic deadline. The cap is what lets a pending Python signal be
# delivered: the bridge checks signals after every native return, and a native
# wait itself is not interruptible by a signal. It caps each request; it is
# not a wall-clock or arbitrary-thread delivery guarantee.
_WAIT_SLICE_US = 250_000


class MoqError(RuntimeError):
    """Native failure with the exact signed result code and operation name."""

    def __init__(self, code: int, operation: str, message: str) -> None:
        super().__init__(code, operation, message)
        self.code = code
        self.operation = operation

    def __str__(self) -> str:
        return f"{self.operation}: {self.args[2]} (code {self.code})"


def _call(
    function: Callable[_Params, _Result],
    /,
    *args: _Params.args,
    **kwargs: _Params.kwargs,
) -> _Result:
    try:
        return function(*args, **kwargs)
    except _native.Error as error:
        code, operation, message = error.args
        raise MoqError(code, operation, message) from error


def build_info() -> BuildInfo:
    """Copy compiled/runtime VERSION identities, Python ABI, and test marker."""
    return cast(BuildInfo, dict(_call(_native.build_info)))


class Endpoint:
    """An endpoint owned by the thread that calls connect().

    Only wake(), set_interrupted(), and closed may be used by other threads.
    Native operations reject handles inherited across fork. Use close() or a
    with block to stop, join, and release native resources deterministically.
    """

    __slots__ = ("__handle",)
    __handle: object

    def __new__(cls) -> Self:
        raise TypeError("Use Endpoint.connect(EndpointConfig(...))")

    @classmethod
    def connect(cls, config: EndpointConfig) -> Self:
        """Start a verified connection now; establishment proceeds asynchronously.

        Observe state and wait() on the calling thread. Native failures raise
        MoqError with the original signed code and operation.
        """
        if not isinstance(config, EndpointConfig):
            raise TypeError("config must be EndpointConfig")
        endpoint = object.__new__(cls)
        endpoint.__handle = _call(
            _native.connect,
            config.url.encode("utf-8"),
            int(config.protocol),
            int(config.backend),
            config.versions,
            b"" if config.sni is None else config.sni.encode("utf-8"),
            b"" if config.ca_file is None else config.ca_file.encode("utf-8"),
            b"" if config.wt_path is None else config.wt_path.encode("utf-8"),
            int(config.wt_profile),
            config.handshake_timeout_us,
        )
        return endpoint

    @property
    def closed(self) -> bool:
        """Whether the native resource has been released, not merely terminalized."""
        return _call(_native.closed, self.__handle)

    @property
    def state(self) -> EndpointState | int:
        """Current connection state, preserving unknown future values as integers."""
        return _known_or_int(EndpointState, _call(_native.state, self.__handle))

    @property
    def terminal(self) -> Terminal:
        """Copy the terminal reason and raw detail bits before explicit close()."""
        reason, detail_code = _call(_native.terminal, self.__handle)
        return Terminal(reason, detail_code)

    @property
    def negotiated_version(self) -> int:
        """Negotiated wire version; zero until the connection is established."""
        return _call(_native.negotiated_version, self.__handle)

    def wait(self, timeout_us: int) -> WaitResult:
        """Wait for activity for 0..INT64_MAX microseconds, releasing the GIL.

        Zero is exactly one nonblocking native poll. WOKEN means activity, not
        necessarily establishment. CLOSED covers clean and failed terminal
        connections; inspect terminal for why. The finite ceiling avoids
        overflow in native deadline arithmetic. Unexpected native failures
        raise MoqError, never an EOF sentinel.

        The wait is one monotonic deadline served by native wait requests of
        at most 250 ms each. Every valid call makes at least one native
        observation -- an already-expired budget is served by one zero poll --
        so a closed, foreign-thread or fork-inherited handle is always refused
        and an already-present WOKEN/INTERRUPTED/CLOSED or native failure is
        never hidden behind a timeout. After an actual native TIMED_OUT with
        the budget exhausted, the timeout is returned without another poll.
        The 250 ms cap bounds each native request; because the bridge checks
        Python signals after each native return, a pending KeyboardInterrupt
        on the main thread is honoured within one slice without anyone waking
        or interrupting the endpoint. That is a cap on native wait requests,
        not a wall-clock or arbitrary-thread delivery guarantee. A timed-out
        slice neither shortens nor restarts the budget; the last slice is the
        exact remainder, rounded up.
        """
        timeout_us = _bounded_integer(timeout_us, "timeout_us", _INT64_MAX)
        deadline_ns = _monotonic_ns() + timeout_us * 1_000
        observed = False
        while True:
            remaining_ns = deadline_ns - _monotonic_ns()
            exhausted = remaining_ns <= 0
            if exhausted and observed:
                return WaitResult.TIMED_OUT
            # Round the remainder UP: a truncated slice would return before
            # the caller's deadline. An exhausted budget still polls once.
            slice_us = 0 if exhausted else min((remaining_ns + 999) // 1_000, _WAIT_SLICE_US)
            result = WaitResult(_call(_native.wait, self.__handle, slice_us))
            observed = True
            if result is not WaitResult.TIMED_OUT:
                return result
            if exhausted:
                # The one guaranteed observation timed out on an already
                # exhausted budget: nothing can un-exhaust it, so no re-read.
                return WaitResult.TIMED_OUT

    def wake(self) -> None:
        """Request a coalesced service cycle; does not clear the interrupt latch."""
        _call(_native.wake, self.__handle)

    def set_interrupted(self, interrupted: bool) -> None:
        """Set or clear the sticky, nonterminal interrupt latch from any thread."""
        if type(interrupted) is not bool:
            raise TypeError("interrupted must be bool")
        _call(_native.set_interrupted, self.__handle, interrupted)

    def close(self) -> None:
        """Stop, join, and release on the owner thread; repeat calls are harmless."""
        _call(_native.close, self.__handle)

    def __enter__(self) -> Self:
        _call(_native.state, self.__handle)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        try:
            self.close()
        except BaseException as cleanup_error:
            if exc_value is None:
                raise
            raise BaseExceptionGroup(
                "Endpoint body and close both failed", [exc_value, cleanup_error]
            ) from None
        return False
