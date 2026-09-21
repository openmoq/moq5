"""Synchronous sender ownership over the private CPython bridge."""

from __future__ import annotations

from types import TracebackType
from typing import Literal, Self

from . import _native, _send_track
from ._endpoint import (Endpoint, MoqError, TrackNameBusy, _call,
                        _endpoint_handle, _sliced_wait)
from ._values import (
    SendObject,
    WaitResult,
    SenderStats,
    SendTrackConfig,
    SenderConfig,
    SenderTerminal,
    Terminal,
    WriteOutcome,
)

_WOULD_BLOCK = -8


def _cancel(handle: object, primary: BaseException) -> None:
    """Cancel an inactive prepared track after `primary` ended the attempt.

    Cancellation must never REPLACE the caller's failure, but a cancellation
    that did not complete must not be hidden either: the ownership claim would
    then be false. On success the primary is re-raised unchanged by the caller;
    on failure both are preserved together, following the same convention the
    endpoint and sender context managers use for a failed body plus a failed
    close.
    """
    try:
        _native.send_track_abort(handle)
    except BaseException as cleanup_error:
        raise BaseExceptionGroup(
            "Track preparation failed and its cancellation also failed",
            [primary, cleanup_error]) from None

__all__ = ["Sender"]


class Sender:
    """A media sender attached to an Endpoint, owned by the attaching thread.

    The sender retains its endpoint until close(): a live Sender keeps the
    Endpoint wrapper and the native endpoint alive, and Endpoint.close() is
    refused by the service (MoqError, WRONG_STATE) while a sender is attached.
    Every operation requires the owner thread; only `closed` may be read from
    another thread. Native operations reject handles inherited across fork.

    Configuration, attachment, readiness and terminal snapshots, owned
    lifetime, declaring and removing tracks, writing media objects, requesting
    a per-track end, requesting broadcast completion, copying a statistics
    snapshot, asking about subscriber demand, and a bounded wait for the
    write level.

    Not here: a graceful finish and a drain. request_complete() is a request,
    not a flush: it does not wait for anything to be emitted or received.
    close() is DESTRUCTIVE -- it detaches immediately and discards anything
    the service had queued.
    """

    __slots__ = ("__handle", "__endpoint", "__weakref__")
    __handle: object
    __endpoint: Endpoint

    def __new__(cls) -> Self:
        raise TypeError("Use Sender.attach(endpoint, SenderConfig(...))")

    @classmethod
    def attach(cls, endpoint: Endpoint, config: SenderConfig) -> Self:
        """Attach to a live endpoint. Native refusals raise MoqError with the
        original signed code and operation "attach"; nothing is retained then.

        The transaction completes before anything is attached: this wrapper,
        the native handle, its capsule and BOTH slot stores are committed
        while the capsule is still inert, and only then is the native
        attachment activated. A subclass whose ``__setattr__`` raises
        therefore cannot strand an attachment, and nothing fallible runs
        after the native call succeeds.
        """
        if not isinstance(endpoint, Endpoint):
            raise TypeError("endpoint must be Endpoint")
        if not isinstance(config, SenderConfig):
            raise TypeError("config must be SenderConfig")
        sender = object.__new__(cls)
        handle = _call(_native.sender_prepare, _endpoint_handle(endpoint))
        sender.__endpoint = endpoint
        sender.__handle = handle
        _call(
            _native.sender_activate,
            handle,
            config.namespace,
            config.catalog_track,
            int(config.backpressure),
            config.block_timeout_us,
            config.queue_max_objects,
            config.queue_max_bytes,
            config.pre_ready_max_objects,
            config.pre_ready_max_bytes,
            config.validate_cmaf,
            config.publish_tracks,
            config.drop_without_demand,
            config.catalog_refresh_interval_us,
        )
        return sender

    def add_track(self, config: SendTrackConfig) -> "_send_track.SendTrack":
        """Register a track and return its handle.

        The transaction completes before anything is registered: the native
        handle, its capsule, the retained name and the whole wrapper are
        established first, and the native add is the LAST fallible step. A
        refusal registers nothing and retains nothing; a retryable busy name
        raises TrackNameBusy.
        """
        if not isinstance(config, SendTrackConfig):
            raise TypeError("config must be SendTrackConfig")
        handle = _call(_native.send_track_prepare, self.__handle, config.name)
        # One cleanup scope covers everything after the prepare: the wrapper
        # construction, its field stores, the argument reads and the
        # activation. Any failure cancels the INACTIVE prepared transaction, so
        # the extra ownership inside the native handle is released even when
        # the caller keeps the exception and its traceback alive. Ordinary
        # frame references to self or the config may of course survive; those
        # are not the retention this releases.
        try:
            track = _send_track.SendTrack._prepare(self, config.name, handle)
            # Argument evaluation happens HERE, outside the translation scope.
            # A subclass property may raise anything at all, including its own
            # MoqError, and that exception is the caller's: it is not a native
            # add result and must reach them exactly as raised.
            arguments = (
                int(config.media_type),
                int(config.packaging),
                config.codec,
                config.bitrate,
                config.timescale,
                config.init_data,
                config.role,
                config.lang,
                config.is_live,
                config.width,
                config.height,
                config.framerate_millis,
                config.samplerate,
                config.channel_config,
                config.track_duration_ms,
            )
            # Only the NATIVE activation result is translated, and only here.
            try:
                _call(_native.send_track_activate, handle, *arguments)
            except MoqError as native_error:
                if native_error.code == _WOULD_BLOCK:
                    raise TrackNameBusy(native_error.code,
                                        native_error.operation,
                                        native_error.args[2]) from native_error
                raise
        except BaseException as primary:
            _cancel(handle, primary)
            raise
        return track

    def write(self, track: "_send_track.SendTrack",
              object: SendObject) -> WriteOutcome:
        """Submit one media object to a track this sender owns.

        Exactly one synchronous native call per invocation. On ACCEPTED the
        service owns the object's spans; the agreed refusals -- WOULD_BLOCK,
        INTERRUPTED and CLOSED -- are returned as values and took no
        ownership. Any other native result keeps its signed code and the
        operation "write" as a MoqError. Nothing is retried, normalised, or
        turned into an implicit remove or end.

        ACCEPTED means accepted by the service, not delivered: a drop policy
        may still evict the object, and there is no receipt here.

        A signal delivered while the call is in flight reaches the caller
        after ownership has been settled. It therefore does NOT certify that
        the object was refused: if the service accepted it, the transfer
        stands. Do not retry blindly on KeyboardInterrupt -- a second write is
        a NEW object.
        """
        if not isinstance(track, _send_track.SendTrack):
            raise TypeError("track must be a SendTrack")
        if track.sender is not self:
            raise RuntimeError("this track belongs to another sender")
        if not isinstance(object, SendObject):
            raise TypeError("object must be a SendObject")
        # Argument reads finish BEFORE the native call, so a subclass property
        # that raises reaches the caller as its own exception and never as a
        # write result.
        arguments = (
            object.payload,
            object.properties,
            object.is_sync,
            object.starts_group,
            object.ends_group,
            object.decode_time_us,
            object.presentation_time_us,
            object.capture_time_us,
            None if object.sap_type is None else int(object.sap_type),
        )
        return WriteOutcome(_call(_native.sender_write, self.__handle,
                                  _send_track._handle_of(track), *arguments))

    def end_track(self, track: "_send_track.SendTrack") -> WriteOutcome:
        """Request that a track this sender owns be ended.

        Exactly one synchronous native call per invocation. ACCEPTED means the
        service accepted the REQUEST: after this track's queued objects drain
        it emits a reliable END_OF_TRACK, which receivers see as a track-ended
        event. It does not mean the terminal has been emitted, that prior
        media was flushed, or that anything was delivered.

        The agreed refusals -- WOULD_BLOCK (the queue is momentarily full),
        INTERRUPTED and CLOSED -- are returned as values and commit nothing;
        any other native result keeps its signed code and the operation
        "end_track" as a MoqError. Nothing is retried here: a retry after
        WOULD_BLOCK is the caller's own second call.

        Idempotence is the SERVICE's: this binding keeps no ended state, so a
        repeat is simply a second call. Because the service checks its
        interrupt and terminal conditions before that idempotent no-op, a
        repeat after the sender terminalizes is CLOSED, not success.

        Ending removes nothing, destroys nothing and closes nothing: the track
        keeps its handle, name and owner, other tracks are untouched, and the
        broadcast and endpoint stay up. A later write on this track is refused
        by the SERVICE (WRONG_STATE), not by a Python latch.

        A signal delivered while the call is in flight reaches the caller
        after the native result is settled, so it does not undo a request the
        service already accepted.
        """
        if not isinstance(track, _send_track.SendTrack):
            raise TypeError("track must be a SendTrack")
        if track.sender is not self:
            raise RuntimeError("this track belongs to another sender")
        return WriteOutcome(_call(_native.sender_end_track, self.__handle,
                                  _send_track._handle_of(track)))

    def request_complete(self) -> None:
        """Ask the service to terminate this broadcast permanently.

        It is a REQUEST, and its successful return means only that the
        service accepted it. Synchronously, the service marks every track
        this sender has as removed -- so `SendTrack.removed` answers True
        afterwards. Everything a receiver would see happens later, on the
        service's own network thread: each active track is ended reliably
        (END_OF_TRACK), and a terminal catalog generation carrying
        isComplete and an empty track list is published and retained for a
        later joiner. Nothing here says that work was emitted, flushed,
        acknowledged or received.

        The queued media of those removed tracks is discarded by the service
        as it tears them down, and counted there. This call performs no
        accounting of its own, and statistics read around it may move because
        the service's own thread is running: an unchanged snapshot is not a
        promise.

        Legal only once the broadcast is ready; the service answers a
        pre-ready request with WRONG_STATE. It is idempotent IN THE SERVICE,
        and this binding keeps no completed state, so a repeat is simply a
        second call: because the service checks its interrupt latch and
        terminal condition before that idempotent no-op, a repeat after
        either is a refusal and not a courtesy success.

        Returns None on acceptance. Every other native result is raised as a
        MoqError with its signed code and the operation "request_complete" --
        there is no retryable outcome here and nothing is retried.

        After an accepted request the service refuses add_track, write,
        end_track and remove_track with WRONG_STATE; the handles stay valid
        but inert. The namespace and the endpoint stay up: this is not
        close(). close() remains destructive -- it detaches and discards
        whatever the service still had queued -- and leaving a `with` block
        still closes.

        A signal delivered while the call is in flight reaches the caller
        after the native result is settled, so it does not undo a request the
        service already accepted: the tracks still report the removal.
        """
        _call(_native.sender_complete, self.__handle)

    def remove_track(self, track: "_send_track.SendTrack") -> None:
        """Remove a track this sender owns. The handle stays valid but inert
        until the sender is destroyed, so its name and owner remain readable.
        """
        if not isinstance(track, _send_track.SendTrack):
            raise TypeError("track must be a SendTrack")
        if track.sender is not self:
            raise RuntimeError("this track belongs to another sender")
        _call(_native.sender_remove_track, self.__handle,
              _send_track._handle_of(track))

    def subscriptions(self, track: "_send_track.SendTrack") -> int:
        """How many active subscriptions this sender's track has, right now.

        An independent point-in-time query over the service's mirrored count,
        covering the whole native range as an ordinary nonnegative int. It is
        not cached, not computed here, and not a promise that a later write
        will have demand.

        A track this sender does not own is refused rather than answered zero.
        A REMOVED track reports what the service reports: the mirrored count
        until its teardown has drained, then zero. No removed latch is
        applied here.
        """
        return _call(_native.sender_subscriptions, self.__handle,
                     self.__track_handle(track))

    def has_subscriber(self, track: "_send_track.SendTrack") -> bool:
        """Whether this sender's track has any subscriber right now.

        Exactly the service's own `subscriptions(track) > 0`, asked of the
        service rather than recomputed here.
        """
        return _call(_native.sender_has_subscriber, self.__handle,
                     self.__track_handle(track))

    def has_media_subscriber(self) -> bool:
        """Whether ANY app-visible media track has a subscriber -- the
        "should I encode?" question.

        One native observation, never a sum over Python wrappers. The internal
        catalog track, the generated SAP and media-timeline tracks and removed
        tracks are all excluded by the service, so a catalog-only subscription
        does NOT make this true. It is independent of `ready`: demand without
        readiness, and readiness without demand, are both ordinary.
        """
        return _call(_native.sender_has_media_subscriber, self.__handle)

    def __track_handle(self, track: "_send_track.SendTrack") -> object:
        """The shared type and ownership check for the demand queries."""
        if not isinstance(track, _send_track.SendTrack):
            raise TypeError("track must be a SendTrack")
        if track.sender is not self:
            raise RuntimeError("this track belongs to another sender")
        return _send_track._handle_of(track)

    def wait(self, timeout_us: int) -> WaitResult:
        """Wait until this sender can accept another write, the endpoint
        wakes, or the timeout elapses.

        Sliced exactly like Endpoint.wait and Receiver.wait, against ONE
        monotonic deadline: bounded native slices, the remainder rounded up,
        and an exhausted or zero budget still making exactly one observation.

        The service's own priority applies, and this binding does not
        reorder it: the interrupt latch wins, then terminal state, then the
        write level -- because write() refuses the first two, so reporting
        "write now" there would be a lie.

        WOKEN is ADVISORY: it means "try another write now", and it is also
        what an endpoint wake reports even when the level does not hold. It is
        not subscriber demand, not a promise that the next write is accepted,
        and not completion, drain or flush. TIMED_OUT means the budget elapsed
        with the level not holding; INTERRUPTED means the latch is set; CLOSED
        means the sender or endpoint is terminal. Any other native result
        keeps its signed code as a MoqError with the operation "wait".

        Waiting has no side effects: it does not clear the interrupt latch,
        write, end or remove a track, close anything, or change the drop
        policy. A terminal sender still answers -- terminal is not closed --
        while a closed owner is refused.
        """
        return _sliced_wait(_native.sender_wait, self.__handle, timeout_us)

    def stats(self) -> SenderStats:
        """Copy a statistics snapshot from the service.

        Each call takes a NEW native snapshot; nothing is cached, computed in
        Python, reset or repaired here, and no native pointer is retained. A
        live sender answers even when it is terminal -- terminal is not the
        same as closed -- while a closed owner is refused.

        `sap_records_evicted` is None when the service's stamped prefix does
        not cover that whole appended field. A stamped zero is a present zero.
        `last_error` is historical data, including an unknown negative code,
        and is never raised.
        """
        return SenderStats(*_call(_native.sender_stats, self.__handle))

    @property
    def endpoint(self) -> Endpoint:
        """The retained endpoint (identity: `tx.endpoint is ep`)."""
        return self.__endpoint

    @property
    def closed(self) -> bool:
        """Whether this owner released the native sender (not: terminal)."""
        return _call(_native.sender_closed, self.__handle)

    @property
    def ready(self) -> bool:
        """Namespace accepted and catalog published, so the publish path can
        accept media. It does NOT mean anyone has subscribed: demand is a
        separate observation this shell does not expose."""
        return _call(_native.sender_ready, self.__handle)

    @property
    def terminal(self) -> SenderTerminal:
        """Copy closed/fatal/fatal_code plus the endpoint's terminal snapshot.
        `fatal` includes an endpoint failure, and `fatal_code` falls back to
        the endpoint's code when the sender itself is not fatal."""
        closed, fatal, fatal_code, reason, detail = _call(_native.sender_terminal, self.__handle)
        return SenderTerminal(closed, fatal, fatal_code, Terminal(reason, detail))

    def close(self) -> None:
        """Destroy the native sender and release the endpoint retention on the
        owner thread; repeat calls are harmless.

        DESTRUCTIVE: anything the service had queued is discarded. This is not
        a flush, and it does not complete a broadcast.
        """
        _call(_native.sender_close, self.__handle)

    def __enter__(self) -> Self:
        _call(_native.sender_enter, self.__handle)
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
                "Sender body and close both failed", [exc_value, cleanup_error]
            ) from None
        return False
