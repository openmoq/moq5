"""Synchronous media-receiver ownership over the private CPython bridge.

Attachment, lifetime, wait, stats, terminal, the per-track control commands,
track events with owned description copies, owned media objects, and the
`drained()` observation over those two exposed streams. The receive loop
itself is caller policy (see examples/receive_loop.py).
"""

from __future__ import annotations

from types import TracebackType
from typing import Literal, Self
from weakref import WeakValueDictionary

from . import _native
from ._endpoint import Endpoint, _call, _endpoint_handle, _sliced_wait
from ._values import (
    Cenc,
    CmafInit,
    CmafSample,
    CodecKind,
    FatalOrigin,
    MediaObject,
    MediaTemplate,
    MediaType,
    ObjectStatus,
    Packaging,
    ParseDrop,
    ParseDropClass,
    PollOutcome,
    ReceiverConfig,
    ReceiverStats,
    ReceiverTerminal,
    StartMode,
    Terminal,
    Track,
    TrackDescription,
    TrackEvent,
    TrackEventKind,
    TrackState,
    VodState,
    WaitResult,
    _bounded_integer,
    _integer,
    _known_or_int,
)

_PRIORITY_MAX = 255


def _description(d: dict) -> TrackDescription:
    """Build the owned, immutable description from the bridge's copied values."""
    init = None
    if d["init"] is not None:
        i = d["init"]
        cenc = None
        if i["cenc"] is not None:
            scheme, protected, iv_size, kid = i["cenc"]
            cenc = Cenc(scheme, protected, iv_size, kid)
        init = CmafInit(
            None if i["codec_kind"] is None else _known_or_int(CodecKind, i["codec_kind"]),
            i["timescale"], i["width"], i["height"], i["samplerate"], i["channel_count"],
            i["codec_config"], i["track_id"], cenc,
        )
    template = None if d["template"] is None else MediaTemplate(*d["template"])
    return TrackDescription(
        name=d["name"],
        role=d["role"],
        codec=d["codec"],
        lang=d["lang"],
        label=d["label"],
        media_type=None if d["media_type"] is None else _known_or_int(MediaType, d["media_type"]),
        packaging=None if d["packaging"] is None else _known_or_int(Packaging, d["packaging"]),
        timescale=d["timescale"],
        transport_version=d["transport_version"],
        init=init,
        init_data=d["init_data"],
        width=d["width"],
        height=d["height"],
        samplerate=d["samplerate"],
        channel_config=d["channel_config"],
        framerate_millis=d["framerate_millis"],
        bitrate=d["bitrate"],
        max_grp_sap=d["max_grp_sap"],
        max_obj_sap=d["max_obj_sap"],
        content_protection_ref_ids=d["content_protection_ref_ids"],
        packaging_text=d["packaging_text"],
        event_type=d["event_type"],
        mime_type=d["mime_type"],
        depends=d["depends"],
        template=template,
        vod=VodState(d["is_live"], d["track_duration_ms"]),
    )


class Receiver:
    """A media receiver attached to an Endpoint, owned by the attaching thread.

    The receiver retains its endpoint until close(): a live Receiver keeps the
    Endpoint wrapper and the native endpoint alive, and Endpoint.close() is
    refused by the service (MoqError, WRONG_STATE) while a receiver is
    attached. Every operation, wait() included, requires the owner thread;
    only `closed` may be read from another thread. To cancel a blocked wait
    from another thread use the retained endpoint's allowed cross-thread
    operations, `receiver.endpoint.set_interrupted(True)` or `wake()`.
    """

    __slots__ = ("__handle", "__endpoint", "__tracks", "__closed", "__weakref__")
    __handle: object
    __endpoint: Endpoint
    __tracks: WeakValueDictionary[int, Track]
    __closed: list[bool]      # last outcome of [poll_track, poll_object] was CLOSED

    def __new__(cls) -> Self:
        raise TypeError("Use Receiver.attach(endpoint, ReceiverConfig(...))")

    @classmethod
    def attach(cls, endpoint: Endpoint, config: ReceiverConfig) -> Self:
        """Attach to a live endpoint. Native refusals raise MoqError with the
        original signed code and operation "attach"; nothing is retained then."""
        if not isinstance(endpoint, Endpoint):
            raise TypeError("endpoint must be Endpoint")
        if not isinstance(config, ReceiverConfig):
            raise TypeError("config must be ReceiverConfig")
        receiver = object.__new__(cls)
        receiver.__tracks = WeakValueDictionary()
        receiver.__closed = [False, False]
        receiver.__handle = _call(
            _native.receiver_attach,
            _endpoint_handle(endpoint),
            config.namespace,
            config.catalog_track,
            config.auto_subscribe,
            int(config.time_mode),
            int(config.overflow),
            config.max_objects,
            config.max_bytes,
            config.max_track_events,
        )
        receiver.__endpoint = endpoint
        return receiver

    @property
    def endpoint(self) -> Endpoint:
        """The retained endpoint (identity: `rx.endpoint is ep`)."""
        return self.__endpoint

    @property
    def closed(self) -> bool:
        """Whether this owner released the native receiver (not: terminal)."""
        return _call(_native.receiver_closed, self.__handle)

    @property
    def terminal(self) -> ReceiverTerminal:
        """Copy closed/fatal/fatal_code plus the endpoint's terminal snapshot."""
        closed, fatal, fatal_code, reason, detail = _call(_native.receiver_terminal, self.__handle)
        return ReceiverTerminal(
            closed,
            fatal,
            fatal_code,
            Terminal(reason, detail),
            FatalOrigin.UNKNOWN if fatal else FatalOrigin.NONE,
        )

    def stats(self) -> ReceiverStats:
        """Copy the stats snapshot; appended fields are None when not stamped."""
        return ReceiverStats(*_call(_native.receiver_stats, self.__handle))

    def wait(self, timeout_us: int) -> WaitResult:
        """Wait for pollable work, a wake, terminal, or the timeout, sliced
        exactly like Endpoint.wait. Native priority: queued work -> WOKEN,
        then terminal -> CLOSED, then the endpoint wait (whose INTERRUPTED and
        TIMED_OUT are returned as is), then the same recheck."""
        return _sliced_wait(_native.receiver_wait, self.__handle, timeout_us)

    def _own(self, track: object) -> object:
        if not isinstance(track, Track):
            raise TypeError("track must be Track")
        if track._receiver is not self:
            raise RuntimeError("track belongs to another receiver")
        return track._capsule

    def subscribe(
        self, track: Track, *, start: StartMode = StartMode.CURRENT, priority: int | None = None
    ) -> None:
        """Record the intent to receive `track`; the service subscribes on its
        thread. Start mode and priority apply only when the subscription is
        first issued. Refusals keep the native code: INVAL, WRONG_STATE
        (ended/removed), CLOSED (terminal), UNSUPPORTED (namespace override)."""
        capsule = self._own(track)
        start = StartMode(_integer(start, "start"))
        if priority is not None:
            priority = _bounded_integer(priority, "priority", _PRIORITY_MAX)
        _call(_native.receiver_subscribe, self.__handle, capsule, int(start), priority)

    def unsubscribe(self, track: Track) -> None:
        """Pause delivery for `track` and purge its queued objects."""
        _call(_native.receiver_unsubscribe, self.__handle, self._own(track))

    def track_state(self, track: Track) -> TrackState | int:
        """A point-in-time delivery state; unknown future states stay integers."""
        return _known_or_int(TrackState, _call(_native.receiver_track_state, self.__handle, self._own(track)))

    def _track_from_capsule(self, capsule: object) -> Track:
        """Identity-cached Track for a native handle capsule (package-internal)."""
        key = _call(_native.track_key, capsule)
        track = self.__tracks.get(key)
        if track is None:
            track = Track(self, capsule, key)
            self.__tracks[key] = track
        return track

    def poll_track(self) -> TrackEvent | PollOutcome:
        """Dequeue one track event, or EMPTY (nothing now) / CLOSED (empty AND
        terminal). Ignores the interrupt latch. The description is the
        track's CURRENT state copied under the receiver mutex at poll time,
        not the state when the event was queued; it is published to
        `track.description` only after the whole event converted. A
        conversion failure raises EventLost for the consumed event; the next
        poll proceeds normally."""
        # A forbidden caller is refused before any evidence changes; then this
        # newer poll withdraws the stream's CLOSED evidence until it observes
        # CLOSED itself (an error or a conversion failure leaves it withdrawn).
        _call(_native.receiver_enter, self.__handle)
        self.__closed[0] = False
        result = _call(_native.receiver_poll_track, self.__handle)
        if isinstance(result, int):
            outcome = PollOutcome(result)
            if outcome is PollOutcome.CLOSED:
                self.__closed[0] = True
            return outcome
        kind, capsule, desc, largest, expires_ms, drop = result
        # Convert everything BEFORE publishing anything: no half-updated
        # Track or cache survives a failure below.
        description = None if desc is None else _description(desc)
        parse_drop = None
        if drop is not None:
            cls, total, delta = drop
            parse_drop = ParseDrop(_known_or_int(ParseDropClass, cls), total, delta)
        track = None
        publish = None
        if capsule is not None:
            key = _call(_native.track_key, capsule)
            track = self.__tracks.get(key)
            if track is None:
                track = Track(self, capsule, key)
                publish = key
        event = TrackEvent(_known_or_int(TrackEventKind, kind), track, description,
                           None if largest is None else (largest[0], largest[1]), expires_ms, parse_drop)
        if publish is not None:
            self.__tracks[publish] = track
        if track is not None:
            track._description = description
        return event

    def poll_object(self) -> MediaObject | PollOutcome:
        """Dequeue one media object as a fully owned value, or the exact
        non-item outcome: INTERRUPTED while the endpoint latch is set (checked
        before the queue and before terminal), CLOSED when the object queue
        is empty AND the receiver is terminal, EMPTY otherwise. One native
        poll, no waiting. A successful dequeue is consumed: its buffers are
        copied and released exactly once on this thread before anything is
        returned, whatever happens afterwards. `track` is the same cached
        Track a track event yields for that handle, described from the
        track's CURRENT state; a new Track or a replaced description is
        published only after the whole object converted, so a conversion
        failure (ObjectLost, or a bare MemoryError) leaves the cache and any
        prior description untouched and the next poll proceeds."""
        _call(_native.receiver_enter, self.__handle)
        self.__closed[1] = False
        result = _call(_native.receiver_poll_object, self.__handle)
        if isinstance(result, int):
            outcome = PollOutcome(result)
            if outcome is PollOutcome.CLOSED:
                self.__closed[1] = True
            return outcome
        (capsule, desc, config_generation, packaging, status, end_of_group, datagram, keyframe,
         capture_time_us, decode_time_us, composition_offset_us, presentation_time_us,
         payload, fragment, mdat_offset, mdat_len, samples) = result
        description = _description(desc)
        key = _call(_native.track_key, capsule)
        track = self.__tracks.get(key)
        publish = None
        if track is None:
            track = Track(self, capsule, key)
            publish = key
        media = MediaObject(
            track=track,
            config_generation=config_generation,
            packaging=_known_or_int(Packaging, packaging),
            status=_known_or_int(ObjectStatus, status),
            end_of_group=end_of_group,
            datagram=datagram,
            keyframe=keyframe,
            capture_time_us=capture_time_us,
            decode_time_us=decode_time_us,
            composition_offset_us=composition_offset_us,
            presentation_time_us=presentation_time_us,
            payload=payload,
            fragment=fragment,
            mdat_offset=mdat_offset,
            mdat_len=mdat_len,
            samples=tuple(CmafSample(*sample) for sample in samples),
        )
        # publish only now: the cache entry first, then the description
        if publish is not None:
            self.__tracks[publish] = track
        track._description = description
        return media

    def drained(self) -> bool:
        """Whether the two EXPOSED streams are drained: the most recent
        poll_track AND the most recent poll_object each returned CLOSED (the
        queue empty and the receiver terminal). Initially False. A pure
        observation of recorded outcomes: it never polls, waits, clears the
        interrupt latch, or infers from terminal state, stats or queue
        counts. Any newer non-CLOSED outcome on a stream (an item, EMPTY,
        INTERRUPTED, a native error or a consumed-item conversion failure)
        withdraws that stream's evidence; the other stream's is kept. The
        native SAP and media-timeline queues are not exposed by this binding
        and are not covered. Requires the owner thread, process and a live
        receiver; a refused caller changes nothing. Close is not drain."""
        _call(_native.receiver_enter, self.__handle)
        return self.__closed[0] and self.__closed[1]

    def close(self) -> None:
        """Destroy the native receiver and release the endpoint retention on the
        owner thread; repeat calls are harmless."""
        _call(_native.receiver_close, self.__handle)

    def __enter__(self) -> Self:
        _call(_native.receiver_enter, self.__handle)
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
                "Receiver body and close both failed", [exc_value, cleanup_error]
            ) from None
        return False
