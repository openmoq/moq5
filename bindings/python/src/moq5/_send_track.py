"""A media track owned by the native sender, handed back by Sender.add_track."""

from __future__ import annotations

from typing import TYPE_CHECKING

from . import _native
from ._endpoint import _call

if TYPE_CHECKING:                                    # pragma: no cover
    from ._sender import Sender

__all__ = ["SendTrack"]


def _handle_of(track: "SendTrack") -> object:
    """The track's capsule, for the owning sender's bridge calls only."""
    return getattr(track, "_SendTrack__handle")


class SendTrack:
    """One track registered with a Sender.

    The NATIVE sender owns its tracks until it is destroyed. Dropping this
    wrapper removes nothing and ends nothing; there is no per-track native
    destructor and no implicit removal on collection. A removed track keeps a
    valid but inert handle until its sender is destroyed, so `name`, `sender`
    and `removed` stay readable after removal.

    Identity is the wrapper's own: two tracks are distinct keys and a track's
    hash and equality do not change when it is removed or when its sender is
    closed. There is no track registry.

    Construct one only through `Sender.add_track(config)`.
    """

    __slots__ = ("__handle", "__sender", "__name", "__weakref__")
    __handle: object
    __sender: "Sender"
    __name: bytes

    def __new__(cls) -> "SendTrack":
        raise TypeError("Use Sender.add_track(SendTrackConfig(...))")

    @classmethod
    def _prepare(cls, sender: "Sender", name: bytes, handle: object) -> "SendTrack":
        """Build the wrapper and commit every field, all BEFORE the native
        registration. This is internal bridge machinery, not a user-facing
        factory, and it is the construction boundary the tests inject at."""
        track = object.__new__(cls)
        track.__sender = sender
        track.__name = name
        track.__handle = handle
        return track

    @property
    def sender(self) -> "Sender":
        """The owning Sender. A live track keeps it, and its endpoint, alive."""
        return self.__sender

    @property
    def name(self) -> bytes:
        """The catalog track name, exactly as it was declared."""
        return self.__name

    @property
    def removed(self) -> bool:
        """Whether this track has been removed from the catalog. A removed
        track is inert, not destroyed.

        Both removals made through this binding count: this track's own
        remove_track, and an accepted Sender.request_complete(), which the
        service answers by marking every track the sender then had removed.
        It stays readable once the sender is closed. It describes catalog
        removal, not that anything was emitted or received.
        """
        return _call(_native.send_track_removed, self.__handle)
