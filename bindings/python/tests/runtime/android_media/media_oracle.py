"""The exact host-receiver oracle for the Android real-media run.

The expectation is the objects file make_input.py writes from the generated
access units (records: u32 len, u8 is_sync, u64 pts_us, bytes), the same file
the runtime publisher reads. It is never derived from anything the publisher
reports about itself.
"""
import struct
from collections import namedtuple
from pathlib import Path

Expected = namedtuple("Expected", "index payload is_sync pts_us")
Received = namedtuple("Received", "payload keyframe pts_us")
_HEAD = struct.Struct("<IBQ")


def load_objects(path):
    data = Path(path).read_bytes()
    out, off = [], 0
    while off < len(data):
        if off + _HEAD.size > len(data):
            raise ValueError(f"truncated record header at byte {off}")
        length, sync, pts = _HEAD.unpack_from(data, off)
        off += _HEAD.size
        if sync not in (0, 1):
            raise ValueError(f"record {len(out)}: is_sync byte {sync} is not 0 or 1")
        if off + length > len(data):
            raise ValueError(f"record {len(out)}: payload of {length} bytes runs past the end of the file")
        out.append(Expected(len(out), data[off:off + length], sync == 1, pts))
        off += length
    return out


def received_from(obj):
    """The compared image of one public moq5 MediaObject."""
    return Received(bytes(obj.payload), bool(obj.keyframe), int(obj.presentation_time_us))


def compare(expected, received):
    """Count, order, payload bytes, keyframe and presentation time. Returns
    named mismatches; an empty list means identical."""
    problems = []
    if len(received) != len(expected):
        problems.append(f"count {len(received)} != expected {len(expected)}")
    for e, r in zip(expected, received):
        if r.payload != e.payload:
            problems.append(f"object {e.index}: payload differs ({len(r.payload)} bytes received, {len(e.payload)} expected)")
        if r.keyframe != e.is_sync:
            problems.append(f"object {e.index}: keyframe {r.keyframe} != {e.is_sync}")
        if r.pts_us != e.pts_us:
            problems.append(f"object {e.index}: pts_us {r.pts_us} != {e.pts_us}")
    return problems
