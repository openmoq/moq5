"""Declared data for the finite RAW/LOC receiver runtime fixture.

Everything the oracle compares against is declared HERE, before any process
is launched. The C publisher sends exactly these bytes (handed to it in a
file the driver writes from this module); its receipts are additional
evidence, never the authority. The payloads are carrier test data with
embedded zero bytes, not playable H.264.
"""

N = 8
NAMESPACE = (b"moq5", b"python-runtime")
TRACK = b"video"
CODEC = b"avc1.42E01E"          # synthetic codec string; nothing here is decodable
TIMESCALE = 90000
WIDTH, HEIGHT, FRAMERATE_MILLIS, BITRATE = 320, 240, 24_000, 400_000
FRAME_US = 1_000_000 // 24

# (index, is_sync, payload). Sync/group starts at 0, 3 and 6; every payload
# distinct, with embedded NUL bytes; lengths differ so a length swap is caught.
OBJECTS = tuple(
    (i, i in (0, 3, 6), bytes([0x00, 0x00, 0x00, 0x01, 0x65 if i in (0, 3, 6) else 0x41, i]) + b"\x00moq5\x00" + bytes(range(i, i + 5 + i)))
    for i in range(N)
)
PTS_US = tuple(i * FRAME_US for i in range(N))          # synthetic relative presentation times

assert len({p for _, _, p in OBJECTS}) == N, "payloads must be distinct"
assert all(b"\x00" in p for _, _, p in OBJECTS)


def declared_expectations(moq5, track):
    """The exact MediaObject values expected, in order, for a delivered Track.

    TimeMode.RAW: the publisher writes no capture time, so the LOC Capture
    Timestamp is the presentation fallback and the receiver reports
    capture == decode == presentation == PTS_US[i], composition 0. These are
    synthetic relative values, not a genuine epoch capture clock.
    end_of_group is true on every object in the sender's sole-subgroup path;
    it is not a last-frame marker and group ids are not exposed in Python.
    """
    return [
        moq5.MediaObject(
            track=track, config_generation=0, packaging=moq5.Packaging.RAW, status=moq5.ObjectStatus.NORMAL,
            end_of_group=True, datagram=False, keyframe=is_sync, capture_time_us=PTS_US[i],
            decode_time_us=PTS_US[i], composition_offset_us=0, presentation_time_us=PTS_US[i],
            payload=payload, fragment=b"", mdat_offset=0, mdat_len=0, samples=())
        for i, is_sync, payload in OBJECTS
    ]


def write_objects_file(path):
    """Binary records for the C publisher: u32 len, u8 is_sync, u64 pts_us, bytes."""
    import struct
    with open(path, "wb") as f:
        for i, is_sync, payload in OBJECTS:
            f.write(struct.pack("<IBQ", len(payload), 1 if is_sync else 0, PTS_US[i]))
            f.write(payload)


def compare(expected, received):
    """The exact inventory comparator: count, order, every public field.
    Returns a list of named mismatches (empty == identical)."""
    problems = []
    if len(received) != len(expected):
        problems.append(f"count {len(received)} != declared {len(expected)}")
    for i, (e, r) in enumerate(zip(expected, received)):
        if r.payload != e.payload:
            problems.append(f"object {i}: payload differs (received {r.payload!r})")
        for field in ("keyframe", "packaging", "status", "end_of_group", "datagram", "capture_time_us",
                      "decode_time_us", "composition_offset_us", "presentation_time_us", "fragment",
                      "mdat_offset", "mdat_len", "samples", "config_generation"):
            if getattr(r, field) != getattr(e, field):
                problems.append(f"object {i}: {field} {getattr(r, field)!r} != {getattr(e, field)!r}")
        if r.track is not e.track:
            problems.append(f"object {i}: track identity differs")
    return problems
