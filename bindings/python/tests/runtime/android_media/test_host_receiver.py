"""Offline tests for the supervised run's host receiver.

    python -I bindings/python/tests/runtime/android_media/test_host_receiver.py

A fake receiver drives the client without sockets. Its value shapes, enum
member names and receiver methods are pinned to the published moq5 package
by parsing that package's source (no native import), so the client path is
the one a real Endpoint/Receiver will drive. Every expectation comes from an
objects file written here and read back through media_oracle.load_objects.
"""
import ast
import socket
import struct
import sys
import tempfile
import unittest
from dataclasses import dataclass, replace
from enum import IntEnum
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))
MOQ5_SRC = HERE.parents[2] / "src" / "moq5"

_real_connect, _real_bind = socket.socket.connect, socket.socket.bind


def _tripwire(*_a, **_k):
    raise AssertionError("network use in an offline test")


socket.socket.connect = _tripwire
socket.socket.bind = _tripwire


# ------------------------------------------------------------------ a moq5-shaped fake
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


class StartMode(IntEnum):
    CURRENT = 0
    NEXT_GROUP = 1


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


FAKE_MOQ5 = SimpleNamespace(TrackEventKind=TrackEventKind, TrackState=TrackState, PollOutcome=PollOutcome,
                            ObjectStatus=ObjectStatus, WaitResult=WaitResult, Packaging=Packaging, MediaType=MediaType,
                            StartMode=StartMode, TrackEvent=TrackEvent, MediaObject=MediaObject,
                            ReceiverStats=ReceiverStats, ReceiverTerminal=ReceiverTerminal)


class Track:
    """An opaque handle; the client learns its name only from `description`."""

    def __init__(self, name, media_type=MediaType.VIDEO):
        self.description = SimpleNamespace(name=name, media_type=media_type, packaging=Packaging.RAW)


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


class FakeReceiver:
    """Scripted delivery. Each wait() advances the clock by its timeout and
    fires the first pending step whose `when(sent)` holds; a step may queue
    events/objects, set a track state, set stats, or make the receiver
    terminal. Records every call so tests can check order and ownership."""

    def __init__(self, clock, sent, steps, initial=()):
        self.clock, self.sent, self.steps = clock, sent, list(steps)
        self.events, self.objects = [], []
        self.states, self.stats_value = {}, ReceiverStats()
        self.terminal = ReceiverTerminal()
        self.calls, self.subscribed, self.state_reads = [], [], []
        self.closed = 0
        self.log, self.close_failure = [], None
        self.object_outcome = None          # a forced non-item poll_object outcome, e.g. INTERRUPTED
        for act in initial:
            act(self)

    def poll_track(self):
        self.calls.append("poll_track")
        if self.events:
            return self.events.pop(0)
        return PollOutcome.CLOSED if self.terminal.closed else PollOutcome.EMPTY

    def poll_object(self):
        self.calls.append("poll_object")
        if self.object_outcome is not None:
            return self.object_outcome
        if self.objects:
            return self.objects.pop(0)
        return PollOutcome.CLOSED if self.terminal.closed else PollOutcome.EMPTY

    def wait(self, timeout_us):
        self.calls.append("wait")
        self.clock.t += timeout_us / 1e6
        for i, (when, act) in enumerate(self.steps):
            if when(self.sent):
                self.steps.pop(i)
                act(self)
                return WaitResult.WOKEN
        return WaitResult.CLOSED if self.terminal.closed else WaitResult.TIMED_OUT

    def subscribe(self, track, *, start=StartMode.CURRENT, priority=None):
        self.calls.append("subscribe")
        self.subscribed.append(track)

    def unsubscribe(self, track):
        self.calls.append("unsubscribe")

    def track_state(self, track):
        s = self.states.get(track, TrackState.DISCOVERED)
        self.state_reads.append((len(self.sent), s))
        return s

    def stats(self):
        return self.stats_value

    def drained(self):
        # Only both streams' last polls being CLOSED count; an INTERRUPTED object poll withdraws that evidence.
        return self.terminal.closed and self.object_outcome is None and not self.events and not self.objects

    def close(self):
        self.closed += 1
        self.sent_at_close = list(self.sent)
        self.log.append("receiver")
        if self.close_failure is not None:
            raise self.close_failure


class FakeEndpoint:
    """Records each close ATTEMPT; a configured failure raises after recording."""

    def __init__(self, log, close_failure=None):
        self.log, self.close_failure, self.attempts = log, close_failure, 0

    def close(self):
        self.attempts += 1
        self.log.append("endpoint")
        if self.close_failure is not None:
            raise self.close_failure


# ------------------------------------------------------------------ the expectation file
def write_objects(path, n=60, sync_every=24, step=41_666):
    with open(path, "wb") as f:
        for i in range(n):
            payload = bytes([0, 0, 0, 1, 9]) + i.to_bytes(2, "big") * (3 + i % 5)
            f.write(struct.pack("<IBQ", len(payload), 1 if i % sync_every == 0 else 0, i * step))
            f.write(payload)


def media_for(track, expected):
    return [MediaObject(track=track, keyframe=e.is_sync, presentation_time_us=e.pts_us, payload=e.payload) for e in expected]



class Scenario:
    """A standard run: video announced; ACTIVE after one wait; after `go`, the
    objects; after `ack-all`, TRACK_ENDED; after `ended-observed`, REMOVED plus
    catalog isComplete. Tests replace pieces."""

    def __init__(self, expected):
        self.expected = expected
        self.video = Track(b"video")
        self.other = Track(b"audio", MediaType.AUDIO)
        self.objects = media_for(self.video, expected)
        self.announce = [TrackEvent(TrackEventKind.ADDED, self.video, self.video.description),
                         TrackEvent(TrackEventKind.CATALOG_READY, None, None)]
        self.activate_after = 1
        self.ended = [TrackEvent(TrackEventKind.ENDED, self.video, self.video.description)]
        self.removed = [TrackEvent(TrackEventKind.REMOVED, self.video, self.video.description)]
        self.complete = True
        self.extra_steps = []

    def steps(self):
        v = self.video

        def activate(rx):
            rx.states[v] = TrackState.ACTIVE

        def media(rx):
            rx.objects.extend(self.objects)

        def ended(rx):
            rx.events.extend(self.ended)

        def removed(rx):
            rx.events.extend(self.removed)
            rx.stats_value = replace(rx.stats_value, catalog_complete=self.complete)
        seq = []
        if self.activate_after is not None:        # ACTIVE on the Nth wait; None: never
            seq = [(lambda s: True, lambda rx: None)] * (self.activate_after - 1) + [(lambda s: True, activate)]
        return self.extra_steps + seq + [
            (lambda s: "go" in s, media),
            (lambda s: "ack-all" in s, ended),
            (lambda s: "ended-observed" in s, removed),
        ]


def run(scenario, deadline_s=10.0, initial=(), receiver_close_failure=None, endpoint_close_failure=None):
    """`closed` is the log of close ATTEMPTS, in order, whether or not they raised."""
    import host_receiver
    clock, sent, closed = Clock(), [], []

    def announce(rx):
        rx.events.extend(scenario.announce)
        rx.states.setdefault(scenario.video, TrackState.PENDING)
    rx = FakeReceiver(clock, sent, scenario.steps(), initial=(announce, *initial))
    rx.log, rx.close_failure = closed, receiver_close_failure
    endpoint = FakeEndpoint(closed, endpoint_close_failure)
    receipt = host_receiver.open_and_run(FAKE_MOQ5, lambda: endpoint, lambda ep: rx if ep is endpoint else None,
                                         scenario.expected, sent.append, clock, deadline_s, wait_us=250_000)
    return receipt, rx, sent, closed, clock


class Expected:
    def __init__(self):
        import media_oracle
        self.dir = tempfile.TemporaryDirectory()
        self.path = Path(self.dir.name) / "objects.bin"
        write_objects(self.path)
        self.list = media_oracle.load_objects(self.path)


# ------------------------------------------------------------------ 0. shapes pinned to the published package
class PublishedShapes(unittest.TestCase):
    def _classes(self, name):
        return {n.name: n for n in ast.parse((MOQ5_SRC / name).read_text()).body if isinstance(n, ast.ClassDef)}

    def _fields(self, cls):
        return [s.target.id for s in cls.body if isinstance(s, ast.AnnAssign) and isinstance(s.target, ast.Name)]

    def _members(self, cls):
        return [s.targets[0].id for s in cls.body if isinstance(s, ast.Assign) and isinstance(s.targets[0], ast.Name)]

    def test_fake_values_match_the_published_dataclasses_and_enums(self):
        c = self._classes("_values.py")
        for fake in (TrackEvent, MediaObject, ReceiverStats, ReceiverTerminal):
            self.assertEqual(self._fields(c[fake.__name__]), list(fake.__dataclass_fields__), fake.__name__)
        for fake in (TrackEventKind, TrackState, PollOutcome, ObjectStatus, WaitResult, Packaging, MediaType, StartMode):
            self.assertEqual(self._members(c[fake.__name__]), [m.name for m in fake], fake.__name__)

    def test_fake_receiver_offers_the_published_receiver_methods_the_client_may_use(self):
        rx = self._classes("_receiver.py")["Receiver"]
        published = {f.name for f in rx.body if isinstance(f, ast.FunctionDef) and not f.name.startswith("_")}
        used = {"poll_track", "poll_object", "wait", "subscribe", "track_state", "stats", "drained", "close"}
        self.assertTrue(used <= published, used - published)
        self.assertTrue(all(callable(getattr(FakeReceiver, m)) for m in used))


# ------------------------------------------------------------------ 1. go only on ACTIVE
class GoOnActive(unittest.TestCase):
    def setUp(self):
        self.e = Expected()
        self.addCleanup(self.e.dir.cleanup)

    def test_go_follows_the_first_ACTIVE_read_never_discovery_or_the_subscribe_call(self):
        s = Scenario(self.e.list)
        s.activate_after = 4
        receipt, rx, sent, closed, _ = run(s)
        self.assertEqual(receipt["controls"], ["go", "ack-all", "ended-observed", "done"], receipt)
        self.assertIn(s.video, rx.subscribed, "the selected track is subscribed on discovery")
        first_active = next(i for i, (_n, st) in enumerate(rx.state_reads) if st == TrackState.ACTIVE)
        self.assertTrue(all(n == 0 for n, _st in rx.state_reads[:first_active]), "no control before ACTIVE")
        self.assertEqual(rx.state_reads[first_active][0], 0, "go is emitted after that read, not before")

    def test_a_track_that_never_becomes_ACTIVE_is_a_named_bounded_failure(self):
        s = Scenario(self.e.list)
        s.activate_after = None
        receipt, rx, sent, closed, clock = run(s, deadline_s=3.0)
        self.assertEqual(sent, [])
        self.assertTrue(receipt["failure"].startswith("go: "), receipt["failure"])
        self.assertIn("ACTIVE", receipt["failure"])
        self.assertLessEqual(clock.t, 3.0 + 0.25)
        self.assertEqual(closed, ["receiver", "endpoint"])

    def test_only_the_named_track_is_subscribed(self):
        s = Scenario(self.e.list)
        s.announce = [TrackEvent(TrackEventKind.ADDED, s.other, s.other.description)] + s.announce
        receipt, rx, *_ = run(s)
        self.assertEqual(rx.subscribed, [s.video])

    def test_the_av_diagnostic_audio_track_is_inventoried_and_never_subscribed(self):
        # The A/V diagnostic's catalog adds "audio" (announced first here), ends and removes it
        # beside video, and its objects may arrive: the host subscribes video alone, and its stdout
        # stays exactly the four controls.
        s = Scenario(self.e.list)
        audio = s.other
        self.assertEqual((audio.description.name, audio.description.media_type), (b"audio", MediaType.AUDIO))
        s.announce = [TrackEvent(TrackEventKind.ADDED, audio, audio.description)] + s.announce
        s.objects = [MediaObject(track=audio, payload=bytes([0x21, i]), presentation_time_us=i * 21_333) for i in range(5)] \
            + s.objects
        s.ended = [TrackEvent(TrackEventKind.ENDED, audio, audio.description)] + s.ended
        s.removed = s.removed + [TrackEvent(TrackEventKind.REMOVED, audio, audio.description)]
        receipt, rx, sent, closed, _ = run(s)
        self.assertEqual(rx.subscribed, [s.video])
        self.assertEqual(sent, ["go", "ack-all", "ended-observed", "done"], receipt)
        self.assertEqual((receipt["problems"], receipt["failure"], receipt["received"]), ([], None, 60))
        self.assertEqual(closed, ["receiver", "endpoint"])


# ------------------------------------------------------------------ 2. ack-all only on exact 60
class ExactMedia(unittest.TestCase):
    def setUp(self):
        self.e = Expected()
        self.addCleanup(self.e.dir.cleanup)

    def _refused(self, s, needle):
        receipt, rx, sent, closed, _ = run(s, deadline_s=4.0)
        self.assertNotIn("ack-all", sent, receipt)
        self.assertTrue(receipt["failure"] and receipt["failure"].startswith("ack-all: "), receipt["failure"])
        self.assertTrue(any(needle in p for p in receipt["problems"]), (needle, receipt["problems"]))
        self.assertEqual(closed, ["receiver", "endpoint"])
        return receipt

    def test_exact_sequence_passes(self):
        receipt, rx, sent, closed, _ = run(Scenario(self.e.list))
        self.assertEqual(sent, ["go", "ack-all", "ended-observed", "done"])
        self.assertEqual((receipt["problems"], receipt["failure"]), ([], None))
        self.assertEqual(closed, ["receiver", "endpoint"])

    def test_an_empty_payload_fails_by_name(self):
        s = Scenario(self.e.list)
        s.objects[7] = replace(s.objects[7], payload=b"")
        self._refused(s, "object 7: payload differs (0 bytes received")

    def test_a_one_byte_corruption_fails_by_name(self):
        s = Scenario(self.e.list)
        p = bytearray(s.objects[30].payload); p[5] ^= 0x01
        s.objects[30] = replace(s.objects[30], payload=bytes(p))
        self._refused(s, "object 30: payload differs")

    def test_a_reorder_fails_by_name(self):
        s = Scenario(self.e.list)
        s.objects[3], s.objects[4] = s.objects[4], s.objects[3]
        self._refused(s, "object 3: payload differs")

    def test_59_and_61_selected_objects_fail_by_count(self):
        s = Scenario(self.e.list)
        s.objects = s.objects[:59]
        self._refused(s, "count 59 != expected 60")
        s = Scenario(self.e.list)
        s.objects = s.objects + [s.objects[-1]]
        self._refused(s, "count 61 != expected 60")

    def test_keyframe_and_presentation_time_are_compared(self):
        s = Scenario(self.e.list)
        s.objects[24] = replace(s.objects[24], keyframe=False)
        self._refused(s, "object 24: keyframe False != True")
        s = Scenario(self.e.list)
        s.objects[5] = replace(s.objects[5], presentation_time_us=s.objects[5].presentation_time_us + 1)
        self._refused(s, "object 5: pts_us")

    def test_an_unrelated_other_track_object_does_not_count_or_invalidate(self):
        s = Scenario(self.e.list)
        s.objects.insert(10, MediaObject(track=s.other, payload=b"\x01\x02", presentation_time_us=5))
        receipt, _rx, sent, *_ = run(s)
        self.assertEqual(sent, ["go", "ack-all", "ended-observed", "done"], receipt)

    def test_an_other_track_object_in_place_of_a_selected_one_is_that_object_missing(self):
        s = Scenario(self.e.list)
        s.objects[10] = replace(s.objects[10], track=s.other)
        self._refused(s, "count 59 != expected 60")

    def test_status_only_objects_are_never_media(self):
        s = Scenario(self.e.list)
        s.objects.insert(20, MediaObject(track=s.video, status=ObjectStatus.END_OF_GROUP))
        s.objects.append(MediaObject(track=s.video, status=ObjectStatus.END_OF_TRACK))
        receipt, _rx, sent, *_ = run(s)
        self.assertEqual(sent, ["go", "ack-all", "ended-observed", "done"], receipt)

    def test_a_perturbed_expectation_file_refuses_correct_media(self):
        import media_oracle
        s = Scenario(self.e.list)
        blob = bytearray(self.e.path.read_bytes())
        first = struct.calcsize("<IBQ")
        blob[first + 4] ^= 0xFF                                  # a payload byte of object 0, in the file only
        self.e.path.write_bytes(bytes(blob))
        s.expected = media_oracle.load_objects(self.e.path)
        self._refused(s, "object 0: payload differs")


# ------------------------------------------------------------------ 3. ended and done only on real events
class TerminalEvents(unittest.TestCase):
    def setUp(self):
        self.e = Expected()
        self.addCleanup(self.e.dir.cleanup)

    def test_ENDED_for_another_track_or_a_quiet_queue_is_not_ended(self):
        for ended in ([], None):
            s = Scenario(self.e.list)
            s.ended = [TrackEvent(TrackEventKind.ENDED, s.other, s.other.description)] if ended is None else ended
            receipt, _rx, sent, closed, clock = run(s, deadline_s=5.0)
            self.assertEqual(sent, ["go", "ack-all"], receipt)
            self.assertTrue(receipt["failure"].startswith("ended-observed: "), receipt["failure"])
            self.assertIn("TRACK_ENDED", receipt["failure"])
            self.assertEqual(closed, ["receiver", "endpoint"])

    def test_done_needs_REMOVED_and_catalog_isComplete_together(self):
        s = Scenario(self.e.list)
        s.complete = False
        receipt, _rx, sent, *_ = run(s, deadline_s=5.0)
        self.assertEqual(sent, ["go", "ack-all", "ended-observed"], receipt)
        self.assertTrue(receipt["failure"].startswith("done: "), receipt["failure"])
        s = Scenario(self.e.list)
        s.removed = []
        receipt, _rx, sent, *_ = run(s, deadline_s=5.0)
        self.assertEqual(sent, ["go", "ack-all", "ended-observed"], receipt)
        self.assertIn("REMOVED", receipt["failure"])

    def test_ENDED_before_all_media_still_waits_for_ack_all_first(self):
        s = Scenario(self.e.list)

        def media_then_ended(rx):
            rx.objects.extend(s.objects)
            rx.events.extend(s.ended)
        steps = s.steps()
        steps[-3] = (lambda sent: "go" in sent, media_then_ended)
        steps[-2] = (lambda sent: False, lambda rx: None)
        s.steps = lambda: steps
        receipt, _rx, sent, *_ = run(s)
        self.assertEqual(sent, ["go", "ack-all", "ended-observed", "done"], receipt)


# ------------------------------------------------------------------ 4. order, once, ownership, bounded terminal
class OrderAndOwnership(unittest.TestCase):
    def setUp(self):
        self.e = Expected()
        self.addCleanup(self.e.dir.cleanup)

    def test_each_control_once_in_order_and_the_run_stops_at_done(self):
        s = Scenario(self.e.list)
        s.extra_steps = []
        receipt, rx, sent, closed, _ = run(s)
        self.assertEqual(sent, list(dict.fromkeys(sent)))
        self.assertEqual(sent, ["go", "ack-all", "ended-observed", "done"])
        self.assertEqual(rx.closed, 1)
        self.assertEqual(closed, ["receiver", "endpoint"])
        self.assertEqual(rx.sent_at_close, ["go", "ack-all", "ended-observed", "done"], "close follows done")

    def test_a_terminal_receiver_before_a_promised_control_is_named_and_bounded(self):
        s = Scenario(self.e.list)

        def fatal(rx):
            rx.terminal = ReceiverTerminal(closed=True, fatal=True, fatal_code=7)
        s.ended = []
        steps = s.steps()
        steps[-2] = (lambda sent: "ack-all" in sent, fatal)
        s.steps = lambda: steps
        receipt, _rx, sent, closed, clock = run(s, deadline_s=30.0)
        self.assertEqual(sent, ["go", "ack-all"])
        self.assertTrue(receipt["failure"].startswith("ended-observed: receiver terminal"), receipt["failure"])
        self.assertIn("fatal_code 7", receipt["failure"])
        self.assertLess(clock.t, 5.0, "terminal is not waited out to the deadline")
        self.assertEqual(closed, ["receiver", "endpoint"])

    def test_a_connect_failure_is_named_with_nothing_to_close(self):
        import host_receiver
        clock, sent = Clock(), []
        refused = ConnectionError("endpoint refused")

        def connect():
            raise refused
        attached = []
        receipt = host_receiver.open_and_run(FAKE_MOQ5, connect, lambda ep: attached.append(ep), self.e.list, sent.append,
                                             clock, 5.0)
        self.assertEqual((sent, attached), ([], []))
        self.assertTrue(receipt["failure"].startswith("connect: "), receipt["failure"])
        self.assertIs(receipt["errors"][0], refused, "the original cause is retained")
        self.assertEqual((receipt["close_attempts"], receipt["closed"], receipt["close_errors"]), ([], [], []),
                         "nothing was acquired, so no close is owed")

    def test_the_live_entry_point_is_not_released(self):
        import host_receiver
        self.assertEqual(host_receiver.main(["host_receiver.py"]), 2)


# ------------------------------------------------------------------ 5. partial ownership and cleanup failures
class Ownership(unittest.TestCase):
    def setUp(self):
        self.e = Expected()
        self.addCleanup(self.e.dir.cleanup)

    def test_an_attach_refusal_still_closes_the_endpoint_it_was_given(self):
        import host_receiver
        log, sent = [], []
        endpoint = FakeEndpoint(log)
        refused = RuntimeError("receiver attach refused: WRONG_STATE")

        def attach(ep):
            self.assertIs(ep, endpoint)
            raise refused
        receipt = host_receiver.open_and_run(FAKE_MOQ5, lambda: endpoint, attach, self.e.list, sent.append, Clock(), 5.0)
        self.assertEqual(sent, [])
        self.assertTrue(receipt["failure"].startswith("attach: "), receipt["failure"])
        self.assertIs(receipt["errors"][0], refused, "the attach cause is retained, not rewritten")
        self.assertEqual(endpoint.attempts, 1, "the acquired endpoint's close is attempted exactly once")
        self.assertEqual(log, ["endpoint"], "no receiver exists, so no receiver close")
        self.assertEqual((receipt["close_attempts"], receipt["closed"], receipt["close_errors"]), (["endpoint"], ["endpoint"], []))

    def test_an_attach_refusal_whose_endpoint_close_also_fails_keeps_both_causes(self):
        import host_receiver
        log = []
        close_failed = OSError("endpoint close refused")
        endpoint = FakeEndpoint(log, close_failed)
        refused = RuntimeError("receiver attach refused")

        def attach(ep):
            raise refused
        receipt = host_receiver.open_and_run(FAKE_MOQ5, lambda: endpoint, attach, self.e.list, [].append, Clock(), 5.0)
        self.assertTrue(receipt["failure"].startswith("attach: "), "the attach failure stays primary: " + receipt["failure"])
        self.assertIs(receipt["errors"][0], refused)
        self.assertEqual(endpoint.attempts, 1)
        self.assertEqual(receipt["close_attempts"], ["endpoint"])
        self.assertEqual(receipt["closed"], [], "a close that raised is an attempt, not a close")
        self.assertEqual([(n, e) for n, e in receipt["close_errors"]], [("endpoint", close_failed)])

    def test_a_successful_run_whose_receiver_close_raises_still_attempts_the_endpoint_close(self):
        close_failed = OSError("receiver close refused")
        receipt, rx, sent, attempts, _ = run(Scenario(self.e.list), receiver_close_failure=close_failed)
        self.assertEqual(sent, ["go", "ack-all", "ended-observed", "done"])
        self.assertEqual(attempts, ["receiver", "endpoint"], "the endpoint close is attempted after the receiver's raised")
        self.assertEqual(receipt["closed"], ["endpoint"])
        self.assertEqual([(n, e) for n, e in receipt["close_errors"]], [("receiver", close_failed)])
        self.assertTrue(receipt["failure"].startswith("close: receiver"), "a clean run's close error is its failure")

    def test_a_named_run_failure_stays_primary_when_both_closes_raise(self):
        s = Scenario(self.e.list)
        s.activate_after = None
        rc, ec = OSError("receiver close refused"), OSError("endpoint close refused")
        receipt, rx, sent, attempts, _ = run(s, deadline_s=2.0, receiver_close_failure=rc, endpoint_close_failure=ec)
        self.assertTrue(receipt["failure"].startswith("go: "), receipt["failure"])
        self.assertEqual(attempts, ["receiver", "endpoint"])
        self.assertEqual(receipt["closed"], [])
        self.assertEqual([(n, e) for n, e in receipt["close_errors"]], [("receiver", rc), ("endpoint", ec)])

    def test_a_raising_run_and_a_raising_close_are_both_inspectable(self):
        broken = RuntimeError("poll_track raised")
        rc = OSError("receiver close refused")

        def explode(rx):
            def poll_track():
                raise broken
            rx.poll_track = poll_track
        with self.assertRaises(BaseExceptionGroup) as caught:
            run(Scenario(self.e.list), initial=(explode,), receiver_close_failure=rc)
        self.assertEqual(list(caught.exception.exceptions), [broken, rc], "the run's cause first, then the close's")

    def test_a_raising_run_with_clean_closes_re_raises_the_same_exception(self):
        broken = RuntimeError("poll_track raised")
        attempts = []

        def explode(rx):
            def poll_track():
                raise broken
            rx.poll_track = poll_track
            attempts.append(rx)
        with self.assertRaises(RuntimeError) as caught:
            run(Scenario(self.e.list), initial=(explode,))
        self.assertIs(caught.exception, broken)
        self.assertEqual(attempts[0].log, ["receiver", "endpoint"])


# ------------------------------------------------------------------ 6. interrupts are propagated, and still owe every close
class InterruptOwnership(unittest.TestCase):
    """KeyboardInterrupt and SystemExit are not receipts: the same objects
    propagate, and every close already owed is still attempted exactly once."""

    def setUp(self):
        self.e = Expected()
        self.addCleanup(self.e.dir.cleanup)

    @staticmethod
    def exploding_run(broken):
        def explode(rx):
            def poll_track():
                raise broken
            rx.poll_track = poll_track
        return explode

    def test_an_interrupted_attach_propagates_and_still_closes_the_endpoint(self):
        import host_receiver
        log = []
        endpoint = FakeEndpoint(log)
        interrupt = KeyboardInterrupt("attach interrupted")

        def attach(ep):
            raise interrupt
        with self.assertRaises(KeyboardInterrupt) as caught:
            host_receiver.open_and_run(FAKE_MOQ5, lambda: endpoint, attach, self.e.list, [].append, Clock(), 5.0)
        self.assertIs(caught.exception, interrupt, "the same interrupt, not a receipt")
        self.assertEqual(endpoint.attempts, 1)
        self.assertEqual(log, ["endpoint"], "no receiver exists, so no receiver close")

    def test_an_interrupted_receiver_close_after_a_run_failure_still_closes_the_endpoint(self):
        broken = RuntimeError("poll_track raised")
        interrupt = KeyboardInterrupt("receiver close interrupted")
        attempts = []
        with self.assertRaises(BaseExceptionGroup) as caught:
            run(Scenario(self.e.list), initial=(self.exploding_run(broken), attempts.append), receiver_close_failure=interrupt)
        self.assertEqual(list(caught.exception.exceptions), [broken, interrupt], "both causes, in order, by identity")
        self.assertEqual(attempts[0].log, ["receiver", "endpoint"], "the endpoint close is still attempted, once")

    def test_both_close_slots_interrupted_after_a_run_failure_keep_every_cause(self):
        broken = RuntimeError("poll_track raised")
        interrupt, exiting = KeyboardInterrupt("receiver close interrupted"), SystemExit(3)
        attempts = []
        with self.assertRaises(BaseExceptionGroup) as caught:
            run(Scenario(self.e.list), initial=(self.exploding_run(broken), attempts.append),
                receiver_close_failure=interrupt, endpoint_close_failure=exiting)
        self.assertEqual(list(caught.exception.exceptions), [broken, interrupt, exiting])
        self.assertEqual(attempts[0].log, ["receiver", "endpoint"])

    def test_an_interrupted_receiver_close_after_a_clean_run_propagates_after_the_endpoint_close(self):
        interrupt = KeyboardInterrupt("receiver close interrupted")
        attempts = []
        with self.assertRaises(KeyboardInterrupt) as caught:
            run(Scenario(self.e.list), initial=(attempts.append,), receiver_close_failure=interrupt)
        self.assertIs(caught.exception, interrupt, "not converted into a receipt")
        self.assertEqual(attempts[0].log, ["receiver", "endpoint"])


# ------------------------------------------------------------------ 7. the two combinations the policy must not merge
class FinalDiscriminators(unittest.TestCase):
    def setUp(self):
        self.e = Expected()
        self.addCleanup(self.e.dir.cleanup)

    def test_an_ordinary_attach_refusal_whose_endpoint_close_is_interrupted_is_not_a_receipt(self):
        import host_receiver
        log = []
        interrupt = KeyboardInterrupt("endpoint close interrupted")
        endpoint = FakeEndpoint(log, interrupt)
        refused = RuntimeError("receiver attach refused")

        def attach(ep):
            raise refused
        with self.assertRaises(BaseExceptionGroup) as caught:
            host_receiver.open_and_run(FAKE_MOQ5, lambda: endpoint, attach, self.e.list, [].append, Clock(), 5.0)
        self.assertEqual(list(caught.exception.exceptions), [refused, interrupt], "both causes, by identity, in order")
        self.assertEqual(endpoint.attempts, 1)
        self.assertEqual(log, ["endpoint"], "no receiver exists, so no receiver close")

    def test_a_fatal_but_not_closed_receiver_is_a_prompt_named_failure(self):
        s = Scenario(self.e.list)

        def fatal_not_closed(rx):
            # Reachable: fatal reads the receiver's own latch, closed reads the endpoint; a latched
            # interrupt makes every object poll INTERRUPTED, so drained() never becomes true.
            rx.terminal = ReceiverTerminal(closed=False, fatal=True, fatal_code=9)
            rx.object_outcome = PollOutcome.INTERRUPTED
        steps = s.steps()
        steps[-3] = (lambda sent: "go" in sent, fatal_not_closed)
        s.steps = lambda: steps
        receipt, rx, sent, closed, clock = run(s, deadline_s=600.0)
        self.assertEqual(sent, ["go"])
        self.assertTrue(receipt["failure"].startswith("ack-all: receiver terminal"), receipt["failure"])
        self.assertIn("fatal True", receipt["failure"])
        self.assertIn("fatal_code 9", receipt["failure"])
        self.assertLess(clock.t, 2.0, "prompt: neither the 600 s deadline nor a spin")
        self.assertLess(rx.calls.count("wait"), 8)
        self.assertEqual(closed, ["receiver", "endpoint"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
