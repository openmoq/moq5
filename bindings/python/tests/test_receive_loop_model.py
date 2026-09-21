"""Bounded MODEL test of the exact receive_loop.py example.

This is not a native receiver test: Receiver here is a deque model of the
DOCUMENTED outcome semantics (media_receiver.c wait/poll branches), so the
loop's control flow can be killed deterministically. Every successful poll is
counted; a version that uses polling as a peek loses an item and is caught
by the injected-item row without any timing.
"""

import ast
import importlib.util
import unittest
from collections import deque
from enum import IntEnum
from pathlib import Path
from types import SimpleNamespace

import os
# The exact shipped example by default; a mutant copy may be pointed at through
# this test-only variable so the model rows can be shown to KILL wrong loops.
EXAMPLE = Path(os.environ.get("MOQ5_RECEIVE_LOOP_EXAMPLE",
                              str(Path(__file__).resolve().parent.parent / "examples" / "receive_loop.py")))


class PollOutcome(IntEnum):
    EMPTY = 1
    INTERRUPTED = -13
    CLOSED = -4


class WaitResult(IntEnum):
    WOKEN = 0
    TIMED_OUT = 1
    INTERRUPTED = -13
    CLOSED = -4


class TrackEventKind(IntEnum):
    ADDED = 1
    UPDATED = 2
    REMOVED = 3
    ENDED = 4
    CATALOG_READY = 5


class Packaging(IntEnum):
    RAW = 1
    CMAF = 2


class MoqError(RuntimeError):
    def __init__(self, code, operation):
        super().__init__(code, operation)
        self.code, self.operation = code, operation


class Track:
    def __init__(self, name, packaging_text=b"cmaf"):
        self.description = SimpleNamespace(packaging_text=packaging_text, vod=None, name=name)

    def __repr__(self):
        return f"Track({self.description.name!r})"


def ev(kind, track=None, vod=None):
    desc = None
    if track is not None:
        desc = SimpleNamespace(packaging_text=track.description.packaging_text, vod=vod, name=track.description.name)
    return SimpleNamespace(kind=kind, track=track, description=desc)


def obj(track, packaging=Packaging.CMAF, status=0, payload=b"", fragment=b"\x00\x00\x00\x08mdat", mdat_offset=8, mdat_len=0, samples=()):
    return SimpleNamespace(track=track, packaging=packaging, status=status, is_status_only=status != 0,
                           payload=payload, fragment=fragment, mdat_offset=mdat_offset, mdat_len=mdat_len, samples=samples)


class ModelReceiver:
    """Documented outcome semantics: poll_track CLOSED only when ITS queue is empty
    and terminal; poll_object INTERRUPTED first when latched; wait: queued work ->
    WOKEN, else terminal -> CLOSED, else the scripted native result, then the
    post-delegation recheck (queued -> WOKEN, terminal -> CLOSED)."""

    WAIT_BUDGET = 64   # a loop that never settles fails by name, never by hanging

    def __init__(self, events=(), objects=(), terminal=False, latched=False, native_waits=(), catalog_complete=False,
                 subscribe_code=None, on_wait=None):
        self.events, self.objects = deque(events), deque(objects)
        self.terminal, self.latched, self.catalog_complete = terminal, latched, catalog_complete
        self.native_waits = deque(native_waits)
        self.subscribe_code = subscribe_code
        self.on_wait = on_wait
        self.subscribed, self.polled_objects, self.polled_events = [], [], []
        self.waits = 0
        self.last = {"track": None, "object": None}

    def poll_track(self):
        if self.events:
            e = self.events.popleft(); self.polled_events.append(e); self.last["track"] = "item"; return e
        r = PollOutcome.CLOSED if self.terminal else PollOutcome.EMPTY
        self.last["track"] = r; return r

    def poll_object(self):
        if self.latched:
            self.last["object"] = PollOutcome.INTERRUPTED; return PollOutcome.INTERRUPTED
        if self.objects:
            o = self.objects.popleft(); self.polled_objects.append(o); self.last["object"] = "item"; return o
        r = PollOutcome.CLOSED if self.terminal else PollOutcome.EMPTY
        self.last["object"] = r; return r

    def drained(self):
        return self.last["track"] is PollOutcome.CLOSED and self.last["object"] is PollOutcome.CLOSED

    def subscribe(self, track):
        if self.subscribe_code is not None:
            raise MoqError(self.subscribe_code, "subscribe")
        self.subscribed.append(track)

    def stats(self):
        return SimpleNamespace(catalog_complete=self.catalog_complete, objects_queued=len(self.objects))

    def wait(self, timeout_us):
        self.waits += 1
        if self.waits > self.WAIT_BUDGET:
            raise AssertionError("model wait budget exhausted: the loop never settled")
        if self.events or self.objects:
            return WaitResult.WOKEN
        if self.terminal:
            return WaitResult.CLOSED
        if self.on_wait:
            self.on_wait(self)                      # an item may arrive DURING the native wait
        native = self.native_waits.popleft() if self.native_waits else WaitResult.TIMED_OUT
        if native in (WaitResult.INTERRUPTED, WaitResult.TIMED_OUT):
            return native                           # returned AS IS (line 2969): no recheck
        if self.events or self.objects:
            return WaitResult.WOKEN
        return WaitResult.CLOSED if self.terminal else native


def load_example():
    source = EXAMPLE.read_text()
    compile(source, str(EXAMPLE), "exec")              # the exact file must compile
    ast.parse(source)
    spec = importlib.util.spec_from_file_location("receive_loop", EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.receive


class ReceiveLoopModelTests(unittest.TestCase):
    def setUp(self):
        self.receive = load_example()
        self.decoded, self.statuses = [], []

    def run_loop(self, rx):
        return self.receive(rx, PollOutcome, TrackEventKind, WaitResult, Packaging, MoqError,
                            decode_raw=lambda d, p: self.decoded.append(("raw", p)),
                            decode_cmaf=lambda d, f, o, n, s: self.decoded.append(("cmaf", f)),
                            note_status=lambda t, s: self.statuses.append(s))

    def test_a_events_and_objects_then_terminal_drain_everything(self):
        v = Track(b"video")
        rx = ModelReceiver(events=[ev(TrackEventKind.ADDED, v), ev(TrackEventKind.CATALOG_READY)],
                           objects=[obj(v), obj(v), obj(v)], terminal=True)
        r = self.run_loop(rx)
        self.assertEqual((r["objects"], r["events"], r["drained"], r["completed"]), (3, 2, True, False))
        self.assertEqual(rx.subscribed, [v])

    def test_b_subscribe_closed_keeps_draining_but_inval_propagates(self):
        v = Track(b"video")
        rx = ModelReceiver(events=[ev(TrackEventKind.ADDED, v)], objects=[obj(v), obj(v)], terminal=True, subscribe_code=-4)
        r = self.run_loop(rx)
        self.assertEqual((r["objects"], r["drained"]), (2, True))
        rx2 = ModelReceiver(events=[ev(TrackEventKind.ADDED, v)], objects=[obj(v)], subscribe_code=-2)
        with self.assertRaises(MoqError) as caught:
            self.run_loop(rx2)
        self.assertEqual(caught.exception.code, -2)

    def test_c_vod_conversion_completes_without_connection_closure(self):
        v = Track(b"video")
        vod = SimpleNamespace(is_live=False, track_duration_ms=60000)
        rx = ModelReceiver(events=[ev(TrackEventKind.ADDED, v)],
                           objects=[obj(v), obj(v), obj(v, status=2)],   # 3rd: END_OF_TRACK status-only
                           native_waits=[WaitResult.WOKEN])
        # After the first drain, the facade delivers the terminal ENDED and the VOD UPDATED.
        def later(m):
            m.events.extend([ev(TrackEventKind.ENDED, v), ev(TrackEventKind.UPDATED, v, vod=vod)])
        rx.on_wait = later
        r = self.run_loop(rx)
        self.assertTrue(r["completed"]); self.assertFalse(r["drained"]); self.assertFalse(rx.terminal)
        self.assertEqual(r["objects"], 3)
        self.assertEqual(self.statuses, [2], "the END_OF_TRACK status-only object never reached a decoder")
        self.assertEqual(len(self.decoded), 2)

    def test_d_permanent_termination_completes(self):
        v = Track(b"video")
        rx = ModelReceiver(events=[ev(TrackEventKind.ADDED, v)], objects=[obj(v)], native_waits=[WaitResult.WOKEN])
        def later(m):
            m.events.extend([ev(TrackEventKind.ENDED, v), ev(TrackEventKind.REMOVED, v)]); m.catalog_complete = True
        rx.on_wait = later
        r = self.run_loop(rx)
        self.assertTrue(r["completed"]); self.assertEqual(r["objects"], 1)

    def test_e_completion_never_discards_the_other_queue(self):
        v = Track(b"video")
        vod = SimpleNamespace(is_live=False, track_duration_ms=1)
        rx = ModelReceiver(events=[ev(TrackEventKind.ADDED, v), ev(TrackEventKind.ENDED, v), ev(TrackEventKind.UPDATED, v, vod=vod)],
                           objects=[obj(v), obj(v), obj(v)])
        r = self.run_loop(rx)
        self.assertTrue(r["completed"]); self.assertEqual(r["objects"], 3, "objects queued behind the completion signals were delivered")

    def test_f_item_injected_after_drain_before_completion_check_is_handled(self):
        # Kills a version that polls as a peek: the completion check would consume this object.
        v = Track(b"video")
        vod = SimpleNamespace(is_live=False, track_duration_ms=1)
        rx = ModelReceiver(events=[ev(TrackEventKind.ADDED, v), ev(TrackEventKind.ENDED, v), ev(TrackEventKind.UPDATED, v, vod=vod)],
                           objects=[obj(v)])
        original = rx.poll_object
        state = {"empties": 0, "injected": False}
        def poll_object():
            # The FIRST empty poll is the drain's own stop; the SECOND is the
            # completion check. An item that lands exactly there must be
            # handled by a re-drain -- a destructive peek would consume it.
            if not rx.objects and not state["injected"]:
                state["empties"] += 1
                if state["empties"] == 2:
                    state["injected"] = True
                    rx.objects.append(obj(v, packaging=Packaging.RAW, payload=b"late"))
            return original()
        rx.poll_object = poll_object
        r = self.run_loop(rx)
        self.assertTrue(r["completed"])
        self.assertEqual(r["objects"], 2)
        self.assertIn(("raw", b"late"), self.decoded, "the injected item went through the handler, not a discarding peek")

    def test_k_late_added_track_after_first_completion_is_re_evaluated(self):
        # Kills a version that tests the criterion BEFORE the second drain only:
        # a new ADDED track injected on the second empty track poll (the
        # post-completion re-drain) is subscribed by that drain and has no ENDED,
        # so the loop must NOT report completed.
        v, a = Track(b"video"), Track(b"audio", packaging_text=b"loc")
        vod = SimpleNamespace(is_live=False, track_duration_ms=1)
        rx = ModelReceiver(events=[ev(TrackEventKind.ADDED, v), ev(TrackEventKind.ENDED, v), ev(TrackEventKind.UPDATED, v, vod=vod)],
                           objects=[obj(v)], native_waits=[WaitResult.INTERRUPTED])
        original = rx.poll_track
        state = {"empties": 0, "injected": False}
        def poll_track():
            if not rx.events and not state["injected"]:
                state["empties"] += 1
                if state["empties"] == 2:
                    state["injected"] = True
                    rx.events.append(ev(TrackEventKind.ADDED, a))
            return original()
        rx.poll_track = poll_track
        r = self.run_loop(rx)
        self.assertTrue(state["injected"], "the late track was injected on the post-completion re-drain")
        self.assertEqual(rx.subscribed, [v, a], "the re-drain subscribed the late track through the handler")
        self.assertFalse(r["completed"], "a subscribed track with no ENDED is not completion")
        self.assertTrue(r["interrupted"])
        self.assertEqual(r["objects"], 1)

    def test_g_negative_controls_are_not_completion(self):
        v = Track(b"video")
        # still-live snapshot (vod present but is_live True) + ENDED: NOT completion
        live = SimpleNamespace(is_live=True, track_duration_ms=None)
        rx = ModelReceiver(events=[ev(TrackEventKind.ADDED, v), ev(TrackEventKind.ENDED, v), ev(TrackEventKind.UPDATED, v, vod=live)],
                           objects=[obj(v)], native_waits=[WaitResult.INTERRUPTED])
        r = self.run_loop(rx)
        self.assertFalse(r["completed"]); self.assertTrue(r["interrupted"])
        # ordinary removal without isComplete + ENDED: NOT completion
        rx2 = ModelReceiver(events=[ev(TrackEventKind.ADDED, v), ev(TrackEventKind.ENDED, v), ev(TrackEventKind.REMOVED, v)],
                            objects=[obj(v)], native_waits=[WaitResult.INTERRUPTED])
        r2 = self.run_loop(rx2)
        self.assertFalse(r2["completed"])
        # ENDED alone (rejection shape): NOT completion
        rx3 = ModelReceiver(events=[ev(TrackEventKind.ADDED, v), ev(TrackEventKind.ENDED, v)], native_waits=[WaitResult.INTERRUPTED])
        r3 = self.run_loop(rx3)
        self.assertFalse(r3["completed"])

    def test_l_non_live_without_duration_is_not_the_conversion_signal(self):
        # MSF-01 section 11.3: the VOD conversion sets isLive false AND adds a
        # track duration. A non-live declaration without a duration is not the
        # signal (the facade classifies that survivor change as a violation and
        # never emits it, so a loop that accepts it is wrong twice).
        v = Track(b"video")
        half = SimpleNamespace(is_live=False, track_duration_ms=None)
        rx = ModelReceiver(events=[ev(TrackEventKind.ADDED, v), ev(TrackEventKind.ENDED, v), ev(TrackEventKind.UPDATED, v, vod=half)],
                           objects=[obj(v)], native_waits=[WaitResult.INTERRUPTED])
        r = self.run_loop(rx)
        self.assertFalse(r["completed"], "isLive false without trackDuration is not the section 11.3 conversion")
        self.assertTrue(r["interrupted"])
        self.assertEqual(r["objects"], 1)

    def test_m_duration_zero_is_a_present_duration(self):
        v = Track(b"video")
        zero = SimpleNamespace(is_live=False, track_duration_ms=0)
        rx = ModelReceiver(events=[ev(TrackEventKind.ADDED, v), ev(TrackEventKind.ENDED, v), ev(TrackEventKind.UPDATED, v, vod=zero)],
                           objects=[obj(v)])
        r = self.run_loop(rx)
        self.assertTrue(r["completed"], "presence is `is not None`, never truthiness")

    def test_h_interrupt_with_queued_objects_is_a_caller_decision(self):
        v = Track(b"video")
        rx = ModelReceiver(events=[ev(TrackEventKind.ADDED, v)], objects=[obj(v)], latched=True)
        r = self.run_loop(rx)
        self.assertTrue(r["interrupted"]); self.assertFalse(r["completed"]); self.assertFalse(r["drained"])
        self.assertEqual(rx.stats().objects_queued, 1, "objects remain queued")

    def test_i_zero_length_normal_raw_is_media(self):
        v = Track(b"audio", packaging_text=b"loc")
        rx = ModelReceiver(events=[ev(TrackEventKind.ADDED, v)], objects=[obj(v, packaging=Packaging.RAW, payload=b"")], terminal=True)
        self.run_loop(rx)
        self.assertEqual(self.decoded, [("raw", b"")]); self.assertEqual(self.statuses, [])

    def test_j_arrival_during_wait_is_pinned_to_the_native_return(self):
        v = Track(b"video")
        # an item arrives DURING the native wait, native returns DONE -> TIMED_OUT as is (no recheck)
        rx = ModelReceiver(native_waits=[WaitResult.TIMED_OUT], on_wait=lambda m: m.objects.append(obj(v)))
        self.assertEqual(rx.wait(1), WaitResult.TIMED_OUT)
        # the same arrival with a native OK wake takes the post-delegation queue path -> WOKEN
        rx2 = ModelReceiver(native_waits=[WaitResult.WOKEN], on_wait=lambda m: m.objects.append(obj(v)))
        self.assertEqual(rx2.wait(1), WaitResult.WOKEN)
        # and the caller loop still delivers it on the next drain either way
        rx3 = ModelReceiver(events=[ev(TrackEventKind.ADDED, v)], native_waits=[WaitResult.TIMED_OUT, WaitResult.INTERRUPTED],
                            on_wait=lambda m: m.objects.append(obj(v)) if not m.polled_objects else None)
        r = self.run_loop(rx3)
        self.assertEqual(r["objects"], 1)


if __name__ == "__main__":
    unittest.main()
