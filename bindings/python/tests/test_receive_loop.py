"""Deterministic integration of the shipped receive loop with the ACTUAL
public Receiver over the fixture C provider.

The loop is `examples/receive_loop.py`, loaded exactly as the model test
loads it (one implementation, never copied). Work and state are injected only
through declared provider boundaries (`_test_at_boundary`): the Nth empty
read of the track or object queue inside the real poll, or the Nth receiver
wait call. Nothing here is a parallel Receiver; the model rows in
test_receive_loop_model.py stay the model.

`Receiver.drained()` is true only after BOTH exposed polling streams (track
events, media objects) returned CLOSED. The SAP and media-timeline native
queues are NOT exposed by this Python slice, so drained() is a statement
about the two exposed streams, never a claim that all four native queues
were polled.

Media values remain carrier evidence (not parsed CMAF, not real media).
"""

import gc
import os
import threading
import unittest
from unittest import mock

import moq5
from moq5 import _native, _receiver

import test_receive_loop_model as model

NS = (b"live",)
K = moq5.TrackEventKind
C = _native.constants
BUDGET = "fixture wait budget exhausted: the loop never settled"

DESC = {"name": b"video", "packaging_text": b"cmaf", "media_type": 1, "packaging": 2,
        "timescale": 90000, "transport_version": 16, "is_live": True}
AUDIO = {**DESC, "name": b"audio", "packaging_text": b"loc", "media_type": 2, "packaging": 1}
FRAGMENT = bytes(range(64))
SAMPLES = ((3000, 17, 0x02000000, -1500), (3000, 21, 0, 0), (1, 2**32 - 1, 0x00010000, 2**31 - 1))
CMAF = {"packaging": 2, "status": 0, "end_of_group": False, "datagram": False, "keyframe": True,
        "capture_time_us": 2**40 + 7, "decode_time_us": 2**63 + 5, "composition_offset_us": -(2**40),
        "presentation_time_us": 2**63 - 2**40 + 5, "fragment": FRAGMENT, "mdat_offset": 8, "mdat_len": 40,
        "samples": SAMPLES, "config_generation": 0}
RAW = {"packaging": 1, "status": 0, "end_of_group": True, "datagram": True, "keyframe": False,
       "capture_time_us": None, "decode_time_us": 0, "composition_offset_us": 0, "presentation_time_us": 0,
       "payload": b"raw\x00bytes", "config_generation": 0}
END_OF_TRACK = {**RAW, "status": 2, "payload": None}


class LoopSubstrateControls(unittest.TestCase):
    """Substrate controls: the declared-boundary hooks, the wait budget and the
    loop counters, independent of the query implementation."""

    def setUp(self):
        gc.collect()
        _native._test_reset()
        self.endpoint = moq5.Endpoint.connect(moq5.EndpointConfig(url="moqt://fixture.invalid"))
        self.receiver = moq5.Receiver.attach(self.endpoint, moq5.ReceiverConfig.live(NS))
        self.handle = self.receiver._Receiver__handle

    def tearDown(self):
        if not self.receiver.closed:
            self.receiver.close()
        self.endpoint.close()
        _native._test_object_settle()

    def track(self, desc=DESC):
        capsule = _native._test_new_track(self.handle)
        _native._test_track_desc(capsule, dict(desc))
        return capsule

    def test_object_hook_fires_once_at_the_declared_empty_read(self):
        a = self.track()
        _native._test_at_boundary("object_empty", 2, lambda: _native._test_push_object(a, {**RAW, "payload": b"late"}))
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.EMPTY, "first empty read: not the boundary")
        self.assertEqual(self.receiver.poll_object().payload, b"late", "second empty read: the hook queued it inside the poll")
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.EMPTY)
        counts = _native._test_loop_counts()
        self.assertEqual((counts["object_polls"], counts["object_empties"], counts["hooks_fired"]), (3, 3, 1))
        self.assertEqual(_native._test_boundary_report(), 1)

    def test_track_hook_fires_once_and_state_changes_apply_to_the_same_poll(self):
        _native._test_at_boundary("track_empty", 1, lambda: _native._test_receiver_state(True, False, 0))
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.CLOSED, "terminal set at the boundary is seen by that poll")
        self.assertEqual(_native._test_loop_counts()["hooks_fired"], 1)
        _native._test_receiver_state(False, False, 0)
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY)
        self.assertEqual(_native._test_boundary_report(), 1)

    def test_wait_hook_runs_before_the_priority_check_and_the_latch_is_honoured(self):
        a = self.track()
        _native._test_at_boundary("wait", 1, lambda: _native._test_push_object(a, dict(RAW)))
        _native._test_at_boundary("wait", 2, lambda: self.endpoint.set_interrupted(True))
        self.assertIs(self.receiver.wait(0), moq5.WaitResult.WOKEN, "queued at the boundary -> WOKEN")
        self.assertEqual(self.receiver.poll_object().payload, b"raw\x00bytes")
        self.assertIs(self.receiver.wait(0), moq5.WaitResult.INTERRUPTED, "latched at the boundary -> INTERRUPTED")
        self.assertEqual(_native._test_boundary_report(), 2)
        self.endpoint.set_interrupted(False)

    def test_wait_budget_fails_by_name_with_the_native_code(self):
        # The budget counts NATIVE wait calls; a zero timeout is exactly one
        # observation per Receiver.wait (the slicer re-observes until a
        # positive deadline), which is what the loop rows use.
        _native._test_wait_budget(2)
        self.assertIs(self.receiver.wait(0), moq5.WaitResult.TIMED_OUT)
        self.assertIs(self.receiver.wait(0), moq5.WaitResult.TIMED_OUT)
        with self.assertRaises(moq5.MoqError) as caught:
            self.receiver.wait(0)
        self.assertEqual((caught.exception.code, caught.exception.operation), (C["ERR_INTERNAL"], "wait"))
        self.assertEqual(_native._test_loop_counts()["waits"], 3)

    def test_hook_exception_is_kept_and_reported_not_swallowed(self):
        def boom():
            raise ValueError("boundary hook failed")
        _native._test_at_boundary("object_empty", 1, boom)
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.EMPTY, "the poll itself completes")
        self.assertEqual(_native._test_loop_counts()["hook_failures"], 1)
        with self.assertRaisesRegex(ValueError, "boundary hook failed"):
            _native._test_boundary_report()

    def test_unfired_hook_is_reported_by_name(self):
        _native._test_at_boundary("track_empty", 5, lambda: None)
        with self.assertRaisesRegex(AssertionError, "1 of 1 armed boundary hooks never fired"):
            _native._test_boundary_report()


class ReceiveLoopIntegrationTests(unittest.TestCase):
    """Every row runs the shipped loop, or the query itself, against the real
    Receiver over the fixture provider."""

    def setUp(self):
        gc.collect()
        _native._test_reset()
        self.receive = model.load_example()
        _native._test_wait_budget(16)
        self.decoded, self.statuses = [], []
        self.endpoint = moq5.Endpoint.connect(moq5.EndpointConfig(url="moqt://fixture.invalid"))
        self.receiver = moq5.Receiver.attach(self.endpoint, moq5.ReceiverConfig.live(NS))
        self.handle = self.receiver._Receiver__handle

    def tearDown(self):
        if not self.receiver.closed:
            self.receiver.close()
        self.endpoint.close()
        _native._test_object_settle()
        # every armed hook fired, and none raised (a hook exception is re-raised here)
        _native._test_boundary_report()

    # ---- helpers ------------------------------------------------------------------

    def track(self, desc=DESC):
        capsule = _native._test_new_track(self.handle)
        _native._test_track_desc(capsule, dict(desc))
        return capsule

    def push(self, capsule, fields):
        _native._test_push_object(capsule, dict(fields))

    def event(self, kind, capsule):
        _native._test_push_event(int(kind), capsule, None, None, None, None)

    def at(self, boundary, nth, *actions):
        def hook():
            for action in actions:
                action()
        _native._test_at_boundary(boundary, nth, hook)

    def interrupt(self):
        return lambda: self.endpoint.set_interrupted(True)

    def run_loop(self, rx=None):
        # wait_us=0: one native observation per loop wait, so the wait
        # boundary and the budget count loop iterations exactly
        rx = self.receiver if rx is None else rx
        try:
            return self.receive(rx, moq5.PollOutcome, K, moq5.WaitResult, moq5.Packaging, moq5.MoqError,
                                decode_raw=lambda d, p: self.decoded.append(("raw", d.name, p)),
                                decode_cmaf=lambda d, f, o, n, s: self.decoded.append(("cmaf", d.name, f, o, n, s)),
                                note_status=lambda t, s: self.statuses.append((t.description.name, s)),
                                wait_us=0)
        except moq5.MoqError as failed:
            if failed.operation == "wait" and failed.code == C["ERR_INTERNAL"]:
                raise AssertionError(BUDGET) from None
            raise

    def counts(self):
        return _native._test_object_counts()

    def loop_counts(self):
        return _native._test_loop_counts()

    def cached(self):
        return self.receiver._Receiver__tracks

    # ---- 1. both streams, terminal ---------------------------------------------------

    def test_both_streams_then_terminal_are_delivered_once_closed_on_both_and_drained(self):
        a = self.track()
        self.event(K.ADDED, a)
        self.event(K.CATALOG_READY, None)
        for fields in (CMAF, {**RAW, "payload": b"two"}, {**RAW, "payload": b"three"}):
            self.push(a, fields)
        # terminal only after the events were handled (subscribe succeeds first)
        self.at("track_empty", 1, lambda: _native._test_receiver_state(True, False, 0))
        r = self.run_loop()
        self.assertEqual((r["events"], r["objects"], r["drained"], r["completed"], r["interrupted"]), (2, 3, True, False, False))
        self.assertEqual([d[0] for d in self.decoded], ["cmaf", "raw", "raw"], "within-stream order preserved")
        self.assertEqual([d[2] for d in self.decoded if d[0] == "raw"], [b"two", b"three"])
        self.assertEqual([s[0] for s in _native._test_subscriptions()], ["subscribe"])
        self.assertTrue(r["terminal"].closed)
        c = self.counts()
        self.assertEqual((c["dequeued"], c["cleanup_calls"], c["queued"]), (3, 3, 0))
        self.assertEqual(c["actual_kept_refcounts"], c["kept_buffers"])

    def test_one_stream_closed_while_the_other_holds_items_is_not_drained_and_drops_nothing(self):
        a = self.track()
        for payload in (b"one", b"two"):
            self.push(a, {**RAW, "payload": payload})
        _native._test_receiver_state(True, False, 0)
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.CLOSED)
        self.assertFalse(self.receiver.drained(), "track CLOSED alone is not drain")
        self.assertEqual(self.receiver.poll_object().payload, b"one")
        self.assertFalse(self.receiver.drained())
        self.assertEqual(self.receiver.poll_object().payload, b"two")
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.CLOSED)
        self.assertTrue(self.receiver.drained(), "both streams CLOSED")
        self.assertEqual(self.counts()["dequeued"], 2)

    # ---- 2. drained semantics and guards ------------------------------------------------

    def test_drained_is_false_for_live_empty_complete_alone_one_closed_and_interrupt(self):
        a = self.track()
        self.assertFalse(self.receiver.drained(), "initial")
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY)
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.EMPTY)
        self.assertFalse(self.receiver.drained(), "live EMPTY on both")
        _native._test_stats(0, 0, 0, 0, 0, 0, 0, 0, 0, False, 0, True)
        self.assertTrue(self.receiver.stats().catalog_complete)
        self.assertFalse(self.receiver.drained(), "catalog complete alone is not drain")
        self.push(a, dict(RAW))
        _native._test_receiver_state(True, False, 0)
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.CLOSED)
        self.assertFalse(self.receiver.drained(), "one CLOSED")
        _native._test_receiver_state(False, False, 0)
        self.endpoint.set_interrupted(True)
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.INTERRUPTED)
        self.assertFalse(self.receiver.drained(), "interrupt with queued media")
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY, "poll_track ignores the latch")
        before = self.loop_counts()
        dequeued = self.counts()["dequeued"]
        for _ in range(3):
            self.assertFalse(self.receiver.drained())
        self.assertEqual(self.loop_counts(), before, "the query polls nothing and waits nothing")
        self.assertEqual(self.counts()["dequeued"], dequeued, "the query consumes nothing")
        self.endpoint.set_interrupted(False)
        self.assertEqual(self.receiver.poll_object().payload, b"raw\x00bytes", "the object was preserved until cleared")

    def test_a_non_closed_result_clears_stale_closed_evidence(self):
        a = self.track()
        _native._test_receiver_state(True, False, 0)
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.CLOSED)
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.CLOSED)
        self.assertTrue(self.receiver.drained())
        # a later successful dequeue on either stream withdraws that stream's evidence
        self.push(a, dict(RAW))
        self.assertEqual(self.receiver.poll_object().payload, b"raw\x00bytes")
        self.assertFalse(self.receiver.drained(), "a consumed object is newer than the CLOSED observation")
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.CLOSED)
        self.assertTrue(self.receiver.drained())
        self.push(a, {**RAW, "malformed": "payload_null_len"})
        with self.assertRaises(moq5.ObjectLost):
            self.receiver.poll_object()
        self.assertFalse(self.receiver.drained(), "a consumed-item conversion failure is not CLOSED evidence")
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.CLOSED)
        self.assertTrue(self.receiver.drained())
        self.event(K.ADDED, a)
        self.assertIs(self.receiver.poll_track().kind, K.ADDED)
        self.assertFalse(self.receiver.drained())

    def test_withdrawal_from_true_drained_for_every_newer_non_closed_outcome(self):
        # WRAPPER STATE-MACHINE PROBE: the fixture scripts a terminal->live
        # transition (state seam, scripted results) that the production facade
        # cannot produce (fatal latches, a closed endpoint stays closed). It
        # checks the recorded-evidence rule, not a reachable production path.
        a = self.track()

        def make_true():
            _native._test_receiver_state(True, False, 0)
            self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.CLOSED)
            self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.CLOSED)
            self.assertTrue(self.receiver.drained())

        make_true()
        _native._test_receiver_state(False, False, 0)
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.EMPTY)
        self.assertFalse(self.receiver.drained(), "EMPTY on the object stream withdraws it")
        make_true()
        _native._test_receiver_state(False, False, 0)
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY)
        self.assertFalse(self.receiver.drained(), "EMPTY on the track stream withdraws it")
        make_true()
        self.endpoint.set_interrupted(True)
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.INTERRUPTED)
        self.assertFalse(self.receiver.drained(), "INTERRUPTED withdraws the object stream")
        self.endpoint.set_interrupted(False)
        make_true()
        _native._test_poll_track_result(-99)
        with self.assertRaises(moq5.MoqError) as caught:
            self.receiver.poll_track()
        _native._test_poll_track_result(0)
        self.assertEqual((caught.exception.code, caught.exception.operation), (-99, "poll_track"), "the native code is kept")
        self.assertFalse(self.receiver.drained(), "a native error withdraws the track stream")
        make_true()
        _native._test_poll_object_result(-99)
        with self.assertRaises(moq5.MoqError):
            self.receiver.poll_object()
        _native._test_poll_object_result(0)
        self.assertFalse(self.receiver.drained(), "a native error withdraws the object stream")
        make_true()
        _native._test_push_event(int(K.ADDED), a, 1, None, None, None)
        with self.assertRaises(moq5.EventLost) as lost:
            self.receiver.poll_track()
        self.assertEqual(lost.exception.stage, "event.struct_size")
        self.assertFalse(self.receiver.drained(), "a consumed-event conversion failure withdraws the track stream")
        make_true()
        self.push(a, dict(RAW))
        with mock.patch.object(_receiver, "MediaObject", side_effect=MemoryError):
            with self.assertRaises(MemoryError):
                self.receiver.poll_object()
        self.assertFalse(self.receiver.drained(), "a wrapper conversion MemoryError withdraws the object stream")
        make_true()
        self.assertEqual(self.counts()["dequeued"], 1)

    def test_guards_refuse_without_resetting_evidence_when_drained_is_true(self):
        _native._test_receiver_state(True, False, 0)
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.CLOSED)
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.CLOSED)
        self.assertTrue(self.receiver.drained())
        refused = []
        worker = threading.Thread(target=lambda: refused.append(self._drained_refused()))
        worker.start()
        worker.join(5)
        self.assertEqual(refused, ["receiver operation requires its creating thread"])
        self.assertTrue(self.receiver.drained(), "a refused caller changed nothing")
        if hasattr(os, "fork"):
            child = os.fork()
            if child == 0:
                try:
                    self.receiver.drained()
                except RuntimeError:
                    os._exit(0)
                os._exit(3)
            _, status = os.waitpid(child, 0)
            self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        self.assertTrue(self.receiver.drained())
        self.receiver.close()
        with self.assertRaises(RuntimeError):
            self.receiver.drained()

    def test_drained_keeps_the_owner_closed_and_fork_guards(self):
        refused = []
        worker = threading.Thread(target=lambda: refused.append(self._drained_refused()))
        worker.start()
        worker.join(5)
        self.assertEqual(refused, ["receiver operation requires its creating thread"])
        if hasattr(os, "fork"):
            child = os.fork()
            if child == 0:
                try:
                    self.receiver.drained()
                except RuntimeError:
                    os._exit(0)
                os._exit(3)
            _, status = os.waitpid(child, 0)
            self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        self.receiver.close()
        with self.assertRaises(RuntimeError):
            self.receiver.drained()

    def _drained_refused(self):
        try:
            self.receiver.drained()
        except RuntimeError as refused:
            return str(refused)
        return "accepted"

    # ---- 3. subscription ----------------------------------------------------------------

    def test_added_before_subscription_subscribes_with_the_actual_description_then_terminal_drains(self):
        a, b = self.track(), self.track(AUDIO)
        self.event(K.ADDED, a)
        self.event(K.ADDED, b)
        self.push(a, CMAF)
        self.push(b, {**RAW, "payload": b""})
        self.at("track_empty", 1, lambda: _native._test_receiver_state(True, False, 0))
        r = self.run_loop()
        # the fixture numbers handles in creation order: video 0, audio 1
        self.assertEqual(_native._test_subscriptions(),
                         [("subscribe", 0, int(moq5.StartMode.CURRENT), False, 0),
                          ("subscribe", 1, int(moq5.StartMode.CURRENT), False, 0)])
        self.assertEqual((r["objects"], r["drained"]), (2, True))
        self.assertEqual(self.decoded[1], ("raw", b"audio", b""), "the zero-byte NORMAL RAW object reached the raw handler")

    def test_subscribe_closed_while_terminal_still_drains_retained_items(self):
        a = self.track()
        self.event(K.ADDED, a)
        self.push(a, CMAF)
        self.push(a, END_OF_TRACK)
        _native._test_receiver_state(True, False, 0)          # terminal BEFORE the ADDED is handled
        r = self.run_loop()
        self.assertEqual(_native._test_subscriptions(), [], "the refused subscribe recorded nothing")
        self.assertEqual((r["events"], r["objects"], r["drained"]), (1, 2, True))
        self.assertEqual(self.statuses, [(b"video", 2)])

    def test_subscribe_refusals_are_not_swallowed_as_completion(self):
        for code in (C["ERR_INVAL"], C["ERR_WRONG_STATE"], C["ERR_UNSUPPORTED"]):
            with self.subTest(code=code):
                a = self.track()
                self.event(K.ADDED, a)
                self.push(a, dict(RAW))
                _native._test_subscribe_result(code)
                with self.assertRaises(moq5.MoqError) as caught:
                    self.run_loop()
                _native._test_subscribe_result(0)
                self.assertEqual((caught.exception.code, caught.exception.operation), (code, "subscribe"))
                self.assertEqual(self.loop_counts()["waits"], 0, "no wait after the error")
                self.assertEqual(self.receiver.poll_object().payload, b"raw\x00bytes", "the queued object was not discarded")

    # ---- 4. finite completion on the corrected predicate -------------------------------

    def test_vod_conversion_completes_with_the_endpoint_open_and_not_drained(self):
        a = self.track()
        held = self.receiver._track_from_capsule(a)     # a held Track keeps the weak cache entry observable
        self.event(K.ADDED, a)
        self.push(a, CMAF)
        self.push(a, {**RAW, "payload": b"two"})
        self.push(a, END_OF_TRACK)
        # the facade's section 11.3 observations, after the first drain: ENDED, then
        # the survivor handle's CURRENT state flips to non-live WITH a duration
        self.at("wait", 1, lambda: self.event(K.ENDED, a),
                lambda: _native._test_track_current(a, False, 60_000), lambda: self.event(K.UPDATED, a))
        r = self.run_loop()
        self.assertEqual((r["completed"], r["drained"], r["interrupted"], r["objects"], r["events"]), (True, False, False, 3, 3))
        self.assertFalse(self.endpoint.closed)
        self.assertFalse(r["terminal"].closed)
        self.assertEqual(self.statuses, [(b"video", 2)], "END_OF_TRACK reached the status handler only")
        self.assertEqual([d[0] for d in self.decoded], ["cmaf", "raw"])
        self.assertEqual(held.description.vod, moq5.VodState(False, 60_000), "the later success published the VOD state")

    def test_vod_conversion_with_duration_zero_is_present_and_accepted(self):
        # presence is `is not None`: a zero duration is a declared duration
        a = self.track()
        self.event(K.ADDED, a)
        self.push(a, dict(RAW))
        self.at("wait", 1, lambda: self.event(K.ENDED, a),
                lambda: _native._test_track_current(a, False, 0), lambda: self.event(K.UPDATED, a))
        r = self.run_loop()
        self.assertEqual((r["completed"], r["drained"], r["objects"]), (True, False, 1))

    def test_terminal_arriving_during_the_completion_redrain_is_drain_not_completion(self):
        # Reachable production shape: the shared terminal predicate flips
        # while the loop re-drains; queued items are still handled, the loop
        # ends as transport drain, never as broadcast completion, with no wait.
        a = self.track()
        self.event(K.ADDED, a)
        self.event(K.ENDED, a)
        _native._test_track_current(a, False, 1)
        self.event(K.UPDATED, a)
        self.push(a, dict(RAW))
        self.at("track_empty", 2, lambda: self.push(a, {**RAW, "payload": b"during"}),
                lambda: _native._test_receiver_state(True, False, 0))
        r = self.run_loop()
        self.assertEqual((r["completed"], r["drained"], r["interrupted"], r["objects"]), (False, True, False, 2))
        self.assertIn(("raw", b"video", b"during"), self.decoded, "the item queued with terminal was handled")
        lc = self.loop_counts()
        self.assertEqual(lc["waits"], 0, "no wait once terminal")
        self.assertEqual((lc["track_polls"], lc["object_polls"]), (6, 5), "bounded: first drain, re-drain, final CLOSED pass")

    def test_permanent_completion_needs_is_complete_and_removal(self):
        a = self.track()
        self.event(K.ADDED, a)
        self.push(a, dict(RAW))
        self.at("wait", 1, lambda: self.event(K.ENDED, a), lambda: self.event(K.REMOVED, a),
                lambda: _native._test_stats(0, 0, 0, 0, 0, 0, 0, 0, 0, False, 0, True))
        r = self.run_loop()
        self.assertEqual((r["completed"], r["drained"], r["objects"]), (True, False, 1))
        self.assertTrue(r["stats"].catalog_complete)
        self.assertFalse(self.endpoint.closed)

    def negative(self, *observe):
        # Not completion: the observations land at the first wait, a scripted
        # interrupt ends the loop at the second; nothing relies on a timeout.
        a = self.track()
        self.event(K.ADDED, a)
        self.push(a, dict(RAW))
        self.at("wait", 1, *(lambda o=o: o(a) for o in observe))
        self.at("wait", 2, self.interrupt())
        r = self.run_loop()
        self.endpoint.set_interrupted(False)
        self.assertEqual((r["completed"], r["interrupted"], r["drained"], r["objects"]), (False, True, False, 1))
        self.assertFalse(r["stats"].catalog_complete)

    def test_negative_non_live_without_duration_is_not_the_conversion(self):
        self.negative(lambda a: self.event(K.ENDED, a), lambda a: _native._test_track_current(a, False, None),
                      lambda a: self.event(K.UPDATED, a))

    def test_negative_live_vod_state_with_ended_is_not_completion(self):
        self.negative(lambda a: self.event(K.ENDED, a), lambda a: _native._test_track_current(a, True, None),
                      lambda a: self.event(K.UPDATED, a))

    def test_negative_ended_alone_is_not_completion(self):
        self.negative(lambda a: self.event(K.ENDED, a))

    def test_negative_ordinary_removal_is_not_completion(self):
        self.negative(lambda a: self.event(K.ENDED, a), lambda a: self.event(K.REMOVED, a))

    # ---- 5. work after the first drain -----------------------------------------------------

    def test_object_injected_at_the_completion_check_reaches_its_handler(self):
        a = self.track()
        self.event(K.ADDED, a)
        self.event(K.ENDED, a)
        _native._test_track_current(a, False, 1)
        self.event(K.UPDATED, a)
        self.push(a, CMAF)
        # the first empty object read ends the drain; the second is the loop's
        # completion re-drain: an object landing there must be handled
        self.at("object_empty", 2, lambda: self.push(a, {**RAW, "payload": b"late"}))
        r = self.run_loop()
        self.assertEqual((r["completed"], r["objects"]), (True, 2))
        self.assertIn(("raw", b"video", b"late"), self.decoded, "handled, not discarded by a peek")
        self.assertEqual(self.counts()["queued"], 0)

    def test_late_added_track_invalidates_the_earlier_completion(self):
        a, b = self.track(), self.track(AUDIO)
        self.event(K.ADDED, a)
        self.event(K.ENDED, a)
        _native._test_track_current(a, False, 1)
        self.event(K.UPDATED, a)
        self.push(a, dict(RAW))
        self.at("track_empty", 2, lambda: self.event(K.ADDED, b))
        self.at("wait", 1, self.interrupt())
        r = self.run_loop()
        self.endpoint.set_interrupted(False)
        self.assertEqual(len(_native._test_subscriptions()), 2, "the re-drain subscribed the late track")
        self.assertEqual((r["completed"], r["interrupted"], r["objects"]), (False, True, 1))

    # ---- 6. handler routing with exact carrier values ------------------------------------

    def test_handlers_receive_exact_values_and_unknown_packaging_is_refused_by_name(self):
        a = self.track()
        self.event(K.ADDED, a)
        self.push(a, {**RAW, "payload": b""})
        self.push(a, {**RAW, "status": 1, "payload": None, "end_of_group": True})
        self.push(a, CMAF)
        self.push(a, {**RAW, "packaging": 9})
        with self.assertRaisesRegex(RuntimeError, "unknown packaging 9"):
            self.run_loop()
        self.assertEqual(self.decoded[0], ("raw", b"video", b""))
        self.assertEqual(self.statuses, [(b"video", 1)])
        self.assertEqual(self.decoded[1], ("cmaf", b"video", FRAGMENT, 8, 40, tuple(moq5.CmafSample(*s) for s in SAMPLES)))
        c = self.counts()
        self.assertEqual((c["dequeued"], c["cleanup_calls"], c["queued"]), (4, 4, 0), "the refused object was consumed and cleaned")
        self.assertEqual(self.loop_counts()["waits"], 0)

    # ---- 7. errors and release ----------------------------------------------------------

    def test_losses_and_handler_exceptions_propagate_with_identity_and_no_retry(self):
        a = self.track()
        self.event(K.ADDED, a)
        self.push(a, {**CMAF, "malformed": "mdat_len_over"})
        self.push(a, {**RAW, "payload": b"after"})
        with self.assertRaises(moq5.ObjectLost) as lost:
            self.run_loop()
        self.assertEqual((lost.exception.stage, lost.exception.presentation_time_us), ("bounds:mdat_len", CMAF["presentation_time_us"]))
        self.assertEqual(self.loop_counts()["waits"], 0, "no wait or retry after the error")
        self.assertEqual(self.receiver.poll_object().payload, b"after", "the neighbour stays available")
        _native._test_push_event(int(K.ADDED), a, 1, None, None, None)     # sub-v0 stamp: a lost event
        with self.assertRaises(moq5.EventLost) as ev_lost:
            self.run_loop()
        self.assertEqual(ev_lost.exception.stage, "event.struct_size")
        self.push(a, CMAF)
        marker = ValueError("decoder refused")
        with self.assertRaises(ValueError) as failed:
            self.receive(self.receiver, moq5.PollOutcome, K, moq5.WaitResult, moq5.Packaging, moq5.MoqError,
                         decode_raw=lambda d, p: None, decode_cmaf=lambda *args: (_ for _ in ()).throw(marker),
                         note_status=lambda t, s: None, wait_us=0)
        self.assertIs(failed.exception, marker, "the handler exception itself, unwrapped")
        c = self.counts()
        self.assertEqual((c["dequeued"], c["cleanup_calls"]), (3, 3))
        self.assertEqual(c["actual_kept_refcounts"], c["kept_buffers"])

    def test_context_managers_release_receiver_before_endpoint_exactly_once_after_an_error(self):
        self.receiver.close()
        self.endpoint.close()
        _native._test_reset()
        with self.assertRaises(moq5.ObjectLost):
            with moq5.Endpoint.connect(moq5.EndpointConfig(url="moqt://fixture.invalid")) as endpoint, \
                    moq5.Receiver.attach(endpoint, moq5.ReceiverConfig.live(NS)) as rx:
                capsule = _native._test_new_track(rx._Receiver__handle)
                _native._test_track_desc(capsule, dict(DESC))
                _native._test_push_object(capsule, {**RAW, "malformed": "payload_null_len"})
                self.run_loop(rx)
        self.assertEqual(_native._test_receiver_counts(), (1, 1), "receiver destroyed once")
        self.assertEqual(_native._test_counts(), (1, 1, 1), "endpoint stopped and destroyed once, after the receiver")
        self.assertTrue(rx.closed)
        self.assertTrue(endpoint.closed)
        # setUp's pair is gone; give tearDown a closed pair to skip
        self.endpoint = endpoint
        self.receiver = rx


if __name__ == "__main__":
    unittest.main()
