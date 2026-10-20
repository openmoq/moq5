"""Sized UPDATE failure mapping; causal production coverage is in the C suite."""

import dataclasses
import unittest

import moq5
from moq5 import _native


class ReceiverUpdateFailureTests(unittest.TestCase):
    def setUp(self):
        _native._test_reset()
        self.endpoint = moq5.Endpoint.connect(moq5.EndpointConfig(url="moqt://fixture.invalid"))
        self.receiver = moq5.Receiver.attach(self.endpoint, moq5.ReceiverConfig.live((b"live",)))
        self.capsule = _native._test_new_track(self.receiver._Receiver__handle)
        _native._test_track_desc(self.capsule, {"name": b"video", "packaging": 1})
        self.layout = _native._test_layout()

    def tearDown(self):
        self.receiver.close()
        self.endpoint.close()

    def event(self, result, *, stamp=None, kind=11, track=True):
        c = _native.constants
        _native._test_push_event(kind, self.capsule if track else None,
                                stamp, None, None, None, result,
                                (123, c["FILL_COMPLETE"], c["FILL_ERROR_NONE"], 0),
                                (c["MEDIA_DELIVERY_FILL"], 0))
        return self.receiver.poll_track()

    def test_exact_signed_result_and_distinct_kind(self):
        self.assertEqual(_native.constants["MEDIA_TRACK_UPDATE_LOCAL_FAILURE"], 11)
        for result in [-1, -5, -9, 0, -(1 << 31), (1 << 31) - 1]:
            with self.subTest(result=result):
                event = self.event(result)
                self.assertEqual(event.kind, 11)
                self.assertIs(event.kind_extended, moq5.ExtendedTrackEventKind.UPDATE_LOCAL_FAILURE)
                self.assertEqual(event.local_result, result)
                self.assertIsNotNone(event.track)
                self.assertIsNone(event.fill_terminal)
                self.assertIsNone(event.parse_drop)
                self.assertIsNone(event.delivery_origin)
                self.assertIsNone(event.delivery_initiating_request_id)
                self.assertEqual(event, dataclasses.replace(event, local_result=None))

    def test_local_result_requires_complete_prefix(self):
        end = self.layout["event_local_result_end"]
        for stamp in range(self.layout["event_v0"], self.layout["event_sizeof"] + 1):
            with self.subTest(stamp=stamp):
                event = self.event(-5, stamp=stamp)
                self.assertEqual(event.local_result, -5 if stamp >= end else None)

    def test_unrelated_kinds_do_not_gain_an_update_error(self):
        c = _native.constants
        for kind in [c["MEDIA_TRACK_ENDED"], c["MEDIA_TRACK_UPDATE_OK"], 0x1234]:
            with self.subTest(kind=kind):
                event = self.event(-5, kind=kind)
                self.assertEqual(event.kind, kind)
                self.assertIsNone(event.local_result)
                self.assertNotEqual(event.kind_extended, moq5.ExtendedTrackEventKind.UPDATE_LOCAL_FAILURE)

    def test_update_failure_requires_a_track_and_is_consumed_once(self):
        with self.assertRaises(moq5.EventLost):
            self.event(-5, track=False)
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY)
