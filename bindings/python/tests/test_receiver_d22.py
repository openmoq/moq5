"""Receiver mapping carriers, not protocol/provider qualification."""

import gc
import dataclasses
import os
import threading
import unittest
from unittest import mock

import moq5
from moq5 import _native, _receiver

MAX = (1 << 64) - 1
C = _native.constants


class ReceiverD22Tests(unittest.TestCase):
    def setUp(self):
        gc.collect()
        _native._test_reset()
        _native._test_fail_allocation_after(-1)
        self.layout = _native._test_layout()
        self.endpoint = moq5.Endpoint.connect(moq5.EndpointConfig(url="moqt://fixture.invalid"))
        self.receiver = moq5.Receiver.attach(self.endpoint, moq5.ReceiverConfig.live((b"live",)))
        self.capsule = _native._test_new_track(self.receiver._Receiver__handle)
        _native._test_track_desc(self.capsule, {"name": b"video", "packaging": 1})
        self.track = self.receiver._track_from_capsule(self.capsule)

    def tearDown(self):
        _native._test_fail_allocation_at(None)
        if not self.receiver.closed:
            self.receiver.close()
        self.endpoint.close()
        _native._test_object_settle()

    def test_companion_profile_defaults_to_auto(self):
        cfg = moq5.ReceiverConfig.live((b"live",))
        self.assertEqual(cfg.companion_profile, moq5.CompanionProfile.AUTO)
        self.assertEqual(_native._test_receiver_config()["companion_profile"],
                         moq5.CompanionProfile.AUTO)
        cfg = moq5.ReceiverConfig.live(
            (b"live",), companion_profile=moq5.CompanionProfile.D22_MSF01_LOC04_PUBLIC)
        self.receiver.close()
        with moq5.Receiver.attach(self.endpoint, cfg):
            self.assertEqual(_native._test_receiver_config()["companion_profile"], cfg.companion_profile)

    def test_optional_subscribe_arguments_preserve_legacy_call(self):
        self.receiver.subscribe(self.track, location=None, fill=None)
        self.assertEqual(_native._test_subscriptions(), [("subscribe", 0, 0, False, 0)])
        self.assertEqual(_native._test_receiver_options(), (None, None))
        _native.receiver_subscribe(self.receiver._Receiver__handle, self.capsule, 1, 0)
        self.assertEqual(_native._test_subscriptions()[-1], ("subscribe", 0, 1, True, 0))
        self.assertEqual(_native._test_receiver_options(), (None, None))

    def test_individual_fill_cancellation_accepts_zero_id(self):
        self.receiver.cancel_fill(self.track, 0)
        self.assertEqual(_native._test_subscriptions(), [])
        self.assertEqual(_native._test_receiver_cancellations(), [(0, 0, 0)])

    def test_fill_track_forwards_values_and_exact_refusals(self):
        fill = moq5.FillConfig(priority=77, group_order=moq5.FillGroupOrder.DESCENDING,
                               timeout_ms=1234,
                               location=moq5.LocationFilter(kind=moq5.LocationFilterKind.OBJECT_END,
                                                            start_group=9, start_object=1,
                                                            end_group=9, end_object=MAX))
        self.receiver.fill_track(self.track, fill)
        self.receiver.fill_track(self.track, moq5.FillConfig())
        commands = _native._test_receiver_fill_commands()
        self.assertEqual([c[0] for c in commands], [0, 0])
        self.assertGreater(commands[0][1], 0)
        self.assertEqual(commands[0][2:], (77, moq5.FillGroupOrder.DESCENDING, 1234,
                                           (moq5.LocationFilterKind.OBJECT_END, 9, 1, 9, MAX), 0))
        self.assertEqual(commands[1][2:], (None, None, None, None, 0), "empty inherits")
        self.assertEqual(_native._test_subscriptions(), [], "a fill is not a subscribe")
        for code in (C["ERR_WOULD_BLOCK"], C["ERR_WRONG_STATE"], C["ERR_UNSUPPORTED"],
                     C["ERR_LIMIT"], C["ERR_CLOSED"], -993):
            _native._test_subscribe_result(code)
            with self.assertRaises(moq5.MoqError) as caught:
                self.receiver.fill_track(self.track, moq5.FillConfig())
            self.assertEqual((caught.exception.code, caught.exception.operation), (code, "fill_track"))
            self.assertEqual(_native._test_receiver_fill_commands()[-1][-1], code)
        _native._test_subscribe_result(0)
        before = len(_native._test_receiver_fill_commands())
        for call in (lambda: self.receiver.fill_track(self.track, None),
                     lambda: self.receiver.fill_track(self.track, {}),
                     lambda: self.receiver.fill_track(object(), moq5.FillConfig())):
            with self.assertRaises(TypeError):
                call()
        self.assertEqual(len(_native._test_receiver_fill_commands()), before)

    def test_fill_track_keeps_the_creating_thread_guard(self):
        refused = []

        def foreign_thread():
            try:
                self.receiver.fill_track(self.track, moq5.FillConfig())
            except RuntimeError as exc:
                refused.append(str(exc))

        worker = threading.Thread(target=foreign_thread)
        worker.start()
        worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(refused, ["receiver operation requires its creating thread"])
        self.assertEqual(_native._test_receiver_fill_commands(), [])

    def test_fill_created_carries_its_id_only_when_stamped(self):
        created = moq5.ExtendedTrackEventKind.FILL_CREATED
        self.assertEqual(int(created), C["MEDIA_TRACK_FILL_CREATED"])
        event = self.event(created, fill=(0, 0, 0, 0))
        self.assertIs(event.kind_extended, created)
        self.assertEqual(event.fill_created_id, 0, "zero is a valid ID")
        self.assertIsNone(event.fill_terminal, "a receipt is not a terminal")
        event = self.event(created, fill=(MAX, 0, 0, 0))
        self.assertEqual(event.fill_created_id, MAX)
        terminal = self.event(moq5.ExtendedTrackEventKind.FILL_TERMINAL, fill=(5, 1, 0, 0))
        self.assertIsNone(terminal.fill_created_id)
        short = self.layout["event_initiating_request_id_end"] - 4
        event = self.event(created, stamp=short, fill=(3, 0, 0, 0))
        self.assertIs(event.kind_extended, created)
        self.assertIsNone(event.fill_created_id, "a partial ID is unobserved")
        legacy = moq5.TrackEvent(int(created), self.track, None, None, None, None)
        self.assertEqual(legacy, dataclasses.replace(legacy, fill_created_id=7),
                         "the ID does not change legacy equality")

    def test_object_has_additive_identity(self):
        _native._test_push_object(self.capsule, {"payload": b"data", "packaging": 1})
        obj = self.receiver.poll_object()
        self.assertEqual((obj.group_id, obj.object_id, obj.publisher_priority), (0, 0, 0))
        self.assertIsNone(obj.subgroup_id)

    def push(self, **fields):
        _native._test_push_object(self.capsule, {"payload": b"media", "packaging": 1, **fields})

    def event(self, kind, *, stamp=None, local=None, fill=None):
        _native._test_push_event(kind, self.capsule, stamp, None, None, None, local, fill)
        return self.receiver.poll_track()

    def test_compiled_names_and_legacy_enum_sets(self):
        for enum, prefix in (
            (moq5.CompanionProfile, "MEDIA_COMPANION_"),
            (moq5.LocationFilterKind, "LOCATION_FILTER_"),
            (moq5.FillGroupOrder, "GROUP_ORDER_"),
            (moq5.MediaTimeDomain, "MEDIA_TIME_DOMAIN_"),
            (moq5.DeliveryOrigin, "MEDIA_DELIVERY_"),
            (moq5.FillReason, "FILL_"),
            (moq5.FillErrorDomain, "FILL_ERROR_"),
        ):
            for item in enum:
                self.assertEqual(item.value, C[prefix + item.name])
        self.assertEqual(list(moq5.TrackEventKind), [1, 2, 3, 4, 5, 6, 7])
        self.assertEqual(list(moq5.TrackState), [0, 1, 2, 3, 4, 5])
        self.assertEqual(moq5.ExtendedTrackEventKind.LOCAL_FAILURE, C["MEDIA_TRACK_LOCAL_FAILURE"])
        self.assertEqual(moq5.ExtendedTrackEventKind.FILL_TERMINAL, C["MEDIA_TRACK_FILL_TERMINAL"])
        self.assertEqual(moq5.ExtendedTrackState.FAILED, C["MEDIA_TRACK_STATE_FAILED"])

    def test_legacy_native_attach_arity_selects_auto_profile(self):
        self.receiver.close()
        capsule = _native.receiver_attach(self.endpoint._Endpoint__handle, (b"live",), b"",
                                         False, 0, 1, 0, 0, 0)
        try:
            self.assertEqual(_native._test_receiver_config()["companion_profile"],
                             moq5.CompanionProfile.AUTO)
            self.assertTrue(_native._test_receiver_config()["full_size"])
        finally:
            _native.receiver_close(capsule)

    def test_transport_does_not_select_companion(self):
        self.receiver.close()
        self.endpoint.close()
        self.endpoint = moq5.Endpoint.connect(moq5.EndpointConfig(
            url="moqt://fixture.invalid", versions=(moq5.Version.DRAFT_22,)))
        self.receiver = moq5.Receiver.attach(self.endpoint, moq5.ReceiverConfig.live((b"live",)))
        self.assertEqual(_native._test_receiver_config()["companion_profile"],
                         moq5.CompanionProfile.AUTO)

    def test_semantic_locations_and_fill_overrides_are_copied_exactly(self):
        for kind in moq5.LocationFilterKind:
            with self.subTest(kind=kind):
                location = moq5.LocationFilter(kind=kind, relative_group_offset=MAX,
                                              start_group=0, start_object=MAX, end_group=MAX, end_object=MAX)
                self.receiver.subscribe(self.track, start=moq5.StartMode.NEXT_GROUP,
                                        priority=0, location=location)
                self.assertEqual(_native._test_receiver_options(), ((kind, MAX, 0, MAX, MAX, MAX), None))
        for timeout in (None, 0, MAX):
            with self.subTest(timeout=timeout):
                fill = moq5.FillConfig(priority=0, group_order=moq5.FillGroupOrder.DESCENDING,
                                      timeout_ms=timeout, location=moq5.LocationFilter())
                self.receiver.subscribe(self.track, fill=fill)
                self.assertEqual(_native._test_receiver_options(), (
                    None, (True, 0, True, moq5.FillGroupOrder.DESCENDING,
                           timeout is not None, 0 if timeout is None else timeout, (0, 0, 0, 0, 0, 0))))
        self.receiver.subscribe(self.track, fill=moq5.FillConfig())
        self.assertEqual(_native._test_receiver_options(), (None, (False, 0, False, 0, False, 0, None)))
        self.receiver.subscribe(self.track, location=moq5.LocationFilter(), fill=moq5.FillConfig(priority=255))
        self.assertEqual(_native._test_receiver_options(), ((0, 0, 0, 0, 0, 0), (True, 255, False, 0, False, 0, None)))

    def test_checked_inputs_and_native_refusals_do_not_become_defaults(self):
        for call in (
            lambda: moq5.ReceiverConfig.live((b"n",), companion_profile=True),
            lambda: moq5.ReceiverConfig.live((b"n",), companion_profile=9),
            lambda: moq5.ReceiverConfig.live((b"n",), companion_profile=3),
            lambda: moq5.LocationFilter(kind=True),
            lambda: moq5.LocationFilter(kind=6),
            lambda: moq5.LocationFilter(start_group=-1),
            lambda: moq5.LocationFilter(end_object=MAX + 1),
            lambda: moq5.FillConfig(priority=256),
            lambda: moq5.FillConfig(priority=True),
            lambda: moq5.FillConfig(group_order=0),
            lambda: moq5.FillConfig(timeout_ms=MAX + 1),
            lambda: moq5.FillConfig(location={}),
            lambda: self.receiver.subscribe(self.track, location={}),
            lambda: self.receiver.subscribe(self.track, fill={}),
        ):
            with self.assertRaises((ValueError, TypeError)):
                call()
        self.assertEqual(_native._test_subscriptions(), [])
        for code in (C["ERR_UNSUPPORTED"], C["ERR_INVAL"], C["ERR_WRONG_STATE"], -991):
            _native._test_subscribe_result(code)
            with self.assertRaises(moq5.MoqError) as caught:
                self.receiver.subscribe(self.track, fill=moq5.FillConfig())
            self.assertEqual((caught.exception.code, caught.exception.operation), (code, "subscribe"))
        self.assertEqual(_native._test_subscriptions(), [])

    def test_cancel_is_individual_nonblocking_and_exact_on_refusal(self):
        _native._test_track_state(self.capsule, moq5.TrackState.ACTIVE, False, False)
        before = _native._test_endpoint_entries()
        self.receiver.cancel_fill(self.track, MAX)
        self.receiver.cancel_fill(self.track, 0)
        for code in (C["ERR_STALE_HANDLE"], C["ERR_WRONG_STATE"], C["ERR_UNSUPPORTED"],
                     C["ERR_WOULD_BLOCK"], C["ERR_CLOSED"], C["ERR_INTERRUPTED"], -992):
            _native._test_subscribe_result(code)
            with self.assertRaises(moq5.MoqError) as caught:
                self.receiver.cancel_fill(self.track, MAX)
            self.assertEqual((caught.exception.code, caught.exception.operation), (code, "cancel_fill"))
            self.assertEqual(_native._test_receiver_cancellations()[-1], (0, MAX, code))
        self.assertEqual(_native._test_receiver_cancellations()[:2], [(0, MAX, 0), (0, 0, 0)])
        self.assertEqual(_native._test_subscriptions(), [])
        self.assertEqual(_native._test_endpoint_entries(), before, "no blocking wait or endpoint wake in bridge")
        self.assertIs(self.receiver.track_state(self.track), moq5.TrackState.ACTIVE)
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY)
        self.assertFalse(self.receiver.terminal.fatal)

    def test_cancel_and_new_subscribe_keep_thread_process_and_lifetime_guards(self):
        calls = (lambda: self.receiver.cancel_fill(self.track, 0),
                 lambda: self.receiver.subscribe(self.track, fill=moq5.FillConfig()),
                 lambda: self.receiver.track_state_extended(self.track))
        refused = []

        def foreign_thread():
            for call in calls:
                try:
                    call()
                except RuntimeError as exc:
                    refused.append(str(exc))

        worker = threading.Thread(target=foreign_thread)
        worker.start()
        worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(refused, ["receiver operation requires its creating thread"] * 3)
        child = os.fork()
        if child == 0:
            for call in calls:
                try:
                    call()
                except RuntimeError:
                    continue
                os._exit(2)
            os._exit(0)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        for bad in (-1, MAX + 1, True, 1.5):
            with self.assertRaises((ValueError, TypeError)):
                self.receiver.cancel_fill(self.track, bad)
        self.receiver.close()
        for call in calls:
            with self.assertRaises(RuntimeError):
                call()
        with moq5.Receiver.attach(self.endpoint, moq5.ReceiverConfig.live((b"other",))) as other:
            with self.assertRaisesRegex(RuntimeError, "another receiver"):
                other.cancel_fill(self.track, 0)
            with self.assertRaisesRegex(RuntimeError, "another receiver"):
                _native.receiver_cancel_fill(other._Receiver__handle, self.capsule, 0)
        self.assertEqual(_native._test_receiver_cancellations(), [])
        self.assertEqual(_native._test_subscriptions(), [])

    def test_local_failure_is_signed_track_local_not_peer_end(self):
        for code in (C["ERR_NOMEM"], C["ERR_BUFFER"]):
            ev = self.event(C["MEDIA_TRACK_LOCAL_FAILURE"], local=code)
            self.assertIs(ev.kind_extended, moq5.ExtendedTrackEventKind.LOCAL_FAILURE)
            self.assertIs(type(ev.kind), int, "legacy kind keeps its unknown-int contract")
            self.assertIs(ev.track, self.track)
            self.assertEqual(ev.local_result, code)
            self.assertIsNone(ev.fill_terminal)
        _native._test_track_state(self.capsule, C["MEDIA_TRACK_STATE_FAILED"], False, False)
        self.assertIs(type(self.receiver.track_state(self.track)), int)
        self.assertIs(self.receiver.track_state_extended(self.track), moq5.ExtendedTrackState.FAILED)
        self.assertFalse(self.receiver.terminal.fatal)
        self.assertFalse(self.receiver.terminal.closed)
        end = self.layout["event_local_result_end"]
        self.assertIsNone(self.event(C["MEDIA_TRACK_LOCAL_FAILURE"], stamp=end - 1, local=-9).local_result)
        self.assertEqual(self.event(C["MEDIA_TRACK_LOCAL_FAILURE"], stamp=end, local=-9).local_result, -9)
        self.assertIsNone(self.event(C["MEDIA_TRACK_ENDED"], local=-9).local_result)

    def test_fill_terminal_preserves_zero_max_unknown_and_partial_fields(self):
        kind = C["MEDIA_TRACK_FILL_TERMINAL"]
        ev = self.event(kind, fill=(0, moq5.FillReason.PEER_CANCELLED_OR_RESET, moq5.FillErrorDomain.STREAM, 0))
        self.assertIs(ev.kind_extended, moq5.ExtendedTrackEventKind.FILL_TERMINAL)
        self.assertEqual(ev.fill_terminal, moq5.FillTerminal(0, moq5.FillReason.PEER_CANCELLED_OR_RESET,
                                                           moq5.FillErrorDomain.STREAM, 0))
        self.assertNotEqual(ev.kind, moq5.TrackEventKind.ENDED)
        self.assertIsNone(ev.local_result)
        full = (MAX, (1 << 32) - 1, (1 << 32) - 1, MAX)
        names = ("initiating_request_id", "reason", "error_domain", "error_code")
        ends = [self.layout["event_" + n + "_end"] for n in
                ("initiating_request_id", "fill_reason", "fill_error_domain", "fill_error_code")]
        for stamp in [self.layout["event_v0"], *[n - 1 for n in ends], *ends]:
            terminal = self.event(kind, stamp=stamp, fill=full).fill_terminal
            for name, end, value in zip(names, ends, full):
                self.assertEqual(getattr(terminal, name), value if stamp >= end else None)
        self.assertFalse(self.receiver.terminal.fatal)
        self.assertIsNone(self.event(C["MEDIA_TRACK_ENDED"], fill=full).fill_terminal)
        unknown = self.event(999)
        self.assertEqual((unknown.kind, unknown.kind_extended), (999, 999))
        self.assertIsNone(unknown.fill_terminal)

    def test_terminal_conversion_is_transactional(self):
        old = self.event(C["MEDIA_TRACK_ADDED"]).description
        _native._test_track_desc(self.capsule, {"name": b"new", "packaging": 1})
        with mock.patch.object(_receiver, "FillTerminal", side_effect=MemoryError):
            with self.assertRaises(MemoryError):
                self.event(C["MEDIA_TRACK_FILL_TERMINAL"], fill=(MAX, 1, 0, 0))
        self.assertIs(self.track.description, old)
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY)
        self.assertEqual(self.event(C["MEDIA_TRACK_FILL_TERMINAL"], fill=(0, 1, 0, 0)).description.name, b"new")
        _native._test_push_event(C["MEDIA_TRACK_LOCAL_FAILURE"], None, None, None, None, None, -9)
        with self.assertRaises(moq5.EventLost):
            self.receiver.poll_track()

    def test_identity_and_origin_are_whole_records(self):
        identity_end = self.layout["object_identity_end"]
        origin_end = self.layout["object_origin_end"]
        for stamp in (self.layout["object_v0"], identity_end - 1, identity_end, origin_end - 1, origin_end):
            self.push(stamp=stamp, group_id=MAX, object_id=0, subgroup_id=0, publisher_priority=255,
                      delivery_origin=moq5.DeliveryOrigin.FILL, initiating_request_id=MAX)
            obj = self.receiver.poll_object()
            self.assertEqual((obj.group_id, obj.object_id, obj.subgroup_id, obj.publisher_priority, obj.has_subgroup),
                             (MAX, 0, 0, 255, True) if stamp >= identity_end else (None,) * 5)
            self.assertEqual((obj.delivery_origin, obj.initiating_request_id),
                             (moq5.DeliveryOrigin.FILL, MAX) if stamp >= origin_end else (None, None))
        for origin, request in ((moq5.DeliveryOrigin.LIVE, 0), (moq5.DeliveryOrigin.FILL, 0), ((1 << 32) - 1, MAX)):
            self.push(delivery_origin=origin, initiating_request_id=request, datagram=True)
            obj = self.receiver.poll_object()
            self.assertEqual(obj.delivery_origin, origin)
            self.assertEqual(obj.initiating_request_id, None if origin == moq5.DeliveryOrigin.LIVE else request)
            self.assertIs(obj.has_subgroup, False)
            self.assertIsNone(obj.subgroup_id)

    def test_raw_metadata_and_config_backing_survive_close(self):
        metadata = {"time_domain": moq5.MediaTimeDomain.APPLICATION_MEDIA, "time_us": 0,
                    "timestamp": MAX, "timescale": MAX, "marking": b"\x00\x80\x00\xff", "audio_level": 255,
                    "video_config": (1, 1, 4), "audio_config": (0, 2, 0), "present": 0x80000000}
        self.push(metadata=metadata, properties=b"object\x00raw", track_properties=b"Tconf\x00raw",
                  delivery_origin=moq5.DeliveryOrigin.FILL, initiating_request_id=0)
        obj = self.receiver.poll_object()
        c = _native._test_object_counts()
        self.assertEqual((c["cleanup_calls"], c["refs_created"], c["refs_released"]), (1, 3, 3))
        self.assertEqual(c["actual_kept_refcounts"], c["kept_buffers"])
        self.receiver.close()
        self.endpoint.close()
        _native._test_scramble_objects()
        self.assertEqual((obj.object_properties, obj.track_properties), (b"object\x00raw", b"Tconf\x00raw"))
        self.assertEqual(obj.metadata, moq5.MediaMetadata(moq5.MediaTimeDomain.APPLICATION_MEDIA, 0, None,
            moq5.LocMetadata(0x8000003f, MAX, MAX, b"\x00\x80\x00\xff", 255, b"conf", b"")))
        self.assertIsNone(obj.capture_time_us, "application clock is not capture time")
        self.assertEqual(obj.initiating_request_id, 0)

    def test_time_domain_presence_and_raw_values_are_not_recomputed(self):
        cases = (
            ({"time_domain": moq5.MediaTimeDomain.UNIX_US, "time_us": MAX, "timestamp": MAX}, MAX, None, MAX),
            ({"time_domain": moq5.MediaTimeDomain.APPLICATION_MEDIA, "time_us": 0, "timestamp": 0,
              "timescale": 1_000_000, "independent": False}, 0, 1_000_000, 0),
            ({"time_domain": moq5.MediaTimeDomain.APPLICATION_MEDIA, "timestamp": MAX, "timescale": 0}, MAX, 0, None),
            ({"time_domain": 0xffffffff, "timestamp": 0, "timescale": MAX, "independent": True}, 0, MAX, None),
            ({"time_domain": moq5.MediaTimeDomain.NONE}, None, None, None),
        )
        for metadata, timestamp, timescale, converted in cases:
            self.push(metadata=metadata)
            obj = self.receiver.poll_object()
            self.assertEqual((obj.metadata.time_domain, obj.metadata.time_us), (metadata["time_domain"], converted))
            self.assertEqual((obj.metadata.loc.timestamp, obj.metadata.loc.timescale), (timestamp, timescale))
            self.assertEqual(obj.metadata.independent, metadata.get("independent"))
            self.assertIsNone(obj.metadata.loc.video_config)
            self.assertIsNone(obj.metadata.loc.audio_config)
            self.assertIsNone(obj.metadata.loc.marking)
            self.assertIsNone(obj.capture_time_us)

    def test_old_metadata_prefixes_are_absent_not_zero_or_empty(self):
        end = self.layout["object_metadata_end"]
        for fields, expected in (
            ({"stamp": end - 1, "metadata": {"timestamp": MAX}}, None),
            ({"stamp": end, "metadata": {"stamp": self.layout["metadata_v0"] - 1}}, None),
            ({"metadata": {"stamp": 0}}, None),
            ({}, None),
        ):
            self.push(**fields)
            self.assertIs(self.receiver.poll_object().metadata, expected)
        self.push(metadata={"loc_stamp": self.layout["loc_v0"] - 1, "time_us": 0})
        obj = self.receiver.poll_object()
        self.assertIsNone(obj.metadata.loc)
        self.assertEqual(obj.metadata.time_us, 0)
        self.push(stamp=self.layout["object_track_properties_end"], track_properties=b"")
        self.assertEqual(self.receiver.poll_object().track_properties, b"")

    def test_malformed_metadata_and_allocation_loss_cleanup_once(self):
        old = self.event(C["MEDIA_TRACK_ADDED"]).description
        _native._test_track_desc(self.capsule, {"name": b"unpublished", "packaging": 1})
        for malformed in ("video_config_null_len", "audio_config_len_over", "marking_len_over"):
            self.push(metadata={"timestamp": 0}, properties=b"object", track_properties=b"track", malformed=malformed)
            before = _native._test_object_counts()
            with self.assertRaises(moq5.ObjectLost):
                self.receiver.poll_object()
            after = _native._test_object_counts()
            self.assertEqual(after["cleanup_calls"] - before["cleanup_calls"], 1)
            self.assertEqual(after["actual_kept_refcounts"], after["kept_buffers"])
            self.assertIs(self.track.description, old)
        self.push(metadata={"timestamp": 0}, properties=b"object", track_properties=b"track")
        _native._test_fail_allocation_at("object.metadata")
        with self.assertRaises(MemoryError):
            self.receiver.poll_object()
        self.assertTrue(_native._test_allocation_site_fired("object.metadata"))
        self.assertIs(self.track.description, old)
        self.push(metadata={"timestamp": 0})
        with mock.patch.object(_receiver, "LocMetadata", side_effect=MemoryError):
            with self.assertRaises(MemoryError):
                self.receiver.poll_object()
        self.assertIs(self.track.description, old)
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.EMPTY)
        c = _native._test_object_counts()
        self.assertEqual((c["dequeued"], c["cleanup_calls"], c["cleanups_on_polling_thread"]), (5, 5, 5))
        self.assertEqual(c["actual_kept_refcounts"], c["kept_buffers"])
        self.push(metadata={"timestamp": MAX})
        self.assertEqual(self.receiver.poll_object().metadata.loc.timestamp, MAX)

    def test_legacy_positional_patterns_equality_hash_and_defaults(self):
        obj = moq5.MediaObject(self.track, 0, moq5.Packaging.RAW, moq5.ObjectStatus.NORMAL,
                              False, False, False, None, 0, 0, 0, b"x", b"", 0, 0, ())
        event = moq5.TrackEvent(moq5.TrackEventKind.ADDED, self.track, None, None, None, None)
        self.assertEqual(len(obj.__match_args__), 16)
        self.assertEqual(len(event.__match_args__), 6)
        self.assertIsNone(obj.metadata)
        self.assertIsNone(obj.group_id)
        self.assertIsNone(event.local_result)
        self.assertIsNone(event.fill_terminal)
        extended = dataclasses.replace(obj, group_id=MAX, delivery_origin=moq5.DeliveryOrigin.FILL, initiating_request_id=0)
        self.assertEqual(obj, extended)
        self.assertEqual(hash(obj), hash(extended))
        self.assertEqual(event, dataclasses.replace(event, local_result=-9))
