"""Sender companion marshalling through the C recorder, not live D22 sending."""

import dataclasses
import gc
import inspect
import unittest
from unittest import mock

import moq5
from moq5 import _native


LEGACY = moq5.CompanionProfile.LEGACY_MSF01_LOC01
D22 = moq5.CompanionProfile.D22_MSF01_LOC04_PUBLIC
AUTO = moq5.CompanionProfile.AUTO
NS = (b"live", b"cam\x001")


class SenderD22Tests(unittest.TestCase):
    def setUp(self):
        gc.collect()
        _native._test_reset()
        _native._test_sender_reset()

    def connect(self, versions=()):
        return moq5.Endpoint.connect(moq5.EndpointConfig(
            url="moqt://fixture.invalid", versions=versions))

    def native_args(self, handle):
        return (handle, NS, b"", int(moq5.Backpressure.DROP_TO_KEYFRAME),
                0, 0, 0, 0, 0, True, False, False, 0)

    def assert_no_attach(self):
        self.assertEqual(_native._test_sender_counts(), {
            "attach_entries": 0, "attached": 0, "destroy_entries": 0,
            "destroyed": 0, "live": False})
        self.assertIsNone(_native._test_sender_config())

    def test_default_and_presets_select_the_auto_profile(self):
        configs = (
            moq5.SenderConfig(namespace=NS, backpressure=moq5.Backpressure.DROP_GROUP),
            moq5.SenderConfig.live(NS), moq5.SenderConfig.lossless(NS))
        with self.connect() as endpoint:
            for config in configs:
                with self.subTest(config=config):
                    self.assertIs(config.companion_profile, AUTO)
                    with moq5.Sender.attach(endpoint, config):
                        self.assertEqual(_native._test_sender_config()["companion_profile"], AUTO)

    def test_explicit_d22_reaches_the_native_config(self):
        with self.connect() as endpoint:
            with moq5.Sender.attach(endpoint, moq5.SenderConfig.live(
                    NS, companion_profile=D22)):
                captured = _native._test_sender_config()
                self.assertEqual(captured["companion_profile"], D22)
                self.assertTrue(captured["full_size"])
                self.assertFalse(captured["unsupported_prefix"])

    def test_profile_is_keyword_only_and_frozen(self):
        parameter = inspect.signature(moq5.SenderConfig).parameters["companion_profile"]
        self.assertIs(parameter.kind, inspect.Parameter.KEYWORD_ONLY)
        config = moq5.SenderConfig.lossless(NS, companion_profile=int(D22))
        self.assertIs(config.companion_profile, D22)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            config.companion_profile = LEGACY
        for item in moq5.CompanionProfile:
            self.assertEqual(item.value, _native.constants["MEDIA_COMPANION_" + item.name])

    def test_initial_group_id_defaults_absent(self):
        config = moq5.SenderConfig.live(NS)
        self.assertIsNone(config.initial_group_id)
        with self.connect() as endpoint:
            with moq5.Sender.attach(endpoint, config):
                captured = _native._test_sender_config()
                self.assertFalse(captured["has_initial_group_id"])
                self.assertEqual(captured["initial_group_id"], 0)

    def test_initial_group_id_reaches_the_native_config_unclamped(self):
        # The service owns the range check: every 64-bit value is passed as
        # given, including past the largest Group ID.
        with self.connect() as endpoint:
            for value in (0, 42, 2**62 - 1, 2**62, 2**64 - 1):
                with self.subTest(value=value):
                    with moq5.Sender.attach(
                            endpoint, moq5.SenderConfig.live(NS, initial_group_id=value)):
                        captured = _native._test_sender_config()
                        self.assertTrue(captured["full_size"])
                        self.assertTrue(captured["has_initial_group_id"])
                        self.assertEqual(captured["initial_group_id"], value)

    def test_initial_group_id_limit_refusal_keeps_its_code(self):
        limit = _native.constants["ERR_LIMIT"]
        with self.connect() as endpoint:
            _native._test_sender_attach_result(limit)
            try:
                with self.assertRaises(moq5.MoqError) as raised:
                    moq5.Sender.attach(
                        endpoint, moq5.SenderConfig.live(NS, initial_group_id=2**62))
                self.assertEqual(raised.exception.code, limit)
                self.assertEqual(raised.exception.operation, "attach")
                self.assertEqual(_native._test_sender_config()["initial_group_id"], 2**62)
            finally:
                _native._test_sender_attach_result(0)

    def test_initial_group_id_is_keyword_only_checked_and_frozen(self):
        parameter = inspect.signature(moq5.SenderConfig).parameters["initial_group_id"]
        self.assertIs(parameter.kind, inspect.Parameter.KEYWORD_ONLY)
        cases = [(value, TypeError) for value in (True, 1.0, "1", b"1")]
        cases += [(value, ValueError) for value in (-1, 2**64)]
        for value, error in cases:
            with self.subTest(value=value):
                with self.assertRaisesRegex(error, "initial_group_id"):
                    moq5.SenderConfig.live(NS, initial_group_id=value)
        config = moq5.SenderConfig.lossless(NS, initial_group_id=7)
        self.assertEqual(config.initial_group_id, 7)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            config.initial_group_id = 8

    def test_invalid_native_initial_group_id_leaves_the_handle_inert(self):
        cases = [(value, TypeError) for value in (True, 1.0, "1")]
        cases += [(value, ValueError) for value in (-1, 2**64)]
        with self.connect() as endpoint:
            handle = _native.sender_prepare(endpoint._Endpoint__handle)
            try:
                for value, error in cases:
                    with self.subTest(value=value):
                        with self.assertRaisesRegex(error, "initial_group_id"):
                            _native.sender_activate(*self.native_args(handle), D22, value)
                        self.assert_no_attach()
                _native.sender_activate(*self.native_args(handle), D22, None)
                self.assertFalse(_native._test_sender_config()["has_initial_group_id"])
            finally:
                _native.sender_close(handle)

    def test_transport_offer_does_not_select_the_media_profile(self):
        for versions in ((), (moq5.Version.DRAFT_16,), (moq5.Version.DRAFT_18,),
                         (moq5.Version.DRAFT_22,)):
            with self.subTest(versions=versions), self.connect(versions) as endpoint:
                for fields, expected in (({}, AUTO), ({"companion_profile": LEGACY}, LEGACY),
                                         ({"companion_profile": D22}, D22),
                                         ({"companion_profile": AUTO}, AUTO)):
                    with self.subTest(profile=expected):
                        with moq5.Sender.attach(endpoint, moq5.SenderConfig.live(NS, **fields)):
                            self.assertEqual(_native._test_sender_config()["companion_profile"], expected)

    def test_other_sender_inputs_are_unchanged(self):
        fields = dict(namespace=NS, catalog_track=b"c\x00t",
                      backpressure=moq5.Backpressure.BLOCK_TIMEOUT,
                      block_timeout_us=2**64 - 1, queue_max_objects=2**32 - 1,
                      queue_max_bytes=12, pre_ready_max_objects=13, pre_ready_max_bytes=14,
                      validate_cmaf=False, publish_tracks=True, drop_without_demand=True,
                      catalog_refresh_interval_us=moq5.CATALOG_REFRESH_DISABLED)
        snapshots = []
        with self.connect() as endpoint:
            for profile in (LEGACY, D22, AUTO):
                with moq5.Sender.attach(endpoint, moq5.SenderConfig(
                        **fields, companion_profile=profile)):
                    captured = _native._test_sender_config()
                    self.assertEqual(captured.pop("companion_profile"), profile)
                    for field, expected in fields.items():
                        self.assertEqual(captured[field], expected, field)
                    self.assertTrue(captured["endpoint_null"])
                    self.assertTrue(captured["callbacks_absent"])
                    self.assertTrue(captured["content_protections_absent"])
                    snapshots.append(captured)
        self.assertEqual(snapshots[0], snapshots[1])
        self.assertEqual(snapshots[0], snapshots[2])

    def test_invalid_config_profiles_precede_prepare_and_attach(self):
        cases = [(value, TypeError) for value in (True, False, None, 1.0, "1", b"1", object())]
        cases += [(value, ValueError) for value in (-1, 3, 2**32 - 1, 2**32, 2**64)]
        with self.connect() as endpoint:
            with mock.patch.object(_native, "sender_prepare") as prepare:
                for value, error in cases:
                    with self.subTest(value=value), self.assertRaises(error):
                        moq5.Sender.attach(endpoint, moq5.SenderConfig.live(
                            NS, companion_profile=value))
                prepare.assert_not_called()
            self.assert_no_attach()

    def test_old_native_arity_selects_the_auto_profile(self):
        with self.connect() as endpoint:
            handle = _native.sender_prepare(endpoint._Endpoint__handle)
            try:
                _native.sender_activate(*self.native_args(handle))
                captured = _native._test_sender_config()
                self.assertEqual(captured["companion_profile"], AUTO)
                self.assertTrue(captured["full_size"])
            finally:
                _native.sender_close(handle)

    def test_optional_native_tail_accepts_known_profiles_and_none(self):
        with self.connect() as endpoint:
            for profile, expected in ((int(LEGACY), LEGACY), (int(D22), D22),
                                      (int(AUTO), AUTO), (None, AUTO)):
                with self.subTest(profile=profile):
                    handle = _native.sender_prepare(endpoint._Endpoint__handle)
                    try:
                        _native.sender_activate(*self.native_args(handle), profile)
                        self.assertEqual(_native._test_sender_config()["companion_profile"], expected)
                    finally:
                        _native.sender_close(handle)

    def test_invalid_native_profile_leaves_the_prepared_handle_inert(self):
        cases = [(value, TypeError) for value in (True, False, 1.0, "1", b"1", object())]
        cases += [(value, ValueError) for value in (-1, 3, 2**32 - 1, 2**32, 2**64)]
        with self.connect() as endpoint:
            handle = _native.sender_prepare(endpoint._Endpoint__handle)
            try:
                for value, error in cases:
                    with self.subTest(value=value):
                        with self.assertRaisesRegex(error, "companion_profile"):
                            _native.sender_activate(*self.native_args(handle), value)
                        self.assert_no_attach()
                # Invalid profiles are refused before even inspecting the
                # namespace whose parts the activation would allocate.
                args = list(self.native_args(handle))
                args[1] = object()
                with self.assertRaisesRegex(ValueError, "companion_profile"):
                    _native.sender_activate(*args, 3)
                self.assert_no_attach()
                _native.sender_activate(*self.native_args(handle), D22)
                self.assertEqual(_native._test_sender_config()["companion_profile"], D22)
            finally:
                _native.sender_close(handle)

    def test_d22_native_refusal_keeps_code_operation_and_no_attachment(self):
        with self.connect() as endpoint:
            for index, code in enumerate((-2, -1, -5, -4), start=1):
                with self.subTest(code=code):
                    _native._test_sender_attach_result(code)
                    try:
                        with self.assertRaises(moq5.MoqError) as raised:
                            moq5.Sender.attach(endpoint, moq5.SenderConfig.live(NS, companion_profile=D22))
                        self.assertEqual(raised.exception.code, code)
                        self.assertEqual(raised.exception.operation, "attach")
                        self.assertEqual(_native._test_sender_config()["companion_profile"], D22)
                        self.assertEqual(_native._test_sender_counts(), {
                            "attach_entries": index, "attached": 0, "destroy_entries": 0,
                            "destroyed": 0, "live": False})
                        self.assertFalse(endpoint.closed)
                    finally:
                        _native._test_sender_attach_result(0)
