"""Raw sender LOC marshalling and ownership, not native wire qualification."""

import dataclasses
import gc
import inspect
import threading
from unittest import mock

import moq5
from moq5 import _native

import test_send_write as writes


FIELDS = ("timestamp", "timescale", "marking", "audio_level", "video_config", "audio_config")
BITS = {name: _native.constants["LOC_METADATA_HAS_" + name.upper()] for name in FIELDS}
MAX = 2**64 - 1


def loc(**fields):
    values = dict.fromkeys(FIELDS)
    values.update(fields)
    present = sum(BITS[name] for name in FIELDS if values[name] is not None)
    return moq5.LocMetadata(present=present, **values)


def packed(value):
    if not isinstance(value, moq5.LocMetadata):
        return value
    return (value.present,) + tuple(getattr(value, field) for field in FIELDS)


def invalid_metadata():
    cases = [("wrong value type", object(), TypeError),
             ("derived metadata", moq5.MediaMetadata(moq5.MediaTimeDomain.NONE, None, None, None), TypeError),
             ("unknown bit", dataclasses.replace(loc(), present=0x40), ValueError),
             ("negative mask", dataclasses.replace(loc(), present=-1), ValueError),
             ("wide mask", dataclasses.replace(loc(), present=2**32), ValueError),
             ("bool mask", dataclasses.replace(loc(), present=True), TypeError)]
    for field, value in zip(FIELDS, (0, 0, b"m", 0, b"", b"")):
        cases.append((field + " without bit", dataclasses.replace(loc(**{field: value}), present=0), ValueError))
        cases.append((field + " without value", dataclasses.replace(loc(), present=BITS[field]), ValueError))
    for field, maximum in (("timestamp", MAX), ("timescale", MAX), ("audio_level", 255)):
        for value, error in ((-1, ValueError), (maximum + 1, ValueError), (True, TypeError), (1.0, TypeError)):
            cases.append((f"{field}={value!r}", loc(**{field: value}), error))
    for field in ("marking", "video_config", "audio_config"):
        cases.append((field + " mutable", loc(**{field: bytearray(b"x")}), TypeError))
    cases.extend((("text config", loc(video_config="x"), TypeError),
                  ("buffer config", loc(audio_config=memoryview(b"x")), TypeError),
                  ("empty marking", loc(marking=b""), ValueError),
                  ("wide marking", loc(marking=b"12345"), ValueError)))
    return cases


class SendMetadataTests(writes.WriteGatedBase):
    def attach(self, endpoint):
        return moq5.Sender.attach(endpoint, moq5.SenderConfig.live(
            writes.NS, companion_profile=moq5.CompanionProfile.D22_MSF01_LOC04_PUBLIC))

    def native_args(self, sender, track):
        return (sender._Sender__handle, track._SendTrack__handle,
                b"frame", None, False, False, False, 0, 0, None, None)

    def assert_metadata(self, raw):
        image = _native._test_write_object()
        self.assertFalse(image["oracle_limit"], image["oracle_limit_reason"])
        observed = image["metadata"]
        self.assertIsNotNone(observed, "explicit LOC metadata reached C as NULL")
        self.assertTrue(observed["full_size"])
        self.assertTrue(observed["loc_full_size"])
        self.assertTrue(observed["derived_absent"])
        self.assertTrue(observed["loc_reserved_zero"])
        self.assertEqual(observed["present"], raw.present)
        for field in FIELDS:
            value = getattr(raw, field)
            empty = b"" if field in ("marking", "video_config", "audio_config") else 0
            self.assertEqual(observed[field], empty if value is None else value, field)
        for field in ("video_config", "audio_config"):
            self.assertIs(observed[field + "_null"], getattr(raw, field) is None)

    def test_explicit_raw_metadata_reaches_the_c_recorder(self):
        raw = loc(timestamp=MAX, timescale=1_000_000, marking=b"\x00\xff\x80\x00",
                  audio_level=255, video_config=b"v\x00\xff", audio_config=b"")
        with self.connect() as endpoint, self.attach(endpoint) as sender:
            outcome = sender.write(self.track(sender), self.obj(loc_metadata=raw))
            image = _native._test_write_object()
            self.assertIs(outcome, moq5.WriteOutcome.ACCEPTED)
            self.assertIsNotNone(image["metadata"], "explicit LOC metadata reached C as NULL")
            self.assertFalse(image["oracle_limit"])
            for field in ("present",) + FIELDS:
                self.assertEqual(image["metadata"][field], getattr(raw, field), field)
            self.assert_metadata(raw)

    def test_codec_string_reaches_c_as_its_exact_utf8_bytes(self):
        with self.connect() as endpoint, self.attach(endpoint) as sender:
            track = self.track(sender)
            for text in ("avc1.640028", " opus\t", "h\u00e9v1", "\U0010ffff"):
                with self.subTest(text=text):
                    self.assertIs(sender.write(track, self.obj(codec_string=text)),
                                  moq5.WriteOutcome.ACCEPTED)
                    self.assertEqual(_native._test_write_object()["codec_string"],
                                     text.encode("utf-8"))
            self.assertIs(sender.write(track, self.obj()), moq5.WriteOutcome.ACCEPTED)
            self.assertIsNone(_native._test_write_object()["codec_string"])

    def test_codec_string_refusals_precede_native_entry(self):
        for value, error in (("\ud800", ValueError), (b"opus", TypeError), (7, TypeError)):
            with self.subTest(value=value):
                with self.assertRaises(error):
                    self.obj(codec_string=value)

    def test_individual_bits_zero_max_and_marking_lengths_are_lossless(self):
        cases = [loc(), loc(timestamp=0), loc(timestamp=MAX), loc(timescale=0),
                 loc(timescale=MAX), loc(timescale=1_000_000), loc(audio_level=0),
                 loc(audio_level=255), loc(video_config=b""), loc(video_config=b"v\x00\xff"),
                 loc(audio_config=b""), loc(audio_config=b"a\x00\xff")]
        cases += [loc(marking=b"\x00\xff\x80\x00"[:size]) for size in range(1, 5)]
        with self.connect() as endpoint, self.attach(endpoint) as sender:
            track = self.track(sender)
            for raw in cases:
                with self.subTest(raw=raw):
                    self.assertIs(sender.write(track, self.obj(loc_metadata=raw)), moq5.WriteOutcome.ACCEPTED)
                    self.assert_metadata(raw)

    def test_default_none_and_old_native_arity_have_no_metadata(self):
        with self.connect() as endpoint, self.attach(endpoint) as sender:
            track = self.track(sender)
            for obj in (self.obj(), self.obj(loc_metadata=None)):
                self.assertIs(sender.write(track, obj), moq5.WriteOutcome.ACCEPTED)
                self.assertIsNone(_native._test_write_object()["metadata"])
            args = self.native_args(sender, track)
            for tail in ((), (None,)):
                self.assertEqual(_native.sender_write(*args, *tail), 0)
                self.assertIsNone(_native._test_write_object()["metadata"])
            # A present zero-mask record is not a missing metadata pointer.
            self.assertEqual(_native.sender_write(*args, packed(loc())), 0)
            self.assert_metadata(loc())

    def test_other_send_object_fields_are_unchanged(self):
        fields = dict(payload=b"\x00frame\xff", properties=b"\x00props\xff",
                      is_sync=True, starts_group=True, ends_group=True,
                      decode_time_us=MAX, presentation_time_us=37, capture_time_us=0,
                      sap_type=moq5.SapType.TYPE_2)
        snapshots = []
        with self.connect() as endpoint, self.attach(endpoint) as sender:
            track = self.track(sender, packaging=moq5.Packaging.CMAF)
            for raw in (None, loc(timestamp=MAX, timescale=0)):
                self.assertIs(sender.write(track, self.obj(**fields, loc_metadata=raw)),
                              moq5.WriteOutcome.ACCEPTED)
                image = _native._test_write_object()
                image.pop("metadata")
                snapshots.append(image)
            for field in ("payload", "properties", "is_sync", "starts_group", "ends_group",
                          "decode_time_us", "presentation_time_us", "capture_time_us", "sap_type"):
                self.assertEqual(snapshots[1][field], fields[field], field)
        self.assertEqual(snapshots[0], snapshots[1])

    def test_additive_field_is_keyword_only_frozen_and_retains_the_raw_value(self):
        parameter = inspect.signature(moq5.SendObject).parameters["loc_metadata"]
        self.assertIs(parameter.kind, inspect.Parameter.KEYWORD_ONLY)
        self.assertIsNone(parameter.default)
        raw = loc(timestamp=0)
        obj = self.obj(loc_metadata=raw)
        self.assertIs(obj.loc_metadata, raw)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            obj.loc_metadata = None

    def test_received_loc_constructor_is_unchanged(self):
        raw = moq5.LocMetadata(0x80000000, None, None, None, None, None, None)
        self.assertEqual(raw.present, 0x80000000)
        self.assertEqual(tuple(field.name for field in dataclasses.fields(raw)), ("present",) + FIELDS)
        with self.assertRaises(ValueError):
            self.obj(loc_metadata=raw)

    def test_public_structural_refusals_precede_all_buffer_effects(self):
        with self.connect() as endpoint, self.attach(endpoint) as sender:
            track = self.track(sender)
            before = _native._test_rcbuf_counts()
            for label, raw, error in invalid_metadata():
                with self.subTest(case=label), self.assertRaises(error):
                    sender.write(track, self.obj(loc_metadata=raw))
                self.assertEqual(_native._test_rcbuf_counts(), before)
                self.assertEqual(_native._test_write_counts()["entries"], 0)

    def test_outbound_validation_is_repeated_when_a_carrier_was_bypassed(self):
        with self.connect() as endpoint, self.attach(endpoint) as sender:
            track = self.track(sender)
            obj = self.obj()
            before = _native._test_rcbuf_counts()
            object.__setattr__(obj, "loc_metadata", loc(timestamp=True))
            with self.assertRaisesRegex(TypeError, "loc_metadata.timestamp"):
                sender.write(track, obj)
            self.assertEqual(_native._test_rcbuf_counts(), before)
            self.assertEqual(_native._test_write_counts()["entries"], 0)

    def test_private_structural_refusals_precede_all_buffer_effects(self):
        with self.connect() as endpoint, self.attach(endpoint) as sender:
            args = self.native_args(sender, self.track(sender))
            before = _native._test_rcbuf_counts()
            _native._test_fail_allocation_at("send_object_payload")
            for label, raw, error in invalid_metadata():
                with self.subTest(case=label), self.assertRaisesRegex(error, "loc_metadata"):
                    _native.sender_write(*args, packed(raw))
                self.assertEqual(_native._test_rcbuf_counts(), before)
                self.assertEqual(_native._test_write_counts()["entries"], 0)
            for tail in ((), (0,) * 6, (0,) * 8, list(packed(loc()))):
                with self.subTest(tail=tail), self.assertRaisesRegex(TypeError, "loc_metadata"):
                    _native.sender_write(*args, tail)
                self.assertEqual(_native._test_rcbuf_counts(), before)
            _native._test_fail_allocation_at(None)
            self.assertEqual(_native.sender_write(*args, packed(loc(timestamp=0))), 0)
            self.assert_metadata(loc(timestamp=0))

    def test_semantic_values_reach_native_and_its_refusal_is_preserved(self):
        cases = [(loc(), "ERR_INVAL"), (loc(timestamp=0, timescale=0), "ERR_INVAL"),
                 (loc(timestamp=MAX, timescale=1), "ERR_INVAL"),
                 (loc(timestamp=0), "ERR_UNSUPPORTED")]
        with self.connect() as endpoint, self.attach(endpoint) as sender:
            track = self.track(sender)
            for index, (raw, result) in enumerate(cases, start=1):
                with self.subTest(raw=raw, result=result):
                    before = _native._test_rcbuf_counts()
                    code = _native.constants[result]
                    _native._test_write_result(code)
                    with self.assertRaises(moq5.MoqError) as raised:
                        sender.write(track, self.obj(presentation_time_us=91, loc_metadata=raw))
                    self.assertEqual((raised.exception.code, raised.exception.operation), (code, "write"))
                    self.assert_metadata(raw)
                    self.assertEqual(_native._test_write_counts()["entries"], index)
                    after = _native._test_rcbuf_counts()
                    self.assertEqual(after["allocations"] - before["allocations"], 1)
                    self.assertEqual(after["frees"] - before["frees"], 1)
                    self.assertEqual(after["outstanding"], before["outstanding"])

    def test_metadata_does_not_change_payload_properties_transfer(self):
        raw = loc(timestamp=MAX, video_config=b"v\x00", audio_config=b"")
        with self.connect() as endpoint, self.attach(endpoint) as sender:
            track = self.track(sender, packaging=moq5.Packaging.CMAF)
            for code in (0, writes.WOULD_BLOCK, writes.INTERRUPTED, writes.CLOSED,
                         writes.INVAL, _native.constants["ERR_UNSUPPORTED"]):
                with self.subTest(code=code):
                    _native._test_write_reset()
                    before = _native._test_rcbuf_counts()
                    _native._test_write_result(code)
                    obj = self.obj(properties=b"\x00props", loc_metadata=raw)
                    if code in (0, writes.WOULD_BLOCK, writes.INTERRUPTED, writes.CLOSED):
                        self.assertEqual(sender.write(track, obj), code)
                    else:
                        with self.assertRaises(moq5.MoqError) as raised:
                            sender.write(track, obj)
                        self.assertEqual((raised.exception.code, raised.exception.operation), (code, "write"))
                    self.assert_metadata(raw)
                    counts = _native._test_rcbuf_counts()
                    self.assertEqual(counts["allocations"] - before["allocations"], 2)
                    self.assertEqual(counts["frees"] - before["frees"], 0 if code == 0 else 2)
                    self.assertEqual(_native._test_write_counts()["accepted"], int(code == 0))
                    _native._test_write_reset()
                    self.assertFalse(_native._test_write_settlement()["premature_release"])
                    self.assertEqual(_native._test_rcbuf_counts()["outstanding"], before["outstanding"])
                    self.assertIs(obj.loc_metadata, raw)

    def test_buffer_allocation_failures_still_settle_before_native_entry(self):
        with self.connect() as endpoint, self.attach(endpoint) as sender:
            track = self.track(sender)
            for site, allocations in (("send_object_payload", 0), ("send_object_properties", 1)):
                with self.subTest(site=site):
                    before = _native._test_rcbuf_counts()
                    _native._test_fail_allocation_at(site)
                    with self.assertRaises(MemoryError):
                        sender.write(track, self.obj(properties=b"props", loc_metadata=loc(timestamp=0)))
                    after = _native._test_rcbuf_counts()
                    self.assertEqual(after["allocations"] - before["allocations"], allocations)
                    self.assertEqual(after["frees"] - before["frees"], allocations)
                    self.assertEqual(after["outstanding"], before["outstanding"])
                    self.assertEqual(_native._test_write_counts()["entries"], 0)

    def test_raw_bytes_survive_the_gil_released_blocked_call(self):
        obj = self.obj(loc_metadata=loc(
            timestamp=MAX, marking=b"\x00\xff\x80\x00",
            video_config=bytes(bytearray(b"video\x00\xff" * 8)),
            audio_config=bytes(bytearray(b"audio\x00\xfe" * 8))))
        progressed = []
        released = []

        def watcher():
            try:
                progressed.append(_native._test_write_wait_entered(5.0))
                # Remove the carrier's references deliberately, bypassing its
                # frozen API. Only the call's immutable snapshot is needed now.
                object.__setattr__(obj, "loc_metadata", None)
                gc.collect()
                scratch = [bytes([index]) * 56 for index in range(64)]
                progressed.append(len(scratch) == 64)
            finally:
                released.append(_native._test_write_release())

        with self.connect() as endpoint, self.attach(endpoint) as sender:
            track = self.track(sender)
            helper = threading.Thread(target=watcher, daemon=True)
            _native._test_write_block(True)
            helper.start()
            try:
                self.assertIs(sender.write(track, obj), moq5.WriteOutcome.ACCEPTED)
            finally:
                _native._test_write_block(False)
                _native._test_write_release()
                helper.join(10.0)
            self.assertFalse(helper.is_alive())
            self.assertEqual(progressed, [True, True])
            self.assertEqual(released, [True])
            self.assertFalse(_native._test_write_counts()["gate_fault"])
            self.assert_metadata(loc(timestamp=MAX, marking=b"\x00\xff\x80\x00",
                                     video_config=b"video\x00\xff" * 8, audio_config=b"audio\x00\xfe" * 8))

    def test_metadata_argument_exception_stays_before_native_effects(self):
        with self.connect() as endpoint, self.attach(endpoint) as sender:
            track = self.track(sender)
            obj = self.obj(loc_metadata=loc(timestamp=0))
            before = _native._test_rcbuf_counts()
            raiser = property(lambda self: (_ for _ in ()).throw(KeyboardInterrupt()))
            with mock.patch.object(moq5.LocMetadata, "video_config", raiser):
                with self.assertRaises(KeyboardInterrupt):
                    sender.write(track, obj)
            self.assertEqual(_native._test_rcbuf_counts(), before)
            self.assertEqual(_native._test_write_counts()["entries"], 0)
