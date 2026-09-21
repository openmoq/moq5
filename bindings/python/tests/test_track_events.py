"""Track events and owned description copies, against the same bridge with the
test-only C provider. Every ABI boundary used here is the compiled one the
fixture reports, never a host-layout literal."""

import gc
import typing
import unittest
from unittest import mock

import moq5
from moq5 import _native, _receiver

NS = (b"live",)

FULL = {
    "name": b"video\x00main", "role": b"video", "codec": b"avc1.64001f", "lang": b"en",
    "label": b"Main camera", "packaging_text": b"cmaf", "event_type": None,
    "mime_type": b"video/mp4", "channel_config": None, "init_data": b"\x00\x00\x00\x08ftyp",
    "depends": (b"audio", b"cap\x00tions"), "content_protection_ref_ids": (b"kid-1",),
    "media_type": 1, "packaging": 2, "timescale": 90000, "transport_version": 16,
    "init": {"codec_kind": 1, "timescale": 90000, "width": 1920, "height": 1080, "samplerate": 0,
             "channel_count": 0, "codec_config": b"\x01\x64\x00\x1f\xff", "track_id": 1,
             "cenc": (b"cbcs", 1, 0, bytes(range(16)))},
    "width": 1920, "height": 1080, "samplerate": None, "framerate_millis": 29970,
    "bitrate": 2**64 - 1, "max_grp_sap": 1, "max_obj_sap": 2,
    "template": (1, 2, 3, 4, 5, 6, 2**64 - 1, 0),
    "is_live": True, "track_duration_ms": None,
}

FULL_EXPECTED = moq5.TrackDescription(
    name=b"video\x00main", role=b"video", codec=b"avc1.64001f", lang=b"en", label=b"Main camera",
    media_type=moq5.MediaType.VIDEO, packaging=moq5.Packaging.CMAF, timescale=90000, transport_version=16,
    init=moq5.CmafInit(moq5.CodecKind.AVC, 90000, 1920, 1080, 0, 0, b"\x01\x64\x00\x1f\xff", 1,
                       moq5.Cenc(b"cbcs", 1, 0, bytes(range(16)))),
    init_data=b"\x00\x00\x00\x08ftyp", width=1920, height=1080, samplerate=None, channel_config=None,
    framerate_millis=29970, bitrate=2**64 - 1, max_grp_sap=1, max_obj_sap=2,
    content_protection_ref_ids=(b"kid-1",), packaging_text=b"cmaf", event_type=None,
    mime_type=b"video/mp4", depends=(b"audio", b"cap\x00tions"),
    template=moq5.MediaTemplate(1, 2, 3, 4, 5, 6, 2**64 - 1, 0),
    vod=moq5.VodState(True, None),
)

K = moq5.TrackEventKind


class TrackEventTests(unittest.TestCase):
    def setUp(self):
        gc.collect()
        _native._test_reset()
        self.layout = _native._test_layout()
        self.endpoint = moq5.Endpoint.connect(moq5.EndpointConfig(url="moqt://fixture.invalid"))
        self.receiver = moq5.Receiver.attach(self.endpoint, moq5.ReceiverConfig.live(NS))
        self.handle = self.receiver._Receiver__handle

    def tearDown(self):
        self.receiver.close()
        self.endpoint.close()

    def track(self, desc=FULL):
        capsule = _native._test_new_track(self.handle)
        _native._test_track_desc(capsule, dict(desc))
        return capsule

    def push(self, kind, capsule, *, stamp=None, largest=None, expires=None, parse_drop=None):
        _native._test_push_event(int(kind), capsule, stamp, largest, expires, parse_drop)

    def push_orphan_desc(self, kind):
        _native._test_push_orphan_desc(int(kind))

    def cache_size(self):
        return len(self.receiver._Receiver__tracks)

    # ---- 1. every known kind -------------------------------------------------

    def test_all_seven_kinds_with_exact_values_identity_and_counts(self):
        a = self.track()
        self.push(K.ADDED, a)
        self.push(K.UPDATED, a)
        self.push(K.REMOVED, a)
        self.push(K.ENDED, a)
        self.push(K.CATALOG_READY, None)
        self.push(K.UPDATE_OK, a, largest=(2**64 - 1, 7), expires=2**64 - 2)
        self.push(K.PARSE_DROP, a, parse_drop=(2, 10, 3))
        events = [self.receiver.poll_track() for _ in range(7)]
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY)
        kinds = [e.kind for e in events]
        self.assertEqual(kinds, [K.ADDED, K.UPDATED, K.REMOVED, K.ENDED, K.CATALOG_READY, K.UPDATE_OK, K.PARSE_DROP])
        track = events[0].track
        self.assertIsInstance(track, moq5.Track)
        for e in events:
            if e.kind is K.CATALOG_READY:
                self.assertIsNone(e.track)
                self.assertIsNone(e.description)
            else:
                self.assertIs(e.track, track, "one native handle, one Track object")
                self.assertEqual(e.description, FULL_EXPECTED)
            if e.kind is not K.UPDATE_OK:
                self.assertIsNone(e.largest)
                self.assertIsNone(e.expires_ms)
            if e.kind is not K.PARSE_DROP:
                self.assertIsNone(e.parse_drop)
        self.assertEqual(events[5].largest, (2**64 - 1, 7))
        self.assertEqual(events[5].expires_ms, 2**64 - 2)
        self.assertEqual(events[6].parse_drop, moq5.ParseDrop(moq5.ParseDropClass.SAP, 10, 3))
        self.assertEqual(self.cache_size(), 1)
        self.assertIs(track.description, events[6].description)

    def test_unknown_kind_is_preserved_without_payload_interpretation(self):
        a = self.track()
        self.push(99, a, largest=(1, 2), expires=3, parse_drop=(1, 4, 5))
        event = self.receiver.poll_track()
        self.assertEqual(event.kind, 99)
        self.assertNotIsInstance(event.kind, K)
        self.assertEqual(event.description, FULL_EXPECTED)
        self.assertIsNone(event.largest)
        self.assertIsNone(event.expires_ms)
        self.assertIsNone(event.parse_drop)

    def test_event_shapes_are_classified_by_known_kind_not_by_handle(self):
        a = self.track()
        # unknown kind without a handle: preserved, nothing inferred
        self.push(99, None)
        event = self.receiver.poll_track()
        self.assertEqual((event.kind, event.track, event.description), (99, None, None))
        self.assertEqual((event.largest, event.expires_ms, event.parse_drop), (None, None, None))
        # CATALOG_READY with a handle is a malformed known shape: lost, no cache effect
        self.push(K.CATALOG_READY, a)
        with self.assertRaises(moq5.EventLost) as caught:
            self.receiver.poll_track()
        self.assertEqual((caught.exception.kind, caught.exception.stage), (K.CATALOG_READY, "track"))
        self.assertEqual(self.cache_size(), 0)
        # every known track-scoped kind without a handle is lost, naming its kind
        for kind in (K.ADDED, K.UPDATED, K.REMOVED, K.ENDED, K.UPDATE_OK, K.PARSE_DROP):
            with self.subTest(kind=kind):
                self.push(kind, None)
                with self.assertRaises(moq5.EventLost) as caught:
                    self.receiver.poll_track()
                self.assertEqual((caught.exception.kind, caught.exception.stage), (kind, "track"))
        # CATALOG_READY with a descriptor but no handle is the other malformed
        # shape: lost with the known kind, the descriptor pointer never read
        self.push_orphan_desc(K.CATALOG_READY)
        with self.assertRaises(moq5.EventLost) as caught:
            self.receiver.poll_track()
        self.assertEqual((caught.exception.kind, caught.exception.stage), (K.CATALOG_READY, "description"))
        self.assertEqual(self.cache_size(), 0)
        # an unknown kind with an orphan descriptor stays preserved and unread
        self.push_orphan_desc(99)
        event = self.receiver.poll_track()
        self.assertEqual((event.kind, event.track, event.description), (99, None, None))
        # same-sequence recovery: the following well-formed events are delivered
        self.push(K.CATALOG_READY, None)
        self.push(K.ADDED, a)
        ready, added = self.receiver.poll_track(), self.receiver.poll_track()
        self.assertEqual((ready.kind, added.kind), (K.CATALOG_READY, K.ADDED))
        self.assertEqual(added.description, FULL_EXPECTED)
        self.assertEqual(self.cache_size(), 1, "held by the delivered event's Track reference")
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY)

    # ---- 2. outcomes ---------------------------------------------------------

    def test_precedence_terminal_drain_empty_closed_and_latch_ignored(self):
        a = self.track()
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY)
        self.push(K.ADDED, a)
        self.push(K.ENDED, a)
        self.endpoint.set_interrupted(True)              # poll_track never consults the latch
        _native._test_receiver_state(True, False, 0)      # terminal: queued events still drain
        self.assertIs(self.receiver.poll_track().kind, K.ADDED)
        self.assertIs(self.receiver.poll_track().kind, K.ENDED)
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.CLOSED)
        _native._test_receiver_state(False, False, 0)
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY)
        for code in (-99, -13, -2):
            with self.subTest(code=code):
                _native._test_poll_track_result(code)
                with self.assertRaises(moq5.MoqError) as caught:
                    self.receiver.poll_track()
                self.assertEqual((caught.exception.code, caught.exception.operation), (code, "poll_track"))
        _native._test_poll_track_result(0)
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY)

    # ---- 3. identity and ownership -------------------------------------------

    def test_same_handle_reuses_track_and_equal_descriptions_stay_distinct(self):
        a, b = self.track(), self.track()
        self.push(K.ADDED, a)
        self.push(K.ADDED, b)
        self.push(K.UPDATED, a)
        first, second, third = (self.receiver.poll_track() for _ in range(3))
        self.assertIs(first.track, third.track)
        self.assertIsNot(first.track, second.track)
        self.assertNotEqual(first.track, second.track, "equal values, different handles")
        self.assertEqual(first.description, second.description)
        self.assertEqual(self.cache_size(), 2)
        weak_first = first.track
        del first, third
        gc.collect()
        self.assertEqual(self.cache_size(), 2, "a live reference keeps the identity")
        del weak_first
        gc.collect()
        self.assertEqual(self.cache_size(), 1, "an unreferenced Track leaves the weak cache")

    def test_owned_copies_survive_backing_mutation_and_receiver_close(self):
        a = self.track()
        self.push(K.ADDED, a)
        event = self.receiver.poll_track()
        description = event.description
        _native._test_scramble_track(a)
        self.assertEqual(description, FULL_EXPECTED, "bytes were copied, not borrowed")
        self.assertEqual(event.track.description, FULL_EXPECTED)
        self.receiver.close()
        self.assertEqual(description, FULL_EXPECTED)
        self.assertEqual(description.depends[1], b"cap\x00tions")
        self.assertEqual(description.init.cenc.default_kid, bytes(range(16)))
        self.endpoint.close()

    # ---- 4. current vs borrowed ----------------------------------------------

    def test_vod_triple_comes_from_the_current_copy_not_the_borrowed_event(self):
        a = self.track()
        _native._test_track_current(a, False, 60000)     # borrowed view still says live
        self.push(K.ADDED, a)
        event = self.receiver.poll_track()
        self.assertEqual(event.description.vod, moq5.VodState(False, 60000))
        self.assertEqual(event.track.description.vod, moq5.VodState(False, 60000))

    def test_snapshot_replaced_transactionally_and_old_copies_immutable(self):
        a = self.track()
        self.push(K.ADDED, a)
        first = self.receiver.poll_track()
        old = first.description
        self.assertIs(first.track.description, old)
        _native._test_track_current(a, False, 42)
        self.push(K.UPDATED, a)
        second = self.receiver.poll_track()
        self.assertIs(second.track, first.track)
        self.assertIs(first.track.description, second.description)
        self.assertEqual(second.description.vod, moq5.VodState(False, 42))
        self.assertEqual(old.vod, moq5.VodState(True, None), "the earlier copy is untouched")
        self.assertIs(first.description, old)
        with self.assertRaises(AttributeError):
            old.vod = None

    # ---- 5. prefix gating with poisoned tails ---------------------------------

    def test_event_prefix_gating_at_every_field_boundary(self):
        a = self.track()
        L = self.layout
        rows = [
            (L["event_v0"], (None, None)),
            (L["event_has_largest_end"], (None, None)),          # has_largest fits, its value does not
            (L["event_largest_object_end"], ((5, 6), None)),
            (L["event_has_expires_end"], ((5, 6), None)),
            (L["event_expires_end"], ((5, 6), 7)),
            (L["event_sizeof"], ((5, 6), 7)),
        ]
        for stamp, (largest, expires) in rows:
            with self.subTest(stamp=stamp):
                self.push(K.UPDATE_OK, a, stamp=stamp, largest=(5, 6), expires=7)
                event = self.receiver.poll_track()
                self.assertEqual((event.largest, event.expires_ms), (largest, expires))
                self.assertEqual(event.description, FULL_EXPECTED, "the v0 part is complete at every stamp")
        drops = [
            (L["event_expires_end"], None),
            (L["event_parse_drop_class_end"], moq5.ParseDrop(moq5.ParseDropClass.MEDIA, None, None)),
            (L["event_parse_drops_total_end"], moq5.ParseDrop(moq5.ParseDropClass.MEDIA, 10, None)),
            (L["event_parse_drops_delta_end"], moq5.ParseDrop(moq5.ParseDropClass.MEDIA, 10, 3)),
        ]
        for stamp, expected in drops:
            with self.subTest(stamp=stamp):
                self.push(K.PARSE_DROP, a, stamp=stamp, parse_drop=(1, 10, 3))
                self.assertEqual(self.receiver.poll_track().parse_drop, expected)
        for stamp in (L["event_v0"] - 1, L["event_sizeof"] + 1):
            with self.subTest(stamp=stamp):
                self.push(K.ADDED, a, stamp=stamp)
                with self.assertRaises(moq5.EventLost) as caught:
                    self.receiver.poll_track()
                self.assertEqual((caught.exception.kind, caught.exception.stage), (None, "event.struct_size"))
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY)

    def test_description_and_nested_prefix_gating(self):
        L = self.layout
        # info: v0 stamp drops transport_version only
        a = self.track({**FULL, "info_stamp": L["info_v0"]})
        self.push(K.ADDED, a)
        d = self.receiver.poll_track().description
        self.assertIsNone(d.transport_version)
        self.assertEqual((d.media_type, d.packaging, d.timescale), (moq5.MediaType.VIDEO, moq5.Packaging.CMAF, 90000))
        # init: stamps between fields
        cases = [
            (L["init_width_end"], dict(width=1920, height=None, samplerate=None, channel_count=None,
                                       codec_config=None, track_id=None, cenc=None)),
            (L["init_codec_config_end"], dict(width=1920, height=1080, samplerate=0, channel_count=0,
                                              codec_config=b"\x01\x64\x00\x1f\xff", track_id=None, cenc=None)),
            (L["init_has_cenc_end"] - 1, dict(width=1920, height=1080, samplerate=0, channel_count=0,
                                              codec_config=b"\x01\x64\x00\x1f\xff", track_id=1, cenc=None)),
            # has_cenc fits and is true: CENC is PRESENT, each field on its own end
            (L["init_has_cenc_end"], dict(track_id=1, cenc=moq5.Cenc(None, None, None, None))),
            (L["init_scheme_end"], dict(cenc=moq5.Cenc(b"cbcs", None, None, None))),
            (L["init_default_is_protected_end"], dict(cenc=moq5.Cenc(b"cbcs", 1, None, None))),
            (L["init_default_per_sample_iv_size_end"], dict(cenc=moq5.Cenc(b"cbcs", 1, 0, None))),
            (L["init_default_kid_end"], dict(cenc=moq5.Cenc(b"cbcs", 1, 0, bytes(range(16))))),
            (L["init_sizeof"], dict(cenc=moq5.Cenc(b"cbcs", 1, 0, bytes(range(16))))),
        ]
        for stamp, expect in cases:
            with self.subTest(init_stamp=stamp):
                b = self.track({**FULL, "init_stamp": stamp})
                self.push(K.ADDED, b)
                init = self.receiver.poll_track().description.init
                self.assertEqual(init.codec_kind, moq5.CodecKind.AVC)
                for key, value in expect.items():
                    self.assertEqual(getattr(init, key), value, key)
        # a present ZERO is distinct from absence: iv size 0 at its end vs None below it
        zero = {**FULL, "init": {**FULL["init"], "cenc": (b"cenc", 0, 0, bytes(16))}}
        for stamp, expect in ((L["init_default_per_sample_iv_size_end"] - 1, moq5.Cenc(b"cenc", 0, None, None)),
                              (L["init_default_per_sample_iv_size_end"], moq5.Cenc(b"cenc", 0, 0, None)),
                              (L["init_default_kid_end"], moq5.Cenc(b"cenc", 0, 0, bytes(16)))):
            with self.subTest(zero_stamp=stamp):
                z = self.track({**zero, "init_stamp": stamp})
                self.push(K.ADDED, z)
                self.assertEqual(self.receiver.poll_track().description.init.cenc, expect)
        # has_cenc false: absent regardless of the stamp
        nocenc = self.track({**FULL, "init": {**FULL["init"], "cenc": None}})
        self.push(K.ADDED, nocenc)
        self.assertIsNone(self.receiver.poll_track().description.init.cenc)
        # absent optional fields are None, never zero
        c = self.track({"name": b"bare", "packaging_text": b"loc", "is_live": False})
        self.push(K.ADDED, c)
        d = self.receiver.poll_track().description
        self.assertEqual(d.name, b"bare")
        for key in ("role", "codec", "lang", "label", "media_type", "packaging", "timescale", "transport_version",
                    "init", "width", "height", "samplerate", "channel_config", "framerate_millis", "bitrate",
                    "max_grp_sap", "max_obj_sap", "event_type", "mime_type", "template"):
            self.assertIsNone(getattr(d, key), key)
        self.assertEqual((d.init_data, d.depends, d.content_protection_ref_ids), (b"", (), ()))
        self.assertEqual(d.vod, moq5.VodState(False, None))
        # invalid stamps are binding faults naming the boundary
        for stamp_key, value, stage in (("desc_stamp", L["desc_v0"] - 1, "description.struct_size"),
                                        ("desc_stamp", L["desc_sizeof"] + 1, "description.struct_size"),
                                        ("info_stamp", L["info_v0"] - 1, "description.info.struct_size"),
                                        ("info_stamp", L["info_sizeof"] + 1, "description.info.struct_size"),
                                        ("init_stamp", 0, "description.init.struct_size"),
                                        ("init_stamp", L["init_sizeof"] + 1, "description.init.struct_size")):
            with self.subTest(stamp_key=stamp_key, value=value):
                e = self.track({**FULL, stamp_key: value})
                self.push(K.ADDED, e)
                with self.assertRaises(moq5.EventLost) as caught:
                    self.receiver.poll_track()
                self.assertEqual((caught.exception.kind, caught.exception.stage), (K.ADDED, stage))
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY)

    def test_invalid_spans_counts_and_copy_refusal_are_event_lost(self):
        for which, stage in (("name", "description.name"), ("depends", "description.depends"),
                             ("content_protection_ref_ids", "description.content_protection_ref_ids"),
                             ("codec_config", "description.init.codec_config"), ("init_data", "description.init_data")):
            with self.subTest(which=which):
                a = self.track({**FULL, "null_span": which})
                self.push(K.UPDATED, a)
                with self.assertRaises(moq5.EventLost) as caught:
                    self.receiver.poll_track()
                self.assertEqual((caught.exception.kind, caught.exception.stage), (K.UPDATED, stage))
                self.assertNotIsInstance(caught.exception, moq5.MoqError)
                self.assertNotIn("0x", str(caught.exception))
        a = self.track()
        _native._test_desc_copy_result(-2)
        self.push(K.ADDED, a)
        with self.assertRaises(moq5.EventLost) as caught:
            self.receiver.poll_track()
        self.assertEqual(caught.exception.stage, "description.copy")
        _native._test_desc_copy_result(0)
        self.push(K.ADDED, None)                          # a track-bearing kind without a handle
        with self.assertRaises(moq5.EventLost) as caught:
            self.receiver.poll_track()
        self.assertEqual((caught.exception.kind, caught.exception.stage), (K.ADDED, "track"))
        self.assertEqual(self.cache_size(), 0, "no Track was published for any lost event")
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY)

    def test_unrepresentable_counts_and_lengths_are_event_lost_not_memory_error(self):
        good = self.track()
        self.push(K.ADDED, good)
        first = self.receiver.poll_track()
        cases = [({"oversized_count": "depends"}, "description.depends"),
                 ({"oversized_count": "content_protection_ref_ids"}, "description.content_protection_ref_ids"),
                 ({"oversized_len": "name"}, "description.name"),
                 ({"oversized_len": "codec_config"}, "description.init.codec_config"),
                 ({"oversized_len": "depends"}, "description.depends")]
        for knob, stage in cases:
            with self.subTest(**knob):
                bad = self.track({**FULL, **knob})
                self.push(K.ADDED, bad)
                self.push(K.UPDATED, good)
                with self.assertRaises(moq5.EventLost) as caught:
                    self.receiver.poll_track()
                self.assertEqual((caught.exception.kind, caught.exception.stage), (K.ADDED, stage))
                self.assertNotIsInstance(caught.exception, MemoryError)
                following = self.receiver.poll_track()
                self.assertIs(following.track, first.track, "the following event is delivered")
                self.assertEqual(following.description, FULL_EXPECTED)
        self.assertEqual(self.cache_size(), 1, "no Track was published for any impossible description")
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY)

    # ---- 6. a failed second event -----------------------------------------------

    def test_failed_second_event_is_reported_once_third_available_nothing_half_published(self):
        a = self.track()
        bad = self.track({**FULL, "name": b"bad", "null_span": "depends"})
        c = self.track({**FULL, "name": b"third"})
        self.push(K.ADDED, a)
        self.push(K.ADDED, bad)
        self.push(K.ADDED, c)
        first = self.receiver.poll_track()
        with self.assertRaises(moq5.EventLost):
            self.receiver.poll_track()
        third = self.receiver.poll_track()
        self.assertEqual(third.description.name, b"third")
        self.assertEqual(self.cache_size(), 2, "the lost event published no Track")
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY)
        # an existing track's snapshot is not half-updated by a failed UPDATED
        _native._test_track_current(a, False, 9)
        _native._test_desc_copy_result(-14)
        self.push(K.UPDATED, a)
        with self.assertRaises(moq5.EventLost):
            self.receiver.poll_track()
        self.assertIs(first.track.description, first.description)
        self.assertEqual(first.track.description.vod, moq5.VodState(True, None))
        _native._test_desc_copy_result(0)

    def test_allocation_failure_propagates_bare_memory_error(self):
        # A conversion-stage allocation failure is the original MemoryError:
        # not EventLost, not MoqError; the event is consumed, nothing published.
        a = self.track()
        self.push(K.ADDED, a)
        self.push(K.ADDED, a)
        with mock.patch.object(_receiver, "_description", side_effect=MemoryError):
            with self.assertRaises(MemoryError) as caught:
                self.receiver.poll_track()
        self.assertIs(type(caught.exception), MemoryError)
        self.assertEqual(self.cache_size(), 0)
        event = self.receiver.poll_track()
        self.assertEqual(event.description, FULL_EXPECTED)
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY)
        # an EXISTING track keeps its published snapshot across a failed update
        self.push(K.UPDATED, a)
        with mock.patch.object(_receiver, "_description", side_effect=MemoryError):
            with self.assertRaises(MemoryError):
                self.receiver.poll_track()
        self.assertIs(event.track.description, event.description)

    def test_declared_types_match_returned_values(self):
        hints = typing.get_type_hints(moq5.TrackEvent)
        self.assertEqual(hints["track"], moq5.Track | None)
        self.assertEqual(hints["description"], moq5.TrackDescription | None)
        drop = typing.get_type_hints(moq5.ParseDrop)
        self.assertEqual((drop["total"], drop["delta"]), (int | None, int | None))
        cenc = typing.get_type_hints(moq5.Cenc)
        self.assertEqual(cenc["scheme"], bytes | None)
        self.assertEqual(cenc["default_is_protected"], int | None)
        self.assertEqual(cenc["default_kid"], bytes | None)

    # ---- integration with the shell ----------------------------------------------

    def test_polled_tracks_drive_the_control_commands(self):
        a = self.track()
        self.push(K.ADDED, a)
        track = self.receiver.poll_track().track
        self.receiver.subscribe(track, start=moq5.StartMode.NEXT_GROUP, priority=3)
        self.assertEqual(_native._test_subscriptions(), [("subscribe", 0, 1, True, 3)])
        self.assertIs(self.receiver.track_state(track), moq5.TrackState.DISCOVERED)
        self.assertEqual(self.receiver.wait(0), moq5.WaitResult.TIMED_OUT)
        self.push(K.ENDED, a)
        self.assertEqual(self.receiver.wait(0), moq5.WaitResult.WOKEN, "a queued event is pollable work")


if __name__ == "__main__":
    unittest.main()
