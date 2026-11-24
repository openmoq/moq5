"""MSF-02 receiver observations through the bridge and the test-only C
provider: replacement links and description generations on track events, the
description's generation, init source and encryption fields, and the stats
block. Every ABI boundary used here is the compiled one the fixture reports."""

import gc
import typing
import unittest

import moq5
from moq5 import _native

NS = (b"live",)
K = moq5.TrackEventKind

VIDEO = {
    "name": b"video", "packaging_text": b"loc", "media_type": 1, "packaging": 1,
    "is_live": True,
}

KEY = b"MTIzNDU2Nzg5MDEyMzQ1Ng=="


class ReceiverMsf02Tests(unittest.TestCase):
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

    def track(self, **fields):
        capsule = _native._test_new_track(self.handle)
        desc = dict(VIDEO)
        desc.update(fields)
        _native._test_track_desc(capsule, desc)
        return capsule

    def push(self, kind, capsule, *, stamp=None, replaces=None, replaced_by=None, generation=0):
        _native._test_push_event(int(kind), capsule, stamp, None, None, None, None, None, None,
                                 replaces, replaced_by, generation)

    # ---- replacement links and generations ----------------------------------

    def test_replacement_links_both_handles_and_keeps_identity(self):
        old = self.track()
        new = self.track(name=b"video")
        self.push(K.ADDED, old)
        self.push(K.REMOVED, old, replaced_by=new)
        self.push(K.ADDED, new, replaces=old)
        added = self.receiver.poll_track()
        removed = self.receiver.poll_track()
        successor = self.receiver.poll_track()
        self.assertIsNone(added.replaces)
        self.assertIsNone(added.replaced_by)
        self.assertIs(removed.track, added.track)
        self.assertIsNone(removed.replaces)
        # The REMOVED alone names the successor: its Track exists before the
        # ADDED is polled, and the ADDED yields that same object.
        self.assertIsInstance(removed.replaced_by, moq5.Track)
        self.assertIs(successor.track, removed.replaced_by)
        self.assertIs(successor.replaces, added.track)
        self.assertIsNone(successor.replaced_by)
        self.assertIsNot(successor.track, added.track)

    def test_event_generation_is_the_announced_description_generation(self):
        a = self.track(config_generation=3)
        self.push(K.ADDED, a)
        self.push(K.UPDATED, a, generation=2)
        added = self.receiver.poll_track()
        updated = self.receiver.poll_track()
        self.assertEqual(added.config_generation, 0)
        self.assertEqual(updated.config_generation, 2)
        # The description is the CURRENT one, whose generation may be newer.
        self.assertEqual(updated.description.config_generation, 3)

    def test_replacement_links_are_gated_by_the_event_stamp(self):
        old = self.track()
        new = self.track()
        L = self.layout
        self.push(K.REMOVED, old, replaced_by=new, stamp=L["event_replaced_by_end"] - 1)
        self.push(K.ADDED, new, replaces=old, stamp=L["event_replaced_by_end"] - 1, generation=1)
        short_removed = self.receiver.poll_track()
        short_added = self.receiver.poll_track()
        self.assertIsNone(short_removed.replaced_by)
        self.assertIsNone(short_added.replaces)
        # config_generation is in the event's frozen prefix.
        self.assertEqual(short_added.config_generation, 1)
        self.push(K.REMOVED, old, replaced_by=new, stamp=L["event_replaced_by_end"])
        self.assertIsNotNone(self.receiver.poll_track().replaced_by)

    # ---- the description ----------------------------------------------------

    def test_description_generation_init_source_and_encryption(self):
        a = self.track(config_generation=4, init_source=2, init_property_type=0x0D,
                       encryption_scheme=b"moq-secure-objects", cipher_suite=b"aes-128-gcm-sha256",
                       key_id=b"key-1", track_base_key=KEY)
        self.push(K.ADDED, a)
        d = self.receiver.poll_track().description
        self.assertEqual(d.config_generation, 4)
        self.assertIs(d.init_source, moq5.InitSource.TRACK_PROPERTY)
        self.assertEqual(d.init_property_type, 0x0D)
        self.assertEqual(d.encryption_scheme, b"moq-secure-objects")
        self.assertEqual(d.cipher_suite, b"aes-128-gcm-sha256")
        self.assertEqual(d.key_id, b"key-1")
        self.assertEqual(d.track_base_key, KEY)
        # Key material never reaches a repr.
        self.assertNotIn(KEY.decode(), repr(d))
        self.assertNotIn(repr(KEY), repr(d))

    def test_absent_fields_are_none_and_an_unknown_source_stays_an_integer(self):
        a = self.track(init_source=9)
        self.push(K.ADDED, a)
        d = self.receiver.poll_track().description
        self.assertEqual(d.init_source, 9)
        self.assertNotIsInstance(d.init_source, moq5.InitSource)
        self.assertEqual(d.config_generation, 0)
        self.assertEqual(d.init_property_type, 0)
        for name in ("encryption_scheme", "cipher_suite", "key_id", "track_base_key"):
            self.assertIsNone(getattr(d, name), name)
        b = self.track(init_source=0, key_id=b"")
        self.push(K.ADDED, b)
        d = self.receiver.poll_track().description
        self.assertIs(d.init_source, moq5.InitSource.NONE)
        # A present empty field is empty bytes, not absence.
        self.assertEqual(d.key_id, b"")
        self.assertIsNone(d.cipher_suite)

    def test_description_fields_are_gated_by_the_description_stamp(self):
        L = self.layout
        a = self.track(config_generation=4, init_source=1, init_property_type=7, key_id=b"k",
                       desc_stamp=L["desc_config_generation_end"] - 1)
        self.push(K.ADDED, a)
        d = self.receiver.poll_track().description
        self.assertIsNone(d.config_generation)
        self.assertIsNone(d.init_source)
        self.assertIsNone(d.init_property_type)
        self.assertEqual(d.key_id, b"k", "the extension list is covered")
        b = self.track(key_id=b"k", desc_stamp=L["desc_extension_count_end"] - 1)
        self.push(K.ADDED, b)
        d = self.receiver.poll_track().description
        self.assertIsNone(d.key_id)
        self.assertIsNone(d.config_generation)

    def test_a_short_supplement_record_is_malformed_output(self):
        a = self.track(key_id=b"k", supplement_stamp=self.layout["supplement_v0"] - 1)
        self.push(K.ADDED, a)
        with self.assertRaises(moq5.EventLost) as caught:
            self.receiver.poll_track()
        self.assertEqual((caught.exception.kind, caught.exception.stage),
                         (K.ADDED, "description.supplement.struct_size"))
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY)

    def test_declared_types_match_returned_values(self):
        hints = typing.get_type_hints(moq5.TrackEvent)
        self.assertEqual(hints["replaces"], moq5.Track | None)
        self.assertEqual(hints["replaced_by"], moq5.Track | None)
        self.assertEqual(hints["config_generation"], int | None)
        desc = typing.get_type_hints(moq5.TrackDescription)
        self.assertEqual(desc["init_source"], moq5.InitSource | int | None)
        self.assertEqual(desc["track_base_key"], bytes | None)

    # ---- stats --------------------------------------------------------------

    def test_stats_block_is_one_stamped_block(self):
        _native._test_stats(1, 2, 3, 4, 5, 6, 7, 8, 9, False, 10, False, 11)
        _native._test_stats_msf02(1, 12, 13, 14, 15, 16, 17, 18, 19, 2**64 - 1)
        stats = self.receiver.stats()
        self.assertIs(stats.catalog_revision, moq5.MsfRevision.MSF02)
        self.assertEqual(
            (stats.objects_decompressed, stats.decompress_failures, stats.decompress_limited,
             stats.compression_refused, stats.compression_conflicts, stats.compression_ignored,
             stats.event_timeline_headers_ignored, stats.loc_order_gaps, stats.loc_order_forced),
            (12, 13, 14, 15, 16, 17, 18, 19, 2**64 - 1))
        _native._test_stats_msf02(7, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        self.assertEqual(self.receiver.stats().catalog_revision, 7)
        _native._test_stats_size(self.layout["stats_loc_order_forced_end"] - 1)
        short = self.receiver.stats()
        self.assertEqual(short.tracks_malformed, 11)
        for name in ("catalog_revision", "objects_decompressed", "compression_refused",
                     "event_timeline_headers_ignored", "loc_order_gaps", "loc_order_forced"):
            self.assertIsNone(getattr(short, name), name)
        _native._test_stats_msf02(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        _native._test_stats_size(self.layout["stats_loc_order_forced_end"])
        exact = self.receiver.stats()
        self.assertIs(exact.catalog_revision, moq5.MsfRevision.MSF01)
        self.assertEqual(exact.loc_order_forced, 0)
        # The block is an observation: it does not change legacy equality.
        self.assertEqual(exact, short)


if __name__ == "__main__":
    unittest.main()
