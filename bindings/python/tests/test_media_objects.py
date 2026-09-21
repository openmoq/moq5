"""Owned media objects: the Receiver.poll_object contract against the fixture
provider, plus controls for the fixture's genuine ownership substrate and the
bridge's test-only allocation-failure seams. Every ABI number comes from the
fixture's compiled layout, never a host-layout guess.

The fixtures are ABI CARRIERS: `bytes(range(64))` is not a parsed CMAF
fragment and the sample sizes exceed the declared mdat range. They exercise
widths, signedness and copy ownership, not the CMAF parser or real media.
Unknown status/packaging preservation is a bridge contract for future values,
not a claim that today's native parser accepts them.

Ownership evidence here is the fixture's: real core buffers (moq_rcbuf_t,
refcounts read back) plus a fixture allocator with a live-block table, and a
fixture cleanup that MIRRORS moq_media_object_cleanup -- the SDK's own
function is not executed by this suite."""

import dataclasses
import gc
import importlib.util
import json
import os
import subprocess
import sys
import threading
import unittest
from pathlib import Path
from unittest import mock

import moq5
from moq5 import _native, _receiver

NS = (b"live",)
K = moq5.TrackEventKind

DESC = {"name": b"video", "packaging_text": b"cmaf", "media_type": 1, "packaging": 2,
        "timescale": 90000, "transport_version": 16, "is_live": True}

# Independently declared fixtures. Sample records are (duration, size, flags,
# composition_offset) in timescale TICKS; composition offsets are signed.
FRAGMENT = bytes(range(64))
SAMPLES = ((3000, 17, 0x02000000, -1500), (3000, 21, 0, 0), (1, 2**32 - 1, 0x00010000, 2**31 - 1))
CMAF = {"packaging": 2, "status": 0, "end_of_group": False, "datagram": False, "keyframe": True,
        "capture_time_us": 2**40 + 7, "decode_time_us": 2**63 + 5, "composition_offset_us": -(2**40),
        "presentation_time_us": 2**63 - 2**40 + 5, "fragment": FRAGMENT, "mdat_offset": 8, "mdat_len": 40,
        "samples": SAMPLES, "config_generation": 0}
RAW = {"packaging": 1, "status": 0, "end_of_group": True, "datagram": True, "keyframe": False,
       "capture_time_us": None, "decode_time_us": 0, "composition_offset_us": 0, "presentation_time_us": 0,
       "payload": b"raw\x00bytes", "config_generation": 0}


class ObjectFixtureControls(unittest.TestCase):
    """GREEN today: the substrate and the allocation gate, independent of the
    absent public method."""

    def setUp(self):
        gc.collect()
        _native._test_reset()
        _native._test_fail_allocation_after(-1)
        self.layout = _native._test_layout()
        self.endpoint = moq5.Endpoint.connect(moq5.EndpointConfig(url="moqt://fixture.invalid"))
        self.receiver = moq5.Receiver.attach(self.endpoint, moq5.ReceiverConfig.live(NS))
        self.handle = self.receiver._Receiver__handle

    def tearDown(self):
        _native._test_fail_allocation_after(-1)
        if not self.receiver.closed:
            self.receiver.close()
        self.endpoint.close()
        # settle NOW: every transferred buffer released, no live sample block
        _native._test_object_settle()

    def track(self, desc=DESC):
        capsule = _native._test_new_track(self.handle)
        _native._test_track_desc(capsule, dict(desc))
        return capsule

    def counts(self):
        return _native._test_object_counts()

    def assert_settled(self, c):
        self.assertEqual(c["actual_kept_refcounts"], c["kept_buffers"],
                         "every transferred reference was actually released (refcount read back)")
        self.assertEqual((c["sample_blocks_live"], c["sample_bad_free"]), (0, 0))
        self.assertEqual(c["cleanups_off_polling_thread"], 0, "cleanup ran on the polling thread")

    def test_layout_reports_the_frozen_object_v0_and_status_codes(self):
        L = self.layout
        self.assertGreater(L["object_v0"], 0)
        self.assertLessEqual(L["object_v0"], L["object_sizeof"])
        self.assertEqual(L["sample_sizeof"], 16, "four 32-bit sample fields")
        self.assertEqual((L["status_normal"], L["status_end_of_group"], L["status_end_of_track"]), (0, 1, 2))

    def test_native_transfer_and_cleanup_are_actually_released_exactly_once(self):
        a = self.track()
        _native._test_push_object(a, {**CMAF, "properties": b"\x01\x02loc"})   # a real properties_ref too
        _native._test_push_object(a, dict(RAW))
        c = self.counts()
        self.assertEqual((c["pushed"], c["refs_created"], c["samples_allocated"], c["kept_buffers"]), (2, 3, 1, 3))
        self.assertEqual(c["actual_kept_refcounts"], 6, "queued: each buffer is held by the queue AND the fixture")
        self.assertEqual(c["sample_blocks_live"], 1)
        rc, stamp, plen, flen, count, cleaned = _native._test_native_object_cycle()
        self.assertEqual((rc, stamp, plen, flen, count, cleaned), (0, self.layout["object_sizeof"], 0, 64, 3, 1))
        c = self.counts()
        self.assertEqual(c["actual_kept_refcounts"], 4, "payload AND properties refs of the first object are gone")
        self.assertEqual(c["sample_blocks_live"], 0, "the sample block was actually freed")
        rc, stamp, plen, flen, count, cleaned = _native._test_native_object_cycle()
        self.assertEqual((rc, plen, flen, count, cleaned), (0, len(RAW["payload"]), 0, 0, 1))
        c = self.counts()
        self.assertEqual((c["dequeued"], c["cleanup_calls"], c["cleanup_effective"]), (2, 2, 2))
        self.assertEqual((c["refs_created"], c["refs_released"]), (3, 3))
        self.assertEqual((c["samples_allocated"], c["samples_freed"]), (1, 1))
        self.assertEqual(c["cleanups_on_polling_thread"], 2)
        self.assert_settled(c)
        rc, *_ = _native._test_native_object_cycle()
        self.assertEqual(rc, moq5.PollOutcome.EMPTY, "a non-OK poll transfers nothing")
        self.assertEqual(self.counts()["cleanup_calls"], 2, "and invokes no cleanup")

    def test_cleanup_on_another_thread_is_detected_by_the_checker(self):
        # A checker control, not a concurrent release: the object is polled
        # here and cleaned on a worker (the fixture mutex serializes it), so
        # the polling-thread evidence must read one off-thread cleanup.
        a = self.track()
        _native._test_push_object(a, dict(RAW))
        seen = []

        def off_thread():
            seen.append(_native._test_native_object_cycle_split()[0])

        # dequeue here, clean there
        _native._test_native_object_hold()
        worker = threading.Thread(target=off_thread)
        worker.start()
        worker.join(5)
        c = self.counts()
        self.assertEqual((c["cleanups_on_polling_thread"], c["cleanups_off_polling_thread"]), (0, 1))
        self.assertEqual(seen, [1])
        self.assert_settled({**c, "cleanups_off_polling_thread": 0})

    def test_native_precedence_latch_then_queue_then_terminal(self):
        a = self.track()
        _native._test_push_object(a, dict(RAW))
        self.endpoint.set_interrupted(True)
        self.assertEqual(_native._test_native_object_cycle()[0], moq5.PollOutcome.INTERRUPTED)
        self.assertEqual(self.counts()["dequeued"], 0, "the latched poll consumed nothing")
        _native._test_receiver_state(True, False, 0)
        self.assertEqual(_native._test_native_object_cycle()[0], moq5.PollOutcome.INTERRUPTED, "latch precedes terminal")
        self.endpoint.set_interrupted(False)
        self.assertEqual(_native._test_native_object_cycle()[0], 0, "clearing the latch recovers the retained object")
        self.assertEqual(_native._test_native_object_cycle()[0], moq5.PollOutcome.CLOSED)

    def test_receiver_close_releases_queued_unpolled_objects_once(self):
        a = self.track()
        for _ in range(3):
            _native._test_push_object(a, {**CMAF, "properties": b"p"})
        self.assertEqual(self.counts()["actual_kept_refcounts"], 12)
        self.receiver.close()
        c = self.counts()
        self.assertEqual((c["released_on_destroy"], c["queued"]), (3, 0))
        self.assertEqual((c["refs_created"], c["refs_released"]), (6, 6))
        self.assertEqual((c["samples_allocated"], c["samples_freed"]), (3, 3))
        self.assertEqual(c["cleanup_calls"], 0, "destroy releases without a caller cleanup")
        self.assertEqual(c["actual_kept_refcounts"], 6, "only the fixture's own references remain")
        self.assertEqual(c["sample_blocks_live"], 0)

    def test_malformed_public_shapes_stay_cleanable(self):
        # every representable corruption keeps its ownership refs genuine
        a = self.track()
        shapes = ["payload_null_len", "fragment_null_len", "payload_len_over", "samples_null",
                  "sample_count_product", "mdat_offset_over", "mdat_len_over", "status_with_payload",
                  "normal_without_payload"]
        for which in shapes:
            base = dict(CMAF) if which not in ("payload_null_len", "payload_len_over") else dict(RAW)
            _native._test_push_object(a, {**base, "malformed": which})
        for which in shapes:
            with self.subTest(which=which):
                rc, *_rest, cleaned = _native._test_native_object_cycle()
                self.assertEqual(rc, 0)
        c = self.counts()
        self.assertEqual(c["refs_created"], c["refs_released"], "every injected object released its buffers")
        self.assertEqual(c["samples_allocated"], c["samples_freed"])
        self.assert_settled(c)

    def test_unrepresentable_stamps_are_refused_without_residue(self):
        a = self.track()
        before = self.counts()
        for stamp in (self.layout["object_v0"] - 1, 2**32, 2**32 + self.layout["object_sizeof"], 2**64 - 1):
            with self.subTest(stamp=stamp):
                with self.assertRaises(ValueError):
                    _native._test_push_object(a, {**CMAF, "stamp": stamp})
        after = self.counts()
        self.assertEqual((after["pushed"], after["queued"], after["kept_buffers"]), (before["pushed"], before["queued"], before["kept_buffers"]))
        self.assertEqual(after["refs_created"], after["refs_released"], "buffers made before the refusal were released")
        self.assertEqual((after["sample_blocks_live"], after["sample_bad_free"]), (0, 0))
        # a representable oversized stamp is still available to the RED rows
        _native._test_push_object(a, {**CMAF, "stamp": self.layout["object_sizeof"] + 1})
        rc, stamp, *_ = _native._test_native_object_cycle()
        self.assertEqual((rc, stamp), (0, self.layout["object_sizeof"] + 1))

    def test_bridge_allocation_gate_fails_a_native_allocation_as_bare_memory_error(self):
        # The gate is a fixture-only countdown over the bridge's own Python
        # allocations: proven here on the accepted track path.
        a = self.track()
        _native._test_push_event(int(K.ADDED), a, None, None, None, None)
        _native._test_push_event(int(K.ADDED), a, None, None, None, None)
        _native._test_fail_allocation_after(0)
        with self.assertRaises(MemoryError) as caught:
            self.receiver.poll_track()
        self.assertIs(type(caught.exception), MemoryError)
        _native._test_fail_allocation_after(-1)
        self.assertEqual(len(self.receiver._Receiver__tracks), 0, "nothing was published")
        event = self.receiver.poll_track()
        self.assertEqual(event.description.name, b"video")
        self.assertEqual(len(self.receiver._Receiver__tracks), 1)
        _native._test_push_event(int(K.UPDATED), a, None, None, None, None)
        _native._test_fail_allocation_after(3)          # deep inside the description conversion
        with self.assertRaises(MemoryError):
            self.receiver.poll_track()
        self.assertIs(event.track.description, event.description, "an existing snapshot survives")
        self.assertIs(self.receiver.poll_track(), moq5.PollOutcome.EMPTY, "the event was consumed")

    def test_reset_refuses_an_unreleased_transferred_buffer(self):
        # The evidence survives: a transferred object that was never cleaned
        # keeps its buffer at refcount 2 after the receiver is gone, and the
        # fixture reset reports it by name instead of erasing it.
        a = self.track()
        _native._test_push_object(a, dict(RAW))
        self.assertEqual(_native._test_native_object_hold(), 0)
        self.receiver.close()
        self.endpoint.close()
        c = self.counts()
        self.assertEqual((c["queued"], c["kept_buffers"], c["actual_kept_refcounts"]), (0, 1, 2))
        with self.assertRaisesRegex(RuntimeError, "transferred buffer unreleased"):
            _native._test_reset()
        with self.assertRaisesRegex(AssertionError, "still referenced"):
            _native._test_object_settle()
        self.assertEqual(_native._test_native_object_cycle_split(), (1,))
        self.assertEqual(self.counts()["actual_kept_refcounts"], 1)
        _native._test_reset()

    def test_gate_seam_is_present_in_the_fixture_module(self):
        self.assertTrue(hasattr(_native, "_test_fail_allocation_after"))
        self.assertTrue(moq5.build_info()["test_backend"])

    def test_gate_seam_is_absent_from_the_production_module(self):
        # Import the PRODUCTION module built beside this test package, in an
        # isolated child, and inspect it directly.
        # the lane's build tree holds the production module above the test package
        candidates = [m for parent in list(Path(moq5.__file__).resolve().parents)[:4]
                      for m in sorted(parent.glob("_native*.so"))
                      if "test-package" not in m.parts]
        self.assertTrue(candidates, "the lane build tree holds a production module")
        env = os.environ.copy()          # a sanitizer lane names its runtime for children
        insert = env.pop("MOQ5_TEST_CHILD_DYLD_INSERT_LIBRARIES", None)
        if insert:
            env["DYLD_INSERT_LIBRARIES"] = insert
        report = subprocess.run([sys.executable, "-I", "-W", "error", "-c", f"""
import importlib.util, json
spec = importlib.util.spec_from_file_location("moq5._native", {str(candidates[0])!r})
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
print(json.dumps({{"seams": sorted(n for n in dir(module) if n.startswith("_test")),
                   "test_backend": module.build_info()["test_backend"]}}))
"""], capture_output=True, text=True, timeout=30, env=env)
        self.assertEqual(report.returncode, 0, report.stderr)
        self.assertEqual(report.stderr, "", "a silent child: no diagnostic of any kind")
        facts = json.loads(report.stdout)
        self.assertEqual(facts["seams"], [], "no test seam is defined by the production module")
        self.assertFalse(facts["test_backend"])


class MediaObjectTests(unittest.TestCase):
    """The public object contract: exact values, real copy ownership,
    object-first Track identity, structural losses, consumed-item errors,
    poll precedence and guards."""

    def setUp(self):
        gc.collect()
        _native._test_reset()
        _native._test_fail_allocation_after(-1)
        self.layout = _native._test_layout()
        self.endpoint = moq5.Endpoint.connect(moq5.EndpointConfig(url="moqt://fixture.invalid"))
        self.receiver = moq5.Receiver.attach(self.endpoint, moq5.ReceiverConfig.live(NS))
        self.handle = self.receiver._Receiver__handle

    def tearDown(self):
        _native._test_fail_allocation_after(-1)
        if not self.receiver.closed:
            self.receiver.close()
        self.endpoint.close()
        _native._test_object_settle()

    def assert_settled(self, c):
        self.assertEqual(c["actual_kept_refcounts"], c["kept_buffers"])
        self.assertEqual((c["sample_blocks_live"], c["sample_bad_free"]), (0, 0))
        self.assertEqual(c["cleanups_off_polling_thread"], 0)

    def track(self, desc=DESC):
        capsule = _native._test_new_track(self.handle)
        _native._test_track_desc(capsule, dict(desc))
        return capsule

    def push(self, capsule, fields):
        _native._test_push_object(capsule, dict(fields))

    def counts(self):
        return _native._test_object_counts()

    def expected_cmaf(self, track):
        return moq5.MediaObject(
            track=track, config_generation=0, packaging=moq5.Packaging.CMAF, status=moq5.ObjectStatus.NORMAL,
            end_of_group=False, datagram=False, keyframe=True, capture_time_us=2**40 + 7,
            decode_time_us=2**63 + 5, composition_offset_us=-(2**40), presentation_time_us=2**63 - 2**40 + 5,
            payload=b"", fragment=FRAGMENT, mdat_offset=8, mdat_len=40,
            samples=tuple(moq5.CmafSample(*s) for s in SAMPLES))

    def expected_raw(self, track):
        return moq5.MediaObject(
            track=track, config_generation=0, packaging=moq5.Packaging.RAW, status=moq5.ObjectStatus.NORMAL,
            end_of_group=True, datagram=True, keyframe=False, capture_time_us=None,
            decode_time_us=0, composition_offset_us=0, presentation_time_us=0,
            payload=b"raw\x00bytes", fragment=b"", mdat_offset=0, mdat_len=0, samples=())

    # ---- 1. exact full-field objects ----------------------------------------------

    def test_exact_cmaf_and_raw_objects(self):
        a = self.track()
        self.push(a, CMAF)
        self.push(a, RAW)
        cmaf = self.receiver.poll_object()
        raw = self.receiver.poll_object()
        self.assertEqual(cmaf, self.expected_cmaf(cmaf.track))
        self.assertEqual(raw, self.expected_raw(cmaf.track))
        self.assertIs(raw.track, cmaf.track)
        self.assertEqual(cmaf.samples[0].composition_offset, -1500, "signed sample offset preserved")
        self.assertEqual(cmaf.samples[2].size, 2**32 - 1)
        self.assertEqual(cmaf.decode_time_us, 2**63 + 5, "unsigned 64-bit timestamp above int64")
        self.assertFalse(cmaf.is_status_only)
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.EMPTY)

    def test_status_and_empty_payload_semantics_never_by_byte_length(self):
        a = self.track()
        self.push(a, {**RAW, "payload": b""})                                  # NORMAL with zero bytes: media
        self.push(a, {**RAW, "status": 1, "payload": None, "end_of_group": True})   # END_OF_GROUP: status-only
        self.push(a, {**RAW, "status": 2, "payload": None})                   # END_OF_TRACK: status-only
        self.push(a, {**RAW, "status": 7, "payload": None})                   # unknown status: preserved
        self.push(a, {**RAW, "packaging": 9})                                 # unknown packaging: preserved
        empty, eog, eot, unknown_status, unknown_packaging = (self.receiver.poll_object() for _ in range(5))
        self.assertEqual((empty.status, empty.payload, empty.is_status_only), (moq5.ObjectStatus.NORMAL, b"", False))
        self.assertEqual((eog.status, eog.is_status_only, eog.payload), (moq5.ObjectStatus.END_OF_GROUP, True, b""))
        self.assertEqual((eot.status, eot.is_status_only), (moq5.ObjectStatus.END_OF_TRACK, True))
        self.assertEqual((unknown_status.status, unknown_status.is_status_only), (7, True))
        self.assertNotIsInstance(unknown_status.status, moq5.ObjectStatus)
        self.assertEqual(unknown_packaging.packaging, 9)
        self.assertNotIsInstance(unknown_packaging.packaging, moq5.Packaging)
        self.assertEqual(unknown_packaging.payload, b"raw\x00bytes", "unknown packaging is not interpreted, bytes kept")

    # ---- 2. real copy ownership --------------------------------------------------

    def test_copies_survive_backing_scramble_and_close_with_exact_cleanup(self):
        a = self.track()
        self.push(a, CMAF)
        obj = self.receiver.poll_object()
        c = self.counts()
        self.assertEqual((c["dequeued"], c["cleanup_calls"], c["cleanup_effective"]), (1, 1, 1),
                         "cleanup exactly once, before the public return")
        self.assertEqual((c["refs_created"], c["refs_released"]), (1, 1))
        self.assert_settled(c)
        _native._test_scramble_objects()
        self.assertEqual(obj.fragment, FRAGMENT, "bytes were copied before cleanup")
        self.assertEqual(obj.samples, tuple(moq5.CmafSample(*s) for s in SAMPLES))
        self.receiver.close()
        self.endpoint.close()
        self.assertEqual(obj.fragment, FRAGMENT)
        seen = []
        worker = threading.Thread(target=lambda: seen.append((obj.fragment[:4], obj.samples[1].size)))
        worker.start()
        worker.join(5)
        self.assertEqual(seen, [(FRAGMENT[:4], 21)], "owned data is readable from any thread")
        with self.assertRaises(RuntimeError):
            obj.track.receiver.track_state(obj.track)   # the receiver is closed; Track ops stay guarded

    def test_non_ok_polls_transfer_nothing_and_clean_nothing(self):
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.EMPTY)
        _native._test_receiver_state(True, False, 0)
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.CLOSED)
        _native._test_receiver_state(False, False, 0)
        self.endpoint.set_interrupted(True)
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.INTERRUPTED)
        self.endpoint.set_interrupted(False)
        for code in (-99, -2, -3):
            _native._test_poll_object_result(code)
            with self.assertRaises(moq5.MoqError) as caught:
                self.receiver.poll_object()
            self.assertEqual((caught.exception.code, caught.exception.operation), (code, "poll_object"))
        _native._test_poll_object_result(0)
        self.assertEqual(self.counts()["cleanup_calls"], 0)

    # ---- 3. object-first Track ----------------------------------------------------

    def test_object_before_its_added_event_yields_the_same_track(self):
        a = self.track()
        self.push(a, RAW)
        _native._test_push_event(int(K.ADDED), a, None, None, None, None)
        obj = self.receiver.poll_object()
        self.assertEqual(obj.track.description.name, b"video", "described from the current copy")
        event = self.receiver.poll_track()
        self.assertIs(event.track, obj.track)
        self.assertEqual(len(self.receiver._Receiver__tracks), 1)
        _native._test_track_current(a, False, 5)
        _native._test_push_event(int(K.UPDATED), a, None, None, None, None)
        self.assertIs(self.receiver.poll_track().track, obj.track, "identity survives updates")
        self.assertEqual(obj.track.description.vod, moq5.VodState(False, 5))

    def test_track_failures_are_named_losses_without_publication(self):
        a = self.track()
        self.push(None, RAW)                      # null handle
        with self.assertRaises(moq5.ObjectLost) as caught:
            self.receiver.poll_object()
        self.assertEqual(caught.exception.stage, "track")
        self.assertNotIn("0x", str(caught.exception))
        _native._test_desc_copy_result(-2)
        self.push(a, RAW)
        with self.assertRaises(moq5.ObjectLost) as caught:
            self.receiver.poll_object()
        self.assertEqual(caught.exception.stage, "track.description")
        self.assertNotIsInstance(caught.exception, moq5.MoqError)
        _native._test_desc_copy_result(0)
        self.assertEqual(len(self.receiver._Receiver__tracks), 0, "no Track was published")
        self.assertEqual(self.counts()["cleanup_effective"], 2, "both consumed objects were cleaned")
        self.assert_settled(self.counts())

    # ---- 4. structural boundaries before reads ------------------------------------

    def test_structural_boundaries_are_object_lost_before_reads_or_allocation(self):
        a = self.track()
        cases = [
            ("payload_null_len", RAW, "payload"),
            ("payload_len_over", RAW, "payload"),
            ("fragment_null_len", CMAF, "fragment"),
            ("samples_null", CMAF, "samples"),
            ("sample_count_product", CMAF, "samples"),
            ("mdat_offset_over", CMAF, "bounds:mdat_offset"),
            ("mdat_len_over", CMAF, "bounds:mdat_len"),
            ("status_with_payload", CMAF, "status"),
            ("normal_without_payload", RAW, "payload"),
        ]
        for which, base, stage in cases:
            with self.subTest(which=which):
                self.push(a, {**base, "malformed": which})
                self.push(a, RAW)
                with self.assertRaises(moq5.ObjectLost) as caught:
                    self.receiver.poll_object()
                self.assertEqual(caught.exception.stage, stage)
                self.assertNotIsInstance(caught.exception, MemoryError)
                self.assertEqual(self.receiver.poll_object(), self.expected_raw(self.receiver._Receiver__tracks[
                    _native.track_key(a)]))
        c = self.counts()
        self.assertEqual(c["refs_created"], c["refs_released"], "every consumed object was cleaned once")
        self.assert_settled(c)

    def test_same_count_wrong_bytes_or_sample_fields_fail_exact_inventory(self):
        a = self.track()
        self.push(a, {**CMAF, "fragment": FRAGMENT[:-1] + b"\xff"})
        self.push(a, {**CMAF, "samples": SAMPLES[:2] + ((1, 2**32 - 1, 0x00010000, 2**31 - 2),)})
        wrong_bytes, wrong_sample = self.receiver.poll_object(), self.receiver.poll_object()
        self.assertNotEqual(wrong_bytes, self.expected_cmaf(wrong_bytes.track))
        self.assertEqual(len(wrong_bytes.fragment), len(FRAGMENT))
        self.assertNotEqual(wrong_sample, self.expected_cmaf(wrong_sample.track))
        self.assertEqual(len(wrong_sample.samples), len(SAMPLES))

    # ---- 5. consumed-item errors ---------------------------------------------------

    def expected_raw_named(self, track, name):
        return moq5.MediaObject(
            track=track, config_generation=0, packaging=moq5.Packaging.RAW, status=moq5.ObjectStatus.NORMAL,
            end_of_group=True, datagram=True, keyframe=False, capture_time_us=None,
            decode_time_us=0, composition_offset_us=0, presentation_time_us=0,
            payload=name, fragment=b"", mdat_offset=0, mdat_len=0, samples=())

    def assert_sequence(self, before, after, *, dequeued, cleaned, refs_released, samples_freed):
        self.assertEqual(after["dequeued"] - before["dequeued"], dequeued)
        self.assertEqual(after["cleanup_calls"] - before["cleanup_calls"], dequeued,
                         "exactly one cleanup CALL per consumed object (status-only objects included)")
        self.assertEqual(after["cleanup_effective"] - before["cleanup_effective"], cleaned)
        self.assertEqual(after["refs_released"] - before["refs_released"], refs_released)
        self.assertEqual(after["samples_freed"] - before["samples_freed"], samples_freed)
        self.assert_settled(after)

    def test_failed_second_of_three_is_reported_once_and_neighbours_delivered(self):
        # Three DISTINCT objects: first RAW b"first", a malformed CMAF, third
        # RAW b"third". The stage of each loss is the injected shape; which
        # acquisition site it hits is measured at GREEN, not asserted here.
        a = self.track()
        for malformed, stage in (("mdat_len_over", "bounds:mdat_len"), ("sample_count_product", "samples")):
            with self.subTest(malformed=malformed):
                before = self.counts()
                self.push(a, {**RAW, "payload": b"first"})
                self.push(a, {**CMAF, "malformed": malformed})
                self.push(a, {**RAW, "payload": b"third"})
                first = self.receiver.poll_object()
                with self.assertRaises(moq5.ObjectLost) as caught:
                    self.receiver.poll_object()
                self.assertEqual(caught.exception.stage, stage)
                self.assertEqual(caught.exception.presentation_time_us, CMAF["presentation_time_us"],
                                 "already-validated scalar context is kept")
                self.assertEqual((caught.exception.status, caught.exception.packaging), (0, 2))
                third = self.receiver.poll_object()
                self.assertEqual(first, self.expected_raw_named(first.track, b"first"))
                self.assertEqual(third, self.expected_raw_named(third.track, b"third"))
                self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.EMPTY)
                self.assert_sequence(before, self.counts(), dequeued=3, cleaned=3, refs_released=3,
                                     samples_freed=1 if malformed != "sample_count_product" else 0)

    def test_description_refusal_for_a_new_handle_is_a_named_loss(self):
        # Native refusal of the current-description copy is a NATIVE loss at
        # `track.description`; it is not a Python construction failure.
        a, b = self.track(), self.track({**DESC, "name": b"other"})
        before = self.counts()
        self.push(a, {**RAW, "payload": b"first"})
        self.push(b, CMAF)
        self.push(a, {**RAW, "payload": b"third"})
        first = self.receiver.poll_object()
        _native._test_desc_copy_result(-14)
        with self.assertRaises(moq5.ObjectLost) as caught:
            self.receiver.poll_object()
        _native._test_desc_copy_result(0)
        self.assertEqual(caught.exception.stage, "track.description")
        self.assertNotIsInstance(caught.exception, moq5.MoqError)
        self.assertEqual(len(self.receiver._Receiver__tracks), 1, "no Track was published for the lost object")
        self.assertEqual(self.receiver.poll_object(), self.expected_raw_named(first.track, b"third"))
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.EMPTY)
        self.assert_sequence(before, self.counts(), dequeued=3, cleaned=3, refs_released=3, samples_freed=1)

    def test_wrapper_construction_and_cache_failures_publish_nothing(self):
        # Failures INSIDE the Python wrapper after a successful native poll and
        # cleanup: MediaObject construction, Track construction, cache
        # insertion. Test-local patches, as the track layer does.
        a, b = self.track(), self.track({**DESC, "name": b"other"})
        cases = [
            ("MediaObject", mock.patch.object(_receiver, "MediaObject", side_effect=MemoryError)),
            ("Track", mock.patch.object(_receiver, "Track", side_effect=MemoryError)),
            ("cache", mock.patch.object(type(self.receiver._Receiver__tracks), "__setitem__", side_effect=MemoryError)),
        ]
        for name, patch in cases:
            with self.subTest(stage=name):
                before = self.counts()
                self.push(a, {**RAW, "payload": b"first"})
                self.push(b, CMAF)                       # a NEW handle: nothing may be inserted
                self.push(a, {**RAW, "payload": b"third"})
                first = self.receiver.poll_object()
                with patch:
                    with self.assertRaises(MemoryError) as caught:
                        self.receiver.poll_object()
                self.assertIs(type(caught.exception), MemoryError, "bare MemoryError, never wrapped")
                self.assertEqual(len(self.receiver._Receiver__tracks), 1, "no partial cache entry for the new handle")
                self.assertEqual(self.receiver.poll_object(), self.expected_raw_named(first.track, b"third"))
                self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.EMPTY)
                self.assert_sequence(before, self.counts(), dequeued=3, cleaned=3, refs_released=3, samples_freed=1)
        # an EXISTING Track whose current description changed keeps its prior snapshot
        before = self.counts()
        self.push(a, {**RAW, "payload": b"first"})
        prior = self.receiver.poll_object().track.description
        _native._test_track_current(a, False, 77)
        self.push(a, CMAF)
        self.push(a, {**RAW, "payload": b"third"})
        with mock.patch.object(_receiver, "MediaObject", side_effect=MemoryError):
            with self.assertRaises(MemoryError):
                self.receiver.poll_object()
        self.assertIs(self.receiver._Receiver__tracks[_native.track_key(a)].description, prior,
                      "the prior description object is unchanged")
        third = self.receiver.poll_object()
        self.assertEqual(third.payload, b"third")
        self.assertEqual(third.track.description.vod, moq5.VodState(False, 77), "a later success publishes the new state")
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.EMPTY)
        self.assert_sequence(before, self.counts(), dequeued=3, cleaned=3, refs_released=3, samples_freed=1)

    def test_native_allocation_failure_is_bare_memory_error_at_named_sites(self):
        # Finite test-only witnesses: one NAMED acquisition site fails once,
        # and the bridge reports the last stage it completed, so each row pins
        # what already existed when the failure struck. On OOM the object was
        # consumed and cleaned; no per-object metadata is promised.
        a = self.track()
        sites = (("object.payload", "validated"),        # before any byte copy
                 ("object.fragment", "payload"),          # a Python-owned byte copy exists
                 ("object.samples", "fragment"),          # during sample construction
                 ("description.init_data", "samples"))    # during description construction
        for site, reached in sites:
            with self.subTest(site=site):
                before = self.counts()
                cached = self.receiver._Receiver__tracks.get(_native.track_key(a))
                prior = None if cached is None else cached.description
                self.push(a, {**CMAF, "properties": b"p"})
                self.push(a, {**RAW, "payload": b"next"})
                _native._test_fail_allocation_at(site)
                try:
                    with self.assertRaises(MemoryError) as caught:
                        self.receiver.poll_object()
                finally:
                    _native._test_fail_allocation_at(None)
                self.assertIs(type(caught.exception), MemoryError, "not ObjectLost, not MoqError")
                self.assertEqual(_native._test_object_progress(), reached)
                self.assertIs(self.receiver._Receiver__tracks.get(_native.track_key(a)), cached,
                              "no cache publication on failure")
                if cached is not None:
                    self.assertIs(cached.description, prior, "no description replacement on failure")
                following = self.receiver.poll_object()
                self.assertEqual(following, self.expected_raw_named(following.track, b"next"))
                self.assertEqual(_native._test_object_progress(), "converted")
                self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.EMPTY)
                self.assert_sequence(before, self.counts(), dequeued=2, cleaned=2, refs_released=3, samples_freed=1)

    def test_wrapper_failure_on_properties_and_status_only_objects_cleans_exactly_once(self):
        # cleanup_effective cannot count a status-only object with nothing to
        # release; cleanup_calls must still rise once per consumed object.
        a = self.track()
        cases = (
            ({**CMAF, "properties": b"props"}, dict(cleaned=2, refs_released=3, samples_freed=1)),
            ({**RAW, "status": 2, "payload": None, "properties": b"p"}, dict(cleaned=2, refs_released=2, samples_freed=0)),
            ({**RAW, "status": 1, "payload": None, "end_of_group": True}, dict(cleaned=1, refs_released=1, samples_freed=0)),
        )
        for fields, deltas in cases:
            with self.subTest(status=fields["status"]):
                before = self.counts()
                self.push(a, fields)
                self.push(a, {**RAW, "payload": b"next"})
                with mock.patch.object(_receiver, "MediaObject", side_effect=MemoryError):
                    with self.assertRaises(MemoryError):
                        self.receiver.poll_object()
                self.assertEqual(self.receiver.poll_object().payload, b"next")
                self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.EMPTY)
                self.assert_sequence(before, self.counts(), dequeued=2, **deltas)

    def test_oversized_stamp_is_a_named_loss_with_safe_cleanup(self):
        # The representable oversized stamp: the refs sit in the v0 prefix, so
        # cleanup is safe; nothing was validated, so no scalar context.
        a = self.track()
        before = self.counts()
        self.push(a, {**CMAF, "properties": b"p", "stamp": self.layout["object_sizeof"] + 1})
        self.push(a, {**RAW, "payload": b"next"})
        with self.assertRaises(moq5.ObjectLost) as caught:
            self.receiver.poll_object()
        e = caught.exception
        self.assertEqual((e.stage, e.presentation_time_us, e.status, e.packaging), ("struct_size", None, None, None))
        self.assertNotIn("0x", str(e))
        self.assertIsInstance(e, moq5.BindingError)
        self.assertNotIsInstance(e, moq5.EventLost)
        self.assertEqual(len(self.receiver._Receiver__tracks), 0)
        self.assertEqual(self.receiver.poll_object().payload, b"next")
        self.assert_sequence(before, self.counts(), dequeued=2, cleaned=2, refs_released=3, samples_freed=1)

    def test_values_are_immutable_and_copies_preserve_every_field(self):
        a = self.track()
        self.push(a, CMAF)
        obj = self.receiver.poll_object()
        with self.assertRaises(dataclasses.FrozenInstanceError):
            obj.payload = b"x"
        with self.assertRaises(dataclasses.FrozenInstanceError):
            obj.samples[0].size = 1
        # no __dict__ to grow: CPython 3.12's frozen+slots __setattr__ reports
        # an unknown name as TypeError, later versions as AttributeError
        with self.assertRaises((AttributeError, TypeError)):
            obj.extra = 1
        copy = dataclasses.replace(obj)
        self.assertEqual(copy, obj)
        self.assertIsNot(copy, obj)
        self.assertIs(copy.track, obj.track)
        self.assertEqual(tuple(dataclasses.astuple(s) for s in copy.samples), SAMPLES)
        self.assertIsInstance(obj.payload, bytes)
        self.assertIsInstance(obj.fragment, bytes)
        self.assertIsInstance(obj.samples, tuple)
        # the carrier fixture is not parsed CMAF: the mdat slice is just bytes 8..48
        self.assertEqual(obj.fragment[obj.mdat_offset:obj.mdat_offset + obj.mdat_len], FRAGMENT[8:48])

    # ---- 6. precedence and guards ---------------------------------------------------

    def test_poll_precedence_and_guards_before_native_consumption(self):
        a = self.track()
        self.push(a, RAW)
        _native._test_push_event(int(K.ADDED), a, None, None, None, None)
        self.endpoint.set_interrupted(True)
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.INTERRUPTED, "latch before the queue")
        self.assertIs(self.receiver.poll_track().kind, K.ADDED, "poll_track is latch-independent")
        _native._test_receiver_state(True, False, 0)
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.INTERRUPTED, "latch before terminal")
        self.endpoint.set_interrupted(False)
        self.assertEqual(self.receiver.poll_object().payload, b"raw\x00bytes", "clearing recovers the object")
        self.assertIs(self.receiver.poll_object(), moq5.PollOutcome.CLOSED, "terminal after draining")
        _native._test_receiver_state(False, False, 0)
        self.push(a, RAW)
        refused = []
        worker = threading.Thread(target=lambda: refused.append(self._poll_refused()))
        worker.start()
        worker.join(5)
        self.assertEqual(refused, ["receiver operation requires its creating thread"])
        self.assertEqual(self.counts()["dequeued"], 1, "a foreign-thread poll consumed nothing")
        if hasattr(os, "fork"):
            child = os.fork()
            if child == 0:
                try:
                    self.receiver.poll_object()
                except RuntimeError:
                    os._exit(0)
                os._exit(3)
            _, status = os.waitpid(child, 0)
            self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        self.assertEqual(self.counts()["dequeued"], 1)
        self.receiver.close()
        with self.assertRaises(RuntimeError):
            self.receiver.poll_object()
        self.assertEqual(self.counts()["released_on_destroy"], 1)

    def _poll_refused(self):
        try:
            self.receiver.poll_object()
        except RuntimeError as refused:
            return str(refused)
        return "accepted"


if __name__ == "__main__":
    unittest.main()
