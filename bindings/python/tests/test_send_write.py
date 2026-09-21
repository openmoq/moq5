"""The Python media-write slice: SendObject, WriteOutcome, Sender.write.

These rows were written RED, against a surface that did not exist, and every
one of them failed by a NAMED missing behaviour rather than skipping. The
surface exists now and they pass. The gate that made that meaningful is kept
rather than removed: a missing or inert name is still a named failure, the
substrate rows still execute the oracles on their own, and the controls still
prove that this file's own machinery discriminates.

Scope: one object submission. No end_track, stats, demand, wait,
request_complete, graceful finish, examples or runtime. No registry, callbacks
or asyncio. Nothing here retries, and nothing ends or removes a track on an
error or on collection.

The contract this file encodes, re-derived from the current sources and the
local specs rather than from the obsolete sketches in 1891-1893:

  moq_media_send_object_t   service/include/moq/media_sender.h:426-470
      struct_size; payload REQUIRED; properties is the CMAF passthrough block
      and MUST be NULL for RAW (a non-NULL value is INVAL); is_sync,
      starts_group, ends_group; decode_time_us advisory in v0;
      presentation_time_us; has_capture_time + capture_time_us;
      has_sap_type + sap_type, which must be CONCRETE.
  moq_media_sender_write    service/include/moq/media_sender.h:473-497
      transfer-on-success: on MOQ_OK the caller's payload and properties refs
      pass to the service; on ANY non-OK no transfer happened and the caller
      still owns them.
  the validation order, service/src/media_sender.c:4285-4370
      NULL args, struct_size, payload, concrete SAP, INTERRUPTED, CLOSED,
      track ownership, generated-timeline refusal, ended/removed/VOD
      (WRONG_STATE), the RAW LOC timestamp bound, then CMAF validation. None
      of those takes ownership.

Spec authorities, read in /Users/jekyll/Projects/MoQ/Spec:
  LOC-01 2.3.1.1 (line 343): the Capture Timestamp is wall-clock microseconds
      since the Unix epoch, encoded as a varint.
  CMSF-00 3.4 (line 186): each group MUST begin with an object containing a
      SAP of type 1 or 2.
  CMSF-00 3.6.1 (line 257): the SAP-type timeline conveys SAP types; the
      concrete type is declared by the encoder, not derived here.

Spans are immutable bytes only. That narrows the earlier buffer-protocol
sketch on purpose: this first synchronous surface takes the same input kind
SendTrackConfig already takes, and bytearray, memoryview, str and int are each
refused. A payload of b"" is a VALUE, passed as a non-NULL zero-length buffer,
while an omitted or None payload stays invalid; properties=None is absence and
properties=b"" is a present empty block, and the two are pinned apart.

Two C implementation constraints are NOT protocol requirements and are named
as such: the varint ceiling applied to a RAW timestamp follows the NEGOTIATED
codec (draft-16 and any not-yet-observed session use MOQ_QUIC_VARINT_MAX,
draft-18 carries the full uint64 range), and the RAW properties refusal is a
v0 service decision because the service owns the LOC block. Neither is
re-implemented in Python: the binding passes the values and reports what the
service returns.
"""

import gc
import os
import subprocess
import sys
import threading
import types
import unittest
import warnings
from pathlib import Path
from unittest import mock

import moq5
from moq5 import _native

import test_foundation as foundation  # noqa: F401
from test_sender import reap_child

INVAL = -2
NOMEM = -1
CLOSED = -4
WRONG_STATE = -5
WOULD_BLOCK = -8
INTERRUPTED = -13

NS = (b"svc", b"demo")
UINT32_MAX = 2 ** 32 - 1
UINT64_MAX = 2 ** 64 - 1
VARINT_MAX = 0x3FFFFFFFFFFFFFFF          # core/include/moq/wire.h:27

SURFACE = ("SendObject", "WriteOutcome", "SapType")
BRIDGE = ("sender_write",)

# The genuinely fallible acquisitions the bridge makes before the native call.
# Each names a real site: one refcounted buffer per span.
ACQUISITION_SITES = ("send_object_payload", "send_object_properties")


def missing_surface(namespace=moq5):
    return tuple(n for n in SURFACE if not hasattr(namespace, n))


# The declared object fields the recorder exposes. An exact image is compared
# field by field against this set, so "a few selected fields" can never be
# mistaken for a full inventory.
IMAGE_FIELDS = (
    "payload", "payload_declared_length", "payload_null",
    "properties", "properties_declared_length", "properties_null",
    "is_sync", "starts_group", "ends_group",
    "decode_time_us", "presentation_time_us",
    "has_capture_time", "capture_time_us",
    "has_sap_type", "sap_type",
    "track_index", "sender_index",
    "full_size", "oracle_limit",
)

DEFAULT_IMAGE = {
    "payload": b"frame", "payload_declared_length": 5, "payload_null": False,
    "properties": b"", "properties_declared_length": 0, "properties_null": True,
    "is_sync": False, "starts_group": False, "ends_group": False,
    "decode_time_us": 0, "presentation_time_us": 0,
    "has_capture_time": False, "capture_time_us": 0,
    "has_sap_type": False, "sap_type": 0,
    "track_index": 0, "sender_index": 0,
    "full_size": True, "oracle_limit": False,
}


def check_image(case, observed, **overrides):
    """Compare the COMPLETE declared image, not a selection of fields."""
    expected = dict(DEFAULT_IMAGE)
    expected.update(overrides)
    case.assertEqual(set(expected), set(IMAGE_FIELDS), "the checker is stale")
    missing = [f for f in IMAGE_FIELDS if f not in observed]
    case.assertEqual(missing, [], f"the recorder stopped reporting {missing}")
    case.assertEqual({f: observed[f] for f in IMAGE_FIELDS}, expected)


class WriteFixtureBase(unittest.TestCase):
    def tearDown(self):
        # Release anything the fake owner accepted, even when an assertion
        # failed and even when this case runs last, rather than leaving it to
        # the next test's setUp.
        _native._test_write_block(False)
        _native._test_write_result(0)
        _native._test_fail_allocation_at(None)
        _native._test_write_reset()

    def setUp(self):
        gc.collect()
        _native._test_reset()
        _native._test_sender_reset()
        _native._test_send_track_reset()
        _native._test_write_reset()

    def connect(self):
        return moq5.Endpoint.connect(moq5.EndpointConfig(url="moqt://fixture.invalid"))

    def attach(self, endpoint):
        return moq5.Sender.attach(
            endpoint,
            moq5.SenderConfig(namespace=NS,
                              backpressure=moq5.Backpressure.DROP_GROUP))

    def track(self, sender, **fields):
        fields.setdefault("name", b"v")
        fields.setdefault("media_type", moq5.MediaType.VIDEO)
        fields.setdefault("packaging", moq5.Packaging.RAW)
        fields.setdefault("codec", b"av01")
        fields.setdefault("bitrate", 1_500_000)
        return sender.add_track(moq5.SendTrackConfig(**fields))


# ---------------------------------------------------------------- substrate --

class WriteSubstrateTests(WriteFixtureBase):
    """The write recorder and its seams, exercised without the write layer.

    These execute today. They are what keeps a missing product name from
    hiding a broken oracle.
    """

    def test_a_reset_reports_an_empty_write_inventory(self):
        self.assertEqual(_native._test_write_counts(),
                         {"entries": 0, "accepted": 0,
                          "holds_references": False, "gate_fault": False})
        self.assertIsNone(_native._test_write_object())

    def test_b_the_recorder_reports_no_owned_references_when_empty(self):
        self.assertEqual(_native._test_write_owned_refs(),
                         {"payload": 0, "properties": 0,
                          "payload_held": False, "properties_held": False,
                          "payload_live": False, "properties_live": False})

    def test_c_the_scripted_result_is_settable_and_cleared_by_reset(self):
        for code in (WOULD_BLOCK, INTERRUPTED, CLOSED, INVAL, 0):
            with self.subTest(code=code):
                _native._test_write_result(code)      # accepted without error
        _native._test_write_reset()
        self.assertEqual(_native._test_write_counts()["entries"], 0)


    def test_d_the_recorder_captures_a_driven_write_exactly(self):
        # The oracle itself, exercised without the bridge: bytes, flags,
        # entry refcounts and the transfer rule.
        self.assertEqual(_native._test_write_simulate(b"\x00fr\xff", b"pr\x00", 1), 0)
        image = _native._test_write_object()
        self.assertEqual(image["payload"], b"\x00fr\xff")
        self.assertEqual(image["properties"], b"pr\x00")
        self.assertIs(image["is_sync"], True)
        self.assertIs(image["full_size"], True)
        self.assertEqual(image["payload_refs_at_entry"], 1)
        self.assertEqual(image["properties_refs_at_entry"], 1)
        counts = _native._test_write_counts()
        self.assertEqual(counts, {"entries": 1, "accepted": 1,
                                  "holds_references": True,
                                  "gate_fault": False})
        self.assertEqual(_native._test_write_owned_refs(),
                         {"payload": 1, "properties": 1,
                          "payload_held": True, "properties_held": True,
                          "payload_live": True, "properties_live": True})

    def test_e_a_scripted_refusal_takes_no_ownership(self):
        _native._test_write_result(WOULD_BLOCK)
        try:
            self.assertEqual(_native._test_write_simulate(b"frame", None, 0),
                             WOULD_BLOCK)
        finally:
            _native._test_write_result(0)
        counts = _native._test_write_counts()
        self.assertEqual(counts["entries"], 1)
        self.assertEqual(counts["accepted"], 0)
        self.assertIs(counts["holds_references"], False,
                      "a refusal took ownership")

    def test_f_the_in_call_barrier_is_a_real_handshake(self):
        # Proves the fixture's own barrier before any product row depends on
        # it: the writer parks inside the native call and another thread
        # reaches it and releases it. Nothing sleeps.
        reached, released, result = [], [], []

        def driver():
            result.append(_native._test_write_simulate(b"frame", None, 0))

        _native._test_write_block(True)
        worker = threading.Thread(target=driver, daemon=True)
        worker.start()
        try:
            reached.append(_native._test_write_wait_entered(5.0))
            released.append(_native._test_write_release())
        finally:
            # settlement is unconditional: a failed assertion above must not
            # leave a parked thread behind for the next row to inherit
            _native._test_write_block(False)
            _native._test_write_release()
            worker.join(10.0)
        self.assertFalse(worker.is_alive(), "the driver never returned")
        self.assertEqual(reached, [True], "the barrier was never reached")
        self.assertEqual(released, [True],
                         "the fixture's own bound expired: harness failure")
        self.assertEqual(result, [0])

    def test_g_the_barrier_does_not_report_a_call_that_never_happened(self):
        _native._test_write_block(True)
        try:
            self.assertIs(_native._test_write_wait_entered(0.2), False,
                          "the barrier reported a call with none in flight")
        finally:
            _native._test_write_block(False)

    def assert_balanced(self, baseline, message):
        now = _native._test_rcbuf_counts()
        self.assertEqual(now["outstanding"], baseline["outstanding"], message)
        self.assertEqual(now["payload"]["outstanding"],
                         baseline["payload"]["outstanding"], message)
        self.assertEqual(now["properties"]["outstanding"],
                         baseline["properties"]["outstanding"], message)
        self.assertIs(now["table_overflow"], False,
                      "the observation table overflowed: harness failure")
        self.assertIs(now["unknown_frees"], False,
                      "a block was freed that the table never saw")

    def test_g2_the_allocation_inventory_detects_each_omitted_cleanup(self):
        # "No transfer" is not proof of release. These are the REAL rcbuf
        # allocation and free boundaries, counted PER SPAN, and each omitted
        # cleanup is detected on its own. An omitted reference is retained by
        # the fixture rather than dropped, so the omission is observable and
        # then releasable -- a deliberately unfreed buffer with no owner would
        # be a real leak in this test, not just a counter.
        baseline = _native._test_rcbuf_counts()
        self.addCleanup(self.assert_balanced, baseline,
                        "the omission case did not return to baseline")
        self.addCleanup(_native._test_write_release_stranded)
        _native._test_write_result(WOULD_BLOCK)
        self.addCleanup(_native._test_write_result, 0)

        self.assertEqual(
            _native._test_write_simulate(b"frame", b"props", 0), WOULD_BLOCK)
        clean = _native._test_rcbuf_counts()
        self.assertEqual(clean["payload"]["allocations"]
                         - baseline["payload"]["allocations"], 1)
        self.assertEqual(clean["properties"]["allocations"]
                         - baseline["properties"]["allocations"], 1)
        self.assert_balanced(baseline, "a refused buffer was leaked")

        for span, flags in (("payload", (1, 0)), ("properties", (0, 1))):
            with self.subTest(span=span):
                self.assertEqual(
                    _native._test_write_simulate(b"frame", b"props", 0, *flags),
                    WOULD_BLOCK)
                omitted = _native._test_rcbuf_counts()
                self.assertEqual(omitted[span]["outstanding"]
                                 - baseline[span]["outstanding"], 1,
                                 f"an omitted {span} release went undetected")
                other = "properties" if span == "payload" else "payload"
                self.assertEqual(omitted[other]["outstanding"],
                                 baseline[other]["outstanding"],
                                 "the wrong span was blamed")
                self.assertGreater(omitted["bytes_outstanding"],
                                   baseline["bytes_outstanding"])
                held = _native._test_write_stranded()
                self.assertEqual(held["held"], 1,
                                 "the omitted reference was dropped, not held")
                self.assertIs(held["overflow"], False)
                self.assertEqual(_native._test_write_release_stranded(), 1)
                self.assert_balanced(
                    baseline, f"releasing the held {span} did not settle it")

    def test_g2b_a_premature_release_of_a_transferred_reference_is_named(self):
        # The acceptance oracle must not learn about an early release by
        # dereferencing a freed pointer. The liveness table answers first, so
        # the perturbation below is a NAMED observation and teardown stays safe.
        baseline = _native._test_rcbuf_counts()
        self.addCleanup(self.assert_balanced, baseline,
                        "the perturbation did not settle")
        self.assertEqual(_native._test_write_simulate(b"frame", b"props", 0), 0)
        held = _native._test_write_owned_refs()
        self.assertIs(held["payload_live"], True)
        self.assertIs(held["properties_live"], True)
        self.assertEqual(held["payload"], 1)

        self.assertEqual(_native._test_write_force_release_owned(), 2)
        after = _native._test_write_owned_refs()
        self.assertIs(after["payload_held"], True, "the pointer was forgotten")
        self.assertIs(after["payload_live"], False,
                      "a released buffer was reported as live")
        self.assertIs(after["properties_live"], False)
        self.assertEqual(after["payload"], 0,
                         "a freed buffer was dereferenced for a refcount")

        _native._test_write_reset()
        settlement = _native._test_write_settlement()
        self.assertIs(settlement["premature_release"], True,
                      "the settlement did not name the early release")
        self.assertEqual(settlement["released"], 0,
                         "settlement released a buffer that was already freed")

    def test_g2c_an_ordinary_acceptance_settles_without_that_alarm(self):
        baseline = _native._test_rcbuf_counts()
        self.addCleanup(self.assert_balanced, baseline, "acceptance leaked")
        self.assertEqual(_native._test_write_simulate(b"frame", b"props", 0), 0)
        _native._test_write_reset()
        settlement = _native._test_write_settlement()
        self.assertIs(settlement["premature_release"], False,
                      "a correct settlement was reported as premature")
        self.assertEqual(settlement["released"], 2)

    def test_g2d_the_fault_site_inventory_is_taken_at_the_fault(self):
        # Reading ownership after the stack has unwound cannot tell
        # cleanup-before-the-result-allocation from cleanup-after-it. This
        # observer records the inventory ON the thread that fired the fault,
        # and the two orderings are told apart by it -- per SPAN, so a late
        # properties release is distinguishable from correct ordering even
        # when the final unwind happens to balance.
        baseline = _native._test_rcbuf_counts()
        self.addCleanup(self.assert_balanced, baseline, "the driver leaked")
        self.addCleanup(_native._test_fail_allocation_at, None)
        self.addCleanup(_native._test_write_result, 0)

        for site in ("write_result", "write_error"):
            for ordering, held in ((1, 0), (0, 2)):
                with self.subTest(site=site, release_before_result=ordering):
                    # per-ITERATION settlement: the armed fault and the
                    # scripted result are cleared before the next row starts,
                    # never left to the end of the test
                    _native._test_write_result(WOULD_BLOCK)
                    _native._test_fail_allocation_at(site)
                    try:
                        outcome = _native._test_write_simulate_ordered(
                            b"frame", b"props", ordering, site)
                        self.assertEqual(outcome["rc"], WOULD_BLOCK)
                        self.assertEqual(outcome["result_fault"], 1,
                                         f"the {site} fault never fired")
                        self.assertIs(
                            _native._test_allocation_site_fired(site), True)
                        at_fault = _native._test_fault_site_inventory(site)
                        self.assertIsNotNone(
                            at_fault, "no inventory was taken at the fault")
                        for span in ("payload", "properties"):
                            self.assertEqual(
                                at_fault[span]["outstanding"]
                                - baseline[span]["outstanding"],
                                held // 2,
                                f"the {span} span's state at the fault does "
                                "not match the ordering under test")
                        self.assertEqual(
                            at_fault["outstanding"]
                            - baseline["outstanding"], held,
                            "the observer cannot tell the orderings apart")
                    finally:
                        _native._test_fail_allocation_at(None)
                        _native._test_write_result(0)
                    self.assert_balanced(
                        baseline, "an iteration left a span outstanding")

    def test_g2e_each_acquisition_fault_stops_before_the_next(self):
        baseline = _native._test_rcbuf_counts()
        self.addCleanup(self.assert_balanced, baseline, "a fault path leaked")
        self.addCleanup(_native._test_fail_allocation_at, None)
        before = _native._test_write_counts()

        _native._test_fail_allocation_at("send_object_payload")
        outcome = _native._test_write_simulate_ordered(b"frame", b"props")
        self.assertEqual(outcome["payload_fault"], 1)
        self.assertEqual(_native._test_write_counts()["entries"],
                         before["entries"], "a failed acquisition still called")
        self.assert_balanced(baseline, "the first fault acquired something")

        _native._test_fail_allocation_at("send_object_properties")
        outcome = _native._test_write_simulate_ordered(b"frame", b"props")
        self.assertEqual(outcome["properties_fault"], 1)
        self.assertEqual(_native._test_write_counts()["entries"],
                         before["entries"], "a failed acquisition still called")
        self.assert_balanced(
            baseline, "the payload was not released when properties failed")

    def test_g3_an_accepted_write_leaves_the_owner_holding_both(self):
        before = _native._test_rcbuf_counts()
        self.assertEqual(_native._test_write_simulate(b"frame", b"props", 0), 0)
        after = _native._test_rcbuf_counts()
        self.assertEqual(after["allocations"] - before["allocations"], 2)
        self.assertEqual(after["outstanding"] - before["outstanding"], 2,
                         "an accepted buffer was released by the caller")
        self.assertIs(_native._test_write_counts()["holds_references"], True)
        # the fake owner eventually releases them, as the service would
        _native._test_write_reset()
        settled = _native._test_rcbuf_counts()
        self.assertEqual(settled["outstanding"], before["outstanding"],
                         "the owner never released what it accepted")

    def signal_driver_child(self, scripted, raising, deliver=True,
                            bad_verdict=False):
        """One driven call with a REAL pending signal, in an owned child.

        The handler either records the signal or RAISES KeyboardInterrupt.
        Settlement is UNCONDITIONAL: the helper is joined and checked, the
        recorder's references released and the inventory compared to baseline
        even when the verdict above has already failed. A cleanup failure is
        reported separately (+100) so it can never mask the diagnostic.
        """
        child = os.fork()
        if child == 0:
            status = 0
            cleanup_status = 0
            try:
                import signal as signal_module

                received = []

                def handler(*args):
                    received.append(True)
                    if raising:
                        raise KeyboardInterrupt

                signal_module.signal(signal_module.SIGUSR1, handler)
                baseline = _native._test_rcbuf_counts()
                helper = None
                try:
                    _native._test_write_result(scripted)
                    reached, released = [], []

                    def arm():
                        entered = _native._test_write_wait_entered(5.0)
                        reached.append(entered)
                        if entered and deliver:
                            os.kill(os.getpid(), signal_module.SIGUSR1)
                        released.append(_native._test_write_release())

                    helper = threading.Thread(target=arm, daemon=True)
                    _native._test_write_block(True)
                    helper.start()
                    interrupted = False
                    try:
                        rc = _native._test_write_simulate(b"frame", b"props", 0)
                    except KeyboardInterrupt:
                        interrupted = True
                        rc = scripted
                    finally:
                        _native._test_write_block(False)
                        _native._test_write_release()
                        helper.join(10.0)
                    counts = _native._test_write_counts()
                    accepted = scripted == 0
                    if helper.is_alive():
                        status = 10         # harness: the helper never settled
                    elif reached != [True]:
                        status = 11         # the call was never entered
                    elif released[:1] != [True] or counts["gate_fault"]:
                        status = 12         # harness: the fixture bound expired
                    elif rc != scripted:
                        status = 13
                    elif deliver and not received:
                        status = 14         # the signal never arrived
                    elif not deliver and received:
                        status = 15
                    elif deliver and raising and not interrupted:
                        status = 16         # the raising handler did not raise
                    elif interrupted and not (deliver and raising):
                        status = 17
                    elif counts["entries"] != 1:
                        status = 18
                    elif counts["accepted"] != (1 if accepted else 0):
                        status = 19
                    elif counts["holds_references"] is not accepted:
                        status = 20         # transfer did not follow the result
                    elif not accepted and (
                            _native._test_rcbuf_counts()["outstanding"]
                            != baseline["outstanding"]):
                        status = 21         # a refusal stranded a span
                    if bad_verdict and status == 0:
                        status = 90         # a deliberate verdict failure
                finally:
                    try:
                        _native._test_write_block(False)
                        _native._test_write_release()
                        if helper is not None:
                            helper.join(10.0)
                            if helper.is_alive():
                                cleanup_status = 1
                        _native._test_write_reset()
                        settlement = _native._test_write_settlement()
                        if settlement["premature_release"]:
                            cleanup_status = 2
                        elif (_native._test_rcbuf_counts()["outstanding"]
                              != baseline["outstanding"]):
                            cleanup_status = 3
                    except BaseException:
                        cleanup_status = 4
            except BaseException:
                status = 29
            os._exit(status + (100 if cleanup_status else 0))
        return reap_child(child)

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only contract")
    def test_g5_a_raising_signal_handler_does_not_disturb_ownership(self):
        # Acceptance first: the signal handler RAISES, and the transfer that
        # already happened is neither undone nor doubled.
        self.assertEqual(self.signal_driver_child(0, True), 0,
                         "a raising signal over an accepted call did not "
                         "settle cleanly")

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only contract")
    def test_g6_a_raising_signal_over_a_refusal_leaves_no_span_held(self):
        self.assertEqual(self.signal_driver_child(WOULD_BLOCK, True), 0,
                         "a raising signal over a refusal stranded a span")

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only contract")
    def test_g7_the_non_raising_and_unsignalled_controls_are_clean(self):
        for scripted in (0, WOULD_BLOCK):
            with self.subTest(scripted=scripted, handler="records"):
                self.assertEqual(
                    self.signal_driver_child(scripted, False), 0)
            with self.subTest(scripted=scripted, handler="none delivered"):
                self.assertEqual(
                    self.signal_driver_child(scripted, True, deliver=False), 0)

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only contract")
    def test_g8_a_failed_verdict_still_settles_what_the_child_owns(self):
        # Process exit is not a cleanup proof. This child ACCEPTS an object,
        # then deliberately fails its verdict; the exit status must still be
        # the bare verdict, because +100 would mean cleanup failed too.
        self.assertEqual(self.signal_driver_child(0, True, bad_verdict=True), 90,
                         "a failed verdict skipped or broke the settlement")

    def test_h_an_oversized_span_is_a_named_limit_not_a_truncation(self):
        self.assertEqual(_native._test_write_simulate(b"p" * 300, None, 0), 0)
        image = _native._test_write_object()
        self.assertIs(image["oracle_limit"], True)
        self.assertIn("payload exceeds fixture capacity",
                      image["oracle_limit_reason"])
        self.assertEqual(image["payload"], b"", "a truncated image was exposed")
        self.assertEqual(image["payload_declared_length"], 300)


# ------------------------------------------------------------- behavioural --

class WriteGatedBase(WriteFixtureBase):
    """Never skips. A missing or inert surface is a NAMED failure, asserted
    before the row acquires an endpoint."""

    def require_surface(self):
        # Kept after GREEN: an inert or removed name must still fail by name,
        # which is what the inert-surface control exercises.
        missing = missing_surface()
        if missing:
            self.fail("the Python media-write surface is not implemented: moq5 "
                      f"is missing {', '.join(missing)}")
        self.assertIsInstance(moq5.SendObject, type, "SendObject is not a type")
        self.assertTrue(hasattr(moq5.WriteOutcome, "ACCEPTED"),
                        "WriteOutcome has no ACCEPTED")
        self.assertTrue(hasattr(moq5.SapType, "TYPE_1"), "SapType has no TYPE_1")
        self.assertTrue(callable(getattr(moq5.Sender, "write", None)),
                        "Sender.write is not callable")

    def obj(self, **fields):
        fields.setdefault("payload", b"frame")
        return moq5.SendObject(**fields)


class SendObjectValueTests(WriteGatedBase):
    def test_a_every_declared_field_reaches_the_service(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender)
                sender.write(track, self.obj(
                    payload=b"\x00\xffframe\x00", is_sync=True,
                    starts_group=True, ends_group=False,
                    decode_time_us=7_000_001, presentation_time_us=7_000_002,
                    capture_time_us=1_700_000_000_000_000))
                image = _native._test_write_object()
            finally:
                sender.close()
        check_image(self, image,
                    payload=b"\x00\xffframe\x00", payload_declared_length=8,
                    is_sync=True, starts_group=True,
                    decode_time_us=7_000_001, presentation_time_us=7_000_002,
                    has_capture_time=True,
                    capture_time_us=1_700_000_000_000_000)

    def test_b_an_absent_optional_differs_from_an_explicit_zero(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender)
                sender.write(track, self.obj())                  # absent
                absent = _native._test_write_object()
                sender.write(track, self.obj(capture_time_us=0))  # explicit 0
                zero = _native._test_write_object()
                sender.write(track, self.obj(sap_type=moq5.SapType.NONE))
                sap_none = _native._test_write_object()
            finally:
                sender.close()
        self.assertIs(absent["has_capture_time"], False)
        self.assertEqual(absent["capture_time_us"], 0)
        self.assertIs(zero["has_capture_time"], True,
                      "a declared zero capture time was sent as absent")
        self.assertEqual(zero["capture_time_us"], 0)
        self.assertIs(absent["has_sap_type"], False)
        self.assertIs(sap_none["has_sap_type"], True,
                      "a declared SAP NONE was sent as absent")
        self.assertEqual(sap_none["sap_type"], 0)

    def test_c_the_full_c_widths_survive(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender)
                sender.write(track, self.obj(
                    decode_time_us=UINT64_MAX,
                    presentation_time_us=UINT64_MAX - 1,
                    capture_time_us=VARINT_MAX))
                at_varint = _native._test_write_object()
                # a SEPARATE value ABOVE the 62-bit varint ceiling: the bridge
                # must map the full uint64. Whether the NEGOTIATED profile
                # accepts it is a C decision and is not mirrored here.
                sender.write(track, self.obj(capture_time_us=UINT64_MAX))
                above = _native._test_write_object()
                sender.write(track, self.obj(ends_group=True))
                ended = _native._test_write_object()
            finally:
                sender.close()
        self.assertEqual(at_varint["decode_time_us"], UINT64_MAX)
        self.assertEqual(at_varint["presentation_time_us"], UINT64_MAX - 1)
        self.assertEqual(at_varint["capture_time_us"], VARINT_MAX)
        self.assertEqual(above["capture_time_us"], UINT64_MAX,
                         "a universal 62-bit clamp was applied")
        self.assertIs(ended["ends_group"], True)
        self.assertIs(ended["starts_group"], False)

    def test_d_properties_are_bytes_and_reach_the_service_untouched(self):
        self.require_surface()
        block = b"\x00moof\xff\x00"
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender, name=b"c", packaging=moq5.Packaging.CMAF)
                sender.write(track, self.obj(properties=block))
                image = _native._test_write_object()
            finally:
                sender.close()
        # a passthrough block: the binding copies the bytes and parses nothing
        self.assertEqual(image["properties"], block)
        self.assertEqual(image["properties_declared_length"], 7)
        self.assertIs(image["properties_null"], False)

    def test_e_spans_are_bytes_only_and_the_value_is_frozen(self):
        self.require_surface()
        for value in (bytearray(b"f"), memoryview(b"f"), "f", 1, None):
            with self.subTest(payload=type(value).__name__):
                with self.assertRaises(TypeError):
                    moq5.SendObject(payload=value)
        for value in (bytearray(b"p"), memoryview(b"p"), "p", 1):
            with self.subTest(properties=type(value).__name__):
                with self.assertRaises(TypeError):
                    self.obj(properties=value)
        obj = self.obj(payload=b"frame")
        for field, value in (("payload", b"other"), ("is_sync", True),
                             ("decode_time_us", 1)):
            with self.subTest(field=field):
                with self.assertRaises(AttributeError):
                    setattr(obj, field, value)
        self.assertEqual(obj.payload, b"frame")

    def test_f_an_empty_payload_is_a_value_and_flags_are_strictly_bool(self):
        self.require_surface()
        # The C API requires a non-NULL rcbuf, NOT a positive length, and
        # core/src/base/rcbuf.c supports length zero. So b"" is a value that
        # reaches the service as a present, empty buffer; the service and its
        # policy decide acceptance, not this binding.
        with self.assertRaises(TypeError):
            moq5.SendObject()                       # payload is required
        with self.assertRaises(TypeError):
            moq5.SendObject(payload=None)           # absence is not a value
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender)
                sender.write(track, self.obj(payload=b""))
                image = _native._test_write_object()
            finally:
                sender.close()
        self.assertIs(image["payload_null"], False,
                      "an empty payload was sent as a NULL buffer")
        self.assertEqual(image["payload"], b"")
        self.assertEqual(image["payload_declared_length"], 0)
        for flag in ("is_sync", "starts_group", "ends_group"):
            with self.subTest(flag=flag):
                with self.assertRaises(TypeError):
                    self.obj(**{flag: 1})

    def test_f2_absent_properties_differ_from_a_present_empty_block(self):
        self.require_surface()
        # RAW refuses non-NULL properties, so "absent" and "present but empty"
        # are different submissions and must not be collapsed.
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender, name=b"c",
                                   packaging=moq5.Packaging.CMAF)
                sender.write(track, self.obj())                  # absent
                absent = _native._test_write_object()
                sender.write(track, self.obj(properties=b""))    # present, empty
                empty = _native._test_write_object()
            finally:
                sender.close()
        self.assertIs(absent["properties_null"], True)
        self.assertIs(empty["properties_null"], False,
                      "an empty properties block was sent as absent")
        self.assertEqual(empty["properties"], b"")
        self.assertEqual(empty["properties_declared_length"], 0)

    def test_g_out_of_range_and_wrongly_typed_numbers_are_refused(self):
        self.require_surface()
        cases = (
            ("decode_time_us", UINT64_MAX + 1, ValueError),
            ("decode_time_us", -1, ValueError),
            ("decode_time_us", 1.5, TypeError),
            ("decode_time_us", True, TypeError),
            ("presentation_time_us", UINT64_MAX + 1, ValueError),
            ("capture_time_us", UINT64_MAX + 1, ValueError),
            ("capture_time_us", -1, ValueError),
        )
        for field, value, expected in cases:
            with self.subTest(field=field, value=repr(value)):
                _native._test_write_reset()
                with self.assertRaises(expected):
                    self.obj(**{field: value})
                self.assertEqual(_native._test_write_counts()["entries"], 0,
                                 "native was entered for a rejected object")

    def test_h_only_a_concrete_sap_type_is_accepted(self):
        self.require_surface()
        # CMSF 3.6.1: the encoder declares a concrete type. The service rejects
        # the internal UNKNOWN sentinel (0xFF), so the binding never offers it.
        self.assertEqual([t.value for t in moq5.SapType], [0, 1, 2, 3])
        self.assertFalse(hasattr(moq5.SapType, "UNKNOWN"),
                         "the internal sentinel must not be offered")
        with self.assertRaises(ValueError):
            self.obj(sap_type=0xFF)
        seen = {}
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender)
                for kind in moq5.SapType:          # every concrete alternative
                    sender.write(track, self.obj(sap_type=kind))
                    seen[int(kind)] = _native._test_write_object()
            finally:
                sender.close()
        for value, image in sorted(seen.items()):
            with self.subTest(sap_type=value):
                self.assertIs(image["has_sap_type"], True)
                self.assertEqual(image["sap_type"], value,
                                 "the SAP mapping was narrowed")


class WriteOutcomeTests(WriteGatedBase):
    def test_a_acceptance_is_reported_and_enters_native_once(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender)
                before = _native._test_write_counts()["entries"]
                outcome = sender.write(track, self.obj())
                counts = _native._test_write_counts()
            finally:
                sender.close()
        self.assertIs(outcome, moq5.WriteOutcome.ACCEPTED)
        self.assertEqual(counts["entries"], before + 1,
                         "exactly one native call per public write")
        self.assertEqual(counts["accepted"], 1)

    def test_b_the_agreed_results_are_outcomes_not_exceptions(self):
        self.require_surface()
        expected = {WOULD_BLOCK: moq5.WriteOutcome.WOULD_BLOCK,
                    INTERRUPTED: moq5.WriteOutcome.INTERRUPTED,
                    CLOSED: moq5.WriteOutcome.CLOSED}
        for code, outcome in expected.items():
            with self.subTest(code=code):
                _native._test_write_reset()
                with self.connect() as endpoint:
                    sender = self.attach(endpoint)
                    try:
                        track = self.track(sender)
                        _native._test_write_result(code)
                        got = sender.write(track, self.obj())
                        self.assertIs(got, outcome)
                        counts = _native._test_write_counts()
                        self.assertEqual(counts["entries"], 1,
                                         "the refusal was retried")
                        self.assertEqual(counts["accepted"], 0)
                    finally:
                        _native._test_write_result(0)
                        sender.close()

    def test_c_would_block_is_backpressure_not_delivery(self):
        self.require_surface()
        # A drop policy ACCEPTING an object is not a delivery guarantee, and
        # WOULD_BLOCK is not an error to swallow. Both are reported, neither
        # is retried here.
        self.assertIsNot(moq5.WriteOutcome.ACCEPTED, moq5.WriteOutcome.WOULD_BLOCK)
        self.assertNotEqual(int(moq5.WriteOutcome.ACCEPTED),
                            int(moq5.WriteOutcome.WOULD_BLOCK))
        self.assertEqual(sorted(o.name for o in moq5.WriteOutcome),
                         ["ACCEPTED", "CLOSED", "INTERRUPTED", "WOULD_BLOCK"])

    def test_d_other_signed_errors_are_not_normalized(self):
        self.require_surface()
        for code in (INVAL, NOMEM, WRONG_STATE):
            with self.subTest(code=code):
                _native._test_write_reset()
                with self.connect() as endpoint:
                    sender = self.attach(endpoint)
                    try:
                        track = self.track(sender)
                        _native._test_write_result(code)
                        with self.assertRaises(moq5.MoqError) as raised:
                            sender.write(track, self.obj())
                        self.assertEqual(raised.exception.code, code)
                        self.assertEqual(raised.exception.operation, "write")
                        del raised
                        self.assertEqual(
                            _native._test_write_counts()["entries"], 1)
                    finally:
                        _native._test_write_result(0)
                        sender.close()


class WriteOwnershipTests(WriteGatedBase):
    def test_a_an_accepted_write_transfers_both_references_once(self):
        self.require_surface()
        baseline = _native._test_rcbuf_counts()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender, name=b"c",
                                   packaging=moq5.Packaging.CMAF)
                sender.write(track, self.obj(payload=b"frame",
                                             properties=b"props"))
                image = _native._test_write_object()
                owned = _native._test_write_owned_refs()
                accepted = _native._test_rcbuf_counts()
            finally:
                sender.close()
        # the bridge created exactly one reference per span and handed it over
        self.assertEqual(image["payload_refs_at_entry"], 1)
        self.assertEqual(image["properties_refs_at_entry"], 1)
        for span in ("payload", "properties"):
            self.assertEqual(accepted[span]["allocations"]
                             - baseline[span]["allocations"], 1,
                             f"the {span} span was not acquired exactly once")
            # liveness is asked FIRST: a premature release is a named
            # observation here, never a read through a freed pointer
            self.assertIs(owned[f"{span}_live"], True,
                          f"the bridge released the transferred {span}")
            self.assertEqual(accepted[span]["frees"], baseline[span]["frees"],
                             f"the transferred {span} was freed by the bridge")
            self.assertEqual(owned[span], 1,
                             "the bridge released a transferred reference")
        # settlement: the owner releases both, and nothing is left outstanding
        _native._test_write_reset()
        settlement = _native._test_write_settlement()
        self.assertIs(settlement["premature_release"], False)
        self.assertEqual(settlement["released"], 2)
        settled = _native._test_rcbuf_counts()
        self.assertEqual(settled["outstanding"], baseline["outstanding"],
                         "the accepted spans were never released")

    def test_b_a_refusal_releases_the_bridge_owned_references(self):
        self.require_surface()
        for code, properties in ((WOULD_BLOCK, b"props"), (INTERRUPTED, None),
                                 (CLOSED, b"props"), (INVAL, None)):
            spans = 2 if properties is not None else 1
            with self.subTest(code=code, spans=spans):
                _native._test_write_reset()
                before = _native._test_rcbuf_counts()
                with self.connect() as endpoint:
                    sender = self.attach(endpoint)
                    try:
                        track = self.track(sender, name=b"c",
                                           packaging=moq5.Packaging.CMAF)
                        _native._test_write_result(code)
                        try:
                            sender.write(track,
                                         self.obj(payload=b"frame",
                                                  properties=properties))
                        except moq5.MoqError:
                            pass
                        image = _native._test_write_object()
                        counts = _native._test_write_counts()
                        after = _native._test_rcbuf_counts()
                    finally:
                        _native._test_write_result(0)
                        sender.close()
                # no transfer happened, AND the bridge actually released both
                self.assertIs(counts["holds_references"], False)
                self.assertEqual(after["outstanding"], before["outstanding"],
                                 "a refused bridge buffer was leaked")
                self.assertEqual(after["allocations"] - before["allocations"],
                                 spans,
                                 "the bridge did not acquire one span each")
                self.assertEqual(after["payload"]["allocations"]
                                 - before["payload"]["allocations"], 1)
                self.assertEqual(after["payload"]["frees"]
                                 - before["payload"]["frees"], 1,
                                 "the refused payload was never freed")
                expected_props = 1 if properties is not None else 0
                self.assertEqual(after["properties"]["allocations"]
                                 - before["properties"]["allocations"],
                                 expected_props,
                                 "an absent properties block was acquired")
                self.assertEqual(after["properties"]["frees"]
                                 - before["properties"]["frees"],
                                 expected_props,
                                 "the refused properties span was never freed")
                self.assertEqual(image["payload_refs_at_entry"], 1,
                                 "the bridge handed over more than it made")

    def test_c_each_acquisition_failure_leaves_no_native_call(self):
        self.require_surface()
        # One refcounted buffer per span: two genuinely fallible acquisitions,
        # both before the native call.
        for site in ACQUISITION_SITES:
            with self.subTest(site=site):
                _native._test_write_reset()
                before = _native._test_rcbuf_counts()
                with self.connect() as endpoint:
                    sender = self.attach(endpoint)
                    try:
                        track = self.track(sender, name=b"c",
                                           packaging=moq5.Packaging.CMAF)
                        obj = self.obj(payload=b"frame", properties=b"props")
                        try:
                            _native._test_fail_allocation_at(site)
                            with self.assertRaises(MemoryError):
                                sender.write(track, obj)
                        finally:
                            _native._test_fail_allocation_at(None)
                        self.assertTrue(
                            _native._test_allocation_site_fired(site),
                            f"the {site} site never fired")
                        counts = _native._test_write_counts()
                        after = _native._test_rcbuf_counts()
                        self.assertEqual(counts["entries"], 0,
                                         "native ran after a failed acquisition")
                        self.assertIs(counts["holds_references"], False)
                        # the span acquired BEFORE the failing one must be
                        # released: zero native calls says nothing about it
                        self.assertEqual(after["outstanding"],
                                         before["outstanding"],
                                         "an earlier span was stranded")
                        expected = 0 if site == ACQUISITION_SITES[0] else 1
                        self.assertEqual(
                            after["allocations"] - before["allocations"],
                            expected,
                            "the acquisition order does not match the contract")
                        self.assertEqual(
                            after["payload"]["allocations"]
                            - before["payload"]["allocations"], expected,
                            "the wrong span was acquired first")
                        self.assertEqual(
                            after["payload"]["frees"]
                            - before["payload"]["frees"], expected,
                            "the earlier payload span was not released")
                        self.assertEqual(
                            after["properties"]["allocations"],
                            before["properties"]["allocations"],
                            "a properties span was acquired after the fault")
                    finally:
                        sender.close()

    def test_d_cleanup_precedes_the_result_allocation_at_the_fault(self):
        self.require_surface()
        # Reading the inventory after the exception has unwound cannot tell
        # cleanup-before-the-allocation from cleanup-after-it-failed. The
        # inventory recorded AT the fault does, per SPAN, so a late properties
        # release is caught even when the final unwind balances.
        #
        # Both shapes are covered: a refusal that becomes an EXCEPTION, and
        # one that becomes an OUTCOME. Neither result object is assumed
        # infallible, and no small-int caching is relied on.
        for code, site in ((INVAL, "write_error"), (WOULD_BLOCK, "write_result")):
            with self.subTest(code=code, site=site):
                # per-ITERATION settlement: this fake holds one owner, so the
                # previous owner and its armed fault are cleared here rather
                # than at the end of the test
                _native._test_write_reset()
                baseline = _native._test_rcbuf_counts()
                endpoint = self.connect()
                sender = self.attach(endpoint)
                try:
                    track = self.track(sender, name=b"c",
                                       packaging=moq5.Packaging.CMAF)
                    obj = self.obj(payload=b"frame", properties=b"props")
                    _native._test_write_result(code)
                    _native._test_fail_allocation_at(site)
                    with self.assertRaises(MemoryError):
                        sender.write(track, obj)
                    self.assertTrue(_native._test_allocation_site_fired(site),
                                    f"the {site} fault never fired")
                    at_fault = _native._test_fault_site_inventory(site)
                    self.assertIsNotNone(
                        at_fault,
                        "no ownership inventory was taken at the fault")
                    for span in ("payload", "properties"):
                        self.assertEqual(
                            at_fault[span]["allocations"]
                            - baseline[span]["allocations"], 1,
                            f"the {span} span was never acquired, so its "
                            "release cannot have been proved")
                        self.assertEqual(
                            at_fault[span]["frees"]
                            - baseline[span]["frees"], 1,
                            f"the {span} span was still held when the result "
                            "was allocated")
                    self.assertEqual(_native._test_write_counts()["entries"], 1,
                                     "the call is witnessed exactly once")
                    self.assertEqual(
                        _native._test_rcbuf_counts()["outstanding"],
                        baseline["outstanding"],
                        "the buffers were stranded by a failed result")
                finally:
                    _native._test_fail_allocation_at(None)
                    _native._test_write_result(0)
                    try:
                        sender.close()
                    finally:
                        endpoint.close()

class WriteGuardTests(WriteGatedBase):
    def test_a_a_wrong_type_is_refused_before_native(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender)
                before = _native._test_write_counts()
                with self.assertRaises(TypeError):
                    sender.write(object(), self.obj())
                with self.assertRaises(TypeError):
                    sender.write(track, object())
                self.assertEqual(_native._test_write_counts(), before,
                                 "a wrong-typed argument entered native")
            finally:
                sender.close()

    def test_a2_the_declared_track_is_the_one_the_service_receives(self):
        self.require_surface()
        # Two real tracks with IDENTICAL object bytes: only the recorded
        # identity distinguishes them, so a bridge that picks the wrong valid
        # track cannot satisfy the mapping rows.
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                first = self.track(sender, name=b"one")
                second = self.track(sender, name=b"two")
                sender.write(first, self.obj())
                a = _native._test_write_object()
                sender.write(second, self.obj())
                b = _native._test_write_object()
            finally:
                sender.close()
        self.assertEqual(a["payload"], b["payload"], "the objects differ")
        self.assertNotEqual(a["track_index"], b["track_index"],
                            "both writes went to the same native track")
        self.assertEqual(a["track_index"], 0)
        self.assertEqual(b["track_index"], 1)
        self.assertEqual(a["sender_index"], 0)
        self.assertEqual(b["sender_index"], 0,
                         "the second write was attributed to another owner")

    def test_a3_a_foreign_owners_track_and_a_receiver_track_are_refused(self):
        self.require_surface()
        # The fake holds one endpoint and one sender, so the two owners are
        # SEQUENTIAL. Declared precedence: the receiving sender refuses
        # because the track is not ITS track, before any native entry.
        #
        # LIMITATION, stated rather than implied: with a sequential fixture the
        # first owner is necessarily closed by the time the second exists, so
        # this row cannot separate "not my track" from "its owner is closed".
        # It pins refusal and zero native entry; it does NOT establish which of
        # the two reasons fired. The receiver-track arm below is a genuine
        # foreign-KIND rejection and is separated by its exception type.
        endpoint = self.connect()
        first = self.attach(endpoint)
        foreign = self.track(first)
        first.close()
        endpoint.close()
        _native._test_reset()
        _native._test_sender_reset()
        _native._test_send_track_reset()
        second_endpoint = self.connect()
        second = self.attach(second_endpoint)
        try:
            receiver = moq5.Receiver.attach(
                second_endpoint, moq5.ReceiverConfig.live(NS))
            try:
                receiver_track = receiver._track_from_capsule(
                    _native._test_new_track(
                        getattr(receiver, "_Receiver__handle")))
                before = _native._test_write_counts()
                with self.assertRaises(RuntimeError):
                    second.write(foreign, self.obj())
                with self.assertRaises(TypeError):
                    second.write(receiver_track, self.obj())
                self.assertEqual(_native._test_write_counts(), before,
                                 "a foreign or receiver track entered native")
            finally:
                receiver.close()
        finally:
            second.close()
            second_endpoint.close()

    def test_a4_the_private_bridge_refuses_wrong_capsules_itself(self):
        self.require_surface()
        missing = tuple(n for n in BRIDGE if not hasattr(_native, n))
        self.assertEqual(missing, (),
                         "the write bridge is not implemented: _native is "
                         f"missing {', '.join(missing)}")
        # A Python isinstance check is not a memory-safe capsule check.
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender)
                sender_handle = getattr(sender, "_Sender__handle")
                track_handle = getattr(track, "_SendTrack__handle")
                endpoint_handle = getattr(endpoint, "_Endpoint__handle")
                before = _native._test_write_counts()
                for label, wrong in (("endpoint", endpoint_handle),
                                     ("sender", sender_handle)):
                    with self.subTest(capsule=label):
                        with self.assertRaises((ValueError, TypeError)):
                            _native.sender_write(sender_handle, wrong,
                                                 b"frame", None, False, False,
                                                 False, 0, 0, None, None)
                self.assertEqual(_native._test_write_counts(), before,
                                 "a wrong capsule reached the service")

                # bytes-only, asserted on the PRIVATE inputs too: the bridge
                # owns its own contract rather than trusting the wrapper
                for label, span in (("bytearray", bytearray(b"f")),
                                    ("memoryview", memoryview(b"f")),
                                    ("str", "f"), ("int", 1)):
                    with self.subTest(private_payload=label):
                        with self.assertRaises(TypeError):
                            _native.sender_write(sender_handle, track_handle,
                                                 span, None, False, False,
                                                 False, 0, 0, None, None)
                for label, span in (("bytearray", bytearray(b"p")),
                                    ("memoryview", memoryview(b"p")),
                                    ("str", "p")):
                    with self.subTest(private_properties=label):
                        with self.assertRaises(TypeError):
                            _native.sender_write(sender_handle, track_handle,
                                                 b"f", span, False, False,
                                                 False, 0, 0, None, None)
                self.assertEqual(_native._test_write_counts(), before,
                                 "a non-bytes private span reached the service")
            finally:
                sender.close()

    def test_a5_the_private_bridge_keeps_the_owner_guards_itself(self):
        self.require_surface()
        missing = tuple(n for n in BRIDGE if not hasattr(_native, n))
        self.assertEqual(missing, (),
                         "the write bridge is not implemented: _native is "
                         f"missing {', '.join(missing)}")
        # The public wrapper's checks are kept, but they are not a substitute
        # for the bridge's own: these call the bridge DIRECTLY.
        endpoint = self.connect()
        sender = self.attach(endpoint)
        # the owner is settled before its endpoint, whatever happens below
        self.addCleanup(endpoint.close)
        self.addCleanup(sender.close)
        track = self.track(sender)
        sender_handle = getattr(sender, "_Sender__handle")
        track_handle = getattr(track, "_SendTrack__handle")
        before = _native._test_write_counts()
        args = (b"frame", None, False, False, False, 0, 0, None, None)

        refused = []

        def other():
            try:
                _native.sender_write(sender_handle, track_handle, *args)
            except RuntimeError as error:
                refused.append(("thread", error))
            except BaseException as error:           # noqa: BLE001
                refused.append(("thread-wrong", error))

        worker = threading.Thread(target=other, daemon=True)
        worker.start()
        worker.join(5.0)
        self.assertFalse(worker.is_alive(), "the direct guard worker hung")
        self.assertEqual([kind for kind, _ in refused], ["thread"],
                         "the bridge admitted a foreign thread")

        if hasattr(os, "fork"):
            child = os.fork()
            if child == 0:
                status = 0
                try:
                    _native.sender_write(sender_handle, track_handle, *args)
                    status = 3                  # the inherited handle worked
                except RuntimeError:
                    pass
                except BaseException:
                    status = 9
                if _native._test_write_counts() != before:
                    status = 4
                os._exit(status)
            self.assertEqual(reap_child(child), 0,
                             "the bridge admitted an inherited handle")

        # The fixture serves one endpoint and one sender at a time, so a
        # SECOND live owner cannot exist here and a direct foreign-owner arm
        # is not constructible. That case stays with the public row above,
        # with its limitation stated there.
        sender.close()
        with self.assertRaises(RuntimeError):
            _native.sender_write(sender_handle, track_handle, *args)
        self.assertEqual(_native._test_write_counts(), before,
                         "a guarded direct call still entered native")

    def test_b_a_removed_track_and_a_closed_owner_are_refused(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            track = self.track(sender)
            sender.remove_track(track)
            before = _native._test_write_counts()
            # a removed handle is inert: the service answers WRONG_STATE, and
            # the binding does not invent a Python-side policy for it
            _native._test_write_result(WRONG_STATE)
            with self.assertRaises(moq5.MoqError) as raised:
                sender.write(track, self.obj())
            code, operation = raised.exception.code, raised.exception.operation
            del raised
            # evidence distinction: WRONG_STATE here is the fixture's SCRIPTED
            # C answer, not a measured service policy. What this row owns is
            # that the call was delivered once, as a write, and was not turned
            # into an acceptance.
            self.assertEqual(code, WRONG_STATE)
            self.assertEqual(operation, "write")
            self.assertEqual(_native._test_write_counts()["entries"],
                             before["entries"] + 1,
                             "the removed track was not delivered exactly once")
            _native._test_write_result(0)
            sender.close()
            after = _native._test_write_counts()
            with self.assertRaises(RuntimeError):
                sender.write(track, self.obj())
            self.assertEqual(_native._test_write_counts(), after,
                             "a closed owner entered native")
            self.assertEqual(before["accepted"], after["accepted"])
        finally:
            _native._test_write_result(0)
            # the owner is closed before the endpoint it is attached to, so a
            # failed assertion above cannot turn this row into an attached-
            # owner cleanup failure
            try:
                sender.close()
            finally:
                endpoint.close()

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only contract")
    def test_c_the_owner_thread_and_pid_guards_precede_native_entry(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        track = self.track(sender)
        before = _native._test_write_counts()
        try:
            refused = []

            def other():
                try:
                    sender.write(track, self.obj())
                except RuntimeError as error:
                    refused.append(error)

            thread = threading.Thread(target=other, daemon=True)
            thread.start()
            thread.join(5.0)
            self.assertFalse(thread.is_alive(), "the guard worker never returned")
            self.assertEqual(len(refused), 1)
            self.assertEqual(_native._test_write_counts(), before,
                             "a foreign thread entered native")

            child = os.fork()
            if child == 0:
                status = 0
                try:
                    sender.write(track, self.obj())
                    status = 3                      # the inherited handle worked
                except RuntimeError:
                    pass
                except BaseException:
                    status = 9
                if _native._test_write_counts() != before:
                    status = 4
                os._exit(status)
            self.assertEqual(reap_child(child), 0,
                             "the fork guard child did not exit cleanly")
        finally:
            sender.close()
            endpoint.close()


class WriteSnapshotTests(WriteGatedBase):
    """The value is snapshotted before the native call, and the GIL is
    released while that call is in flight."""

    def test_a_another_thread_progresses_while_the_native_call_is_in_flight(self):
        self.require_surface()
        # A REAL barrier, not a sleep: the fake parks inside the native call
        # and another thread must reach it and release it. If the GIL were
        # held across the call, the watcher could never run and the fixture's
        # own bound would report that as a harness failure.
        progressed = []
        released = []

        def watcher():
            entered = _native._test_write_wait_entered(5.0)
            progressed.append(entered)
            released.append(_native._test_write_release())

        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender)
                helper = threading.Thread(target=watcher, daemon=True)
                _native._test_write_block(True)
                helper.start()
                try:
                    outcome = sender.write(track, self.obj())
                finally:
                    # disarm, release and JOIN unconditionally: an unexpected
                    # raise out of write must not skip the join and strand the
                    # watcher inside the native call
                    _native._test_write_block(False)
                    _native._test_write_release()
                    helper.join(10.0)
                self.assertFalse(helper.is_alive(), "the watcher never returned")
                self.assertEqual(progressed, [True],
                                 "no other thread reached the native call")
                self.assertEqual(released, [True],
                                 "the fixture's own bound expired: harness failure")
                self.assertIs(outcome, moq5.WriteOutcome.ACCEPTED)
                self.assertEqual(_native._test_write_counts()["accepted"], 1)
            finally:
                sender.close()

    def test_b_the_public_value_is_an_immutable_snapshot(self):
        self.require_surface()
        # The public surface takes immutable bytes, so there is no mutable
        # backing to hold in flight. This pins that the value the service saw
        # is the value the object carried, and that the object still carries it.
        payload = b"\x00declared\xff"
        obj = self.obj(payload=payload)
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender)
                sender.write(track, obj)
                image = _native._test_write_object()
            finally:
                sender.close()
        self.assertEqual(image["payload"], payload)
        self.assertEqual(obj.payload, payload, "the object lost its value")


class WriteInterruptPropagationTests(WriteGatedBase):
    """Ordinary exception propagation out of argument evaluation.

    This is NOT an OS-signal experiment. It pins that an exception raised
    while the object's values are read reaches the caller and leaves nothing
    behind.
    """

    def test_a_an_exception_while_reading_values_leaves_no_native_entry(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender)
                before = _native._test_write_counts()
                allocations = _native._test_rcbuf_counts()
                obj = self.obj()
                raiser = property(
                    lambda self: (_ for _ in ()).throw(KeyboardInterrupt()))
                with mock.patch.object(moq5.SendObject, "payload", raiser):
                    with self.assertRaises(KeyboardInterrupt):
                        sender.write(track, obj)
                self.assertEqual(_native._test_write_counts(), before,
                                 "native was entered despite the failure")
                self.assertEqual(_native._test_rcbuf_counts()["outstanding"],
                                 allocations["outstanding"],
                                 "a span was acquired and stranded")
            finally:
                sender.close()


class WriteSignalBoundaryTests(WriteGatedBase):
    """A REAL pending signal delivered while the native call is in flight.

    Derivation, corrected. It is NOT true that Python only handles signals at
    a bytecode boundary: C code can process them explicitly, and this binding
    already does so after a blocking native call (native/module.c:326 and
    :573, each calling PyErr_CheckSignals after Py_END_ALLOW_THREADS).

    The processing order write must therefore use:

      1. acquire both spans
      2. Py_BEGIN_ALLOW_THREADS, native write, Py_END_ALLOW_THREADS
      3. with the GIL reacquired, settle OWNERSHIP first: on any non-OK
         release both spans; on MOQ_OK release nothing
      4. only then build the result, and only then may a pending signal be
         processed

    A signal can therefore never strand a buffer. What it CAN do is reach the
    caller after the service has already accepted the object, which is the
    limitation this class pins rather than papers over.
    """

    # The declared result table: these codes are OUTCOMES, anything else is
    # an exception carrying its signed code.
    OUTCOME_CODES = {0: "ACCEPTED", WOULD_BLOCK: "WOULD_BLOCK",
                     INTERRUPTED: "INTERRUPTED", CLOSED: "CLOSED"}

    def signal_child(self, deliver, raising, scripted=0, bad_verdict=False):
        """Run one write in an owned child with a real pending signal.

        Exit status is the verdict, so nothing is inferred from timing and no
        second call stands in for the signal. Settlement -- the helper thread,
        the recorder's references, the owner and its endpoint -- happens in
        UNCONDITIONAL cleanup, so a failed verdict is still reported by its own
        number while a cleanup failure is reported separately (+100).
        """
        child = os.fork()
        if child == 0:
            status = 0
            cleanup_status = 0
            try:
                import signal as signal_module

                received = []

                def handler(*args):
                    received.append(True)
                    if raising:
                        raise KeyboardInterrupt

                signal_module.signal(signal_module.SIGUSR1, handler)
                baseline = _native._test_rcbuf_counts()
                endpoint = self.connect()
                sender = self.attach(endpoint)
                helper = None
                try:
                    # a CMAF track with BOTH spans, so the real bridge -- not
                    # only a stand-in -- proves the two-span signal cleanup
                    track = self.track(sender, name=b"c",
                                       packaging=moq5.Packaging.CMAF)
                    _native._test_write_result(scripted)
                    reached, released = [], []

                    def arm():
                        entered = _native._test_write_wait_entered(5.0)
                        reached.append(entered)
                        if entered and deliver:
                            os.kill(os.getpid(), signal_module.SIGUSR1)
                        released.append(_native._test_write_release())

                    helper = threading.Thread(target=arm, daemon=True)
                    _native._test_write_block(True)
                    helper.start()
                    interrupted = False
                    outcome = None
                    raised = None
                    try:
                        outcome = sender.write(
                            track, self.obj(payload=b"frame",
                                            properties=b"props"))
                    except KeyboardInterrupt:
                        interrupted = True
                    except moq5.MoqError as error:
                        raised = error
                    finally:
                        _native._test_write_block(False)
                        _native._test_write_release()
                        helper.join(10.0)

                    counts = _native._test_write_counts()
                    accepted = scripted == 0
                    expected_name = self.OUTCOME_CODES.get(scripted)
                    expected = (getattr(moq5.WriteOutcome, expected_name)
                                if expected_name is not None else None)
                    if helper.is_alive():
                        status = 10         # harness: the helper never settled
                    elif reached != [True]:
                        status = 11         # the call was never entered
                    elif released[:1] != [True] or counts["gate_fault"]:
                        status = 12         # harness: a fixture bound expired
                    elif deliver and not received:
                        status = 13         # the signal never arrived
                    elif not deliver and received:
                        status = 14
                    elif counts["entries"] != 1:
                        status = 15         # not exactly one native call
                    elif counts["accepted"] != (1 if accepted else 0):
                        status = 16
                    elif counts["holds_references"] is not accepted:
                        status = 17         # the transfer did not follow
                    elif not accepted and (
                            _native._test_rcbuf_counts()["outstanding"]
                            != baseline["outstanding"]):
                        status = 18         # a refusal stranded a span
                    elif not accepted and (
                            _native._test_rcbuf_counts()["payload"]["frees"]
                            - baseline["payload"]["frees"] != 1
                            or _native._test_rcbuf_counts()["properties"]["frees"]
                            - baseline["properties"]["frees"] != 1):
                        status = 24         # one of the two spans was kept
                    elif deliver and raising and not interrupted:
                        # A handler that RAISES must reach the caller. Unknown
                        # acceptance does not license swallowing it.
                        status = 19
                    elif interrupted and not (deliver and raising):
                        status = 21         # an interrupt nobody arranged
                    elif interrupted:
                        status = 25         # acceptance stands, caller saw it
                    elif expected is not None and outcome is not expected:
                        status = 22         # the EXACT outcome is required
                    elif expected is None and (raised is None
                                               or raised.code != scripted):
                        status = 23         # a signed error must stay one
                    if bad_verdict and status in (0, 25):
                        status = 90         # a deliberate verdict failure
                finally:
                    # unconditional settlement, whatever the verdict above
                    try:
                        _native._test_write_block(False)
                        _native._test_write_release()
                        if helper is not None:
                            helper.join(10.0)
                            if helper.is_alive():
                                cleanup_status = 1
                        sender.close()
                        endpoint.close()
                        _native._test_write_reset()
                        settlement = _native._test_write_settlement()
                        if settlement["premature_release"]:
                            cleanup_status = 2
                        elif (_native._test_rcbuf_counts()["outstanding"]
                              != baseline["outstanding"]):
                            cleanup_status = 3
                    except BaseException:
                        cleanup_status = 4
            except BaseException:
                status = 29
            os._exit(status + (100 if cleanup_status else 0))
        return reap_child(child)

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only contract")
    def test_a_a_raising_signal_does_not_disturb_an_accepted_transfer(self):
        self.require_surface()
        outcome = self.signal_child(deliver=True, raising=True)
        # 25 is the DOCUMENTED limitation: acceptance already happened and the
        # caller also saw the interrupt. It is not a second transfer, and the
        # child proved its buffers were settled before exiting. A swallowed
        # interrupt is NOT admitted: that is verdict 19.
        self.assertEqual(outcome, 25,
                         "the signalled write child did not report an "
                         "accepted-and-interrupted inventory")

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only contract")
    def test_a2_a_raising_signal_over_a_refusal_holds_no_span(self):
        self.require_surface()
        self.assertEqual(self.signal_child(deliver=True, raising=True,
                                           scripted=WOULD_BLOCK), 25,
                         "a signalled refusal did not free both spans and "
                         "propagate the interrupt")

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only contract")
    def test_b_the_same_arrangement_without_a_signal_returns_the_outcome(self):
        self.require_surface()
        # Every agreed code returns its EXACT WriteOutcome; nothing here is an
        # exception, and nothing is normalised.
        for scripted in (0, WOULD_BLOCK, INTERRUPTED, CLOSED):
            with self.subTest(scripted=scripted):
                self.assertEqual(
                    self.signal_child(deliver=False, raising=True,
                                      scripted=scripted), 0,
                    "the unsignalled control child did not return the exact "
                    "outcome and settle cleanly")

    def test_c_acceptance_is_never_retried(self):
        self.require_surface()
        # Blind retry is prohibited: a second write is a NEW object. There is
        # no receipt API and no statistics in this slice, so a caller that
        # cannot tell whether acceptance happened has no way to find out, and
        # the binding must not guess on their behalf.
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender)
                self.assertIs(sender.write(track, self.obj()),
                              moq5.WriteOutcome.ACCEPTED)
                self.assertEqual(_native._test_write_counts()["entries"], 1)
                sender.write(track, self.obj(payload=b"second"))
                self.assertEqual(_native._test_write_counts()["entries"], 2)
                self.assertEqual(_native._test_write_object()["payload"],
                                 b"second")
            finally:
                sender.close()


# ------------------------------------------------------------------ control --

PUBLIC_GATED_CLASSES = ("SendObjectValueTests", "WriteOutcomeTests",
                        "WriteOwnershipTests", "WriteGuardTests",
                        "WriteSnapshotTests",
                        "WriteInterruptPropagationTests",
                        "WriteSignalBoundaryTests")


class WriteControlTests(unittest.TestCase):
    """This file's own machinery. Never evidence about the product.

    What the inert-name control establishes is the GATE: every gated row is
    invoked and refuses by a named assertion, with no skip, no arbitrary error
    and no resource acquired. It does NOT execute the behavioural bodies --
    each stops inside require_surface. The scoped-stand-in controls below are
    what exercise the bodies and their oracles, which is why they exist.
    """

    def test_a_inert_public_names_are_refused_by_every_gated_row(self):
        import test_send_write as module

        saved = {n: getattr(moq5, n, None) for n in SURFACE}
        try:
            for name in SURFACE:
                setattr(moq5, name, object())
            loader = unittest.defaultTestLoader
            suite = unittest.TestSuite(
                loader.loadTestsFromTestCase(getattr(module, c))
                for c in PUBLIC_GATED_CLASSES)
            result = unittest.TestResult()
            with warnings.catch_warnings(record=True) as raised:
                warnings.simplefilter("always")
                suite.run(result)
        finally:
            for name, value in saved.items():
                if value is None:
                    if hasattr(moq5, name):
                        delattr(moq5, name)
                else:
                    setattr(moq5, name, value)
        self.assertGreater(result.testsRun, 0)
        self.assertEqual(result.skipped, [], "a gated row skipped")
        self.assertEqual(result.errors, [],
                         "a gated row raised an arbitrary error instead of "
                         "refusing by a named assertion")
        failed = {getattr(case, "test_case", case).id()
                  for case, _ in result.failures}
        self.assertEqual(len(failed), result.testsRun,
                         "an inert surface satisfied a behavioural row")
        self.assertEqual([w.category for w in raised], [],
                         "the gate acquired resources before refusing")

    def test_b_the_image_checker_accepts_a_right_image_and_rejects_wrong_ones(self):
        """The checker itself, exercised directly: a valid observation passes,
        and same-count observations with one wrong field or one wrong byte
        must fail. Otherwise a broken checker could pass everything."""
        # owned recorder state is released even if an assertion below fails
        self.addCleanup(_native._test_write_reset)
        _native._test_write_reset()
        self.assertEqual(_native._test_write_simulate(b"frame", None, 0), 0)
        observed = _native._test_write_object()
        # the fixture driver writes through no public track, so its identity
        # is the fixture's own; the declared image still has to name it
        identity = {"track_index": observed["track_index"],
                    "sender_index": observed["sender_index"]}
        self.assertEqual(identity, {"track_index": -1, "sender_index": 1},
                         "the driver's declared identity changed")
        check_image(self, observed, **identity)             # the positive

        for field, wrong in (("is_sync", True),
                             ("payload", b"frame"[:-1] + b"X"),
                             ("payload_declared_length", 4),
                             ("properties_null", False),
                             ("sap_type", 2),
                             ("track_index", observed["track_index"] + 1),
                             ("sender_index", observed["sender_index"] + 1)):
            with self.subTest(field=field):
                broken = dict(observed)
                broken[field] = wrong
                self.assertEqual(len(broken), len(observed),
                                 "the wrong image changed the field count")
                with self.assertRaises(AssertionError):
                    check_image(self, broken, **identity)
        # a missing field is named rather than silently ignored
        dropped = {k: v for k, v in observed.items() if k != "is_sync"}
        with self.assertRaises(AssertionError):
            check_image(self, dropped, **identity)

    def test_d_the_signal_child_verdict_discriminates(self):
        """The child's verdict itself, exercised against a scoped stand-in.

        A RED row cannot show that its oracle accepts the right answers and
        rejects the wrong ones, because its surface is missing. So the four
        answers are fed to the ACTUAL child helper through a temporary
        adapter over the substrate driver: no write bridge, no product change,
        and the stand-in is removed again here.
        """
        if not hasattr(os, "fork"):
            self.skipTest("fork is a POSIX-only contract")

        class Outcome:
            def __init__(self, name):
                self.name = name

        stand_in = types.SimpleNamespace(
            ACCEPTED=Outcome("ACCEPTED"), WOULD_BLOCK=Outcome("WOULD_BLOCK"),
            INTERRUPTED=Outcome("INTERRUPTED"), CLOSED=Outcome("CLOSED"))
        by_code = {0: stand_in.ACCEPTED, WOULD_BLOCK: stand_in.WOULD_BLOCK,
                   INTERRUPTED: stand_in.INTERRUPTED, CLOSED: stand_in.CLOSED}

        def adapter(swallow):
            def write(sender, track, obj):
                try:
                    code = _native._test_write_simulate(b"frame", b"props", 0)
                except KeyboardInterrupt:
                    if not swallow:
                        raise
                    code = 0            # the swallowing bridge under test
                return by_code[code]
            return write

        class Probe(WriteSignalBoundaryTests):
            def require_surface(self):
                pass                     # the stand-in IS the surface here

            def obj(self, **fields):
                return fields            # SendObject does not exist yet

            def runTest(self):
                pass

        probe = Probe("runTest")
        self.addCleanup(_native._test_write_reset)
        with mock.patch.object(moq5, "WriteOutcome", stand_in, create=True):
            with mock.patch.object(moq5.Sender, "write", adapter(False),
                                   create=True):
                self.assertEqual(
                    probe.signal_child(deliver=False, raising=True), 0,
                    "a correct ACCEPTED return was rejected")
                self.assertEqual(
                    probe.signal_child(deliver=False, raising=True,
                                       scripted=WOULD_BLOCK), 0,
                    "a correct WOULD_BLOCK OUTCOME was rejected as though it "
                    "had to be an exception")
                self.assertEqual(
                    probe.signal_child(deliver=True, raising=True), 25,
                    "a propagated interrupt over an accepted transfer was not "
                    "reported as the documented limitation")
            with mock.patch.object(moq5.Sender, "write", adapter(True),
                                   create=True):
                self.assertEqual(
                    probe.signal_child(deliver=True, raising=True), 19,
                    "a SWALLOWED KeyboardInterrupt passed the verdict")

    def test_e_the_fault_ordering_row_is_coupled_to_its_own_assertions(self):
        """The ACTUAL fault-ordering method, invoked under a scoped stand-in.

        This does not copy that row's sequence: it runs the real method object,
        so an edit to the row is an edit to what this control qualifies. The
        stand-in replaces only the bridge call, routing it through the
        substrate's ordering driver, which is what makes the row's fault-time
        assertions meaningful independently of the product's own path.
        """
        name = "test_d_cleanup_precedes_the_result_allocation_at_the_fault"
        sites = {INVAL: "write_error", WOULD_BLOCK: "write_result"}
        entered = []

        def write(sender, track, obj):
            site = sites[_native._test_write_result_current()]
            outcome = _native._test_write_simulate_ordered(
                obj.payload, obj.properties, 1, site)
            entered.append(outcome)
            if outcome["result_fault"]:
                raise MemoryError
            return moq5.WriteOutcome(outcome["rc"])

        self.addCleanup(_native._test_write_reset)
        with mock.patch.object(moq5.Sender, "write", write):
            case = WriteOwnershipTests(name)
            result = unittest.TestResult()
            case.run(result)
        self.assertEqual([str(e) for _, e in result.failures + result.errors],
                         [], "the actual fault-ordering body did not pass "
                             "against the ordering driver")
        self.assertEqual(result.testsRun, 1)
        self.assertEqual(len(entered), 2, "both iterations did not execute")

    def test_c_the_bridge_is_present_and_callable(self):
        # This row was the RED gate's "the bridge is absent" assertion. The
        # bridge exists now, so it pins presence and callability instead --
        # and that a wrong-typed call is refused rather than crashing.
        missing = tuple(n for n in BRIDGE if not hasattr(_native, n))
        self.assertEqual(missing, (), f"the write bridge lost {missing}")
        for name in BRIDGE:
            self.assertTrue(callable(getattr(_native, name)),
                            f"_native.{name} is not callable")
        with self.assertRaises(TypeError):
            _native.sender_write()
