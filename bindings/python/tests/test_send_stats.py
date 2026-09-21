"""Sender statistics: Sender.stats() -> SenderStats.

These rows were written RED, against a method that did not exist, and each
failed by a NAMED missing behaviour rather than skipping. The method exists
now and they pass. The gate is kept: a missing or inert name is still a named
failure, the substrate rows still execute the recorder's oracles on their own,
and the scoped stand-in control still proves these bodies are satisfiable
independently of the product.

Scope: ONE read-only snapshot per call. No demand, wait, completion, drain or
graceful finish, and nothing here is a readiness poll or a lifecycle barrier.

The contract, re-derived from THIS worktree:

  service/include/moq/media_sender.h:571-623
      Eleven reported values. Ten are uint64 counters plus a SIGNED
      last_error: objects_written, objects_sent, objects_queued, bytes_queued,
      objects_dropped, groups_dropped, keyframes_dropped, groups_abandoned,
      backpressure_stalls, last_error, and the appended sap_records_evicted.
  service/src/media_sender.c:39-43
      The frozen v0 floor is offsetof(last_error) + sizeof(moq_result_t): v0
      ENDS at last_error, and sap_records_evicted is appended after it.
  service/src/media_sender.c:4779-4805
      out_size below the v0 floor, or a NULL argument, is INVAL. Otherwise the
      library writes min(out_size, its own sizeof) bytes and STAMPS
      out->struct_size with the number of bytes written. There is no terminal
      or readiness check: a live handle answers.

  The population rule, stated in the header and NOT enforced here:
      objects_written == objects_sent + objects_queued + objects_dropped
      holds within the MEDIA population. The binding reports; it does not
      check, repair or compute this.

What the values mean, as the header defines them:

  * `objects_queued` and `bytes_queued` count MEDIA only. The private
    END_OF_TRACK marker end_track() queues is NOT media and appears in none of
    the five population counters, so `objects_queued == 0` means no media is
    pending -- it does NOT mean the lifecycle finished or that a terminal has
    been emitted.
  * `objects_sent` means handed to the session, not delivered to a receiver.
  * `groups_dropped` counts distinct (track, group) values that lost at least
    one queued object; `groups_abandoned` counts open wire subgroups actually
    driven through the RESET lifecycle. They are different facts, and one
    partially emitted group can contribute to BOTH.
  * `last_error` is the last non-OK write() return, 0 for none. It is DATA: a
    recorded historical failure is not an exception, and it is signed.

Python shape, following Receiver.stats and ReceiverStats: a frozen, slotted
snapshot of ordinary ints, taken anew on every call, retaining no native
pointer. `sap_records_evicted` is None when the stamped prefix does not cover
the whole appended field, and a stamped zero is a present zero.
"""

import dataclasses
import gc
import os
import sys
import threading
import unittest
from unittest import mock

import moq5
from moq5 import _native

import test_foundation as foundation  # noqa: F401
from test_sender import reap_child

NS = (b"svc", b"demo")

INVAL = -2
CLOSED = -4
WRONG_STATE = -5
UNKNOWN_CODE = -23456

FIELDS = ("objects_written", "objects_sent", "objects_queued", "bytes_queued",
          "objects_dropped", "groups_dropped", "keyframes_dropped",
          "groups_abandoned", "backpressure_stalls", "last_error",
          "sap_records_evicted")

# Independently declared, DISTINCT values. Each is chosen to kill a different
# mistake: narrowing to 32 bits, signed reinterpretation of a high uint64, and
# any swap between two fields.
SCRIPTED = {
    "objects_written": 2 ** 32 + 7,        # above 2^32
    "objects_sent": 2 ** 32 + 8,
    "objects_queued": 2 ** 63 + 9,         # above 2^63: signed reads go wrong
    "bytes_queued": (1 << 64) - 1,         # UINT64_MAX
    "objects_dropped": 2 ** 40 + 11,
    "groups_dropped": 2 ** 41 + 12,
    "keyframes_dropped": 2 ** 42 + 13,
    "groups_abandoned": 2 ** 43 + 14,
    "backpressure_stalls": 2 ** 44 + 15,
    "last_error": UNKNOWN_CODE,            # signed, and not a known code
    "sap_records_evicted": 2 ** 45 + 16,
}


def script(**overrides):
    values = dict(SCRIPTED)
    values.update(overrides)
    _native._test_sender_stats(*(values[name] for name in FIELDS))
    return values


def assert_fields(case, stats, expected):
    """The mapping assertion itself, in one place.

    The behavioural row calls this, and so do its negative controls, so a
    control cannot pass by comparing two dictionaries the row never uses.
    Expected values are declared by this file; observed ones come from the
    product reading the fixture.
    """
    observed = {name: getattr(stats, name) for name in FIELDS}
    case.assertEqual(observed, expected,
                     "a field was narrowed, swapped, reinterpreted or lost")
    for name in FIELDS:
        value = observed[name]
        if expected[name] is None:
            case.assertIsNone(value, f"{name} should be absent")
        else:
            case.assertIs(type(value), int,
                          f"{name} is not an ordinary int")


def missing_surface() -> list[str]:
    return [name for name in ("stats",)
            if not callable(getattr(moq5.Sender, name, None))]


class StatsFixtureBase(unittest.TestCase):
    def setUp(self):
        gc.collect()
        _native._test_reset()
        _native._test_sender_reset()
        _native._test_send_track_reset()
        _native._test_sender_stats_reset()

    def tearDown(self):
        _native._test_sender_stats_result(0)
        _native._test_sender_stats_reset()

    def connect(self):
        return moq5.Endpoint.connect(moq5.EndpointConfig(url="moqt://fixture.invalid"))

    def attach(self, endpoint):
        return moq5.Sender.attach(
            endpoint,
            moq5.SenderConfig(namespace=NS,
                              backpressure=moq5.Backpressure.DROP_GROUP))


# ---------------------------------------------------------------- substrate --

class StatsSubstrateTests(StatsFixtureBase):
    """The stats recorder and its seams, exercised without the bridge."""

    @staticmethod
    def check_layout(case, layout):
        """The relations the ABI actually requires, nothing more.

        The appended field must lie OUTSIDE the frozen v0 prefix, which allows
        it to start exactly at the prefix's end: padding between them is a
        host detail, not a contract. Its end must be covered by the struct,
        and it is a uint64. Tail padding is not a field, so its end is
        compared with sizeof by <=, never by equality of a padded tail.
        """
        case.assertLessEqual(layout["v0_end"], layout["sap_offset"],
                             "the appended field overlaps the v0 prefix")
        case.assertEqual(layout["sap_offset"] + 8, layout["sap_end"],
                         "the appended field is not a uint64")
        case.assertLessEqual(layout["sap_end"], layout["sizeof"],
                             "the appended field runs past the struct")

    def test_a_the_layout_facts_come_from_the_sdk(self):
        # Exactly the ABI relations, and nothing else: whether the appended
        # field happens to end at sizeof on THIS host is a measurement, not a
        # contract, so it is not asserted here.
        self.check_layout(self, _native._test_sender_stats_layout())

    def test_a2_the_layout_checker_accepts_both_padding_shapes(self):
        # The ACTUAL row is what is qualified here, not only its helper: each
        # synthetic layout is fed to the real method body through the seam it
        # reads. These are synthetic layouts qualifying the oracle; they are
        # not claims about any other architecture.
        def run(layout):
            with mock.patch.object(_native, "_test_sender_stats_layout",
                                   lambda: dict(layout)):
                self.test_a_the_layout_facts_come_from_the_sdk()

        for name, layout in (
                ("padded", {"sizeof": 96, "v0_end": 84, "sap_offset": 88,
                            "sap_end": 96}),
                ("packed", {"sizeof": 96, "v0_end": 88, "sap_offset": 88,
                            "sap_end": 96}),
                ("trailing padding", {"sizeof": 104, "v0_end": 84,
                                      "sap_offset": 88, "sap_end": 96})):
            with self.subTest(layout=name):
                run(layout)                          # the ACTUAL row, passing
        for name, layout in (
                ("overlapping", {"sizeof": 96, "v0_end": 92, "sap_offset": 88,
                                 "sap_end": 96}),
                ("not a uint64", {"sizeof": 96, "v0_end": 84,
                                  "sap_offset": 88, "sap_end": 92}),
                ("past the struct", {"sizeof": 90, "v0_end": 84,
                                     "sap_offset": 88, "sap_end": 96})):
            with self.subTest(rejects=name):
                with self.assertRaises(AssertionError):
                    run(layout)                      # the ACTUAL row, failing

    def test_b_the_recorder_reports_the_callers_real_capacity(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                script()
                before = _native._test_sender_stats_calls()
                self.assertEqual(before["entries"], 0)
                # nothing has called it yet; the capacity is recorded when the
                # bridge does, and the gated rows assert it
                self.assertEqual(before["capacity"], 0)
                self.assertEqual(before["target"], -1)
            finally:
                sender.close()

    @staticmethod
    def expected_image(values):
        """An independently built image of the declared values.

        Offsets and widths come from the SDK, the byte order from this
        interpreter. The recorder's own buffer is never read back to produce
        this answer, and padding bytes are deliberately left unspecified: they
        are not a public ABI rule.
        """
        fields = _native._test_sender_stats_fields()
        size = _native._test_sender_stats_layout()["sizeof"]
        image = bytearray(b"\x00" * size)
        covered = bytearray(b"\x00" * size)      # 1 where a FIELD lives
        for name, value in values.items():
            offset, width = fields[name]
            signed = name == "last_error"
            image[offset:offset + width] = int(value).to_bytes(
                width, sys.byteorder, signed=signed)
            covered[offset:offset + width] = b"\x01" * width
        return bytes(image), bytes(covered)

    def assert_copied_bytes(self, prefix, values, copied):
        """Every FIELD byte inside the copied prefix must be the declared
        value, including the covered part of a partially copied field."""
        image, covered = self.expected_image(values)
        mismatches = [i for i in range(min(copied, len(prefix)))
                      if covered[i] and prefix[i] != image[i]]
        self.assertEqual(mismatches, [],
                         "the copied prefix is not the scripted image at "
                         f"byte offsets {mismatches[:8]}")
        # and the copy really carried data: at least one field byte in it
        self.assertGreater(sum(covered[:copied]), 0,
                           "nothing in the copied prefix is a declared field")

    def test_b2_the_actual_recorder_respects_every_copy_bound(self):
        """The REAL fake get_stats, driven with owned bounded storage.

        The stamp is what the fixture REPORTS; it is never permission to read
        its source past its end or to write past the caller's capacity. Each
        case checks the canary beyond the caller's struct, how much of the
        caller's own storage is still poison, the stamp it reported and the
        recorded capacity.
        """
        layout = _native._test_sender_stats_layout()
        script()
        for name, stamp, capacity in (
                ("full stamp, full capacity", layout["sizeof"], layout["sizeof"]),
                ("v0 stamp, full capacity", layout["v0_end"], layout["sizeof"]),
                ("partial appended field", layout["sap_end"] - 1, layout["sizeof"]),
                ("stamp beyond the struct", layout["sizeof"] + 64, layout["sizeof"]),
                ("full stamp, v0 capacity", layout["sizeof"], layout["v0_end"]),
                ("stamp beyond both", layout["sizeof"] + 64, layout["v0_end"]),
                # a NEWER caller: more capacity than the library's struct. The
                # copy must still stop at the source's own size.
                ("newer caller, honest stamp", layout["sizeof"],
                 layout["sizeof"] + 16),
                ("newer caller, oversized stamp", layout["sizeof"] + 64,
                 layout["sizeof"] + 16)):
            with self.subTest(case=name):
                _native._test_sender_stats_reset()
                script()
                _native._test_sender_stats_size(stamp)
                outcome = _native._test_sender_stats_drive(capacity)
                self.assertEqual(outcome["rc"], 0, "the driver was refused")
                self.assertIs(outcome["canary_intact"], True,
                              "the recorder wrote past the caller's struct")
                self.assertIs(outcome["beyond_struct_untouched"], True,
                              "the recorder wrote beyond its own struct")
                copied = min(stamp, layout["sizeof"], capacity)
                self.assertEqual(len(outcome["prefix"]),
                                 min(capacity, layout["sizeof"]))
                self.assertGreaterEqual(
                    outcome["untouched_tail"],
                    max(0, layout["sizeof"] - capacity),
                    "the recorder wrote past the offered capacity")
                self.assertEqual(outcome["stamp"], stamp,
                                 "the reported stamp was not the scripted one")
                calls = _native._test_sender_stats_calls()
                self.assertEqual(calls["entries"], 1, "one call per drive")
                self.assertEqual(calls["capacity"], capacity,
                                 "the recorder mis-recorded the real capacity")
                # no sender is attached in these substrate rows, so the
                # driver asks the fixture's STUB owner: target 1, not 0
                self.assertEqual(calls["target"], 1,
                                 "the recorder mis-recorded which owner was asked")
                # the copied prefix really IS the scripted image, compared
                # byte by byte at the SDK's own field offsets -- the stamp is
                # excluded, since it is scripted separately and asserted above
                values = {name: SCRIPTED[name] for name in SCRIPTED}
                self.assert_copied_bytes(outcome["prefix"], values, copied)
                # and the bytes beyond the copy are the driver's own poison
                self.assertEqual(
                    outcome["prefix"][copied:],
                    b"\xa5" * (min(capacity, layout["sizeof"]) - copied),
                    "the recorder touched storage beyond the copy")

    def test_b3_the_recorder_honours_the_v0_floor_and_error_paths(self):
        layout = _native._test_sender_stats_layout()
        _native._test_sender_stats_reset()
        script()
        # a capacity below the frozen v0 floor is INVAL and copies nothing
        outcome = _native._test_sender_stats_drive(layout["v0_end"] - 1)
        self.assertEqual(outcome["rc"], INVAL,
                         "a below-floor capacity was served anyway")
        self.assertIs(outcome["canary_intact"], True)
        self.assertEqual(outcome["untouched_tail"], layout["sizeof"],
                         "a refused call still wrote to the caller")
        # a NULL sender is INVAL, and the poison is untouched
        outcome = _native._test_sender_stats_drive(layout["sizeof"], 1)
        self.assertEqual(outcome["rc"], INVAL)
        self.assertEqual(outcome["untouched_tail"], layout["sizeof"])
        self.assertEqual(_native._test_sender_stats_calls()["target"], -1,
                         "a NULL owner was not recorded as such")
        # a scripted native failure leaves the poisoned output untouched
        _native._test_sender_stats_result(UNKNOWN_CODE)
        try:
            outcome = _native._test_sender_stats_drive(layout["sizeof"])
        finally:
            _native._test_sender_stats_result(0)
        self.assertEqual(outcome["rc"], UNKNOWN_CODE)
        self.assertEqual(outcome["untouched_tail"], layout["sizeof"],
                         "a failed call wrote into the caller's storage")
        self.assertIs(outcome["canary_intact"], True)
        # the inventory is NONEMPTY here, and reset really clears it
        self.assertGreater(_native._test_sender_stats_calls()["entries"], 0)
        _native._test_sender_stats_reset()
        self.assertEqual(_native._test_sender_stats_calls(),
                         {"entries": 0, "capacity": 0, "target": -1})

    def test_c_the_scripted_stamp_and_result_are_settable(self):
        layout = _native._test_sender_stats_layout()
        script()
        for size in (layout["v0_end"], layout["sizeof"]):
            with self.subTest(size=size):
                _native._test_sender_stats_size(size)      # accepted
        for code in (INVAL, CLOSED, UNKNOWN_CODE, 0):
            with self.subTest(code=code):
                _native._test_sender_stats_result(code)    # accepted
        _native._test_sender_stats_result(0)

    def test_d_reset_clears_the_script_and_a_nonempty_inventory(self):
        layout = _native._test_sender_stats_layout()
        script()
        _native._test_sender_stats_drive(layout["sizeof"])   # a REAL entry
        self.assertEqual(_native._test_sender_stats_calls()["entries"], 1)
        _native._test_sender_stats_size(4)
        _native._test_sender_stats_result(CLOSED)
        _native._test_sender_stats_reset()
        calls = _native._test_sender_stats_calls()
        self.assertEqual(calls, {"entries": 0, "capacity": 0, "target": -1})
        # and the script is back to a clean, full-size default
        outcome = _native._test_sender_stats_drive(layout["sizeof"])
        self.assertEqual(outcome["rc"], 0, "the scripted result survived reset")
        self.assertEqual(outcome["stamp"], layout["sizeof"],
                         "the scripted stamp survived reset")


# ------------------------------------------------------------- behavioural --

class StatsGatedBase(StatsFixtureBase):
    """Never skips. A missing or inert surface is a NAMED failure, asserted
    before the row acquires an endpoint."""

    def require_surface(self):
        # Kept after GREEN: an inert or removed name must still fail by name.
        missing = missing_surface()
        if missing:
            self.fail("the sender statistics surface is not implemented: "
                      f"moq5.Sender is missing {', '.join(missing)}")
        self.assertTrue(hasattr(moq5, "SenderStats"),
                        "SenderStats is missing")

    def observe(self, sender, **overrides):
        values = script(**overrides)
        return sender.stats(), values


class StatsValueTests(StatsGatedBase):
    def test_a_every_declared_field_is_reported_exactly(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                stats, values = self.observe(sender)
                assert_fields(self, stats, values)
                calls = _native._test_sender_stats_calls()
                self.assertEqual(calls["entries"], 1,
                                 "one native snapshot per call")
                self.assertEqual(calls["target"], 0,
                                 "another sender was asked")
                self.assertEqual(
                    calls["capacity"],
                    _native._test_sender_stats_layout()["sizeof"],
                    "the bridge did not offer its real output capacity")
            finally:
                sender.close()

    def test_b_a_negative_last_error_is_data_not_an_exception(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                stats, _ = self.observe(sender, last_error=UNKNOWN_CODE)
                self.assertEqual(stats.last_error, UNKNOWN_CODE)
                self.assertIs(type(stats.last_error), int)
                # and a clean history is a plain zero
                stats, _ = self.observe(sender, last_error=0)
                self.assertEqual(stats.last_error, 0)
            finally:
                sender.close()

    def test_c_the_snapshot_is_frozen_and_slotted(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                stats, values = self.observe(sender)
                # An existing field is frozen: dataclasses raise
                # FrozenInstanceError, which IS an AttributeError.
                with self.assertRaises(AttributeError):
                    stats.objects_written = 0
                # An UNKNOWN attribute is refused too, but the exception class
                # is an implementation detail of frozen+slots dataclasses on
                # this interpreter -- it raises TypeError, not AttributeError.
                # What this row owns is that the assignment is refused and
                # changes nothing, not which class carries the refusal.
                with self.assertRaises((AttributeError, TypeError)):
                    stats.new_attribute = 1
                self.assertFalse(hasattr(stats, "__dict__"),
                                 "the snapshot carries a mutable __dict__")
                self.assertFalse(hasattr(stats, "new_attribute"),
                                 "a refused assignment still took effect")
                self.assertEqual({name: getattr(stats, name) for name in FIELDS},
                                 values,
                                 "a refused assignment changed the snapshot")
            finally:
                sender.close()

    def test_d_each_call_is_a_new_independent_snapshot(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                first, _ = self.observe(sender, objects_written=1)
                second, _ = self.observe(sender, objects_written=2)
                self.assertEqual(first.objects_written, 1,
                                 "the earlier snapshot was rewritten")
                self.assertEqual(second.objects_written, 2)
                self.assertIsNot(first, second,
                                 "the same object was handed out twice")
                self.assertEqual(
                    _native._test_sender_stats_calls()["entries"], 2,
                    "a snapshot was cached instead of taken")
            finally:
                sender.close()

    def test_e_a_snapshot_survives_its_owner(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            stats, values = self.observe(sender)
            sender.close()
            gc.collect()
            observed = {name: getattr(stats, name) for name in FIELDS}
            self.assertEqual(observed, values,
                             "closing the owner invalidated a copied snapshot")
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()

    def test_f_a_terminal_sender_still_answers(self):
        self.require_surface()
        # The native get_stats has no terminal or readiness check: it answers
        # while the handle is live. A terminal sender is NOT a destroyed
        # Python owner, and this row pins that difference.
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            _native._test_sender_state(True, True, 42)
            self.assertIs(sender.terminal.fatal, True,
                          "the fixture did not terminalize the sender")
            stats, values = self.observe(sender)
            self.assertEqual(stats.objects_written, values["objects_written"],
                             "a terminal sender refused to report")
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()


class StatsSizeTests(StatsGatedBase):
    """The ABI boundary: the stamp decides what is present."""

    def layout(self):
        return _native._test_sender_stats_layout()

    def test_a_the_full_current_stamp_reports_the_appended_field(self):
        self.require_surface()
        layout = self.layout()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                script()
                _native._test_sender_stats_size(layout["sizeof"])
                stats = sender.stats()
                self.assertEqual(stats.sap_records_evicted,
                                 SCRIPTED["sap_records_evicted"])
            finally:
                sender.close()

    def test_b_a_frozen_v0_stamp_reports_absence_not_zero(self):
        self.require_surface()
        layout = self.layout()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                script()
                _native._test_sender_stats_size(layout["v0_end"])
                stats = sender.stats()
                self.assertIsNone(stats.sap_records_evicted,
                                  "an uncovered appended field was reported "
                                  "as a value")
                # every v0 field is still present and exact, checked by the
                # same assertion the full-inventory row uses
                expected = dict(SCRIPTED)
                expected["sap_records_evicted"] = None
                assert_fields(self, stats, expected)
            finally:
                sender.close()

    def test_c_one_byte_short_of_the_appended_field_is_absence(self):
        self.require_surface()
        layout = self.layout()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                script()
                _native._test_sender_stats_size(layout["sap_end"] - 1)
                stats = sender.stats()
                self.assertIsNone(stats.sap_records_evicted,
                                  "a PARTIALLY covered appended field was "
                                  "read anyway")
                _native._test_sender_stats_size(layout["sap_end"])
                self.assertEqual(sender.stats().sap_records_evicted,
                                 SCRIPTED["sap_records_evicted"],
                                 "the exactly covered field was refused")
            finally:
                sender.close()

    def test_d_a_stamped_zero_is_a_present_zero(self):
        self.require_surface()
        layout = self.layout()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                script(sap_records_evicted=0)
                _native._test_sender_stats_size(layout["sizeof"])
                stats = sender.stats()
                self.assertEqual(stats.sap_records_evicted, 0,
                                 "a stamped zero was reported as absence")
                self.assertIsNotNone(stats.sap_records_evicted)
            finally:
                sender.close()

    def test_e_a_malformed_stamp_is_a_binding_fault(self):
        self.require_surface()
        layout = self.layout()
        for size, why in ((layout["v0_end"] - 1, "below the v0 prefix"),
                          (0, "below the v0 prefix"),
                          (layout["sizeof"] + 8, "exceeds sizeof")):
            with self.subTest(size=size):
                endpoint = self.connect()
                sender = self.attach(endpoint)
                try:
                    script()
                    _native._test_sender_stats_size(size)
                    with self.assertRaises(moq5.BindingError) as raised:
                        sender.stats()
                    # a malformed SUCCESSFUL output is a binding fault, never
                    # a native result: checked on the exception itself, before
                    # its traceback is released
                    self.assertNotIsInstance(raised.exception, moq5.MoqError)
                    message = str(raised.exception)
                    del raised
                    self.assertIn(why, message,
                                  "the malformed stamp was not named")
                finally:
                    _native._test_sender_stats_size(layout["sizeof"])
                    try:
                        sender.close()
                    finally:
                        endpoint.close()

    def test_f_uncovered_storage_is_never_read(self):
        self.require_surface()
        # The fixture writes ONLY the stamped prefix, so whatever the bridge
        # left in the rest of its own storage stays there. If the bridge read
        # past the stamp it would read that poison; absence is the only
        # correct answer, and a poison value must never surface.
        layout = self.layout()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                script()
                _native._test_sender_stats_size(layout["v0_end"])
                for _ in range(3):
                    stats = sender.stats()
                    self.assertIsNone(stats.sap_records_evicted,
                                      "uncovered storage was reported")
            finally:
                sender.close()


class StatsErrorTests(StatsGatedBase):
    def test_a_native_failures_keep_their_signed_code(self):
        self.require_surface()
        for code in (INVAL, CLOSED, WRONG_STATE, UNKNOWN_CODE):
            with self.subTest(code=code):
                endpoint = self.connect()
                sender = self.attach(endpoint)
                try:
                    script()
                    _native._test_sender_stats_result(code)
                    with self.assertRaises(moq5.MoqError) as raised:
                        sender.stats()
                    signed = raised.exception.code
                    operation = raised.exception.operation
                    del raised
                    self.assertEqual(signed, code,
                                     "the signed code was normalised")
                    self.assertEqual(operation, "stats",
                                     "the operation was mislabelled")
                finally:
                    _native._test_sender_stats_result(0)
                    try:
                        sender.close()
                    finally:
                        endpoint.close()

    def test_b_the_native_result_is_checked_before_the_stamp(self):
        self.require_surface()
        # An unknown failure with UNUSABLE output: the bridge must report the
        # native result, not a binding fault about the garbage stamp, and must
        # not half-construct a snapshot.
        layout = self.layout() if hasattr(self, "layout") else \
            _native._test_sender_stats_layout()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            script()
            _native._test_sender_stats_size(layout["v0_end"] - 1)
            _native._test_sender_stats_result(UNKNOWN_CODE)
            with self.assertRaises(moq5.MoqError) as raised:
                sender.stats()
            signed = raised.exception.code
            del raised
            self.assertEqual(signed, UNKNOWN_CODE,
                             "a malformed stamp masked the native failure")
        finally:
            _native._test_sender_stats_result(0)
            _native._test_sender_stats_size(layout["sizeof"])
            try:
                sender.close()
            finally:
                endpoint.close()

    def test_c_a_failed_observation_leaves_the_sender_usable(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            track = sender.add_track(moq5.SendTrackConfig(
                name=b"v", media_type=moq5.MediaType.VIDEO,
                packaging=moq5.Packaging.RAW, codec=b"av01", bitrate=1))
            before = _native._test_sender_counts()
            script()
            _native._test_sender_stats_result(CLOSED)
            with self.assertRaises(moq5.MoqError):
                sender.stats()
            _native._test_sender_stats_result(0)
            # no write, end, remove or destroy effect, and the sender works
            operations = tuple(op for op in _native._test_sender_log()
                               if op in ("write", "end_track", "remove_track",
                                         "sender_destroy"))
            self.assertEqual(operations, (),
                             "a failed snapshot had a lifecycle effect")
            self.assertEqual(_native._test_sender_counts()["destroy_entries"],
                             before["destroy_entries"])
            self.assertIs(sender.closed, False)
            self.assertIs(sender.write(track, moq5.SendObject(payload=b"f")),
                          moq5.WriteOutcome.ACCEPTED,
                          "the sender was left unusable")
            self.assertEqual(sender.stats().objects_written,
                             SCRIPTED["objects_written"],
                             "a later snapshot was refused")
        finally:
            _native._test_sender_stats_result(0)
            try:
                sender.close()
            finally:
                endpoint.close()


class StatsConstructionTests(StatsGatedBase):
    """Failures while the RESULT is built, at the real boundaries."""

    def sender(self):
        endpoint = self.connect()
        sender = self.attach(endpoint)
        self.addCleanup(endpoint.close)
        self.addCleanup(sender.close)
        return sender

    def test_a_each_native_construction_fault_leaves_the_owner_usable(self):
        self.require_surface()
        sender = self.sender()
        for site in ("sender_stats_field", "sender_stats_result"):
            with self.subTest(site=site):
                script()
                before = _native._test_sender_stats_calls()["entries"]
                try:
                    _native._test_fail_allocation_at(site)
                    with self.assertRaises(MemoryError):
                        sender.stats()
                finally:
                    _native._test_fail_allocation_at(None)
                self.assertTrue(_native._test_allocation_site_fired(site),
                                f"the {site} fault never fired")
                self.assertEqual(
                    _native._test_sender_stats_calls()["entries"], before + 1,
                    "the snapshot was not taken before the failure")
                # no lifecycle effect, and a RETRY on the same live owner works
                operations = tuple(op for op in _native._test_sender_log()
                                   if op in ("write", "end_track",
                                             "remove_track", "sender_destroy"))
                self.assertEqual(operations, (),
                                 "a failed construction had a lifecycle effect")
                self.assertIs(sender.closed, False)
                stats = sender.stats()
                assert_fields(self, stats, SCRIPTED)

    def test_b_a_wrapper_construction_failure_is_the_callers(self):
        self.require_surface()
        sender = self.sender()
        script()
        before = _native._test_sender_stats_calls()["entries"]

        class Refuses(Exception):
            pass

        def refuse(*args, **kwargs):
            raise Refuses("the wrapper refused to build")

        with mock.patch.object(moq5._sender, "SenderStats", refuse):
            with self.assertRaises(Refuses):
                sender.stats()
        self.assertEqual(_native._test_sender_stats_calls()["entries"],
                         before + 1,
                         "the snapshot was not taken")
        # the same LIVE owner still answers, with counters unchanged
        stats = sender.stats()
        assert_fields(self, stats, SCRIPTED)
        self.assertEqual(_native._test_sender_stats_calls()["entries"],
                         before + 2,
                         "the retry did not take its own snapshot")
        self.assertIs(sender.closed, False)


class StatsGuardTests(StatsGatedBase):
    def test_a_a_closed_sender_is_refused_before_native(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            sender.close()
            before = _native._test_sender_stats_calls()
            with self.assertRaises(RuntimeError):
                sender.stats()
            self.assertEqual(_native._test_sender_stats_calls()["entries"],
                             before["entries"],
                             "a closed owner entered native")
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()

    def test_b_the_owner_thread_and_pid_guards_precede_native_entry(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            script()
            before = _native._test_sender_stats_calls()
            refused = []

            def other():
                try:
                    sender.stats()
                except RuntimeError as error:
                    refused.append(error)

            worker = threading.Thread(target=other, daemon=True)
            worker.start()
            worker.join(5.0)
            self.assertFalse(worker.is_alive(), "the guard worker never returned")
            self.assertEqual(len(refused), 1, "a foreign thread was admitted")
            self.assertEqual(_native._test_sender_stats_calls()["entries"],
                             before["entries"],
                             "a foreign thread entered native")

            if hasattr(os, "fork"):
                child = os.fork()
                if child == 0:
                    status = 0
                    try:
                        sender.stats()
                        status = 3              # the inherited handle worked
                    except RuntimeError:
                        pass
                    except BaseException:
                        status = 9
                    if (_native._test_sender_stats_calls()["entries"]
                            != before["entries"]):
                        status = 4
                    os._exit(status)
                self.assertEqual(reap_child(child), 0,
                                 "the fork guard child did not exit cleanly")
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()

    def test_c_the_private_bridge_refuses_a_wrong_capsule(self):
        self.require_surface()
        self.assertTrue(hasattr(_native, "sender_stats"),
                        "the stats bridge is not implemented: _native is "
                        "missing sender_stats")
        endpoint = self.connect()
        sender = self.attach(endpoint)
        self.addCleanup(endpoint.close)
        self.addCleanup(sender.close)
        endpoint_handle = getattr(endpoint, "_Endpoint__handle")
        before = _native._test_sender_stats_calls()
        with self.assertRaises((ValueError, TypeError)):
            _native.sender_stats(endpoint_handle)
        with self.assertRaises(TypeError):
            _native.sender_stats()
        self.assertEqual(_native._test_sender_stats_calls()["entries"],
                         before["entries"],
                         "a wrong capsule reached the service")


# ------------------------------------------------------------------ control --

PUBLIC_GATED_CLASSES = ("StatsValueTests", "StatsSizeTests", "StatsErrorTests",
                        "StatsConstructionTests", "StatsGuardTests")


class StatsControlTests(unittest.TestCase):
    """This file's own machinery. Never evidence about the product.

    The inert-name control establishes the GATE: every gated row is invoked
    and refuses by a named assertion. It does NOT execute the bodies -- each
    stops inside require_surface. The scoped stand-in control below runs those
    bodies, and the oracle control proves the field checker can reject a bad
    observation.
    """

    def test_a_inert_names_are_refused_by_every_gated_row(self):
        import test_send_stats as module

        saved = getattr(moq5.Sender, "stats", None)
        try:
            moq5.Sender.stats = object()
            loader = unittest.defaultTestLoader
            suite = unittest.TestSuite(
                loader.loadTestsFromTestCase(getattr(module, c))
                for c in PUBLIC_GATED_CLASSES)
            result = unittest.TestResult()
            suite.run(result)
        finally:
            if saved is None:
                if hasattr(moq5.Sender, "stats"):
                    del moq5.Sender.stats
            else:
                moq5.Sender.stats = saved
        self.assertGreater(result.testsRun, 0)
        self.assertEqual(result.skipped, [], "a gated row skipped")
        self.assertEqual(result.errors, [],
                         "a gated row raised an arbitrary error instead of "
                         "refusing by a named assertion")
        failed = {getattr(case, "test_case", case).id()
                  for case, _ in result.failures}
        self.assertEqual(len(failed), result.testsRun,
                         "an inert surface satisfied a behavioural row")

    def test_b_the_field_oracle_rejects_a_bad_observation(self):
        """The ACTUAL mapping assertion, driven with wrong observations.

        `assert_fields` is what the behavioural row uses, so this control
        cannot pass by comparing two dictionaries the row never touches. A
        correct observation must pass it; each same-shape perturbation must
        make it fail by a named assertion.
        """

        class Observation:
            __slots__ = FIELDS

            def __init__(self, values):
                for name in FIELDS:
                    object.__setattr__(self, name, values[name])

        assert_fields(self, Observation(SCRIPTED), SCRIPTED)     # the positive

        for name, wrong in (("objects_written", SCRIPTED["objects_sent"]),
                            ("bytes_queued", (1 << 32) - 1),
                            ("objects_queued", -(2 ** 63) + 9),
                            ("last_error", -UNKNOWN_CODE),
                            ("sap_records_evicted", None),
                            ("groups_dropped", float(SCRIPTED["groups_dropped"]))):
            with self.subTest(field=name):
                broken = dict(SCRIPTED)
                broken[name] = wrong
                with self.assertRaises(AssertionError):
                    assert_fields(self, Observation(broken), SCRIPTED)

        # and absence is checked in BOTH directions: a present value where
        # absence is expected must fail too
        expected_absent = dict(SCRIPTED)
        expected_absent["sap_records_evicted"] = None
        assert_fields(self, Observation(expected_absent), expected_absent)
        with self.assertRaises(AssertionError):
            assert_fields(self, Observation(SCRIPTED), expected_absent)

    def test_c_the_gated_bodies_pass_against_a_scoped_stand_in(self):
        """Every behavioural body, run against a stand-in stats().

        Rows about the NATIVE entry point are excluded BY NAME: a Python
        stand-in has no capsule, no capacity and no binding-fault path. They
        execute against the real bridge once it exists.
        """
        import test_send_stats as module

        layout = _native._test_sender_stats_layout()
        excluded = {
            "StatsValueTests.test_a_every_declared_field_is_reported_exactly",
            "StatsSizeTests.test_e_a_malformed_stamp_is_a_binding_fault",
            "StatsErrorTests.test_b_the_native_result_is_checked_before_the_stamp",
            "StatsGuardTests.test_c_the_private_bridge_refuses_a_wrong_capsule",
            "StatsGuardTests.test_b_the_owner_thread_and_pid_guards_precede_native_entry",
            "StatsConstructionTests.test_a_each_native_construction_fault_leaves_the_owner_usable",
            "StatsConstructionTests.test_b_a_wrapper_construction_failure_is_the_callers",
        }

        # An ORDINARY frozen, slotted dataclass -- the shape the product will
        # use -- so the control cannot hide a difference by defining its own
        # __setattr__.
        Snapshot = dataclasses.make_dataclass(
            "SenderStats", [(name, object) for name in FIELDS],
            frozen=True, slots=True)

        def build(values):
            return Snapshot(**{name: values[name] for name in FIELDS})

        def stats(sender):
            if sender.closed:
                raise RuntimeError("sender is closed")
            raw = _native._test_sender_stats_snapshot()
            if raw is None:
                raise moq5.MoqError(_native._test_sender_stats_last_result(),
                                    "stats", "scripted")
            values = dict(raw)
            stamp = raw["struct_size"]
            values.pop("struct_size")
            if stamp < layout["sap_end"]:
                values["sap_records_evicted"] = None
            return build(values)

        saved = getattr(moq5.Sender, "stats", None)
        saved_type = getattr(moq5, "SenderStats", None)
        self.addCleanup(_native._test_sender_stats_reset)
        try:
            moq5.Sender.stats = stats
            moq5.SenderStats = Snapshot
            loader = unittest.defaultTestLoader
            names = []
            for class_name in PUBLIC_GATED_CLASSES:
                case_class = getattr(module, class_name)
                for test in loader.getTestCaseNames(case_class):
                    if f"{class_name}.{test}" in excluded:
                        continue
                    names.append(f"{class_name}.{test}")
            suite = loader.loadTestsFromNames(names, module)
            result = unittest.TestResult()
            suite.run(result)
        finally:
            if saved is None:
                if hasattr(moq5.Sender, "stats"):
                    del moq5.Sender.stats
            else:
                moq5.Sender.stats = saved
            if saved_type is None:
                if hasattr(moq5, "SenderStats"):
                    del moq5.SenderStats
            else:
                moq5.SenderStats = saved_type
        self.assertEqual(
            [f"{case.id()}: {error}" for case, error in
             result.failures + result.errors], [],
            "a gated body failed against the declared contract")
        self.assertGreater(result.testsRun, 0)

    def test_d_the_bridge_is_present_and_refuses_wrong_arguments(self):
        # This row was the RED gate's "the bridge is absent" assertion. The
        # bridge exists now, so it pins presence, callability and that a
        # wrong-shaped call is refused rather than crashing.
        self.assertTrue(hasattr(_native, "sender_stats"),
                        "the stats bridge disappeared")
        self.assertTrue(callable(_native.sender_stats))
        before = _native._test_sender_stats_calls()
        for args in ((), (object(),), (object(), object())):
            with self.subTest(arity=len(args)):
                with self.assertRaises((TypeError, ValueError)):
                    _native.sender_stats(*args)
        self.assertEqual(_native._test_sender_stats_calls()["entries"],
                         before["entries"],
                         "a wrong-shaped call reached the service")
