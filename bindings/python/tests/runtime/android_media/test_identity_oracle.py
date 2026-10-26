"""The identity oracle, offline: the expected (track, group, subgroup, object, priority) sequence is derived
from the publisher's own ledger and configuration, never from anything the player printed; the observed
sequence is parsed from the player's diagnostic breadcrumbs; the comparison names every difference. The
controls here are what the device result rests on: a wrong object or group fails, an absent identity
fails, an absent subgroup is not subgroup zero, a short or long inventory fails, and only the exact
inventory passes.
"""
import json
import struct
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import identity_oracle as io_  # noqa: E402
import media_oracle           # noqa: E402

# is_sync per ledger index: three groups of 4, 4 and 2 objects.
SYNC = [1, 0, 0, 0, 1, 0, 0, 0, 1, 0]
EXPECTED = [("video", 0, 0, 0, 128), ("video", 0, 0, 1, 128), ("video", 0, 0, 2, 128), ("video", 0, 0, 3, 128),
            ("video", 1, 0, 0, 128), ("video", 1, 0, 1, 128), ("video", 1, 0, 2, 128), ("video", 1, 0, 3, 128),
            ("video", 2, 0, 0, 128), ("video", 2, 0, 1, 128)]


def ledger(sync=SYNC):
    return [media_oracle.Expected(i, b"x" * (i + 1), bool(s), i * 41666) for i, s in enumerate(sync)]


def breadcrumb(n, group, obj, subgroup="0", priority=128, lane="video"):
    return (f"09-28 19:01:02.345  4242  4266 I MoqPlayerDiag: {n} object identity lane={lane} group={group} "
            f"subgroup={subgroup} object={obj} priority={priority}")


def log_for(rows, extra=()):
    lines = ["--------- beginning of main",
             "09-28 19:01:01.000  4242  4242 I .samples.player: Late-enabling -Xcheck:jni",
             "09-28 19:01:02.000  4242  4266 I MoqPlayerDiag: 1 enter configure"]
    n = 2
    for track, group, subgroup, obj, priority in rows:
        lines.append(breadcrumb(n, group, obj, subgroup=("none" if subgroup is None else subgroup), priority=priority, lane=track))
        n += 1
    lines.extend(extra)
    lines.append(f"09-28 19:01:05.000  4242  4266 I MoqPlayerDiag: {n} read-end receiver objects={len(rows)}")
    return "\n".join(lines) + "\n"


class ExpectedSequence(unittest.TestCase):
    def test_groups_open_at_every_sync_object_and_objects_restart_at_zero(self):
        self.assertEqual(EXPECTED, io_.expected_sequence(ledger()))

    def test_priority_and_track_are_the_publisher_configuration(self):
        got = io_.expected_sequence(ledger([1, 0]), track="cam", priority=7)
        self.assertEqual([("cam", 0, 0, 0, 7), ("cam", 0, 0, 1, 7)], got)

    def test_a_ledger_that_does_not_open_with_a_sync_object_is_refused(self):
        with self.assertRaises(ValueError):
            io_.expected_sequence(ledger([0, 1]))
        with self.assertRaises(ValueError):
            io_.expected_sequence([])

    def test_the_default_priority_is_the_publisher_facade_default(self):
        self.assertEqual(128, io_.PUBLISHER_DEFAULT_PRIORITY)


class ObservedSequence(unittest.TestCase):
    def test_every_identity_breadcrumb_is_read_in_order_and_other_lines_are_ignored(self):
        self.assertEqual(EXPECTED, io_.observed_sequence(log_for(EXPECTED)))

    def test_absent_identity_and_absent_subgroup_are_kept_distinct(self):
        text = log_for([("video", 7, None, 5, 200)],
                       extra=["09-28 19:01:03.000  4242  4266 I MoqPlayerDiag: 3 object identity lane=video absent"])
        self.assertEqual([("video", 7, None, 5, 200), None], io_.observed_sequence(text))

    def test_a_malformed_identity_line_is_refused_not_skipped(self):
        bad = "09-28 19:01:03.000  4242  4266 I MoqPlayerDiag: 3 object identity lane=video group=1 subgroup=0"
        with self.assertRaises(ValueError):
            io_.observed_sequence(log_for([], extra=[bad]))

    def test_the_breadcrumb_cap_line_is_refused(self):
        cap = "09-28 19:01:03.000  4242  4266 I MoqPlayerDiag: 4097 breadcrumbs capped at 4096; later ones are dropped"
        with self.assertRaises(ValueError):
            io_.observed_sequence(log_for(EXPECTED, extra=[cap]))


class Comparison(unittest.TestCase):
    def judge(self, observed, expected=EXPECTED):
        return io_.compare(expected, observed)

    def test_the_exact_inventory_passes_and_reports_the_progression(self):
        v = self.judge(list(EXPECTED))
        self.assertTrue(v["ok"], v)
        self.assertEqual([], v["failures"])
        self.assertEqual({"expected": 10, "observed": 10, "matched": 10}, v["counts"])
        self.assertEqual([0, 1, 2], v["groups"])
        self.assertTrue(v["nonzero_group_progression"])
        self.assertTrue(v["nonzero_object_progression"])
        self.assertTrue(v["subgroup_zero_present"])
        self.assertFalse(v["subgroup_absent_present"])

    def test_a_wrong_object_fails_by_position(self):
        obs = list(EXPECTED)
        obs[5] = ("video", 1, 0, 2, 128)
        v = self.judge(obs)
        self.assertFalse(v["ok"])
        self.assertEqual(["object 5: expected ('video', 1, 0, 1, 128), observed ('video', 1, 0, 2, 128)"], v["failures"])

    def test_a_wrong_group_fails(self):
        obs = list(EXPECTED)
        obs[4] = ("video", 0, 0, 0, 128)
        v = self.judge(obs)
        self.assertFalse(v["ok"])
        self.assertIn("object 4: expected ('video', 1, 0, 0, 128), observed ('video', 0, 0, 0, 128)", v["failures"])

    def test_an_absent_identity_fails(self):
        obs = list(EXPECTED)
        obs[0] = None
        v = self.judge(obs)
        self.assertFalse(v["ok"])
        self.assertEqual(["object 0: expected ('video', 0, 0, 0, 128), observed no identity"], v["failures"])

    def test_an_absent_subgroup_is_not_subgroup_zero(self):
        obs = list(EXPECTED)
        obs[9] = ("video", 2, None, 1, 128)
        v = self.judge(obs)
        self.assertFalse(v["ok"])
        self.assertTrue(v["subgroup_absent_present"])

    def test_a_wrong_priority_or_track_fails(self):
        obs = list(EXPECTED)
        obs[1] = ("video", 0, 0, 1, 127)
        self.assertFalse(self.judge(obs)["ok"])
        obs = list(EXPECTED)
        obs[1] = ("audio", 0, 0, 1, 128)
        self.assertFalse(self.judge(obs)["ok"])

    def test_a_short_or_long_inventory_fails(self):
        v = self.judge(EXPECTED[:-1])
        self.assertFalse(v["ok"])
        self.assertEqual(["observed 9 identities, expected 10"], v["failures"])
        v = self.judge(EXPECTED + [("video", 2, 0, 2, 128)])
        self.assertFalse(v["ok"])
        self.assertEqual(["observed 11 identities, expected 10"], v["failures"])

    def test_all_zero_identities_are_values_that_still_fail_a_real_progression(self):
        v = self.judge([("video", 0, 0, 0, 128)] * 10)
        self.assertFalse(v["ok"])
        self.assertEqual(9, len(v["failures"]))


class RunDirectory(unittest.TestCase):
    def write_run(self, root, sync=SYNC, log=None):
        rows = b"".join(struct.pack("<IBQ", i + 1, s, i * 41666) + b"x" * (i + 1) for i, s in enumerate(sync))
        (root / "objects.bin").write_bytes(rows)
        (root / io_.DIAGNOSTIC_LOG).write_text(log if log is not None else log_for(EXPECTED))

    def test_a_run_directory_is_judged_and_the_verdict_written_once(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            self.write_run(root)
            self.assertEqual(0, io_.main([str(root)]))
            written = json.loads((root / io_.VERDICT).read_text())
            self.assertTrue(written["ok"])
            self.assertEqual(10, written["counts"]["matched"])
            self.assertEqual("128 = the publisher facade default (no explicit track priority), forwarded verbatim by the relay",
                             written["oracle"]["priority"])
            self.assertEqual(2, io_.main([str(root)]), "a second verdict must not overwrite the first")

    def test_a_failing_run_exits_one_and_still_writes_its_verdict(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            self.write_run(root, log=log_for(EXPECTED[:-1]))
            self.assertEqual(1, io_.main([str(root)]))
            self.assertFalse(json.loads((root / io_.VERDICT).read_text())["ok"])

    def test_a_missing_log_or_ledger_is_refused(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            self.assertEqual(2, io_.main([str(root)]))
            self.assertFalse((root / io_.VERDICT).exists())


if __name__ == "__main__":
    unittest.main()
