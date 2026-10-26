"""The reference publisher's AAC-LC track for the A/V diagnostic, judged offline through its selftest
modes, which run the same declaration, validation and merge code as the publishing path:

  --audio-config-selftest <audio objects> <asc hex> <samplerate> <channels>
  --merge-selftest <video objects> <audio objects>

The binary under test is PUBLISHER_BIN (an absolute path); nothing here connects anywhere."""
import json
import os
import struct
import subprocess
import tempfile
import unittest
from pathlib import Path

BIN = os.environ.get("PUBLISHER_BIN")


def ledger(path, records):
    with open(path, "wb") as f:
        for payload, sync, pts in records:
            f.write(struct.pack("<IBQ", len(payload), sync, pts))
            f.write(payload)
    return str(path)


def aac_ledger(path, n=5, rate=48_000):
    return ledger(path, [(bytes([0x21]) + i.to_bytes(2, "big"), 1, i * 1024 * 1_000_000 // rate) for i in range(n)])


@unittest.skipUnless(BIN, "set PUBLISHER_BIN to the publisher under test")
class AudioConfiguration(unittest.TestCase):
    def run_selftest(self, *args):
        r = subprocess.run([BIN, "--audio-config-selftest", *args], capture_output=True, text=True, timeout=10)
        return r.returncode, r.stdout, r.stderr

    def test_the_declared_lc_track_is_exactly_what_the_catalog_carries(self):
        with tempfile.TemporaryDirectory() as tmp:
            rc, out, err = self.run_selftest(aac_ledger(Path(tmp) / "a.bin"), "1190", "48000", "2")
        self.assertEqual((rc, err), (0, ""))
        self.assertEqual(json.loads(out), {"event": "track_config", "track": "audio", "codec": "mp4a.40.2",
                                           "samplerate": 48000, "channel_config": "2", "timescale": 90000,
                                           "init_data": "1190", "objects": 5})

    def test_inconsistent_or_malformed_declarations_are_refused_by_name_before_anything_else(self):
        with tempfile.TemporaryDirectory() as tmp:
            good = aac_ledger(Path(tmp) / "a.bin")
            cases = {
                "not AAC-LC": (good, "0990", "48000", "2"),                  # object type 1 (Main)
                "sampling frequency": (good, "1190", "44100", "2"),
                "channel configuration": (good, "1190", "48000", "1"),
                "trailing bits": (good, "1191", "48000", "2"),
                "hex": (good, "11g0", "48000", "2"),
                "two bytes": (good, "119", "48000", "2"),
                "sample rate": (good, "1190", "48001", "2"),
                "channels": (good, "1190", "48000", "9"),
                "not a sync": (ledger(Path(tmp) / "ns.bin", [(b"\x21\x00", 1, 0), (b"\x21\x01", 0, 21333)]), "1190", "48000", "2"),
                "strictly increasing": (ledger(Path(tmp) / "eq.bin", [(b"\x21\x00", 1, 0), (b"\x21\x01", 1, 0)]), "1190", "48000", "2"),
                "shared origin": (ledger(Path(tmp) / "late.bin", [(b"\x21\x00", 1, 5)]), "1190", "48000", "2"),
                "no audio objects": (ledger(Path(tmp) / "empty.bin", []), "1190", "48000", "2"),
                "bad audio objects file": (aac_ledger(Path(tmp) / "big.bin", n=513), "1190", "48000", "2"),
            }
            for name, args in cases.items():
                with self.subTest(name):
                    rc, out, err = self.run_selftest(*args)
                    self.assertEqual(rc, 2, err)
                    self.assertEqual(out, "")
                    self.assertIn(name, err)


@unittest.skipUnless(BIN, "set PUBLISHER_BIN to the publisher under test")
class MergeOrder(unittest.TestCase):
    def test_both_ledgers_merge_by_pts_with_video_first_on_ties(self):
        with tempfile.TemporaryDirectory() as tmp:
            v = ledger(Path(tmp) / "v.bin", [(b"v0", 1, 0), (b"v1", 0, 41_666), (b"v2", 0, 83_332)])
            a = aac_ledger(Path(tmp) / "a.bin", n=5)
            r = subprocess.run([BIN, "--merge-selftest", v, a], capture_output=True, text=True, timeout=10)
        self.assertEqual((r.returncode, r.stderr), (0, ""))
        got = json.loads(r.stdout)
        self.assertEqual(got["event"], "merge_order")
        self.assertEqual([(e["track"], e["index"], e["pts_us"]) for e in got["entries"]], [
            ("video", 0, 0), ("audio", 0, 0), ("audio", 1, 21_333), ("video", 1, 41_666),
            ("audio", 2, 42_666), ("audio", 3, 64_000), ("video", 2, 83_332), ("audio", 4, 85_333)])

    def test_the_real_fixture_sizes_merge_completely(self):
        with tempfile.TemporaryDirectory() as tmp:
            v = ledger(Path(tmp) / "v.bin", [(bytes([i]), int(i % 24 == 0), i * 41_666) for i in range(60)])
            a = aac_ledger(Path(tmp) / "a.bin", n=118)
            r = subprocess.run([BIN, "--merge-selftest", v, a], capture_output=True, text=True, timeout=10)
        self.assertEqual((r.returncode, r.stderr), (0, ""))
        entries = json.loads(r.stdout)["entries"]
        self.assertEqual(len(entries), 178)
        for track, n in (("video", 60), ("audio", 118)):
            self.assertEqual([e["index"] for e in entries if e["track"] == track], list(range(n)))
        pts = [e["pts_us"] for e in entries]
        self.assertEqual(pts, sorted(pts))


if __name__ == "__main__":
    unittest.main()
