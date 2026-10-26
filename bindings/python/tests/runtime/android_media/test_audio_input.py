"""The finite AAC-LC fixture for the A/V diagnostic: its controlled ADTS reader, the AudioSpecificConfig
the catalog carries, the shared media time origin with the video input, bounded finite coverage of the
video's span, argument refusal before any effect, warning-clean tools, and a real generation that is
identical twice, agrees with ffprobe, and ships its authoritative files only when every check passes."""
import os
import shutil
import stat
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

OUTPUTS = ("in.aac", "audio-objects.bin", "audio-expected.tsv")


def adts(payload, *, profile=1, freq_index=3, channels=2, crc=False, sync=0xFFF, layer=0, blocks=0, length=None):
    """One ADTS frame around `payload` (ISO/IEC 13818-7 / 14496-3 1.A.2.2): 7 header bytes, 9 with CRC."""
    header_len = 7 if not crc else 9
    total = header_len + len(payload) if length is None else length
    bits = (sync << 44) | (0 << 43) | (layer << 41) | ((0 if crc else 1) << 40) | (profile << 38) \
        | (freq_index << 34) | (0 << 33) | (channels << 30) | (0 << 29) | (0 << 28) | (0 << 27) | (0 << 26) \
        | (total << 13) | (0x7FF << 2) | blocks
    head = bits.to_bytes(7, "big")
    return head + (b"\xAB\xCD" if crc else b"") + payload


def fake_tool(directory, name, body):
    """An executable shell script standing in for ffmpeg/ffprobe; `$REAL` is the real tool's path."""
    real = shutil.which(name) or "/nonexistent"
    path = Path(directory) / name
    path.write_text(f"#!/bin/sh\nREAL={real}\n{body}\n")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return str(path)


class AdtsParsing(unittest.TestCase):
    def test_a_frame_yields_its_fields_and_raw_access_unit(self):
        import make_audio_input as m
        f = m.parse_adts_frame(adts(b"\x01\x02\x03"), 0)
        self.assertEqual((f.object_type, f.sample_rate, f.channels, f.length, f.header_length), (2, 48_000, 2, 10, 7))
        self.assertEqual(f.access_unit, b"\x01\x02\x03")

    def test_a_crc_header_is_nine_bytes_and_the_crc_is_not_media(self):
        import make_audio_input as m
        f = m.parse_adts_frame(adts(b"\x09" * 4, crc=True), 0)
        self.assertEqual((f.header_length, f.length, f.access_unit), (9, 13, b"\x09" * 4))

    def test_malformed_frames_are_named_refusals(self):
        import make_audio_input as m
        cases = {
            "syncword": adts(b"\x00", sync=0xFFE),
            "layer": adts(b"\x00", layer=1),
            "raw data blocks": adts(b"\x00", blocks=1),
            "frame length": adts(b"\x00", length=6),
            "runs past": adts(b"\x00\x00", length=40),
            "sampling frequency": adts(b"\x00", freq_index=13),
            "channel configuration": adts(b"\x00", channels=0),
        }
        for name, frame in cases.items():
            with self.subTest(name), self.assertRaises(ValueError) as caught:
                m.parse_adts_frame(frame, 0)
            self.assertIn(name, str(caught.exception))
        with self.assertRaises(ValueError) as caught:
            m.parse_adts_frame(b"\xFF\xF1\x50", 0)
        self.assertIn("truncated", str(caught.exception))

    def test_a_stream_splits_exactly_and_refuses_trailing_bytes(self):
        import make_audio_input as m
        stream = adts(b"\x01") + adts(b"\x02\x02") + adts(b"\x03" * 3)
        frames = m.split_adts(stream)
        self.assertEqual([f.access_unit for f in frames], [b"\x01", b"\x02\x02", b"\x03" * 3])
        with self.assertRaises(ValueError):
            m.split_adts(stream + b"\xFF")


class DecoderConfiguration(unittest.TestCase):
    def test_the_lc_audio_specific_config_matches_the_adts_fields(self):
        import make_audio_input as m
        self.assertEqual(m.lc_audio_specific_config(48_000, 2), b"\x11\x90")    # AAC-LC, index 3, stereo
        self.assertEqual(m.lc_audio_specific_config(44_100, 1), b"\x12\x08")
        for rate, channels in ((12_345, 2), (48_000, 0), (48_000, 8)):
            with self.subTest(rate=rate, channels=channels), self.assertRaises(ValueError):
                m.lc_audio_specific_config(rate, channels)

    def test_the_codec_string_is_aac_lc(self):
        import make_audio_input as m
        self.assertEqual(m.LC_CODEC, "mp4a.40.2")


class SharedTimeline(unittest.TestCase):
    def test_audio_times_are_exact_frame_multiples_from_the_video_origin(self):
        import make_audio_input as m
        self.assertEqual([m.audio_pts_us(i, 48_000) for i in range(4)], [0, 21_333, 42_666, 64_000])
        self.assertEqual(m.audio_pts_us(118, 48_000), 2_517_333)

    def test_frame_count_covers_the_whole_video_span_and_no_more(self):
        import make_audio_input as m
        video_end_us = 60 * 41_666
        n = m.frames_covering(video_end_us, 48_000)
        self.assertEqual(n, 118)
        self.assertGreaterEqual(m.audio_pts_us(n, 48_000), video_end_us)
        self.assertLess(m.audio_pts_us(n - 1, 48_000), video_end_us)

    def test_coverage_is_exact_at_frame_boundaries(self):
        import make_audio_input as m
        self.assertEqual(m.frames_covering(1, 48_000), 1)
        self.assertEqual(m.frames_covering(m.audio_pts_us(3, 48_000), 48_000), 3, "an end on a frame start needs no extra frame")
        self.assertEqual(m.frames_covering(m.audio_pts_us(3, 48_000) + 1, 48_000), 4)
        self.assertEqual(m.frames_covering(64_000, 48_000), 3, "exactly three 1024-sample frames at 48 kHz")

    def test_the_span_is_positive_and_bounded(self):
        import make_audio_input as m
        for end in (0, -1, m.MAX_SPAN_US + 1):
            with self.subTest(end=end), self.assertRaises(ValueError):
                m.frames_covering(end, 48_000)
        self.assertEqual(m.frames_covering(m.MAX_SPAN_US, 48_000), -(-m.MAX_SPAN_US * 48_000 // (1024 * 1_000_000)))


class ArgumentsBeforeEffects(unittest.TestCase):
    def test_invalid_counts_and_steps_are_refused_before_any_directory_or_tool(self):
        import make_audio_input as m
        with tempfile.TemporaryDirectory() as tmp:
            for frames, step in ((0, 41_666), (-1, 41_666), (60, 0), (60, -5), (True, 41_666), (60, 1.5),
                                 (100_000, 41_666)):
                target = Path(tmp) / f"run-{frames}-{step}"
                with self.subTest(frames=frames, step=step), self.assertRaises(ValueError):
                    m.generate(target, frames, step, ffmpeg="/nonexistent/ffmpeg", ffprobe="/nonexistent/ffprobe")
                self.assertFalse(target.exists(), "nothing is created for refused arguments")

    def test_a_run_directory_holding_fixture_outputs_is_never_written_over(self):
        import make_audio_input as m
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "in.aac").write_bytes(b"earlier")
            r = m.generate(Path(tmp), 60, 41_666, ffmpeg="/nonexistent/ffmpeg", ffprobe="/nonexistent/ffprobe")
            self.assertFalse(r["ok"])
            self.assertIn("already", r["error"])
            self.assertEqual((Path(tmp) / "in.aac").read_bytes(), b"earlier")


@unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "needs ffmpeg and ffprobe on PATH")
class ToolDiagnostics(unittest.TestCase):
    def _run(self, ffmpeg_body, ffprobe_body='exec "$REAL" "$@"'):
        import make_audio_input as m
        tools = tempfile.mkdtemp()
        out = Path(tempfile.mkdtemp()) / "run"
        try:
            r = m.generate(out, 60, 41_666, ffmpeg=fake_tool(tools, "ffmpeg", ffmpeg_body),
                           ffprobe=fake_tool(tools, "ffprobe", ffprobe_body))
            shipped = [n for n in OUTPUTS if (out / n).exists()]
            return r, shipped, out
        finally:
            shutil.rmtree(tools)

    def test_the_tools_run_at_warning_level(self):
        import make_audio_input as m
        with tempfile.TemporaryDirectory() as tmp:
            r = m.generate(Path(tmp) / "run", 60, 41_666)
            self.assertTrue(r["ok"], r.get("error") or r["checks"])
            generated = [c["argv"] for c in r["commands"]]
            self.assertTrue(generated and all(a[a.index("-loglevel") + 1] == "warning" for a in generated if "-loglevel" in a))
            self.assertTrue(all("-loglevel" in a or "-v" in a for a in generated if a[-1] != "-version"))

    def test_a_zero_exit_warning_fails_the_fixture_and_ships_nothing(self):
        r, shipped, _ = self._run('"$REAL" "$@"; rc=$?; case "$*" in *-version*) ;; *) echo "[aac @ 0x0] warning: injected" >&2;; esac; exit $rc')
        self.assertFalse(r["ok"])
        self.assertIn("injected", r["error"])
        self.assertEqual(shipped, [])

    def test_a_probe_warning_fails_the_fixture(self):
        r, shipped, _ = self._run('exec "$REAL" "$@"', '"$REAL" "$@"; rc=$?; echo "warning: probe" >&2; exit $rc')
        self.assertFalse(r["ok"])
        self.assertIn("probe", r["error"])
        self.assertEqual(shipped, [])

    def test_a_failed_version_command_is_recorded_and_refused(self):
        r, shipped, _ = self._run('case "$*" in *-version*) echo "no version here" >&2; exit 3;; esac; exec "$REAL" "$@"')
        self.assertFalse(r["ok"])
        self.assertEqual(r["version"]["rc"], 3)
        self.assertIn("no version here", r["version"]["stderr"])
        self.assertEqual(shipped, [])

    def test_empty_and_insufficient_encoder_output_are_named_failures(self):
        empty = 'case "$*" in *-version*) exec "$REAL" "$@";; esac; for a; do last=$a; done; : > "$last"; exit 0'
        r, shipped, out = self._run(empty)
        self.assertFalse(r["ok"])
        self.assertIn("0 of 118", r["error"])
        self.assertEqual(shipped, [])
        self.assertTrue((out / "audio-receipt.json").exists(), "the failed receipt is still written")
        one = ('case "$*" in *-version*) exec "$REAL" "$@";; esac; for a; do last=$a; done; '
               'printf "\\377\\361\\120\\200\\001\\037\\374\\000" > "$last"; exit 0')
        r, shipped, _ = self._run(one)
        self.assertFalse(r["ok"])
        self.assertIn("1 of 118", r["error"])
        self.assertEqual(shipped, [])


@unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "needs ffmpeg and ffprobe on PATH")
class Generation(unittest.TestCase):
    def test_generate_twice_identical_valid_and_ledgered(self):
        import make_audio_input as m
        import media_oracle
        with tempfile.TemporaryDirectory() as tmp:
            r = m.generate(Path(tmp) / "run", video_frames=60, video_step_us=41_666)
            self.assertTrue(r["ok"], r.get("error") or r["checks"])
            self.assertEqual(r["codec"], "mp4a.40.2")
            self.assertEqual((r["sample_rate"], r["channels"], r["frames"]), (48_000, 2, 118))
            self.assertEqual(r["audio_specific_config"], "1190")
            run = Path(tmp) / "run"
            loaded = media_oracle.load_objects(run / "audio-objects.bin")
            self.assertEqual(len(loaded), 118)
            self.assertTrue(all(e.is_sync for e in loaded), "every AAC access unit is independently decodable")
            self.assertEqual([e.pts_us for e in loaded], [m.audio_pts_us(i, 48_000) for i in range(118)])
            raw = (run / "in.aac").read_bytes()
            self.assertEqual([e.payload for e in loaded], [f.access_unit for f in m.split_adts(raw)])
            self.assertEqual(r["checks"], {k: True for k in r["checks"]})
            prefix = r["decoded"]["low_amplitude_prefix"]
            self.assertEqual(prefix["threshold"], m.LOW_AMPLITUDE)
            self.assertIsInstance(prefix["samples"], int)
            self.assertNotIn("leading_silence_samples", r["decoded"], "a threshold-defined prefix, not a delay claim")
            planned = r.get("planned_warnings", [])
            self.assertTrue(all(w["label"] in ("probe", "decode") and m.PLANNED_DURATION_WARNING.fullmatch(w["stderr"])
                                for w in planned), "only the declared duration-estimate line is tolerated, and it is recorded")

    def test_missing_tools_are_named_not_guessed(self):
        import make_audio_input as m
        with tempfile.TemporaryDirectory() as tmp:
            r = m.generate(Path(tmp) / "run", video_frames=60, video_step_us=41_666, ffmpeg=None, ffprobe="/nonexistent/ffprobe")
            self.assertFalse(r["ok"])
            self.assertIn("ffmpeg", r["error"])


if __name__ == "__main__":
    unittest.main()
