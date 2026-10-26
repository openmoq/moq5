"""The explicit av-diagnostic mode: the publisher's exact two-track verdict (video-only unchanged), and
the composition that feeds the player both tracks with the checked budget, observer off, the publisher's
declared AAC track, and the route proof of the player's one audio subscription. Offline; nothing runs."""
import json
import struct
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import device_lane                 # noqa: E402
import live_actors                 # noqa: E402
import route_dump                  # noqa: E402
from run_android_media import publisher_checks  # noqa: E402
from test_live_actors import Fixture             # noqa: E402

VIDEO_ONLY_KEYS = {"publisher_lifecycle_exact", "publisher_writes_admitted", "publisher_end_track_rc_ok",
                   "publisher_complete_rc_ok", "publisher_shutdown_ok"}


def video_events(n=3):
    return ([{"event": "connected"}, {"event": "ready"}, {"event": "demand", "active": 1}]
            + [{"event": "wrote", "index": i, "pts_us": i * 41_666, "rc": 0} for i in range(n)]
            + [{"event": "end_track", "rc": 0}, {"event": "complete", "rc": 0}, {"event": "stats", "rc": 0},
               {"event": "demand_log"}, {"event": "shutdown", "drain_rc": 0, "stop_rc": 0, "fatal": 0, "exit": 0}])


def av_events(nv=3, na=5):
    """The publisher's A/V record stream: both demands, the writes merged by pts (video first on ties),
    each track ended once, then completion."""
    v = [("video", i, i * 41_666) for i in range(nv)]
    a = [("audio", j, j * 21_333) for j in range(na)]
    merged = sorted(v + a, key=lambda w: (w[2], w[0] != "video"))
    writes = [{"event": "wrote", "index": i, "pts_us": pts, "rc": 0} if t == "video"
              else {"event": "wrote_audio", "track": "audio", "index": i, "pts_us": pts, "rc": 0}
              for t, i, pts in merged]
    return ([{"event": "connected"}, {"event": "ready"}, {"event": "demand", "active": 1},
             {"event": "demand_audio", "track": "audio", "active": 1}] + writes
            + [{"event": "end_track", "rc": 0}, {"event": "end_track_audio", "track": "audio", "rc": 0},
               {"event": "complete", "rc": 0}, {"event": "stats", "rc": 0}, {"event": "demand_log"},
               {"event": "demand_log_audio", "track": "audio"},
               {"event": "shutdown", "drain_rc": 0, "stop_rc": 0, "fatal": 0, "exit": 0}])


class PublisherVerdict(unittest.TestCase):
    def test_video_only_verdict_is_unchanged(self):
        got = publisher_checks(video_events(), 3)
        self.assertEqual(set(got), VIDEO_ONLY_KEYS)
        self.assertTrue(all(got.values()), got)
        self.assertFalse(publisher_checks(av_events(), 3)["publisher_lifecycle_exact"],
                         "audio records are not part of the video-only lifecycle")

    def test_the_exact_av_stream_passes_every_check(self):
        got = publisher_checks(av_events(), 3, n_audio=5)
        self.assertEqual(set(got), VIDEO_ONLY_KEYS)
        self.assertTrue(all(got.values()), got)

    def test_every_departure_fails_its_named_check(self):
        def mutated(fn):
            ev = av_events()
            fn(ev)
            return publisher_checks(ev, 3, n_audio=5)

        def drop(name):
            return lambda ev: ev.remove(next(e for e in ev if e["event"] == name))

        def audio_write(j):
            return lambda ev: next(e for e in ev if e["event"] == "wrote_audio" and e["index"] == j)

        def swap_first_tie(ev):
            i = next(k for k, e in enumerate(ev) if e["event"] == "wrote")      # video 0 at pts 0 precedes audio 0
            ev[i], ev[i + 1] = ev[i + 1], ev[i]

        def out_of_pts(ev):
            k = next(k for k, e in enumerate(ev) if e["event"] == "wrote_audio" and e["index"] == 3)
            ev.insert(k - 2, ev.pop(k))

        cases = {
            "publisher_lifecycle_exact": [drop("demand_audio"), drop("end_track_audio"), drop("demand_log_audio"),
                                          lambda ev: ev.insert(4, {"event": "demand_audio", "active": 1})],
            "publisher_writes_admitted": [lambda ev: audio_write(2)(ev).update(rc=-11),
                                          lambda ev: audio_write(4)(ev).update(index=5),
                                          lambda ev: audio_write(1)(ev).update(track="video"),
                                          drop("wrote_audio"), swap_first_tie, out_of_pts],
            "publisher_end_track_rc_ok": [lambda ev: next(e for e in ev if e["event"] == "end_track_audio").update(rc=-1)],
        }
        for key, fns in cases.items():
            for n, fn in enumerate(fns):
                with self.subTest(key=key, case=n):
                    self.assertFalse(mutated(fn)[key])

    def test_the_audio_count_is_exact(self):
        self.assertFalse(publisher_checks(av_events(na=4), 3, n_audio=5)["publisher_writes_admitted"])
        self.assertFalse(publisher_checks(av_events(na=6), 3, n_audio=5)["publisher_writes_admitted"])


def write_audio_fixture(run_dir, n=118, ok=True, asc="1190", rate=48_000, channels=2, receipt_rate=None):
    with open(run_dir / "audio-objects.bin", "wb") as f:
        for j in range(n):
            payload = bytes([0x21]) + j.to_bytes(2, "big")
            f.write(struct.pack("<IBQ", len(payload), 1, j * 1024 * 1_000_000 // rate) + payload)
    (run_dir / "audio-receipt.json").write_text(json.dumps(
        {"ok": ok, "codec": "mp4a.40.2", "audio_specific_config": asc,
         "sample_rate": rate if receipt_rate is None else receipt_rate, "channels": channels,
         "frames": n}))


class Composition(unittest.TestCase):
    def compose(self, fx, mode):
        lane = device_lane.EmulatorLane("/opt/sdk/emulator/emulator", str(fx.adb), fx.root / "emulator.log",
                                        runner=lambda *a, **k: None)
        return live_actors.compose(fx.live_run(), lane, "/builds/player.apk", player_mode=mode)

    def test_av_mode_feeds_both_tracks_with_the_checked_budget_and_the_observer_off(self):
        fx = Fixture(self)
        write_audio_fixture(fx.run_dir)
        seen = {}
        real = route_dump.route_barrier
        route_dump.route_barrier = lambda *a, **k: seen.update(k) or real(*a, **k)
        self.addCleanup(setattr, route_dump, "route_barrier", real)
        actors, n = self.compose(fx, "av-diagnostic")
        player = actors.player_start.player
        self.assertEqual(n, 60, "the host and the video write ledger judge the video track")
        self.assertEqual(actors.audio_objects, 118)
        self.assertEqual((player.observing, player.tracing), (False, True))
        self.assertEqual(player.n_objects, 178)
        self.assertEqual(player.inputs["Objects to receive"], "178")
        self.assertEqual(actors.publisher_argv[-5:], ["--audio", str(fx.run_dir / "audio-objects.bin"), "1190", "48000", "2"])
        actors.route_barrier(object(), object(), [])
        self.assertEqual(seen.get("audio_track"), b"audio")

    def test_video_modes_are_unchanged(self):
        for mode in ("acceptance", "observer-off-diagnostic"):
            with self.subTest(mode):
                fx = Fixture(self)
                write_audio_fixture(fx.run_dir)
                actors, n = self.compose(fx, mode)
                self.assertEqual((n, actors.audio_objects, actors.player_start.player.n_objects), (60, None, 60))
                self.assertNotIn("--audio", actors.publisher_argv)

    def test_an_unusable_audio_fixture_is_refused_before_anything_runs(self):
        for label, kw in (("not ok", {"ok": False}), ("no objects", {"n": 0}), ("rate", {"receipt_rate": "48000"}),
                          ("asc", {"asc": "11"})):
            with self.subTest(label):
                fx = Fixture(self)
                write_audio_fixture(fx.run_dir, **kw)
                with self.assertRaises(ValueError):
                    self.compose(fx, "av-diagnostic")
        fx = Fixture(self)
        with self.assertRaises(ValueError):
            self.compose(fx, "av-diagnostic")                    # no fixture at all


if __name__ == "__main__":
    unittest.main()
