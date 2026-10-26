"""The relay route dump behind the `go` barrier's `route_two_subscriptions`.

Dumps here are written out by hand in the exact text of `relay/src/core/relay.c`
moqr_core_route_dump_text, framed as `tools/moq5-relay/cli/main.c` dump_emit writes them to
stderr. The probe rows run a stub relay under the real `Child`, which answers SIGUSR1 by writing
the next scripted block to its stderr; no relay is started and nothing connects anywhere.
"""
import json
import signal
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import route_dump                                            # noqa: E402
from route_dump import RouteDump, RouteDumpError, Sub, Track  # noqa: E402
from runtime_support import Child                            # noqa: E402

NS, NAME = (b"live", b"cam1"), b"video"

EMPTY = ["epochs: node=0 shard=0 route=0",
         "announces:", "  (none)",
         "namespace_subscriptions:", "  (none)",
         "tracks:", "  (none)"]


def live_dump(*sub_lines, state="active", upstream="1", route=4):
    return [f"epochs: node=1 shard=0 route={route}",
            "announces:", "  live/cam1 -> binding 1",
            "namespace_subscriptions:", "  (none)",
            "tracks:",
            f'  live/cam1 "video" state={state} upstream_binding={upstream}',
            "    log: groups=0 records=0 bytes=0 oldest=null newest=null eot=0",
            "    subs:",
            *(sub_lines or ("      (none)",))]


def sub_line(binding, state="active"):
    return (f"      binding={binding} state={state} start=0/0 end=- lag_groups=0 frontier=null "
            "skip_pending=0 start_before_retention=0")


ONE_SUB = live_dump(sub_line(2))
TWO_SUBS = live_dump(sub_line(2), sub_line(3))

POPULATED = ["epochs: node=0 shard=0 route=7",
             "announces:", "  v/cam -> binding 1", "  v/cam2 -> binding 1",
             "namespace_subscriptions:", "  v -> binding 2",
             "tracks:",
             '  v/cam "hd" state=active upstream_binding=1',
             "    log: groups=3 records=3 bytes=96 oldest=1 newest=3 eot=0",
             "    subs:",
             "      binding=3 state=active start=1/0 end=- lag_groups=3 frontier=1 skip_pending=0 start_before_retention=0",
             '  v/cam2 "x" state=pending upstream_binding=1',
             "    log: groups=0 records=0 bytes=0 oldest=null newest=null eot=0",
             "    subs:",
             "      binding=4 state=parked start=0/0 end=9 lag_groups=0 frontier=null skip_pending=1 start_before_retention=1",
             '  v/old "w" state=warm upstream_binding=-',
             "    log: groups=1 records=2 bytes=10 oldest=5 newest=5 eot=1",
             "    subs:",
             "      (none)"]


def framed(block):
    """The block as dump_emit writes it: an empty line, the header, the dump, an empty line."""
    return ["", route_dump.ROUTES_HEADER, *block, ""]


class Parse(unittest.TestCase):
    def test_an_empty_relay(self):
        self.assertEqual(route_dump.parse(EMPTY), RouteDump((0, 0, 0), (), (), ()))

    def test_the_declared_track_with_two_subscriptions(self):
        s2 = Sub(2, "active", 0, 0, None, 0, None, False, False)
        self.assertEqual(route_dump.parse(TWO_SUBS), RouteDump(
            (1, 0, 4), ((NS, 1),), (),
            (Track(NS, NAME, "active", 1, {"groups": 0, "records": 0, "bytes": 0, "oldest": None, "newest": None,
                                           "eot": False}, (s2, Sub(3, "active", 0, 0, None, 0, None, False, False))),)))

    def test_every_field_form(self):
        d = route_dump.parse(POPULATED)
        self.assertEqual(d.epochs, (0, 0, 7))
        self.assertEqual(d.announces, (((b"v", b"cam"), 1), ((b"v", b"cam2"), 1)))
        self.assertEqual(d.namespace_subscriptions, (((b"v",), 2),))
        hd, x, w = d.tracks
        self.assertEqual((hd.namespace, hd.name, hd.state, hd.upstream_binding), ((b"v", b"cam"), b"hd", "active", 1))
        self.assertEqual(hd.log, {"groups": 3, "records": 3, "bytes": 96, "oldest": 1, "newest": 3, "eot": False})
        self.assertEqual(hd.subs, (Sub(3, "active", 1, 0, None, 3, 1, False, False),))
        self.assertEqual(x.subs, (Sub(4, "parked", 0, 0, 9, 0, None, True, True),))
        self.assertEqual((w.state, w.upstream_binding, w.subs, w.log["eot"]), ("warm", None, (), True))

    def test_escaped_bytes_and_an_empty_namespace(self):
        d = route_dump.parse(["epochs: node=0 shard=0 route=1",
                              "announces:", r"  a\x2fb/c -> binding 5",
                              "namespace_subscriptions:", "  (none)",
                              "tracks:",
                              r'   "q\x22\x5c\xff" state=unknown upstream_binding=-',
                              "    log: groups=0 records=0 bytes=0 oldest=null newest=null eot=0",
                              "    subs:", "      (none)"])
        self.assertEqual(d.announces, (((b"a/b", b"c"), 5),))
        self.assertEqual((d.tracks[0].namespace, d.tracks[0].name), ((), b'q"\\\xff'))

    def test_every_departure_from_the_grammar_is_named(self):
        cases = (
            ("no lines", [], "end of dump"),
            ("an unknown first line", ["epoch: node=0 shard=0 route=0"] + EMPTY[1:], "line 1"),
            ("a missing section", EMPTY[:3] + EMPTY[5:], "line 4"),
            ("(none) beside an entry", EMPTY[:2] + ["  (none)", "  v -> binding 1"] + EMPTY[3:], "line 4"),
            ("a track without its log line", TWO_SUBS[:7] + TWO_SUBS[8:], "line 8"),
            ("a subscription at the wrong indent", TWO_SUBS[:10] + ["    " + TWO_SUBS[10].lstrip()] + TWO_SUBS[11:], "line 11"),
            ("upper-case hex", EMPTY[:2] + [r"  a\x2Fb -> binding 1"] + EMPTY[3:], "line 3"),
            ("a raw slash in a name", live_dump()[:6] + ['  live/cam1 "vi/deo" state=active upstream_binding=1'] + live_dump()[7:], "line 7"),
            ("an unknown track state", live_dump(state="idle"), "line 7"),
            ("an unknown subscription state", live_dump(sub_line(2, "paused")), "line 10"),
            ("a line after the dump", EMPTY + ["  (none)"], "line 8"),
            ("a negative binding", EMPTY[:2] + ["  v -> binding -1"] + EMPTY[3:], "line 3"),
            ("a non-ASCII digit", ["epochs: node=0 shard=\u0661 route=0"] + EMPTY[1:], "line 1"),
        )
        for label, lines, where in cases:
            with self.subTest(label):
                with self.assertRaises(RouteDumpError) as caught:
                    route_dump.parse(lines)
                self.assertIn(where, str(caught.exception))


class Framing(unittest.TestCase):
    def test_complete_blocks_are_found_among_other_stderr(self):
        stderr = (["{\"schema\":\"RELAY_READY_V1\"}", "", "===== MOQ5 Relay metrics =====", "moq_relay_up 1", ""]
                  + framed(ONE_SUB) + ["# HELP moq_relay_up relay"] + framed(TWO_SUBS))
        self.assertEqual(route_dump.complete_blocks(stderr), [ONE_SUB, TWO_SUBS])
        after_first = stderr.index("# HELP moq_relay_up relay")
        self.assertEqual(route_dump.complete_blocks(stderr, after_first), [TWO_SUBS])

    def test_an_unterminated_block_is_not_yet_a_block(self):
        self.assertEqual(route_dump.complete_blocks(framed(ONE_SUB)[:-1]), [])
        self.assertEqual(route_dump.complete_blocks(framed(ONE_SUB)[:4]), [])

    def test_a_multi_lane_or_interrupted_block_is_refused(self):
        for label, lines, needle in (
                ("a shard routes header", ["", "===== MOQ5 Relay shard 0 routes =====", *ONE_SUB, ""], "single-lane"),
                ("a second header inside a block", framed(ONE_SUB)[:5] + ["===== MOQ5 Relay routes ====="] + ONE_SUB + [""],
                 "before the routes block")):
            with self.subTest(label):
                with self.assertRaises(RouteDumpError) as caught:
                    route_dump.complete_blocks(lines)
                self.assertIn(needle, str(caught.exception))


def with_announce(dump, binding):
    return [line.replace("  live/cam1 -> binding 1", f"  live/cam1 -> binding {binding}") for line in dump]


def extra_track(dump):
    """`dump` with a companion catalog track in the same namespace (not the target)."""
    return dump + ['  live/cam1 "catalog" state=active upstream_binding=1',
                   "    log: groups=1 records=1 bytes=40 oldest=0 newest=0 eot=0",
                   "    subs:", sub_line(7), sub_line(8)]


class BaselineVerdicts(unittest.TestCase):
    """Before the player start: the exact route, and exactly one active subscription on H != U."""

    def test_baseline_verdicts(self):
        cases = (
            ("no relay routes", EMPTY, "waiting", None),
            ("the track without its announce", with_announce(ONE_SUB, 1)[:2] + ["  (none)"] + ONE_SUB[3:], "waiting", None),
            ("announced twice", ONE_SUB[:3] + ["  live/cam1 -> binding 5"] + ONE_SUB[3:], "refused", None),
            ("pending track", live_dump(sub_line(2), state="pending"), "waiting", None),
            ("no upstream yet", live_dump(sub_line(2), upstream="-"), "waiting", None),
            ("upstream is not the announcer", with_announce(ONE_SUB, 9), "refused", None),
            ("no subscription", live_dump(), "waiting", None),
            ("one parked subscription", live_dump(sub_line(2, "parked")), "waiting", None),
            ("one active subscription", ONE_SUB, "ready", (1, 2)),
            ("with a companion track", extra_track(ONE_SUB), "ready", (1, 2)),
            ("a subscription on the upstream", live_dump(sub_line(1)), "refused", None),
            ("two subscriptions before the player", TWO_SUBS, "refused", None),
            ("one active and one parked", live_dump(sub_line(2), sub_line(3, "parked")), "refused", None),
            ("the track twice", ONE_SUB + ONE_SUB[6:], "refused", None),
        )
        for label, lines, verdict, value in cases:
            with self.subTest(label):
                got, reason, base = route_dump.judge_baseline(route_dump.parse(lines), NS, NAME)
                self.assertEqual(got, verdict, reason)
                self.assertIn('live/cam1 "video"', reason)
                self.assertEqual(None if base is None else (base.upstream, base.host), value)


def av_dump(video_subs, audio_subs, audio_state="active", audio_upstream="1"):
    """The video dump with the declared audio track beside it; `audio_subs` None leaves no audio route."""
    lines = live_dump(*video_subs)
    if audio_subs is None:
        return lines
    return lines + [f'  live/cam1 "audio" state={audio_state} upstream_binding={audio_upstream}',
                    "    log: groups=0 records=0 bytes=0 oldest=null newest=null eot=0",
                    "    subs:", *(audio_subs or ("      (none)",))]


class AudioVerdicts(unittest.TestCase):
    """A/V mode, after the video proof: the audio track has exactly one subscription, active, and it
    is the player's video binding V -- never the host's H, which subscribes only video."""

    def test_audio_verdicts(self):
        video = (sub_line(2), sub_line(3))
        cases = (
            ("no audio route yet", av_dump(video, None), "waiting", None),
            ("routed, no subscription yet", av_dump(video, []), "waiting", None),
            ("the player's one subscription", av_dump(video, [sub_line(3)]), "ready", 3),
            ("the player's subscription not yet active", av_dump(video, [sub_line(3, "parked")]), "waiting", None),
            ("the host on audio", av_dump(video, [sub_line(2)]), "refused", None),
            ("the host and the player on audio", av_dump(video, [sub_line(3), sub_line(2)]), "refused", None),
            ("a foreign binding", av_dump(video, [sub_line(4)]), "refused", None),
            ("the player twice", av_dump(video, [sub_line(3), sub_line(3)]), "refused", None),
            ("audio on its upstream", av_dump(video, [sub_line(1)]), "refused", None),
            ("audio from another upstream", av_dump(video, [sub_line(3)], audio_upstream="7"), "refused", None),
        )
        for label, lines, verdict, value in cases:
            with self.subTest(label):
                got, reason, player = route_dump.judge_player_audio(route_dump.parse(lines), NS, b"audio", host=2, player=3)
                self.assertEqual(got, verdict, reason)
                self.assertEqual(player, value)


class AfterStartVerdicts(unittest.TestCase):
    """After the player start: the baseline's route and {H, V}, V new, both active, neither U."""

    def test_after_start_verdicts(self):
        base = route_dump.Baseline(upstream=1, host=2)
        cases = (
            ("the same single subscription (established, not yet subscribed)", ONE_SUB, "waiting", None),
            ("the baseline and one new", TWO_SUBS, "ready", 3),
            ("with a companion track", extra_track(TWO_SUBS), "ready", 3),
            ("the baseline gone, two others", live_dump(sub_line(3), sub_line(4)), "refused", None),
            ("the baseline gone, one other", live_dump(sub_line(3)), "refused", None),
            ("a third subscription", live_dump(sub_line(2), sub_line(3), sub_line(4)), "refused", None),
            ("the new one parked", live_dump(sub_line(2), sub_line(3, "parked")), "refused", None),
            ("the baseline parked", live_dump(sub_line(2, "parked"), sub_line(3)), "refused", None),
            ("the baseline twice", live_dump(sub_line(2), sub_line(2)), "refused", None),
            ("a subscription on the upstream", live_dump(sub_line(2), sub_line(1)), "refused", None),
            ("the announce moved", with_announce(live_dump(sub_line(2), sub_line(3), upstream="5"), 5), "refused", None),
            ("upstream is not the announcer", with_announce(TWO_SUBS, 9), "refused", None),
            ("the route lost", EMPTY, "refused", None),
            ("the track gone pending", live_dump(sub_line(2), sub_line(3), state="pending"), "refused", None),
        )
        for label, lines, verdict, value in cases:
            with self.subTest(label):
                got, reason, new = route_dump.judge_after_start(route_dump.parse(lines), NS, NAME, base)
                self.assertEqual(got, verdict, reason)
                self.assertEqual(new, value)

    def test_no_single_dump_count_sets_the_barrier(self):
        self.assertFalse(hasattr(route_dump, "judge"))
        self.assertFalse(hasattr(route_dump, "route_probe"))


STUB_RELAY = r'''
import json, signal, sys, time
blocks, mode, log = json.load(open(sys.argv[1])), sys.argv[2], sys.argv[3]
served = [0]
def on_usr1(*_):
    with open(log, "a") as f:
        f.write("usr1\n")
    if mode == "silent":
        return
    block = blocks[min(served[0], len(blocks) - 1)]
    served[0] += 1
    sys.stderr.write("# HELP moq_relay_up relay\nmoq_relay_up 1\n")
    sys.stderr.write("\n===== MOQ5 Relay routes =====\n")
    sys.stderr.flush()
    time.sleep(0.05)                                   # the reader sees a partial block first
    sys.stderr.write("".join(line + "\n" for line in block) + "\n")
    sys.stderr.flush()
signal.signal(signal.SIGUSR1, on_usr1)
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
print("ready", flush=True)
deadline = time.monotonic() + (0.3 if mode == "exit" else 30)
while time.monotonic() < deadline:
    time.sleep(0.02)
'''


class FakeGate:
    """Records facts and refusals, each also appended to the run's ordered event log."""

    def __init__(self, log):
        self.facts, self.refusals, self.changed, self.log = [], [], threading.Event(), log

    def fact(self, name):
        self.facts.append(name)
        self.log(f"fact {name}")
        self.changed.set()

    def refuse(self, reason):
        self.refusals.append(reason)
        self.log("refuse")
        self.changed.set()


class StagedProbe(unittest.TestCase):
    """The staged probe against a stub relay owned by the real `Child`: its stderr reader is in the
    path. The test plays the supervisor. Progress and absence are judged from one ordered event
    log: the stub appends `usr1` when it receives each request, the test appends `host_active` and
    `player_started` just before setting those events, and the gate appends its facts and
    refusals. Before setting either event the test awaits a latch proving the probe has entered
    its wait for that event while it is still unset: `route_dump._wait` is wrapped, for the test
    only, to append `waiting <event>` and release the latch. Every wait is bounded and fails by
    name."""

    TIMEOUT_S = 5.0

    def start(self, blocks, mode="normal", audio_track=None):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        d = Path(tmp.name)
        (d / "relay.py").write_text(STUB_RELAY)
        (d / "blocks.json").write_text(json.dumps(blocks))
        self.log = d / "events.log"
        ready = threading.Event()
        self.relay = Child("relay", [sys.executable, "-I", d / "relay.py", d / "blocks.json", mode, self.log],
                           {"PATH": "/usr/bin:/bin"}, str(d), on_line=lambda line: ready.set() if line == "ready" else None)
        self.addCleanup(self.relay.settle, 5, signal_first=signal.SIGTERM)
        self.assertTrue(ready.wait(self.TIMEOUT_S), "stub relay never became ready")
        self.gate, self.stop, self.record = FakeGate(self.note), threading.Event(), []
        self.host_active, self.player_started = threading.Event(), threading.Event()
        names = {id(self.host_active): "host_active", id(self.player_started): "player_started"}
        self.entered = {"host_active": threading.Event(), "player_started": threading.Event()}
        real_wait = route_dump._wait

        def entering(event, stop, alive):
            name = names[id(event)]
            if not event.is_set():
                self.note(f"waiting {name}")
                self.entered[name].set()
            return real_wait(event, stop, alive)
        route_dump._wait = entering
        self.addCleanup(setattr, route_dump, "_wait", real_wait)
        self.probe, self.baseline_recorded = route_dump.route_barrier(NS, NAME, self.host_active, self.player_started,
                                                                      interval_s=0.05, record=self.record,
                                                                      audio_track=audio_track)
        self.thread = threading.Thread(target=self.probe, args=(self.gate, self.relay, self.stop), daemon=True)
        self.thread.start()

    def note(self, what):
        with open(self.log, "a") as f:
            f.write(what + "\n")

    def events(self):
        return self.log.read_text().splitlines() if self.log.exists() else []

    def announce(self, event, name, entered=True):
        """Sets `event`, logged; by default only once the probe is waiting for it."""
        if entered:
            self.assertTrue(self.entered[name].wait(self.TIMEOUT_S),
                            f"the probe did not enter its {name} wait within {self.TIMEOUT_S}s; events {self.events()}")
        self.note(name)
        event.set()

    def wait_until(self, predicate, what):
        deadline = time.monotonic() + self.TIMEOUT_S
        while not predicate():
            if time.monotonic() > deadline:
                self.fail(f"{what} not observed within {self.TIMEOUT_S}s; events {self.events()}, record {self.record}")
            time.sleep(0.01)

    def after_start(self):
        return [r for r in self.record if r["phase"] == "after start"]

    def end_probe(self):
        self.stop.set()
        self.thread.join(self.TIMEOUT_S)
        self.assertFalse(self.thread.is_alive(), "probe did not return")

    def await_gate(self):
        self.assertTrue(self.gate.changed.wait(self.TIMEOUT_S), f"no gate fact or refusal within {self.TIMEOUT_S}s")
        self.end_probe()

    def test_the_fact_lands_only_on_the_staged_transition(self):
        # baseline: none, then H=2; after start: still {2} (established without a subscription), then {2, 3}
        self.start([EMPTY, ONE_SUB, ONE_SUB, TWO_SUBS])
        self.announce(self.host_active, "host_active")
        self.assertTrue(self.baseline_recorded.wait(self.TIMEOUT_S), "baseline not recorded")
        self.announce(self.player_started, "player_started")
        self.await_gate()
        self.assertEqual(self.events(), ["waiting host_active", "host_active", "usr1", "usr1",
                                         "waiting player_started", "player_started", "usr1", "usr1",
                                         "fact route_two_subscriptions"])
        self.assertEqual([(r["phase"], r["verdict"]) for r in self.record],
                         [("baseline", "waiting"), ("baseline", "ready"), ("after start", "waiting"), ("after start", "ready")])

    def test_in_av_mode_the_fact_waits_for_the_players_one_audio_subscription(self):
        video = (sub_line(2), sub_line(3))
        self.start([ONE_SUB, av_dump(video, None), av_dump(video, []), av_dump(video, [sub_line(3)])], audio_track=b"audio")
        self.announce(self.host_active, "host_active")
        self.assertTrue(self.baseline_recorded.wait(self.TIMEOUT_S), "baseline not recorded")
        self.announce(self.player_started, "player_started")
        self.await_gate()
        self.assertEqual(self.gate.facts, ["route_two_subscriptions"])
        self.assertEqual(self.gate.refusals, [])
        self.assertEqual([(r["phase"], r["verdict"]) for r in self.record],
                         [("baseline", "ready"), ("after start", "ready"), ("audio", "waiting"), ("audio", "ready")])

    def test_in_av_mode_the_host_on_audio_is_refused(self):
        video = (sub_line(2), sub_line(3))
        self.start([ONE_SUB, av_dump(video, [sub_line(2)])], audio_track=b"audio")
        self.announce(self.host_active, "host_active")
        self.assertTrue(self.baseline_recorded.wait(self.TIMEOUT_S), "baseline not recorded")
        self.announce(self.player_started, "player_started")
        self.await_gate()
        self.assertEqual(self.gate.facts, [])
        self.assertEqual(len(self.gate.refusals), 1)
        self.assertIn("(audio)", self.gate.refusals[0])

    def test_an_unchanged_set_after_start_never_sets_the_fact(self):
        self.start([ONE_SUB])
        self.announce(self.host_active, "host_active")
        self.assertTrue(self.baseline_recorded.wait(self.TIMEOUT_S), "baseline not recorded")
        self.announce(self.player_started, "player_started")
        self.wait_until(lambda: len(self.after_start()) >= 2, "two judged after-start dumps")
        self.end_probe()
        after = self.after_start()
        self.assertEqual({(r["verdict"], r["reason"]) for r in after},
                         {("waiting", 'live/cam1 "video" has no new subscription beside the baseline 2')})
        events = self.events()
        self.assertEqual(events[:5], ["waiting host_active", "host_active", "usr1", "waiting player_started", "player_started"])
        requested_after_start = events[5:]
        self.assertEqual(set(requested_after_start), {"usr1"})          # requests only: no fact, no refusal
        # each judged dump answered one request; at most one more request was outstanding when stopped
        self.assertIn(len(requested_after_start) - len(after), (0, 1))
        self.assertEqual((self.gate.facts, self.gate.refusals), ([], []))

    def test_two_subscriptions_before_the_player_are_refused(self):
        self.start([TWO_SUBS])
        self.announce(self.host_active, "host_active")
        self.await_gate()
        self.assertFalse(self.baseline_recorded.is_set())
        self.assertEqual(self.events(), ["waiting host_active", "host_active", "usr1", "refuse"])
        self.assertEqual(self.gate.refusals, ['relay route dump (baseline): live/cam1 "video" has 2 subscriptions '
                                              "before the player start; the baseline is one"])

    def test_a_player_started_before_the_baseline_is_refused(self):
        self.start([ONE_SUB, TWO_SUBS])
        self.assertTrue(self.entered["host_active"].wait(self.TIMEOUT_S), "the probe did not enter its host_active wait")
        self.announce(self.player_started, "player_started", entered=False)   # early: no baseline exists yet
        self.announce(self.host_active, "host_active")
        self.await_gate()                # the probe has returned: no request can follow
        self.assertEqual(self.events(), ["waiting host_active", "player_started", "host_active", "refuse"])
        self.assertEqual(self.gate.refusals, ["relay route dump: the player was started before the route baseline"])

    def test_the_baseline_subscription_vanishing_is_refused(self):
        self.start([ONE_SUB, live_dump(sub_line(3), sub_line(4))])
        self.announce(self.host_active, "host_active")
        self.assertTrue(self.baseline_recorded.wait(self.TIMEOUT_S), "baseline not recorded")
        self.announce(self.player_started, "player_started")
        self.await_gate()
        self.assertEqual(self.events(), ["waiting host_active", "host_active", "usr1", "waiting player_started",
                                         "player_started", "usr1", "refuse"])
        self.assertEqual(len(self.gate.refusals), 1)
        self.assertIn("no longer has the baseline subscription 2", self.gate.refusals[0])

    def test_a_malformed_dump_is_refused_by_name(self):
        self.start([ONE_SUB[:7] + ONE_SUB[8:]])
        self.announce(self.host_active, "host_active")
        self.await_gate()
        self.assertEqual(self.events(), ["waiting host_active", "host_active", "usr1", "refuse"])
        self.assertTrue(self.gate.refusals[0].startswith("relay route dump line 8:"), self.gate.refusals[0])

    def test_a_relay_that_never_answers_gets_one_request(self):
        self.start([ONE_SUB], mode="silent")
        self.announce(self.host_active, "host_active")
        self.wait_until(lambda: "usr1" in self.events(), "the first request")
        time.sleep(0.5)                  # no retry within ten probe intervals; not a claim about later
        self.end_probe()
        self.assertEqual((self.events(), self.record), (["waiting host_active", "host_active", "usr1"], []))

    def test_a_relay_that_exits_ends_the_probe(self):
        self.start([EMPTY], mode="exit")
        self.announce(self.host_active, "host_active")
        self.thread.join(self.TIMEOUT_S)                    # ends by itself: `stop` is never set
        self.assertFalse(self.thread.is_alive(), "probe outlived the relay")
        self.assertFalse(self.relay.alive())
        self.assertEqual((self.gate.facts, self.gate.refusals), ([], []))


if __name__ == "__main__":
    unittest.main()
