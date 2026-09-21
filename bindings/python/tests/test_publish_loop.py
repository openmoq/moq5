"""RED contract for a finite publisher example: examples/publish_loop.py.

These rows are written RED, against a file that does not exist yet. A missing
example is an ATTRIBUTABLE failure -- the loader fails by a named assertion
carrying the path, never a swallowed ImportError -- and the controls at the
bottom run every row, model AND integration, against scoped stand-ins to show
the assertions have discriminating power before any product exists.

Scope: ONE finite submission loop over the delivered synchronous sender
binding (write / end_track / wait / stats / has_media_subscriber). It is not a
codec, a capture layer, an asyncio runtime, a graceful finish or a delivery
proof, and it adds no binding method and no native prerequisite.

The intended entry point:

    publish(sender, plan, now_us, deadline_us,
            WriteOutcome, WaitResult, MoqError, note_dropped,
            wait_us=250_000, require_demand=False) -> dict

  sender      a LIVE Sender the CALLER owns. The loop never attaches, closes,
              destroys, adds or removes a track, and never requests
              completion: closing discards whatever the service still has
              queued, and an accepted completion request marks every track
              removed, whose queued media the service then discards as it
              tears them down. Either would turn accepted work into lost work
              and report success, so both stay the caller's decision, after
              this record has been read.
  plan        a FINITE ordered sequence of (track, object) submissions -- a
              sequence, not a stream. The loop holds the EXACT object it was
              given until that submission is accepted, advances only then,
              and never submits an accepted object again.
  now_us      an injected monotonic clock, and `deadline_us` an absolute stop
              on it.
  note_dropped(delta)
              called with an INCREMENTAL delta each time the sender's named
              drop counters advance -- never one final aggregate, and never
              for an unchanged observation. The counters compared are exactly
              objects_dropped, groups_dropped, keyframes_dropped and
              groups_abandoned; last_error and every other statistic are not
              drop counters and are not differenced. The record's `dropped`
              is the whole-loop aggregate taken from the SAME final snapshot
              as `stats`. A callback that raises propagates: the loop does
              not resubmit and does not clean up owners it does not own.
  require_demand
              opt-in and finite. `has_media_subscriber()` is the service's
              AGGREGATE question -- does ANY app-visible media track have a
              subscriber -- so it is not per-track demand gating, and an
              unrelated subscribed track satisfies it. Readiness is a
              different thing again: the loop makes no readiness query at
              all, because writing pre-ready is legal and bounded by the
              service's own pre-ready caps.

The deadline is a COOPERATIVE submission budget, not preemption. It bounds
how long the loop keeps attempting and clips every wait it asks for; it
cannot cancel a native call already in progress, a caller callback, or the
evaluation of the sequence it was handed. A BLOCK_TIMEOUT sender can
therefore spend its whole configured block timeout inside ONE write, past the
loop's remaining budget; a bounded run wants RETURN_WOULD_BLOCK. Nothing here
redesigns a native timeout or claims the injected clock cancels a write.

Every wait the loop makes is `min(wait_us, deadline_us - now_us())`, and with
no budget left it does not wait at all.

The record (exact keys, and every value pinned):

    stop         "submitted" | "deadline" | "interrupted" | "closed"
    submitted    accepted submissions
    attempts     write calls made
    pending      the plan index not yet accepted, or None once every planned
                 object has been accepted (an end that stops early leaves it
                 None: the writes were done)
    would_block  write refusals OBSERVED. It does not promise each was
                 retried: the last one can be followed by the budget running
                 out, an interrupt or a terminal service.
    ended        tracks whose end_track was ACCEPTED
    ends_refused ((track, code), ...) -- ends the service refused with the
                 named track-local code WRONG_STATE. Any other MoqError from
                 end_track propagates; it is not swallowed into a successful
                 overall result.
    interrupted  set on EVERY interruption path, whichever call observed it
    dropped      the whole-loop drop aggregate, from the final snapshot
    terminal     sender.terminal, read once at the end
    stats        sender.stats(), the final snapshot

`stop == "submitted"` means every planned object was ACCEPTED and every
track's end was REQUESTED -- accepted, or refused with that one named code.
A non-empty `ends_refused` therefore means not every track terminated. It is
not emission, flush, acknowledgment or receipt; it says nothing about
END_OF_TRACK reaching a peer or a catalog being published; and it is not a
queue-empty completion predicate.

Deferred on purpose, and not enlarged by this slice: VOD conversion, CMAF and
richer media, SAP/timeline hints, content protections, push mode, callbacks,
asyncio, a graceful finish or drain, and the receiver-observed runtime
coordination, which stays a separate fixture.
"""

import gc
import importlib.util
import os
import unittest
from enum import IntEnum
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import moq5
from moq5 import _native

import test_foundation as foundation  # noqa: F401

EXAMPLE = Path(os.environ.get(
    "MOQ5_PUBLISH_LOOP_EXAMPLE",
    str(Path(__file__).resolve().parent.parent / "examples" / "publish_loop.py")))

NS = (b"svc", b"demo")

OK = 0
INVAL = -2
CLOSED = -4
WRONG_STATE = -5
WOULD_BLOCK = -8
INTERRUPTED = -13

# The counters that are drop counters. Nothing else in the statistics is
# differenced: last_error is historical data, not a count.
DROP_FIELDS = ("objects_dropped", "groups_dropped", "keyframes_dropped",
               "groups_abandoned")

# Declared payloads: distinct, different lengths, embedded NUL bytes, so a
# swapped, replayed or truncated submission is caught by identity alone.
PAYLOADS = tuple(b"\x00moq5\x00" + bytes([i + 1]) * (i + 1) for i in range(4))

WAIT_US = 250_000


def load_example():
    """The exact shipped file, or a NAMED failure if it is not there."""
    if not EXAMPLE.exists():
        raise AssertionError(
            f"the publisher example is not implemented: {EXAMPLE} is missing")
    source = EXAMPLE.read_text()
    compile(source, str(EXAMPLE), "exec")
    spec = importlib.util.spec_from_file_location("publish_loop", EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    entry = getattr(module, "publish", None)
    if not callable(entry):
        raise AssertionError(
            f"{EXAMPLE} does not define a callable publish(...)")
    return entry


def drop_image(stats):
    """The named drop counters of any stats object, model or real."""
    return {field: getattr(stats, field) for field in DROP_FIELDS}


# ------------------------------------------------------------------ model --

class WriteOutcome(IntEnum):
    ACCEPTED = OK
    WOULD_BLOCK = WOULD_BLOCK
    INTERRUPTED = INTERRUPTED
    CLOSED = CLOSED


class WaitResult(IntEnum):
    WOKEN = 0
    TIMED_OUT = 1
    INTERRUPTED = INTERRUPTED
    CLOSED = CLOSED


class MoqError(RuntimeError):
    def __init__(self, code, operation, detail=""):
        super().__init__(code, operation, detail)
        self.code, self.operation = code, operation


class ModelStats:
    """Slots, like the real SenderStats: a stand-in that reaches for __dict__
    is refused here exactly as it would be against the product."""

    __slots__ = DROP_FIELDS + ("last_error",)

    def __init__(self, counters, last_error=0):
        for field in DROP_FIELDS:
            setattr(self, field, counters[field])
        self.last_error = last_error


class ModelTrack:
    def __init__(self, name):
        self.name = name

    def __repr__(self):                                   # pragma: no cover
        return f"ModelTrack({self.name!r})"


class ModelSender:
    """The DOCUMENTED outcome semantics of the sender surface, as a script.

    It is not a second service: it queues nothing and schedules nothing. Every
    call is recorded in `ops` with its exact arguments -- object IDENTITY
    included -- so ordering, identity and call counts are the oracle, and the
    scripted results decide the branch. `drops_on` advances the named drop
    counters at a declared call ("write" or "wait") so a row can place a
    discard where it wants one.
    """

    def __init__(self, writes=(), ends=(), waits=(), demand=(), drops_on=None,
                 baseline=None, terminal=None):
        self.ops = []
        self.writes = list(writes)
        self.ends = list(ends)
        self.waits = list(waits)
        self.demand = list(demand)
        self.drops_on = dict(drops_on or {})
        self.counters = dict(baseline or {field: 0 for field in DROP_FIELDS})
        self.base = dict(self.counters)
        self.stats_calls = 0
        self.closed = False
        state = terminal or {"closed": False, "fatal": False}
        self.terminal = SimpleNamespace(**state)

    def terminal_image(self):
        return {"closed": self.terminal.closed, "fatal": self.terminal.fatal}

    def _advance(self, when):
        for field, amount in self.drops_on.pop(when, {}).items():
            self.counters[field] += amount

    def write(self, track, obj):
        self.ops.append(("write", track, obj))
        self._advance("write")
        self._advance(f"write{sum(1 for op in self.ops if op[0] == 'write')}")
        outcome = self.writes.pop(0) if self.writes else WriteOutcome.ACCEPTED
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def end_track(self, track):
        self.ops.append(("end_track", track))
        outcome = self.ends.pop(0) if self.ends else WriteOutcome.ACCEPTED
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def wait(self, timeout_us):
        self.ops.append(("wait", timeout_us))
        self._advance("wait")
        result = self.waits.pop(0) if self.waits else WaitResult.WOKEN
        if isinstance(result, BaseException):
            raise result
        return result

    def has_media_subscriber(self):
        self.ops.append(("has_media_subscriber",))
        return self.demand.pop(0) if self.demand else True

    def stats(self):
        self.ops.append(("stats",))
        self.stats_calls += 1
        self._advance(f"stats{self.stats_calls}")
        return ModelStats(self.counters)

    # -- things the loop must never touch ---------------------------------
    def close(self):                                      # pragma: no cover
        raise AssertionError("the loop closed a sender it does not own")

    def request_complete(self):                           # pragma: no cover
        raise AssertionError("the loop requested completion")

    def add_track(self, config):                          # pragma: no cover
        raise AssertionError("the loop added a track")

    def remove_track(self, track):                        # pragma: no cover
        raise AssertionError("the loop removed a track")

    @property
    def ready(self):                                      # pragma: no cover
        raise AssertionError("the loop asked about readiness")

    def kinds(self):
        return tuple(op[0] for op in self.ops)

    def written(self):
        return tuple((op[1], op[2]) for op in self.ops if op[0] == "write")

    def waits_made(self):
        return tuple(op[1] for op in self.ops if op[0] == "wait")

    def ends_made(self):
        return tuple(op[1] for op in self.ops if op[0] == "end_track")


class Clock:
    """A declared clock. Each read returns `now` and advances it by `step_us`;
    a zero step freezes time, which is what the exact-timeout rows need."""

    def __init__(self, step_us=0, start_us=0):
        self.step_us, self.now = step_us, start_us
        self.reads = 0

    def __call__(self):
        self.reads += 1
        value = self.now
        self.now += self.step_us
        return value


def obj(index):
    """One planned submission object, identified by its declared payload."""
    return SimpleNamespace(payload=PAYLOADS[index], index=index)


class PublishLoopModelTests(unittest.TestCase):
    """The loop's control flow, killed deterministically against a model."""

    def publish(self, sender, plan, **kwargs):
        entry = load_example()
        clock = kwargs.pop("clock", None) or Clock()
        deadline = kwargs.pop("deadline_us", 10_000_000)
        dropped = kwargs.pop("dropped", None)
        if dropped is None:
            dropped = []
        callback = kwargs.pop("callback", None) or dropped.append
        self.sender = sender
        return entry(sender, plan, clock, deadline, WriteOutcome, WaitResult,
                     MoqError, callback, **kwargs)

    def plan(self, track, count=len(PAYLOADS)):
        return tuple((track, obj(i)) for i in range(count))

    def assert_record(self, record, **expected):
        """The COMPLETE record: every declared key, every value.

        Values not named by the row take the declared defaults -- an empty
        run's values, which really are invariant. The two snapshots are
        checked against the MODEL's own state, never against the record: the
        terminal image must be the sender's, the statistics must be the final
        observation, and the aggregate must be that SAME observation minus
        the baseline, so an aggregate taken from an earlier snapshot is
        caught whenever the counters moved in between.
        """
        sender = self.sender
        self.assertEqual(sorted(record), sorted(
            ["stop", "submitted", "attempts", "pending", "would_block",
             "ended", "ends_refused", "interrupted", "dropped", "terminal",
             "stats"]), "the record's declared keys changed")
        declared = {"submitted": 0, "attempts": 0, "pending": None,
                    "would_block": 0, "ended": (), "ends_refused": (),
                    "interrupted": False}
        unknown = set(expected) - set(declared) - {"stop", "dropped"}
        self.assertEqual(unknown, set(), f"undeclared expectations {unknown}")
        declared.update(expected)
        for key in ("stop", "submitted", "attempts", "pending", "would_block",
                    "ended", "ends_refused", "interrupted"):
            self.assertEqual(record[key], declared[key],
                             f"record[{key!r}] is {record[key]!r}, declared "
                             f"{declared[key]!r}")
        if "dropped" in expected:
            self.assertEqual(record["dropped"], expected["dropped"],
                             "the drop aggregate is not the declared one")
        self.assertIsNotNone(record["terminal"],
                             "the record dropped the terminal snapshot")
        self.assertEqual(
            {"closed": record["terminal"].closed,
             "fatal": record["terminal"].fatal},
            sender.terminal_image(),
            "the terminal snapshot is not the sender's")
        self.assertEqual(
            drop_image(record["stats"]), dict(sender.counters),
            "the returned statistics are not the final observation")
        self.assertEqual(
            record["dropped"],
            {field: sender.counters[field] - sender.base[field]
             for field in DROP_FIELDS},
            "the aggregate and the returned statistics are not the same "
            "observation")

    # -- submission ------------------------------------------------------
    def test_a_a_finite_plan_is_submitted_once_each_and_then_ended(self):
        track = ModelTrack(b"v")
        sender = ModelSender()
        plan = self.plan(track)
        record = self.publish(sender, plan)
        self.assert_record(record, stop="submitted", submitted=len(plan),
                           attempts=len(plan), pending=None, would_block=0,
                           ended=(track,), ends_refused=(), interrupted=False,
                           dropped={field: 0 for field in DROP_FIELDS})
        submitted = sender.written()
        self.assertEqual(len(submitted), len(plan))
        for (declared_track, declared), (seen_track, seen) in zip(plan,
                                                                  submitted):
            self.assertIs(seen_track, declared_track,
                          "a submission named another track")
            self.assertIs(seen, declared,
                          "a submission carried another object")
        self.assertEqual(sender.ends_made(), (track,),
                         "a track was ended more than once")

    def test_b_would_block_retries_the_same_object_and_waits_between(self):
        track = ModelTrack(b"v")
        sender = ModelSender(writes=[WriteOutcome.WOULD_BLOCK,
                                     WriteOutcome.WOULD_BLOCK,
                                     WriteOutcome.ACCEPTED])
        plan = self.plan(track, 1)
        record = self.publish(sender, plan)
        self.assert_record(record, stop="submitted", submitted=1, attempts=3,
                           would_block=2, pending=None, interrupted=False,
                           ended=(track,))
        attempts = sender.written()
        self.assertEqual(len(attempts), 3, "one write per attempt")
        for attempt in attempts:
            self.assertIs(attempt[1], plan[0][1],
                          "a retry must submit the SAME object, not a copy")
        self.assertEqual(
            [kind for kind in sender.kinds() if kind in ("write", "wait")],
            ["write", "wait", "write", "wait", "write"],
            "a refusal was retried without a wait between the attempts")

    def test_c_an_accepted_object_is_never_submitted_again(self):
        track = ModelTrack(b"v")
        sender = ModelSender(writes=[WriteOutcome.ACCEPTED,
                                     WriteOutcome.WOULD_BLOCK,
                                     WriteOutcome.ACCEPTED])
        plan = self.plan(track, 2)
        self.publish(sender, plan)
        payloads = [written.payload for _, written in sender.written()]
        self.assertEqual(payloads,
                         [PAYLOADS[0], PAYLOADS[1], PAYLOADS[1]],
                         "an accepted object was replayed")

    def test_d_a_refused_write_ends_the_attempt_without_retrying(self):
        for name, outcome, stop, flag in (
                ("interrupted", WriteOutcome.INTERRUPTED, "interrupted", True),
                ("closed", WriteOutcome.CLOSED, "closed", False)):
            with self.subTest(case=name):
                track = ModelTrack(b"v")
                sender = ModelSender(writes=[outcome])
                record = self.publish(sender, self.plan(track, 2))
                self.assertEqual(sender.kinds().count("write"), 1,
                                 "a refused submission was retried")
                self.assertEqual(sender.kinds().count("end_track"), 0,
                                 "a track was ended after a refusal")
                self.assert_record(record, stop=stop, submitted=0, attempts=1,
                                   pending=0, ended=(), ends_refused=(),
                                   interrupted=flag)

    def test_e_an_unexpected_error_is_not_swallowed_or_recovered(self):
        track = ModelTrack(b"v")
        sender = ModelSender(writes=[MoqError(INVAL, "write")])
        with self.assertRaises(MoqError,
                               msg="the loop retried past an error") as caught:
            self.publish(sender, self.plan(track, 2))
        self.assertEqual(caught.exception.code, INVAL)
        self.assertEqual(sender.kinds().count("write"), 1,
                         "the loop retried past an error")
        self.assertNotIn("end_track", sender.kinds(),
                         "the loop used end_track as error recovery")

    def test_f_a_signal_after_acceptance_is_not_retried(self):
        """A KeyboardInterrupt raised on the way out of an accepted write is
        ambiguous to the caller and is NOT permission to resubmit."""
        track = ModelTrack(b"v")
        sender = ModelSender(writes=[KeyboardInterrupt()])
        with self.assertRaises(KeyboardInterrupt):
            self.publish(sender, self.plan(track, 2))
        self.assertEqual(sender.kinds().count("write"), 1,
                         "an ambiguous submission was submitted again")

    # -- the budget ------------------------------------------------------
    def test_g_a_permanently_full_queue_stops_at_the_deadline(self):
        track = ModelTrack(b"v")
        sender = ModelSender(writes=[WriteOutcome.WOULD_BLOCK] * 50,
                             waits=[WaitResult.TIMED_OUT] * 50)
        clock = Clock(step_us=1_000)
        record = self.publish(sender, self.plan(track, 1), clock=clock,
                              deadline_us=10_000)
        self.assertEqual(record["submitted"], 0,
                         "the loop ignored its deadline")
        self.assertLess(sender.kinds().count("write"), 50,
                        "the loop ignored its deadline")
        self.assert_record(record, stop="deadline", submitted=0, pending=0,
                           ended=(), interrupted=False,
                           attempts=sender.kinds().count("write"),
                           would_block=sender.kinds().count("write"))
        self.assertGreater(clock.reads, 0, "the loop never read its clock")
        self.assertNotIn("write", sender.kinds()[-1:],
                         "a submission was attempted after the budget ran out")
        self.assertNotIn("end_track", sender.kinds(),
                         "a track was ended after the budget ran out")

    def test_g2_every_wait_is_clipped_to_the_remaining_budget(self):
        for name, start, deadline, expected in (
                ("a short remainder", 0, 7_000, 7_000),
                ("a full slice", 0, 10_000_000, WAIT_US)):
            with self.subTest(case=name):
                track = ModelTrack(b"v")
                sender = ModelSender(writes=[WriteOutcome.WOULD_BLOCK,
                                             WriteOutcome.ACCEPTED])
                self.publish(sender, self.plan(track, 1),
                             clock=Clock(step_us=0, start_us=start),
                             deadline_us=deadline)
                self.assertEqual(
                    sender.waits_made(), (expected,),
                    "the wait ignored the remaining budget")

    def test_g3_an_exhausted_budget_does_not_wait_at_all(self):
        track = ModelTrack(b"v")
        sender = ModelSender(writes=[WriteOutcome.WOULD_BLOCK])
        record = self.publish(sender, self.plan(track, 1),
                              clock=Clock(step_us=1_000, start_us=0),
                              deadline_us=1_000)
        self.assert_record(record, stop="deadline", submitted=0, pending=0,
                           attempts=1, would_block=1)
        self.assertEqual(sender.waits_made(), (),
                         "the loop waited with no budget left")

    # -- the wait matrix -------------------------------------------------
    def test_h_a_woken_or_timed_out_wait_rechecks_and_retries(self):
        for name, result in (("woken", WaitResult.WOKEN),
                             ("timed out", WaitResult.TIMED_OUT)):
            with self.subTest(case=name):
                track = ModelTrack(b"v")
                sender = ModelSender(writes=[WriteOutcome.WOULD_BLOCK,
                                             WriteOutcome.ACCEPTED],
                                     waits=[result])
                record = self.publish(sender, self.plan(track, 1))
                self.assert_record(record, stop="submitted", submitted=1,
                                   attempts=2, would_block=1,
                                   ended=(track,))

    def test_h2_an_interrupted_or_closed_wait_stops_the_loop(self):
        for name, result, stop, flag in (
                ("interrupted", WaitResult.INTERRUPTED, "interrupted", True),
                ("closed", WaitResult.CLOSED, "closed", False)):
            with self.subTest(case=name):
                track = ModelTrack(b"v")
                sender = ModelSender(writes=[WriteOutcome.WOULD_BLOCK],
                                     waits=[result])
                record = self.publish(sender, self.plan(track, 1))
                self.assertEqual(sender.kinds().count("write"), 1,
                                 "a write followed a terminal wait")
                self.assertEqual(sender.kinds().count("end_track"), 0,
                                 "an end followed a terminal wait")
                self.assert_record(record, stop=stop, submitted=0, pending=0,
                                   attempts=1, would_block=1, ended=(),
                                   interrupted=flag)

    def test_h3_a_raising_wait_propagates_without_recovery(self):
        track = ModelTrack(b"v")
        sender = ModelSender(writes=[WriteOutcome.WOULD_BLOCK],
                             waits=[MoqError(INVAL, "wait")])
        with self.assertRaises(MoqError):
            self.publish(sender, self.plan(track, 1))
        self.assertEqual(sender.kinds().count("write"), 1,
                         "the loop wrote again after a failed wait")

    # -- demand and readiness --------------------------------------------
    def test_i_an_absent_subscriber_is_bounded_not_awaited_forever(self):
        track = ModelTrack(b"v")
        sender = ModelSender(demand=[False] * 50,
                             waits=[WaitResult.TIMED_OUT] * 50)
        record = self.publish(sender, self.plan(track, 1),
                              clock=Clock(step_us=1_000), deadline_us=5_000,
                              require_demand=True)
        self.assert_record(record, stop="deadline", submitted=0, pending=0,
                           attempts=0)
        self.assertEqual(sender.kinds().count("write"), 0,
                         "the loop submitted without the demand it required")

    def test_i2_an_interrupted_or_closed_demand_wait_stops(self):
        for name, result, stop, flag in (
                ("interrupted", WaitResult.INTERRUPTED, "interrupted", True),
                ("closed", WaitResult.CLOSED, "closed", False)):
            with self.subTest(case=name):
                track = ModelTrack(b"v")
                sender = ModelSender(demand=[False], waits=[result])
                record = self.publish(sender, self.plan(track, 1),
                                      require_demand=True)
                self.assert_record(record, stop=stop, submitted=0, attempts=0,
                                   pending=0, interrupted=flag)
                self.assertEqual(sender.kinds().count("write"), 0,
                                 "a write followed a terminal demand wait")

    def test_i2b_a_woken_or_timed_out_demand_wait_rechecks_demand(self):
        for name, result in (("woken", WaitResult.WOKEN),
                             ("timed out", WaitResult.TIMED_OUT)):
            with self.subTest(case=name):
                track = ModelTrack(b"v")
                sender = ModelSender(demand=[False, True], waits=[result])
                record = self.publish(sender, self.plan(track, 1),
                                      require_demand=True)
                self.assert_record(record, stop="submitted", submitted=1,
                                   attempts=1, ended=(track,))
                self.assertEqual(sender.kinds().count("has_media_subscriber"),
                                 2, "the loop did not recheck demand")

    def test_i2c_a_raising_demand_wait_propagates(self):
        track = ModelTrack(b"v")
        sender = ModelSender(demand=[False], waits=[MoqError(INVAL, "wait")])
        with self.assertRaises(MoqError):
            self.publish(sender, self.plan(track, 1), require_demand=True)
        self.assertEqual(sender.kinds().count("write"), 0,
                         "the loop submitted after a failed demand wait")

    def test_i3_demand_is_not_consulted_by_default(self):
        track = ModelTrack(b"v")
        sender = ModelSender(demand=[False] * 10)
        record = self.publish(sender, self.plan(track, 1))
        self.assert_record(record, stop="submitted", submitted=1, attempts=1,
                           ended=(track,))
        self.assertNotIn("has_media_subscriber", sender.kinds(),
                         "the loop asked about demand it was not told to want")

    def test_i4_readiness_is_never_queried(self):
        """Writing pre-ready is legal and bounded by the service's own
        pre-ready caps, so the loop has no readiness precondition and makes no
        readiness query: the model refuses one by name."""
        track = ModelTrack(b"v")
        sender = ModelSender()
        self.publish(sender, self.plan(track, 1))
        self.assertNotIn("ready", sender.kinds())

    # -- drops -----------------------------------------------------------
    def test_j_drops_are_reported_incrementally_from_a_nonzero_baseline(self):
        track = ModelTrack(b"v")
        baseline = {"objects_dropped": 5, "groups_dropped": 2,
                    "keyframes_dropped": 1, "groups_abandoned": 0}
        sender = ModelSender(
            writes=[WriteOutcome.ACCEPTED, WriteOutcome.WOULD_BLOCK,
                    WriteOutcome.ACCEPTED],
            baseline=baseline,
            drops_on={"write1": {"objects_dropped": 2, "keyframes_dropped": 1},
                      "wait": {"objects_dropped": 3, "groups_dropped": 1}})
        seen = []
        record = self.publish(sender, self.plan(track, 2), dropped=seen)
        self.assertEqual(
            seen,
            [{"objects_dropped": 2, "groups_dropped": 0,
              "keyframes_dropped": 1, "groups_abandoned": 0},
             {"objects_dropped": 3, "groups_dropped": 1,
              "keyframes_dropped": 0, "groups_abandoned": 0}],
            "the drop deltas were not reported incrementally and exactly")
        self.assert_record(
            record, stop="submitted", submitted=2, attempts=3, would_block=1,
            ended=(track,),
            dropped={"objects_dropped": 5, "groups_dropped": 1,
                     "keyframes_dropped": 1, "groups_abandoned": 0})
        self.assertEqual(record["stats"].objects_dropped,
                         baseline["objects_dropped"] + 5,
                         "the record's stats are not the final snapshot")

    def test_j2_a_drop_during_a_successful_write_is_reported_without_waits(self):
        track = ModelTrack(b"v")
        sender = ModelSender(drops_on={"write": {"objects_dropped": 1}})
        seen = []
        record = self.publish(sender, self.plan(track, 1), dropped=seen)
        self.assertEqual(sender.kinds().count("wait"), 0,
                         "this row must not need a wait")
        self.assertEqual(
            seen, [{"objects_dropped": 1, "groups_dropped": 0,
                    "keyframes_dropped": 0, "groups_abandoned": 0}],
            "a discard during an accepted write was not reported")
        self.assertEqual(record["dropped"]["objects_dropped"], 1,
                         "accepted writes were reported as lossless")

    def test_j3_a_raising_drop_callback_propagates(self):
        track = ModelTrack(b"v")
        sender = ModelSender(drops_on={"write": {"objects_dropped": 1}})

        def boom(delta):
            raise ValueError("the caller's drop callback failed")

        with self.assertRaisesRegex(
                ValueError, "drop callback failed",
                msg="the drop deltas were not reported to the caller"):
            self.publish(sender, self.plan(track, 2), callback=boom)
        self.assertEqual(sender.kinds().count("write"), 1,
                         "the loop resubmitted after a failing callback")
        self.assertIs(sender.closed, False,
                      "the loop cleaned up an owner it does not own")

    def test_j4_an_increment_during_an_end_wait_is_observed(self):
        track = ModelTrack(b"v")
        sender = ModelSender(ends=[WriteOutcome.WOULD_BLOCK,
                                   WriteOutcome.ACCEPTED],
                             drops_on={"wait": {"objects_dropped": 3}})
        seen = []
        record = self.publish(sender, self.plan(track, 1), dropped=seen)
        self.assertEqual(
            seen, [{"objects_dropped": 3, "groups_dropped": 0,
                    "keyframes_dropped": 0, "groups_abandoned": 0}],
            "a discard observed during the end phase was not reported")
        self.assert_record(record, stop="submitted", submitted=1, attempts=1,
                           ended=(track,),
                           dropped={"objects_dropped": 3, "groups_dropped": 0,
                                    "keyframes_dropped": 0,
                                    "groups_abandoned": 0})

    def test_j5_an_increment_first_seen_in_the_final_observation_is_reported(self):
        """The promise is nonzero increments BETWEEN OBSERVATIONS, so the
        remainder the final observation discovers is reported once -- and
        only once -- and that same observation is what the record carries."""
        track = ModelTrack(b"v")
        sender = ModelSender(drops_on={"stats3": {"objects_dropped": 2,
                                                  "groups_dropped": 1}})
        seen = []
        record = self.publish(sender, self.plan(track, 1), dropped=seen)
        self.assertEqual(
            seen, [{"objects_dropped": 2, "groups_dropped": 1,
                    "keyframes_dropped": 0, "groups_abandoned": 0}],
            "the final remainder was not reported exactly once")
        self.assert_record(record, stop="submitted", submitted=1, attempts=1,
                           ended=(track,),
                           dropped={"objects_dropped": 2, "groups_dropped": 1,
                                    "keyframes_dropped": 0,
                                    "groups_abandoned": 0})

    # -- ending ----------------------------------------------------------
    def test_k_a_named_track_local_refusal_leaves_siblings_independent(self):
        one, two = ModelTrack(b"one"), ModelTrack(b"two")
        sender = ModelSender(ends=[MoqError(WRONG_STATE, "end_track"),
                                   WriteOutcome.ACCEPTED])
        plan = ((one, obj(0)), (two, obj(1)))
        record = self.publish(sender, plan)
        self.assert_record(record, stop="submitted", submitted=2, attempts=2,
                           ended=(two,), ends_refused=((one, WRONG_STATE),),
                           interrupted=False)
        self.assertEqual(sender.ends_made(), (one, two),
                         "each track is ended once, in plan order")

    def test_k2_an_unnamed_end_error_is_not_swallowed(self):
        one, two = ModelTrack(b"one"), ModelTrack(b"two")
        sender = ModelSender(ends=[MoqError(INVAL, "end_track")])
        with self.assertRaises(
                MoqError,
                msg="an unnamed end error was swallowed") as caught:
            self.publish(sender, ((one, obj(0)), (two, obj(1))))
        self.assertEqual(caught.exception.code, INVAL,
                         "an unnamed end error was swallowed")
        self.assertEqual(sender.ends_made(), (one,),
                         "the loop ended a sibling after an unnamed error")

    def test_k3_a_stopping_end_stops_the_siblings_too(self):
        for name, outcome, stop, flag in (
                ("interrupted", WriteOutcome.INTERRUPTED, "interrupted", True),
                ("closed", WriteOutcome.CLOSED, "closed", False)):
            with self.subTest(case=name):
                one, two = ModelTrack(b"one"), ModelTrack(b"two")
                sender = ModelSender(ends=[outcome])
                record = self.publish(sender, ((one, obj(0)), (two, obj(1))))
                self.assertEqual(sender.ends_made(), (one,),
                                 "an end ran after the loop stopped")
                self.assert_record(record, stop=stop, submitted=2,
                                   attempts=2, pending=None, ended=(),
                                   ends_refused=(), interrupted=flag)

    def test_l_a_blocked_end_is_retried_within_the_budget(self):
        track = ModelTrack(b"v")
        sender = ModelSender(ends=[WriteOutcome.WOULD_BLOCK,
                                   WriteOutcome.ACCEPTED])
        record = self.publish(sender, self.plan(track, 1),
                              clock=Clock(step_us=0), deadline_us=9_000)
        self.assert_record(record, stop="submitted", submitted=1, attempts=1,
                           ended=(track,))
        self.assertEqual(sender.ends_made(), (track, track))
        self.assertEqual(sender.waits_made(), (9_000,),
                         "the end's wait ignored the remaining budget")

    def test_l2_a_blocked_end_stops_at_the_deadline(self):
        track = ModelTrack(b"v")
        sender = ModelSender(ends=[WriteOutcome.WOULD_BLOCK] * 20,
                             waits=[WaitResult.TIMED_OUT] * 20)
        record = self.publish(sender, self.plan(track, 1),
                              clock=Clock(step_us=1_000), deadline_us=6_000)
        self.assert_record(record, stop="deadline", submitted=1, attempts=1,
                           pending=None, ended=(), ends_refused=())
        self.assertLess(sender.kinds().count("end_track"), 20,
                        "the loop ignored its deadline")

    def test_l3_a_terminal_wait_inside_the_end_phase_stops_everything(self):
        """The END phase's wait is its own branch: a blocked end that waits
        into an interrupt or a terminal service must stop there, without
        retrying that end and without ending the sibling behind it."""
        for name, result, stop, flag in (
                ("interrupted", WaitResult.INTERRUPTED, "interrupted", True),
                ("closed", WaitResult.CLOSED, "closed", False)):
            with self.subTest(case=name):
                one, two = ModelTrack(b"one"), ModelTrack(b"two")
                sender = ModelSender(ends=[WriteOutcome.WOULD_BLOCK,
                                           WriteOutcome.ACCEPTED],
                                     waits=[result])
                record = self.publish(sender, ((one, obj(0)), (two, obj(1))))
                self.assertEqual(
                    sender.ends_made(), (one,),
                    "an end followed a terminal wait in the end phase")
                self.assertEqual(sender.waits_made(), (WAIT_US,),
                                 "the end phase waited more than once")
                self.assert_record(record, stop=stop, submitted=2,
                                   attempts=2, pending=None, ended=(),
                                   ends_refused=(), interrupted=flag)

    def test_l4_a_raising_wait_inside_the_end_phase_propagates(self):
        one, two = ModelTrack(b"one"), ModelTrack(b"two")
        sender = ModelSender(ends=[WriteOutcome.WOULD_BLOCK],
                             waits=[MoqError(INVAL, "wait")])
        with self.assertRaises(MoqError) as caught:
            self.publish(sender, ((one, obj(0)), (two, obj(1))))
        self.assertEqual(caught.exception.code, INVAL)
        self.assertEqual(sender.ends_made(), (one,),
                         "an end followed a failed wait in the end phase")

    # -- shape -----------------------------------------------------------
    def test_m_the_loop_owns_nothing_and_completes_nothing(self):
        track = ModelTrack(b"v")
        sender = ModelSender()
        self.publish(sender, self.plan(track))
        allowed = {"write", "end_track", "wait", "stats",
                   "has_media_subscriber"}
        self.assertTrue(set(sender.kinds()) <= allowed,
                        f"the loop used {set(sender.kinds()) - allowed}")
        self.assertIs(sender.closed, False)

    def test_n_an_empty_plan_submits_nothing_and_ends_nothing(self):
        sender = ModelSender()
        record = self.publish(sender, ())
        self.assert_record(record, stop="submitted", submitted=0, attempts=0,
                           pending=None, ended=(), ends_refused=(),
                           would_block=0, interrupted=False)
        self.assertEqual(sender.kinds().count("write"), 0)
        self.assertEqual(sender.kinds().count("end_track"), 0)


# ------------------------------------------------------- real integration --

class WriteObserver:
    """A TRANSPARENT observer over the real Sender.

    It forwards every call unchanged and captures the fixture's own image of
    that call immediately afterwards, before the next call overwrites the
    recorder. It adds no production seam: everything it reads is an existing
    test hook.
    """

    def __init__(self, sender):
        self._sender = sender
        self.calls = []

    def write(self, track, obj):
        outcome = self._sender.write(track, obj)
        self.calls.append(("write", int(outcome),
                           _native._test_write_object()))
        return outcome

    def end_track(self, track):
        before = _native._test_end_counts()["entries"]
        try:
            outcome = self._sender.end_track(track)
        except BaseException:
            self.calls.append(("end_track", None, _native._test_end_target(),
                               _native._test_end_counts()["entries"] - before))
            raise
        self.calls.append(("end_track", int(outcome),
                           _native._test_end_target(),
                           _native._test_end_counts()["entries"] - before))
        return outcome

    def wait(self, timeout_us):
        self.calls.append(("wait", timeout_us))
        return self._sender.wait(timeout_us)

    def has_media_subscriber(self):
        answer = self._sender.has_media_subscriber()
        self.calls.append(("has_media_subscriber", answer))
        return answer

    def stats(self):
        return self._sender.stats()

    @property
    def terminal(self):
        return self._sender.terminal

    @property
    def closed(self):
        return self._sender.closed

    def kinds(self):
        return tuple(call[0] for call in self.calls)

    def writes(self):
        return tuple(call for call in self.calls if call[0] == "write")

    def ends(self):
        return tuple(call for call in self.calls if call[0] == "end_track")


class PublishLoopIntegrationTests(unittest.TestCase):
    """The same example against the ACTUAL public Sender over the fixture
    provider. The model rows stay the model; these prove that every call
    reaches the real binding with the exact declared identity and bytes."""

    def setUp(self):
        gc.collect()
        # every fixture family this file actually uses, independently of
        # whatever ran before
        _native._test_reset()
        _native._test_sender_reset()
        _native._test_send_track_reset()
        _native._test_write_reset()
        _native._test_end_reset()
        _native._test_demand_reset()
        _native._test_sender_stats_reset()
        _native._test_sender_wait_reset()
        _native._test_sender_wait_level(True)

    def tearDown(self):
        _native._test_write_result(0)
        _native._test_end_result(0)
        _native._test_write_reset()
        _native._test_end_reset()
        _native._test_demand_reset()
        _native._test_sender_stats_reset()
        _native._test_sender_wait_reset()

    def live(self, names=(b"v",)):
        endpoint = moq5.Endpoint.connect(
            moq5.EndpointConfig(url="moqt://fixture.invalid"))
        sender = moq5.Sender.attach(
            endpoint,
            moq5.SenderConfig(
                namespace=NS,
                backpressure=moq5.Backpressure.RETURN_WOULD_BLOCK))
        self.addCleanup(endpoint.close)
        self.addCleanup(sender.close)
        tracks = tuple(
            sender.add_track(moq5.SendTrackConfig(
                name=name, media_type=moq5.MediaType.VIDEO,
                packaging=moq5.Packaging.RAW, codec=b"av01",
                bitrate=1_500_000))
            for name in names)
        return endpoint, sender, tracks

    def run_loop(self, sender, plan, **kwargs):
        entry = load_example()
        clock = kwargs.pop("clock", None) or Clock()
        deadline = kwargs.pop("deadline_us", 10_000_000)
        dropped = kwargs.pop("dropped", None)
        if dropped is None:
            dropped = []
        return entry(sender, plan, clock, deadline, moq5.WriteOutcome,
                     moq5.WaitResult, moq5.MoqError, dropped.append, **kwargs)

    def submission(self, index):
        return moq5.SendObject(payload=PAYLOADS[index], is_sync=(index == 0),
                               starts_group=(index == 0),
                               presentation_time_us=index * 1_000)

    def declared(self, tracks):
        """Interleaved across two tracks, so a same-count wrong selection
        cannot pass: v, a, v, a."""
        return tuple((tracks[i % len(tracks)], self.submission(i))
                     for i in range(len(PAYLOADS)))

    def assert_inventory(self, observer, plan, tracks):
        """EVERY call's native image, in order, against the declared plan."""
        writes = observer.writes()
        self.assertEqual(len(writes), len(plan),
                         "the number of native writes is not the plan's")
        for position, ((track, submitted), (_kind, outcome, image)) in \
                enumerate(zip(plan, writes)):
            index = tracks.index(track)
            self.assertEqual(
                image["payload"], submitted.payload,
                f"submission {position} carried other bytes to the service")
            self.assertEqual(
                image["payload_declared_length"], len(submitted.payload),
                f"submission {position} declared another length")
            self.assertEqual(
                image["track_index"], index,
                f"submission {position} named another track")
            self.assertEqual(image["is_sync"], submitted.is_sync,
                             f"submission {position} lost its sync flag")
            self.assertEqual(image["starts_group"], submitted.starts_group,
                             f"submission {position} lost its group start")
            self.assertEqual(image["presentation_time_us"],
                             submitted.presentation_time_us,
                             f"submission {position} lost its timestamp")
            self.assertEqual(outcome, OK,
                             f"submission {position} was not accepted")

    def test_a_every_declared_submission_reaches_the_service_in_order(self):
        endpoint, sender, tracks = self.live((b"v", b"a"))
        observer = WriteObserver(sender)
        plan = self.declared(tracks)
        record = self.run_loop(observer, plan)
        self.assertEqual(record["stop"], "submitted")
        self.assert_inventory(observer, plan, tracks)
        ends = observer.ends()
        self.assertEqual(len(ends), len(tracks),
                         "each track is ended exactly once")
        for (_kind, outcome, target, entries), track in zip(ends, tracks):
            self.assertEqual(target["track_index"], tracks.index(track),
                             "the end named another track")
            self.assertEqual(entries, 1, "one native end call per request")
            self.assertEqual(outcome, OK)
        self.assertEqual(observer.kinds()[-len(tracks):],
                         ("end_track",) * len(tracks),
                         "an end was requested before the plan finished")

    def test_b_every_retry_is_observed_not_only_the_last(self):
        endpoint, sender, tracks = self.live()
        observer = WriteObserver(sender)
        plan = ((tracks[0], self.submission(0)),)
        _native._test_write_result(WOULD_BLOCK)
        record = self.run_loop(observer, plan, clock=Clock(step_us=1_000),
                               deadline_us=5_000)
        self.assertEqual(record["stop"], "deadline")
        self.assertEqual(record["submitted"], 0)
        writes = observer.writes()
        self.assertGreater(len(writes), 1, "the loop never retried")
        for position, (_kind, outcome, image) in enumerate(writes):
            self.assertEqual(image["payload"], PAYLOADS[0],
                             f"retry {position} submitted different bytes")
            self.assertEqual(image["track_index"], 0,
                             f"retry {position} named another track")
            self.assertEqual(outcome, WOULD_BLOCK)
        self.assertEqual(_native._test_write_counts()["accepted"], 0)

    def test_c_a_terminal_service_ends_the_attempt(self):
        endpoint, sender, tracks = self.live()
        observer = WriteObserver(sender)
        _native._test_write_result(CLOSED)
        record = self.run_loop(observer, ((tracks[0], self.submission(0)),))
        self.assertEqual(record["stop"], "closed")
        self.assertEqual(_native._test_write_counts()["entries"], 1,
                         "a closed service was written to twice")
        self.assertEqual(_native._test_end_counts()["entries"], 0,
                         "a track was ended after a terminal write")

    def test_d_the_loop_neither_closes_nor_completes_the_sender(self):
        endpoint, sender, tracks = self.live()
        observer = WriteObserver(sender)
        before = _native._test_sender_counts()["destroy_entries"]
        self.run_loop(observer, ((tracks[0], self.submission(0)),))
        self.assertIs(sender.closed, False, "the loop closed the sender")
        self.assertIs(tracks[0].removed, False,
                      "the loop removed or completed the track")
        self.assertEqual(_native._test_sender_counts()["destroy_entries"],
                         before, "the loop destroyed the sender")
        self.assertNotIn("complete", _native._test_sender_log(),
                         "the loop requested completion")

    def test_e_demand_is_the_services_aggregate_not_per_track_gating(self):
        """`has_media_subscriber()` asks whether ANY app-visible media track
        has a subscriber. An unrelated subscribed track satisfies it while the
        planned track has none: that is the documented boundary, not a
        per-track gate."""
        endpoint, sender, tracks = self.live((b"v", b"other"))
        _native._test_demand_set(1, 3)            # the OTHER track only
        self.assertEqual(sender.subscriptions(tracks[0]), 0,
                         "the planned track must have no subscriber here")
        self.assertIs(sender.has_media_subscriber(), True)
        observer = WriteObserver(sender)
        plan = ((tracks[0], self.submission(0)),)
        record = self.run_loop(observer, plan, require_demand=True)
        self.assertEqual(record["stop"], "submitted",
                         "the aggregate answer did not satisfy the gate")
        self.assertEqual(len(observer.writes()), 1)

    def test_f_drops_observed_through_the_real_statistics(self):
        endpoint, sender, tracks = self.live()
        _native._test_sender_stats(0, 0, 0, 0, 4, 1, 2, 0, 0, 0, 0)
        observer = WriteObserver(sender)
        seen = []
        record = self.run_loop(observer, ((tracks[0], self.submission(0)),),
                               dropped=seen)
        self.assertEqual(record["stop"], "submitted")
        self.assertEqual(record["dropped"],
                         {field: 0 for field in DROP_FIELDS},
                         "a static counter was reported as a new discard")
        self.assertEqual(seen, [], "an unchanged counter was reported")
        self.assertEqual(record["stats"].objects_dropped, 4,
                         "the record's stats are not the service's snapshot")


# ----------------------------------------------------------------- controls --

class PublishLoopControlTests(unittest.TestCase):
    """This file's own machinery. Never evidence about the product.

    The absence control is the RED attribution. The stand-in controls prove
    the rows have discriminating power: every row -- model AND integration --
    passes against a stand-in that follows the declared contract, and each
    deliberately wrong stand-in is caught by a NAMED assertion.
    """

    ROW_CLASSES = ("PublishLoopModelTests", "PublishLoopIntegrationTests")

    def test_a_the_example_is_present_with_the_declared_signature(self):
        # This row was the RED gate's "the example is absent" assertion. The
        # file exists now, so it pins presence, callability, the exact
        # parameter list and defaults, and that no test scaffolding came with
        # it.
        import inspect

        self.assertTrue(EXAMPLE.exists(), f"{EXAMPLE} is missing")
        entry = load_example()
        signature = inspect.signature(entry)
        self.assertEqual(
            list(signature.parameters),
            ["sender", "plan", "now_us", "deadline_us", "WriteOutcome",
             "WaitResult", "MoqError", "note_dropped", "wait_us",
             "require_demand"],
            f"unexpected publish signature: {signature}")
        self.assertEqual(signature.parameters["wait_us"].default, WAIT_US)
        self.assertIs(signature.parameters["require_demand"].default, False)
        for absent in ("fault", "budget", "_test", "mock"):
            self.assertNotIn(absent, list(signature.parameters),
                             f"the example carries a {absent} parameter")
        source = EXAMPLE.read_text()
        for scaffold in ("fault", "_native", "import test_", "unittest"):
            self.assertNotIn(scaffold, source,
                             f"the example carries {scaffold!r}")

    def run_rows(self, entry, classes=None, reverse=False):
        """Every row of the named classes, against a supplied implementation."""
        import test_publish_loop as module

        loader = unittest.defaultTestLoader
        names = []
        for name in (classes or self.ROW_CLASSES):
            case_class = getattr(module, name)
            for test in loader.getTestCaseNames(case_class):
                names.append(f"{name}.{test}")
        if reverse:
            names.reverse()
        suite = loader.loadTestsFromNames(names, module)
        result = unittest.TestResult()
        with mock.patch.object(module, "load_example", lambda: entry):
            suite.run(result)
        return result

    def test_b_every_row_passes_against_a_contract_stand_in(self):
        result = self.run_rows(reference_publish)
        self.assertEqual(
            [f"{case.id()}: {error}" for case, error in
             result.failures + result.errors], [],
            "a row failed against the declared contract")
        self.assertGreater(result.testsRun, 0)

    def test_b2_the_rows_do_not_depend_on_their_order(self):
        """Reversed, the same rows still pass: each sets up the fixture
        families it uses and leaves them settled."""
        result = self.run_rows(reference_publish, reverse=True)
        self.assertEqual(
            [f"{case.id()}: {error}" for case, error in
             result.failures + result.errors], [],
            "a row depended on an earlier row's fixture state")

    def test_b3_the_integration_rows_pass_on_their_own(self):
        result = self.run_rows(reference_publish,
                               classes=("PublishLoopIntegrationTests",))
        self.assertEqual(
            [f"{case.id()}: {error}" for case, error in
             result.failures + result.errors], [],
            "an integration row needed the model rows to run first")
        self.assertGreater(result.testsRun, 0)

    # Faults that deliberately ignore a stopping condition are bounded only by
    # a finite script, so they run against the MODEL rows, whose sender is
    # scripted. Against the real fixture -- which answers WOULD_BLOCK or
    # CLOSED for as long as it is told to -- such a stand-in would not
    # terminate, which is the defect they exist to represent.
    MODEL_ONLY_FAULTS = ("ignore_deadline", "wait_without_budget",
                         "ignore_closed_wait", "no_wait_between_refusals",
                         "retry_refusal")

    def test_c_each_wrong_stand_in_is_caught_by_a_named_row(self):
        for fault, expected in (
                ("replay", "an accepted object was replayed"),
                ("copy", "a retry must submit the SAME object"),
                ("retry_refusal", "a refused submission was retried"),
                ("ignore_deadline", "the loop ignored its deadline"),
                ("unclipped", "the wait ignored the remaining budget"),
                ("wait_without_budget", "the loop waited with no budget left"),
                ("ignore_closed_wait", "a write followed a terminal wait"),
                ("swallow", "the loop retried past an error"),
                ("swallow_end_errors", "an unnamed end error was swallowed"),
                ("ends_after_stop", "an end ran after the loop stopped"),
                ("complete", "the loop requested completion"),
                ("hide_drops", "the drop deltas were not reported"),
                ("aggregate_drops", "reported incrementally"),
                ("end_twice", "a track was ended more than once"),
                ("demand_always", "the loop asked about demand"),
                ("no_wait_between_refusals", "without a wait between"),
                ("reorder", "carried other bytes to the service"),
                ("moved_end", "an end was requested before the plan"),
                ("forge_terminal", "the record dropped the terminal snapshot"),
                ("previous_snapshot", "the drop aggregate is not the declared one"),
                ("no_end_wait_terminal",
                 "an end followed a terminal wait in the end phase"),
                ("no_final_report",
                 "the final remainder was not reported exactly once"),
                ("duplicate_final_report",
                 "the final remainder was not reported exactly once"),
                ("wrong_track_end", "the end named another track"),
        ):
            with self.subTest(fault=fault):
                classes = ("PublishLoopModelTests",) \
                    if fault in self.MODEL_ONLY_FAULTS else None
                result = self.run_rows(make_faulty_publish(fault),
                                       classes=classes)
                messages = "\n".join(
                    str(error) for _, error in
                    result.failures + result.errors)
                self.assertTrue(result.failures or result.errors,
                                f"the {fault} stand-in satisfied every row")
                self.assertIn(expected, messages,
                              f"the {fault} stand-in was not caught by its "
                              "own named assertion")


# The contract stand-in and its faults live at the bottom: they exist ONLY to
# qualify the rows above, and are never the shipped example.

def reference_publish(sender, plan, now_us, deadline_us, WriteOutcomeT,
                      WaitResultT, MoqErrorT, note_dropped, wait_us=WAIT_US,
                      require_demand=False, fault=None):
    submissions = list(plan)
    if fault == "reorder" and len(submissions) > 1:
        submissions[0], submissions[1] = submissions[1], submissions[0]
    order = []
    for track, _submitted in submissions:
        if track not in order:
            order.append(track)
    base = drop_image(sender.stats())
    seen = dict(base)
    submitted = attempts = would_block = 0
    index = 0
    interrupted = False
    stop = None
    ended, refused = [], []

    budget = [2_000] if fault else [None]      # harness bound for faults only

    def spend():
        if budget[0] is None:
            return
        budget[0] -= 1
        if budget[0] <= 0:
            raise AssertionError("the fault harness bound expired")

    def remaining():
        spend()
        return deadline_us - now_us()

    def note():
        now = drop_image(sender.stats())
        delta = {field: now[field] - seen[field] for field in DROP_FIELDS}
        if any(delta.values()) and fault != "hide_drops":
            seen.update(now)
            if fault != "aggregate_drops":
                note_dropped(delta)

    def bounded_wait():
        """The wait result, or None when there is no budget left to wait in."""
        left = remaining()
        if left <= 0 and fault not in ("wait_without_budget",
                                       "ignore_deadline"):
            return None
        timeout = wait_us if fault == "unclipped" else min(wait_us,
                                                           max(left, 0))
        return sender.wait(timeout)

    def record(reason):
        # ONE final observation carries the returned statistics, the
        # aggregate, and the remainder no earlier observation had seen.
        previous = dict(seen)                  # the snapshot before this one
        stats = sender.stats()
        final = drop_image(stats)
        remainder = {field: final[field] - seen[field] for field in DROP_FIELDS}
        if any(remainder.values()) and fault not in ("hide_drops",
                                                     "no_final_report"):
            seen.update(final)
            if fault != "aggregate_drops":
                note_dropped(remainder)
        aggregate = {field: final[field] - base[field] for field in DROP_FIELDS}
        if fault == "hide_drops":
            aggregate = {field: 0 for field in DROP_FIELDS}
        if fault == "previous_snapshot":
            # the aggregate from the snapshot BEFORE this final one, with the
            # returned statistics and the callback left correct
            aggregate = {field: previous[field] - base[field]
                         for field in DROP_FIELDS}
        if fault == "aggregate_drops" and any(aggregate.values()):
            note_dropped(dict(aggregate))
        if fault == "duplicate_final_report" and any(remainder.values()):
            note_dropped(dict(remainder))
        return {"stop": reason, "submitted": submitted, "attempts": attempts,
                "pending": None if index >= len(submissions) else index,
                "would_block": would_block, "ended": tuple(ended),
                "ends_refused": tuple(refused), "interrupted": interrupted,
                "dropped": aggregate,
                "terminal": None if fault == "forge_terminal"
                else sender.terminal,
                "stats": stats}

    if require_demand:
        while not sender.has_media_subscriber():
            result = bounded_wait()
            if result is None:
                return record("deadline")
            note()
            if result is WaitResultT.INTERRUPTED:
                interrupted = True
                return record("interrupted")
            if result is WaitResultT.CLOSED:
                return record("closed")
    elif fault == "demand_always":
        sender.has_media_subscriber()

    moved_early = None
    while index < len(submissions):
        if fault == "moved_end" and len(order) > 1 \
                and index == len(submissions) - 1 and moved_early is None:
            # the SAME end, at the wrong position: counts and identities are
            # untouched, so only an ordering oracle can catch it
            moved_early = order[0]
            try:
                if sender.end_track(moved_early) is WriteOutcomeT.ACCEPTED:
                    ended.append(moved_early)
            except MoqErrorT:
                moved_early = None             # nothing moved after all
        if remaining() <= 0 and fault != "ignore_deadline":
            stop = "deadline"
            break
        track, submission = submissions[index]
        if fault == "copy":
            submission = SimpleNamespace(**submission.__dict__)
        attempts += 1
        outcome = sender.write(track, submission)
        note()
        if outcome is WriteOutcomeT.ACCEPTED:
            submitted += 1
            if fault != "replay" or submitted > len(submissions):
                index += 1
            continue
        if outcome is WriteOutcomeT.WOULD_BLOCK:
            would_block += 1
            if fault == "no_wait_between_refusals":
                continue
            result = bounded_wait()
            if result is None:
                stop = "deadline"
                break
            note()
            if result is WaitResultT.INTERRUPTED:
                interrupted = True
                stop = "interrupted"
                break
            if result is WaitResultT.CLOSED and fault != "ignore_closed_wait":
                stop = "closed"
                break
            continue
        if outcome is WriteOutcomeT.INTERRUPTED:
            interrupted = True
            stop = "interrupted"
            if fault == "retry_refusal":
                continue
            break
        stop = "closed"
        if fault == "retry_refusal":
            continue
        break

    if stop is None:
        for track in order:
            if fault == "moved_end" and track is moved_early:
                continue                       # already ended, out of order
            if fault == "wrong_track_end":
                track = order[-1]
            while True:
                if remaining() <= 0:
                    stop = "deadline"
                    break
                try:
                    outcome = sender.end_track(track)
                except MoqErrorT as error:
                    if error.code == WRONG_STATE or \
                            fault == "swallow_end_errors":
                        refused.append((track, error.code))
                        break
                    raise
                if outcome is WriteOutcomeT.ACCEPTED:
                    ended.append(track)
                    if fault == "end_twice":
                        sender.end_track(track)
                    break
                if outcome is WriteOutcomeT.WOULD_BLOCK:
                    result = bounded_wait()
                    if result is None:
                        stop = "deadline"
                        break
                    note()
                    if fault != "no_end_wait_terminal":
                        if result is WaitResultT.INTERRUPTED:
                            interrupted = True
                            stop = "interrupted"
                            break
                        if result is WaitResultT.CLOSED:
                            stop = "closed"
                            break
                    continue
                if outcome is WriteOutcomeT.INTERRUPTED:
                    interrupted = True
                    stop = "interrupted"
                    break
                stop = "closed"
                break
            if stop is not None and fault != "ends_after_stop":
                break

    if fault == "complete":
        sender.request_complete()
    return record(stop or "submitted")


def make_faulty_publish(fault):
    def publish(*args, **kwargs):
        if fault == "swallow":
            try:
                return reference_publish(*args, **kwargs)
            except Exception:
                return reference_publish(*args, **kwargs)
        kwargs["fault"] = fault
        return reference_publish(*args, **kwargs)
    return publish


if __name__ == "__main__":                                # pragma: no cover
    unittest.main()
