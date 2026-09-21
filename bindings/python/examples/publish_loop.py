"""Caller-driven finite publish loop over the delivered synchronous sender.

A plan-driven loop over moq5.Sender (write / end_track / wait / stats /
has_media_subscriber): the model test and the fixture-provider integration
tests drive this exact file. It is not an encoder, a capture layer or a
scheduler; the plan it is given decides what bytes are submitted, and the
caller owns the endpoint, the sender and the tracks on both sides of the call.

What it does NOT do, deliberately: it never closes or destroys an owner,
never adds or removes a track, never requests completion, never drains, and
never starts a thread or a process. Closing discards whatever the service
still has queued, and an accepted completion request marks every track
removed -- whose queued media the service then discards as it tears them
down -- so either would turn accepted work into lost work and report success.
Both stay the caller's decision, after this report has been read.

The returned record is a SUBMISSION report. `stop == "submitted"` means every
planned object was accepted by the service and every track's end was
requested; it is not emission, flush, acknowledgment or receipt, it says
nothing about END_OF_TRACK reaching a peer or a catalog being published, and
it is not a completion predicate.

`deadline_us` is a COOPERATIVE submission budget read through `now_us`. It
bounds how long this loop keeps attempting and clips every wait it asks for.
It cannot cancel a native call already in progress, a caller callback, or the
evaluation of the sequence: a BLOCK_TIMEOUT sender can spend its whole
configured block timeout inside ONE write, past the remaining budget. A
bounded run wants RETURN_WOULD_BLOCK.

`note_dropped` receives the increment between OBSERVATIONS whenever the
service's named drop counters advance -- never one final aggregate, and never
a callback per native transition, which is not observable from here.
Accepted is not delivered: under a drop policy the service may evict accepted
media. Losslessness is the caller's own configuration, never applied here.

`require_demand` is opt-in and finite. `has_media_subscriber()` is the
service's AGGREGATE question -- does ANY app-visible media track have a
subscriber -- so an unrelated subscribed track satisfies it; it is not
per-track demand gating. Readiness is a different thing again and is never
queried: writing pre-ready is legal and bounded by the service's own
pre-ready caps.
"""

from __future__ import annotations

# The statistics that are drop counters. Nothing else is differenced:
# last_error is historical data, not a count.
DROP_COUNTERS = ("objects_dropped", "groups_dropped", "keyframes_dropped",
                 "groups_abandoned")

# The one native code this loop interprets: a track the service already
# considers terminal. Every other code stays the caller's to read.
_WRONG_STATE = -5


def _drops(stats):
    return {name: getattr(stats, name) for name in DROP_COUNTERS}


def publish(sender, plan, now_us, deadline_us, WriteOutcome, WaitResult,
            MoqError, note_dropped, wait_us=250_000, require_demand=False):
    """Submit one finite plan to a live sender; return a submission report.

    `plan` is a finite ordered sequence of (track, SendObject) pairs. The
    exact object is retained until its submission is ACCEPTED, the plan
    advances only then, and an accepted object is never submitted again.
    """
    submissions = list(plan)
    order = []                                   # tracks, in first-use order
    for track, _submission in submissions:
        if not any(track is seen_track for seen_track in order):
            order.append(track)

    base = _drops(sender.stats())
    seen = dict(base)
    submitted = attempts = would_block = 0
    index = 0
    interrupted = False
    stop = None
    ended, refused = [], []

    def remaining():
        return deadline_us - now_us()

    def observe():
        """Report the increment since the last observation, if there is one."""
        current = _drops(sender.stats())
        delta = {name: current[name] - seen[name] for name in DROP_COUNTERS}
        if any(delta.values()):
            seen.update(current)
            note_dropped(delta)

    def bounded_wait():
        """One wait, clipped to the remaining budget; None when none is left."""
        left = remaining()
        if left <= 0:
            return None
        return sender.wait(min(wait_us, left))

    def report(reason):
        # ONE final observation carries the returned statistics, the whole-run
        # aggregate, and the remainder no earlier observation had seen.
        stats = sender.stats()
        final = _drops(stats)
        remainder = {name: final[name] - seen[name] for name in DROP_COUNTERS}
        if any(remainder.values()):
            seen.update(final)
            note_dropped(remainder)
        return {
            "stop": reason,
            "submitted": submitted,
            "attempts": attempts,
            "pending": None if index >= len(submissions) else index,
            "would_block": would_block,
            "ended": tuple(ended),
            "ends_refused": tuple(refused),
            "interrupted": interrupted,
            "dropped": {name: final[name] - base[name]
                        for name in DROP_COUNTERS},
            "terminal": sender.terminal,
            "stats": stats,
        }

    # Demand, when the caller asked for it: bounded by the same deadline.
    if require_demand:
        while not sender.has_media_subscriber():
            result = bounded_wait()
            if result is None:
                return report("deadline")
            observe()
            if result is WaitResult.INTERRUPTED:
                interrupted = True
                return report("interrupted")
            if result is WaitResult.CLOSED:
                return report("closed")

    # Submission. One write per attempt; the same object until accepted.
    while index < len(submissions):
        if remaining() <= 0:
            stop = "deadline"
            break
        track, submission = submissions[index]
        attempts += 1
        outcome = sender.write(track, submission)
        observe()
        if outcome is WriteOutcome.ACCEPTED:
            submitted += 1
            index += 1
            continue
        if outcome is WriteOutcome.WOULD_BLOCK:
            would_block += 1
            result = bounded_wait()
            if result is None:
                stop = "deadline"
                break
            observe()
            if result is WaitResult.INTERRUPTED:
                interrupted = True
                stop = "interrupted"
                break
            if result is WaitResult.CLOSED:
                stop = "closed"
                break
            continue                             # WOKEN or TIMED_OUT: retry
        if outcome is WriteOutcome.INTERRUPTED:
            interrupted = True
            stop = "interrupted"
            break
        stop = "closed"                          # the agreed CLOSED outcome
        break

    # Ending, only if nothing has stopped the run. A track whose end the
    # service refuses with the one named track-local code is recorded and its
    # siblings continue; anything else is the caller's error and propagates.
    if stop is None:
        for track in order:
            while True:
                if remaining() <= 0:
                    stop = "deadline"
                    break
                try:
                    outcome = sender.end_track(track)
                except MoqError as refusal:
                    if refusal.code == _WRONG_STATE:
                        refused.append((track, refusal.code))
                        break
                    raise
                if outcome is WriteOutcome.ACCEPTED:
                    ended.append(track)
                    break
                if outcome is WriteOutcome.WOULD_BLOCK:
                    result = bounded_wait()
                    if result is None:
                        stop = "deadline"
                        break
                    observe()
                    if result is WaitResult.INTERRUPTED:
                        interrupted = True
                        stop = "interrupted"
                        break
                    if result is WaitResult.CLOSED:
                        stop = "closed"
                        break
                    continue
                if outcome is WriteOutcome.INTERRUPTED:
                    interrupted = True
                    stop = "interrupted"
                    break
                stop = "closed"
                break
            if stop is not None:                 # a stop stops the siblings
                break

    return report(stop or "submitted")
