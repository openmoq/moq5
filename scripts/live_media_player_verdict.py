#!/usr/bin/env python3
"""Lifecycle verdict rules of scripts/live_media_player_cases.sh.

The runner loads this file by path and calls pubkill_verdict for its
pubkill lifecycle; --self-test runs the rules on synthetic outcomes.

pubkill: the publisher is killed with SIGKILL. The run exercises the case
only when the player was READY before the kill and the kill was sent;
otherwise it is NOT_EXERCISED, never a pass. An exercised run passes when,
within the bound after the kill, every selected track has ended and none
ended COMPLETE, or the receiver failed for good after the kill. A partial
set of ENDs, a receiver failure from before the kill, or the player's own
deadline with nothing terminal, does not pass.
"""
import sys

DISPOSITION_COMPLETE = "1"
PUBKILL_BOUND_S = 20


def pubkill_verdict(ends, expected_tracks, complete, fatal, lag_s,
                    ready=True, killed=True, fatal_after_kill=None):
    """(verdict, notes) for a pubkill run.

    ends             [(track, cause, disposition)] in END order
    expected_tracks  how many tracks the case selected
    complete         the player reported the finite run COMPLETE
    fatal            the receiver failed for good
    lag_s            seconds from the kill to the player's exit
    ready            the player was READY before the kill
    killed           the kill was sent
    fatal_after_kill whether the failure came after the kill (None: unknown,
                     not counted as after)
    """
    notes = []
    if not (ready and killed):
        notes.append("not exercised: %s" % ("no kill sent" if not killed
                                            else "the player was not READY before the kill"))
        return "NOT_EXERCISED", notes
    if fatal and fatal_after_kill is not True:
        notes.append("the receiver failed before the kill (or at an unknown time)")
        fatal = False
    ended = {track for track, _, _ in ends}
    if any(disp == DISPOSITION_COMPLETE for _, _, disp in ends) or complete:
        notes.append("a track ended COMPLETE after the publisher was killed")
        return "FAIL", notes
    all_ended = expected_tracks > 0 and len(ended) >= expected_tracks
    if not (all_ended or fatal):
        notes.append("%d of %d selected tracks ended, receiver not failed" %
                     (len(ended), expected_tracks))
        return "FAIL", notes
    if lag_s > PUBKILL_BOUND_S:
        return "FAIL", notes
    return "PASS", notes


def self_test():
    fails = 0

    def expect(what, got, want):
        nonlocal fails
        if got != want:
            fails += 1
            print("FAIL %s: got %r, want %r" % (what, got, want), file=sys.stderr)

    cut = [("video", "2", "2"), ("audio", "2", "2")]
    expect("both ended in time", pubkill_verdict(cut, 2, False, False, 5)[0], "PASS")
    expect("both ended late", pubkill_verdict(cut, 2, False, False, 32)[0], "FAIL")
    expect("one of two ended", pubkill_verdict(cut[:1], 2, False, False, 5)[0], "FAIL")
    expect("deadline, nothing ended", pubkill_verdict([], 2, False, False, 5)[0], "FAIL")
    expect("receiver failed late", pubkill_verdict(
        [], 2, False, True, 32, fatal_after_kill=True)[0], "FAIL")
    expect("one ended COMPLETE", pubkill_verdict(
        [("video", "1", "1"), ("audio", "2", "2")], 2, False, False, 5)[0], "FAIL")
    expect("run COMPLETE", pubkill_verdict(cut, 2, True, False, 5)[0], "FAIL")
    expect("same track twice is one", pubkill_verdict(
        [("video", "2", "2"), ("video", "2", "2")], 2, False, False, 5)[0], "FAIL")
    expect("receiver failed after the kill", pubkill_verdict(
        [], 2, False, True, 5, fatal_after_kill=True)[0], "PASS")
    expect("receiver failed before the kill", pubkill_verdict(
        [], 2, False, True, 5, fatal_after_kill=False)[0], "FAIL")
    expect("receiver failed, time unknown", pubkill_verdict(
        [], 2, False, True, 5, fatal_after_kill=None)[0], "FAIL")
    expect("not READY before the kill", pubkill_verdict(
        cut, 2, False, False, 5, ready=False)[0], "NOT_EXERCISED")
    expect("no kill sent", pubkill_verdict(cut, 2, False, False, 5, killed=False)[0],
           "NOT_EXERCISED")
    print("self-test: %s" % ("FAIL" if fails else "ok"))
    return fails


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        sys.exit(1 if self_test() else 0)
    print(__doc__.strip(), file=sys.stderr)
    sys.exit(2)
