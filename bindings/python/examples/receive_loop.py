"""Caller-driven receive loop over the delivered synchronous receiver binding.

A handler-driven loop over moq5.Receiver (poll_track / poll_object / wait /
drained / subscribe): the model test and the fixture-provider integration
tests drive this exact file. It is not a media decoder or a relay command;
the handlers it is given decide what to do with owned bytes. Every successful
poll goes through the same handler; polling is never a peek. It distinguishes
three exits: transport drain (`rx.drained()`), a requested interruption, and
finite broadcast completion -- the last being THIS LOOP'S POLICY over what
the C facade observes (TRACK_ENDED, which includes rejection; the survivor
description's live->VOD change; TRACK_REMOVED; the catalog isComplete latch),
not proof that every track ended successfully or of the publisher's full
wire sequence.
"""

from __future__ import annotations


def receive(rx, PollOutcome, TrackEventKind, WaitResult, Packaging, MoqError,
            decode_raw, decode_cmaf, note_status, wait_us=250_000):
    """Run one receiver to a stop; return an observation record (all booleans exact)."""
    wanted: set = set()          # tracks we subscribed
    ended: set = set()           # tracks whose TERMINAL ENDED event was drained
    removed: set = set()         # tracks the catalog removed
    vod: set = set()             # tracks whose declared state became NON-live (isLive false)
    state = {"terminal_seen": False, "interrupted": False, "complete_signal": False,
             "objects": 0, "events": 0}

    def handle_event(ev):
        state["events"] += 1
        if ev.kind is TrackEventKind.ADDED:
            desc = ev.description
            if desc.packaging_text in (b"cmaf", b"loc") and not state["terminal_seen"]:
                try:
                    rx.subscribe(ev.track)
                    wanted.add(ev.track)
                except MoqError as refused:
                    if refused.code == int(PollOutcome.CLOSED):
                        state["terminal_seen"] = True       # terminal: keep draining, no more commands
                    else:
                        raise                               # INVAL/WRONG_STATE/UNSUPPORTED are real errors
        elif ev.kind is TrackEventKind.ENDED:
            ended.add(ev.track)                             # includes rejection: never completion by itself
        elif ev.kind is TrackEventKind.UPDATED:
            vod_state = ev.description.vod
            # MSF-01 section 11.3: the conversion sets isLive false AND adds a
            # trackDuration. Presence is `is not None` (zero is a duration);
            # a VodState merely existing, or non-live without a duration, is
            # not the signal.
            if vod_state is not None and vod_state.is_live is False and vod_state.track_duration_ms is not None:
                vod.add(ev.track)
        elif ev.kind is TrackEventKind.REMOVED:
            removed.add(ev.track)
        if rx.stats().catalog_complete:
            state["complete_signal"] = True

    def handle_object(obj):
        state["objects"] += 1
        if obj.is_status_only:
            note_status(obj.track, obj.status)
        elif obj.packaging is Packaging.RAW:
            decode_raw(obj.track.description, obj.payload)
        elif obj.packaging is Packaging.CMAF:
            decode_cmaf(obj.track.description, obj.fragment, obj.mdat_offset, obj.mdat_len, obj.samples)
        else:
            raise RuntimeError(f"unknown packaging {obj.packaging!r}")

    def drain_events():
        """Drain to EMPTY/CLOSED; every item is handled. Returns the stopping outcome."""
        while True:
            ev = rx.poll_track()
            if ev is PollOutcome.EMPTY:
                return ev
            if ev is PollOutcome.CLOSED:
                state["terminal_seen"] = True
                return ev
            handle_event(ev)

    def drain_objects():
        while True:
            obj = rx.poll_object()
            if obj is PollOutcome.EMPTY:
                return obj
            if obj is PollOutcome.CLOSED:
                state["terminal_seen"] = True
                return obj
            if obj is PollOutcome.INTERRUPTED:
                state["interrupted"] = True                 # caller decision, not completion
                return obj
            handle_object(obj)

    def broadcast_complete():
        """This loop's completion policy over the facade's observations of
        MSF-01 sec. 11.3: every wanted track reached its TERMINAL ENDED
        (rejection included), and either every wanted track declared non-live
        WITH a duration (VOD conversion) or the catalog signalled isComplete
        with every wanted track removed. Ordinary removal without isComplete,
        ENDED alone, and non-live without a duration are not completion."""
        if not wanted or not wanted <= ended:
            return False
        if wanted <= vod:
            return True
        return state["complete_signal"] and wanted <= removed

    completed = False
    while not state["interrupted"]:
        e_out = drain_events()
        o_out = drain_objects()
        if rx.drained():                                    # both exposed streams CLOSED: transport terminal
            break
        if broadcast_complete():
            # Completion never discards the other queue: drain once more through
            # the SAME handlers. Those handlers can change the criterion (a late
            # ADDED track is subscribed and has no ENDED yet), so the criterion
            # is re-evaluated AFTER the second drain; stop only when both drains
            # found nothing and it still holds.
            e_out = drain_events()
            o_out = drain_objects()
            # Under the facade's shared, monotonic terminal predicate a CLOSED
            # event re-drain is always followed by objects draining to CLOSED
            # (or INTERRUPTED), so the e_out comparison is a readable guard,
            # not a separate reachable branch.
            if e_out is PollOutcome.EMPTY and o_out is PollOutcome.EMPTY and broadcast_complete():
                completed = True
                break
            continue
        if not state["interrupted"] and rx.wait(wait_us) is WaitResult.INTERRUPTED:
            state["interrupted"] = True
    return {"completed": completed, "drained": rx.drained(), "interrupted": state["interrupted"],
            "objects": state["objects"], "events": state["events"],
            "terminal": rx.terminal, "stats": rx.stats()}
