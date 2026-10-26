"""The wire-identity oracle for one supervised run: the expected (track, group, subgroup, object,
priority) sequence is derived from the publisher's own inputs and configuration, the observed sequence is
read from the player's diagnostic breadcrumbs, and the two are compared position by position.

The expectation never comes from the player. It follows from what the publisher was given and what the
service does with it: the publisher opens a new group at every sync object of its ledger (`starts_group =
is_sync`, publisher.c), the service numbers groups from 0 and objects from 0 within each group
(media_sender.c), the publisher facade emits one subgroup per group with subgroup id 0 (facade
publisher.c), the publisher declares no track priority so the facade default 128 goes on the subgroup
header, and the relay stores and forwards that priority verbatim (relay/include/moq/relay/log.h). Only
subgroup-stream delivery is exercised, so every expected subgroup is 0; an observed `none` (a datagram)
is a difference, never a match.

Scope of a passing comparison:
  - priority: 128 is the effective value expected at the player. Because it is also the default a relay
    would substitute, a match proves delivery of the expected effective value, not that the relay
    preserved a publisher-chosen priority rather than substituting 128.
  - order: positional equality is this finite fixture's expectation (one publisher, one track, one
    subgroup per group, written in order). The receiver delivers in arrival order and this oracle does
    not claim it sorts inter-stream objects; a reordered inventory is retained and classified before any
    conclusion about identity is drawn.

    python3 identity_oracle.py <run-dir>     reads objects.bin and player-diagnostic.log, writes
                                             identity-oracle.json exclusively; exit 0 pass, 1 fail,
                                             2 refused (missing input, unreadable log, verdict exists)
"""
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import media_oracle  # noqa: E402

PUBLISHER_DEFAULT_PRIORITY = 128   # moq_pub_track_cfg_init / moq_pub_cfg default_publisher_priority
TRACK = "video"                    # the lane the player names for the selected video track
DIAGNOSTIC_LOG = "player-diagnostic.log"
LEDGER = "objects.bin"
VERDICT = "identity-oracle.json"
PRIORITY_DERIVATION = ("128 = the publisher facade default (no explicit track priority), "
                       "forwarded verbatim by the relay")

_TAG = re.compile(r"\bI MoqPlayerDiag: (\d+) (.*)$")
_IDENTITY = re.compile(r"^object identity lane=(\w+) (?:(absent)|group=(\d+) subgroup=(none|\d+) object=(\d+) priority=(\d+))$")
_CAP = re.compile(r"^\d+ breadcrumbs capped at \d+; later ones are dropped$")


def expected_sequence(entries, track=TRACK, priority=PUBLISHER_DEFAULT_PRIORITY):
    """The tuples the publisher's ledger commits it to, in ledger order. Refuses a ledger that does not
    open with a sync object: the publisher itself refuses that input."""
    if not entries or not entries[0].is_sync:
        raise ValueError("the ledger must open with a sync object; the publisher refuses anything else")
    out, group, obj = [], -1, 0
    for e in entries:
        if e.is_sync:
            group, obj = group + 1, 0
        out.append((track, group, 0, obj, priority))
        obj += 1
    return out


def observed_sequence(text):
    """Every identity breadcrumb of the player's diagnostic log, in order: a tuple, or None for an
    object the binding delivered without identity. Any other breadcrumb is ignored; a malformed identity
    line or a capped trace is refused, because a partial inventory cannot be judged."""
    out = []
    for line in text.splitlines():
        m = _TAG.search(line)
        if not m:
            continue
        body = m.group(2)
        if _CAP.match(m.group(1) + " " + body):
            raise ValueError("the diagnostic trace was capped; the inventory is incomplete")
        if not body.startswith("object identity"):
            continue
        f = _IDENTITY.match(body)
        if not f:
            raise ValueError(f"malformed identity breadcrumb: {body!r}")
        lane, absent, group, subgroup, obj, priority = f.groups()
        if absent:
            out.append(None)
        else:
            out.append((lane, int(group), None if subgroup == "none" else int(subgroup), int(obj), int(priority)))
    return out


def compare(expected, observed):
    """Position by position. A count difference is the only failure named for a short or long
    inventory; otherwise every differing position is named. The progression facts describe what the
    run actually exercised and are reported whether or not it passed."""
    failures = []
    if len(observed) != len(expected):
        failures.append(f"observed {len(observed)} identities, expected {len(expected)}")
    else:
        for i, (want, got) in enumerate(zip(expected, observed)):
            if got is None:
                failures.append(f"object {i}: expected {want}, observed no identity")
            elif got != want:
                failures.append(f"object {i}: expected {want}, observed {got}")
    present = [o for o in observed if o is not None]
    matched = sum(1 for want, got in zip(expected, observed) if got == want)
    return {
        "ok": not failures,
        "failures": failures,
        "counts": {"expected": len(expected), "observed": len(observed), "matched": matched},
        "groups": sorted({o[1] for o in present}),
        "nonzero_group_progression": any(o[1] > 0 for o in present),
        "nonzero_object_progression": any(o[3] > 0 for o in present),
        "subgroup_zero_present": any(o[2] == 0 for o in present),
        "subgroup_absent_present": any(o[2] is None for o in present),
        "absent_identities": sum(1 for o in observed if o is None),
    }


def judge(run_dir):
    run_dir = Path(run_dir)
    entries = media_oracle.load_objects(run_dir / LEDGER)
    expected = expected_sequence(entries)
    observed = observed_sequence((run_dir / DIAGNOSTIC_LOG).read_text(errors="replace"))
    verdict = compare(expected, observed)
    verdict["oracle"] = {
        "ledger": LEDGER, "ledger_objects": len(entries), "sync_indices": [e.index for e in entries if e.is_sync],
        "track": TRACK, "subgroup": "0 = one subgroup per group (subgroup-stream delivery only)",
        "priority": PRIORITY_DERIVATION, "log": DIAGNOSTIC_LOG,
    }
    verdict["expected"] = expected
    verdict["observed"] = observed
    return verdict


def main(argv):
    if len(argv) != 1:
        print("usage: identity_oracle.py <run-dir>", file=sys.stderr)
        return 2
    run_dir = Path(argv[0])
    for name in (LEDGER, DIAGNOSTIC_LOG):
        if not (run_dir / name).is_file():
            print(f"identity_oracle: refused: no {name} under {run_dir}", file=sys.stderr)
            return 2
    if (run_dir / VERDICT).exists():
        print(f"identity_oracle: refused: {run_dir / VERDICT} exists; a verdict is never overwritten", file=sys.stderr)
        return 2
    try:
        verdict = judge(run_dir)
    except ValueError as e:
        print(f"identity_oracle: refused: {e}", file=sys.stderr)
        return 2
    with open(run_dir / VERDICT, "x") as f:
        json.dump(verdict, f, indent=1, sort_keys=True)
        f.write("\n")
    c = verdict["counts"]
    print(f"identity_oracle: {'passed' if verdict['ok'] else 'FAILED'}: {c['matched']}/{c['expected']} matched, "
          f"groups {verdict['groups']}, failures {len(verdict['failures'])}; verdict {run_dir / VERDICT}", file=sys.stderr)
    return 0 if verdict["ok"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
