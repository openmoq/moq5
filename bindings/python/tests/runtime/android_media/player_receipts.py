"""The player's two logcat receipts, parsed strictly (the Android sample's `PresentationSnapshot.line`
and `PresentationClosedReceipt.line`, `PlaybackPresentation.kt`):

    presented: pass=<bool> released=<n> latched=<n> distinctTs=<n> indices=[a,b,…] preVideo=<n>
               unreadable=<n> syncInvalid=<n> complementInvalid=<n> captureRefusals=<n> ordered=<bool>
               settled=<bool> ordinal=<n> leaseId=<n> refusal=<code|none> freezeReason=<settled|cancelled>
    observer-closed: lateAfterFreeze=<n> ignoredForeign=<n> retiredAfterFreeze=<n>

(one line each; shown wrapped). Keys must appear exactly once, in this order, single-space separated,
with typed values. The only other line admitted is logcat's own buffer separator, `--------- beginning
of <buffer>`; anything else is a named refusal. These describe the text the sample writes; whether a
device's `logcat -v raw` reproduces it byte for byte is a device-side gate.
"""
import re

PRESENTED = ("pass", "released", "latched", "distinctTs", "indices", "preVideo", "unreadable", "syncInvalid",
             "complementInvalid", "captureRefusals", "ordered", "settled", "ordinal", "leaseId", "refusal",
             "freezeReason")
CLOSED = ("lateAfterFreeze", "ignoredForeign", "retiredAfterFreeze")
_BOOL = {"pass", "ordered", "settled"}
_VALUE = {
    "bool": re.compile(r"true|false"),
    "int": re.compile(r"0|[1-9][0-9]*"),
    "indices": re.compile(r"\[\]|\[(0|[1-9][0-9]*)(,(0|[1-9][0-9]*))*\]"),
    "refusal": re.compile(r"none|[a-z][a-z-]*(:[a-z][a-z-]*)?"),
    "freezeReason": re.compile(r"settled|cancelled"),
}
_SEPARATOR = re.compile(r"--------- beginning of [a-z]+")
# `PresentationRecorder`'s default `capacity` (PlaybackPresentation.kt), which the sample's attempt uses:
# `offer` refuses before counting once `facts == capacity`, so a snapshot never latches more.
RECORDER_CAPACITY = 512


class ReceiptError(ValueError):
    pass


def _kind(key):
    if key in _BOOL:
        return "bool"
    if key in ("indices", "refusal", "freezeReason"):
        return key
    return "int"


def _fields(body, keys, prefix):
    parts = body.split(" ")
    if [p.split("=", 1)[0] for p in parts] != list(keys) or any(p.count("=") != 1 for p in parts):
        raise ReceiptError(f"{prefix} keys are {[p.split('=', 1)[0] for p in parts]}, expected {list(keys)}")
    out = {}
    for part in parts:
        key, value = part.split("=")
        kind = _kind(key)
        if not _VALUE[kind].fullmatch(value):
            raise ReceiptError(f"{prefix} {key}={value!r} is not a {kind}")
        out[key] = (value == "true" if kind == "bool" else int(value) if kind == "int"
                    else [int(i) for i in value[1:-1].split(",")] if kind == "indices" and value != "[]"
                    else [] if kind == "indices" else value)
    return out


def parse_line(line):
    """("presented" | "observer-closed", fields) for a receipt; None for a logcat buffer separator."""
    if _SEPARATOR.fullmatch(line):
        return None
    for prefix, keys in (("presented", PRESENTED), ("observer-closed", CLOSED)):
        if line.startswith(prefix + ": "):
            return prefix, _fields(line[len(prefix) + 2:], keys, prefix)
    raise ReceiptError(f"not a player receipt: {line!r}")


def presented_rejection(fields, n_objects):
    """None when a `presented:` receipt accepts this run's presentation; otherwise every reason why not.

    The receipt's own `pass` is not trusted alone: each condition the recorder's freeze requires for a
    pass (`PresentationRecorder.freeze`, `PlaybackPresentation.kt`) must visibly hold in the reported
    evidence. The lease is the registry's (`leaseId` counts from 1, `ordinal` 1 is a fresh surface);
    the valid indices are a sorted set of at least two, ending at the final index; at least two
    distinct timestamps; every accepted capture is latched, each distinct index or timestamp
    needs a valid one after the pre-video captures, and no more than the recorder's capacity are;
    a capture, sync or complement refusal fails the recorder, and an unreadable capture after the
    first valid index does too. The run's input carries
    only markers 0..n_objects-1; a strictly increasing list of non-negative indices ending at the
    final index cannot hold any other, so no separate range check is needed."""
    wrong = []
    if fields["leaseId"] < 1:
        wrong.append(f"leaseId={fields['leaseId']}, expected >= 1")
    indices, final = fields["indices"], n_objects - 1
    if len(indices) < 2:
        wrong.append(f"indices={indices}, expected at least 2")
    if not indices or indices[-1] != final:
        wrong.append(f"indices={indices} do not end at the final index {final}")
    if any(a >= b for a, b in zip(indices, indices[1:])):
        wrong.append(f"indices={indices} are not strictly increasing")
    if fields["distinctTs"] < 2:
        wrong.append(f"distinctTs={fields['distinctTs']}, expected >= 2")
    valid = max(len(indices), fields["distinctTs"])
    if fields["latched"] < fields["preVideo"] + valid:
        wrong.append(f"latched={fields['latched']}, expected >= preVideo {fields['preVideo']} + {valid} valid")
    if fields["latched"] > RECORDER_CAPACITY:
        wrong.append(f"latched={fields['latched']} exceeds the recorder's capacity {RECORDER_CAPACITY}")
    for key in ("syncInvalid", "complementInvalid", "captureRefusals"):
        if fields[key] != 0:
            wrong.append(f"{key}={fields[key]}, expected 0")
    if fields["unreadable"] > fields["preVideo"]:
        wrong.append(f"unreadable={fields['unreadable']} exceeds preVideo={fields['preVideo']}")
    want = {"pass": True, "released": n_objects, "ordinal": 1, "refusal": "none", "freezeReason": "settled",
            "settled": True, "ordered": True}
    wrong += [f"{k}={fields[k]!r}, expected {v!r}" for k, v in want.items() if fields[k] != v]
    return "; ".join(wrong) or None
