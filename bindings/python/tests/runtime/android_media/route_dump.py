"""The relay's SIGUSR1 route dump, read from its stderr, and the `go` barrier it serves.

The barrier is an operational attribution by run construction, not a proof of peer identity: the
dump names a subscription only by its binding cookie, which is a reused bind-layer slot number,
and nothing the relay exposes pairs a cookie with a peer. What is required exactly is the route:
the declared namespace's one announce, on binding U, is the declared track's upstream. The
subscriptions are attributed by staging:

    baseline     after the host receiver reports ACTIVE and before the player is started, one
                 dump shows exactly one active subscription on the track, on binding H != U
    after start  a dump requested after the player start shows the same route and exactly two
                 active subscriptions {H, V}, V != H, neither on U

Only that transition sets `route_two_subscriptions`. Two ambiguities remain and are not detected:
an unowned local client subscribing between the two snapshots is indistinguishable from the
player, and a binding that closes and whose cookie is reused between them leaves no trace in
the dump. The request/response pairing also assumes the probe is the relay's only SIGUSR1
producer: a block caused by another actor's concurrent signal is not distinguishable.

On SIGUSR1 the single-lane relay writes, among its other blocks, one route block to stderr
(`tools/moq5-relay/cli/main.c`, dump_emit):

    <empty line>
    ===== MOQ5 Relay routes =====
    <moqr_core_route_dump_text output: non-empty lines, each ending in LF>
    <empty line>

A block is complete only once its terminating empty line has been read. The dump's grammar is
that of `relay/src/core/relay.c` moqr_core_route_dump_text:

    epochs: node=N shard=N route=N
    announces:
      <namespace> -> binding N            (or "  (none)")
    namespace_subscriptions:
      <namespace> -> binding N            (or "  (none)")
    tracks:
      <namespace> "<name>" state=pending|active|warm|unknown upstream_binding=N|-
        log: groups=N records=N bytes=N oldest=N|null newest=N|null eot=0|1
        subs:
          binding=N state=parked|active|unknown start=N/N end=N|- lag_groups=N frontier=N|null skip_pending=0|1 start_before_retention=0|1
                                          (or "      (none)"; a track list may be "  (none)")

A namespace is its parts joined by "/"; a part or name byte outside 0x20..0x7e, and every "\\",
"/" and '"', is written "\\xNN" in lower-case hex. Every line of a complete block must match;
anything else is a named parse failure, never skipped.
"""
import re
import signal
import threading
from dataclasses import dataclass

ROUTES_HEADER = "===== MOQ5 Relay routes ====="
_HEADER = re.compile(r"===== MOQ5 Relay (.+) =====")

_PART = r'(?:[\x20-\x21\x23-\x2e\x30-\x5b\x5d-\x7e]|\\x[0-9a-f]{2})*'
_NS = rf"(?:{_PART}(?:/{_PART})*)?"
_EPOCHS = re.compile(r"epochs: node=([0-9]+) shard=([0-9]+) route=([0-9]+)")
_BOUND = re.compile(rf"  ({_NS}) -> binding ([0-9]+)")
_TRACK = re.compile(rf'  ({_NS}) "({_PART})" state=(pending|active|warm|unknown) upstream_binding=([0-9]+|-)')
_LOG = re.compile(r"    log: groups=([0-9]+) records=([0-9]+) bytes=([0-9]+) oldest=([0-9]+|null) newest=([0-9]+|null) eot=([01])")
_SUB = re.compile(r"      binding=([0-9]+) state=(parked|active|unknown) start=([0-9]+)/([0-9]+) end=([0-9]+|-) "
                  r"lag_groups=([0-9]+) frontier=([0-9]+|null) skip_pending=([01]) start_before_retention=([01])")


class RouteDumpError(ValueError):
    pass


@dataclass(frozen=True)
class Sub:
    binding: int
    state: str
    start_group: int
    start_object: int
    end_group: object          # int, or None for "-"
    lag_groups: int
    frontier: object           # int, or None for "null"
    skip_pending: bool
    start_before_retention: bool


@dataclass(frozen=True)
class Track:
    namespace: tuple           # of bytes
    name: bytes
    state: str
    upstream_binding: object   # int, or None for "-"
    log: dict
    subs: tuple


@dataclass(frozen=True)
class RouteDump:
    epochs: tuple              # (node, shard, route)
    announces: tuple           # ((namespace, binding), ...)
    namespace_subscriptions: tuple
    tracks: tuple


def _bytes(text):
    out, i = bytearray(), 0
    while i < len(text):
        if text[i] == "\\":
            out.append(int(text[i + 2:i + 4], 16))
            i += 4
        else:
            out.append(ord(text[i]))
            i += 1
    return bytes(out)


def _namespace(text):
    return tuple(_bytes(p) for p in text.split("/")) if text else ()


def _opt(text, none):
    return None if text == none else int(text)


def parse(lines):
    """The RouteDump for one complete block's dump lines (header and terminator excluded)."""
    lines = list(lines)
    pos = 0

    def fail(why):
        where = f"line {pos + 1}: {lines[pos]!r}" if pos < len(lines) else f"after line {len(lines)}: end of dump"
        raise RouteDumpError(f"route dump {where}: {why}")

    def take(pattern, why):
        nonlocal pos
        m = pattern.fullmatch(lines[pos]) if pos < len(lines) else None
        if m is None:
            fail(why)
        pos += 1
        return m

    def literal(text):
        nonlocal pos
        if pos >= len(lines) or lines[pos] != text:
            fail(f"expected {text!r}")
        pos += 1

    def at(text):
        return pos < len(lines) and lines[pos] == text

    def bound_section(title):
        nonlocal pos
        literal(title)
        if at("  (none)"):
            pos += 1
            return ()
        out = [take(_BOUND, "expected '<namespace> -> binding N' or '  (none)'")]
        while pos < len(lines) and _BOUND.fullmatch(lines[pos]):
            out.append(take(_BOUND, ""))
        return tuple((_namespace(m[1]), int(m[2])) for m in out)

    e = take(_EPOCHS, "expected 'epochs: node=N shard=N route=N'")
    epochs = (int(e[1]), int(e[2]), int(e[3]))
    announces = bound_section("announces:")
    ns_subs = bound_section("namespace_subscriptions:")
    literal("tracks:")
    tracks = []
    if at("  (none)"):
        pos += 1
    else:
        while True:
            t = take(_TRACK, "expected a track line")
            lg = take(_LOG, "expected the track's log line")
            literal("    subs:")
            subs = []
            if at("      (none)"):
                pos += 1
            else:
                while True:
                    s = take(_SUB, "expected a subscription line or '      (none)'")
                    subs.append(Sub(int(s[1]), s[2], int(s[3]), int(s[4]), _opt(s[5], "-"), int(s[6]),
                                    _opt(s[7], "null"), s[8] == "1", s[9] == "1"))
                    if pos >= len(lines) or not lines[pos].startswith("      "):
                        break
            tracks.append(Track(_namespace(t[1]), _bytes(t[2]), t[3], _opt(t[4], "-"),
                                {"groups": int(lg[1]), "records": int(lg[2]), "bytes": int(lg[3]),
                                 "oldest": _opt(lg[4], "null"), "newest": _opt(lg[5], "null"), "eot": lg[6] == "1"},
                                tuple(subs)))
            if pos >= len(lines):
                break
    if pos != len(lines):
        fail("unexpected line after the dump")
    return RouteDump(epochs, announces, ns_subs, tuple(tracks))


def complete_blocks(lines, start=0):
    """The dump lines of every complete route block whose header is at or after `start` in the
    relay's stderr `lines`. A header for any other routes label (a multi-lane relay's
    `shard N routes`) or a second header before a block's terminating empty line is refused."""
    blocks, i, n = [], start, len(lines)
    while i < n:
        m = _HEADER.fullmatch(lines[i])
        if m is None or not m[1].endswith("routes"):
            i += 1
            continue
        if lines[i] != ROUTES_HEADER:
            raise RouteDumpError(f"stderr line {i + 1}: {lines[i]!r} is not the single-lane routes header")
        j = i + 1
        while j < n and lines[j] != "":
            if _HEADER.fullmatch(lines[j]):
                raise RouteDumpError(f"stderr line {j + 1}: {lines[j]!r} begins before the routes block at line {i + 1} ended")
            j += 1
        if j == n:
            break                                  # not yet terminated
        blocks.append(lines[i + 1:j])
        i = j + 1
    return blocks


def _label(namespace, track):
    return "/".join(p.decode("ascii", "backslashreplace") for p in namespace) + \
        f' "{track.decode("ascii", "backslashreplace")}"'


def _route(dump, namespace, track):
    """(verdict, reason, U, track record) for the exact route: one announce of `namespace` on U,
    one `track` in it, active, upstream U, and no subscription on U or on one binding twice.
    verdict None means the route holds."""
    ns, label = tuple(namespace), _label(namespace, track)
    announces = [b for n, b in dump.announces if n == ns]
    if len(announces) > 1:
        return "refused", f"{label}: its namespace is announced by bindings {announces}", None, None
    if not announces:
        return "waiting", f"{label}: its namespace is not announced", None, None
    upstream = announces[0]
    found = [t for t in dump.tracks if t.namespace == ns and t.name == track]
    if len(found) > 1:
        return "refused", f"{label} appears {len(found)} times", upstream, None
    if not found:
        return "waiting", f"{label} has no route", upstream, None
    t = found[0]
    if t.upstream_binding is not None and t.upstream_binding != upstream:
        return "refused", f"{label} has upstream {t.upstream_binding}, but its namespace is announced by {upstream}", upstream, t
    bindings = [s.binding for s in t.subs]
    if upstream in bindings:
        return "refused", f"{label} has a subscription on its upstream binding {upstream}", upstream, t
    if len(set(bindings)) != len(bindings):
        return "refused", f"{label} has more than one subscription on one binding: {bindings}", upstream, t
    if t.state != "active" or t.upstream_binding is None:
        return "waiting", f"{label} is {t.state} with upstream {t.upstream_binding}", upstream, t
    return None, f"{label} routed from {upstream}", upstream, t


@dataclass(frozen=True)
class Baseline:
    upstream: int              # U, the announcing binding and the track's upstream
    host: int                  # H, the one downstream subscription before the player start


def judge_baseline(dump, namespace, track):
    """("ready", reason, Baseline) | ("waiting" | "refused", reason, None) before the player start."""
    verdict, reason, upstream, t = _route(dump, namespace, track)
    if verdict is not None:
        return verdict, reason, None
    label = _label(namespace, track)
    if len(t.subs) > 1:
        return "refused", f"{label} has {len(t.subs)} subscriptions before the player start; the baseline is one", None
    if not t.subs or t.subs[0].state != "active":
        return "waiting", f"{label} has no active subscription yet", None
    return "ready", f"{label} routed from {upstream}, baseline subscription {t.subs[0].binding}", \
        Baseline(upstream, t.subs[0].binding)


def judge_after_start(dump, namespace, track, baseline):
    """("ready", reason, V) | ("waiting" | "refused", reason, None) after the player start. The
    route recorded at the baseline must still hold; losing it is refused, not waited for."""
    verdict, reason, upstream, t = _route(dump, namespace, track)
    label = _label(namespace, track)
    if verdict == "refused":
        return verdict, reason, None
    if verdict == "waiting":
        return "refused", f"{reason}, after the baseline routed it from {baseline.upstream}", None
    if upstream != baseline.upstream:
        return "refused", f"{label} is now routed from {upstream}; the baseline was {baseline.upstream}", None
    bindings = [s.binding for s in t.subs]
    if baseline.host not in bindings:
        return "refused", f"{label} no longer has the baseline subscription {baseline.host}: {bindings}", None
    if len(t.subs) > 2:
        return "refused", f"{label} has {len(t.subs)} subscriptions; the run declares 2", None
    inactive = [s.binding for s in t.subs if s.state != "active"]
    if inactive:
        return "refused", f"{label} subscription(s) {inactive} are not active", None
    if len(t.subs) == 1:
        return "waiting", f"{label} has no new subscription beside the baseline {baseline.host}", None
    new = next(b for b in bindings if b != baseline.host)
    return "ready", f"{label} routed from {upstream}, subscriptions {{{baseline.host}, {new}}}", new


def judge_player_audio(dump, namespace, track, host, player):
    """("ready", reason, player) | ("waiting" | "refused", reason, None) in A/V mode, after the video
    proof: the audio `track` has exactly one subscription, active, on the player's video binding
    `player`. The host subscribes only video, so its binding `host` on audio is refused."""
    verdict, reason, _upstream, t = _route(dump, namespace, track)
    label = _label(namespace, track)
    if verdict is not None:
        return verdict, reason, None
    bindings = [s.binding for s in t.subs]
    if host in bindings:
        return "refused", f"{label} has the host's binding {host}; the host subscribes only video", None
    if len(t.subs) > 1:
        return "refused", f"{label} has {len(t.subs)} subscriptions {bindings}; the run declares the player's one", None
    if not t.subs:
        return "waiting", f"{label} has no subscription yet", None
    if bindings[0] != player:
        return "refused", f"{label} is subscribed by binding {bindings[0]}, not the player's video binding {player}", None
    if t.subs[0].state != "active":
        return "waiting", f"{label} subscription {player} is {t.subs[0].state}", None
    return "ready", f"{label}: the player's binding {player} is its one subscription", player


def _wait(event, stop, alive):
    while not event.is_set():
        if stop.is_set() or not alive():
            return False
        event.wait(0.02)
    return True


def route_barrier(namespace, track, host_active, player_started, interval_s=0.25, record=None, audio_track=None):
    """The `go` barrier's staged route probe. Returns (probe, baseline_recorded): `probe` is
    callable(gate, relay, stop) for a thread; `baseline_recorded` is set once the baseline holds,
    and the player may be started only after it. `host_active` and `player_started` are events
    the supervisor sets. Each dump is requested with SIGUSR1 and judged from the first complete
    route block after the request, and a new request is sent only once the last one was answered.
    With this probe as the relay's only SIGUSR1 producer, no block can answer an earlier request;
    the stderr mark cannot tell a block caused by another actor's concurrent signal from ours.
    Every judged dump is appended to `record`. With `audio_track` (the A/V diagnostic), the fact is
    set only once `judge_player_audio` also holds: the audio track's one subscription is the
    player's video binding, never the host's."""
    baseline_recorded = threading.Event()

    def probe(gate, relay, stop):
        def dump():
            mark = len(relay.stderr_lines)
            relay.proc.send_signal(signal.SIGUSR1)
            while not stop.is_set() and relay.alive():
                blocks = complete_blocks(relay.stderr_lines, mark)
                if blocks:
                    return parse(blocks[0])
                stop.wait(0.02)
            return None

        def observe(phase, judge_fn):
            while not stop.is_set() and relay.alive():
                d = dump()
                if d is None:
                    return None
                verdict, reason, value = judge_fn(d)
                if record is not None:
                    record.append({"phase": phase, "verdict": verdict, "reason": reason})
                if verdict == "ready":
                    return value
                if verdict == "refused":
                    gate.refuse(f"relay route dump ({phase}): {reason}")
                    return None
                stop.wait(interval_s)
            return None

        try:
            if not _wait(host_active, stop, relay.alive):
                return
            if player_started.is_set():
                gate.refuse("relay route dump: the player was started before the route baseline")
                return
            base = observe("baseline", lambda d: judge_baseline(d, namespace, track))
            if base is None:
                return
            if player_started.is_set():
                gate.refuse("relay route dump: the player was started before the route baseline was recorded")
                return
            baseline_recorded.set()
            if not _wait(player_started, stop, relay.alive):
                return
            player = observe("after start", lambda d: judge_after_start(d, namespace, track, base))
            if player is None:
                return
            if audio_track is not None and observe(
                    "audio", lambda d: judge_player_audio(d, namespace, audio_track, base.host, player)) is None:
                return
            gate.fact("route_two_subscriptions")
        except RouteDumpError as e:
            gate.refuse(f"relay {e}")
    return probe, baseline_recorded
