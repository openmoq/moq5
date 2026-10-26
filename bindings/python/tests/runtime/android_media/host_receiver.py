"""The supervised Android run's host receiver: the published moq5 package,
file-backed expectations, and the four positive controls.

    python -I host_receiver.py --package <installed moq5 root> --url moqt://127.0.0.1:<port> \\
        --ca <ca.pem> --sni <name> --objects <objects.bin> --establish-s <s> --deadline-s <s>

Every flag is required and nothing else is accepted: no insecure mode, no CA
fallback. Before any connection it checks, by name: a loopback raw `moqt://`
listener (127.0.0.1, localhost or [::1], port 1..65535, no path, query,
fragment or userinfo), the CA file, a non-empty SNI, an objects file holding
exactly the recipe's frame count, finite positive bounds within the run's
545 s bound, and a moq5 package imported from inside the given root. Its
stdout carries the `CTRL` lines only; everything else, including anything the
package prints, goes to stderr. A named failure exits 1 after owed cleanup,
a refused argument or input exits 2, and success exits 0.

It attaches one receiver for the namespace `live/cam1`, subscribes the track
named `video` when the catalog announces it, and emits `CTRL <word>` lines,
each once and in this order, only on these public facts:

    go              receiver.track_state(video) is ACTIVE (not discovery, not the subscribe call)
    ack-all         exactly 60 NORMAL media objects on `video`, equal in order to the objects
                    file (payload bytes, keyframe, presentation microseconds); objects on any
                    other track and status-only objects never count
    ended-observed  a polled TRACK_ENDED event for `video`
    done            a polled TRACK_REMOVED for `video` while stats().catalog_complete is True

The expectation is only `media_oracle.load_objects(<objects.bin>)`; nothing is
inferred from a quiet queue or from what the publisher reports. Every wait is
bounded; a terminal receiver (closed or fatal), a deadline, or a media
mismatch is a named failure. Every owed close, receiver then endpoint, is
attempted exactly once on every path; interrupts propagate, never as a
receipt (see `open_and_run`).
"""
import json
import math
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import media_oracle          # noqa: E402

NAMESPACE = (b"live", b"cam1")
RUN_BOUND_S = 545.0          # the whole supervised run's bound: no child bound may exceed it
LOOPBACK_HOSTS = ("127.0.0.1", "localhost", "::1")
TRACK = b"video"
CONTROLS = ("go", "ack-all", "ended-observed", "done")


class HostReceiver:
    """One receiver run against an expectation list. `moq5` is the package (or
    an object with the same public names), `receiver` the attached Receiver,
    `emit(word)` delivers a control, `clock()` is monotonic seconds."""

    def __init__(self, moq5, receiver, expected, emit, clock, deadline_s, wait_us=250_000):
        self.moq5, self.rx, self.expected, self.emit = moq5, receiver, list(expected), emit
        self.clock, self.deadline_s, self.wait_us = clock, deadline_s, wait_us

    def run(self):
        """Drive the receiver to `done` or a named failure. Returns the receipt:
        {"controls": [...], "problems": [...], "failure": str | None, ...}."""
        m, rx = self.moq5, self.rx
        sent, received = [], []
        video, ended, removed = None, False, False
        deadline = self.clock() + self.deadline_s

        def receipt(failure, problems=()):
            return {"controls": list(sent), "problems": list(problems), "failure": failure, "received": len(received)}

        def pending():
            return CONTROLS[len(sent)] if len(sent) < len(CONTROLS) else None

        while True:
            while True:
                ev = rx.poll_track()
                if isinstance(ev, m.PollOutcome):
                    break
                if ev.kind == m.TrackEventKind.ADDED and video is None and ev.description is not None \
                        and ev.description.name == TRACK:
                    video = ev.track
                    rx.subscribe(video)
                elif ev.kind == m.TrackEventKind.ENDED and video is not None and ev.track == video:
                    ended = True
                elif ev.kind == m.TrackEventKind.REMOVED and video is not None and ev.track == video:
                    removed = True
            while True:
                obj = rx.poll_object()
                if isinstance(obj, m.PollOutcome):
                    break
                if video is not None and obj.track == video and not obj.is_status_only:
                    received.append(media_oracle.received_from(obj))
            if "go" not in sent and video is not None and rx.track_state(video) == m.TrackState.ACTIVE:
                sent.append("go"); self.emit("go")
            if pending() == "ack-all":
                if len(received) == len(self.expected):
                    problems = media_oracle.compare(self.expected, received)
                    if problems:
                        return receipt("ack-all: the selected track's media does not match the objects file", problems)
                    sent.append("ack-all"); self.emit("ack-all")
                elif len(received) > len(self.expected):
                    return receipt("ack-all: the selected track's media does not match the objects file",
                                   media_oracle.compare(self.expected, received))
            elif len(sent) > 1 and len(received) != len(self.expected):
                return receipt(f"{pending()}: media after ack-all", media_oracle.compare(self.expected, received))
            if pending() == "ended-observed" and ended:
                sent.append("ended-observed"); self.emit("ended-observed")
            if pending() == "done" and removed and rx.stats().catalog_complete is True:
                sent.append("done"); self.emit("done")
                return receipt(None)
            # fatal and closed are distinct facts: a fatal receiver whose endpoint is not closed (and
            # whose object polls may be INTERRUPTED, so never drained) is still terminal for this run.
            t = rx.terminal
            if rx.drained() or t.closed or t.fatal:
                return receipt(f"{pending()}: receiver terminal (closed {t.closed}, fatal {t.fatal}, fatal_code {t.fatal_code})"
                               f" before {self._awaited(pending())}", media_oracle.compare(self.expected, received))
            left = deadline - self.clock()
            if left <= 0:
                problems = media_oracle.compare(self.expected, received) if pending() == "ack-all" else []
                what = {"go": f"the selected track was not ACTIVE",
                        "ack-all": f"{len(received)} selected media objects did not match the objects file",
                        "ended-observed": "the selected track's TRACK_ENDED was not observed",
                        "done": f"the selected track's REMOVED with catalog isComplete was not observed "
                                f"(removed {removed}, complete {rx.stats().catalog_complete})"}[pending()]
                return receipt(f"{pending()}: {what} within {self.deadline_s}s", problems)
            rx.wait(min(self.wait_us, max(1, int(left * 1e6))))

    @staticmethod
    def _awaited(control):
        return {"go": "ACTIVE", "ack-all": "the 60 objects", "ended-observed": "the selected TRACK_ENDED",
                "done": "REMOVED with catalog isComplete"}[control]


def open_and_run(moq5, connect, attach, expected, emit, clock, deadline_s, wait_us=250_000):
    """Own one endpoint and one receiver: `connect()` returns the endpoint and
    `attach(endpoint)` the receiver attached to it. Ownership is taken one
    resource at a time, so an attach refusal still owes the endpoint's close.

    Every owed close is ATTEMPTED exactly once, receiver before endpoint, even
    when an earlier close raised. The receipt records `close_attempts`,
    `closed` (successful closes only) and `close_errors`. A named failure from
    the run, or an attach/connect refusal, stays the receipt's failure; a close
    error only names the failure when nothing else failed. An exception raised
    by the run is re-raised, grouped with any close errors
    (`BaseExceptionGroup`) so neither is lost."""
    attempts, closed, close_errors = [], [], []

    def refusal(failure, cause):
        return {"controls": [], "problems": [], "failure": failure, "received": 0, "errors": [cause]}

    def close(name, resource):
        # BaseException, like the public context managers: an interrupt raised
        # by one close must not skip the next owed close.
        attempts.append(name)
        try:
            resource.close()
        except BaseException as failed:
            close_errors.append((name, failed))
        else:
            closed.append(name)

    def interrupted():
        """Close failures that are not ordinary errors are never a receipt."""
        return any(not isinstance(failed, Exception) for _name, failed in close_errors)

    def reraise(primary, message):
        causes = [failed for _name, failed in close_errors]
        if primary is None and len(causes) == 1:
            raise causes[0]
        raise BaseExceptionGroup(message, ([primary] if primary is not None else []) + causes) from None

    def receipt(result):
        result.update(close_attempts=attempts, closed=closed, close_errors=close_errors)
        return result

    try:
        endpoint = connect()
    except Exception as refused:
        return receipt(refusal(f"connect: {refused!r}", refused))
    try:
        receiver = attach(endpoint)
    except BaseException as refused:
        close("endpoint", endpoint)
        # An interrupt, from attach or from the endpoint close, is never a receipt.
        if not isinstance(refused, Exception) or interrupted():
            if close_errors:
                reraise(refused, "host receiver attach and endpoint close both failed")
            raise
        return receipt(refusal(f"attach: {refused!r}", refused))
    try:
        result = HostReceiver(moq5, receiver, expected, emit, clock, deadline_s, wait_us).run()
    except BaseException as raised:
        close("receiver", receiver)
        close("endpoint", endpoint)
        if close_errors:
            raise BaseExceptionGroup("host receiver run and close both failed",
                                     [raised, *(failed for _name, failed in close_errors)]) from None
        raise
    close("receiver", receiver)
    close("endpoint", endpoint)
    if interrupted():
        reraise(None, "host receiver close failed")
    result["errors"] = []
    if result["failure"] is None and close_errors:
        name, failed = close_errors[0]
        result["failure"] = f"close: {name} close raised {failed!r}"
    return receipt(result)


FLAGS = ("--package", "--url", "--ca", "--sni", "--objects", "--establish-s", "--deadline-s")


class Refused(Exception):
    """A named refusal before any connection is attempted."""


def declared_objects():
    """The run's object count, from the recipe that generated the media: the single authority."""
    return json.loads((HERE / "marker_recipe.json").read_text())["source"]["frames"]


def check_url(url):
    """A loopback raw listener URL with a valid port and nothing else; returns it unchanged."""
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError as bad:
        raise Refused(f"url: {url!r}: {bad}") from None
    if parts.scheme != "moqt" or parts.hostname not in LOOPBACK_HOSTS or port is None or not 1 <= port <= 65535 \
            or parts.path or parts.query or parts.fragment or parts.username is not None or parts.password is not None:
        raise Refused(f"url: {url!r} is not a bare loopback moqt:// listener with a port in 1..65535")
    return url


def check_bounds(establish_s, deadline_s):
    """Finite, positive, and together within the run bound."""
    for name, value in (("establish-s", establish_s), ("deadline-s", deadline_s)):
        if not (isinstance(value, float) and math.isfinite(value) and value > 0):
            raise Refused(f"{name}: {value!r} is not a finite positive number of seconds")
        if value > RUN_BOUND_S:
            raise Refused(f"{name}: {value:g}s exceeds the run bound {RUN_BOUND_S:g}s")
    if establish_s + deadline_s > RUN_BOUND_S:
        raise Refused(f"establish-s + deadline-s: {establish_s + deadline_s}s exceeds the run bound {RUN_BOUND_S:g}s")


def parse(argv):
    """Exactly the seven flags, each once, each with a value; nothing else."""
    values, rest = {}, list(argv)
    while rest:
        flag = rest.pop(0)
        if flag not in FLAGS:
            raise Refused(f"unknown argument {flag!r}")
        if flag in values:
            raise Refused(f"{flag} given twice")
        if not rest:
            raise Refused(f"{flag} has no value")
        values[flag] = rest.pop(0)
    missing = [f for f in FLAGS if f not in values]
    if missing:
        raise Refused("missing " + ", ".join(missing))
    return values


def seconds(values, flag):
    try:
        return float(values[flag])
    except ValueError:
        raise Refused(f"{flag.lstrip('-')}: not a number: {values[flag]!r}") from None


def load_package(root):
    """Imports moq5 from exactly `root`; refuses if it resolves anywhere else. Only the root is
    resolved: a `moq5` entry that is itself a link to another place is not this package."""
    root = Path(root)
    if not (root / "moq5" / "__init__.py").is_file():
        raise Refused(f"package: no moq5 package under {str(root)!r}")
    sys.path.insert(0, str(root))
    import moq5
    here, inside = Path(moq5.__file__).resolve(), root.resolve() / "moq5"
    if here.parent != inside:
        raise Refused(f"package: moq5 resolved to {str(here)!r}, not under {str(inside)!r}")
    return moq5


def main(argv):
    def refuse(message, code=2):
        print(f"host_receiver: {message}", file=sys.stderr)
        return code
    try:
        values = parse(argv[1:])
        url = check_url(values["--url"])
        ca = values["--ca"]
        if not Path(ca).is_file():
            raise Refused(f"ca: no CA file at {ca!r}")
        sni = values["--sni"]
        if not sni:
            raise Refused("sni: empty")
        establish_s, deadline_s = seconds(values, "--establish-s"), seconds(values, "--deadline-s")
        check_bounds(establish_s, deadline_s)
        try:
            expected = media_oracle.load_objects(values["--objects"])
        except (OSError, ValueError) as bad:
            raise Refused(f"objects: {bad}") from None
        if len(expected) != declared_objects():
            raise Refused(f"objects: {len(expected)} records, the recipe declares {declared_objects()}")
    except Refused as refused:
        return refuse(str(refused))

    # From here stdout is the control stream only: fd 1 is pointed at stderr, so anything printed --
    # by this process or by the package -- lands there, and controls go to a private duplicate.
    sys.stdout.flush()
    control, saved = os.dup(1), os.dup(1)
    os.dup2(2, 1)
    try:
        try:
            moq5 = load_package(values["--package"])
        except Refused as refused:
            return refuse(str(refused))

        def emit(word):
            os.write(control, f"CTRL {word}\n".encode())

        def connect():
            return moq5.Endpoint.connect(moq5.EndpointConfig(
                url=url, protocol=moq5.Protocol.RAW_QUIC, backend=moq5.Backend.PICOQUIC,
                versions=(moq5.Version.DRAFT_18,), sni=sni, ca_file=ca))

        def attach(endpoint):
            # The endpoint is owned here: a bounded establishment wait that fails is an attach
            # refusal, so open_and_run still closes it.
            start = time.monotonic()
            while endpoint.state != moq5.EndpointState.ESTABLISHED:
                reason = endpoint.terminal.reason
                if reason != moq5.TerminalReason.NONE:
                    raise RuntimeError(f"endpoint terminal ({getattr(reason, 'name', reason)}) before establishment")
                left = start + establish_s - time.monotonic()
                if left <= 0:
                    raise RuntimeError(f"endpoint not established within {establish_s}s")
                endpoint.wait(max(1, int(min(left, 0.25) * 1e6)))
            return moq5.Receiver.attach(endpoint, moq5.ReceiverConfig.flow_control(NAMESPACE, auto_subscribe=False))

        receipt = open_and_run(moq5, connect, attach, expected, emit, time.monotonic, deadline_s)
        for name, failed in receipt["close_errors"]:
            print(f"host_receiver: {name} close raised {failed!r}", file=sys.stderr)
        if receipt["failure"] is not None:
            for problem in receipt["problems"]:
                print(f"host_receiver:   {problem}", file=sys.stderr)
            return refuse(receipt["failure"], 1)
        return 0
    finally:
        sys.stdout.flush()
        os.dup2(saved, 1)
        os.close(saved)
        os.close(control)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
