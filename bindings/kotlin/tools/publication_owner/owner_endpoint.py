"""The retained publication owner, served over an inherited socketpair.

The launcher creates the socketpair, forks this owner onto one end and RETAINS
the other for its own lifetime. There is no pathname to reconnect to: a caller
either inherits the launcher's alias or has no owner at all.

It forks each fixture worker itself and keeps it unreaped, so its identity
cannot be reassigned before the owner consumes the wait status. Settlement is
`kill` then `waitpid` serialized through the retained slot; a consumer never
holds a bare number it may signal.

Carrier identity is the full tuple `(generation, scope, ref)`. The ref is an
opaque owner-minted token: it embeds neither generation, scope, pid nor the
caller's key, and it is looked up byte-for-byte.
"""
import json
import os
import signal
import socket
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import owner_wire as W

N_LIVE = 8
N_TOTAL = 256
MAX_GENERATION = 32
MAX_SCOPE = 64
MAX_REF = 32
MAX_KEY = 128
TOKEN_CHARS = set("abcdefghijklmnopqrstuvwxyz"
                  "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")

RESERVED, OWNED, RETIRED, QUARANTINED, FAILED = (
    "reserved", "owned", "retired", "quarantined", "failed")
LIVE_STATES = (RESERVED, OWNED, QUARANTINED)

PHASE_S = 5.0
POLL_S = 0.02


def legal_token(value, bound):
    return (isinstance(value, str) and 0 < len(value) <= bound
            and all(c in TOKEN_CHARS for c in value))


ACK_READY = b"READY\n"
ACK_PAST_EOF = b"PASTEOF\n"


def _worker(lifetime_r, lifetime_w, ack_r, ack_w, keep_open, stubborn):
    """The fixture worker.

    It holds ONLY its own lifetime reader: every other descriptor the owner
    held -- the launcher's channel end and any other worker's lifetime writer --
    is closed here, so no worker can keep another's channel alive.
    """
    try:
        os.close(lifetime_w)
        os.close(ack_r)
        for fd in keep_open:
            try:
                os.close(fd)
            except OSError:
                pass
        if stubborn:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        os.write(ack_w, ACK_READY)
        while True:
            if os.read(lifetime_r, 1) == b"":
                break
        os.write(ack_w, ACK_PAST_EOF)
        if not stubborn:
            os._exit(0)
        while True:
            try:
                os.read(lifetime_r, 1)
            except OSError:
                pass
    except BaseException:
        pass
    finally:
        os._exit(127)


class Slot(object):
    def __init__(self, ref, scope, key):
        self.ref, self.scope, self.key = ref, scope, key
        self.state = RESERVED
        self.pid = None
        self.status = None
        self.reason = None
        self.signals = []
        self.lifetime_w = None
        self.ack_r = None
        self.acknowledged = False


class Owner(object):
    def __init__(self, generation, n_live=N_LIVE, n_total=N_TOTAL):
        self.generation = generation
        self.n_live, self.n_total = n_live, n_total
        self.slots, self.by_key, self.replies = {}, {}, {}
        self.minted, self.forks = 0, []
        self.closing = False
        self.channel_fd = None
        self._entropy = "%012x" % (os.getpid() & 0xffffffffffff)

    @property
    def live(self):
        return dict((r, s.pid) for r, s in self.slots.items()
                    if s.state in LIVE_STATES)

    def _mint(self):
        token = "r%04x%s" % (self.minted, self._entropy)
        self.minted += 1
        if not legal_token(token, MAX_REF):
            raise RuntimeError("minted a token outside the signed grammar")
        return token

    def create(self, scope, key, stubborn=True):
        if self.closing:
            return {"reply": "refused", "code": "owner-closing",
                    "reason": "the owner is shutting down"}
        if not legal_token(key, MAX_KEY):
            return {"reply": "refused", "code": "key-invalid",
                    "reason": "the idempotency key is missing or over-bound"}
        if not legal_token(scope, MAX_SCOPE):
            return {"reply": "refused", "code": "scope-invalid",
                    "reason": "the scope is outside the signed grammar or its "
                              "%d-byte bound" % MAX_SCOPE}
        if (scope, key) in self.by_key:
            ref = self.by_key[(scope, key)]
            recorded = self.replies.get(ref)
            if recorded is None:
                slot = self.slots[ref]
                return {"reply": "partial", "generation": self.generation,
                        "scope": slot.scope, "ref": ref, "state": slot.state,
                        "reason": "the outcome of this create was not recorded"}
            return dict(recorded)
        if len(self.live) >= self.n_live:
            return {"reply": "refused", "code": "live-exhausted",
                    "reason": "%d live children is the limit" % self.n_live}
        if self.minted >= self.n_total:
            return {"reply": "refused", "code": "total-exhausted",
                    "reason": "%d refs minted is the total" % self.n_total}

        ref = self._mint()
        slot = Slot(ref, scope, key)
        self.slots[ref] = slot
        self.by_key[(scope, key)] = ref

        lifetime_r, lifetime_w = os.pipe()
        ack_r, ack_w = os.pipe()
        # Every descriptor this owner already holds, so the new worker inherits
        # none of them: not the launcher's channel, not a sibling's lifetime.
        inherited = [fd for fd in self.held_descriptors()
                     if fd not in (lifetime_r, ack_w)]
        try:
            pid = os.fork()
        except OSError as exc:
            os.close(lifetime_r)
            os.close(lifetime_w)
            os.close(ack_r)
            os.close(ack_w)
            slot.state = FAILED
            reply = {"reply": "refused", "code": "fork-failed",
                     "reason": "fork failed: %s" % exc}
            self.replies[ref] = reply
            return dict(reply)
        if pid == 0:
            _worker(lifetime_r, lifetime_w, ack_r, ack_w, inherited, stubborn)
        slot.pid, slot.state = pid, OWNED
        self.forks.append(pid)
        os.close(lifetime_r)
        os.close(ack_w)
        slot.lifetime_w = lifetime_w
        slot.ack_r = ack_r
        slot.acknowledged = self._await_ack(slot)
        reply = {"reply": "created", "generation": self.generation,
                 "scope": scope, "ref": ref, "child": str(pid),
                 "state": OWNED, "acknowledged": slot.acknowledged}
        self.replies[ref] = reply
        return dict(reply)

    def settle(self, generation, scope, ref):
        if generation != self.generation:
            return {"reply": "refused", "code": "generation-mismatch",
                    "reason": "the request names generation %r, not %r"
                              % (generation, self.generation)}
        slot = self.slots.get(ref)
        if slot is None:
            return {"reply": "refused", "code": "unknown-ref",
                    "reason": "the owner does not know %r" % (ref,)}
        if scope is not None and scope != slot.scope:
            return {"reply": "refused", "code": "scope-mismatch",
                    "reason": "the request names scope %r, not %r"
                              % (scope, slot.scope)}
        if slot.state == RETIRED:
            return {"reply": "retired already", "ref": ref,
                    "status": slot.status}
        if slot.state == QUARANTINED:
            return {"reply": "unresolved", "ref": ref, "reason": slot.reason}
        if slot.pid is None:
            slot.state = QUARANTINED
            slot.reason = "the slot holds no child identity"
            return {"reply": "unresolved", "ref": ref, "reason": slot.reason}
        self._close_lifetime(slot)
        for sig, name in ((signal.SIGTERM, "TERM"), (signal.SIGKILL, "KILL")):
            deadline = time.monotonic() + PHASE_S
            try:
                os.kill(slot.pid, sig)
                slot.signals.append(name)
            except OSError as exc:
                slot.state = QUARANTINED
                slot.reason = "the %s signal failed: %s" % (name, exc)
                return {"reply": "unresolved", "ref": ref,
                        "reason": slot.reason}
            while time.monotonic() < deadline:
                try:
                    got, raw = os.waitpid(slot.pid, os.WNOHANG)
                except ChildProcessError as exc:
                    slot.state = QUARANTINED
                    slot.reason = "lost wait authority: %s" % exc
                    return {"reply": "unresolved", "ref": ref,
                            "reason": slot.reason}
                if got == 0:
                    time.sleep(POLL_S)
                    continue
                if got != slot.pid or not (os.WIFEXITED(raw)
                                           or os.WIFSIGNALED(raw)):
                    slot.state = QUARANTINED
                    slot.reason = "unusable wait result %r/%r" % (got, raw)
                    return {"reply": "unresolved", "ref": ref,
                            "reason": slot.reason}
                slot.status, slot.state = raw, RETIRED
                return {"reply": "retired", "ref": ref, "child": str(slot.pid),
                        "status": raw, "signals": list(slot.signals)}
        slot.state = QUARANTINED
        slot.reason = "the bounded deadline expired"
        return {"reply": "unresolved", "ref": ref, "reason": slot.reason}

    def held_descriptors(self):
        """Descriptors this owner holds that a child must never inherit."""
        held = []
        if self.channel_fd is not None:
            held.append(self.channel_fd)
        for slot in self.slots.values():
            if slot.lifetime_w is not None:
                held.append(slot.lifetime_w)
            if slot.ack_r is not None:
                held.append(slot.ack_r)
        return held

    def _await_ack(self, slot, deadline_s=10.0):
        """The worker's own READY, under a real bound."""
        import select
        deadline = time.monotonic() + deadline_s
        seen = b""
        while len(seen) < len(ACK_READY):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            ready, _w, _x = select.select([slot.ack_r], [], [], remaining)
            if not ready:
                return False
            chunk = os.read(slot.ack_r, len(ACK_READY) - len(seen))
            if chunk == b"":
                return False
            seen += chunk
        return seen == ACK_READY

    def _close_lifetime(self, slot):
        if slot.lifetime_w is not None:
            os.close(slot.lifetime_w)
            slot.lifetime_w = None
        if slot.ack_r is not None:
            os.close(slot.ack_r)
            slot.ack_r = None

    def state_of(self, ref):
        slot = self.slots.get(ref)
        if slot is None:
            return {"reply": "state", "known": False}
        return {"reply": "state", "known": True, "state": slot.state,
                "scope": slot.scope, "holds_pid": slot.pid is not None}

    def snapshot(self):
        return {"reply": "snapshot", "generation": self.generation,
                "live": sorted(self.live),
                "retired": sorted(r for r, s in self.slots.items()
                                  if s.state == RETIRED),
                "complete": True}

    def stats(self):
        return {"reply": "stats", "minted": self.minted,
                "forks": len(self.forks), "live": len(self.live),
                "retired": len([s for s in self.slots.values()
                                if s.state == RETIRED])}

    def shutdown(self):
        self.closing = True
        settled = {}
        for ref in list(self.slots):
            if self.slots[ref].state in LIVE_STATES:
                settled[ref] = self.settle(self.generation, None,
                                           ref)["reply"]
        for slot in self.slots.values():
            self._close_lifetime(slot)
        return {"reply": "shutdown", "settled": settled,
                "forks": len(self.forks)}


def serve(channel_fd, owner):
    """Framed request/reply on the RETAINED channel. Returns True on shutdown.

    There is one reader at a time by construction: the launcher passes an alias
    of this channel to exactly one consumer at a time.
    """
    owner.channel_fd = channel_fd
    reader = W.FrameReader(channel_fd)
    stopping = False
    while True:
        try:
            # Idle is not an error: the owner waits for its next request for as
            # long as the launcher holds the channel. Only a frame already in
            # flight is under a deadline.
            frame = reader.read_frame(idle_wait=True)
        except W.WireError as exc:
            if exc.code in ("channel-broken", "read-deadline"):
                break                       # the launcher's end is gone
            W.write_frame(channel_fd,
                          json.dumps({"reply": "refused", "code": exc.code,
                                      "reason": str(exc)}).encode())
            continue
        if frame is None:
            break
        try:
            request = json.loads(frame.decode())
            verb = request["op"]
        except Exception as exc:
            W.write_frame(channel_fd,
                          json.dumps({"reply": "refused",
                                      "code": "request-malformed",
                                      "reason": "unreadable request: %s"
                                                % exc}).encode())
            continue
        if verb == "create":
            out = owner.create(request.get("scope"), request.get("key"),
                               request.get("stubborn", True))
        elif verb == "settle":
            out = owner.settle(request.get("gen"), request.get("scope"),
                               request.get("ref"))
        elif verb == "state":
            out = owner.state_of(request.get("ref"))
        elif verb == "snapshot":
            out = owner.snapshot()
        elif verb == "stats":
            out = owner.stats()
        elif verb == "shutdown":
            out = owner.shutdown()
        else:
            out = {"reply": "refused", "code": "unknown-op",
                   "reason": "unknown op %r" % (verb,)}
        W.write_frame(channel_fd, json.dumps(out).encode())
        if verb == "shutdown":
            stopping = True
            break
    return stopping


def run(channel_fd, generation, n_live=N_LIVE, n_total=N_TOTAL):
    """Own children on `channel_fd` until the launcher's end closes."""
    if not legal_token(generation, MAX_GENERATION):
        raise SystemExit("the generation is outside the signed grammar")
    owner = Owner(generation, n_live, n_total)
    try:
        serve(channel_fd, owner)
    finally:
        owner.shutdown()
    return 0


def main(argv):
    """owner_endpoint.py <channel-fd> <generation>

    Serves the owner on an ALREADY INHERITED descriptor. There is deliberately
    no listening path: an owner cannot be discovered, only inherited.
    """
    if len(argv) != 3:
        sys.stderr.write("usage: owner_endpoint.py <channel-fd> <generation>\n")
        return 2
    return run(int(argv[1]), argv[2])


if __name__ == "__main__":
    sys.exit(main(sys.argv))
