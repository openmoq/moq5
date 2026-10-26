"""The driver's short-lived call over the INHERITED owner channel.

It uses the descriptor the launcher passed (named by `MOQ5_PUBBIND_OWNER_FD`).
It never connects by pathname, never substitutes another owner, and never
treats an absent channel as a settled child.

    owner_call.py create   <scope> <key>
    owner_call.py settle   <generation> <scope> <ref>
    owner_call.py state    <ref>
    owner_call.py snapshot | stats
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import owner_wire as W

ALIAS_VAR = "MOQ5_PUBBIND_OWNER_FD"


def channel():
    raw = os.environ.get(ALIAS_VAR)
    if not raw or not raw.isdigit():
        raise W.WireError("no-owner-channel",
                          "no owner channel was inherited (%s unset)"
                          % ALIAS_VAR)
    fd = int(raw)
    try:
        os.fstat(fd)
    except OSError as exc:
        raise W.WireError("no-owner-channel",
                          "the inherited descriptor %d is not usable: %s"
                          % (fd, exc))
    return fd


def request(payload, deadline_s=10.0):
    fd = channel()
    W.write_frame(fd, json.dumps(payload).encode(), deadline_s)
    frame = W.FrameReader(fd, deadline_s).read_frame()
    if frame is None:
        raise W.WireError("channel-broken",
                          "the owner closed the inherited channel")
    return json.loads(frame.decode())


def complete_create(out, scope):
    """Validate the WHOLE reply, not the reply word alone."""
    for field in ("generation", "scope", "ref", "child"):
        if not out.get(field):
            return "the create reply is missing %r" % field
    if out.get("scope") != scope:
        return ("the create reply names scope %r, not the requested %r"
                % (out.get("scope"), scope))
    if out.get("state") != "owned":
        return "the create reply is in state %r, not owned" % out.get("state")
    if out.get("acknowledged") is not True:
        return "the child did not acknowledge its own readiness"
    return None


def main(argv):
    if len(argv) < 2:
        sys.stderr.write("usage: owner_call.py <op> [...]\n")
        return 2
    op = argv[1]
    try:
        if op == "create":
            if len(argv) != 4:
                sys.stderr.write("usage: create <scope> <key>\n")
                return 2
            out = request({"op": "create", "scope": argv[2], "key": argv[3]})
        elif op == "settle":
            if len(argv) != 5:
                sys.stderr.write("usage: settle <generation> <scope> <ref>\n")
                return 2
            out = request({"op": "settle", "gen": argv[2], "scope": argv[3],
                           "ref": argv[4]})
        elif op == "state":
            out = request({"op": "state", "ref": argv[2]})
        elif op in ("snapshot", "stats"):
            out = request({"op": op})
        else:
            sys.stderr.write("unknown op %r\n" % (op,))
            return 2
    except (OSError, W.WireError) as exc:
        sys.stdout.write("refused: %s: %s\n"
                         % (getattr(exc, "code", "no-owner-channel"), exc))
        return 3

    reply = out.get("reply")
    if reply == "refused":
        sys.stdout.write("refused: %s: %s\n"
                         % (out.get("code", "unknown"), out.get("reason", "")))
        return 4
    if op == "create":
        if reply not in ("created", "partial"):
            sys.stdout.write("refused: unexpected-create-reply: %s\n" % reply)
            return 4
        bad = complete_create(out, argv[2])
        if bad is not None:
            sys.stdout.write("refused: incomplete-create-reply: %s\n" % bad)
            return 4
        sys.stdout.write("%s %s %s %s\n" % (out["generation"], out["scope"],
                                            out["ref"], out["child"]))
        return 0
    if op == "settle":
        # The whole reply is validated: the word alone is not the outcome.
        if reply == "retired":
            if out.get("ref") != argv[4] or out.get("status") is None:
                sys.stdout.write("refused: incomplete-settle-reply: %s\n"
                                 % json.dumps(out, sort_keys=True))
                return 4
        elif reply == "retired already":
            if out.get("ref") != argv[4]:
                sys.stdout.write("refused: incomplete-settle-reply: %s\n"
                                 % json.dumps(out, sort_keys=True))
                return 4
        else:
            sys.stdout.write("%s\n" % reply)
            return 5
        sys.stdout.write("%s\n" % reply)
        return 0
    sys.stdout.write("%s\n" % json.dumps(out, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
