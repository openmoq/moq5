"""The launcher: it creates the retained owner channel and holds its end.

    socketpair()
      child end  -> the owner endpoint process
      parent end -> RETAINED here for the launcher's lifetime

A consumer never learns a pathname. The launcher passes an explicit alias of
its retained end -- one inherited descriptor, at a fixed number -- to exactly
one consumer at a time, and takes it back when that consumer returns. That is
what makes the connection the authority rather than a discoverable service.
"""
import os
import signal
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import owner_endpoint as E
import owner_wire as W

ALIAS_FD = 9                    # the only descriptor a consumer is given
ALIAS_VAR = "MOQ5_PUBBIND_OWNER_FD"
REAP_S = 20.0
STOP_S = 10.0                   # bound on each stop step for one consumer


class HandoffUnresolved(Exception):
    """The consumer that held the alias could not be accounted for."""


class RetainedOwner(object):
    """Owns the endpoint process and the retained end of its channel."""

    def __init__(self, generation="g1"):
        self.generation = generation
        self.pid = None
        self.channel = None          # the RETAINED parent end
        self.status = None
        self._handed_out = False
        self.unresolved = None       # why the alias may never be handed again
        self.shutdown_receipt = None
        self.last_stop_receipt = None

    def start(self):
        parent, child = socket.socketpair()
        pid = os.fork()
        if pid == 0:
            try:
                parent.close()
                os.close(0)
                E.run(child.fileno(), self.generation)
            except BaseException:
                pass
            finally:
                os._exit(0)
        child.close()
        self.pid, self.channel = pid, parent
        return self

    def alias(self):
        """The explicit alias handed to one consumer at a time.

        Serialization is enforced here rather than assumed: a second consumer
        cannot be given the channel while the first still holds it, so the
        endpoint never has two request/reply readers.
        """
        if self.unresolved is not None:
            raise HandoffUnresolved(
                "the owner channel is unresolved and cannot be handed out "
                "again: %s" % self.unresolved)
        if self._handed_out:
            raise RuntimeError("the owner channel is already handed out; a "
                               "second consumer would race the first")
        self._handed_out = True
        return self.channel.fileno()

    def take_back(self):
        """Recover the alias. Only a consumer that is stopped and accounted
        for may release it, so this is never called from a bare `finally`."""
        self._handed_out = False

    def _stop_and_account(self, proc):
        """Stop one consumer within a finite bound and consume its status.

        Returns a wait receipt, or raises `HandoffUnresolved` when the
        consumer is still not accounted for -- the caller must then keep the
        authority unresolved rather than recover the alias anyway.
        """
        for step in ("terminate", "kill"):
            getattr(proc, step)()
            try:
                proc.communicate(timeout=STOP_S)
            except subprocess.TimeoutExpired:
                continue
            except Exception as exc:
                raise HandoffUnresolved(
                    "waiting for consumer %s after %s failed: %r"
                    % (proc.pid, step, exc))
            return "consumer %s stopped by %s, status %s" % (
                proc.pid, step, proc.returncode)
        raise HandoffUnresolved(
            "consumer %s did not stop within %.0fs of SIGTERM and SIGKILL; "
            "it may still hold the inherited alias" % (proc.pid, STOP_S * 2))

    def run_consumer(self, argv, env=None, timeout=600):
        """Run one consumer with the alias inherited at `ALIAS_FD`."""
        fd = self.alias()
        try:
            merged = dict(os.environ)
            merged.update(env or {})
            merged[ALIAS_VAR] = str(ALIAS_FD)
            merged.pop("MOQ5_PUBBIND_OWNER_SOCKET", None)

            def preexec():
                os.dup2(fd, ALIAS_FD)

            proc = subprocess.Popen(argv, env=merged, preexec_fn=preexec,
                                    pass_fds=(fd,), stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
        except BaseException:
            self.take_back()         # nothing ever inherited the alias
            raise
        try:
            out, err = proc.communicate(timeout=timeout)
        except BaseException as primary:
            try:
                receipt = self._stop_and_account(proc)
            except HandoffUnresolved as unresolved:
                self.unresolved = str(unresolved)
                raise primary from unresolved
            self.take_back()
            self.last_stop_receipt = receipt
            raise primary
        self.take_back()
        return proc.returncode, out.decode(), err.decode()

    def request(self, payload, deadline_s=10.0):
        """A launcher-side request on the retained end.

        The launcher must not become a second reader of a channel it has
        handed out, so this refuses rather than racing the consumer.
        """
        if self._handed_out:
            raise RuntimeError("the owner channel is handed out; a "
                               "launcher-side request would race its reader")
        if self.unresolved is not None:
            raise HandoffUnresolved(
                "the owner channel is unresolved: %s" % self.unresolved)
        import json
        fd = self.channel.fileno()
        W.write_frame(fd, json.dumps(payload).encode(), deadline_s)
        frame = W.FrameReader(fd, deadline_s).read_frame()
        if frame is None:
            raise W.WireError("channel-broken",
                              "the owner closed the retained channel")
        return json.loads(frame.decode())

    def release(self):
        """Bounded shutdown, then reap the owner. Nothing outlives it."""
        if self.pid is None:
            return None
        if self._handed_out or self.unresolved is not None:
            # A graceful shutdown would write into a channel another reader
            # may still hold, and its worker receipts would be unavailable.
            self.shutdown_receipt = "unavailable: %s" % (
                self.unresolved or "the alias is still handed out")
        else:
            try:
                self.shutdown_receipt = self.request({"op": "shutdown"})
            except Exception as exc:
                self.shutdown_receipt = "unavailable: %r" % (exc,)
        try:
            self.channel.close()
        except OSError:
            pass
        deadline = time.monotonic() + REAP_S
        while time.monotonic() < deadline:
            got, raw = os.waitpid(self.pid, os.WNOHANG)
            if got == self.pid:
                self.status = raw
                break
            time.sleep(0.02)
        if self.status is None:
            os.kill(self.pid, signal.SIGKILL)
            _got, raw = os.waitpid(self.pid, 0)
            self.status = raw
        self.pid = None
        return self.status

    def __enter__(self):
        return self.start()

    def __exit__(self, *_exc):
        self.release()
        return False
