"""Length-prefixed framing for the publication owner's request/reply channel.

`<decimal length> <exactly that many bytes>`. Complete reads and writes, an
explicit maximum, and refusal rather than unbounded buffering.
"""
import errno
import os
import select
import time

MAX_PAYLOAD = 4096
MAX_LEN_DIGITS = len(str(MAX_PAYLOAD))
MAX_FRAME = MAX_LEN_DIGITS + 1 + MAX_PAYLOAD


class WireError(Exception):
    def __init__(self, code, message):
        Exception.__init__(self, "[%s] %s" % (code, message))
        self.code = code


def set_nonblocking(fd):
    """A deadline can only bound I/O that does not block indefinitely."""
    import fcntl
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    return fd


def write_frame(fd, payload, deadline_s=10.0):
    """Write one complete frame under a real deadline.

    The descriptor is non-blocking and readiness is awaited with `select`, so a
    blocked or vanished peer ends the write rather than parking in it.
    """
    if len(payload) > MAX_PAYLOAD:
        raise WireError("frame-too-large",
                        "%d bytes exceeds the %d-byte maximum"
                        % (len(payload), MAX_PAYLOAD))
    set_nonblocking(fd)
    buf = ("%d " % len(payload)).encode() + payload
    deadline = time.monotonic() + deadline_s
    while buf:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise WireError("write-deadline",
                            "the bounded write deadline expired with %d byte(s) "
                            "unsent" % len(buf))
        _r, ready, _x = select.select([], [fd], [], remaining)
        if not ready:
            continue
        try:
            written = os.write(fd, buf)
        except OSError as exc:
            if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                continue
            raise WireError("channel-broken", "the channel failed: %s" % exc)
        buf = buf[written:]
    return len(payload)


class FrameReader(object):
    """One frame at a time; nothing beyond a declared length is buffered."""

    def __init__(self, fd, deadline_s=10.0):
        self.fd = set_nonblocking(fd)
        self.deadline_s = deadline_s
        self.buf = b""

    def _fill(self, deadline):
        """Wait for readability, then read what is there.

        `deadline` of None is an IDLE wait: nothing is in flight, so there is
        nothing to time out -- an owner that has simply not been asked anything
        yet must not decide its channel is gone. Once any byte of a frame has
        arrived the caller passes a real deadline, and from then on the
        descriptor is non-blocking and readiness is awaited with `select`, so a
        peer that stops mid-frame expires that deadline instead of blocking.
        """
        if deadline is None:
            select.select([self.fd], [], [], None)
        else:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise WireError("read-deadline",
                                "the bounded read deadline expired holding "
                                "%d byte(s)" % len(self.buf))
            ready, _w, _x = select.select([self.fd], [], [], remaining)
            if not ready:
                raise WireError("read-deadline",
                                "the owner did not reply within the bounded "
                                "deadline (%d byte(s) held)" % len(self.buf))
        try:
            chunk = os.read(self.fd, 4096)
        except OSError as exc:
            if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                return True
            raise WireError("channel-broken", "the channel failed: %s" % exc)
        if chunk == b"":
            return False
        self.buf += chunk
        return True

    def read_frame(self, idle_wait=False):
        """One frame, or None at a clean EOF between frames.

        `idle_wait` is for a server waiting for its next request: the wait for
        the FIRST byte is unbounded, and the frame's own deadline starts only
        once part of it has arrived.
        """
        deadline = None if (idle_wait and not self.buf) \
            else time.monotonic() + self.deadline_s
        while b" " not in self.buf:
            if len(self.buf) > MAX_LEN_DIGITS:
                raise WireError("frame-too-large",
                                "the length prefix is over-long: %r"
                                % self.buf[:16])
            if not self._fill(deadline):
                if not self.buf:
                    return None
                raise WireError("channel-broken",
                                "EOF inside a length prefix: %r" % self.buf)
            if deadline is None and self.buf:
                deadline = time.monotonic() + self.deadline_s
        head, _sep, rest = self.buf.partition(b" ")
        if not head.isdigit():
            raise WireError("frame-malformed",
                            "no decimal length prefix: %r" % head[:16])
        want = int(head)
        if want > MAX_PAYLOAD:
            raise WireError("frame-too-large",
                            "declared %d bytes, maximum is %d"
                            % (want, MAX_PAYLOAD))
        while len(rest) < want:
            self.buf = rest
            if not self._fill(deadline):
                raise WireError("channel-broken",
                                "EOF inside a payload: %d of %d byte(s)"
                                % (len(self.buf), want))
            rest = self.buf
        self.buf = rest[want:]
        return rest[:want]
