"""One owned relay, real peer exchanges, finite watchdogs. No traffic retries."""
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import tempfile
import time


def main():
    relay, peers, cert, key = sys.argv[1:]
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("LD_", "DYLD_"))}
    with tempfile.TemporaryDirectory(prefix="simple-relay-") as temp:
        log = Path(temp) / "relay.log"
        with log.open("wb") as errors:
            child = subprocess.Popen([relay, cert, key, "0"], env=env,
                                     stdout=subprocess.PIPE, stderr=errors)
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(child.stdout, selectors.EVENT_READ)
                    deadline = time.monotonic() + 10
                    line = b""
                    while b"\n" not in line:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0 or not selector.select(remaining):
                            raise RuntimeError("relay readiness deadline")
                        part = os.read(child.stdout.fileno(), 256)
                        if not part or len(line) + len(part) > 256:
                            raise RuntimeError("relay readiness absent/malformed")
                        line += part
                match = re.fullmatch(rb"Listening on 127\.0\.0\.1:([0-9]+)\n", line)
                if not match or not 0 < int(match[1]) <= 65535:
                    raise RuntimeError(f"unexpected readiness: {line!r}")
                # Four protocol pairings, then enough fresh connections to
                # exceed the 16-slot cap cumulatively and exercise retirement.
                pairings = [("16", "16"), ("18", "18"), ("16", "18"), ("18", "16")]
                pairings += [("18", "18")] * 5
                for pub, sub in pairings:
                    result = subprocess.run([peers, match[1].decode(), pub, sub],
                                            env=env, capture_output=True, text=True, timeout=20)
                    print(result.stdout, end="")
                    if result.returncode != 0 or not result.stdout.startswith("PASS:"):
                        raise RuntimeError(f"peer result {result.returncode}: {result.stderr}")
                    if child.poll() is not None:
                        raise RuntimeError("relay exited during campaign")
                child.send_signal(signal.SIGTERM)
                if child.wait(timeout=10) != 0:
                    raise RuntimeError("relay shutdown failed")
                print("PASS: all draft pairs, 18 connections, clean relay shutdown")
            except BaseException:
                print(log.read_text(errors="replace"), file=sys.stderr)
                raise
            finally:
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait(timeout=5)
                child.stdout.close()


if __name__ == "__main__":
    main()
