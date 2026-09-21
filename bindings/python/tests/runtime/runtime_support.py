"""Bounded ownership/diagnostic mechanics for the runtime fixtures.

Every child process is a direct Popen of the supervisor, retained as a
handle through termination/wait: no kill authority is ever recovered from
a PID file or a process name. Every command result keeps argv, rc, stdout,
stderr and timeout; one diagnostic rule applies to all of them (any
"warning" line, whatever the status, no exemption). Reader threads record
exceptions and close their pipes; settlement records exit, how, and whether
the readers settled. Stdlib only.
"""
import json, os, re, signal, socket, stat, subprocess, threading, time
from pathlib import Path

OPENSSL = "/opt/homebrew/Caskroom/miniconda/base/bin/openssl"
MSQUIC_DIR = "/Users/jekyll/Projects/MoQ/LibMoQ/msquic/build-b632469/bin/Release"
EXPECTED_ALPN = "moqt-18+moqt-16"
READY_S, STOP_S, JOIN_S, CMD_S = 10.0, 5.0, 3.0, 30.0
CONTROL_MAX, CONTROL_LIMIT = 16, 4        # the finite control protocol: <= 4 lines of <= 16 bytes


class ControlProtocol:
    """The finite four-message control protocol the client may send to the
    publisher, admitted in declared order only. Unknown, repeated, overlong
    or out-of-order lines are rejected (recorded) and never forwarded."""

    def __init__(self, sequence):
        self.sequence = list(sequence)
        self.accepted, self.rejected = [], []

    def admit(self, line):
        why = None
        if len(line.encode()) > CONTROL_MAX:
            why = "overlong"
        elif line not in self.sequence:
            why = "unknown"
        elif line in self.accepted:
            why = "repeated"
        elif len(self.accepted) >= CONTROL_LIMIT or self.sequence[len(self.accepted)] != line:
            why = "out-of-order"
        if why:
            self.rejected.append({"line": line[:64], "why": why})
            return False
        self.accepted.append(line)
        return True
WARN = re.compile(r"warning", re.IGNORECASE)


def _text(v):
    """Captured streams are text at this boundary: TimeoutExpired carries
    bytes even under text=True; partial output is preserved."""
    if v is None:
        return ""
    return v.decode("utf-8", errors="replace") if isinstance(v, (bytes, bytearray)) else v


def cmd(argv, timeout=CMD_S, **kw):
    """One bounded command. On timeout subprocess.run has killed and reaped
    the child; the partial streams are retained as text."""
    try:
        r = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, **kw)
        return {"argv": argv, "rc": r.returncode, "stdout": r.stdout, "stderr": r.stderr, "timeout": False}
    except subprocess.TimeoutExpired as t:
        return {"argv": argv, "rc": None, "stdout": _text(t.stdout), "stderr": _text(t.stderr), "timeout": True}


def collect_diagnostics(r):
    """Every 'warning' line in stdout or stderr is an unexpected diagnostic,
    whatever the exit status. No exemption."""
    return [f"{r.get('name')} [{stream}]: {line}"
            for stream in ("stdout", "stderr")
            for line in (r.get(stream) or "").splitlines() if WARN.search(line)]


def free_udp_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def vmmap_inventory(pid, runner):
    """SCOPED loaded-image inventory of a live process through the common
    bounded command path: only mapped paths naming the project/TLS images
    (module, publisher, relay, MsQuic, libssl, libcrypto) are returned, as
    whole paths after the share-mode column. It is not an enumeration of
    every DSO the process maps; the full vmmap output stays in the command
    record the runner keeps."""
    vm = runner("vmmap", ["vmmap", str(pid)], timeout=20)
    images = set()
    for line in (vm.get("stdout") or "").splitlines():
        m = re.search(r"SM=\S+\s+(/.*?)\s*$", line)
        if m and any(k in line for k in ("libmsquic", "libssl", "libcrypto", "_native", "moq5_runtime_publisher", "moq5-relay")):
            images.add(m.group(1))
    return {"rc": vm["rc"], "timeout": vm["timeout"], "stderr": vm["stderr"]}, sorted(images)


class Child:
    """A directly owned child: Popen handle retained until settled."""

    def __init__(self, name, argv, env, cwd, stdin=False, on_line=None):
        self.name, self.argv = name, [str(a) for a in argv]
        self.stdout_lines, self.stderr_lines, self.reader_errors, self.write_errors = [], [], [], []
        self.on_line = on_line
        self.proc = subprocess.Popen(self.argv, stdin=subprocess.PIPE if stdin else None, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, text=True, env=env, cwd=cwd)
        self.readers = [threading.Thread(target=self._reader, args=(self.proc.stdout, self.stdout_lines, True), daemon=True),
                        threading.Thread(target=self._reader, args=(self.proc.stderr, self.stderr_lines, False), daemon=True)]
        for t in self.readers:
            t.start()
        self.settled = None

    def _reader(self, pipe, sink, is_stdout):
        try:
            for line in pipe:
                line = line.rstrip("\n")
                sink.append(line)
                if is_stdout and self.on_line:
                    self.on_line(line)
        except Exception as e:
            self.reader_errors.append(f"{self.name}: {e!r}")
        finally:
            pipe.close()

    def write_line(self, line):
        """A control write. Bounded by the protocol, not by this call: the
        caller admits at most CONTROL_LIMIT lines of at most CONTROL_MAX bytes
        each (see ControlProtocol), so the whole lifetime traffic on this pipe
        is under CONTROL_LIMIT * (CONTROL_MAX + 1) bytes -- far below the
        kernel pipe buffer -- and a write can never block even if the child
        never reads. A failure is a recorded fact."""
        data = (line + "\n").encode()
        assert len(data) <= CONTROL_MAX + 1
        try:
            os.write(self.proc.stdin.fileno(), data)
            return True
        except (BrokenPipeError, OSError, ValueError) as e:
            self.write_errors.append(f"{line}: {e!r}")
            return False

    def alive(self):
        return self.proc.poll() is None

    def settle(self, timeout, expected_rc=0, signal_first=None):
        """Wait bounded; then SIGTERM, then SIGKILL, each bounded. Idempotent."""
        if self.settled:
            return self.settled
        how = "exited"
        rc = self.proc.poll()
        if self.proc.stdin:                      # the finite protocol is over: EOF for a reader child, before waiting
            try:
                self.proc.stdin.close()
            except OSError as e:
                self.write_errors.append(f"stdin close: {e!r}")
        if rc is None and signal_first is not None:
            self.proc.send_signal(signal_first)
            how = signal.Signals(signal_first).name
        if rc is None:
            try:
                rc = self.proc.wait(timeout)
            except subprocess.TimeoutExpired:
                self.proc.send_signal(signal.SIGTERM)
                how = "SIGTERM_after_timeout" if signal_first is None else how + "+SIGTERM"
                try:
                    rc = self.proc.wait(STOP_S)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
                    how += "+SIGKILL"
                    rc = self.proc.wait(STOP_S)
        for t in self.readers:
            t.join(JOIN_S)
        intended = {"exited"} | ({signal.Signals(signal_first).name} if signal_first is not None else set())
        self.settled = {"name": self.name, "pid": self.proc.pid, "argv": self.argv, "rc": rc, "exit": rc, "how": how,
                        "expected_rc": expected_rc, "timeout": how not in intended,
                        "readers_settled": not any(t.is_alive() for t in self.readers),
                        "reader_errors": list(self.reader_errors), "write_errors": list(self.write_errors),
                        "stdout": "\n".join(self.stdout_lines), "stderr": "\n".join(self.stderr_lines)}
        return self.settled


class Harness:
    def __init__(self, run, relay=None):
        self.run = run
        self.relay_binary = relay
        self.children = []
        self.rec = {"harness_failures": [], "commands": [], "command_warnings": [], "primary_error": None,
                    "cleanup_errors": [], "run_dir": str(run),
                    "loader_overrides_in_effect": {k: v for k, v in os.environ.items() if k.startswith(("DYLD_", "PYTHON", "LD_"))},
                    "environment_keys": sorted(os.environ)}

    def fail(self, msg):
        self.rec["harness_failures"].append(msg)
        raise RuntimeError("HARNESS: " + msg)

    def run_cmd(self, name, argv, expect_rc=0, expect_stderr_substring=None, **kw):
        r = cmd(argv, **kw)
        r["name"], r["expected_rc"], r["expected_diagnostic"] = name, expect_rc, expect_stderr_substring
        self.rec["commands"].append(r)
        self.rec["command_warnings"].extend(collect_diagnostics(r))
        if r["timeout"]:
            self.fail(f"{name}: timed out")
        if r["rc"] != expect_rc:
            self.fail(f"{name}: rc {r['rc']} != {expect_rc}: {r['stderr'].strip()[:300]}")
        if expect_stderr_substring is not None and expect_stderr_substring not in (r["stderr"] + r["stdout"]):
            self.fail(f"{name}: expected diagnostic {expect_stderr_substring!r} absent: {r['stderr'].strip()[:300]}")
        return r

    def spawn(self, name, argv, env, stdin=False, on_line=None):
        c = Child(name, argv, env, self.run, stdin=stdin, on_line=on_line)
        self.children.append(c)
        return c

    def settle_all(self, timeouts):
        """Settle the listed owned children in the given order and record each
        as a command-like record. The relay is settled by stop_relay (its
        intended path is SIGTERM); any other unlisted child is settled last."""
        listed = [c for c, _ in timeouts]
        order = listed + [c for c in self.children if c not in listed and c is not getattr(self, "relay", None)]
        for c in order:
            t = dict(timeouts).get(c, STOP_S)
            if c.settled:
                continue
            try:
                rec = c.settle(t)
            except Exception as e:
                self.rec["cleanup_errors"].append(f"settle {c.name}: {e!r}")
                continue
            self.rec["commands"].append(rec)
            self.rec["command_warnings"].extend(collect_diagnostics(rec))

    def make_credentials(self):
        d = self.run / "creds"
        d.mkdir(mode=0o700)
        os.chmod(d, 0o700)
        self.run_cmd("openssl req ca", [OPENSSL, "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1",
                     "-nodes", "-days", "2", "-subj", "/CN=moq5 python runtime test CA", "-addext", "basicConstraints=critical,CA:TRUE",
                     "-addext", "keyUsage=critical,keyCertSign,cRLSign", "-keyout", "ca.key", "-out", "ca.pem"], cwd=d)
        self.run_cmd("openssl req server", [OPENSSL, "req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes",
                     "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
                     "-addext", "extendedKeyUsage=serverAuth", "-addext", "keyUsage=critical,digitalSignature",
                     "-keyout", "srv.key", "-out", "srv.csr"], cwd=d)
        self.run_cmd("openssl x509 sign", [OPENSSL, "x509", "-req", "-in", "srv.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
                     "-CAcreateserial", "-days", "2", "-copy_extensions", "copy", "-out", "srv.pem"], cwd=d,
                     expect_stderr_substring="Certificate request self-signature ok")
        for key in ("ca.key", "srv.key"):
            os.chmod(d / key, 0o600)
        ca_ext = self.run_cmd("openssl x509 ca ext", [OPENSSL, "x509", "-in", "ca.pem", "-noout", "-ext", "basicConstraints,keyUsage"], cwd=d)["stdout"]
        if "CA:TRUE" not in ca_ext or "Certificate Sign" not in ca_ext:
            self.fail("CA extensions: " + ca_ext)
        srv_ext = self.run_cmd("openssl x509 srv ext", [OPENSSL, "x509", "-in", "srv.pem", "-noout", "-ext",
                               "subjectAltName,extendedKeyUsage,basicConstraints"], cwd=d)["stdout"]
        if "DNS:localhost" not in srv_ext or "IP Address:127.0.0.1" not in srv_ext or "TLS Web Server Authentication" not in srv_ext:
            self.fail("server extensions: " + srv_ext)
        self.run_cmd("verify chain", [OPENSSL, "verify", "-CAfile", "ca.pem", "srv.pem"], cwd=d)
        self.run_cmd("verify hostname", [OPENSSL, "verify", "-CAfile", "ca.pem", "-verify_hostname", "localhost", "srv.pem"], cwd=d)
        return {"dir_mode": oct(stat.S_IMODE(d.stat().st_mode)),
                "modes": {p.name: oct(stat.S_IMODE(p.stat().st_mode)) for p in sorted(d.iterdir())},
                "ca_sha256": self.run_cmd("sha ca", ["shasum", "-a", "256", str(d / "ca.pem")])["stdout"].split()[0],
                "srv_sha256": self.run_cmd("sha srv", ["shasum", "-a", "256", str(d / "srv.pem")])["stdout"].split()[0]}

    def start_relay(self, port):
        creds = self.run / "creds"
        cfg = {"listener": {"host": "127.0.0.1", "port": port, "versions": [18, 16], "lanes": 1,
                            "cert": str(creds / "srv.pem"), "key": str(creds / "srv.key")},
               "logging": {"format": "json"}, "auth": {"mode": "allow_all"}}
        cfg_path = self.run / "relay.json"
        cfg_path.write_text(json.dumps(cfg, indent=1))
        env = {"PATH": "/usr/bin:/bin", "DYLD_FALLBACK_LIBRARY_PATH": MSQUIC_DIR}
        ready = threading.Event()
        info = {"binary": str(self.relay_binary), "port": port, "config": cfg, "ready_record": None}

        def on_line(line):
            if not ready.is_set() and line.startswith("{"):
                rec = json.loads(line)
                if rec.get("schema") == "RELAY_READY_V1" and rec.get("listener") == "raw" \
                        and rec.get("host") == "127.0.0.1" and rec.get("port") == port:
                    info["ready_record"] = rec
                    ready.set()
        self.relay = self.spawn("relay", [self.relay_binary, "serve", "--config", cfg_path], env, on_line=on_line)
        info["pid"] = self.relay.proc.pid
        self.rec["relay"] = info
        if not ready.wait(READY_S):
            self.fail(f"relay readiness not observed within {READY_S}s")
        r = info["ready_record"]
        if r.get("alpn_set") != EXPECTED_ALPN or r.get("lanes") != 1:
            self.fail(f"readiness identity mismatch: {r}")

    def stop_relay(self):
        info = self.rec["relay"]
        if self.relay.alive():
            self.relay.proc.send_signal(signal.SIGUSR1)      # declared contract: metrics + routes to stderr
            time.sleep(0.5)
        rec = self.relay.settle(STOP_S, signal_first=signal.SIGTERM)
        self.rec["commands"].append(rec)
        self.rec["command_warnings"].extend(collect_diagnostics(rec))
        info["stop"] = {"pid": rec["pid"], "exit": rec["exit"], "how": rec["how"]}
        info["readers_settled"], info["reader_errors"] = rec["readers_settled"], rec["reader_errors"]
        info["schemas"] = sorted({json.loads(l).get("schema") for l in self.relay.stdout_lines if l.startswith("{")})
        info["stdout_lines"], info["stderr_lines"] = list(self.relay.stdout_lines), list(self.relay.stderr_lines)
        (self.run / "relay.stdout").write_text("\n".join(self.relay.stdout_lines) + "\n")
        (self.run / "relay.stderr").write_text("\n".join(self.relay.stderr_lines) + "\n")


def relay_checks(rec):
    relay = rec.get("relay", {}) or {}
    stop = relay.get("stop") or {}
    ready = relay.get("ready_record") or {}
    return {
        "relay_ready_identity": ready.get("schema") == "RELAY_READY_V1" and ready.get("listener") == "raw"
            and ready.get("host") == "127.0.0.1" and ready.get("port") == relay.get("port")
            and ready.get("alpn_set") == EXPECTED_ALPN and ready.get("lanes") == 1,
        "relay_intended_shutdown_exit_0": stop.get("how") == "SIGTERM" and stop.get("exit") == 0,
        "relay_readers_settled_no_reader_error": relay.get("readers_settled") is True and not relay.get("reader_errors"),
        "relay_stop_record_present": "RELAY_STOP_V1" in (relay.get("schemas") or []),
        "no_command_warnings": not rec.get("command_warnings"),
        "commands_match_declared_status_no_timeout": bool(rec.get("commands")) and all(
            not x.get("timeout") and x.get("rc") == x.get("expected_rc") for x in rec.get("commands", []) if x.get("name") != "relay"),
        "no_harness_failure": not rec.get("harness_failures") and rec.get("primary_error") is None and not rec.get("cleanup_errors"),
    }
