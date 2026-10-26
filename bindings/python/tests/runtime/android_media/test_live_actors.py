"""The live run's actors and their offline preflight.

Nothing here starts a listener, connects anywhere or runs adb. Credentials are made with
openssl in a temporary directory; the relay and publisher are small scripts that record their
argv, so the preflight's commands and the exact configuration file are observable.
"""
import contextlib
import hashlib
import io
import json
import os
import signal
import stat
import struct
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import device_lane                                           # noqa: E402
import live_actors                                           # noqa: E402
import run_android_media                                     # noqa: E402
from runtime_support import EXPECTED_ALPN, MSQUIC_DIR, OPENSSL, cmd   # noqa: E402

# How each live child ends: the relay at SIGTERM, which it handles; the publisher and the host by
# themselves; and the player-log sidecar -- `adb logcat`, which does not handle SIGTERM -- at the
# signal, status -15.
LIVE_EXITS = {"relay": ("SIGTERM", 0), "publisher": ("exited", 0), "host-receiver": ("exited", 0),
              "player-log": ("SIGTERM", -15)}

# The runs' declared relay port: any free high port other than the player's default 4443.
OWNED_PORT = 46443

# A capacity report with the shape `moq5-relay capacity` prints (tools/moq5-relay/cli/main.c,
# print_capacity): heading, fixed note, per-shard row, optional rows in order, closing row.
CAPACITY_HEAD = ["relay-state allocation-request ceiling: 5242880 bytes",
                 "  (allocator requests only \u2014 excludes malloc metadata, size-class rounding,",
                 "   fragmentation, thread stacks, transient diagnostics, and MsQuic/session",
                 "   memory that remains adapter-owned)",
                 "  per shard: structure 1048576 + payloads 3145728 + binding 524288 + trace 262144 bytes"]
CAPACITY_OPTIONAL = ["  cross-shard: 4096 bytes (runtime, channels, canon keys, staging)",
                     "  cli runtime: 2048 bytes (snapshot rows, plus the serve context at lanes > 1; included above)",
                     "  admin endpoint: 8192 bytes (listener object, both body banks, and the coordinator's per-lane "
                     "rows and views, its signal-sink body and its /api/v1/info document; included above)",
                     "  admin thread stack reservation: 524288 bytes (not part of the ceiling above)"]
CAPACITY_TAIL = ["  usable client bindings per shard: 1024"]
CAPACITY_REPORT = CAPACITY_HEAD + CAPACITY_OPTIONAL[2:] + CAPACITY_TAIL

# The fixture input's parameter sets (Constrained Baseline 3.0, as the declared avc1.42C01E), and the
# avcC ISO/IEC 14496-15 lays out for them: version 1, profile/compatibility/level from the SPS, a 4-byte
# NAL length (0xFF), one SPS (0xE1) then one PPS, each with a 16-bit length.
FIXTURE_SPS, FIXTURE_PPS = bytes.fromhex("6742c01e95a0a0fc"), bytes.fromhex("68ce3c80")
FIXTURE_AVCC = (bytes.fromhex("0142c01effe1") + len(FIXTURE_SPS).to_bytes(2, "big") + FIXTURE_SPS + b"\x01"
                + len(FIXTURE_PPS).to_bytes(2, "big") + FIXTURE_PPS)
TRACK_CONFIG = {"event": "track_config", "codec": "avc1.42C01E", "width": 320, "height": 240,
                "framerate_millis": 24000, "timescale": 90000, "bitrate": 400000, "declared": True,
                "init_data": FIXTURE_AVCC.hex()}
START = b"\x00\x00\x00\x01"


def write_objects(path, n=60, sync_every=24, step=41_666, first=None):
    """A well-formed objects file of `n` records (the run declares 60). The first, a sync object, opens
    with the fixture's SPS and PPS (or `first`, a whole replacement payload)."""
    with open(path, "wb") as f:
        for i in range(n):
            payload = bytes([0, 0, 0, 1, 9]) + i.to_bytes(2, "big") * (3 + i % 5)
            if i == 0:
                payload = first if first is not None else (START + FIXTURE_SPS + START + FIXTURE_PPS + START + b"\x65\x88" + payload[4:])
            f.write(struct.pack("<IBQ", len(payload), 1 if i % sync_every == 0 else 0, i * step))
            f.write(payload)


def make_credentials(creds, san="IP:10.0.2.2,IP:127.0.0.1"):
    """A local CA and a relay certificate for `san`, as the publisher sample's README makes them."""
    creds.mkdir(mode=0o700)
    os.chmod(creds, 0o700)
    steps = [
        [OPENSSL, "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-days", "1",
         "-subj", "/CN=moq5 test CA", "-addext", "basicConstraints=critical,CA:TRUE,pathlen:0",
         "-addext", "keyUsage=critical,keyCertSign,cRLSign", "-keyout", "ca.key", "-out", "ca.pem"],
        [OPENSSL, "req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
         "-subj", "/CN=moq5 test relay", "-keyout", "relay.key", "-out", "relay.csr"],
    ]
    (creds / "relay.ext").write_text("basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\n"
                                     f"extendedKeyUsage=serverAuth\nsubjectAltName={san}\n")
    steps.append([OPENSSL, "x509", "-req", "-in", "relay.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial",
                  "-days", "1", "-extfile", "relay.ext", "-out", "relay.pem"])
    for argv in steps:
        r = cmd(argv, cwd=creds)
        if r["rc"] != 0:
            raise RuntimeError(f"{argv[1:3]}: {r['stderr']}")
    for key in ("ca.key", "relay.key"):
        os.chmod(creds / key, 0o600)


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class Fixture:
    """A complete set of live inputs under directory names containing spaces: credentials, the
    objects file, an installed-package root, and relay/publisher scripts that log their argv.
    `relay mode` / `publisher mode` files steer the scripts."""

    def __init__(self, test, san="IP:10.0.2.2,IP:127.0.0.1"):
        tmp = tempfile.TemporaryDirectory()
        test.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()
        self.run_dir = self.root / "live run"
        self.run_dir.mkdir()
        self.credentials, refused = live_actors.fresh_credentials(self.run_dir, san=san)
        assert refused is None, refused
        write_objects(self.run_dir / "objects.bin")
        self.package = self.root / "installed package"
        (self.package / "moq5").mkdir(parents=True)
        (self.package / "moq5" / "__init__.py").write_text("")
        self.bin = self.root / "tool bin"
        self.bin.mkdir()
        self.log = self.root / "argv.log"
        # The declared provider, and the relay stand-in: a Python script, so the real dyld loads it and
        # prints its own image lines under DYLD_PRINT_LIBRARIES (a /bin/sh script would never see that
        # variable); it adds one image line per entry of `provider lines`, as the pinned relay does
        # for the MsQuic library it loads.
        self.provider = self.root / "msquic build" / "bin" / "libmsquic.2.6.0.dylib"
        self.provider.parent.mkdir(parents=True)
        self.provider.write_bytes(b"the reviewed MsQuic provider\n")
        self.provider_lines = self.root / "provider lines"
        self.provider_lines.write_text(f"{self.provider}\n")
        self.relay = self.bin / "moq5-relay"
        (self.root / "relay mode").write_text("normal")
        (self.root / "serve mode").write_text("normal")
        # `serve`, as the pinned relay runs it: the loader's lines (under DYLD_PRINT_LIBRARIES), the
        # capacity report on stderr, the raw and admin RELAY_READY_V1 records on stdout, then running
        # until SIGTERM, which it handles with status 0. `serve mode` steers the departures.
        self.relay.write_text(f"""#!{sys.executable} -I
import os, sys
with open({str(self.log)!r}, "a") as log:
    log.write("".join(w + "\\n" for w in ["relay", *sys.argv[1:]]))
if sys.argv[1:2] == ["serve"]:
    import json, signal, time
    serve = open({str(self.root / "serve mode")!r}).read()
    port = json.load(open(sys.argv[3]))["listener"]["port"]
    if serve == "occupied":
        sys.stderr.write(f"transport create failed (port {{port}})\\n")
        sys.exit(1)
    if os.environ.get("DYLD_PRINT_LIBRARIES") == "1" and serve != "no provider in serve":
        pid = os.getpid() + 1 if serve == "foreign pid" else os.getpid()
        for path in open({str(self.provider_lines)!r}).read().splitlines():
            sys.stderr.write(f"dyld[{{pid}}]: <1C84AFF8-23AE-30A6-B804-9BE34CB0A3FD> {{path}}\\n")
    if serve == "loader diagnostic":
        sys.stderr.write(f"dyld[{{os.getpid()}}]: Library not loaded: @rpath/libmsquic.2.dylib\\n")
    stop = []
    signal.signal(signal.SIGTERM, signal.SIG_IGN if serve == "ignore term" else lambda *a: stop.append(1))
    sys.stderr.write(open({str(self.root / "capacity report")!r}).read())
    if serve == "warn":
        sys.stderr.write("warning: pool capacity rounded up\\n")
    sys.stderr.flush()
    if serve != "never ready":
        print(json.dumps({{"schema": "RELAY_READY_V1", "elapsed_us": 1, "listener": "raw", "host": "127.0.0.1",
                          "port": port, "alpn_set": "moqt-18+moqt-16", "lanes": 1}}), flush=True)
        print(json.dumps({{"schema": "RELAY_READY_V1", "elapsed_us": 2, "listener": "admin", "host": "127.0.0.1",
                          "port": 9109}}), flush=True)
    while not stop:
        time.sleep(0.02)
    sys.exit(0)
mode = open({str(self.root / "relay mode")!r}).read()
if mode == "refuse":
    sys.stderr.write("moq5-relay: listener.port: out of range\\n")
    sys.exit(2)
if mode == "warn":
    print("warning: pool capacity rounded up")
if os.environ.get("DYLD_PRINT_LIBRARIES") == "1":
    for path in open({str(self.provider_lines)!r}).read().splitlines():
        sys.stderr.write(f"dyld[{{os.getpid()}}]: <1C84AFF8-23AE-30A6-B804-9BE34CB0A3FD> {{path}}\\n")
sys.stdout.write(open({str(self.root / "capacity report")!r}).read())
""")
        os.chmod(self.relay, 0o755)
        self.capacity_report(CAPACITY_REPORT)
        self.relay_digest, self.provider_digest = sha256(self.relay), sha256(self.provider)   # the reviewed receipt
        self.adb = self._script("adb", "adb", "exit 97\n")              # judged as a file, never run
        self.publisher = self._script("moq5_runtime_publisher", "publisher",
                                      'if [ "$mode" = warn ]; then echo "warning: track declared twice"; fi\n'
                                      'if [ "$mode" = wrong ]; then echo \'{"event":"track_config","codec":"avc1.42E01E",'
                                      '"width":320,"height":240,"framerate_millis":24000,"declared":false}\'; exit 0; fi\n'
                                      f"echo '{json.dumps(TRACK_CONFIG)}'\n")

    def _script(self, name, role, body):
        path = self.bin / name
        mode = self.root / f"{role} mode"
        mode.write_text("normal")
        path.write_text(f"#!/bin/sh\nprintf '%s\\n' '{role}' \"$@\" >> '{self.log}'\nmode=$(cat '{mode}')\n{body}")
        os.chmod(path, 0o755)
        return path

    def capacity_report(self, lines):
        (self.root / "capacity report").write_text("".join(line + "\n" for line in lines))

    def mode(self, role, value):
        (self.root / f"{role} mode").write_text(value)

    def live_run(self, **kw):
        fields = dict(run_dir=self.run_dir, relay_binary=self.relay, publisher_binary=self.publisher,
                      package=self.package, admin_port=9109, port=OWNED_PORT, adb=self.adb, python="/usr/bin/python3",
                      relay_sha256=self.relay_digest, provider=self.provider, provider_sha256=self.provider_digest)
        fields.update(kw)
        return live_actors.LiveRun(**fields)


class RecordingRunner:
    """The preflight's command runner: every call is recorded, then run for real (openssl and the
    two scripts) unless `timeout_for` names it. `append` adds text to a named command's real
    output, {name: (stream, text)}; `replace` replaces it, {name: (stream, text)}; `rc` replaces a
    named command's exit status, {name: rc}."""

    def __init__(self, timeout_for=None, append=None, rc=None, replace=None):
        self.calls, self.timeout_for, self.append, self.rc = [], timeout_for, append or {}, rc or {}
        self.replace = replace or {}

    def __call__(self, name, argv, timeout, env=None):
        self.calls.append({"name": name, "argv": list(argv), "timeout": timeout, "env": env})
        if name == self.timeout_for:
            return {"argv": argv, "rc": None, "stdout": "", "stderr": "", "timeout": True}
        r = cmd(argv, timeout=timeout, env=env)
        if name in self.append:
            stream, text = self.append[name]
            r[stream] += text
        if name in self.replace:
            stream, text = self.replace[name]
            r[stream] = text
        r["rc"] = self.rc.get(name, r["rc"])
        return r


class DeclaredIdentity(unittest.TestCase):
    def test_declarations_match_the_player_host_and_publisher(self):
        self.assertEqual((live_actors.LISTENER_HOST, live_actors.PLAYER_DEFAULT_PORT, live_actors.EMULATOR_HOST), ("127.0.0.1", 4443, "10.0.2.2"))
        self.assertEqual((live_actors.NAMESPACE_TEXT, live_actors.TRACK_TEXT, live_actors.SNI), ("live/cam1", "video", "127.0.0.1"))
        self.assertEqual(live_actors.TRACK_DECL, ("avc1.42C01E", "320x240", "24000"))
        self.assertEqual((live_actors.HOST_ESTABLISH_S, live_actors.HOST_DEADLINE_S), (25.0, 80.0))
        import host_receiver
        host_receiver.check_bounds(live_actors.HOST_ESTABLISH_S, live_actors.HOST_DEADLINE_S)


class LiveIdentity(unittest.TestCase):
    def setUp(self):
        self.fx = Fixture(self)

    def test_actor_argv_is_exact(self):
        fx, run = self.fx, self.fx.live_run()
        actors = live_actors.actor_argv(run)
        self.assertEqual(actors["relay"], [str(fx.relay), "serve", "-c", str(fx.run_dir / "relay.json")])
        self.assertEqual(actors["publisher"], [str(fx.publisher), "moqt://127.0.0.1:46443", "live/cam1", "video",
                                               str(fx.run_dir / "creds" / "ca.pem"), "127.0.0.1", str(fx.run_dir / "objects.bin"),
                                               "positive", "pull", "--track", "avc1.42C01E", "320x240", "24000",
                                               "--avc-config-from-first-sync"])
        self.assertEqual(actors["host"], [
            "/usr/bin/python3", "-I", "-B", str(run_android_media.HOST_RECEIVER), "--package", str(fx.package),
            "--url", "moqt://127.0.0.1:46443", "--ca", str(fx.run_dir / "creds" / "ca.pem"), "--sni", "127.0.0.1",
            "--objects", str(fx.run_dir / "objects.bin"), "--establish-s", "25.0", "--deadline-s", "80.0"])
        self.assertEqual(actors["env"], {"PATH": "/usr/bin:/bin", "DYLD_FALLBACK_LIBRARY_PATH": MSQUIC_DIR})
        self.assertEqual(actors["relay_identity"], run_android_media.RelayIdentity("raw", "127.0.0.1", 46443, EXPECTED_ALPN, 1))
        self.assertEqual(actors["expected_exits"], LIVE_EXITS)
        self.assertEqual(set(actors), {"relay", "publisher", "host", "env", "relay_identity", "expected_exits"})

    def test_the_port_is_declared_never_defaulted(self):
        fields = dict(run_dir=self.fx.run_dir, relay_binary=self.fx.relay, publisher_binary=self.fx.publisher,
                      package=self.fx.package, admin_port=9109, adb=self.fx.adb, python="/usr/bin/python3",
                      relay_sha256=self.fx.relay_digest, provider=self.fx.provider, provider_sha256=self.fx.provider_digest)
        with self.assertRaises(TypeError):
            live_actors.LiveRun(**fields)
        for port in (1024, 4444, 46443, 65535):
            self.assertEqual(self.fx.live_run(port=port).port, port)

    def test_the_player_reaches_the_same_declared_port(self):
        run = self.fx.live_run(port=51234)
        actors, config = live_actors.actor_argv(run), live_actors.relay_config(run)
        self.assertEqual(live_actors.player_url(run), "moqt://10.0.2.2:51234")
        ports = {config["listener"]["port"], actors["relay_identity"].port,
                 int(actors["publisher"][1].rsplit(":", 1)[1]),
                 int(actors["host"][actors["host"].index("--url") + 1].rsplit(":", 1)[1]),
                 int(live_actors.player_url(run).rsplit(":", 1)[1])}
        self.assertEqual(ports, {51234})

    def test_the_player_actor_is_built_from_the_declared_port(self):
        run = self.fx.live_run(port=51234)
        calls = []
        device = run_android_media.Device(run_android_media.DEDICATED_SERIAL, lambda *a: calls.append(a))
        player = live_actors.player_actor(run, device, "/builds/player.apk", 60)
        config = live_actors.relay_config(run)
        self.assertEqual(player.inputs, {"Relay URL": "moqt://10.0.2.2:51234", "Namespace": "live/cam1",
                                         "Objects to receive": "60"})
        self.assertEqual(int(player.inputs["Relay URL"].rsplit(":", 1)[1]), config["listener"]["port"])
        self.assertEqual(player.inputs["Namespace"], live_actors.NAMESPACE_TEXT)
        self.assertEqual(calls, [])                                   # building the actor runs nothing

    def test_relay_config_is_exact(self):
        fx, run = self.fx, self.fx.live_run(admin_port=40123)
        self.assertEqual(live_actors.relay_config(run), {
            "listener": {"host": "127.0.0.1", "port": 46443, "versions": [18, 16], "lanes": 1,
                         "cert": str(fx.run_dir / "creds" / "relay.pem"), "key": str(fx.run_dir / "creds" / "relay.key")},
            "admin": {"tcp": {"host": "127.0.0.1", "port": 40123}},
            "logging": {"format": "json"}, "auth": {"mode": "allow_all"}})

    def test_every_actor_shares_one_listener_ca_and_objects_file(self):
        run = self.fx.live_run()
        actors, config = live_actors.actor_argv(run), live_actors.relay_config(run)
        url = f"moqt://{config['listener']['host']}:{config['listener']['port']}"
        self.assertEqual(actors["publisher"][1], url)
        self.assertEqual(actors["host"][actors["host"].index("--url") + 1], url)
        self.assertEqual((actors["relay_identity"].host, actors["relay_identity"].port),
                         (config["listener"]["host"], config["listener"]["port"]))
        ca, objects = actors["publisher"][4], actors["publisher"][6]
        self.assertEqual(actors["host"][actors["host"].index("--ca") + 1], ca)
        self.assertEqual(actors["host"][actors["host"].index("--objects") + 1], objects)
        self.assertEqual(actors["publisher"][5], actors["host"][actors["host"].index("--sni") + 1])

    def test_refused_identities_are_named(self):
        fx = self.fx
        for kw, prefix in (({"port": 4443}, "port:"), ({"port": 1023}, "port:"), ({"port": 65536}, "port:"),
                           ({"port": 46443.0}, "port:"), ({"port": True}, "port:"), ({"port": "46443"}, "port:"),
                           ({"admin_port": 0}, "admin_port:"), ({"admin_port": 65536}, "admin_port:"),
                           ({"python": "python3"}, "python:"),
                           ({"admin_port": "9109"}, "admin_port:"), ({"admin_port": True}, "admin_port:"),
                           ({"run_dir": Path("live run")}, "run_dir:"), ({"relay_binary": str(fx.relay)}, "relay_binary:"),
                           ({"package": Path("pkg")}, "package:"), ({"adb": Path("adb")}, "adb:"),
                           ({"adb": "/opt/sdk/platform-tools/adb"}, "adb:"),
                           ({"provider": Path("libmsquic.2.dylib")}, "provider:"),
                           ({"relay_sha256": "ABC"}, "relay_sha256:"), ({"provider_sha256": "0" * 63}, "provider_sha256:")):
            with self.subTest(kw=kw):
                with self.assertRaises(ValueError) as caught:
                    fx.live_run(**kw)
                self.assertTrue(str(caught.exception).startswith(prefix), str(caught.exception))


class ActorExits(unittest.TestCase):
    """The factory's `expected_exits` is the mapping the live verdict judges its children by."""

    def settled(self, name, program, signal_first):
        """A real child running `program`, which prints `armed` once its SIGTERM disposition is in
        place; settled with SIGTERM only after that line, or after it exits by itself."""
        armed = threading.Event()
        child = run_android_media.Child(name, [sys.executable, "-I", "-c", program], {"PATH": "/usr/bin:/bin"}, str(HERE),
                                        on_line=lambda line: armed.set() if line == "armed" else None)
        self.addCleanup(lambda: child.proc.poll() is None and child.proc.kill())
        if signal_first:
            self.assertTrue(armed.wait(5), f"{name}: the child never reported its signal disposition")
        else:
            child.proc.wait(5)
        return child.settle(5, signal_first=signal.SIGTERM)

    HANDLED = ("import signal, sys, time; signal.signal(signal.SIGTERM, lambda *_: sys.exit(0)); "
               "print('armed', flush=True); time.sleep(60)")
    UNHANDLED = "import time; print('armed', flush=True); time.sleep(60)"
    EXITS_BY_ITSELF = "pass"

    def verdict(self, player_log_program, exits):
        rec = {"checks": dict.fromkeys(run_android_media.REQUIRED_CHECKS, True), "threads_settled": True,
               "children": [self.settled("relay", self.HANDLED, True),
                            self.settled("publisher", self.EXITS_BY_ITSELF, False),
                            self.settled("host-receiver", self.EXITS_BY_ITSELF, False),
                            self.settled("player-log", player_log_program, player_log_program != self.EXITS_BY_ITSELF)]}
        return run_android_media.final_verdict(rec, exits)

    def test_the_factory_declares_the_adb_sidecar_at_minus_15_and_the_others_unchanged(self):
        exits = live_actors.actor_argv(Fixture(self).live_run())["expected_exits"]
        self.assertEqual(exits.get("player-log"), ("SIGTERM", -15),
                         "player-log: the live sidecar is adb logcat, which ends at SIGTERM with status -15")
        self.assertEqual(exits, LIVE_EXITS)

    def test_the_verdict_judges_real_children_by_the_factorys_own_mapping(self):
        exits = live_actors.actor_argv(Fixture(self).live_run())["expected_exits"]
        self.assertEqual(self.verdict(self.UNHANDLED, exits), {"ok": True, "failures": []})
        self.assertEqual(self.verdict(self.HANDLED, exits)["failures"],
                         ["player-log: SIGTERM rc 0, expected SIGTERM rc -15"])
        self.assertEqual(self.verdict(self.EXITS_BY_ITSELF, exits)["failures"],
                         ["player-log: exited rc 0, expected SIGTERM rc -15"])


class Composition(unittest.TestCase):
    """The live run's actors, composed from one LiveRun and the dedicated device. Nothing runs."""

    def compose(self, port=51234, adb=None, ca=None):
        fx, calls = Fixture(self), []
        lane = device_lane.EmulatorLane("/opt/sdk/emulator/emulator", str(fx.adb) if adb is None else adb,
                                        fx.root / "emulator.log", runner=lambda *a, **k: calls.append(a))
        run = fx.live_run(port=port)
        this_run = json.loads(run.credentials_receipt.read_text())["ca_der_sha256"]
        lane.ca = None if ca is None else dict(ca, ca_der_sha256=ca.get("ca_der_sha256", this_run))
        self.lane = lane
        return run, live_actors.compose(run, lane, "/builds/player.apk"), calls

    def test_the_host_actors_are_the_declared_ones(self):
        run, (actors, n), calls = self.compose()
        argv = live_actors.actor_argv(run)
        self.assertEqual((actors.relay_argv, actors.publisher_argv, actors.host_argv),
                         (argv["relay"], argv["publisher"], argv["host"]))
        self.assertEqual((actors.relay_identity, actors.env), (argv["relay_identity"], argv["env"]))
        self.assertEqual(actors.expected_exits, {**LIVE_EXITS, "player-diag": ("SIGTERM", -15)})
        self.assertEqual(actors.hooks, {})
        self.assertEqual(calls, [])

    def test_the_relay_alone_carries_the_loader_list_and_is_judged_by_its_own_record(self):
        run, (actors, _n), calls = self.compose()
        self.assertEqual(actors.env, live_actors.serve_env())                   # the shared base, no loader list
        self.assertEqual(actors.relay_env, {"DYLD_PRINT_LIBRARIES": "1"})
        pid = 4242
        image = f"dyld[{pid}]: <1C84AFF8-23AE-30A6-B804-9BE34CB0A3FD> "
        self.assertIsNone(actors.relay_evidence({"pid": pid, "stderr": image + str(run.provider)}))
        self.assertIn("expected only the serve process 4243",
                      actors.relay_evidence({"pid": pid + 1, "stderr": image + str(run.provider)}) or "")
        self.assertIn("loader lines from processes [], expected only the serve process 4242",
                      actors.relay_evidence({"pid": pid, "stderr": ""}) or "")
        self.assertEqual(calls, [])

    def test_the_player_mode_reaches_the_player_and_the_relay_snapshot_is_wired(self):
        fx = Fixture(self)
        lane = device_lane.EmulatorLane("/opt/sdk/emulator/emulator", str(fx.adb), fx.root / "emulator.log",
                                        runner=lambda *a, **k: None)
        run = fx.live_run()
        actors, _n = live_actors.compose(run, lane, "/builds/player.apk", player_mode="observer-off-diagnostic")
        player = actors.player_start.player
        self.assertEqual((player.observing, player.tracing), (False, True))
        self.assertIsNotNone(actors.relay_snapshot)
        default, _n = live_actors.compose(run, lane, "/builds/player.apk")
        self.assertEqual((default.player_start.player.observing, default.player_start.player.tracing), (True, False))
        with self.assertRaises(ValueError):
            live_actors.compose(run, lane, "/builds/player.apk", player_mode="something")

    def test_the_diagnostic_boundary_requests_player_stats_once_then_snapshots_the_relay(self):
        calls, order = [], []
        fx = Fixture(self)

        def runner(name, argv, timeout):
            calls.append((name, argv, timeout))
            order.append(name)
            out = ("09-28 12:00:00.000  4242  4242 I MoqPlayerDiag: 3 diagnostics-listener armed "
                   "thread=moq5-player-diagnostics\n" if name == "player_listener_armed" else "Broadcast completed: result=0\n")
            return {"rc": 0, "stdout": out, "stderr": "", "timeout": False}
        lane = device_lane.EmulatorLane("/opt/sdk/emulator/emulator", str(fx.adb), fx.root / "emulator.log", runner=runner)
        run = fx.live_run()
        waits = []
        actors, _n = live_actors.compose(run, lane, "/builds/player.apk", player_mode="observer-off-diagnostic",
                                         observe_wait=waits.append,
                                         relay_snapshot=lambda relay: order.append("relay-snapshot") or {"route": "r"})
        actors.player_start.player.pid = 4242
        snap = actors.relay_snapshot(object())
        self.assertEqual(order, ["player_listener_armed", "player_stats_request", "relay-snapshot"])
        (_n1, armed_argv, _t1), (name, argv, _t) = calls
        self.assertEqual(armed_argv, [str(fx.adb), "-s", "emulator-5580", "logcat", "-d", "--pid=4242", "-v", "threadtime",
                                      "-s", "MoqPlayerDiag:I"])
        self.assertTrue(snap["player_listener_armed"]["armed"])
        self.assertEqual(argv, [str(fx.adb), "-s", "emulator-5580", "shell", "am", "broadcast", "-a",
                                "org.openmoq.moq5.samples.player.DIAGNOSTIC_STATS", "-p", "org.openmoq.moq5.samples.player"])
        self.assertEqual(waits, [live_actors.DIAG_OBSERVE_S])
        self.assertEqual(snap["player_stats_request"]["rc"], 0)
        self.assertIn("sent_monotonic", snap["player_stats_request"])
        self.assertEqual(snap["route"], "r")
        acceptance, _n = live_actors.compose(run, lane, "/builds/player.apk", observe_wait=waits.append,
                                             relay_snapshot=lambda relay: {"route": "r"})
        calls.clear()
        self.assertEqual(acceptance.relay_snapshot(object()), {"route": "r"})
        self.assertEqual(calls, [])                                           # no request outside the diagnostic mode

    def test_a_listener_not_shown_armed_gets_no_stats_request(self):
        calls = []
        fx = Fixture(self)

        def runner(name, argv, timeout):
            calls.append(name)
            return {"rc": 0, "stdout": "I MoqPlayerDiag: 1 diagnostics-listener unarmed: debuggable=false sdk=36\n",
                    "stderr": "", "timeout": False}
        lane = device_lane.EmulatorLane("/opt/sdk/emulator/emulator", str(fx.adb), fx.root / "emulator.log", runner=runner)
        waits = []
        actors, _n = live_actors.compose(fx.live_run(), lane, "/builds/player.apk", player_mode="observer-off-diagnostic",
                                         observe_wait=waits.append, relay_snapshot=lambda relay: {"route": "r"})
        actors.player_start.player.pid = 4242
        snap = actors.relay_snapshot(object())
        self.assertEqual(calls, ["player_listener_armed"])
        self.assertEqual(waits, [])
        self.assertFalse(snap["player_listener_armed"]["armed"])
        self.assertEqual(snap["player_stats_request"], {"skipped": "the diagnostic listener was not shown armed"})
        self.assertEqual(snap["route"], "r")

    def test_one_player_on_the_declared_port_serves_every_player_role(self):
        run, (actors, n), calls = self.compose()
        roles = (actors.player_install, actors.player_prepare, actors.player_start, actors.player_probe, actors.player_log_argv)
        players = {id(getattr(role, "player", None) or role.__self__) for role in roles}
        self.assertEqual(len(players), 1, "every player role is the same DevicePlayer")
        player = actors.player_start.player
        self.assertEqual([role.__name__ for role in roles], ["install", "prepare", "start", "observe", "log_argv"])
        self.assertEqual(player.inputs["Relay URL"], live_actors.player_url(run))
        self.assertEqual(player.inputs["Objects to receive"], str(n))
        self.assertEqual(calls, [])

    def test_the_object_count_is_the_hosts_declaration(self):
        import host_receiver
        _run, (_actors, n), _calls = self.compose()
        self.assertEqual(n, host_receiver.declared_objects())
        self.assertEqual(n, 60)

    def test_the_route_barrier_watches_the_declared_track(self):
        import threading
        import route_dump
        seen = []
        original = route_dump.route_barrier
        route_dump.route_barrier = lambda *a, **kw: seen.append((a, kw)) or ("probe", "baseline")
        self.addCleanup(setattr, route_dump, "route_barrier", original)
        _run, (actors, _n), _calls = self.compose()
        host_active, player_started, record = threading.Event(), threading.Event(), []
        self.assertEqual(actors.route_barrier(host_active, player_started, record), ("probe", "baseline"))
        (args, kw), = seen
        self.assertEqual(args, ((b"live", b"cam1"), b"video", host_active, player_started))
        self.assertEqual(kw, {"record": record})

    def test_the_device_uses_exactly_the_runs_declared_adb(self):
        # The children run with PATH=/usr/bin:/bin: the receipt stream's adb is the declared absolute one.
        for adb in ("adb", "platform-tools/adb", "", "/opt/other-sdk/platform-tools/adb"):
            with self.subTest(adb=adb):
                with self.assertRaises(ValueError) as caught:
                    self.compose(adb=adb)
                self.assertTrue(str(caught.exception).startswith("adb: "), str(caught.exception))
        run, (actors, _n), _calls = self.compose()
        self.assertEqual(actors.player_start.player.device.adb, str(run.adb))

    CLEAN = device_lane.StoreInventory(existed=False, entries=(), parseable=True)

    def placed(self, before=CLEAN, placed=True):
        return {"path": "/data/misc/user/0/cacerts-added/d56d3822.0", "stage": "/data/local/tmp/moq5-test-ca.pem",
                "ca_created": True, "stage_created": False, "store_created": not before.existed,
                "store_before": before, "placed": placed}

    def test_install_and_start_require_the_clean_store_prerequisite(self):
        other = device_lane.StoreInventory(existed=True, entries=("aabbccdd.0",), parseable=True)
        unparseable = device_lane.StoreInventory(existed=True, entries=("garbage",), parseable=False)
        for label, ca, needle in (
                ("no CA placed", None, "no verified run CA is placed"),
                ("a placement that did not complete", self.placed(placed=False), "no verified run CA is placed"),
                ("another certificate was already there", self.placed(other), "already held ['aabbccdd.0']"),
                ("the listing was not parseable", self.placed(unparseable), "listing before placement was not parseable")):
            with self.subTest(label):
                run, (actors, _n), calls = self.compose(ca=ca)
                player = actors.player_install.player
                player.install = lambda stop, budget_s: "ungated: the player's install ran"
                player.start = lambda stop, dispatched, admit: "ungated: the player's Start ran"
                for role, args in ((actors.player_install, (threading.Event(), 60)),
                                   (actors.player_start, (threading.Event(), lambda: None, lambda: None))):
                    reason = role(*args)
                    self.assertTrue(reason.startswith("trust prerequisite: "), f"{role.__name__} was not refused: {reason}")
                    self.assertIn(needle, reason)
                self.assertEqual(calls, [])                                 # nothing reached the device

    def test_a_clean_prerequisite_admits_and_is_judged_at_each_call(self):
        run, (actors, _n), calls = self.compose(ca=self.placed())
        player = actors.player_install.player
        player.install = lambda stop, budget_s: "the player's own install ran"
        player.start = lambda stop, dispatched, admit: "the player's own start ran"
        self.assertEqual(actors.player_install(threading.Event(), 60), "the player's own install ran")
        self.lane.ca = None                                                  # removed between install and Start
        self.assertTrue(actors.player_start(threading.Event(), None, None).startswith("trust prerequisite: "))


class Preflight(unittest.TestCase):
    def failing(self, result):
        return {c["name"]: c["reason"] for c in result["checks"] if not c["ok"]}

    def test_complete_inputs_pass_with_only_local_commands(self):
        fx = Fixture(self)
        run, runner = fx.live_run(), RecordingRunner()
        result = live_actors.preflight(run, runner)
        self.assertEqual(self.failing(result), {})
        self.assertIs(result["ok"], True)
        self.assertEqual([c["name"] for c in result["checks"]], list(live_actors.PREFLIGHT_CHECKS))
        self.assertIn("host_python", live_actors.PREFLIGHT_CHECKS)
        self.assertIn("adb_binary", live_actors.PREFLIGHT_CHECKS)
        self.assertNotIn(str(fx.adb), [c["argv"][0] for c in runner.calls])           # judged, never run
        self.assertEqual({c["argv"][0] for c in runner.calls}, {OPENSSL, str(fx.relay), str(fx.publisher)})
        self.assertTrue(all(c["timeout"] == live_actors.PREFLIGHT_COMMAND_S for c in runner.calls))
        self.assertIn([str(fx.relay), "capacity", "-c", str(fx.run_dir / "relay.json")], [c["argv"] for c in runner.calls])
        self.assertIn([str(fx.publisher), "--track-config-selftest", "--track", "avc1.42C01E", "320x240", "24000",
                       "--avc-config-from-first-sync", str(fx.run_dir / "objects.bin")], [c["argv"] for c in runner.calls])
        self.assertFalse(any(a in ("serve", "adb") for c in runner.calls for a in c["argv"]))
        self.assertEqual(json.loads((fx.run_dir / "relay.json").read_text()), live_actors.relay_config(run))
        self.assertEqual(fx.log.read_text().splitlines(),
                         ["relay", "capacity", "-c", str(fx.run_dir / "relay.json"),
                          "relay", "capacity", "-c", str(fx.run_dir / "relay.json"),
                          "publisher", "--track-config-selftest", "--track", "avc1.42C01E", "320x240", "24000",
                          "--avc-config-from-first-sync", str(fx.run_dir / "objects.bin")])
        provider = next(c for c in runner.calls if c["name"] == "relay provider")
        self.assertEqual(provider["env"], dict(live_actors.actor_argv(run)["env"], DYLD_PRINT_LIBRARIES="1"))
        self.assertEqual(result["open_obligations"], live_actors.OPEN_OBLIGATIONS)
        self.assertIn("serve process", result["open_obligations"][0])

    def test_each_defect_is_refused_by_name(self):
        def missing_relay(fx):
            fx.relay.unlink()

        def unexecutable_publisher(fx):
            os.chmod(fx.publisher, 0o644)

        def missing_package(fx):
            (fx.package / "moq5" / "__init__.py").unlink()

        def short_objects(fx):
            write_objects(fx.run_dir / "objects.bin", n=59)

        def open_key(fx):
            os.chmod(fx.run_dir / "creds" / "relay.key", 0o644)

        cases = (
            ("missing relay", missing_relay, None, {"relay_binary": "is not a regular file", "relay_config": "FileNotFoundError",
                                                    "relay_provider": "FileNotFoundError"}),
            ("unexecutable publisher", unexecutable_publisher, None,
             {"publisher_binary": "is not executable", "publisher_track": "PermissionError"}),
            ("missing package", missing_package, None, {"package": "__init__.py is missing", "host_argv": "package: no moq5 package"}),
            ("59 objects", short_objects, None, {"objects": "has 59 records; the run declares 60"}),
            ("missing adb", lambda fx: fx.adb.unlink(), None, {"adb_binary": "is not a regular file"}),
            ("unexecutable adb", lambda fx: os.chmod(fx.adb, 0o644), None, {"adb_binary": "is not executable"}),
            ("open key", open_key, None, {"credential_modes": "relay.key 0o644, expected 0o600"}),
            ("relay refuses config", lambda fx: fx.mode("relay", "refuse"), None,
             {"relay_config": "relay capacity rc 2: moq5-relay: listener.port: out of range",
              "relay_provider": "capacity rc 2"}),
            ("publisher track differs", lambda fx: fx.mode("publisher", "wrong"), None, {"publisher_track": "avc1.42E01E"}),
            ("relay capacity times out", None, "relay capacity", {"relay_config": "relay capacity timed out after 15.0s"}),
        )
        for label, damage, timeout_for, want in cases:
            with self.subTest(label):
                fx = Fixture(self)
                if damage:
                    damage(fx)
                result = live_actors.preflight(fx.live_run(), RecordingRunner(timeout_for))
                got = self.failing(result)
                self.assertEqual(set(got), set(want), json.dumps(got, indent=1))
                for name, needle in want.items():
                    self.assertIn(needle, got[name])
                self.assertIs(result["ok"], False)

    def test_a_certificate_without_the_emulator_address_is_refused(self):
        fx = Fixture(self, san="DNS:localhost,IP:127.0.0.1")        # the host-only runtime recipe's names
        got = self.failing(live_actors.preflight(fx.live_run(), RecordingRunner()))
        self.assertEqual(set(got), {"certificate_chain", "certificate_san"}, json.dumps(got, indent=1))
        self.assertTrue(got["certificate_chain"].startswith("10.0.2.2: rc 2:"), got["certificate_chain"])
        self.assertIn("IP address mismatch", got["certificate_chain"])
        self.assertIn("DNS:localhost, IP Address:127.0.0.1", got["certificate_san"])

    def test_a_certificate_from_another_ca_is_refused(self):
        fx, other = Fixture(self), Fixture(self)
        os.replace(other.run_dir / "creds" / "ca.pem", fx.run_dir / "creds" / "ca.pem")
        got = self.failing(live_actors.preflight(fx.live_run(), RecordingRunner()))
        self.assertEqual(set(got), {"run_credentials", "certificate_chain", "certificate_wrong_ip_refused"}, json.dumps(got, indent=1))
        self.assertIn("unable to get local issuer certificate", got["certificate_chain"])
        self.assertIn("CA DER sha256 ", got["run_credentials"])             # not the CA this run generated


class PreflightProvider(unittest.TestCase):
    """The relay and the MsQuic provider it loads are the reviewed artifacts: digests declared up front,
    the loaded image judged from the loader itself under the serve environment."""

    def failing(self, fx, runner=None, **kw):
        result = live_actors.preflight(fx.live_run(**kw), runner or RecordingRunner())
        return {c["name"]: c["reason"] for c in result["checks"] if not c["ok"]}

    def test_the_declared_provider_loaded_by_the_relay_passes(self):
        self.assertEqual(self.failing(Fixture(self)), {})

    def test_each_departure_from_the_declared_provider_is_refused(self):
        def same_name_elsewhere(fx):
            other = fx.root / "another build" / "libmsquic.2.6.0.dylib"
            other.parent.mkdir()
            other.write_bytes(fx.provider.read_bytes())                    # identical bytes, another artifact
            fx.provider_lines.write_text(f"{other}\n")

        cases = (
            ("no provider loaded", lambda fx: fx.provider_lines.write_text(""), None,
             "relay provider: 0 libmsquic images loaded"),
            ("two providers loaded", lambda fx: fx.provider_lines.write_text(f"{fx.provider}\n{fx.provider}\n"), None,
             "relay provider: 2 libmsquic images loaded"),
            ("the same name from another build", same_name_elsewhere, None, "relay provider: loaded "),
            ("an unplanned diagnostic", None, RecordingRunner(append={"relay provider": ("stderr", "warning: rounding\n")}),
             "relay provider: unplanned stderr ['warning: rounding']"),
            ("a loader line from another process", None,
             RecordingRunner(append={"relay provider": ("stderr", "dyld[1]: move loaded to delayed: CoreWiFi\n")}),
             "relay provider: loader lines from 2 processes"),
            ("a capacity report off its grammar", None, RecordingRunner(replace={"relay provider": ("stdout", "ceiling: 1\n")}),
             "relay provider: capacity line 1"))
        for label, damage, runner, needle in cases:
            with self.subTest(label):
                fx = Fixture(self)
                if damage:
                    damage(fx)
                got = self.failing(fx, runner)
                self.assertEqual(set(got), {"relay_provider"}, got)
                self.assertTrue(got["relay_provider"].startswith(needle), got["relay_provider"])

    def test_altered_bytes_are_refused_by_digest(self):
        fx = Fixture(self)
        declared = fx.provider_digest
        fx.provider.write_bytes(b"altered after it was declared\n")
        got = self.failing(fx)
        self.assertEqual(set(got), {"relay_provider"}, got)
        self.assertIn(f"provider {fx.provider} sha256 ", got["relay_provider"])
        self.assertIn(f"declared {declared}", got["relay_provider"])
        fx = Fixture(self)
        fx.relay.write_text(fx.relay.read_text() + "# altered after it was declared\n")
        got = self.failing(fx)
        self.assertEqual(set(got), {"relay_provider"}, got)
        self.assertIn(f"relay binary {fx.relay} sha256 ", got["relay_provider"])


class ServeQualification(unittest.TestCase):
    """The owned `serve` process's own loader evidence: the relay is spawned as a directly owned child
    with the serve environment and the loader's image list, judged ready by its declared listener's
    record, terminated and reaped, and only then are ITS loader lines judged against its pid."""
    READY_S, SETTLE_S = 10.0, 2.0

    def setUp(self):
        self.fx = Fixture(self)
        self.run = self.fx.live_run()
        refused = live_actors.preflight(self.run)
        self.assertTrue(refused["ok"], json.dumps(refused, indent=1))

    def qualify(self, **kw):
        return live_actors.qualify_serve(self.run, ready_s=self.READY_S, settle_s=self.SETTLE_S, **kw)

    def assert_refused(self, got, needle):
        self.assertIsNotNone(got["refusal"], json.dumps(got, indent=1, default=str))
        self.assertIn(needle, got["refusal"])
        self.assertEqual(got["retained"], [], got["retained"])

    def test_the_serve_process_that_loads_the_declared_provider_qualifies(self):
        got = self.qualify()
        self.assertIsNone(got["refusal"], json.dumps(got, indent=1, default=str))
        self.assertEqual(got["argv"], live_actors.actor_argv(self.run)["relay"])
        self.assertEqual(got["env"], dict(live_actors.serve_env(), DYLD_PRINT_LIBRARIES="1"))
        self.assertEqual(got["ready"]["outcome"], "on-time")
        self.assertEqual(got["ready_record"]["port"], OWNED_PORT)
        settled = got["settled"]
        self.assertEqual((settled["pid"], settled["how"], settled["exit"], settled["readers_settled"]),
                         (got["pid"], "SIGTERM", 0, True))
        self.assertEqual(Path(got["loader"]["provider_image"]).resolve(), self.fx.provider.resolve())
        self.assertEqual(got["loader"]["pids"], [str(got["pid"])])
        self.assertIn("usable client bindings per shard: 1024", got["loader"]["own_lines"][-1])

    def test_it_depends_on_the_relay_inputs_only(self):
        run = self.fx.live_run(package=self.fx.root / "absent package", publisher_binary=self.fx.root / "absent publisher")
        got = live_actors.qualify_serve(run, ready_s=self.READY_S, settle_s=self.SETTLE_S)
        self.assertIsNone(got["refusal"], json.dumps(got, indent=1, default=str))
        self.assertEqual(got["argv"], [str(self.fx.relay), "serve", "-c", str(run.relay_config_path)])

    def test_the_provider_image_departures_refuse(self):
        def same_name_elsewhere(fx):
            other = fx.root / "another build" / fx.provider.name
            other.parent.mkdir()
            other.write_bytes(fx.provider.read_bytes())
            fx.provider_lines.write_text(f"{other}\n")
        for label, rig, needle in (
                ("no provider image", lambda fx: fx.provider_lines.write_text(""), "0 libmsquic images loaded"),
                ("two provider images", lambda fx: fx.provider_lines.write_text(f"{fx.provider}\n{fx.provider}\n"),
                 "2 libmsquic images loaded"),
                ("the same name from another build", same_name_elsewhere, "relay serve: loaded ")):
            with self.subTest(label):
                self.setUp()
                rig(self.fx)
                self.assert_refused(self.qualify(), needle)

    def test_capacity_loader_evidence_does_not_stand_in_for_serve(self):
        self.fx.mode("serve", "no provider in serve")
        self.assertTrue(live_actors.preflight(self.run)["ok"])            # the capacity process still loads it
        self.assert_refused(self.qualify(), "relay serve: 0 libmsquic images loaded")

    def test_loader_lines_of_another_process_refuse(self):
        self.fx.mode("serve", "foreign pid")
        self.assert_refused(self.qualify(), "expected only the serve process")

    def test_unexpected_diagnostics_refuse(self):
        for mode, needle in (("loader diagnostic", "relay serve: unplanned loader line"),
                             ("warn", "unplanned diagnostic: relay [stderr]: warning: pool capacity rounded up")):
            with self.subTest(mode):
                self.fx.mode("serve", mode)
                got = self.qualify()
                self.assert_refused(got, needle)
                self.assertEqual((got["settled"]["how"], got["settled"]["exit"]), ("SIGTERM", 0))

    def test_exit_before_readiness_refuses_with_the_childs_output_retained(self):
        self.fx.mode("serve", "occupied")
        got = self.qualify()
        self.assert_refused(got, "relay serve readiness (raw 127.0.0.1:46443 moqt-18+moqt-16 lanes 1): exited")
        self.assertIsNone(got["loader"])                                   # an unready process qualifies nothing
        self.assertEqual((got["settled"]["how"], got["settled"]["exit"]), ("exited", 1))
        self.assertIn("transport create failed (port 46443)", got["settled"]["stderr"])

    def test_readiness_absent_within_its_window_refuses_and_the_child_is_reaped(self):
        self.fx.mode("serve", "never ready")
        got = live_actors.qualify_serve(self.run, ready_s=0.5, settle_s=self.SETTLE_S)
        self.assert_refused(got, "): absent within 0.5s")
        self.assertIsNone(got["loader"])
        self.assertEqual((got["settled"]["how"], got["settled"]["exit"], got["settled"]["readers_settled"]),
                         ("SIGTERM", 0, True))

    def test_a_child_that_does_not_end_at_sigterm_is_a_cleanup_failure(self):
        self.fx.mode("serve", "ignore term")
        got = self.qualify()
        self.assert_refused(got, "relay serve: ended SIGTERM+SIGTERM+SIGKILL rc -9, expected SIGTERM rc 0")

    def test_a_settle_that_raises_names_the_retained_child(self):
        spawned = []

        class NoSettle(live_actors.Child):
            def settle(self, *a, **kw):
                raise OSError("settle refused")

        def spawn(*a, **kw):
            spawned.append(NoSettle(*a, **kw))
            return spawned[-1]
        got = self.qualify(spawn=spawn)
        child = spawned[0]
        self.addCleanup(lambda: live_actors.Child.settle(child, self.SETTLE_S, signal_first=signal.SIGTERM))
        self.assertIn("relay serve: settle raised OSError('settle refused')", got["refusal"])
        self.assertEqual(got["retained"], [f"relay serve pid {child.proc.pid}: not proven reaped"])
        self.assertIsNone(got["settled"])
        self.assertIsNone(got["loader"])                                   # nothing is judged from an unsettled child

    def test_declared_artifacts_are_checked_before_any_spawn(self):
        self.fx.provider.write_bytes(b"altered after it was declared\n")
        got = self.qualify()
        self.assert_refused(got, f"provider {self.fx.provider} sha256 ")
        self.assertIsNone(got["pid"])
        self.assertNotIn("serve", self.fx.log.read_text().split())


class FreshCredentials(unittest.TestCase):
    """The run's CA and relay key are generated fresh, here, in a directory this call creates."""

    def run_dir(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        d = Path(tmp.name).resolve() / "run"
        d.mkdir()
        (d / "objects.bin").write_bytes(b"not ours to touch")
        return d

    def test_a_fresh_run_gets_private_credentials_and_a_receipt(self):
        d = self.run_dir()
        receipt, refused = live_actors.fresh_credentials(d, nonce="0123456789abcdef")
        self.assertIsNone(refused)
        self.assertEqual(receipt["nonce"], "0123456789abcdef")
        self.assertTrue(receipt["ca_subject"].endswith("moq5 run CA 0123456789abcdef"), receipt["ca_subject"])
        self.assertRegex(receipt["ca_der_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(json.loads((d / "credentials.json").read_text()), receipt)
        self.assertEqual(stat.S_IMODE((d / "creds").stat().st_mode), 0o700)
        for key in ("ca.key", "relay.key"):
            self.assertEqual(stat.S_IMODE((d / "creds" / key).stat().st_mode), 0o600)
        der = subprocess.run([OPENSSL, "x509", "-in", str(d / "creds" / "ca.pem"), "-outform", "DER"],
                             capture_output=True, check=True, timeout=15).stdout
        self.assertEqual(hashlib.sha256(der).hexdigest(), receipt["ca_der_sha256"])
        two, _ = live_actors.fresh_credentials(self.run_dir())
        self.assertNotEqual(two["nonce"], receipt["nonce"])                  # a nonce per run by default

    def test_an_existing_destination_is_refused_and_left_as_it_was(self):
        d = self.run_dir()
        (d / "creds").mkdir()
        (d / "creds" / "someone else's.pem").write_text("theirs")
        receipt, refused = live_actors.fresh_credentials(d)
        self.assertIsNone(receipt)
        self.assertTrue(refused.startswith(f"credentials: {d / 'creds'} already exists"), refused)
        self.assertEqual([x.name for x in (d / "creds").iterdir()], ["someone else's.pem"])
        self.assertFalse((d / "credentials.json").exists())
        d = self.run_dir()
        (d / "credentials.json").write_text("{}")
        receipt, refused = live_actors.fresh_credentials(d)
        self.assertTrue(refused.startswith(f"credentials: {d / 'credentials.json'} already exists"), refused)
        self.assertFalse((d / "creds").exists())
        self.assertEqual((d / "credentials.json").read_text(), "{}")

    def test_a_partial_generation_removes_only_what_it_created(self):
        for step in ("ca", "relay key", "relay cert", "ca fingerprint"):
            with self.subTest(step):
                d = self.run_dir()

                def runner(name, argv, timeout, env=None, step=step):
                    if name == step:
                        return {"rc": 1, "stdout": "", "stderr": f"{name}: failed\n", "timeout": False}
                    return cmd(argv, timeout=timeout, env=env)
                receipt, refused = live_actors.fresh_credentials(d, runner=runner)
                self.assertIsNone(receipt)
                self.assertTrue(refused.startswith(f"credentials: {step}: rc 1"), refused)
                self.assertFalse((d / "creds").exists())
                self.assertFalse((d / "credentials.json").exists())
                self.assertEqual(sorted(x.name for x in d.iterdir()), ["objects.bin"])
                self.assertEqual((d / "objects.bin").read_bytes(), b"not ours to touch")

    def test_the_preflight_binds_the_ca_to_the_runs_receipt(self):
        fx = Fixture(self)
        receipt = json.loads((fx.run_dir / "credentials.json").read_text())
        (fx.run_dir / "credentials.json").write_text(json.dumps(dict(receipt, nonce="feedfacefeedface")))
        result = live_actors.preflight(fx.live_run(), RecordingRunner())
        got = {c["name"]: c["reason"] for c in result["checks"] if not c["ok"]}
        self.assertEqual(set(got), {"run_credentials"}, got)
        self.assertIn("nonce 'feedfacefeedface'", got["run_credentials"])
        (fx.run_dir / "credentials.json").unlink()
        got = {c["name"]: c["reason"] for c in live_actors.preflight(fx.live_run(), RecordingRunner())["checks"] if not c["ok"]}
        self.assertEqual(set(got), {"run_credentials"}, got)
        self.assertIn("FileNotFoundError", got["run_credentials"])


class CredentialDiagnostics(unittest.TestCase):
    """The credential commands' output is judged as every other preflight command's: exactly what the
    command plans, at rc 0 as well."""

    def run_dir(self):
        return FreshCredentials.run_dir(self)

    def test_an_unplanned_diagnostic_fails_generation_and_retires_its_files(self):
        for step in ("ca", "relay key", "relay cert", "ca subject", "ca fingerprint"):
            for stream in ("stderr", "stdout"):
                with self.subTest(step=step, stream=stream):
                    d = self.run_dir()

                    def runner(name, argv, timeout, env=None, step=step, stream=stream):
                        r = cmd(argv, timeout=timeout, env=env)
                        if name == step:
                            r[stream] += "warning: injected diagnostic\n"
                        return r
                    receipt, refused = live_actors.fresh_credentials(d, runner=runner)
                    self.assertIsNone(receipt, "a diagnostic at rc 0 was accepted")
                    self.assertTrue(refused.startswith(f"credentials: {step}: unplanned output"), refused)
                    self.assertEqual(sorted(x.name for x in d.iterdir()), ["objects.bin"])

    def test_an_unplanned_diagnostic_fails_the_preflight_credential_check(self):
        for step in ("ca subject", "ca fingerprint"):
            for stream in ("stderr", "stdout"):
                with self.subTest(step=step, stream=stream):
                    fx = Fixture(self)
                    runner = RecordingRunner(append={step: (stream, "warning: injected diagnostic\n")})
                    result = live_actors.preflight(fx.live_run(), runner)
                    got = {c["name"]: c["reason"] for c in result["checks"] if not c["ok"]}
                    self.assertEqual(set(got), {"run_credentials"}, got)
                    self.assertIn("unplanned output", got["run_credentials"])


class ReceiptOwnership(unittest.TestCase):
    """The receipt this call creates exclusively is this call's to retire if writing it fails."""

    def run_dir(self):
        return FreshCredentials.run_dir(self)

    class FailingOs:
        """`os` for live_actors only, with `write` or `close` failing on the receipt's descriptor."""

        def __init__(self, fail):
            self.fail, self.fds = fail, set()

        def __getattr__(self, name):
            return getattr(os, name)

        def open(self, path, flags, mode=0o777):
            fd = os.open(path, flags, mode)
            if str(path).endswith("credentials.json"):
                self.fds.add(fd)
            return fd

        def write(self, fd, data):
            if fd in self.fds and self.fail == "write":
                os.write(fd, data[:5])                                     # partial content, then the failure
                raise OSError("receipt write failure")
            return os.write(fd, data)

        def close(self, fd):
            os.close(fd)
            if fd in self.fds and self.fail == "close":
                raise OSError("receipt close failure")

    def test_a_failed_receipt_write_or_close_is_retired(self):
        for fail in ("write", "close"):
            with self.subTest(fail):
                d = self.run_dir()
                fake = self.FailingOs(fail)
                original = live_actors.os
                live_actors.os = fake
                try:
                    receipt, refused = live_actors.fresh_credentials(d)
                finally:
                    live_actors.os = original
                self.assertIsNone(receipt)
                self.assertIn(f"receipt {fail} failure", refused)
                self.assertEqual(sorted(x.name for x in d.iterdir()), ["objects.bin"], refused)
                self.assertEqual((d / "objects.bin").read_bytes(), b"not ours to touch")

    def test_an_unretirable_receipt_is_named(self):
        d = self.run_dir()
        fake = self.FailingOs("write")
        fake.unlink = lambda path, missing_ok=False: (_ for _ in ()).throw(OSError("unlink refused")) \
            if str(path).endswith("credentials.json") else os.unlink(path)
        original = live_actors.os
        live_actors.os = fake
        try:
            receipt, refused = live_actors.fresh_credentials(d)
        finally:
            live_actors.os = original
        self.assertIsNone(receipt)
        self.assertIn(f"retained: {d / 'credentials.json'}", refused)


class PreflightInterpreter(unittest.TestCase):
    """The host actor's interpreter is an argv input like the two binaries: it must be spawnable."""

    def test_the_host_interpreter_must_be_an_executable_file(self):
        fx = Fixture(self)
        unexecutable = fx.bin / "python3 copy"
        unexecutable.write_text("#!/bin/sh\n")
        os.chmod(unexecutable, 0o644)
        for label, python, want in (("running interpreter", str(Path(sys.executable).resolve()), None),
                                    ("absent", "/absent/python", "is not a regular file"),
                                    ("not executable", str(unexecutable), "is not executable"),
                                    ("a directory", str(fx.bin), "is not a regular file")):
            with self.subTest(label):
                result = live_actors.preflight(fx.live_run(python=python), RecordingRunner())
                got = {c["name"]: c["reason"] for c in result["checks"] if not c["ok"]}
                if want is None:
                    self.assertEqual(got, {})
                    self.assertIs(result["ok"], True)
                else:
                    self.assertEqual(set(got), {"host_python"}, json.dumps(got, indent=1))
                    self.assertIn(want, got["host_python"])
                    self.assertIs(result["ok"], False)


class PreflightDiagnostics(unittest.TestCase):
    """A local command's output is exactly what its check plans for; any other line -- a warning
    at rc 0 above all -- fails that check by name, and a clean response passes."""

    def test_unplanned_output_fails_the_check_that_saw_it(self):
        cases = (
            ("openssl verify warning at rc 0", {"append": {"verify 10.0.2.2": ("stderr", "warning: weak digest\n")}}, None,
             "certificate_chain", "warning: weak digest"),
            ("openssl san warning at rc 0", {"append": {"san": ("stderr", "warning: legacy extension\n")}}, None,
             "certificate_san", "warning: legacy extension"),
            ("wrong-ip mismatch plus a warning", {"append": {"verify 10.0.2.3": ("stderr", "warning: weak digest\n")}}, None,
             "certificate_wrong_ip_refused", "warning: weak digest"),
            ("wrong-ip mismatch text at rc 0", {"rc": {"verify 10.0.2.3": 0}}, None,
             "certificate_wrong_ip_refused", "10.0.2.3 was not refused: rc 0"),
            ("capacity stdout warning at rc 0", {}, ("relay", "warn"), ("relay_config", "relay_provider"),
             "warning: pool capacity rounded up"),
            ("publisher stdout warning at rc 0", {}, ("publisher", "warn"), "publisher_track", "warning: track declared twice"),
        )
        for label, runner_kw, mode, name, needle in cases:
            with self.subTest(label):
                fx = Fixture(self)
                if mode:
                    fx.mode(*mode)
                result = live_actors.preflight(fx.live_run(), RecordingRunner(**runner_kw))
                got = {c["name"]: c["reason"] for c in result["checks"] if not c["ok"]}
                names = set(name) if isinstance(name, tuple) else {name}
                self.assertEqual(set(got), names, json.dumps(got, indent=1))
                for each in names:
                    self.assertIn(needle, got[each])
                self.assertIs(result["ok"], False)

    def test_the_wrong_address_refusal_is_exactly_the_mismatch(self):
        fx = Fixture(self)
        runner = RecordingRunner()
        self.assertIs(live_actors.preflight(fx.live_run(), runner)["ok"], True)
        wrong = [c for c in runner.calls if c["name"] == "verify 10.0.2.3"]
        self.assertEqual(len(wrong), 1)
        r = cmd(wrong[0]["argv"])
        cert = fx.run_dir / "creds" / "relay.pem"
        self.assertEqual((r["rc"], r["stdout"]), (2, ""))
        self.assertEqual(r["stderr"].splitlines(), [f"CN=moq5 run relay {fx.credentials['nonce']}",
                                                    "error 64 at 0 depth lookup: IP address mismatch",
                                                    f"error {cert}: verification failed"])


class PreflightCapacityReport(unittest.TestCase):
    """`capacity` stdout is judged by the report's line grammar: every required row present, the
    optional rows only in their order and at most once, and no other line. Numbers are free."""

    def failing(self, fx):
        result = live_actors.preflight(fx.live_run(), RecordingRunner())
        return {c["name"]: c["reason"] for c in result["checks"] if not c["ok"]}, result["ok"]

    def test_reports_with_any_subset_of_optional_rows_pass(self):
        for label, optional in (("no optional rows", []), ("admin rows only", CAPACITY_OPTIONAL[2:]),
                                ("every optional row", CAPACITY_OPTIONAL)):
            with self.subTest(label):
                fx = Fixture(self)
                fx.capacity_report(CAPACITY_HEAD + optional + CAPACITY_TAIL)
                self.assertEqual(self.failing(fx), ({}, True))

    def test_a_report_off_the_grammar_fails_relay_config_by_name(self):
        head, tail, opt = CAPACITY_HEAD, CAPACITY_TAIL, CAPACITY_OPTIONAL
        cases = (
            ("an error line after the report at rc 0", CAPACITY_REPORT + ["error: invalid capacity"], "error: invalid capacity"),
            ("the heading missing", head[1:] + tail, "ceiling"),
            ("the closing row missing", head + opt[2:], "usable client bindings"),
            ("the per-shard row missing", head[:4] + tail, "per shard"),
            ("the note reworded", head[:1] + ["  (allocator requests only)"] + head[2:] + tail, "allocator requests only"),
            ("a non-numeric ceiling", ["relay-state allocation-request ceiling: many bytes"] + head[1:] + tail, "many bytes"),
            ("optional rows out of order", head + [opt[3], opt[2]] + tail, "admin endpoint"),
            ("an optional row twice", head + [opt[2], opt[2]] + tail, "admin endpoint"),
            ("an unknown row", head + ["  spare pool: 1 bytes"] + tail, "spare pool"),
            ("a row after the closing row", head + tail + [opt[0]], "cross-shard"),
            ("an empty report", [], "ceiling"),
            ("a non-ASCII digit in the heading", ["relay-state allocation-request ceiling: \u0661 bytes"] + head[1:] + tail,
             "capacity line 1: 'relay-state allocation-request ceiling: \u0661 bytes', expected the ceiling heading"),
            ("a non-ASCII digit in the per-shard row",
             head[:4] + ["  per shard: structure 1 + payloads \u0662 + binding 3 + trace 4 bytes"] + tail,
             "capacity line 5: '  per shard: structure 1 + payloads \u0662 + binding 3 + trace 4 bytes', expected the 'per shard:' row"),
            ("a non-ASCII digit in the closing row", head + ["  usable client bindings per shard: \u0967\u0968"],
             "capacity line 6: '  usable client bindings per shard: \u0967\u0968', expected an optional row in order, "
             "or the closing 'usable client bindings per shard' row"),
        )
        for label, lines, needle in cases:
            with self.subTest(label):
                fx = Fixture(self)
                fx.capacity_report(lines)
                got, ok = self.failing(fx)
                # the report is the relay's own stdout, so both of its capacity runs judge it
                self.assertEqual(set(got), {"relay_config", "relay_provider"}, json.dumps(got, indent=1))
                self.assertIn(needle, got["relay_config"])
                self.assertIn(needle, got["relay_provider"])
                self.assertIs(ok, False)


    def test_capacity_stderr_at_rc_0_fails_relay_config(self):
        fx = Fixture(self)
        result = live_actors.preflight(fx.live_run(), RecordingRunner(append={"relay capacity": ("stderr", "note: budgets rounded\n")}))
        got = {c["name"]: c["reason"] for c in result["checks"] if not c["ok"]}
        self.assertEqual(set(got), {"relay_config"}, json.dumps(got, indent=1))
        self.assertIn("note: budgets rounded", got["relay_config"])


class PreflightTrackRecord(unittest.TestCase):
    """The self-test's one line is exactly the declared `track_config` event: every field, the
    exact integer values (no bools), and no other field."""

    def test_the_track_record_is_the_complete_declared_event(self):
        def line(**changes):
            record = dict(TRACK_CONFIG, **changes)
            return json.dumps({k: v for k, v in record.items() if v is not None}) + "\n"
        extra = dict(TRACK_CONFIG, profile="baseline")
        cases = (
            ("timescale and bitrate wrong", line(timescale=1, bitrate=0), "timescale"),
            ("timescale missing", line(timescale=None), "timescale"),
            ("bitrate missing", line(bitrate=None), "bitrate"),
            ("bitrate as a string", line(bitrate="400000"), "bitrate"),
            ("width as a float", line(width=320.0), "width"),
            ("declared as 1", line(declared=1), "declared"),
            ("timescale as true", line(timescale=True), "timescale"),
            ("an unplanned field", json.dumps(extra) + "\n", "profile"),
            ("no init_data", line(init_data=None), "init_data"),
            ("an empty init_data", line(init_data=""), "init_data is empty"),
            ("init_data not hex", line(init_data="zz"), "init_data"),
            ("the avcC of another SPS", line(init_data=FIXTURE_AVCC.replace(FIXTURE_SPS, bytes.fromhex("6764001facd9"))
                                                                  .replace(b"\x42\xc0\x1e", b"\x64\x00\x1f", 1)
                                                                  .replace(b"\x00\x08", b"\x00\x06", 1).hex()), "SPS"),
            ("an avcC without the PPS", line(init_data=FIXTURE_AVCC[:6 + 2 + len(FIXTURE_SPS)].hex() + "00"), "PPS"),
        )
        for label, stdout, needle in cases:
            with self.subTest(label):
                fx = Fixture(self)
                result = live_actors.preflight(fx.live_run(), RecordingRunner(replace={"publisher track": ("stdout", stdout)}))
                got = {c["name"]: c["reason"] for c in result["checks"] if not c["ok"]}
                self.assertEqual(set(got), {"publisher_track"}, json.dumps(got, indent=1))
                self.assertIn(needle, got["publisher_track"])
                self.assertIs(result["ok"], False)

    def test_the_declared_event_itself_passes(self):
        fx = Fixture(self)
        runner = RecordingRunner(replace={"publisher track": ("stdout", json.dumps(TRACK_CONFIG) + "\n")})
        self.assertIs(live_actors.preflight(fx.live_run(), runner)["ok"], True)


class LiveModeRefusesWithoutDeclaredInputs(unittest.TestCase):
    def test_live_without_its_inputs_is_refused_with_and_without_authorization(self):
        for argv in (["run_android_media.py", "--live"], ["run_android_media.py", "--live", "--authorized"]):
            with self.subTest(argv=argv):
                err = io.StringIO()
                with contextlib.redirect_stderr(err):
                    rc = run_android_media.main(argv)
                self.assertEqual(rc, 2)
                self.assertTrue(err.getvalue().startswith("run_android_media --live: refused: "), err.getvalue())


if __name__ == "__main__":
    unittest.main()
