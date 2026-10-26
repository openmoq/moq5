"""The live run's three host-side actors, derived from one declared identity, and the
offline preflight that judges their inputs before anything is spawned.

    relay       moq5-relay serve -c <run>/relay.json      raw QUIC on 127.0.0.1:<port>, drafts 18 and 16
    publisher   moq5_runtime_publisher (pull, positive)   the run's objects file, avc1.42C01E 320x240 24000
    host        python -I host_receiver.py ...            the installed moq5 package, draft 18, RAW/PICOQUIC

The listener port is the run's own declared port, never the player's default 4443 (whose default
URL `moqt://10.0.2.2:4443` would reach whatever else holds that port). The player is launched with
`player_url(run)` as a launch extra; nothing is typed into its fields. The emulator reaches the
host's loopback as 10.0.2.2, so the relay certificate names both addresses; the host-side clients
verify it as 127.0.0.1.

`preflight` judges exactly the inputs named in PREFLIGHT_CHECKS, with local commands only:
`openssl` on the run's credential files, the relay's `capacity -c` (which validates the
configuration without opening a listener) and the publisher's `--track-config-selftest`. It never
starts a listener, never connects anywhere and never runs adb: the declared adb is judged as an
executable file only. Every check runs and is recorded
by name; the run may start only when all pass. A command's output must be exactly what its check
plans for: any other line, a warning at rc 0 included, fails that check. The `capacity` report
is judged by its line grammar (CAPACITY_HEAD / CAPACITY_OPTIONAL / CAPACITY_CLOSING), the track
self-test by its one complete `track_config` record.

It does not prove that the actors' shared libraries load, that the UDP listener or admin port is
free, what the Android trust store holds, or how a real relay binary treats the configuration
(`capacity` is judged here through whatever relay binary the run names). Those remain separate
acceptance gates.

`qualify_serve` is the host-only qualification of the relay's `serve` process itself: one directly
owned serve child, launched with the serve environment and the loader's image list, judged by its
declared listener's readiness record, terminated and reaped, and only then judged by its own loader
lines. It qualifies the artifacts and environment through that one process; a later process owes
its own evidence.
"""
import dataclasses
import hashlib
import json
import os
import re
import secrets
import signal
import stat
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
import device_player                                                      # noqa: E402
import host_receiver                                                      # noqa: E402
import media_oracle                                                       # noqa: E402
import route_dump                                                         # noqa: E402
from run_android_media import (ADB_LOGCAT_EXIT, LIVE_EXITS, Actors, Deadlines, HostIdentity, MonotonicClock, RelayIdentity,  # noqa: E402
                               StampedEvent, await_stamped, host_argv, relay_ready_record)
from runtime_support import EXPECTED_ALPN, MSQUIC_DIR, OPENSSL, WARN, Child, cmd, collect_diagnostics  # noqa: E402

LISTENER_HOST = "127.0.0.1"
PLAYER_DEFAULT_PORT = 4443          # the sample's default URL port: refused as a run's port
EMULATOR_HOST = "10.0.2.2"
SNI = LISTENER_HOST
NAMESPACE_TEXT = "/".join(p.decode() for p in host_receiver.NAMESPACE)
TRACK_TEXT = host_receiver.TRACK.decode()
TRACK_DECL = ("avc1.42C01E", "320x240", "24000")
RELAY_SAN = "IP Address:10.0.2.2, IP Address:127.0.0.1"
RELAY_SAN_EXT = "IP:10.0.2.2,IP:127.0.0.1"          # the same names, as the certificate extension spells them
RUN_CA_NAME = "moq5 run CA"                        # the fresh CA's common name, followed by the run's nonce
PREFLIGHT_COMMAND_S = 15.0
# The two track fields the publisher declares beside the three it takes from --track
# (bindings/python/tests/runtime/publisher.c, emit_track_decl and the write path).
TRACK_TIMESCALE = 90000
TRACK_BITRATE = 400000

# `moq5-relay capacity` stdout, line by line (tools/moq5-relay/cli/main.c, print_capacity): the
# ceiling heading, a fixed three-line note, the per-shard row, the optional rows (each at most
# once, in this order), and the closing row. Numbers are free; wording is not.
_N = r"[0-9]+"                       # printf decimal: ASCII digits only, never any Unicode digit
CAPACITY_HEAD = (("the ceiling heading", re.compile(rf"relay-state allocation-request ceiling: {_N} bytes")),
                 ("the allocator note", re.compile(re.escape("  (allocator requests only \u2014 excludes malloc metadata, "
                                                             "size-class rounding,"))),
                 ("the allocator note", re.compile(re.escape("   fragmentation, thread stacks, transient diagnostics, "
                                                             "and MsQuic/session"))),
                 ("the allocator note", re.compile(re.escape("   memory that remains adapter-owned)"))),
                 ("the 'per shard:' row", re.compile(rf"  per shard: structure {_N} \+ payloads {_N} \+ binding {_N} "
                                                     rf"\+ trace {_N} bytes")))
CAPACITY_OPTIONAL = (re.compile(rf"  cross-shard: {_N} bytes \(runtime, channels, canon keys, staging\)"),
                     re.compile(rf"  cli runtime: {_N} bytes " + re.escape("(snapshot rows, plus the serve context at "
                                                                           "lanes > 1; included above)")),
                     re.compile(rf"  admin endpoint: {_N} bytes " + re.escape(
                         "(listener object, both body banks, and the coordinator's per-lane rows and views, its "
                         "signal-sink body and its /api/v1/info document; included above)")),
                     re.compile(rf"  admin thread stack reservation: {_N} bytes \(not part of the ceiling above\)"))
CAPACITY_CLOSING = ("the closing 'usable client bindings per shard' row", re.compile(rf"  usable client bindings per shard: {_N}"))

_d = Deadlines()
# The host is ACTIVE within the `go` window, and finishes within the phases from `go` to the
# publisher's shutdown record.
HOST_ESTABLISH_S = _d.go_window
HOST_DEADLINE_S = _d.go_window + _d.ack_all + _d.ended_observed + _d.done + _d.shutdown

PREFLIGHT_CHECKS = ("relay_binary", "publisher_binary", "host_python", "adb_binary", "package", "objects", "credential_modes",
                    "run_credentials", "certificate_chain", "certificate_wrong_ip_refused", "certificate_san", "relay_config",
                    "relay_provider", "publisher_track", "host_argv")


@dataclass(frozen=True)
class LiveRun:
    """The declared identity of one live run. Every actor's argv, the relay's configuration
    and its expected readiness record are derived from these fields and nothing else."""
    run_dir: Path
    relay_binary: Path
    publisher_binary: Path
    package: Path
    admin_port: int
    port: int                                     # the relay's UDP listener; declared, never defaulted
    adb: Path                                     # the dedicated device's adb, by absolute path
    relay_sha256: str                             # the reviewed relay build's binary, by digest
    provider: Path                                # the reviewed MsQuic library the relay must load (canonical file)
    provider_sha256: str                          # ... and its digest
    python: str = sys.executable                  # the host actor's interpreter

    def __post_init__(self):
        for name in ("run_dir", "relay_binary", "publisher_binary", "package", "adb", "provider"):
            value = getattr(self, name)
            if not isinstance(value, Path) or not value.is_absolute():
                raise ValueError(f"{name}: must be an absolute path, got {value!r}")
        for name in ("relay_sha256", "provider_sha256"):
            if not isinstance(getattr(self, name), str) or not re.fullmatch(r"[0-9a-f]{64}", getattr(self, name)):
                raise ValueError(f"{name}: must be a lowercase hex sha256, got {getattr(self, name)!r}")
        if not isinstance(self.python, (str, Path)) or not Path(self.python).is_absolute():
            raise ValueError(f"python: must be an absolute path, got {self.python!r}")
        if type(self.port) is not int or not 1024 <= self.port <= 65535 or self.port == PLAYER_DEFAULT_PORT:
            raise ValueError(f"port: must be an integer from 1024 through 65535 other than the player's default "
                             f"{PLAYER_DEFAULT_PORT}, got {self.port!r}")
        if type(self.admin_port) is not int or not 1 <= self.admin_port <= 65535:
            raise ValueError(f"admin_port: must be an integer from 1 through 65535, got {self.admin_port!r}")

    @property
    def creds(self):
        return self.run_dir / "creds"

    @property
    def ca(self):
        return self.creds / "ca.pem"

    @property
    def cert(self):
        return self.creds / "relay.pem"

    @property
    def key(self):
        return self.creds / "relay.key"

    @property
    def credentials_receipt(self):
        return self.run_dir / "credentials.json"

    @property
    def objects(self):
        return self.run_dir / "objects.bin"

    @property
    def audio_objects(self):
        return self.run_dir / "audio-objects.bin"

    @property
    def audio_receipt(self):
        return self.run_dir / "audio-receipt.json"

    @property
    def relay_config_path(self):
        return self.run_dir / "relay.json"

    @property
    def url(self):
        return f"moqt://{LISTENER_HOST}:{self.port}"


# The publisher's harness-only option: the catalog carries the track's avcC, built by the public
# codec-signaling utility from the Annex B SPS/PPS of the run's first (sync) object.
AVC_CONFIG_FLAG = "--avc-config-from-first-sync"
_NAL_START = re.compile(b"\x00\x00\x01")


def _annexb_nals(payload):
    """The NAL units of an Annex B payload, each without its start code or trailing zero bytes."""
    return [nal.rstrip(b"\x00") for nal in _NAL_START.split(payload)[1:]]


def avc_config_error(init_hex, objects_path):
    """None when `init_hex` is an avcC carrying exactly the SPS and PPS the run's first object opens
    with (and that SPS's profile, compatibility and level); otherwise the departure, named. It reads
    the record, it does not build one: the publisher's record is the utility's."""
    try:
        record = bytes.fromhex(init_hex)
    except (TypeError, ValueError):
        return f"init_data {init_hex!r} is not hex"
    if not record:
        return "init_data is empty"
    objects = media_oracle.load_objects(objects_path)
    if not objects or not objects[0].is_sync:
        return "the run's first object is not a sync object"
    nals = _annexb_nals(objects[0].payload)
    sps = [n for n in nals if n and n[0] & 0x1F == 7]
    pps = [n for n in nals if n and n[0] & 0x1F == 8]
    if not sps or not pps:
        return f"the run's first object carries {len(sps)} SPS and {len(pps)} PPS"
    pos, got = 6, {"SPS": [], "PPS": []}
    try:
        if record[0] != 1 or record[1:4] != sps[0][1:4]:
            return f"init_data version/profile {record[:4].hex()}, expected 01{sps[0][1:4].hex()} (SPS)"
        counts = (("SPS", record[5] & 0x1F), ("PPS", None))
        for kind, n in counts:
            if n is None:
                n, pos = record[pos], pos + 1
            for _ in range(n):
                size = int.from_bytes(record[pos:pos + 2], "big")
                if size == 0 or pos + 2 + size > len(record):
                    raise IndexError(kind)
                got[kind].append(record[pos + 2:pos + 2 + size])
                pos += 2 + size
    except IndexError as e:
        return f"init_data is truncated in its {e}"
    wrong = [f"init_data {kind} {[x.hex() for x in got[kind]]}, the input's {[x.hex() for x in want]}"
             for kind, want in (("SPS", sps), ("PPS", pps)) if got[kind] != want]
    return "; ".join(wrong) or None


def player_url(run):
    """The player's relay URL: the run's listener as the emulator reaches the host's loopback."""
    return f"moqt://{EMULATOR_HOST}:{run.port}"


# The player's launch configurations: (observer armed, diagnostic trace on). Only "acceptance" can pass;
# "observer-off-diagnostic" is ordinary playback with the breadcrumbs on, for diagnosis only.
# "av-diagnostic" also publishes the run's AAC-LC fixture as a second track and never passes: it is
# neither video acceptance nor A/V presentation acceptance.
PLAYER_MODES = {"acceptance": (True, False), "observer-off-diagnostic": (False, True), "av-diagnostic": (False, True)}
AV_MODE = "av-diagnostic"
AUDIO_TRACK = b"audio"
AUDIO_MAX_OBJECTS = 512                       # the publisher's audio ledger bound


def audio_publisher_check(run, timeout_s=10):
    """(record, None) when the run's publisher, through its own `--audio-config-selftest`, accepts the
    audio declaration and reports exactly the fixture's facts; otherwise (record, the refusal). Runs the
    publisher offline, connecting nowhere."""
    try:
        tail, count = audio_declaration(run)
    except ValueError as e:
        return {"error": str(e)}, str(e)
    argv = [str(run.publisher_binary), "--audio-config-selftest", *tail[1:]]
    try:
        r = subprocess.run(argv, capture_output=True, text=True, timeout=timeout_s, env={"PATH": "/usr/bin:/bin"})
    except (OSError, subprocess.TimeoutExpired) as e:
        return {"argv": argv, "error": repr(e)}, f"audio publisher selftest: {e!r}"
    record = {"argv": argv, "rc": r.returncode, "stdout": r.stdout, "stderr": r.stderr}
    if r.returncode != 0 or r.stderr:
        return record, f"audio publisher selftest: rc {r.returncode} stderr {r.stderr.strip()[:300]!r}"
    try:
        got = json.loads(r.stdout)
    except ValueError:
        return record, f"audio publisher selftest: not one JSON record: {r.stdout[:200]!r}"
    want = {"event": "track_config", "track": "audio", "codec": "mp4a.40.2", "samplerate": int(tail[3]),
            "channel_config": tail[4], "timescale": 90000, "init_data": tail[2], "objects": count}
    if got != want:
        return record, f"audio publisher selftest: declares {got!r}, the fixture is {want!r}"
    return record, None


def audio_declaration(run):
    """The av-diagnostic's audio facts from the run's own fixture: (publisher --audio argv tail, object
    count). The fixture's receipt must be ok and name AAC-LC with a two-byte AudioSpecificConfig, an
    integer rate and channel count; its ledger must load with 1..AUDIO_MAX_OBJECTS objects. Anything
    else raises ValueError, before any actor is built."""
    try:
        receipt = json.loads(run.audio_receipt.read_text())
    except (OSError, ValueError) as e:
        raise ValueError(f"audio: the run's fixture receipt cannot be read: {e!r}") from None
    if receipt.get("ok") is not True or receipt.get("codec") != "mp4a.40.2":
        raise ValueError(f"audio: the fixture receipt is not an ok AAC-LC fixture: ok={receipt.get('ok')!r} "
                         f"codec={receipt.get('codec')!r}")
    asc, rate, channels = receipt.get("audio_specific_config"), receipt.get("sample_rate"), receipt.get("channels")
    if not (isinstance(asc, str) and re.fullmatch(r"[0-9a-f]{4}", asc)):
        raise ValueError(f"audio: the AudioSpecificConfig {asc!r} is not two bytes of lowercase hex")
    for name, value in (("sample rate", rate), ("channels", channels)):
        if type(value) is not int or value <= 0:
            raise ValueError(f"audio: the {name} {value!r} is not a positive integer")
    try:
        count = len(media_oracle.load_objects(run.audio_objects))
    except (OSError, ValueError) as e:
        raise ValueError(f"audio: the run's audio ledger cannot be loaded: {e!r}") from None
    if not 1 <= count <= AUDIO_MAX_OBJECTS:
        raise ValueError(f"audio: {count} audio objects; the publisher declares 1..{AUDIO_MAX_OBJECTS}")
    return ["--audio", str(run.audio_objects), asc, str(rate), str(channels)], count


def player_actor(run, device, apk, n_objects, observe=True, trace=False):
    """The run's player: its relay URL is `player_url(run)`, the same declared port as the relay,
    publisher and host. `compose` builds the live run's player only through this."""
    return device_player.DevicePlayer(device, apk, n_objects, player_url(run), observe=observe, trace=trace)


ADMIN_BODY_BOUND = 1024 * 1024
DIAG_OBSERVE_S = 5.0     # after the one stats request, how long the still-running streams observe before the snapshot
SNAPSHOT_S = 3.0


def relay_boundary_snapshot(admin_port, timeout=SNAPSHOT_S):
    """The relay's state at the run's retirement boundary, through its own read-only interfaces: its
    loopback admin endpoint's /metrics and /api/v1/shards, then one SIGUSR1 route dump (whose
    subscription rows carry each binding's frontier). Each part is bounded and a failed part is named.
    Returns a callable of the owned, still-running relay child."""
    import signal
    import time
    import urllib.request

    def take(relay):
        snap = {"admin": {}, "route": None}
        for path in ("/metrics", "/api/v1/shards"):
            try:
                with urllib.request.urlopen(f"http://{LISTENER_HOST}:{admin_port}{path}", timeout=timeout) as r:
                    snap["admin"][path] = {"status": r.status, "body": r.read(ADMIN_BODY_BOUND).decode("utf-8", "replace")}
            except Exception as e:
                snap["admin"][path] = {"error": repr(e)}
        mark = len(relay.stderr_lines)
        try:
            relay.proc.send_signal(signal.SIGUSR1)
            deadline = time.monotonic() + timeout
            blocks = []
            while not blocks and time.monotonic() < deadline:
                blocks = route_dump.complete_blocks(relay.stderr_lines, mark)
                if not blocks:
                    time.sleep(0.05)
            if not blocks:
                snap["route"] = {"error": f"no complete route block within {timeout}s"}
            else:
                snap["route"] = {"lines": blocks[0], "summary": _route_summary(route_dump.parse(blocks[0]))}
        except Exception as e:
            snap["route"] = {"error": repr(e)}
        return snap
    return take


def boundary_snapshot(player, device, relay_snapshot, observe_wait):
    """The retirement-boundary evidence, taken before the relay is settled. For a traced player, first
    its breadcrumbs must show the diagnostic listener armed on its own thread; only then is ONE recorded
    stats request sent (its send time, argv and status), followed by a fixed DIAG_OBSERVE_S for the
    still-running streams to record the answer and any progress the request itself causes. Then the
    relay snapshot. A listener not shown armed gets no request, and that is recorded. Any other player
    gets the relay snapshot only."""
    def record(name, argv):
        at = time.monotonic()
        r = device.runner(name, argv, device_player.COMMAND_S)
        return {"monotonic": at, "argv": argv, "rc": r.get("rc"), "timeout": r.get("timeout"),
                "stdout": (r.get("stdout") or "")[:2000], "stderr": (r.get("stderr") or "")[:2000]}

    def take(relay):
        armed = request = None
        if player.tracing:
            armed = record("player_listener_armed", player.listener_armed_argv())
            armed["armed"] = armed["rc"] == 0 and device_player.LISTENER_ARMED in armed["stdout"]
            if armed["armed"]:
                request = record("player_stats_request", player.stats_request_argv())
                request["sent_monotonic"] = request.pop("monotonic")
                observe_wait(DIAG_OBSERVE_S)
            else:
                request = {"skipped": "the diagnostic listener was not shown armed"}
        snap = dict(relay_snapshot(relay))
        if armed is not None:
            snap["player_listener_armed"] = armed
            snap["player_stats_request"] = request
        return snap
    return take


def _route_summary(dump):
    """Every track row as the dump states it: its state, upstream, log counts and each subscription."""
    return {"tracks": [{"namespace": "/".join(p.decode("ascii", "backslashreplace") for p in t.namespace),
                        "name": t.name.decode("ascii", "backslashreplace"), "state": t.state,
                        "upstream_binding": t.upstream_binding, "log": dict(t.log),
                        "subscriptions": [dataclasses.asdict(sub) for sub in t.subs]} for t in dump.tracks]}


def relay_config(run):
    return {"listener": {"host": LISTENER_HOST, "port": run.port, "versions": [18, 16], "lanes": 1,
                         "cert": str(run.cert), "key": str(run.key)},
            "admin": {"tcp": {"host": LISTENER_HOST, "port": run.admin_port}},
            "logging": {"format": "json"}, "auth": {"mode": "allow_all"}}


def relay_identity(run):
    return RelayIdentity(listener="raw", host=LISTENER_HOST, port=run.port, alpn_set=EXPECTED_ALPN, lanes=1)


def host_identity(run):
    return HostIdentity(package=run.package, url=run.url, ca_file=run.ca, sni=SNI, objects=run.objects,
                        establish_s=HOST_ESTABLISH_S, deadline_s=HOST_DEADLINE_S)


# What the relay alone adds to `serve_env()`: the loader's image list, its own loader evidence.
RELAY_LOADER_ENV = {"DYLD_PRINT_LIBRARIES": "1"}


def serve_env():
    """The environment every host child runs with; the provider check launches the relay with it too."""
    return {"PATH": "/usr/bin:/bin", "DYLD_FALLBACK_LIBRARY_PATH": MSQUIC_DIR}


def relay_argv(run):
    """The relay's serve argv: the run's relay binary and configuration, and nothing else of the run."""
    return [str(run.relay_binary), "serve", "-c", str(run.relay_config_path)]


def actor_argv(run):
    """The three children's argv, the environment they share, the relay readiness they must
    produce and how each child, the player's `adb logcat` sidecar included, is declared to end."""
    return {"relay": relay_argv(run),
            "publisher": [str(run.publisher_binary), run.url, NAMESPACE_TEXT, TRACK_TEXT, str(run.ca), SNI,
                          str(run.objects), "positive", "pull", "--track", *TRACK_DECL, AVC_CONFIG_FLAG],
            "host": host_argv(host_identity(run), run.python),
            "env": serve_env(),
            "relay_identity": relay_identity(run),
            "expected_exits": dict(LIVE_EXITS)}


def trust_prerequisite(lane, run):
    """None when the lane has placed and verified THIS run's CA -- the certificate whose DER SHA-256 the
    run's credentials receipt records -- into a user store that held no other certificate just before
    (as listed then); otherwise the refusal. Judged afresh at each call. This is an observed snapshot
    of a clean user store, not exclusive trust: the platform's system anchors still apply, and nothing
    here watches the store after the listing."""
    ca = lane.ca
    if ca is None or ca.get("placed") is not True:
        return "trust prerequisite: no verified run CA is placed in the device's user store"
    before = ca.get("store_before")
    if before is None or not before.parseable:
        return f"trust prerequisite: the user store's listing before placement was not parseable: {before!r}"
    if before.entries:
        return (f"trust prerequisite: the user store already held {list(before.entries)} before placement; "
                "a live run needs a store with no other user certificate")
    try:
        wanted = json.loads(run.credentials_receipt.read_text()).get("ca_der_sha256")
    except (OSError, ValueError) as e:
        return f"trust prerequisite: the run's credentials receipt cannot be read: {e!r}"
    if ca.get("ca_der_sha256") != wanted:
        return (f"trust prerequisite: the placed CA (DER sha256 {ca.get('ca_der_sha256')}) is not this run's CA "
                f"(receipt {wanted})")
    return None


def compose(run, lane, apk, player_mode="acceptance", observe_wait=None, relay_snapshot=None):
    """The supervised run's actors for `run` on the dedicated emulator `lane`, and the object count
    they judge: the three host children from `actor_argv`, and one `player_actor` serving every
    player role, whose install and Start are refused unless `trust_prerequisite(lane)` holds at that
    moment. The relay alone also receives RELAY_LOADER_ENV, and the run's verdict judges that relay's
    own settled record with `relay_evidence_judge`. Builds only; runs nothing. The children run with
    PATH=/usr/bin:/bin, so the device's adb must be the run's declared absolute one."""
    device = lane.device
    if device.adb != str(run.adb):
        raise ValueError(f"adb: the device uses {device.adb!r}, not the run's declared {str(run.adb)!r}")
    n_objects = host_receiver.declared_objects()
    argv = actor_argv(run)
    if player_mode not in PLAYER_MODES:
        raise ValueError(f"player mode {player_mode!r}: not one of {sorted(PLAYER_MODES)}")
    observe, trace = PLAYER_MODES[player_mode]
    n_audio, audio_track = None, None
    if player_mode == AV_MODE:
        # The host and the video write ledger judge the video track alone; the player reads both
        # tracks, so its object budget is their checked sum. The observer stays off: its final index
        # is derived from a video-only budget.
        tail, n_audio = audio_declaration(run)
        argv["publisher"] = [*argv["publisher"], *tail]
        audio_track = AUDIO_TRACK
    player_budget = n_objects if n_audio is None else n_objects + n_audio
    if type(player_budget) is not int or not 0 < player_budget <= n_objects + AUDIO_MAX_OBJECTS:
        raise ValueError(f"player budget {player_budget!r} is outside the declared tracks' bound")
    player = player_actor(run, device, apk, player_budget, observe=observe, trace=trace)

    def route_barrier(host_active, player_started, record):
        extra = {} if audio_track is None else {"audio_track": audio_track}
        return route_dump.route_barrier(host_receiver.NAMESPACE, host_receiver.TRACK, host_active, player_started,
                                        record=record, **extra)
    def install(stop, budget_s):
        return trust_prerequisite(lane, run) or player.install(stop, budget_s)

    def start(stop, dispatched, admit):
        return trust_prerequisite(lane, run) or player.start(stop, dispatched, admit)
    install.player = start.player = player
    actors = Actors(relay_argv=argv["relay"], publisher_argv=argv["publisher"], host_argv=argv["host"],
                    player_probe=player.observe, route_barrier=route_barrier, player_start=start,
                    player_install=install, player_prepare=player.prepare, player_log_argv=player.log_argv,
                    relay_identity=argv["relay_identity"], env=argv["env"],
                    expected_exits={**argv["expected_exits"], "player-diag": ADB_LOGCAT_EXIT},
                    relay_env=dict(RELAY_LOADER_ENV), relay_evidence=relay_evidence_judge(run.provider),
                    player_diag_argv=player.diagnostic_argv, audio_objects=n_audio,
                    relay_snapshot=boundary_snapshot(player, lane.device, relay_snapshot or relay_boundary_snapshot(run.admin_port),
                                                     observe_wait or time.sleep))
    return actors, n_objects


# `openssl x509 -fingerprint -sha256`: the SHA-256 of the certificate's DER encoding.
_FINGERPRINT = re.compile(r"sha256 Fingerprint=[0-9A-F]{2}(?::[0-9A-F]{2}){31}")


def _fingerprint_digest(line):
    return line.split("=", 1)[1].replace(":", "").lower()


def fresh_credentials(run_dir, runner=None, nonce=None, san=RELAY_SAN_EXT):
    """A fresh CA and relay certificate for one run, generated here, in a new private `creds`
    directory under `run_dir`, and the run's receipt of them. Returns (receipt, None), or (None, the
    refusal) with everything this call created removed and nothing that existed before touched.

    The receipt (`credentials.json`, created exclusively) binds the run's nonce to the CA's subject
    and the SHA-256 of its DER encoding (`ca_der_sha256`). Every openssl command must print exactly
    its planned output. The nonce in the subject is a label; that the keys are fresh is the fact
    this generation path supplies."""
    runner = runner or default_runner
    run_dir = Path(run_dir)
    creds, receipt_path = run_dir / "creds", run_dir / "credentials.json"
    if receipt_path.exists():
        return None, f"credentials: {receipt_path} already exists; a run's credentials are never regenerated over it"
    try:
        os.mkdir(creds, 0o700)
    except FileExistsError:
        return None, f"credentials: {creds} already exists; fresh credentials are never written over existing files"
    except OSError as e:
        return None, f"credentials: {creds}: {e!r}"
    nonce = nonce or secrets.token_hex(8)
    made = []
    acquired_receipts = set()

    def _publish_receipt(path, text):
        """Creates `path` exclusively and writes `text` to it. The moment the exclusive create succeeds
        the file is this call's, recorded in `acquired_receipts`, so a failed write or close retires it."""
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        acquired_receipts.add(path)
        try:
            data = text.encode()
            while data:
                data = data[os.write(fd, data):]
        finally:
            os.close(fd)
        acquired_receipts.discard(path)
        return path

    def openssl(name, args, stdout=(), stderr=()):
        """The command's stdout, when it ran with rc 0 and printed exactly its planned lines: `stdout`
        and `stderr` are the lines, each a string or a compiled pattern, openssl prints on success."""
        r = runner(name, [str(a) for a in (OPENSSL, *args)], PREFLIGHT_COMMAND_S)
        if r.get("timeout"):
            raise RuntimeError(f"{name}: timed out")
        if r.get("rc") != 0:
            raise RuntimeError(f"{name}: rc {r.get('rc')}: {(r.get('stderr') or '').strip()[-200:]!r}")
        for stream, planned in (("stdout", stdout), ("stderr", stderr)):
            lines = (r.get(stream) or "").splitlines()
            if len(lines) != len(planned) or not all(
                    (w.fullmatch(line) if hasattr(w, "fullmatch") else w == line) for w, line in zip(planned, lines)):
                raise RuntimeError(f"{name}: unplanned output: {stream} {lines!r}")
        return (r.get("stdout") or "").strip()

    try:
        os.chmod(creds, 0o700)
        made += [creds / n for n in ("ca.key", "ca.pem", "relay.key", "relay.csr", "relay.pem", "relay.ext", "ca.srl")]
        (creds / "relay.ext").write_text("basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\n"
                                         f"extendedKeyUsage=serverAuth\nsubjectAltName={san}\n")
        # openssl 3's own progress lines on success (observed): `req` prints "-----"; `x509 -req` names
        # the request's self-signature and the new certificate's subject. Anything else is unplanned.
        openssl("ca", ("req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-days", "1",
                       "-subj", f"/CN={RUN_CA_NAME} {nonce}", "-addext", "basicConstraints=critical,CA:TRUE,pathlen:0",
                       "-addext", "keyUsage=critical,keyCertSign,cRLSign", "-keyout", creds / "ca.key",
                       "-out", creds / "ca.pem"), stderr=("-----",))
        openssl("relay key", ("req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
                              "-subj", f"/CN=moq5 run relay {nonce}", "-keyout", creds / "relay.key",
                              "-out", creds / "relay.csr"), stderr=("-----",))
        openssl("relay cert", ("x509", "-req", "-in", creds / "relay.csr", "-CA", creds / "ca.pem", "-CAkey", creds / "ca.key",
                               "-CAserial", creds / "ca.srl", "-CAcreateserial", "-days", "1", "-extfile",
                               creds / "relay.ext", "-out", creds / "relay.pem"),
                stderr=("Certificate request self-signature ok", f"subject=CN=moq5 run relay {nonce}"))
        for key in ("ca.key", "relay.key"):
            os.chmod(creds / key, 0o600)
        subject = openssl("ca subject", ("x509", "-in", creds / "ca.pem", "-noout", "-subject"),
                          stdout=(f"subject=CN={RUN_CA_NAME} {nonce}",))
        fingerprint = openssl("ca fingerprint", ("x509", "-in", creds / "ca.pem", "-noout", "-fingerprint", "-sha256"),
                              stdout=(_FINGERPRINT,))
        receipt = {"nonce": nonce, "ca_subject": subject, "ca_der_sha256": _fingerprint_digest(fingerprint)}
        _publish_receipt(receipt_path, json.dumps(receipt, indent=1) + "\n")
        return receipt, None
    except Exception as e:
        left = []
        if receipt_path in acquired_receipts:             # this call created it exclusively: it is this call's
            try:
                os.unlink(receipt_path)
            except OSError as gone:
                left.append(f"retained: {receipt_path} ({gone!r})")
        for path in made:
            try:
                path.unlink(missing_ok=True)
            except OSError as gone:
                left.append(f"{path}: {gone!r}")
        try:
            creds.rmdir()
        except OSError as gone:
            left.append(f"{creds}: {gone!r}")
        acquired_receipts.discard(receipt_path)
        return None, f"credentials: {e}" + (f"; cleanup incomplete: {'; '.join(left)}" if left else "")


def capacity_report_error(lines):
    """None when `lines` are a capacity report by CAPACITY_HEAD / CAPACITY_OPTIONAL /
    CAPACITY_CLOSING; otherwise the first departure, named."""
    def at(pos, expected):
        return f"capacity line {pos + 1}: {lines[pos]!r}, expected {expected}" if pos < len(lines) \
            else f"capacity report ended, expected {expected}"
    pos = 0
    for name, pattern in CAPACITY_HEAD:
        if pos >= len(lines) or not pattern.fullmatch(lines[pos]):
            return at(pos, name)
        pos += 1
    closing_name, closing = CAPACITY_CLOSING
    next_optional = 0
    while pos < len(lines) and not closing.fullmatch(lines[pos]):
        k = next((k for k in range(next_optional, len(CAPACITY_OPTIONAL)) if CAPACITY_OPTIONAL[k].fullmatch(lines[pos])), None)
        if k is None:
            return at(pos, f"an optional row in order, or {closing_name}")
        next_optional, pos = k + 1, pos + 1
    if pos >= len(lines):
        return at(pos, closing_name)
    if pos + 1 != len(lines):
        return f"capacity line {pos + 2}: {lines[pos + 1]!r} follows {closing_name}"
    return None


def default_runner(name, argv, timeout, env=None):
    return cmd(argv, timeout=timeout, env=env)


def _sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


# `DYLD_PRINT_LIBRARIES=1` stderr, as the pinned relay's `capacity -c` printed it on this host: image
# lines and deferred-load notes, every one from the one process. Nothing else is planned.
LOADER_IMAGE = re.compile(r"dyld\[([0-9]+)\]: <[0-9A-F]{8}(?:-[0-9A-F]{4}){3}-[0-9A-F]{12}> (/.+)")
LOADER_DELAYED = re.compile(r"dyld\[([0-9]+)\]: move loaded to delayed: [A-Za-z0-9_.+-]+")
# What the preflight's provider check cannot establish: it qualifies the `capacity` process. The owned
# `serve` process's own loaded provider is still owed by the live run.
OPEN_OBLIGATIONS = ("the owned relay serve process's loaded provider (this preflight qualifies the capacity process only)",)


def declared_artifacts_error(run):
    """None when the relay binary and the provider are byte-for-byte the declared artifacts."""
    wrong = [f"{label} {path} sha256 {_sha256(path)}, declared {want}"
             for label, path, want in (("relay binary", run.relay_binary, run.relay_sha256),
                                       ("provider", run.provider, run.provider_sha256)) if _sha256(path) != want]
    return "; ".join(wrong) or None


def loader_judgment(lines, provider, pid=None):
    """(summary, None) when `lines` -- one process's stderr under DYLD_PRINT_LIBRARIES -- show exactly
    one libmsquic image, the declared `provider` file; otherwise (None, the refusal).

    With `pid` None (the capacity process, run through the preflight's runner) every line must be a
    loader line, all from one process. With `pid` given (an owned serve child) every loader line must
    be that pid's; a line that is not the loader's is the relay's own output, returned for the
    diagnostic rule, while any other `dyld` line is an unplanned loader line."""
    pids, images, delayed, own, unplanned = set(), [], 0, [], []
    for line in lines:
        match = LOADER_IMAGE.fullmatch(line) or LOADER_DELAYED.fullmatch(line)
        if match is None:
            (own if pid is not None and not line.startswith("dyld") else unplanned).append(line)
            continue
        pids.add(match.group(1))
        if match.re is LOADER_IMAGE:
            images.append(match.group(2))
        else:
            delayed += 1
    if unplanned:
        return None, (f"unplanned loader line {unplanned[:3]!r}" if pid is not None else f"unplanned stderr {unplanned[:3]!r}")
    if pid is None and len(pids) != 1:
        return None, f"loader lines from {len(pids)} processes, expected the one capacity process"
    if pid is not None and pids != {str(pid)}:
        return None, f"loader lines from processes {sorted(pids)}, expected only the serve process {pid}"
    loaded = [path for path in images if Path(path).name.startswith("libmsquic")]
    if len(loaded) != 1:
        return None, f"{len(loaded)} libmsquic images loaded {loaded!r}, expected exactly the declared one"
    if Path(loaded[0]).resolve() != provider.resolve():
        return None, f"loaded {loaded[0]}, declared {provider}"
    return {"pids": sorted(pids), "images": len(images), "delayed": delayed, "provider_image": loaded[0],
            "own_lines": own}, None


def relay_evidence_judge(provider):
    """The judge of one run's own relay: its settled record's stderr must hold loader lines under its
    own pid naming exactly `provider` (`loader_judgment`). Returns None, or the refusal."""
    def judge(settled):
        _, refused = loader_judgment((settled.get("stderr") or "").splitlines(), provider, pid=settled.get("pid"))
        return refused
    return judge


def qualify_serve(run, ready_s=None, settle_s=None, clock=None, spawn=Child):
    """One host-only qualification of the relay's `serve` process, as a receipt whose `refusal` is
    None only when it qualified.

    The declared relay and provider bytes are checked first; a departure refuses before anything is
    spawned. Then `serve` (the run's own relay argv, in its run directory, with the serve environment
    plus DYLD_PRINT_LIBRARIES=1) is one directly owned child, and its declared raw listener's
    RELAY_READY_V1 record must be stamped by its stdout reader within `ready_s`. Whatever happened, the
    child is then terminated (SIGTERM, bounded escalation) and reaped with both readers settled. Only a
    process that was ready and ended as the relay's SIGTERM exit declares is judged: its loader lines
    by `loader_judgment` against its own pid, and every line it wrote by the no-warning rule. A settle
    that fails names the child in `retained`; nothing is judged from an unsettled child.

    This qualifies the artifacts and environment through this one process. It sends nothing to the
    listener, and a process launched later owes its own loader evidence."""
    d = Deadlines()
    ready_s = d.relay_ready if ready_s is None else ready_s
    settle_s = d.settle if settle_s is None else settle_s
    clock = clock or MonotonicClock()
    identity = relay_identity(run)
    rec = {"argv": relay_argv(run), "env": dict(serve_env(), **RELAY_LOADER_ENV),
           "identity": identity.fields(), "pid": None, "ready": None, "ready_record": None, "settled": None,
           "loader": None, "failures": [], "retained": [], "refusal": None}
    failures = rec["failures"]

    def done():
        rec["refusal"] = "; ".join(failures) or None
        return rec
    wrong = declared_artifacts_error(run)
    if wrong:
        failures.append(wrong)
        return done()
    ready = StampedEvent(clock, "relay_ready")

    def on_line(line):
        if not ready.is_set():
            match = relay_ready_record(line, identity)
            if match is not None:
                rec["ready_record"] = match
                ready.set()
    try:
        child = spawn("relay", rec["argv"], rec["env"], str(run.run_dir), on_line=on_line)
    except Exception as e:
        failures.append(f"relay serve: spawn failed: {e!r}")
        return done()
    rec["pid"] = child.proc.pid
    deadline = clock.now() + ready_s
    try:
        outcome = await_stamped(ready, deadline, clock, alive=child.alive)
        rec["ready"] = {"outcome": outcome, "stamp": ready.t, "deadline": deadline}
        if outcome != "on-time":
            failures.append(f"relay serve readiness ({identity.describe()}): {outcome} within {ready_s}s")
    finally:
        try:
            settled = child.settle(settle_s, signal_first=signal.SIGTERM)
        except Exception as e:
            failures.append(f"relay serve: settle raised {e!r}")
            rec["retained"].append(f"relay serve pid {rec['pid']}: not proven reaped")
            return done()
    rec["settled"] = settled
    if not settled["readers_settled"] or settled["reader_errors"]:
        failures.append(f"relay serve: readers settled {settled['readers_settled']}, errors {settled['reader_errors']}")
    failures.extend(f"unplanned diagnostic: {line}" for line in collect_diagnostics(settled))
    if rec["ready"]["outcome"] != "on-time":
        return done()
    if (settled["how"], settled["exit"]) != LIVE_EXITS["relay"]:
        failures.append(f"relay serve: ended {settled['how']} rc {settled['exit']}, "
                        f"expected {LIVE_EXITS['relay'][0]} rc {LIVE_EXITS['relay'][1]}")
    loader, refused = loader_judgment(settled["stderr"].splitlines(), run.provider, pid=settled["pid"])
    if refused:
        failures.append(f"relay serve: {refused}")
    rec["loader"] = loader
    return done()


def _executable(path):
    if not path.is_file():
        return f"{path} is not a regular file"
    if not os.access(path, os.X_OK):
        return f"{path} is not executable"
    return None


def _mode(path):
    return stat.S_IMODE(path.stat().st_mode)


def preflight(run, runner=default_runner):
    """Every check in PREFLIGHT_CHECKS order, each {name, ok, reason}. Writes the relay
    configuration to its declared path so that `capacity -c` judges the exact file `serve`
    will read. Runs no command other than openssl, the relay's `capacity` (twice: once as the
    configuration's judge, once under the serve environment with the loader's image list) and the
    publisher's `--track-config-selftest`."""
    checks = []

    def check(name, fn):
        try:
            reason = fn()
        except Exception as e:                      # a check that cannot run is a named failure
            reason = f"{type(e).__name__}: {e}"
        checks.append({"name": name, "ok": reason is None, "reason": reason})

    def run_cmd(name, argv, env=None):
        r = runner(name, [str(a) for a in argv], PREFLIGHT_COMMAND_S, env=env)
        if r.get("timeout"):
            raise RuntimeError(f"{name} timed out after {PREFLIGHT_COMMAND_S}s")
        return r

    def planned(r, stdout=None, stderr=()):
        """None when `r`'s output is exactly what its check plans: stderr lines exactly `stderr`, and
        stdout lines exactly `stdout` or, when `stdout` is None, free of warning lines. Otherwise
        the unplanned output, named."""
        out, err = r["stdout"].splitlines(), r["stderr"].splitlines()
        wrong = [] if err == list(stderr) else [f"stderr {err!r}, expected {list(stderr)!r}"]
        if stdout is None:
            wrong += [f"stdout warning {line!r}" for line in out if WARN.search(line)]
        elif out != list(stdout):
            wrong.append(f"stdout {out!r}, expected {list(stdout)!r}")
        return "unplanned output: " + "; ".join(wrong) if wrong else None

    def objects():
        declared, loaded = host_receiver.declared_objects(), media_oracle.load_objects(run.objects)
        return None if len(loaded) == declared else f"{run.objects} has {len(loaded)} records; the run declares {declared}"

    def credentials():
        """The CA is the one this run generated: its subject and fingerprint are the receipt's."""
        receipt = json.loads(run.credentials_receipt.read_text())
        subject = run_cmd("ca subject", [OPENSSL, "x509", "-in", run.ca, "-noout", "-subject"])
        fingerprint = run_cmd("ca fingerprint", [OPENSSL, "x509", "-in", run.ca, "-noout", "-fingerprint", "-sha256"])
        if subject["rc"] != 0 or fingerprint["rc"] != 0:
            return f"ca subject/fingerprint rc {subject['rc']}/{fingerprint['rc']}"
        lines = subject["stdout"].splitlines()
        unplanned = planned(subject, stdout=lines[:1]) or planned(fingerprint, stdout=fingerprint["stdout"].splitlines()[:1])
        if unplanned or not _FINGERPRINT.fullmatch(fingerprint["stdout"].strip()):
            return unplanned or f"ca fingerprint: {fingerprint['stdout'].strip()!r}"
        got_subject = subject["stdout"].strip()
        got_digest = _fingerprint_digest(fingerprint["stdout"].strip())
        wrong = []
        if not got_subject.endswith(f"{RUN_CA_NAME} {receipt.get('nonce')}") or got_subject != receipt.get("ca_subject"):
            wrong.append(f"CA subject {got_subject!r}, receipt {receipt.get('ca_subject')!r} (nonce {receipt.get('nonce')!r})")
        if got_digest != receipt.get("ca_der_sha256"):
            wrong.append(f"CA DER sha256 {got_digest}, receipt {receipt.get('ca_der_sha256')}")
        return "; ".join(wrong) or None

    def modes():
        wrong = [f"{p.name} {oct(_mode(p))}, expected {oct(m)}"
                 for p, m in ((run.creds, 0o700), (run.creds / "ca.key", 0o600), (run.key, 0o600)) if _mode(p) != m]
        for p in (run.ca, run.cert):
            if not p.is_file():
                wrong.append(f"{p.name} is missing")
        return "; ".join(wrong) or None

    def chain():
        for ip in (EMULATOR_HOST, LISTENER_HOST):
            r = run_cmd("verify " + ip, [OPENSSL, "verify", "-CAfile", run.ca, "-purpose", "sslserver", "-verify_ip", ip, run.cert])
            if r["rc"] != 0:
                return f"{ip}: rc {r['rc']}: {(r['stdout'] + r['stderr']).strip()}"
            unplanned = planned(r, stdout=[f"{run.cert}: OK"])
            if unplanned:
                return f"{ip}: {unplanned}"
        return None

    def wrong_ip():
        s = run_cmd("subject", [OPENSSL, "x509", "-in", run.cert, "-noout", "-subject"])
        subject = s["stdout"].strip()
        if s["rc"] != 0 or not subject.startswith("subject="):
            return f"subject: rc {s['rc']}: {(s['stdout'] + s['stderr']).strip()}"
        unplanned = planned(s, stdout=[subject])
        if unplanned:
            return f"subject: {unplanned}"
        r = run_cmd("verify 10.0.2.3", [OPENSSL, "verify", "-CAfile", run.ca, "-purpose", "sslserver", "-verify_ip", "10.0.2.3", run.cert])
        if r["rc"] != 2:
            return f"10.0.2.3 was not refused: rc {r['rc']}: {(r['stdout'] + r['stderr']).strip()}"
        return planned(r, stdout=[], stderr=[subject[len("subject="):], "error 64 at 0 depth lookup: IP address mismatch",
                                              f"error {run.cert}: verification failed"])

    def san():
        r = run_cmd("san", [OPENSSL, "x509", "-in", run.cert, "-noout", "-ext", "subjectAltName"])
        if r["rc"] != 0:
            return f"subjectAltName rc {r['rc']}: {r['stderr'].strip()}"
        return planned(r, stdout=["X509v3 Subject Alternative Name: ", "    " + RELAY_SAN])

    def config():
        run.relay_config_path.write_text(json.dumps(relay_config(run), indent=1) + "\n")
        r = run_cmd("relay capacity", [run.relay_binary, "capacity", "-c", run.relay_config_path])
        if r["rc"] != 0:
            return f"relay capacity rc {r['rc']}: {r['stderr'].strip()}"
        if r["stderr"]:
            return f"unplanned output: stderr {r['stderr'].splitlines()!r}"
        return capacity_report_error(r["stdout"].splitlines())

    def provider():
        """The relay and its provider are the declared artifacts, and the relay, launched with the
        serve command's own environment, loads exactly the declared provider."""
        wrong = declared_artifacts_error(run)
        if wrong:
            return wrong
        env = dict(serve_env(), DYLD_PRINT_LIBRARIES="1")
        r = run_cmd("relay provider", [run.relay_binary, "capacity", "-c", run.relay_config_path], env=env)
        if r["rc"] != 0:
            return f"relay provider: capacity rc {r['rc']}: {r['stderr'].strip()[-200:]!r}"
        report = capacity_report_error(r["stdout"].splitlines())
        if report:
            return f"relay provider: {report}"
        _, refused = loader_judgment(r["stderr"].splitlines(), run.provider)
        return f"relay provider: {refused}" if refused else None

    def track():
        r = run_cmd("publisher track", [run.publisher_binary, "--track-config-selftest", "--track", *TRACK_DECL,
                                        AVC_CONFIG_FLAG, run.objects])
        if r["rc"] != 0:
            return f"publisher track rc {r['rc']}: {r['stdout'].strip()!r} {r['stderr'].strip()!r}"
        unplanned = planned(r, stdout=r["stdout"].splitlines()[:1])           # one line, empty stderr
        if unplanned:
            return unplanned
        try:
            got = json.loads(r["stdout"])
        except ValueError as e:
            return f"publisher track line is not JSON: {e}"
        width, height = (int(v) for v in TRACK_DECL[1].split("x"))
        want = {"event": "track_config", "codec": TRACK_DECL[0], "width": width, "height": height,
                "framerate_millis": int(TRACK_DECL[2]), "timescale": TRACK_TIMESCALE, "bitrate": TRACK_BITRATE,
                "declared": True, "init_data": str}
        if not isinstance(got, dict):
            return f"publisher track line is {got!r}, not an object"
        wrong = [f"missing {k}" for k in want if k not in got]
        wrong += [f"unplanned field {k} {got[k]!r}" for k in got if k not in want]
        wrong += [f"{k} {got[k]!r}, expected {want[k]!r}" for k in want
                  if k in got and want[k] is not str and (type(got[k]) is not type(want[k]) or got[k] != want[k])]
        if "init_data" in got:
            config = avc_config_error(got["init_data"], run.objects)
            if config:
                wrong.append(config)
        return "publisher track_config: " + "; ".join(wrong) if wrong else None

    def host():
        host_argv(host_identity(run), run.python)          # raises ValueError naming the first bad input

    check("relay_binary", lambda: _executable(run.relay_binary))
    check("publisher_binary", lambda: _executable(run.publisher_binary))
    check("host_python", lambda: _executable(Path(run.python)))
    check("adb_binary", lambda: _executable(run.adb))                 # judged as a file; never run here
    check("package", lambda: None if (run.package / "moq5" / "__init__.py").is_file()
          else f"{run.package / 'moq5' / '__init__.py'} is missing")
    check("objects", objects)
    check("credential_modes", modes)
    check("run_credentials", credentials)
    check("certificate_chain", chain)
    check("certificate_wrong_ip_refused", wrong_ip)
    check("certificate_san", san)
    check("relay_config", config)
    check("relay_provider", provider)
    check("publisher_track", track)
    check("host_argv", host)
    return {"checks": checks, "ok": all(c["ok"] for c in checks), "open_obligations": OPEN_OBLIGATIONS}
