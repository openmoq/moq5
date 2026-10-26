"""The finite live entry: one owned device/media run from explicit, declared inputs.

    python -I run_android_media.py --live --authorized --out <new dir> \\
        --relay <moq5-relay> --relay-sha256 <hex> --provider <libmsquic> --provider-sha256 <hex> \\
        --publisher <moq5_runtime_publisher> --package <moq5 package root> --python <python3> \\
        --adb <adb> --emulator <emulator> --apk <player apk> --apk-sha256 <hex> \\
        --ffmpeg <ffmpeg> --ffprobe <ffprobe> --port <udp> --admin-port <tcp>

Every input is named on the command line: nothing is taken from PATH, a default or an earlier run.
`--authorized` is the operator's opt-in and nothing else; a missing, invalid or unknown input refuses
before any effect. The run then, in order and each only after the one before succeeded:

    creates --out, exclusively (an existing directory is refused untouched)
    generates the run's credentials and media (live_actors.fresh_credentials, make_input.generate)
    runs the real-input preflight (live_actors.preflight), before any emulator or relay exists
    owns the dedicated lane (device_lane.EmulatorLane): start, then place this run's CA
    composes the actors (live_actors.compose: the trust gate and the relay's own loader evidence)
    supervises them (run_android_media.supervise), whose final verdict is the media verdict

and, whatever happened from the lane's creation on -- a refusal, an exception, an interruption --
the owned cleanup: remove the CA, restore the packages (once a baseline exists), stop and reap the
emulator. A failing cleanup step never skips a later one. The complete receipt is written exclusively
into --out and made read-only; evidence is kept, never cleaned up. The exit status is nonzero unless
the setup, the media verdict, every cleanup step and the receipt all succeeded.
"""
import argparse
import contextlib
import hashlib
import json
import os
import re
import signal
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))

EXIT_OK = 0
EXIT_MEDIA = 1           # the run was supervised and its media verdict failed
EXIT_REFUSED = 2         # refused before any effect
EXIT_SETUP = 3           # the run failed before or outside a media verdict
EXIT_CLEANUP = 4         # an owned cleanup step failed (whatever the media verdict)
EXIT_RECEIPT = 5         # the final receipt could not be written
EXIT_INTERRUPTED = 6     # the operator interrupted the run
RECEIPT = "live-receipt.json"
STACK_S = 30.0           # one debuggerd snapshot, bounded by the host, outside the player's own looper


@dataclass(frozen=True)
class Deps:
    """The entry's effects, each one callable: the offline tests replace them all."""
    credentials: object      # (run_dir) -> (receipt, None) | (None, refusal)
    media: object            # (run_dir, ffmpeg, ffprobe) -> make_input receipt ({"ok": ...})
    preflight: object        # (LiveRun) -> {"ok", "checks", "open_obligations"}
    lane: object             # (emulator, adb, log_path) -> an EmulatorLane
    supervise: object        # (run_dir, actors, deadlines, n_objects) -> the supervisor's record
    open_receipt: object     # (path) -> a writable text file created exclusively at path
    final_ui: object         # (actors, out) -> record of one read-only UI dump kept after a failed verdict
    stacks: object           # (actors, out) -> record of the player's thread stacks kept after a failed verdict
    audio_media: object = None   # av-diagnostic: (run_dir, ffmpeg, ffprobe) -> make_audio_input receipt ({"ok": ...})
    audio_check: object = None   # av-diagnostic: (LiveRun) -> (record, None) | (record, refusal), the publisher's own validation


def open_receipt(path):
    return open(path, "x")


def capture_final_ui(actors, out):
    """One read-only UI dump of the player as it was left, taken after supervision and before any
    cleanup: the raw output, exactly as `uiautomator` returned it, kept in <out>/final-ui.xml."""
    import device_player
    player = actors.player_start.player
    r = player.device.run("final_ui_dump", "exec-out", "uiautomator", "dump", "/dev/tty", timeout=device_player.COMMAND_S)
    path = out / "final-ui.xml"
    with open(path, "x") as f:
        f.write(r.get("stdout") or "")
    return {"path": str(path), "rc": r.get("rc"), "timeout": r.get("timeout"), "stderr": r.get("stderr")}


def capture_stacks(actors, out):
    """The prepared player's thread stacks as it was left: Java (`debuggerd -j`) then native, every
    thread (`debuggerd`), each through a per-command `su 0`, bounded by STACK_S on the host. Immediately
    before each, the target is revalidated: the package's only pid is the prepared one and its cmdline
    names the package; otherwise that snapshot is refused and nothing is sent to any process. Each
    output is kept exclusively in <out>/stacks-<kind>.txt."""
    import device_player
    player = actors.player_start.player
    device, pid = player.device, player.pid
    rec = {"pid": pid}

    def owned():
        if pid is None:
            return "no prepared player pid"
        r = device.run("stack_pidof", "shell", "pidof", device_player.PACKAGE, timeout=device_player.COMMAND_S)
        got = (r.get("stdout") or "").split()
        if r.get("rc") != 0 or got != [str(pid)]:
            return f"pidof {device_player.PACKAGE}: rc {r.get('rc')} {got!r}, prepared {pid}"
        c = device.run("stack_cmdline", "shell", "cat", f"/proc/{pid}/cmdline", timeout=device_player.COMMAND_S)
        name = (c.get("stdout") or "").split("\x00")[0]
        if c.get("rc") != 0 or name != device_player.PACKAGE:
            return f"/proc/{pid}/cmdline: rc {c.get('rc')} {name!r}, not {device_player.PACKAGE}"
        return None
    for kind, args in (("java", ("-j", str(pid))), ("native", (str(pid),))):
        refused = owned()
        if refused:
            rec[kind] = {"refused": refused}
            continue
        r = device.run(f"stack_{kind}", "shell", "su", "0", "debuggerd", *args, timeout=STACK_S)
        path = out / f"stacks-{kind}.txt"
        with open(path, "x") as f:
            f.write(r.get("stdout") or "")
        rec[kind] = {"path": str(path), "rc": r.get("rc"), "timeout": r.get("timeout"), "stderr": (r.get("stderr") or "")[:4000]}
    return rec


def _default_deps():
    import device_lane
    import live_actors
    import make_input
    import run_android_media
    import make_audio_input
    source = make_input.RECIPE
    return Deps(credentials=live_actors.fresh_credentials,
                media=lambda run_dir, ffmpeg, ffprobe: make_input.generate(run_dir, ffmpeg=ffmpeg, ffprobe=ffprobe),
                audio_media=lambda run_dir, ffmpeg, ffprobe: make_audio_input.generate(
                    run_dir, source["source"]["frames"], source["pts_us_step"], ffmpeg=ffmpeg, ffprobe=ffprobe),
                audio_check=live_actors.audio_publisher_check,
                preflight=live_actors.preflight,
                lane=lambda emulator, adb, log_path: device_lane.EmulatorLane(emulator, adb, log_path),
                supervise=run_android_media.supervise, open_receipt=open_receipt, final_ui=capture_final_ui,
                stacks=capture_stacks)


_injected = []


@contextlib.contextmanager
def injected(deps):
    """Runs the entry with `deps` in place of the real effects (offline tests only)."""
    _injected.append(deps)
    try:
        yield
    finally:
        _injected.pop()


class Refused(Exception):
    pass


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise Refused(message)


_HEX64 = re.compile(r"[0-9a-f]{64}")
_FILES = ("relay", "provider", "publisher", "python", "adb", "emulator", "apk", "ffmpeg", "ffprobe")
_DIGESTS = (("relay_sha256", "relay"), ("provider_sha256", "provider"), ("apk_sha256", "apk"))


def _sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def parse(argv):
    """The declared inputs, every one checked; raises Refused naming the first bad one. Runs nothing
    and creates nothing."""
    p = _Parser(prog="run_android_media.py --live", add_help=False)
    p.add_argument("--authorized", action="store_true")
    p.add_argument("--player-mode", default="acceptance")
    for name in ("out", *_FILES, "package", "relay-sha256", "provider-sha256", "apk-sha256", "port", "admin-port"):
        p.add_argument(f"--{name}", required=True)
    a = p.parse_args(argv)
    if not a.authorized:
        raise Refused("the live device/relay run is not released without --authorized")
    for name in ("out", *_FILES, "package"):
        if not Path(getattr(a, name)).is_absolute():
            raise Refused(f"--{name}: must be an absolute path, got {getattr(a, name)!r}")
    for name in _FILES:
        if not Path(getattr(a, name)).is_file():
            raise Refused(f"--{name}: not a file: {getattr(a, name)}")
    if not (Path(a.package) / "moq5" / "__init__.py").is_file():
        raise Refused(f"--package: no moq5 package under {a.package}")
    for digest, of in _DIGESTS:
        value = getattr(a, digest)
        if not _HEX64.fullmatch(value):
            raise Refused(f"--{digest.replace('_', '-')}: must be a lowercase hex sha256, got {value!r}")
        got = _sha256(getattr(a, of))
        if got != value:
            raise Refused(f"--{digest.replace('_', '-')}: {getattr(a, of)} sha256 {got}, declared {value}")
    ports = {}
    for name in ("port", "admin_port"):
        flag, value = f"--{name.replace('_', '-')}", getattr(a, name)
        if not re.fullmatch(r"[0-9]{1,5}", value) or not 1024 <= int(value) <= 65535 or int(value) == 4443:
            raise Refused(f"{flag}: must be a port from 1024 through 65535 other than 4443, got {value!r}")
        ports[name] = int(value)
    if ports["admin_port"] == ports["port"]:
        raise Refused("--admin-port: must differ from --port")
    import live_actors
    if a.player_mode not in live_actors.PLAYER_MODES:
        raise Refused(f"--player-mode: not one of {sorted(live_actors.PLAYER_MODES)}, got {a.player_mode!r}")
    out = Path(a.out)
    if not out.parent.is_dir():
        raise Refused(f"--out: parent directory {out.parent} does not exist")
    if os.path.lexists(out):
        raise Refused(f"--out: already exists: {out}; a run never adopts an earlier output")
    return {"out": out, **{n: Path(getattr(a, n)) for n in (*_FILES, "package")},
            **{d: getattr(a, d) for d, _ in _DIGESTS}, **ports, "player_mode": a.player_mode}


def _identities(inputs):
    ids = {name: {"path": str(inputs[name]), "sha256": _sha256(inputs[name])} for name in _FILES}
    pkg = inputs["package"] / "moq5"
    ids["package"] = {"path": str(inputs["package"]),
                      "files": {p.name: _sha256(p) for p in sorted(pkg.iterdir()) if p.is_file()}}
    return ids


def run(inputs, deps):
    """One owned run from checked `inputs`. Returns the receipt (not yet written)."""
    import live_actors
    import run_android_media
    out = inputs["out"]
    rec = {"inputs": {k: str(v) for k, v in inputs.items()}, "identities": None, "player": None, "credentials": None,
           "media_input": None, "preflight": None, "lane": [], "media": "not started", "media_verdict": None,
           "supervision": None, "final_ui": None, "stacks": None, "cleanup": [],
           "outcome": {"exit": None, "first_failure": None, "cleanup_failures": [], "retained": []}}
    outcome = rec["outcome"]
    lane = None

    def fail(step, reason, code):
        if outcome["first_failure"] is None:
            outcome["first_failure"] = {"step": step, "reason": reason}
            outcome["exit"] = code
    try:
        os.mkdir(out, 0o700)
    except OSError as e:
        fail("out", f"cannot create {out}: {e!r}", EXIT_REFUSED)
        return rec
    try:
        rec["identities"] = _identities(inputs)
        receipt, refused = deps.credentials(out)
        rec["credentials"] = receipt
        if refused:
            fail("credentials", refused, EXIT_SETUP)
            return rec
        media = deps.media(out, inputs["ffmpeg"], inputs["ffprobe"])
        rec["media_input"] = media
        if not media.get("ok"):
            fail("media", f"declared media not generated: {media.get('error') or media.get('checks')}", EXIT_SETUP)
            return rec
        av = inputs["player_mode"] == live_actors.AV_MODE
        if av:
            # The av-diagnostic's second track: generated and judged offline, then validated by the
            # publisher's own declaration code, all before the lane holds anything.
            audio = deps.audio_media(out, inputs["ffmpeg"], inputs["ffprobe"])
            rec["audio_input"] = audio
            if not audio.get("ok"):
                fail("audio_media", f"declared audio not generated: {audio.get('error') or audio.get('checks')}", EXIT_SETUP)
                return rec
        live = live_actors.LiveRun(run_dir=out, relay_binary=inputs["relay"], publisher_binary=inputs["publisher"],
                                   package=inputs["package"], admin_port=inputs["admin_port"], port=inputs["port"],
                                   adb=inputs["adb"], relay_sha256=inputs["relay_sha256"], provider=inputs["provider"],
                                   provider_sha256=inputs["provider_sha256"], python=str(inputs["python"]))
        if av:
            checked, refused = deps.audio_check(live)
            rec["audio_publisher"] = checked
            if refused:
                fail("audio_publisher", refused, EXIT_SETUP)
                return rec
        pre = deps.preflight(live)
        rec["preflight"] = pre
        if pre.get("ok") is not True:
            failed = [f"{c['name']}: {c['reason']}" for c in pre.get("checks", []) if not c.get("ok")]
            fail("preflight", "; ".join(failed) or "preflight did not pass", EXIT_SETUP)
            return rec
        # From here on the lane may hold resources: the cleanup below is owed on every path.
        lane = deps.lane(inputs["emulator"], inputs["adb"], out / "emulator.log")
        for step, call in (("lane.start", lane.start), ("lane.place_ca", lambda: lane.place_ca(live.ca))):
            refused = call()
            rec["lane"].append({"step": step, "result": refused})
            if refused is not None:
                fail(step, refused, EXIT_SETUP)
                return rec
        try:
            actors, n_objects = live_actors.compose(live, lane, str(inputs["apk"]), player_mode=inputs["player_mode"])
            rec["player"] = {"mode": inputs["player_mode"],
                             "launch_args": list(actors.player_start.player.launch_args())}
        except Exception as e:
            fail("compose", repr(e), EXIT_SETUP)
            return rec
        rec["media"] = "started"
        sup = deps.supervise(out, actors, run_android_media.Deadlines(), n_objects)
        rec["supervision"], rec["media_verdict"] = sup, sup.get("verdict")
        rec["media"] = "verdict"
        outcome["retained"] = list(sup.get("retained") or [])       # supervisor resources not proven reaped
        passed = (sup.get("verdict") or {}).get("ok") is True
        if not passed:
            for key, capture in (("final_ui", deps.final_ui), ("stacks", deps.stacks)):
                try:                                               # evidence only; the lane cleanup follows regardless
                    rec[key] = capture(actors, out)
                except Exception as e:
                    rec[key] = {"error": repr(e)}
        if sup.get("interruptions"):
            # The supervisor retired what it owned and returned its record: the run was interrupted,
            # whatever its verdict says.
            first = sup["interruptions"][0]
            reason = sup.get("primary_error") or f"interrupted during {first['where']}: {first['what']}"
            if not reason.startswith("interrupted"):
                reason += f"; then interrupted during {first['where']}: {first['what']}"
            fail("supervise", reason, EXIT_INTERRUPTED)
        elif passed and inputs["player_mode"] != "acceptance":
            fail("player_mode", f"OBSERVER-OFF DIAGNOSTIC ({inputs['player_mode']}): a diagnostic run never passes "
                 "acceptance, whatever its verdict", EXIT_MEDIA)
        elif not passed:
            fail("media_verdict", "; ".join((sup.get("verdict") or {}).get("failures") or ["no verdict"]), EXIT_MEDIA)
        return rec
    except KeyboardInterrupt as e:
        fail(_current_step(rec, lane), f"interrupted: {e!r}", EXIT_INTERRUPTED)
        return rec
    except Exception as e:
        fail(_current_step(rec, lane), f"{e!r}\n{traceback.format_exc(limit=8)}", EXIT_SETUP)
        return rec
    finally:
        if lane is not None:
            _cleanup(lane, rec)


def _current_step(rec, lane):
    if rec["media"] == "started":
        return "supervise"
    if lane is not None:
        done = [s["step"] for s in rec["lane"]]
        return "lane.place_ca" if "lane.start" in done else "lane.start"
    if rec["preflight"] is not None:
        return "lane.create"
    if rec["media_input"] is not None:
        return "preflight"
    return "media" if rec["credentials"] is not None else "credentials"


def _cleanup(lane, rec):
    """Every owned cleanup step, each attempted whatever the one before did."""
    def step(name, call, failed):
        try:
            result = call()
        except BaseException as e:                    # an interruption here must not skip the later steps
            result, reason = f"raised {e!r}", f"raised {e!r}"
        else:
            reason = failed(result)
        rec["cleanup"].append({"step": name, "result": result})
        if reason is not None:
            rec["outcome"]["cleanup_failures"].append({"step": name, "reason": reason})
    step("lane.remove_ca", lane.remove_ca, lambda r: r)
    if getattr(lane, "baseline", None) is not None:
        step("lane.restore", lane.restore, lambda r: r)
    step("lane.stop", lane.stop, lambda r: "; ".join(r.get("failures") or []) or None)


def _exit_code(rec):
    outcome = rec["outcome"]
    if outcome["first_failure"] is not None:
        return outcome["exit"]
    return EXIT_CLEANUP if outcome["cleanup_failures"] or outcome["retained"] else EXIT_OK


def write_receipt(out, rec, deps):
    """Writes `rec` exclusively as <out>/live-receipt.json, read-only. The bytes go to a `.incomplete`
    file first, renamed only once complete, so a failed write never leaves something that reads as a
    receipt; the incomplete file is kept for attribution. Returns None or the failure."""
    part = out / (RECEIPT + ".incomplete")
    try:
        text = json.dumps(rec, indent=1, default=str) + "\n"
        with deps.open_receipt(part) as f:
            f.write(text)
        os.chmod(part, 0o444)
        if os.path.lexists(out / RECEIPT):
            return f"receipt: {out / RECEIPT} already exists"
        os.rename(part, out / RECEIPT)
    except Exception as e:
        return f"receipt: {e!r}"
    return None


def main(argv):
    """`argv` after `--live`. The exit status of the run (see EXIT_*)."""
    deps = _injected[-1] if _injected else _default_deps()
    try:
        inputs = parse(argv)
    except Refused as e:
        print(f"run_android_media --live: refused: {e}", file=sys.stderr)
        return EXIT_REFUSED

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(signal.Signals(signum).name)
    previous = signal.signal(signal.SIGTERM, interrupted)
    try:
        rec = run(inputs, deps)
    finally:
        signal.signal(signal.SIGTERM, previous)
    code = _exit_code(rec)
    rec["outcome"]["exit"] = code
    if rec["outcome"]["first_failure"] is not None and rec["outcome"]["first_failure"]["step"] == "out":
        print(f"run_android_media --live: refused: {rec['outcome']['first_failure']['reason']}", file=sys.stderr)
        return EXIT_REFUSED
    wrote = write_receipt(inputs["out"], rec, deps)
    if wrote:
        print(f"run_android_media --live: {wrote}", file=sys.stderr)
        return EXIT_RECEIPT
    first = rec["outcome"]["first_failure"]
    summary = "passed" if code == EXIT_OK else (f"failed at {first['step']}: {first['reason'].splitlines()[0]}" if first
                                                 else f"cleanup failed: {rec['outcome']['cleanup_failures']}")
    print(f"run_android_media --live: {summary}; receipt {inputs['out'] / RECEIPT}", file=sys.stderr)
    return code
