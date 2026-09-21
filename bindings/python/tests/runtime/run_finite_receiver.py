"""Finite RAW/LOC receiver runtime: independent C publisher -> OUR moq5-relay
(raw loopback) -> the PRODUCTION moq5 package driving the SHIPPED
examples/receive_loop.py. Synthetic carrier acceptance; no decoder.

    run_finite_receiver.py --package <installed moq5 dir> --relay <moq5-relay> \\
        --publisher <moq5_runtime_publisher> [--negative] [--push]
    run_finite_receiver.py --oracle-red                 comparator RED checks (no processes)
    run_finite_receiver.py --verdict-red RESULTS.json   verdict RED mutations of a receipt (offline)
    run_finite_receiver.py --child-controls             bounded offline ownership controls (stub children)
    run_finite_receiver.py --control-selftests --publisher <bin>   the C control reader's classification

Ownership. The supervisor is the direct Popen owner of the relay, the
publisher and the receiver client, and retains every handle through
settlement; nothing is ever signalled by a PID read from a file. The client
child emits its milestones as `CTRL <line>` on its stdout; the supervisor
forwards them to the publisher's stdin over the pipe it owns. The client
runs the shipped receive loop through a thin OBSERVER that only delegates
poll_track/poll_object/wait/subscribe/track_state/stats to the real Receiver,
records the returned owned values and milestones, and returns them
unchanged; controls are emitted inside wait(), i.e. only when the loop found
nothing queued. Every item still goes through the example's own handlers.
Expected data comes from fixture.py, never from what the publisher reports.
"""
import datetime, importlib.util, json, os, subprocess, sys, threading, time, traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import fixture                                              # noqa: E402
from runtime_support import (Harness, MSQUIC_DIR, ControlProtocol, cmd, collect_diagnostics, free_udp_port,   # noqa: E402
                             relay_checks, vmmap_inventory)
import hashlib   # noqa: E402

EXAMPLE = HERE.parent.parent / "examples" / "receive_loop.py"
CLIENT_S, PUBLISHER_EXIT_S, READY_S, CONNECT_S, LOOP_S = 180.0, 30.0, 30.0, 10.0, 90.0
NS_TEXT = "/".join(p.decode() for p in fixture.NAMESPACE)
FIELDS = ("config_generation", "packaging", "status", "end_of_group", "datagram", "keyframe", "capture_time_us",
          "decode_time_us", "composition_offset_us", "presentation_time_us", "payload", "fragment", "mdat_offset", "mdat_len", "samples")
EXPECTED_EVENTS = ["ADDED", "CATALOG_READY", "ENDED", "REMOVED"]      # the receiver emits CATALOG_READY once (first effective catalog)
MSQUIC_IMAGE = MSQUIC_DIR + "/libmsquic.2.6.0.dylib"
OPENSSL3_IMAGES = ("/opt/homebrew/Cellar/openssl@3/3.6.3/lib/libssl.3.dylib", "/opt/homebrew/Cellar/openssl@3/3.6.3/lib/libcrypto.3.dylib")
# Observed non-project images, explicitly classified: the libSystem-side SSL images present in every process, and the
# interpreter's own OpenSSL (the foundation venv is miniconda-based; hashlib loads it in the client). Nothing else is admitted.
SYSTEM_SSL_IMAGES = ("/usr/lib/libssl.48.dylib", "/usr/lib/libcrypto.46.dylib",
                     "/opt/homebrew/Caskroom/miniconda/base/lib/libcrypto.3.dylib", "/opt/homebrew/Caskroom/miniconda/base/lib/libssl.3.dylib")
CONTROLS_POSITIVE = ["go", "ack-all", "ended-observed", "done"]
REQUIRED_IMAGES = {"module", "publisher", "msquic", "libssl", "libcrypto"}
import re   # noqa: E402
SHA256_RE = re.compile(r"[0-9a-f]{64}")


def is_sha256(v):
    """An actual str that is exactly 64 lowercase hex digits: fullmatch on
    the original value, no coercion, no stripping."""
    return isinstance(v, str) and SHA256_RE.fullmatch(v) is not None
CONTROLS_NEGATIVE = ["go", "ended-observed", "done"]


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def declared_images():
    """Expected object images computed from the fixture alone (N fixed)."""
    return [{"track": "T0", "config_generation": 0, "packaging": 1, "status": 0, "end_of_group": True, "datagram": False,
             "keyframe": is_sync, "capture_time_us": fixture.PTS_US[i], "decode_time_us": fixture.PTS_US[i], "composition_offset_us": 0,
             "presentation_time_us": fixture.PTS_US[i], "payload": {"hex": p.hex(), "len": len(p)}, "fragment": {"hex": "", "len": 0},
             "mdat_offset": 0, "mdat_len": 0, "samples": []} for i, is_sync, p in fixture.OBJECTS]


DECLARED_DESCRIPTION = {"name": {"hex": fixture.TRACK.hex(), "len": len(fixture.TRACK)}, "codec": {"hex": fixture.CODEC.hex(), "len": len(fixture.CODEC)},
                        "packaging_text": {"hex": b"loc".hex(), "len": 3}, "init_data": {"hex": "", "len": 0}, "media_type": 1, "packaging": 1,
                        "timescale": fixture.TIMESCALE, "transport_version": 18, "width": fixture.WIDTH, "height": fixture.HEIGHT,
                        "framerate_millis": fixture.FRAMERATE_MILLIS, "bitrate": fixture.BITRATE}


def events_match_declared(events):
    """Full event identity: kinds, track labels and the description fields
    that are declared, on every event that carries a description."""
    if [e.get("kind") for e in events] != EXPECTED_EVENTS:
        return False
    for e in events:
        if e["kind"] == "CATALOG_READY":
            if e.get("track") is not None or e.get("description") is not None:
                return False
            continue
        if e.get("track") != "T0" or not isinstance(e.get("description"), dict):
            return False
        d = e["description"]
        if any(d.get(k) != v for k, v in DECLARED_DESCRIPTION.items()):
            return False
    if events[0]["description"].get("vod") != {"is_live": True, "track_duration_ms": None}:
        return False
    return True


def image(obj, track_label):
    """Recheckable image of one public MediaObject: every compared field,
    bytes as hex, the track by identity label."""
    d = {"track": track_label}
    for f in FIELDS:
        v = getattr(obj, f)
        if isinstance(v, (bytes, bytearray)):
            v = {"hex": v.hex(), "len": len(v)}
        elif isinstance(v, tuple):
            v = [list(x) if hasattr(x, "__iter__") else x for x in v]
        elif hasattr(v, "value"):
            v = int(v)
        d[f] = v
    return d


def description_image(d):
    if d is None:
        return None
    out = {}
    for f in ("name", "codec", "packaging_text", "init_data", "role", "lang"):
        v = getattr(d, f)
        out[f] = None if v is None else {"hex": v.hex(), "len": len(v)}
    for f in ("media_type", "packaging", "timescale", "transport_version", "width", "height", "framerate_millis", "bitrate"):
        v = getattr(d, f)
        out[f] = int(v) if v is not None and hasattr(v, "value") else v
    out["vod"] = {"is_live": d.vod.is_live, "track_duration_ms": d.vod.track_duration_ms}
    return out


# ------------------------------------------------------------------ oracle RED
def oracle_red():
    import types
    track = object()
    def mk(i, is_sync, payload):
        return types.SimpleNamespace(track=track, config_generation=0, packaging=1, status=0, end_of_group=True, datagram=False,
                                     keyframe=is_sync, capture_time_us=fixture.PTS_US[i], decode_time_us=fixture.PTS_US[i],
                                     composition_offset_us=0, presentation_time_us=fixture.PTS_US[i], payload=payload, fragment=b"",
                                     mdat_offset=0, mdat_len=0, samples=())
    expected = [mk(i, s, p) for i, s, p in fixture.OBJECTS]
    good = [mk(i, s, p) for i, s, p in fixture.OBJECTS]
    wrong = [mk(i, s, p if i != 4 else p[:-1] + b"\xff") for i, s, p in fixture.OBJECTS]
    reordered = list(good)
    reordered[1], reordered[2] = reordered[2], reordered[1]
    results = {
        "identical_passes": fixture.compare(expected, good) == [],
        "empty_fails": any("count 0 != declared 8" in p for p in fixture.compare(expected, [])),
        "wrong_payload_fails": any(p.startswith("object 4: payload differs") for p in fixture.compare(expected, wrong)),
        "reordered_fails": any(p.startswith("object 1: payload differs") for p in fixture.compare(expected, reordered)),
        "problems": {"empty": fixture.compare(expected, []), "wrong": fixture.compare(expected, wrong),
                     "reordered": fixture.compare(expected, reordered)[:3]},
    }
    print(json.dumps(results, indent=1))
    return 0 if all(v for k, v in results.items() if k != "problems") else 1


# ------------------------------------------------------------------ observer
class Observer:
    def __init__(self, rx, moq5, negative, emit):
        self.rx, self.moq5, self.negative, self.emit = rx, moq5, negative, emit
        self.objects, self.events, self.outcomes, self.waits = [], [], [], []
        self.track = None
        self.sent = {}
        self.track_states = []
        self.descriptions = []

    def __getattr__(self, name):
        return getattr(self.rx, name)

    def control(self, line):
        if line in self.sent:
            return
        self.sent[line] = round(time.monotonic(), 6)
        self.emit(line)

    def subscribe(self, track, **kw):
        self.rx.subscribe(track, **kw)
        self.track = track
        self.descriptions.append(track.description)

    def poll_track(self):
        ev = self.rx.poll_track()
        if not isinstance(ev, self.moq5.PollOutcome):
            self.events.append(ev)
        else:
            self.outcomes.append(("track", ev.name))
        return ev

    def poll_object(self):
        obj = self.rx.poll_object()
        if not isinstance(obj, self.moq5.PollOutcome):
            self.objects.append(obj)
        else:
            self.outcomes.append(("object", obj.name))
        return obj

    def drained(self):
        return self.rx.drained()

    def wait(self, timeout_us):
        if self.track is not None and "go" not in self.sent:
            state = self.rx.track_state(self.track)
            self.track_states.append(state.name if hasattr(state, "name") else int(state))
            if state == self.moq5.TrackState.ACTIVE:
                self.control("go")
        if len(self.objects) == fixture.N and "ack-all" not in self.sent and not self.negative:
            self.control("ack-all")
        if any(e.kind == self.moq5.TrackEventKind.ENDED for e in self.events) and "ended-observed" not in self.sent:
            self.control("ended-observed")
        r = self.rx.wait(timeout_us)
        self.waits.append(r.name)
        return r


def bounded_loop(run_loop, latch, deadline_s):
    """Run the receive loop with a watchdog that latches the endpoint
    interrupt after deadline_s. The watchdog is GENUINELY joined (no join
    timeout) before this returns, whether the loop returned or raised, so a
    latch still executing can never overlap the caller's cleanup; a latch
    that never returns is bounded by the SUPERVISOR's child deadline (the
    child is then terminated as a whole), not by this function. The
    watchdog's own exception is retained. Returns (result_or_None,
    loop_error, watchdog_record)."""
    done = threading.Event()
    record = {"fired": False, "error": None, "settled": None}

    def watchdog():
        try:
            if not done.wait(deadline_s):
                record["fired"] = True
                latch()
        except BaseException as e:
            record["error"] = repr(e)
    t = threading.Thread(target=watchdog, daemon=True)
    t.start()
    result, error = None, None
    try:
        result = run_loop()
    except BaseException as e:
        error = "".join(traceback.format_exception(e)).strip()
    finally:
        done.set()
        t.join()
        record["settled"] = not t.is_alive()
    return result, error, record


# ------------------------------------------------------------------ client child
def client_main(run, port, package, negative):
    def emit(line):
        sys.stdout.write("CTRL " + line + "\n")
        sys.stdout.flush()
    out = {"commands": [], "command_warnings": [], "loop": None, "oracle": None, "harness_failures": [],
           "close_sequence": [], "inventory": None}
    sys.path.insert(0, package)
    import moq5
    out["package"] = {"file": moq5.__file__, "build_info": dict(moq5.build_info())}
    spec = importlib.util.spec_from_file_location("receive_loop", EXAMPLE)
    example = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(example)
    creds = run / "creds"
    url = f"moqt://127.0.0.1:{port}"
    decoded, statuses, cmafs = [], [], []
    endpoint = receiver = obs = None
    snapshots = desc_snapshot = None
    problems = None

    def runner(name, argv, **kw):
        r = cmd(argv, **kw)
        r["name"], r["expected_rc"], r["expected_diagnostic"] = name, 0, None
        out["commands"].append(r)
        out["command_warnings"].extend(collect_diagnostics(r))
        return r
    try:
        endpoint = moq5.Endpoint.connect(moq5.EndpointConfig(url=url, backend=moq5.Backend.PICOQUIC,
                                                              versions=(moq5.Version.DRAFT_18,), sni="localhost",
                                                              ca_file=str(creds / "ca.pem")))
        t0 = time.monotonic()
        while endpoint.state != moq5.EndpointState.ESTABLISHED:
            if time.monotonic() - t0 > CONNECT_S or endpoint.terminal.reason != moq5.TerminalReason.NONE:
                raise RuntimeError(f"receiver endpoint not established: state={endpoint.state} terminal={endpoint.terminal}")
            endpoint.wait(250_000)
        out["receiver_endpoint"] = {"negotiated_version": endpoint.negotiated_version, "state": endpoint.state.name,
                                    "ms": round((time.monotonic() - t0) * 1000)}
        receiver = moq5.Receiver.attach(endpoint, moq5.ReceiverConfig.flow_control(fixture.NAMESPACE, auto_subscribe=False))
        obs = Observer(receiver, moq5, negative, emit)
        t1 = time.monotonic()
        result, loop_error, watchdog = bounded_loop(
            lambda: example.receive(obs, moq5.PollOutcome, moq5.TrackEventKind, moq5.WaitResult, moq5.Packaging, moq5.MoqError,
                                    decode_raw=lambda d, p: decoded.append(p),
                                    decode_cmaf=lambda *a: cmafs.append(len(a)),
                                    note_status=lambda t, s: statuses.append(int(s)),
                                    wait_us=250_000),
            lambda: endpoint.set_interrupted(True), LOOP_S)
        out["watchdog"] = watchdog
        if watchdog["fired"]:
            out["harness_failures"].append(f"loop watchdog fired after {LOOP_S}s; interrupt latched")
        if watchdog["error"]:
            out["harness_failures"].append("watchdog thread error: " + watchdog["error"])
        if loop_error:
            out["harness_failures"].append("receive() raised: " + loop_error)
        if result is not None:
            out["loop"] = {"completed": result["completed"], "drained": result["drained"], "interrupted": result["interrupted"],
                           "objects": result["objects"], "events": result["events"], "elapsed_ms": round((time.monotonic() - t1) * 1000),
                           "terminal": {"closed": result["terminal"].closed, "fatal": result["terminal"].fatal,
                                        "fatal_code": result["terminal"].fatal_code},
                           "stats": {k: getattr(result["stats"], k) for k in
                                     ("objects_received", "objects_queued", "bytes_queued", "objects_dropped", "groups_dropped",
                                      "keyframes_dropped", "parse_drops", "overflow_events", "pause_transitions", "paused",
                                      "catalog_drops", "catalog_complete")}}
        out["endpoint_after_loop"] = {"closed": endpoint.closed, "state": endpoint.state.name,
                                      "terminal": endpoint.terminal.reason.name, "negotiated_version": endpoint.negotiated_version}
        obs.control("done")
        # ---- inventories (recheckable) and oracle ----
        track = obs.track
        expected = fixture.declared_expectations(moq5, track) if track is not None else []
        problems = fixture.compare(expected, obs.objects)
        d0 = obs.descriptions[0] if obs.descriptions else None
        desc_expect = {"name": fixture.TRACK, "packaging_text": b"loc", "packaging": moq5.Packaging.RAW, "media_type": moq5.MediaType.VIDEO,
                       "codec": fixture.CODEC, "timescale": fixture.TIMESCALE, "width": fixture.WIDTH, "height": fixture.HEIGHT,
                       "framerate_millis": fixture.FRAMERATE_MILLIS, "bitrate": fixture.BITRATE, "transport_version": 18, "init_data": b""}
        desc_problems = [] if d0 is not None else ["no ADDED description recorded (discovery missing)"]
        for k, v in desc_expect.items():
            if d0 is not None and getattr(d0, k) != v:
                desc_problems.append(f"description.{k} {getattr(d0, k)!r} != {v!r}")
        if d0 is not None and d0.vod != moq5.VodState(True, None):
            desc_problems.append(f"description.vod {d0.vod!r} != live")
        labels = {id(track): "T0"} if track is not None else {}
        def tl(t):
            return None if t is None else labels.setdefault(id(t), f"T{len(labels)}")
        out["inventory"] = {
            "declared": [image(e, "T0") for e in expected] if track is not None else "no track discovered",
            "received": [image(o, tl(o.track)) for o in obs.objects],
            "description_at_added": description_image(d0),
            "events": [{"kind": e.kind.name if hasattr(e.kind, "name") else int(e.kind), "track": tl(e.track),
                        "description": description_image(e.description)} for e in obs.events],
            "track_labels": {v: k for k, v in labels.items()},
        }
        out["oracle"] = {"inventory_problems": problems, "description_problems": desc_problems,
                         "event_kinds": [e["kind"] for e in out["inventory"]["events"]],
                         "track_states_seen": obs.track_states, "controls": obs.sent,
                         "identity_one_track": track is not None and all(o.track is track for o in obs.objects),
                         "decoded_count": len(decoded), "status_handler": statuses, "cmaf_handler": len(cmafs),
                         "decoded_matches_objects": decoded == [o.payload for o in obs.objects],
                         "poll_outcomes_tail": obs.outcomes[-6:], "waits_tail": obs.waits[-6:]}
        snapshots = [image(o, tl(o.track)) for o in obs.objects]
        desc_snapshot = description_image(d0)
        out["vmmap"], out["loaded_images"] = vmmap_inventory(os.getpid(), runner)
    except BaseException as e:
        out["harness_failures"].append("client: " + "".join(traceback.format_exception(e)).strip())
    finally:
        seq = out["close_sequence"]
        if receiver is not None:
            try:
                receiver.close(); seq.append(("receiver", "closed"))
            except BaseException as e:
                seq.append(("receiver", repr(e)))
        if endpoint is not None:
            try:
                endpoint.close(); seq.append(("endpoint", "closed"))
            except BaseException as e:
                seq.append(("endpoint", repr(e)))
        if obs is not None and snapshots is not None:
            labels = {id(obs.track): "T0"} if obs.track is not None else {}
            after = [image(o, labels.get(id(o.track), "?")) for o in obs.objects]
            d0 = obs.descriptions[0] if obs.descriptions else None
            out["inventory"]["received_after_close"] = after
            out["inventory"]["description_after_close"] = description_image(d0)
            out["oracle"]["retained_after_close_unchanged"] = after == snapshots and description_image(d0) == desc_snapshot and \
                fixture.compare(fixture.declared_expectations(moq5, obs.track) if obs.track is not None else [], obs.objects) == problems
        (run / "client.json").write_text(json.dumps(out, indent=1, default=str))
    return 0 if not out["harness_failures"] else 1


# ------------------------------------------------------------------ verdict
def verdict(rec):
    v = relay_checks(rec)
    o = rec.get("oracle") or {}
    loop = rec.get("loop") or {}
    st = loop.get("stats") or {}
    pub = rec.get("publisher") or {}
    events = pub.get("events") or []
    names = [e.get("event") for e in events]
    wrote = [e for e in events if e.get("event") == "wrote"]
    by = {}
    for e in events:                       # first occurrence only; duplicates are caught by the exact lifecycle check
        by.setdefault(e.get("event"), e)
    declared_writes = [{"index": i, "len": len(p), "is_sync": s, "pts_us": fixture.PTS_US[i], "rc": 0}
                       for i, s, p in fixture.OBJECTS]
    lifecycle = [n for n in names if n != "wrote"]
    expected_lifecycle = ["connected", "ready", "demand", "end_track", "complete", "stats", "demand_log", "shutdown"]
    if rec.get("mode") == "negative":
        expected_lifecycle.insert(3, "producer-finished")
    demand_log = (by.get("demand_log") or {}).get("entries") or []
    ep_after = rec.get("endpoint_after_loop") or {}
    close = rec.get("close_sequence") or []
    client = rec.get("client") or {}
    wd = rec.get("watchdog") or {}
    inv = rec.get("inventory") or {}
    negative = rec.get("mode") == "negative"
    # the complete publisher record sequence in declared protocol order (writes in place, not a filtered list)
    expected_sequence = [("connected",), ("ready",), ("demand",)]
    if negative:
        expected_sequence.append(("producer-finished",))
    else:
        expected_sequence += [("wrote", i, len(p), s, fixture.PTS_US[i], 0) for i, s, p in fixture.OBJECTS]
    expected_sequence += [("end_track",), ("complete",), ("stats",), ("demand_log",), ("shutdown",)]
    actual_sequence = [(e.get("event"), e.get("index"), e.get("len"), e.get("is_sync"), e.get("pts_us"), e.get("rc")) if e.get("event") == "wrote"
                       else (e.get("event"),) for e in events]
    expected_images = declared_images()          # N fixed from the fixture, whether or not discovery happened
    sel = rec.get("selected_images") or {}
    loaded = rec.get("loaded_images") or []
    pub_loaded = rec.get("publisher_images") or []
    # the REQUIRED selected set: exact keys, typed non-empty paths, well-formed SHA-256 (64 lowercase hex) before and after
    sel_ok = (isinstance(sel, dict) and set(sel) == REQUIRED_IMAGES
              and all(isinstance(v, dict) and isinstance(v.get("path"), str) and v["path"].startswith("/")
                      and is_sha256(v.get("sha256_before")) and is_sha256(v.get("sha256_after"))
                      and v["sha256_before"] == v["sha256_after"] for v in sel.values()))
    client_required = [sel[k]["path"] for k in ("module", "msquic", "libssl", "libcrypto")] if sel_ok else None
    pub_required = [sel[k]["path"] for k in ("publisher", "msquic", "libssl", "libcrypto")] if sel_ok else None
    other_client = [p for p in loaded if p not in (client_required or [])]
    other_pub = [p for p in pub_loaded if p not in (pub_required or [])]
    v.update({
        "declared_inventory_matches": o.get("inventory_problems") == [],
        "received_images_equal_declared": inv.get("received") == expected_images,
        "after_close_images_equal_declared": inv.get("received_after_close") == expected_images,
        "description_images_equal_declared": isinstance(inv.get("description_at_added"), dict)
            and all(inv["description_at_added"].get(k) == v for k, v in DECLARED_DESCRIPTION.items())
            and inv.get("description_after_close") == inv.get("description_at_added"),
        "event_identities_and_descriptions_match_declared": events_match_declared(inv.get("events") or []),
        "publisher_record_sequence_exact": actual_sequence == expected_sequence,
        "description_matches_declared": o.get("description_problems") == [],
        "one_track_identity": o.get("identity_one_track") is True,
        "all_objects_through_raw_handler_only": o.get("decoded_matches_objects") is True and o.get("status_handler") == []
            and o.get("cmaf_handler") == 0 and o.get("decoded_count") == fixture.N,
        "track_event_inventory_exact": o.get("event_kinds") == EXPECTED_EVENTS,
        "go_sent_only_after_active": "go" in o.get("controls", {}) and o.get("track_states_seen", [])[-1:] == ["ACTIVE"]
            and all(s in ("PENDING", "ACTIVE") for s in o.get("track_states_seen", [])),
        "control_order": list(o.get("controls", {}).keys()) == ["go", "ack-all", "ended-observed", "done"],
        "loop_completed_not_drained_not_interrupted": loop.get("completed") is True and loop.get("drained") is False
            and loop.get("interrupted") is False and loop.get("objects") == fixture.N and loop.get("events") == len(EXPECTED_EVENTS),
        "loop_terminal_clean": (loop.get("terminal") or {}).get("closed") is False and (loop.get("terminal") or {}).get("fatal") is False,
        "endpoint_open_after_loop_v18": ep_after.get("closed") is False and ep_after.get("state") == "ESTABLISHED"
            and ep_after.get("terminal") == "NONE" and ep_after.get("negotiated_version") == 18
            and (rec.get("receiver_endpoint") or {}).get("negotiated_version") == 18,
        "receiver_counters_no_loss": st.get("objects_received") == fixture.N and st.get("objects_queued") == 0 and st.get("bytes_queued") == 0
            and st.get("objects_dropped") == 0 and st.get("groups_dropped") == 0 and st.get("keyframes_dropped") == 0
            and st.get("parse_drops") == 0 and st.get("overflow_events") == 0 and st.get("pause_transitions") == 0
            and st.get("paused") is False and st.get("catalog_drops") == 0 and st.get("catalog_complete") is True,
        "publisher_writes_match_declared": [{k: w.get(k) for k in ("index", "len", "is_sync", "pts_us", "rc")} for w in wrote] == declared_writes
            and all(1 <= w.get("attempts", 0) <= 40 for w in wrote),
        "publisher_lifecycle_exact": lifecycle == expected_lifecycle,
        "controls_admitted_none_rejected": not rec.get("controls_rejected"),
        "publisher_counters_no_loss": (by.get("stats") or {}).get("rc") == 0 and (by.get("stats") or {}).get("written") == fixture.N
            and (by.get("stats") or {}).get("sent") == fixture.N and (by.get("stats") or {}).get("queued") == 0
            and (by.get("stats") or {}).get("dropped") == 0 and (by.get("stats") or {}).get("groups_dropped") == 0
            and (by.get("stats") or {}).get("groups_abandoned") == 0 and (by.get("stats") or {}).get("stalls") == 0
            and (by.get("stats") or {}).get("last_error") == 0,
        "publisher_lifecycle_rcs_ok": (by.get("end_track") or {}).get("rc") == 0 and (by.get("complete") or {}).get("rc") == 0
            and (by.get("shutdown") or {}).get("stats_rc_ok") is True and (by.get("shutdown") or {}).get("drain_rc") == 0
            and (by.get("shutdown") or {}).get("stop_rc") == 0 and (by.get("shutdown") or {}).get("fatal") == 0
            and (by.get("shutdown") or {}).get("exit") == 0 and "wait_error" not in names,
        # demand contract: the first logged entry for the returned track is a join with one active subscription;
        # every entry belongs to the returned track; the log dropped nothing. Ordering of later join/leave entries
        # is not asserted beyond that.
        "publisher_demand_contract": bool(demand_log) and demand_log[0].get("joined") is True and demand_log[0].get("active") == 1
            and all(e.get("track_is_returned") is True for e in demand_log) and (by.get("demand_log") or {}).get("dropped") == 0
            and (by.get("demand") or {}).get("active") == 1 and (by.get("demand") or {}).get("query") == 1,
        "publisher_exited_0_readers_settled": pub.get("exit") == 0 and pub.get("how") == "exited" and pub.get("readers_settled") is True
            and not pub.get("reader_errors") and not pub.get("write_errors") and pub.get("stderr") == "",
        "client_exited_0_readers_settled": client.get("exit") == 0 and client.get("how") == "exited" and client.get("readers_settled") is True
            and not client.get("reader_errors"),
        "controls_forwarded_in_order": [c.get("line") for c in rec.get("controls_forwarded") or []] == ["go", "ack-all", "ended-observed", "done"]
            and all(c.get("forwarded") is True for c in rec.get("controls_forwarded") or []),
        "watchdog_settled_not_fired": wd.get("settled") is True and wd.get("fired") is False and wd.get("error") is None,
        "retained_values_unchanged_after_close": o.get("retained_after_close_unchanged") is True,
        "close_order_receiver_then_endpoint": [tuple(x) for x in close] == [("receiver", "closed"), ("endpoint", "closed")],
        "all_children_settled": rec.get("threads_settled") is True and all(c.get("settled") for c in rec.get("children") or []),
        # The exact required selected images loaded in the right process, file identities unchanged across the run,
        # and any other image IN THE SCOPED INVENTORY explicitly classified. Scope: vmmap_inventory filters the
        # process's mapped paths by project/TLS name fragments (see runtime_support), so this is a statement about
        # that scoped inventory, not about every DSO the process maps; the full vmmap output is retained in the
        # command records.
        "loaded_images_client_and_publisher": sel_ok
            and (rec.get("vmmap") or {}).get("rc") == 0 and not (rec.get("vmmap") or {}).get("stderr")
            and (rec.get("publisher_vmmap") or {}).get("rc") == 0 and not (rec.get("publisher_vmmap") or {}).get("stderr")
            and all(p in loaded for p in client_required) and all(p in pub_loaded for p in pub_required)
            and sel["module"]["path"] not in pub_loaded and sel["publisher"]["path"] not in loaded
            and set(other_client) <= set(SYSTEM_SSL_IMAGES) and set(other_pub) <= set(SYSTEM_SSL_IMAGES),
    })
    return v


def apply_set(node, path, value):
    keys = path.split(".")
    for k in keys[:-1]:
        node = node[int(k)] if isinstance(node, list) else node[k]
    k = keys[-1]
    if isinstance(node, list):
        node[int(k)] = value
    else:
        node[k] = value


def verdict_red(path):
    """RED-first: mutations of an ACTUAL receipt must each fail by name."""
    import copy
    base = json.loads(Path(path).read_text())
    base_v = verdict(base)
    def ev(r, name, n=0):
        return [e for e in r["publisher"]["events"] if e["event"] == name][n]
    def move_writes_last(r):
        evs = r["publisher"]["events"]
        r["publisher"]["events"] = [e for e in evs if e["event"] != "wrote"] + [e for e in evs if e["event"] == "wrote"]
    # name -> (mutation, required named check, other checks allowed to fail as a consequence)
    mutations = {
        "foreign_ENDED_track_T9": (lambda r: apply_set(r["inventory"]["events"][2], "track", "T9"),
                                   "event_identities_and_descriptions_match_declared", set()),
        "ADDED_description_timescale_1": (lambda r: apply_set(r["inventory"]["events"][0]["description"], "timescale", 1),
                                          "event_identities_and_descriptions_match_declared", set()),
        "writes_moved_after_lifecycle": (move_writes_last, "publisher_record_sequence_exact", set()),
        "module_path_replaced": (lambda r: apply_set(r, "loaded_images", ["/tmp/foreign/_native.abi3.so"] + [p for p in r["loaded_images"] if not p.endswith("_native.abi3.so")]),
                                 "loaded_images_client_and_publisher", set()),
        "wrong_received_payload_image_cached_problems_unchanged": (lambda r: apply_set(r["inventory"]["received"][5]["payload"], "hex", "00"),
                                                                   "received_images_equal_declared", set()),
        "shutdown_drain_rc_-8": (lambda r: apply_set(ev(r, "shutdown"), "drain_rc", -8), "publisher_lifecycle_rcs_ok", set()),
        "receiver_overflow_and_catalog_drops": (lambda r: (apply_set(r, "loop.stats.overflow_events", 1), apply_set(r, "loop.stats.catalog_drops", 1)),
                                                "receiver_counters_no_loss", set()),
        "extra_UPDATED_and_CATALOG_READY_events": (lambda r: (r["oracle"]["event_kinds"].extend(["UPDATED", "CATALOG_READY"]),
                                                              r["inventory"]["events"].extend([{"kind": "UPDATED", "track": "T0", "description": r["inventory"]["events"][0]["description"]},
                                                                                               {"kind": "CATALOG_READY", "track": None, "description": None}])),
                                                   "track_event_inventory_exact", {"event_identities_and_descriptions_match_declared"}),
        "duplicate_complete_record": (lambda r: r["publisher"]["events"].insert(
            [i for i, e in enumerate(r["publisher"]["events"]) if e["event"] == "complete"][0], {"event": "complete", "rc": 0}),
                                      "publisher_lifecycle_exact", {"publisher_record_sequence_exact"}),
        "wrong_write_identity_same_count": (lambda r: apply_set(ev(r, "wrote", 3), "len", 99),
                                            "publisher_writes_match_declared", {"publisher_record_sequence_exact"}),
        "empty_received_images_N_fixed": (lambda r: (apply_set(r, "inventory.received", []), apply_set(r, "oracle.inventory_problems", ["count 0 != declared 8"])),
                                          "received_images_equal_declared", {"declared_inventory_matches"}),
        "close_order_swapped": (lambda r: apply_set(r, "close_sequence", [["endpoint", "closed"], ["receiver", "closed"]]),
                                "close_order_receiver_then_endpoint", set()),
        "watchdog_fired": (lambda r: apply_set(r, "watchdog.fired", True), "watchdog_settled_not_fired", set()),
        "publisher_stderr_text": (lambda r: apply_set(r, "publisher.stderr", "publisher: something"), "publisher_exited_0_readers_settled", set()),
        "demand_log_foreign_track": (lambda r: apply_set(ev(r, "demand_log")["entries"][0], "track_is_returned", False),
                                     "publisher_demand_contract", set()),
        "receiver_endpoint_v16": (lambda r: apply_set(r, "receiver_endpoint.negotiated_version", 16), "endpoint_open_after_loop_v18", set()),
        "publisher_stop_rc_-5": (lambda r: apply_set(ev(r, "shutdown"), "stop_rc", -5), "publisher_lifecycle_rcs_ok", set()),
        "control_not_forwarded": (lambda r: apply_set(r["controls_forwarded"][1], "forwarded", False), "controls_forwarded_in_order", set()),
        "control_rejected_recorded": (lambda r: apply_set(r, "controls_rejected", [{"line": "go", "why": "repeated"}]), "controls_admitted_none_rejected", set()),
        "image_identity_changed": (lambda r: apply_set(r["selected_images"]["module"], "sha256_after", "0" * 64), "loaded_images_client_and_publisher", set()),
        "msquic_requirement_and_observation_removed": (lambda r: (r["selected_images"].pop("msquic"),
                                                                  apply_set(r, "loaded_images", [p for p in r["loaded_images"] if not p.endswith("libmsquic.2.6.0.dylib")]),
                                                                  apply_set(r, "publisher_images", [p for p in r["publisher_images"] if not p.endswith("libmsquic.2.6.0.dylib")])),
                                                       "loaded_images_client_and_publisher", set()),
        "hashes_not_hexadecimal_but_equal": (lambda r: [ (apply_set(v, "sha256_before", "z" * 64), apply_set(v, "sha256_after", "z" * 64)) for v in r["selected_images"].values()],
                                             "loaded_images_client_and_publisher", set()),
        "hashes_hex_with_trailing_newline_equal": (lambda r: [ (apply_set(v, "sha256_before", "a" * 64 + "\n"), apply_set(v, "sha256_after", "a" * 64 + "\n")) for v in r["selected_images"].values()],
                                                   "loaded_images_client_and_publisher", set()),
        "hashes_integer_not_string_equal": (lambda r: [ (apply_set(v, "sha256_before", int("1" * 64)), apply_set(v, "sha256_after", int("1" * 64))) for v in r["selected_images"].values()],
                                            "loaded_images_client_and_publisher", set()),
        "module_requirement_and_observation_removed": (lambda r: (r["selected_images"].pop("module"),
                                                                  apply_set(r, "loaded_images", [p for p in r["loaded_images"] if not p.endswith("_native.abi3.so")])),
                                                       "loaded_images_client_and_publisher", set()),
        "unclassified_foreign_image": (lambda r: r["loaded_images"].append("/tmp/foreign/libwhatever.dylib"), "loaded_images_client_and_publisher", set()),
    }
    report = {"baseline_failed": [k for k, ok in base_v.items() if not ok], "controls": {}}
    for name, (mutate, required, extras) in mutations.items():
        r = copy.deepcopy(base)
        mutate(r)
        failed = {k for k, ok in verdict(r).items() if not ok}
        report["controls"][name] = {"required": required, "failed": sorted(failed),
                                    "attributed": required in failed and failed <= ({required} | extras)}
    report["all_named"] = not report["baseline_failed"] and all(v["attributed"] for v in report["controls"].values())
    print(json.dumps(report, indent=1))
    return 0 if report["all_named"] else 1


# ------------------------------------------------------------------ supervisor
def supervise(run, relay, publisher_bin, package, negative, transport_mode, child_argv=None, publisher_argv=None,
              client_timeout=CLIENT_S, ready_timeout=READY_S):
    """The one supervisor: owns relay (optional), publisher and client. The
    offline child controls pass stub argvs and no relay."""
    h = Harness(run, relay)
    h.rec.update({"mode": "negative" if negative else "positive", "transport_mode": transport_mode})
    pub = client = None
    try:
        if relay is not None:
            h.rec["credentials"] = h.make_credentials()
            port = free_udp_port()
            h.start_relay(port)
        else:
            port = 0
        creds = run / "creds"
        objects_file = run / "declared_objects.bin"
        fixture.write_objects_file(objects_file)
        env = {"PATH": "/usr/bin:/bin", "DYLD_FALLBACK_LIBRARY_PATH": MSQUIC_DIR}
        ready = threading.Event()
        pub_events = []

        def on_pub_line(line):
            if line.startswith("{"):
                try:
                    e = json.loads(line)
                except ValueError:
                    return
                pub_events.append(e)
                if e.get("event") == "ready":
                    ready.set()
        argv = publisher_argv or [publisher_bin, f"moqt://127.0.0.1:{port}", NS_TEXT, fixture.TRACK.decode(), creds / "ca.pem",
                                  "localhost", objects_file, "negative" if negative else "positive", transport_mode]
        pub = h.spawn("publisher", argv, env, stdin=True, on_line=on_pub_line)
        if not ready.wait(ready_timeout):
            if not pub.alive():
                rec = pub.settle(1)
                h.fail(f"publisher exited before ready: rc {rec['exit']}: {rec['stderr'][-300:]}")
            h.fail(f"publisher not ready within {ready_timeout}s")
        if relay is not None:
            h.rec["publisher_vmmap"], h.rec["publisher_images"] = vmmap_inventory(pub.proc.pid, h.run_cmd)
            # the selected immutable images and their identities BEFORE the run
            selected = {"module": str(Path(package) / "moq5" / "_native.abi3.so"), "publisher": str(Path(publisher_bin).resolve()),
                        "msquic": MSQUIC_IMAGE, "libssl": OPENSSL3_IMAGES[0], "libcrypto": OPENSSL3_IMAGES[1]}
            h.rec["selected_images"] = {k: {"path": p, "sha256_before": sha256_file(p)} for k, p in selected.items()}
        forwarded = []
        protocol = ControlProtocol(CONTROLS_NEGATIVE if negative else CONTROLS_POSITIVE)

        def on_client_line(line):
            if line.startswith("CTRL "):
                ctrl = line[5:]
                if protocol.admit(ctrl):
                    ok = pub.write_line(ctrl)
                    forwarded.append({"line": ctrl, "forwarded": ok, "t": round(time.monotonic(), 6)})
        cargv = child_argv or [sys.executable, "-I", "-W", "error", Path(__file__).resolve(), "--client", run, port, package,
                               "negative" if negative else "positive"]
        client = h.spawn("client", cargv, env, on_line=on_client_line)
        crec = client.settle(client_timeout)
        h.rec["commands"].append(crec)
        h.rec["command_warnings"].extend(collect_diagnostics(crec))
        h.rec["client"] = {k: crec[k] for k in ("pid", "exit", "how", "readers_settled", "reader_errors", "timeout")}
        h.rec["controls_forwarded"] = forwarded
        h.rec["controls_rejected"] = protocol.rejected
        prec = pub.settle(PUBLISHER_EXIT_S)
        h.rec["commands"].append(prec)
        h.rec["command_warnings"].extend(collect_diagnostics(prec))
        h.rec["publisher"] = {k: prec[k] for k in ("pid", "exit", "how", "readers_settled", "reader_errors", "write_errors", "timeout", "stderr")}
        h.rec["publisher"]["events"] = pub_events
        h.rec["publisher"]["stdout_other"] = [l for l in pub.stdout_lines if not l.startswith("{")]
        if crec["timeout"]:
            h.fail(f"client child: {crec['how']} (deadline {client_timeout}s)")
        if crec["exit"] != 0:
            h.fail(f"client child: rc {crec['exit']}: {crec['stderr'][-400:]}")
    except BaseException as e:
        h.rec["primary_error"] = repr(e)
    finally:
        # every owned handle settled, in order: client, publisher, then relay
        h.settle_all([(c, PUBLISHER_EXIT_S if c is pub else 5.0) for c in (client, pub) if c is not None])
        for key, c in (("client", client), ("publisher", pub)):
            if c is not None and key not in h.rec and c.settled:
                h.rec[key] = {k: c.settled[k] for k in ("pid", "exit", "how", "readers_settled", "reader_errors", "timeout", "stderr")}
                if key == "publisher":
                    h.rec[key]["events"] = pub_events
        try:
            if relay is not None and hasattr(h, "relay"):
                h.stop_relay()
        except BaseException as e:
            h.rec["cleanup_errors"].append(repr(e))
        h.rec["threads_settled"] = all(c.settled and c.settled["readers_settled"] for c in h.children)
        for k, v in (h.rec.get("selected_images") or {}).items():
            try:
                v["sha256_after"] = sha256_file(v["path"])
            except OSError as e:
                v["sha256_after"] = repr(e)
        h.rec["children"] = [{"name": c.name, "pid": c.proc.pid, "exit": c.proc.poll(), "settled": bool(c.settled),
                              "how": c.settled and c.settled["how"]} for c in h.children]
        client_file = run / "client.json"
        if client_file.exists():
            c = json.loads(client_file.read_text())
            h.rec["commands"].extend(c.pop("commands", []))
            h.rec["command_warnings"].extend(c.pop("command_warnings", []))
            h.rec["harness_failures"].extend(c.pop("harness_failures", []))
            h.rec.update(c)
        h.rec["verdict"] = verdict(h.rec) if relay is not None else None
        (run / "results.json").write_text(json.dumps(h.rec, indent=1, default=str))
    return h.rec


# ------------------------------------------------------------------ offline child controls
def child_controls():
    """Bounded offline ownership controls with stub children (no network, no
    relay, no real unrelated PID): every owned handle must be settled by the
    supervisor itself and every error retained."""
    root = HERE.parent.parent.parent.parent / "build" / "py-runtime-fixture"
    root.mkdir(parents=True, exist_ok=True)
    base = root / ("child-controls-" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
    base.mkdir(mode=0o700)
    py = sys.executable
    ready_stub = [py, "-c", "import sys, time; print('{\"event\":\"ready\"}', flush=True); time.sleep(60)"]
    results = {}
    # 1. client failure/timeout with a live owned publisher: the supervisor must settle the publisher itself
    (base / "client_timeout").mkdir()
    r = supervise(base / "client_timeout", None, None, None, False, "pull", child_argv=[py, "-c", "import time; time.sleep(60)"],
                  publisher_argv=ready_stub, client_timeout=2.0)
    results["client_timeout_live_publisher"] = {
        "primary": r["primary_error"], "children": r["children"], "threads_settled": r["threads_settled"],
        "ok": "client child: SIGTERM_after_timeout" in (r["primary_error"] or "") and all(c["settled"] and c["exit"] is not None for c in r["children"])
              and r["threads_settled"] and [c["how"] for c in r["children"]] == ["SIGTERM_after_timeout", "SIGTERM_after_timeout"]}
    # 2. publisher early exit: recorded as the primary failure, client never launched, publisher settled
    (base / "publisher_early_exit").mkdir()
    r = supervise(base / "publisher_early_exit", None, None, None, False, "pull", child_argv=[py, "-c", "print('unused')"],
                  publisher_argv=[py, "-c", "import sys; sys.stderr.write('publisher: fatal\\n'); sys.exit(3)"], ready_timeout=3.0)
    results["publisher_early_exit"] = {
        "primary": r["primary_error"], "children": r["children"], "threads_settled": r["threads_settled"],
        "ok": "publisher exited before ready: rc 3" in (r["primary_error"] or "") and len(r["children"]) == 1 and r["children"][0]["settled"]
              and r["threads_settled"]}
    # 3. receive() raises before loop_done: the watchdog is settled, the error retained, no late latch
    latched = []
    def boom():
        raise ValueError("receive failed")
    result, err, wd = bounded_loop(boom, lambda: latched.append(True), 5.0)
    results["receive_raises_before_loop_done"] = {"result": result, "error_retained": err is not None and "receive failed" in err,
                                                  "watchdog": wd, "latched": list(latched),          # snapshot at capture
                                                  "ok": result is None and err is not None and wd["settled"] is True and wd["fired"] is False and latched == []}
    # 4. a hanging loop: the watchdog fires the latch exactly once and is settled
    ev = threading.Event()
    def hang():
        ev.wait(10)
        return "returned after latch"
    result, err, wd = bounded_loop(hang, lambda: (latched.append("latched"), ev.set()), 1.0)
    results["hanging_loop_latched"] = {"result": result, "watchdog": wd, "latched": list(latched),
                                       "ok": result == "returned after latch" and wd["fired"] is True and wd["settled"] is True and latched == ["latched"]}
    # 5. Event-controlled DELAYED latch, load-bearing against the old timed join. The latch HOLDS a gate that
    #    only this test releases; bounded_loop runs on a worker. With the genuine join the worker cannot return
    #    while the latch is held (checked well past the old 5 s join window); the old join(5) variant returns early
    #    with settled=False. Every thread is released and joined on every path; a finite outer guard bounds the row.
    def old_bounded_loop(run_loop, latch, deadline_s):          # test-only replica of the superseded t.join(5) code
        done = threading.Event(); record = {"fired": False, "error": None, "settled": None}
        def watchdog():
            if not done.wait(deadline_s):
                record["fired"] = True; latch()
        t = threading.Thread(target=watchdog, daemon=True); t.start()
        try:
            return run_loop(), None, record
        finally:
            done.set(); t.join(5); record["settled"] = not t.is_alive()

    def delayed_latch_row(impl):
        entered, release, timeline, out = threading.Event(), threading.Event(), [], {}
        def held_latch():
            timeline.append("latch-enter"); entered.set(); release.wait(20); timeline.append("latch-return")
        def worker():
            out["value"] = impl(lambda: threading.Event().wait(0.2), held_latch, 0.05)
            timeline.append("impl-returned")
        w = threading.Thread(target=worker, daemon=True)
        try:
            w.start()
            entered_ok = entered.wait(5)
            w.join(6.0)                                     # longer than the superseded join(5): a timed join returns here
            returned_while_held = not w.is_alive()
            release.set()
            w.join(10)
            worker_settled = not w.is_alive()
            wd = out.get("value", (None, None, {}))[2]
            return {"entered": entered_ok, "returned_while_latch_held": returned_while_held, "timeline": list(timeline),
                    "watchdog": wd, "worker_settled": worker_settled,
                    "ok": entered_ok and not returned_while_held and worker_settled and wd.get("settled") is True and wd.get("fired") is True
                          and timeline == ["latch-enter", "latch-return", "impl-returned"]}
        finally:
            release.set(); w.join(10)
    old_row = delayed_latch_row(old_bounded_loop)
    new_row = delayed_latch_row(bounded_loop)
    results["delayed_latch_old_join_RED"] = {**old_row, "ok": old_row["returned_while_latch_held"] is True and old_row["watchdog"].get("settled") is False
                                             and old_row["worker_settled"] and old_row["timeline"][:2] == ["latch-enter", "impl-returned"]}
    results["delayed_latch_joined_before_return"] = new_row
    results["no_test_thread_alive"] = {"active": threading.active_count(), "ok": threading.active_count() == 1}
    # 6. cmd(): a timed-out real child with partial stdout+stderr (incl. a warning) is retained as TEXT; reaping is
    #    proven through the OWNED Popen handle (a local observation wrapper around the real implementation), not a
    #    global process query. subprocess.run's kill-and-wait on timeout is preserved.
    import runtime_support as _rs
    observed = []
    class ObservedPopen(_rs.subprocess.Popen):
        def __init__(self, *a, **k):
            super().__init__(*a, **k); observed.append(self)
    real_popen = _rs.subprocess.Popen
    _rs.subprocess.Popen = ObservedPopen
    try:
        r = cmd([sys.executable, "-c", "import sys, time; print('partial', flush=True); sys.stderr.write('warning: partial diag\\n'); sys.stderr.flush(); time.sleep(30)"], timeout=0.5)
        r2 = cmd([sys.executable, "-c", "import time; time.sleep(30)"], timeout=0.3)
    finally:
        _rs.subprocess.Popen = real_popen
    r["name"], r2["name"] = "timeout-stub", "timeout-silent"
    diags = collect_diagnostics(r)
    results["cmd_timeout_partial_text"] = {"timeout": r["timeout"], "stdout": r["stdout"], "stderr": r["stderr"], "diagnostics": diags,
        "ok": r["timeout"] is True and isinstance(r["stdout"], str) and isinstance(r["stderr"], str) and r["stdout"].strip() == "partial"
              and diags == ["timeout-stub [stderr]: warning: partial diag"]}
    results["cmd_timeout_no_output"] = {"timeout": r2["timeout"], "stdout": r2["stdout"], "stderr": r2["stderr"],
        "ok": r2["timeout"] is True and r2["stdout"] == "" and r2["stderr"] == "" and collect_diagnostics(r2) == []}
    handles = [{"pid": pp.pid, "returncode": pp.returncode, "poll": pp.poll()} for pp in observed]
    results["cmd_timeout_children_reaped_via_owned_handles"] = {"handles": handles,
        "ok": len(handles) == 2 and all(h["returncode"] is not None and h["poll"] is not None and h["returncode"] < 0 for h in handles)}
    # 7. the finite control protocol: malformed, duplicate, out-of-order and overlong lines are rejected and never forwarded
    from runtime_support import ControlProtocol, CONTROL_MAX
    echo_stub = [py, "-c", "import sys\nfor line in sys.stdin: sys.stdout.write('ECHO ' + line); sys.stdout.flush()"]
    h = Harness(base / "control_protocol", None)
    (base / "control_protocol").mkdir()
    stub = h.spawn("publisher-stub", echo_stub, {"PATH": "/usr/bin:/bin"}, stdin=True)
    proto = ControlProtocol(["go", "ack-all", "ended-observed", "done"])
    attempts = ["ack-all", "bogus", "go", "go", "x" * (CONTROL_MAX + 1), "ack-all", "ended-observed", "done", "done"]
    forwarded = [a for a in attempts if proto.admit(a) and stub.write_line(a)]
    srec = stub.settle(3.0)
    results["control_protocol_finite"] = {"forwarded": forwarded, "rejected": proto.rejected, "echoed": srec["stdout"].splitlines(),
        "ok": forwarded == ["go", "ack-all", "ended-observed", "done"] and [x["why"] for x in proto.rejected] == ["out-of-order", "unknown", "repeated", "overlong", "repeated"]
              and srec["stdout"].splitlines() == ["ECHO go", "ECHO ack-all", "ECHO ended-observed", "ECHO done"] and srec["exit"] == 0 and srec["readers_settled"]}
    results["all_ok"] = all(v["ok"] for k, v in results.items() if k != "all_ok")
    (base / "child-controls.json").write_text(json.dumps(results, indent=1, default=str))
    print(json.dumps({"run_dir": str(base), **results}, indent=1, default=str))
    return 0 if results["all_ok"] else 1


def control_selftests(publisher_bin):
    """The C control reader's classification with an outer timeout guard."""
    env = {"PATH": "/usr/bin:/bin", "DYLD_FALLBACK_LIBRARY_PATH": MSQUIC_DIR}
    def run_case(name, stdin_bytes, close_stdin):
        p = subprocess.Popen([publisher_bin, "--control-selftest"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, env=env)
        if stdin_bytes:
            p.stdin.write(stdin_bytes); p.stdin.flush()
        if close_stdin:
            p.stdin.close()
        try:
            p.wait(timeout=10)                                       # outer guard; the reader's own deadline is 2 s
        except subprocess.TimeoutExpired:
            p.kill(); p.wait(5)
            return {"name": name, "class": "OUTER-TIMEOUT", "rc": p.returncode}
        out, err = p.stdout.read(), p.stderr.read()                # stdin held open until the child exited (small outputs)
        if not close_stdin:
            p.stdin.close()
        lines = [l for l in out.decode().splitlines() if l.startswith("{")]
        rec = json.loads(lines[-1]) if lines else {"class": "NO-RECORD"}
        return {"name": name, "class": rec["class"], "rc": p.returncode, "stderr": err.decode().strip()}
    cases = [("exact line", b"go\n", True, "ok"),
             ("partial line no newline (stdin held open)", b"g", False, "partial"),
             ("overlength line", b"x" * 40 + b"\n", True, "overlength"),
             ("eof, nothing sent", b"", True, "eof"),
             ("mismatch", b"nope\n", True, "mismatch"),
             ("embedded NUL after the exact prefix", b"go\x00junk\n", True, "mismatch"),
             ("exact prefix with suffix", b"go!\n", True, "mismatch"),
             ("nothing sent, stdin held open", b"", False, "timeout")]
    results = []
    for name, data, close, expect in cases:
        r = run_case(name, data, close)
        r["expected"], r["ok"] = expect, r["class"] == expect and r["rc"] == 0
        results.append(r)
    print(json.dumps(results, indent=1))
    return 0 if all(r["ok"] for r in results) else 1


def main(argv):
    if argv[1:2] == ["--oracle-red"]:
        return oracle_red()
    if argv[1:2] == ["--verdict-red"]:
        return verdict_red(argv[2])
    if argv[1:2] == ["--child-controls"]:
        return child_controls()
    if argv[1:2] == ["--control-selftests"]:
        return control_selftests(argv[3])
    if argv[1:2] == ["--client"]:
        return client_main(Path(argv[2]), int(argv[3]), argv[4], argv[5] == "negative")
    flags = [a for a in argv[1:] if a in ("--negative", "--push")]
    rest = [a for a in argv[1:] if a not in flags]
    args = dict(zip(rest[0::2], rest[1::2]))
    negative, transport_mode = "--negative" in flags, ("push" if "--push" in flags else "pull")
    root = Path(args.get("--runs", str(HERE.parent.parent.parent.parent / "build" / "py-runtime-fixture")))
    root.mkdir(parents=True, exist_ok=True)
    run = root / (("negative-" if negative else "positive-") + datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
    run.mkdir(mode=0o700)
    rec = supervise(run, args["--relay"], args["--publisher"], args["--package"], negative, transport_mode)
    v = rec["verdict"]
    print(json.dumps({"run_dir": str(run), "mode": rec["mode"], "transport_mode": transport_mode,
                      "failed": [k for k, ok in v.items() if not ok], "oracle": rec.get("oracle"), "loop": rec.get("loop"),
                      "publisher_events": (rec.get("publisher") or {}).get("events"),
                      "publisher": {k: (rec.get("publisher") or {}).get(k) for k in ("exit", "how", "readers_settled", "stderr")},
                      "client": rec.get("client"), "children": rec.get("children"), "relay_stop": (rec.get("relay") or {}).get("stop"),
                      "close_sequence": rec.get("close_sequence"), "watchdog": rec.get("watchdog"), "controls_forwarded": rec.get("controls_forwarded"),
                      "warnings": rec["command_warnings"], "primary_error": rec["primary_error"], "cleanup_errors": rec["cleanup_errors"],
                      "harness_failures": rec["harness_failures"]}, indent=1, default=str))
    return 0 if all(v.values()) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
