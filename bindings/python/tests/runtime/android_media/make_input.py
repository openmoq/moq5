"""Generates and validates the Android real-media input from marker_recipe.json.

    python -I make_input.py <run dir>

Writes into <run dir> (never into the source tree): in.h264, objects.bin (the
runtime publisher's input and the exact receiver oracle's expectation),
expected.tsv (index, len, is_sync, pts_us, sha256 per access unit) and
input-receipt.json. The receipt records the ffmpeg version and every check;
the media hash is evidence, not a cross-machine pin, because encoder output
may differ between ffmpeg builds. The per-run gate is two identical
generations plus the structural and decoded-marker checks below.
"""
import hashlib
import json
import random
import shutil
import struct
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import media_marker          # noqa: E402
import media_oracle          # noqa: E402

RECIPE = json.loads((HERE / "marker_recipe.json").read_text())
CMD_TIMEOUT_S = 60


def marker_filter():
    m = RECIPE["marker"]
    cell, parts = m["cell"], []
    data_y, comp_y = (r["y"] for r in m["rows"])
    for k in m["bit_cells"]:
        x, on = k * cell, f"eq(mod(floor(n/{1 << k}),2),1)"
        parts += [f"drawbox=x={x}:y={data_y}:w={cell}:h={cell}:color=black:t=fill",
                  f"drawbox=x={x}:y={data_y}:w={cell}:h={cell}:color=white:t=fill:enable='{on}'",
                  f"drawbox=x={x}:y={comp_y}:w={cell}:h={cell}:color=white:t=fill",
                  f"drawbox=x={x}:y={comp_y}:w={cell}:h={cell}:color=black:t=fill:enable='{on}'"]
    for row in m["rows"]:
        for c, colour in sorted(row["sync"].items()):
            parts.append(f"drawbox=x={int(c) * cell}:y={row['y']}:w={cell}:h={cell}:color={colour}:t=fill")
    return ",".join(parts)


def _source():
    s = RECIPE["source"]
    return ["-f", "lavfi", "-i", f"{s['filter']}=size={s['width']}x{s['height']}:rate={s['rate']}", "-frames:v", str(s["frames"])]


def _run(argv, **kw):
    r = subprocess.run(argv, capture_output=True, timeout=CMD_TIMEOUT_S, **kw)
    return r.returncode, r.stdout, r.stderr.decode(errors="replace")


def _start_codes(b):
    out, i = [], 0
    while True:
        j = b.find(b"\x00\x00\x01", i)
        if j < 0:
            return out
        out.append((j - 1 if j > 0 and b[j - 1] == 0 else j, j + 3))
        i = j + 3


def split_access_units(b):
    nals = [(s, h, b[h] & 0x1F) for s, h in _start_codes(b)]
    auds = [s for s, _h, t in nals if t == 9]
    aus = [b[auds[k]:(auds[k + 1] if k + 1 < len(auds) else len(b))] for k in range(len(auds))]
    return aus, (auds[0] if auds else None), nals


def _nal_types(au):
    return [au[h] & 0x1F for _s, h in _start_codes(au)]


def _frames(raw, w, h):
    n = len(raw) // (w * h)
    return [raw[i * w * h:(i + 1) * w * h] for i in range(n)]


def _shift_right(frame, w, h, px, fill=16):
    out = bytearray([fill]) * (w * h)
    for y in range(h):
        out[y * w + px:(y + 1) * w] = frame[y * w:(y + 1) * w - px]
    return bytes(out)


def _shift_down(frame, w, h, px, fill=128):
    return bytes([fill]) * (w * px) + frame[:w * (h - px)]


def generate(run_dir, ffmpeg=None, ffprobe=None):
    """The run's media from `ffmpeg`/`ffprobe` as given (absolute paths), or as found on PATH when
    neither is given."""
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    if ffmpeg is None and ffprobe is None:
        ffmpeg, ffprobe = shutil.which("ffmpeg"), shutil.which("ffprobe")
    ffmpeg, ffprobe = (str(t) if t is not None else None for t in (ffmpeg, ffprobe))
    checks = {}
    receipt = {"recipe_sha256": hashlib.sha256((HERE / "marker_recipe.json").read_bytes()).hexdigest(),
               "ffmpeg": ffmpeg, "ffprobe": ffprobe, "checks": checks, "commands": []}
    if not ffmpeg or not ffprobe:
        receipt.update(ok=False, error="ffmpeg/ffprobe: not both given, nor both on PATH")
        return receipt
    rc, out, _ = _run([ffmpeg, "-hide_banner", "-version"])
    receipt["ffmpeg_version"] = out.decode(errors="replace").splitlines()[0] if rc == 0 else None
    vf = marker_filter()
    receipt["filter_sha256"] = hashlib.sha256(vf.encode()).hexdigest()
    outputs = []
    for label in ("a", "b"):
        target = run_dir / f"in-{label}.h264"
        argv = [ffmpeg, "-hide_banner", "-nostdin", "-loglevel", "error", *_source(), "-vf", vf, *RECIPE["encoder"], str(target)]
        rc, _o, err = _run(argv)
        receipt["commands"].append({"argv": argv, "rc": rc, "stderr": err})
        if rc != 0 or err:
            receipt.update(ok=False, error=f"generation {label}: rc {rc} stderr {err.strip()[:300]!r}")
            return receipt
        outputs.append(target.read_bytes())
    checks["two_runs_identical"] = outputs[0] == outputs[1]
    media = outputs[0]
    (run_dir / "in.h264").write_bytes(media)
    for label in ("a", "b"):
        (run_dir / f"in-{label}.h264").unlink()
    receipt["sha256"], receipt["bytes"] = hashlib.sha256(media).hexdigest(), len(media)

    aus, lead, nals = split_access_units(media)
    frames = RECIPE["source"]["frames"]
    checks["au_count_declared"] = len(aus) == frames
    checks["au_concatenation"] = lead == 0 and b"".join(aus) == media
    idr = [5 in _nal_types(au) for au in aus]
    checks["idr_positions_declared"] = [i for i, x in enumerate(idr) if x] == RECIPE["expected_idr"]
    sps = [h for _s, h, t in nals if t == 7]
    codec = "avc1.%02X%02X%02X" % (media[sps[0] + 1], media[sps[0] + 2], media[sps[0] + 3]) if sps else None
    receipt["codec"] = codec
    checks["sps_codec_matches"] = codec is not None and all(
        "avc1.%02X%02X%02X" % (media[h + 1], media[h + 2], media[h + 3]) == codec for h in sps)
    rc, probe, err = _run([ffprobe, "-v", "error", "-f", "h264", "-show_packets", "-show_entries", "packet=size,flags",
                           "-of", "csv=p=0", str(run_dir / "in.h264")])
    rows = [line.split(",") for line in probe.decode().splitlines() if line]
    checks["ffprobe_sizes_match"] = rc == 0 and not err and [int(r[0]) for r in rows] == [len(a) for a in aus]
    checks["ffprobe_keyframes_match_idr"] = rc == 0 and [("K" in r[1]) for r in rows] == idr

    step = RECIPE["pts_us_step"]
    with open(run_dir / "objects.bin", "wb") as f:
        for i, au in enumerate(aus):
            f.write(struct.pack("<IBQ", len(au), 1 if idr[i] else 0, i * step))
            f.write(au)
    with open(run_dir / "expected.tsv", "w") as f:
        f.write("index\tlen\tis_sync\tpts_us\tsha256\n")
        for i, au in enumerate(aus):
            f.write(f"{i}\t{len(au)}\t{int(idr[i])}\t{i * step}\t{hashlib.sha256(au).hexdigest()}\n")
    loaded = media_oracle.load_objects(run_dir / "objects.bin")
    checks["objects_file_matches_ledger"] = [(e.payload, e.is_sync, e.pts_us) for e in loaded] == \
        [(au, idr[i], i * step) for i, au in enumerate(aus)]

    decoded, shifts = {}, {}
    for scale in RECIPE["verify_scales"]:
        w, h = map(int, scale.split("x"))
        for scaler in RECIPE["verify_scalers"]:
            rc, raw, err = _run([ffmpeg, "-hide_banner", "-nostdin", "-loglevel", "error", "-i", str(run_dir / "in.h264"),
                                 "-vf", f"scale={w}:{h}:flags={scaler},format=gray", "-f", "rawvideo", "-"])
            got = [media_marker.classify(fr, w, h) for fr in _frames(raw, w, h)] if rc == 0 and not err else []
            decoded[f"{scale}/{scaler}"] = sum(1 for i, r in enumerate(got) if r == ("ok", i))
            if scaler == "bilinear":
                fr = _frames(raw, w, h)
                cell_w = max(1, round(RECIPE["marker"]["cell"] * w / RECIPE["source"]["width"]))
                row_h = max(1, round(60 * h / RECIPE["source"]["height"]))
                shifts[f"{scale}/x+1cell"] = sorted({media_marker.classify(_shift_right(f, w, h, cell_w), w, h).outcome for f in fr})
                shifts[f"{scale}/y+60px"] = sorted({media_marker.classify(_shift_down(f, w, h, row_h), w, h).outcome for f in fr})
    receipt["decoded_exact"] = decoded
    checks["marker_decoded_exact"] = all(v == frames for v in decoded.values()) and len(decoded) == \
        len(RECIPE["verify_scales"]) * len(RECIPE["verify_scalers"])
    receipt["shift_outcomes"] = shifts
    checks["shifted_markers_refused"] = all("ok" not in v for v in shifts.values())

    w, h = 64, 36
    rc, raw, err = _run([ffmpeg, "-hide_banner", "-nostdin", "-loglevel", "error", *_source(),
                         "-vf", f"scale={w}:{h},format=gray", "-f", "rawvideo", "-"])
    rng = random.Random(20260924)
    controls = {"unmarked_testsrc": _frames(raw, w, h) if rc == 0 and not err else [],
                "black": [bytes([16]) * (w * h), bytes(w * h)], "white": [bytes([235]) * (w * h)],
                "grey": [bytes([128]) * (w * h)],
                "noise": [bytes(rng.getrandbits(8) for _ in range(w * h)) for _ in range(200)]}
    receipt["negative_controls"] = {k: {"n": len(v), "accepted": sum(1 for f in v if media_marker.classify(f, w, h).outcome == "ok")}
                                    for k, v in controls.items()}
    checks["negative_controls_rejected"] = all(v["n"] > 0 and v["accepted"] == 0 for v in receipt["negative_controls"].values())
    receipt["ok"] = all(checks.values())
    (run_dir / "input-receipt.json").write_text(json.dumps(receipt, indent=1))
    return receipt


def main(argv):
    if len(argv) != 2:
        print("usage: make_input.py <run dir>", file=sys.stderr)
        return 2
    r = generate(Path(argv[1]))
    print(json.dumps({"ok": r["ok"], "checks": r["checks"], "codec": r.get("codec"), "sha256": r.get("sha256"),
                      "error": r.get("error")}, indent=1))
    return 0 if r["ok"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
