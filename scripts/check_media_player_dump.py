#!/usr/bin/env python3
"""Check a moq_example_media_player --dump directory against the published file.

  check_media_player_dump.py --source FILE --dump DIR [--video NAME]
                             [--audio NAME] [--expect-start]

The dump holds, per track, NAME.bin (the received access units, back to
back), NAME.units ("offset len dts_us pts_us generation independent group
object" per unit, "#configure" lines per configuration) and NAME.config.N (the decoder
configuration of generation N). FILE is the exact file the publisher read.

For each named track the check requires:
  configuration  every unit names a generation that has a configuration
                 file and a "#configure" line;
  order          locations (group, then object) strictly increase, which
                 is decode order; on CMAF decode timestamps never decrease
                 and no unit presents before it decodes (RAW/LOC's decode
                 time copies its media time and may decrease with
                 reordered frames);
  media          video: the received units, decoded, give the same frames
                 (per-frame md5) as the reference from the join point to its
                 last frame; audio: the received units are the reference's
                 AAC packets byte for byte, contiguous to the last one, and
                 they decode once framed with their configuration;
  timing         presentation timestamps keep the reference's spacing
                 (within 2 us of a single offset per track);
  unit times     unit by unit in decode (object) order, each presentation
                 timestamp is the reference's at the same stored position,
                 shifted by the track's offset (within 2 us): every unit
                 keeps its own time, and reordered video frames keep both
                 their decode position and their presentation time.
Without --expect-start a late join is allowed: the comparison starts at the
reference index where the whole received run matches. Repetitive media (a
test tone's identical AAC packets) can match at several indices; the audio
start is then the one candidate whose timestamp offset matches the video
track's (unique within half the spacing between candidates), and when none
is unique the alignment is reported "ambiguous" and the media and tail
checks fail -- an index is never chosen because it makes a check pass. With
--expect-start the first received unit must be the reference's first. When
both tracks are given, the difference between their offsets is reported
(the A/V correlation) and checked: the file aligns its tracks on one
timeline, so a player that maps both onto one clock gives both the same
offset; the difference must be within 2 us (each offset carries at most
1 us of rounding from its track's timescale). That proves the timestamps and
their shared mapping, not when sound and image were actually presented.
--self-test runs the alignment and A/V rules on synthetic runs (no ffmpeg
needed).

Decoding uses ffmpeg/ffprobe from PATH. Prints one JSON object; exits 0 when
every check holds, 1 when one fails, 2 for bad usage or a missing tool.
"""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys

AAC_RATES = [96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050,
             16000, 12000, 11025, 8000, 7350]


def run(cmd, data=None):
    r = subprocess.run(cmd, input=data, capture_output=True)
    if r.returncode != 0:
        raise RuntimeError("%s: %s" % (" ".join(cmd[:3]),
                                       r.stderr.decode(errors="replace")[-400:]))
    return r.stdout


def framemd5(args, data=None):
    out = run(["ffmpeg", "-v", "error"] + args + ["-f", "framemd5", "-"], data)
    md5s = []
    for line in out.decode().splitlines():
        if line.startswith("#") or not line.strip():
            continue
        md5s.append(line.split(",")[-1].strip())
    return md5s


def read_units(dump, name):
    units, configs = [], {}
    with open(os.path.join(dump, name + ".units")) as f:
        for line in f:
            if line.startswith("#configure"):
                parts = line.split()
                configs[int(parts[1])] = dict(p.split("=", 1) for p in parts[2:])
            elif line.startswith("#") or not line.strip():
                continue
            else:
                off, ln, dts, pts, gen, ind, grp, obj = line.split()
                units.append(dict(off=int(off), len=int(ln), dts=int(dts),
                                  pts=int(pts), gen=int(gen), ind=int(ind),
                                  group=int(grp), object=int(obj)))
    with open(os.path.join(dump, name + ".bin"), "rb") as f:
        blob = f.read()
    return units, configs, blob


def ref_packets(source, sel):
    """Reference packets of the first stream of `sel` ("v" or "a"):
    pts in microseconds (presentation order as stored) and sizes."""
    out = run(["ffprobe", "-v", "error", "-select_streams", sel + ":0",
               "-show_entries", "stream=time_base:packet=pts,dts,size",
               "-of", "json", source])
    j = json.loads(out)
    num, den = (int(x) for x in j["streams"][0]["time_base"].split("/"))
    pk = [dict(pts=int(p["pts"]) * 1000000 * num // den, size=int(p["size"]))
          for p in j["packets"]]
    return pk


def check_common(units, configs, dump, name, res):
    bad_gen = [u["gen"] for u in units
               if u["gen"] not in configs or
               not os.path.exists(os.path.join(dump, "%s.config.%d" % (name, u["gen"])))]
    res["generations"] = sorted(configs)
    res["configuration_ok"] = not bad_gen and bool(units)
    loc_ok = all((units[i]["group"], units[i]["object"]) >
                 (units[i - 1]["group"], units[i - 1]["object"]) for i in range(1, len(units)))
    cmaf = any(c.get("packaging") == "cmaf" for c in configs.values())
    res["packaging"] = "cmaf" if cmaf else "raw"
    dts_ok = not cmaf or all(units[i]["dts"] >= units[i - 1]["dts"]
                             for i in range(1, len(units)))
    pts_ok = not cmaf or all(u["pts"] >= u["dts"] for u in units)
    groups = sorted({u["group"] for u in units})
    res["groups"] = [groups[0], groups[-1]] if groups else []
    res["missing_groups"] = [g for g in range(groups[0], groups[-1] + 1)
                             if g not in set(groups)] if groups else []
    res["reordered"] = any(units[i]["pts"] < units[i - 1]["pts"] for i in range(1, len(units)))
    res["location_order_ok"] = loc_ok
    res["order_ok"] = loc_ok and dts_ok and pts_ok
    return res["configuration_ok"] and res["order_ok"]


def timing(units_pts, ref_pts, start, res):
    rp = sorted(ref_pts)[start:start + len(units_pts)]
    up = sorted(units_pts)
    if len(rp) != len(up) or not up:
        res["timing_ok"] = False
        return None
    offs = [u - r for u, r in zip(up, rp)]
    res["pts_offset_us"] = offs[0]
    res["pts_offset_spread_us"] = max(offs) - min(offs)
    res["timing_ok"] = res["pts_offset_spread_us"] <= 2
    return offs[0]


def find_starts(received, reference):
    """Every reference index where the whole received run matches; else (a
    run with a gap or a wrong tail) the first index where its leading entries
    do, alone; [] when none."""
    if not received:
        return []
    n = len(received)
    full = [k for k in range(len(reference) - n + 1)
            if reference[k] == received[0] and reference[k:k + n] == received]
    if full:
        return full
    probe = min(n, 8)
    for k in range(len(reference) - probe + 1):
        if reference[k:k + probe] == received[:probe]:
            return [k]
    return []


def choose_start(cands, offset_of, anchor_offset, res):
    """The start among `cands`: the only one, or -- when content alone cannot
    decide -- the unique candidate whose timestamp offset (offset_of(k)) is
    within half the spacing between candidate offsets of `anchor_offset`
    (the other track's). -1 and res["alignment"] = "ambiguous" otherwise."""
    if not cands:
        res["alignment"] = "none"
        return -1
    if len(cands) == 1:
        res["alignment"] = "content"
        return cands[0]
    res["alignment_candidates"] = len(cands)
    if anchor_offset is not None:
        offs = sorted((offset_of(k), k) for k in cands)
        gap = min(b[0] - a[0] for a, b in zip(offs, offs[1:]))
        near = [k for o, k in offs if 2 * abs(o - anchor_offset) < gap]
        if len(near) == 1:
            res["alignment"] = "timing"
            return near[0]
    res["alignment"] = "ambiguous"
    return -1


# Each track's offset is a difference of microsecond timestamps each
# rounded once from its own timescale: two offsets that map one shared
# timeline can differ by up to 2 us.
AV_TOLERANCE_US = 2


def av_sync(video_offset, audio_offset, res):
    """The A/V check on two track offsets; records and returns whether
    they map the file's one timeline to the same clock."""
    diff = audio_offset - video_offset
    res["av_offset_difference_us"] = diff
    res["av_sync_ok"] = abs(diff) <= AV_TOLERANCE_US
    return res["av_sync_ok"]


def decode_order(units_pts, ref_pts_stored, start, offset, res):
    """Units in decode order against the reference's stored (decode)
    order from `start`: each unit's own presentation timestamp matches the
    reference's at its position, shifted by `offset`, within 2 us."""
    ref = ref_pts_stored[start:start + len(units_pts)]
    res["decode_order_ok"] = (len(ref) == len(units_pts) and bool(units_pts) and
                              all(abs(u - offset - r) <= 2
                                  for u, r in zip(units_pts, ref)))
    return res["decode_order_ok"]


def first_offset(units_pts, ref_pts, k):
    """The presentation offset of the first received unit against reference
    unit k (both in presentation order)."""
    return min(units_pts) - sorted(ref_pts)[k]


def check_video(source, dump, name, expect_start):
    res = dict(track=name, kind="video")
    units, configs, blob = read_units(dump, name)
    ok = check_common(units, configs, dump, name, res)
    res["received_units"] = len(units)
    got = framemd5(["-f", "h264", "-i", "-"], blob)
    ref = framemd5(["-i", source, "-map", "0:v:0"])
    res["decoded_frames"] = len(got)
    res["reference_frames"] = len(ref)
    upts = [u["pts"] for u in units]
    rpts = [p["pts"] for p in ref_packets(source, "v")]
    start = choose_start(find_starts(got, ref),
                         lambda k: first_offset(upts, rpts, k), None, res)
    res["join_index"] = start
    media_ok = (start >= 0 and len(got) == len(units) and
                got == ref[start:start + len(got)] and start + len(got) == len(ref))
    if expect_start and start != 0:
        media_ok = False
    res["tail_ok"] = bool(got) and start >= 0 and start + len(got) == len(ref) and \
        got[-1] == ref[-1]
    res["media_ok"] = media_ok
    off = timing(upts, rpts, start, res) if start >= 0 else None
    if off is None:
        res["timing_ok"] = False
        res["decode_order_ok"] = False
    else:
        decode_order(upts, rpts, start, off, res)
    return ok and media_ok and res["timing_ok"] and res["decode_order_ok"], res, off


def adts(config, frame):
    obj = (config[0] >> 3) - 1
    freq = ((config[0] & 7) << 1) | (config[1] >> 7)
    chan = (config[1] >> 3) & 0xF
    n = len(frame) + 7
    return bytes([0xFF, 0xF1, (obj << 6) | (freq << 2) | (chan >> 2),
                  ((chan & 3) << 6) | (n >> 11), (n >> 3) & 0xFF,
                  ((n & 7) << 5) | 0x1F, 0xFC]) + frame


def check_audio(source, dump, name, expect_start, video_offset=None):
    res = dict(track=name, kind="audio")
    units, configs, blob = read_units(dump, name)
    ok = check_common(units, configs, dump, name, res)
    res["received_units"] = len(units)
    got = [blob[u["off"]:u["off"] + u["len"]] for u in units]
    raw = run(["ffmpeg", "-v", "error", "-i", source, "-map", "0:a:0", "-c", "copy",
               "-f", "data", "-"])
    pk = ref_packets(source, "a")
    ref, pos = [], 0
    for p in pk:
        ref.append(raw[pos:pos + p["size"]])
        pos += p["size"]
    res["reference_units"] = len(ref)
    gh = [hashlib.md5(x).hexdigest() for x in got]
    rh = [hashlib.md5(x).hexdigest() for x in ref]
    upts = [u["pts"] for u in units]
    rpts = [p["pts"] for p in pk]
    start = choose_start(find_starts(gh, rh),
                         lambda k: first_offset(upts, rpts, k), video_offset, res)
    res["join_index"] = start
    media_ok = start >= 0 and gh == rh[start:start + len(gh)] and start + len(gh) == len(rh)
    if expect_start and start != 0:
        media_ok = False
    res["tail_ok"] = bool(gh) and start >= 0 and start + len(gh) == len(rh) and gh[-1] == rh[-1]
    decoded = []
    gens = sorted({u["gen"] for u in units})
    if len(gens) == 1:
        with open(os.path.join(dump, "%s.config.%d" % (name, gens[0])), "rb") as f:
            cfg = f.read()
        if len(cfg) >= 2 and cfg[0] >> 3 == 2:   # AAC-LC AudioSpecificConfig
            res["samplerate"] = AAC_RATES[((cfg[0] & 7) << 1) | (cfg[1] >> 7)]
            res["channels"] = (cfg[1] >> 3) & 0xF
            decoded = framemd5(["-f", "aac", "-i", "-"], b"".join(adts(cfg, x) for x in got))
        else:
            res["note"] = "configuration is not AAC-LC; not decoded"
    else:
        res["note"] = "several configurations; not decoded"
    res["decoded_frames"] = len(decoded)
    decode_ok = len(decoded) == len(got)
    res["media_ok"] = media_ok and decode_ok
    off = timing(upts, rpts, start, res) if start >= 0 else None
    if off is None:
        res["timing_ok"] = False
        res["decode_order_ok"] = False
    else:
        decode_order(upts, rpts, start, off, res)
    return (ok and media_ok and decode_ok and res["timing_ok"] and res["decode_order_ok"],
            res, off)


def self_test():
    """The alignment rules on synthetic runs; returns a failure count."""
    fails = 0

    def expect(what, got, want):
        nonlocal fails
        if got != want:
            fails += 1
            print("FAIL %s: got %r, want %r" % (what, got, want), file=sys.stderr)

    # Distinct content: one candidate, decided by content.
    ref = list("abcdefgh")
    res = {}
    expect("distinct", choose_start(find_starts(list("defgh"), ref),
                                    lambda k: 0, None, res), 3)
    expect("distinct alignment", res["alignment"], "content")
    # A lost tail of distinct content: the start is still the true join, and
    # the run does not reach the reference end.
    got = list("cde")
    k = choose_start(find_starts(got, ref), lambda k: 0, None, {})
    expect("lost tail start", k, 2)
    expect("lost tail reaches end", k + len(got) == len(ref), False)
    # Repetitive content (a tone): the whole run matches at every index.
    tone = ["t"] * 10
    run = ["t"] * 4
    cands = find_starts(run, tone)
    expect("tone candidates", cands, list(range(7)))
    upts = [k * 21334 + 500000 for k in range(4)]       # received at +500 ms
    rpts = [k * 21334 for k in range(10)]
    off = lambda k: first_offset(upts, rpts, k)
    # No other track to correlate with: ambiguous, never an index.
    res = {}
    expect("tone without anchor", choose_start(cands, off, None, res), -1)
    expect("tone without anchor alignment", res["alignment"], "ambiguous")
    # The video's offset picks the one consistent candidate: here a complete
    # late join at index 6 (+500 ms - 6 frames = video offset 371996 us).
    res = {}
    expect("tone with anchor", choose_start(cands, off, 500000 - 6 * 21334, res), 6)
    expect("tone with anchor alignment", res["alignment"], "timing")
    # An anchor exactly between two candidates is not unique: ambiguous.
    res = {}
    expect("tone between", choose_start(cands, off, 500000 - 6 * 21334 + 10667, res), -1)
    expect("tone between alignment", res["alignment"], "ambiguous")
    # A tone dump that lost its tail does not pass by choosing the last
    # index: the anchor names the true join, short of the reference end.
    k = choose_start(cands, off, 500000 - 2 * 21334, {})
    expect("tone lost tail start", k, 2)
    expect("tone lost tail reaches end", k + len(run) == len(tone), False)
    # A/V: offsets that map one timeline pass; an audio track shifted by
    # one AAC frame (21333 us at 48 kHz) or by 3 us does not.
    expect("av aligned", av_sync(371996, 371997, {}), True)
    expect("av one frame late", av_sync(371996, 371996 + 21333, {}), False)
    expect("av 3 us", av_sync(371996, 371999, {}), False)
    # Decode order with B-frames: stored order I P B B presents 0, 99, 33,
    # 66 ms; units in that decode order with those times pass, the same
    # times in presentation order do not.
    stored = [0, 99000, 33000, 66000]
    expect("bframes decode order", decode_order([500000 + p for p in stored],
                                                stored, 0, 500000, {}), True)
    expect("bframes presentation order", decode_order(
        [500000 + p for p in sorted(stored)], stored, 0, 500000, {}), False)
    # Audio units in order with their timestamps permuted: the same set of
    # times (sorted spacing passes) but not each unit's own.
    res = {}
    expect("audio permuted times offset", timing([500000, 542666, 521333],
                                                 [0, 21333, 42666], 0, res), 500000)
    expect("audio permuted times sorted spacing", res["timing_ok"], True)
    expect("audio permuted times", decode_order([500000, 542666, 521333],
                                                [0, 21333, 42666], 0, 500000, {}), False)
    print("self-test: %s" % ("FAIL" if fails else "ok"))
    return fails


def main():
    if sys.argv[1:] == ["--self-test"]:
        return 1 if self_test() else 0
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--dump", required=True)
    ap.add_argument("--video")
    ap.add_argument("--audio")
    ap.add_argument("--expect-start", action="store_true")
    a = ap.parse_args()
    if not (a.video or a.audio) or not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        ap.print_usage(sys.stderr)
        return 2
    out, ok, offs = dict(source=a.source, dump=a.dump, tracks=[]), True, {}
    try:
        for name, fn in ((a.video, check_video), (a.audio, check_audio)):
            if not name:
                continue
            if fn is check_audio:
                t_ok, res, off = fn(a.source, a.dump, name, a.expect_start,
                                    offs.get("video"))
            else:
                t_ok, res, off = fn(a.source, a.dump, name, a.expect_start)
            res["ok"] = t_ok
            out["tracks"].append(res)
            ok = ok and t_ok
            if off is not None:
                offs[res["kind"]] = off
    except (OSError, RuntimeError, ValueError, KeyError) as e:
        out["error"] = str(e)
        ok = False
    if "video" in offs and "audio" in offs:
        ok = av_sync(offs["video"], offs["audio"], out) and ok
    out["ok"] = ok
    print(json.dumps(out, indent=1))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
