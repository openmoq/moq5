"""Generates and validates the finite AAC-LC input for the A/V diagnostic beside the video input.

    python -I make_audio_input.py <run dir> <video frames> <video pts step us>

Writes into <run dir> (never into the source tree, never over an existing fixture file): in.aac (ADTS),
audio-objects.bin (the runtime publisher's record format: u32 len, u8 is_sync, u64 pts_us, then the RAW
AAC access unit without its ADTS header), audio-expected.tsv and audio-receipt.json. The first three are
shipped only when every check passes; audio-receipt.json is always written, and records the failure.

This is a controlled fixture, not a general AAC library: the reader accepts only what this recipe
produces (single-block AAC-LC ADTS frames) and names anything else. The audio shares the video input's
media time origin (both start at 0 us) and is finite and bounded: exactly the frames that cover the
video's span, and the span itself is at most MAX_SPAN_US.

The encoder is asked for the declared frame count and may emit more; in.aac is exactly the first
declared frames of what it emitted. Ledger times are ADTS frame times from the shared origin. The
receipt records the decoded low-amplitude prefix (channel-0 samples before the first whose magnitude
exceeds LOW_AMPLITUDE): a threshold-defined measurement of this input, not an encoder-delay model, and
nothing compensates any time for it.

ffmpeg and ffprobe run at warning level; any stderr output from generation, probe or decode fails the
fixture, as does a failed version command, except the one planned duration-estimate line raw ADTS
causes in the probe and the decode (PLANNED_DURATION_WARNING), matched exactly and recorded as
`planned_warnings`. Encoder output may differ between ffmpeg builds; the hash is
evidence, not a pin.
"""
import hashlib
import json
import math
import os
import re
import shutil
import struct
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import media_oracle          # noqa: E402

CMD_TIMEOUT_S = 60
SAMPLES_PER_FRAME = 1024
MAX_SPAN_US = 10_000_000                     # a fixture covers at most ten seconds of video
MAX_VIDEO_FRAMES = 600
LOW_AMPLITUDE = 64
LC_CODEC = "mp4a.40.2"
RECIPE = {"tone_hz": 1000, "sample_rate": 48_000, "channels": 2, "bitrate": "128k"}
OUTPUTS = ("in.aac", "audio-objects.bin", "audio-expected.tsv")
# The one planned diagnostic: a raw ADTS stream carries no duration, so the probe and the decode each
# estimate one at open (the decode's line names its input). The fixture never uses a duration; any other
# stderr line from any tool, and any stderr at all from generation, is refused.
PLANNED_DURATION_WARNING = re.compile(r"\[(?:in#0/)?aac @ 0x[0-9a-f]+\] Estimating duration from bitrate, this may be inaccurate\n")
RECEIPT = "audio-receipt.json"
# ISO/IEC 14496-3 Table 1.18: sampling_frequency_index 0..12.
RATES = (96_000, 88_200, 64_000, 48_000, 44_100, 32_000, 24_000, 22_050, 16_000, 12_000, 11_025, 8_000, 7_350)


@dataclass(frozen=True)
class AdtsFrame:
    offset: int
    length: int
    header_length: int
    object_type: int
    sample_rate: int
    channels: int
    access_unit: bytes


def parse_adts_frame(data, offset):
    """The ADTS frame at `offset` (ISO/IEC 13818-7 / 14496-3 1.A.2.2), or a ValueError naming the field."""
    if len(data) - offset < 7:
        raise ValueError(f"ADTS header at byte {offset} is truncated")
    bits = int.from_bytes(data[offset:offset + 7], "big")
    field = lambda shift, width: (bits >> shift) & ((1 << width) - 1)   # noqa: E731
    if field(44, 12) != 0xFFF:
        raise ValueError(f"ADTS syncword missing at byte {offset}")
    if field(41, 2) != 0:
        raise ValueError(f"ADTS layer is not 00 at byte {offset}")
    header_length = 7 if field(40, 1) else 9
    freq_index = field(34, 4)
    if freq_index >= len(RATES):
        raise ValueError(f"ADTS sampling frequency index {freq_index} is reserved at byte {offset}")
    channels = field(30, 3)
    if channels == 0:
        raise ValueError(f"ADTS channel configuration 0 (in-band PCE) is not supported at byte {offset}")
    if field(0, 2) != 0:
        raise ValueError(f"ADTS frame at byte {offset} carries more than one raw data blocks")
    length = field(13, 13)
    if length < header_length:
        raise ValueError(f"ADTS frame length {length} is shorter than its {header_length}-byte header at byte {offset}")
    if offset + length > len(data):
        raise ValueError(f"ADTS frame at byte {offset} runs past the end of the stream")
    return AdtsFrame(offset, length, header_length, field(38, 2) + 1, RATES[freq_index], channels,
                     bytes(data[offset + header_length:offset + length]))


def split_adts(data):
    """Every frame of an ADTS stream, back to back from byte 0 to the last byte."""
    frames, offset = [], 0
    while offset < len(data):
        frame = parse_adts_frame(data, offset)
        frames.append(frame)
        offset += frame.length
    return frames


def lc_audio_specific_config(sample_rate, channels):
    """The fixture's two-byte AAC-LC AudioSpecificConfig: object type 2, frequency index, channels, 3 zero bits."""
    if sample_rate not in RATES:
        raise ValueError(f"sample rate {sample_rate} has no frequency index")
    if not 1 <= channels <= 7:
        raise ValueError(f"channel configuration {channels} is outside 1..7")
    return ((2 << 11) | (RATES.index(sample_rate) << 7) | (channels << 3)).to_bytes(2, "big")


def audio_pts_us(index, sample_rate):
    """Frame `index`'s presentation time from the shared origin, exact to the microsecond (floor)."""
    return index * SAMPLES_PER_FRAME * 1_000_000 // sample_rate


def frames_covering(end_us, sample_rate):
    """The fewest frames whose end reaches `end_us`, 0 < end_us <= MAX_SPAN_US, in integer arithmetic."""
    if isinstance(end_us, bool) or not isinstance(end_us, int) or not 0 < end_us <= MAX_SPAN_US:
        raise ValueError(f"the covered span must be an integer in (0, {MAX_SPAN_US}] us, not {end_us!r}")
    return -(-end_us * sample_rate // (SAMPLES_PER_FRAME * 1_000_000))


def _checked_span(video_frames, video_step_us):
    for name, value, upper in (("video frames", video_frames, MAX_VIDEO_FRAMES), ("video pts step", video_step_us, MAX_SPAN_US)):
        if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= upper:
            raise ValueError(f"{name} must be an integer in (0, {upper}], not {value!r}")
    return video_frames * video_step_us


def _run(argv):
    r = subprocess.run(argv, capture_output=True, timeout=CMD_TIMEOUT_S)
    return r.returncode, r.stdout, r.stderr.decode(errors="replace")


def _pcm_channel0(pcm, channels):
    return struct.unpack(f"<{len(pcm) // 2}h", pcm[:len(pcm) // 2 * 2])[::channels]


def _tone_hz(pcm, rate, channels):
    """The dominant frequency of the first channel's middle half, by rising zero crossings, and its RMS."""
    samples = _pcm_channel0(pcm, channels)
    mid = samples[len(samples) // 4:3 * len(samples) // 4]
    if not mid:
        return 0.0, 0.0
    rising = sum(1 for a, b in zip(mid, mid[1:]) if a < 0 <= b)
    return rising * rate / len(mid), math.sqrt(sum(s * s for s in mid) / len(mid))


def _low_amplitude_prefix(pcm, channels):
    """Channel-0 samples before the first whose magnitude exceeds LOW_AMPLITUDE."""
    samples = _pcm_channel0(pcm, channels)
    return next((i for i, s in enumerate(samples) if abs(s) > LOW_AMPLITUDE), len(samples))


def _write_exclusive(path, data):
    with open(path, "xb") as f:
        f.write(data)


def generate(run_dir, video_frames, video_step_us, ffmpeg=None, ffprobe=None):
    """The run's audio from `ffmpeg`/`ffprobe` as given (absolute paths), or as found on PATH when
    neither is given. Invalid counts or steps raise ValueError before anything is created or run."""
    video_end_us = _checked_span(video_frames, video_step_us)
    rate, channels = RECIPE["sample_rate"], RECIPE["channels"]
    frames = frames_covering(video_end_us, rate)
    run_dir = Path(run_dir)
    checks = {}
    receipt = {"recipe": RECIPE, "ffmpeg": None, "ffprobe": None, "checks": checks, "commands": [],
               "video_end_us": video_end_us, "frames": frames}

    def finish(error=None):
        receipt["ok"] = error is None and bool(checks) and all(checks.values())
        if error is not None:
            receipt["error"] = error
        elif not receipt["ok"]:
            receipt["error"] = "checks failed: " + ", ".join(k for k, v in checks.items() if not v)
        try:
            _write_exclusive(run_dir / RECEIPT, json.dumps(receipt, indent=1).encode())
        except OSError as e:
            receipt.update(ok=False, error=f"{receipt.get('error') or 'receipt'}; receipt not written: {e!r}")
        return receipt

    run_dir.mkdir(parents=True, exist_ok=True)
    existing = [n for n in (*OUTPUTS, RECEIPT) if (run_dir / n).exists()]
    if existing:
        receipt.update(ok=False, error=f"{', '.join(existing)} already exist in {run_dir}; a fixture is never written over")
        return receipt
    if ffmpeg is None and ffprobe is None:
        ffmpeg, ffprobe = shutil.which("ffmpeg"), shutil.which("ffprobe")
    ffmpeg, ffprobe = (str(t) if t is not None else None for t in (ffmpeg, ffprobe))
    receipt.update(ffmpeg=ffmpeg, ffprobe=ffprobe)
    if not ffmpeg or not ffprobe:
        return finish("ffmpeg/ffprobe: not both given, nor both on PATH")
    try:
        rc, out, err = _run([ffmpeg, "-hide_banner", "-version"])
    except OSError as e:
        return finish(f"ffmpeg -version could not run: {e!r}")
    receipt["version"] = {"rc": rc, "stderr": err, "line": out.decode(errors="replace").splitlines()[0] if out else None}
    if rc != 0 or err:
        return finish(f"ffmpeg -version: rc {rc} stderr {err.strip()[:300]!r}")

    def tool(label, argv, planned=None):
        rc, out, err = _run(argv)
        receipt["commands"].append({"label": label, "argv": argv, "rc": rc, "stderr": err})
        unplanned = err if planned is None or not planned.fullmatch(err) else ""
        if planned is not None and err and not unplanned:
            receipt.setdefault("planned_warnings", []).append({"label": label, "stderr": err})
        return (out, None) if rc == 0 and not unplanned else (out, f"{label}: rc {rc} stderr {err.strip()[:300]!r}")

    staged = {}
    outputs = []
    for label in ("a", "b"):
        target = run_dir / f".in-{label}.aac"
        _out, failure = tool(f"generation {label}", [
            ffmpeg, "-hide_banner", "-nostdin", "-loglevel", "warning", "-f", "lavfi",
            "-i", f"sine=frequency={RECIPE['tone_hz']}:sample_rate={rate}", "-ac", str(channels),
            "-c:a", "aac", "-b:a", RECIPE["bitrate"], "-frames:a", str(frames),
            "-flags", "+bitexact", "-fflags", "+bitexact", "-f", "adts", "-y", str(target)])
        data = target.read_bytes() if target.exists() else b""
        target.unlink(missing_ok=True)
        if failure:
            return finish(failure)
        outputs.append(data)
    checks["two_runs_identical"] = outputs[0] == outputs[1]
    try:
        encoded = split_adts(outputs[0])
    except ValueError as e:
        return finish(f"ADTS: {e}")
    receipt["encoded_frames"] = len(encoded)
    if len(encoded) < frames:
        return finish(f"the encoder produced {len(encoded)} of {frames} declared frames")
    parsed = encoded[:frames]
    media = outputs[0][:sum(f.length for f in parsed)]
    receipt["sha256"], receipt["bytes"] = hashlib.sha256(media).hexdigest(), len(media)
    checks["frame_count_declared"] = len(parsed) == frames
    checks["adts_concatenation"] = b"".join(media[f.offset:f.offset + f.length] for f in parsed) == media
    first = parsed[0]
    checks["uniform_lc_fields"] = all((f.object_type, f.sample_rate, f.channels) == (2, rate, channels) for f in parsed)
    receipt.update(codec=LC_CODEC, sample_rate=first.sample_rate, channels=first.channels,
                   audio_specific_config=lc_audio_specific_config(first.sample_rate, first.channels).hex())

    stage = run_dir / ".in.aac"
    stage.write_bytes(media)
    staged["in.aac"] = stage
    try:
        probe, failure = tool("probe", [ffprobe, "-v", "warning", "-f", "aac", "-count_packets", "-show_entries",
                                        "stream=codec_name,profile,sample_rate,channels,nb_read_packets", "-of", "json", str(stage)],
                                PLANNED_DURATION_WARNING)
        if failure:
            return finish(failure)
        stream = (json.loads(probe or b"{}").get("streams") or [{}])[0]
        receipt["ffprobe_stream"] = stream
        checks["ffprobe_stream_matches"] = (stream.get("codec_name"), stream.get("profile"), stream.get("sample_rate"),
                                            stream.get("channels")) == ("aac", "LC", str(rate), channels)
        checks["ffprobe_packets_match"] = stream.get("nb_read_packets") == str(frames)

        ledger = b"".join(struct.pack("<IBQ", len(f.access_unit), 1, audio_pts_us(i, rate)) + f.access_unit
                          for i, f in enumerate(parsed))
        staged["audio-objects.bin"] = run_dir / ".audio-objects.bin"
        staged["audio-objects.bin"].write_bytes(ledger)
        tsv = "index\tlen\tis_sync\tpts_us\tsha256\n" + "".join(
            f"{i}\t{len(f.access_unit)}\t1\t{audio_pts_us(i, rate)}\t{hashlib.sha256(f.access_unit).hexdigest()}\n"
            for i, f in enumerate(parsed))
        staged["audio-expected.tsv"] = run_dir / ".audio-expected.tsv"
        staged["audio-expected.tsv"].write_text(tsv)
        loaded = media_oracle.load_objects(staged["audio-objects.bin"])
        checks["objects_file_matches_ledger"] = [(e.payload, e.is_sync, e.pts_us) for e in loaded] == \
            [(f.access_unit, True, audio_pts_us(i, rate)) for i, f in enumerate(parsed)]
        checks["shared_origin"] = bool(loaded) and loaded[0].pts_us == 0
        checks["covers_video_span"] = audio_pts_us(frames, rate) >= video_end_us > audio_pts_us(frames - 1, rate)

        pcm, failure = tool("decode", [ffmpeg, "-hide_banner", "-nostdin", "-loglevel", "warning", "-f", "aac",
                                       "-i", str(stage), "-f", "s16le", "-acodec", "pcm_s16le", "-"], PLANNED_DURATION_WARNING)
        if failure:
            return finish(failure)
        decoded = len(pcm) // (2 * channels)
        tone, rms = _tone_hz(pcm, rate, channels)
        receipt["decoded"] = {"samples_per_channel": decoded, "tone_hz": round(tone, 1), "rms": round(rms, 1),
                              "low_amplitude_prefix": {"threshold": LOW_AMPLITUDE, "samples": _low_amplitude_prefix(pcm, channels)}}
        checks["decoded_length_declared"] = decoded == frames * SAMPLES_PER_FRAME
        checks["decoded_tone_present"] = abs(tone - RECIPE["tone_hz"]) <= RECIPE["tone_hz"] * 0.02 and rms > 1000
        if not all(checks.values()):
            return finish()
        for name in OUTPUTS:
            os.link(staged[name], run_dir / name)          # fails rather than replacing an existing file
        return finish()
    finally:
        for path in staged.values():
            path.unlink(missing_ok=True)


def main(argv):
    if len(argv) != 4:
        print("usage: make_audio_input.py <run dir> <video frames> <video pts step us>", file=sys.stderr)
        return 2
    r = generate(Path(argv[1]), int(argv[2]), int(argv[3]))
    print(json.dumps({k: r.get(k) for k in ("ok", "checks", "codec", "sample_rate", "channels", "frames",
                                            "audio_specific_config", "sha256", "decoded", "error")}, indent=1))
    return 0 if r["ok"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
