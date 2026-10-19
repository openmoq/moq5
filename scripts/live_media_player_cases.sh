#!/usr/bin/env bash
# Live cases for moq_example_media_player against real relays (opt-in; never
# run by CTest, since it needs a network and a relay).
#
#   MOQ_LIVE_HOST=relay.example MOQ_LIVE_MEDIA=/path/to/videos \
#   MOQ_LIVE_OUT=/tmp/live scripts/live_media_player_cases.sh [CASE...]
#
# Without CASE arguments every case in the table below runs; otherwise the
# named ones. Each case publishes a finite clip, plays it with the example,
# and records one JSON verdict line in $MOQ_LIVE_OUT/results.jsonl, with the
# case's logs and dump under $MOQ_LIVE_OUT/<case>/. Every process a case
# starts is stopped and reaped before the next case.
#
# Environment:
#   MOQ_LIVE_HOST   the relay host (TLS is verified against this name)
#   MOQ_LIVE_MEDIA  directory holding the INPUTS files below
#   MOQ_LIVE_OUT    output directory (created; an existing case directory
#                   is replaced)
#   MOQ_BUILD       libmoq build tree with the service examples
#                   (default: build/slice4-live)
#   GST_PLUGIN_PATH where gst-moq's moqsink is (for the gst publisher)
#   MOQ_LIVE_REPEAT run each selected case this many times (default 1)
#   MOQ_LIVE_CLIP_S seconds of each input to publish (default 30; the
#                   inputs are cut once per MOQ_LIVE_OUT, so a different
#                   length needs its own output directory)
#
# Relays (ports as a relay lab advertises them):
#   moqx  raw moqt://HOST:4433, WT https://HOST:4433/moq-relay
#   moq5  raw moqt://HOST:4440, WT https://HOST:4441/moq
#
# Publishers:
#   gst  gst-moq moqsink: H.264 + raw AAC from the clip, LOC, WebTransport
#        only; it declares the two one render group, so AUTO plays both and
#        "av" selects both tracks by name
#   loc  ffmpeg Annex-B | moq_example_media_publish_loc: video only, over
#        the case's consumer transport; it streams from the start, so its
#        player joins live and is checked from its entry
#
# Lifecycles and their verdicts (COMPLETE always also requires the dump
# check, scripts/check_media_player_dump.py, to pass):
#   finite     PASS when the run is COMPLETE from the first unit
#   join       the player joins 12 s in; PASS when COMPLETE from its entry
#   interrupt  SIGINT to the player at 8 s; PASS when it stops within 2 s,
#              says so, and claims no COMPLETE
#   pubkill    the publisher is killed (SIGKILL) at 10 s; PASS when, within
#              20 s, every selected track ends and none ends COMPLETE, or
#              the receiver fails for good (scripts/live_media_player_verdict.py)
#   stall      a 3 s decoder stall 5 s after the first unit; PASS when the
#              run ends before its deadline and any COMPLETE is backed by
#              the dump check
set -u

HOST=${MOQ_LIVE_HOST:?set MOQ_LIVE_HOST}
MEDIA=${MOQ_LIVE_MEDIA:?set MOQ_LIVE_MEDIA}
OUT=${MOQ_LIVE_OUT:?set MOQ_LIVE_OUT}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BUILD=${MOQ_BUILD:-$ROOT/build/slice4-live}
PLAYER=$BUILD/examples/service/moq_example_media_player
PUBLOC=$BUILD/examples/service/moq_example_media_publish_loc
CHECK=$ROOT/scripts/check_media_player_dump.py
VERDICT=$ROOT/scripts/live_media_player_verdict.py
REPEAT=${MOQ_LIVE_REPEAT:-1}
CLIP_S=${MOQ_LIVE_CLIP_S:-30}

# name  file
INPUTS="
h264-24     test-1080p-h264-24fps-300s-1M.mp4
h264-30     test-1080p-h264-30fps-300s-1M.mp4
h264-60     test-1080p-h264-60fps-300s-1M.mp4
film-24     night-of-the-living-dead-5m.mp4
bframes-30  Trailers/Lethal Weapon (1987) Official Trailer - Mel Gibson, Danny Glover Action Movie HD-bKeW-MGu-qQ.mp4
"

# name  publisher relay transport draft select lifecycle input
CASES="
av-moqx-wt-d16      gst moqx wt   16   av    finite    h264-60
av-moqx-wt-d18      gst moqx wt   18   av    finite    h264-60
av-moqx-wt-auto     gst moqx wt   auto av    finite    h264-60
av-moqx-raw-d16     gst moqx raw  16   av    finite    h264-60
av-moqx-raw-d18     gst moqx raw  18   av    finite    h264-60
auto-moqx-wt-d16    gst moqx wt   16   auto  finite    h264-60
audio-moqx-wt-d16   gst moqx wt   16   audio finite    h264-60
av-24fps            gst moqx wt   16   av    finite    h264-24
av-30fps            gst moqx wt   16   av    finite    h264-30
av-film-native-aac  gst moqx wt   16   av    finite    film-24
av-bframes          gst moqx wt   16   av    finite    bframes-30
av-original-asc     gst moqx wt   16   av    finite    h264-60-original
video-loc-moqx-raw  loc moqx raw  18   auto  finite    h264-60
video-loc-moqx-wt   loc moqx wt   18   auto  finite    h264-60
video-loc-moqx-d16  loc moqx raw  16   auto  finite    h264-60
video-loc-moqx-wt16 loc moqx wt   16   auto  finite    h264-60
video-loc-moq5-raw  loc moq5 raw  18   auto  finite    h264-60
video-loc-moq5-d16  loc moq5 raw  16   auto  finite    h264-60
av-moq5-wt          gst moq5 wt   16   av    finite    h264-60
join-av             gst moqx wt   16   av    join      h264-60
interrupt-av        gst moqx wt   16   av    interrupt h264-60
pubkill-av          gst moqx wt   16   av    pubkill   h264-60
stall-av            gst moqx wt   16   av    stall     h264-60
"

mkdir -p "$OUT/inputs"
RESULTS=$OUT/results.jsonl

# A CLIP_S stream copy of each input. A 2-byte AAC-LC AudioSpecificConfig is
# what gst-moq's LOC audio pad accepts; a longer one (an SBR sync extension
# saying no SBR) is rewritten to its 2-byte form through ADTS, which leaves
# every audio and video payload byte unchanged, and the original is kept as
# NAME-original for the case that records its refusal.
# Its ffmpeg and ffprobe runs read nothing from stdin, which is the input
# table its caller is reading.
prepare_input() {
    local name=$1 file=$2 out=$OUT/inputs/$1.mp4
    [ -f "$out" ] && return 0
    ffmpeg -nostdin -hide_banner -loglevel error -y -i "$MEDIA/$file" -t $CLIP_S -map 0:v:0 \
        -map 0:a:0 -c copy -movflags +faststart "$OUT/inputs/$name-original.mp4" || return 1
    local asc
    asc=$(ffprobe -v error -select_streams a:0 -show_entries stream=extradata_size \
        -of csv=p=0 "$OUT/inputs/$name-original.mp4" < /dev/null)
    if [ "$asc" = 2 ]; then
        cp "$OUT/inputs/$name-original.mp4" "$out"
        echo "$name: $file, native ${asc}-byte AAC config" >> "$OUT/inputs/manifest.txt"
    else
        ffmpeg -nostdin -hide_banner -loglevel error -y -i "$OUT/inputs/$name-original.mp4" \
            -map 0:a -c copy -f adts "$OUT/inputs/$name.aac" &&
        ffmpeg -nostdin -hide_banner -loglevel error -y -i "$OUT/inputs/$name-original.mp4" \
            -i "$OUT/inputs/$name.aac" -map 0:v -map 1:a -c copy -movflags +faststart "$out" ||
            return 1
        rm -f "$OUT/inputs/$name.aac"
        echo "$name: $file, ${asc}-byte AAC config normalized to 2 bytes (payloads unchanged)" \
            >> "$OUT/inputs/manifest.txt"
    fi
}

# SIGKILL the publisher <pid> for a pubkill case, recording in <dir> what
# happened: fault_realtime_us (the wall time just after the signal) only
# when a live publisher was signalled; fault_not_sent (why) otherwise -- it
# had already exited (gone, or a zombie not yet reaped) or kill failed.
pubkill() {
    local pid=$1 dir=$2 state err
    state=$(ps -o stat= -p "$pid" 2>/dev/null | tr -d ' ')
    case "$state" in
        "")  echo "the publisher had already exited" > "$dir/fault_not_sent"; return ;;
        Z*)  echo "the publisher had already exited (zombie)" > "$dir/fault_not_sent"; return ;;
    esac
    if err=$(kill -KILL "$pid" 2>&1); then
        python3 -I -c 'import time; print(int(time.time() * 1e6))' > "$dir/fault_realtime_us"
    else
        echo "kill -KILL failed: $err" > "$dir/fault_not_sent"
    fi
}

relay_urls() {   # relay transport -> "pub_port pub_path consumer_url"
    case "$1 $2" in
        "moqx raw") echo "4433 /moq-relay moqt://$HOST:4433" ;;
        "moqx wt")  echo "4433 /moq-relay https://$HOST:4433/moq-relay" ;;
        "moq5 raw") echo "4441 /moq moqt://$HOST:4440" ;;
        "moq5 wt")  echo "4441 /moq https://$HOST:4441/moq" ;;
    esac
}

stop() {   # pid: stop and reap, escalating
    local pid=$1
    kill -0 "$pid" 2>/dev/null || { wait "$pid" 2>/dev/null; return; }
    kill -TERM "$pid" 2>/dev/null
    for _ in $(seq 1 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    kill -KILL "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
}

run_case() {
    local name=$1 pub=$2 relay=$3 tr=$4 draft=$5 sel=$6 life=$7 input=$8 rep=$9
    local tag=$name; [ "$REPEAT" -gt 1 ] && tag=$name.$rep
    local O=$OUT/$tag clip=$OUT/inputs/$input.mp4
    rm -rf "$O"; mkdir -p "$O/dump"
    read -r pport ppath curl <<<"$(relay_urls "$relay" "$tr")"
    local ns=moq-live-$tag-$$-$(date +%s)
    local dargs=() pdraft=0 sargs=() ntracks=2 vname=video aname=audio
    [ "$draft" != auto ] && dargs=(--draft "$draft") && pdraft=$draft
    case $sel in
        av)    sargs=(--track video --track audio) ;;
        audio) sargs=(--track audio); ntracks=1; vname= ;;
        video) sargs=(--track video); ntracks=1; aname= ;;
        auto)  ntracks=1; aname= ;;
    esac
    # gst-moq declares its A/V pads one render group, so AUTO plays both.
    [ "$pub" = gst ] && [ "$sel" = auto ] && ntracks=2 && aname=audio
    [ "$pub" = loc ] && aname=

    # Publisher.
    local P
    if [ "$pub" = gst ]; then
        GST_DEBUG=moqsink:4 gst-launch-1.0 -e moqsink name=ms host="$HOST" port="$pport" \
            relay-path="$ppath" namespace="$ns" draft="$pdraft" \
            filesrc location="$clip" ! qtdemux name=d \
            d.video_0 ! queue ! h264parse config-interval=-1 \
            ! video/x-h264,stream-format=byte-stream,alignment=au ! ms.sink \
            d.audio_0 ! queue ! aacparse ! audio/mpeg,mpegversion=4,stream-format=raw ! ms.audio \
            < /dev/null > "$O/publisher.log" 2>&1 &
        P=$!
    else
        local purl=$curl
        ( ffmpeg -hide_banner -loglevel error -re -i "$clip" -map 0:v -c copy \
            -bsf:v h264_mp4toannexb,h264_metadata=aud=insert -f h264 pipe:1 |
          "$PUBLOC" "$purl" "$ns" video ) < /dev/null > "$O/publisher.log" 2>&1 &
        P=$!
    fi

    # Player.
    local delay=2 extra=(--finite --expect-tracks "$ntracks") secs=$((CLIP_S + 30))
    [ "$life" = join ] && delay=12
    [ "$life" = stall ] && extra+=(--decoder-stall 5,3000)
    sleep "$delay"
    local t0 t1 C
    t0=$(date +%s)
    "$PLAYER" "$curl" "$ns" "${dargs[@]}" --seconds "$secs" --dump "$O/dump" "${sargs[@]}" \
        "${extra[@]}" < /dev/null > "$O/player.log" 2>&1 &
    C=$!
    local fault_at=0
    case $life in
        interrupt) sleep 8; fault_at=$(date +%s); kill -INT "$C" ;;
        pubkill)   sleep 10; fault_at=$(date +%s); pubkill "$P" "$O" ;;
    esac
    wait "$C"; local prc=$?
    t1=$(date +%s)
    stop "$P"

    # Verdict.
    local check='{}' crc=0
    local oargs=(--source "$clip" --dump "$O/dump")
    [ -n "$vname" ] && [ -f "$O/dump/$vname.units" ] && oargs+=(--video "$vname")
    [ -n "$aname" ] && [ -f "$O/dump/$aname.units" ] && oargs+=(--audio "$aname")
    # gst-moq starts sending once the player subscribes, so its finite runs
    # are checked from the first unit; the loc publisher streams from the
    # start, so its player always joins live and is checked from its entry.
    [ "$life" = finite ] && [ "$pub" = gst ] && oargs+=(--expect-start)
    if [ ${#oargs[@]} -gt 4 ]; then
        check=$(python3 -I "$CHECK" "${oargs[@]}" < /dev/null 2>"$O/check.err"); crc=$?
        echo "$check" > "$O/check.json"
    fi
    # -B: the block imports the verdict helper by path; keep its bytecode
    # out of the source tree.
    python3 -I -B - "$name" "$tag" "$pub" "$relay" "$tr" "$draft" "$sel" "$life" "$input" \
        "$prc" "$crc" "$((t1 - t0))" "$fault_at" "$t1" "$O" "$ns" "$ntracks" "$VERDICT" \
        >> "$RESULTS" <<'PY'
import importlib.util, json, re, sys
(name, tag, pub, relay, tr, draft, sel, life, inp, prc, crc, dur, fault_at, t1, O, ns,
 ntracks, verdict_py) = sys.argv[1:]
spec = importlib.util.spec_from_file_location("live_media_player_verdict", verdict_py)
verdicts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verdicts)
log = open(O + "/player.log", errors="replace").read()
pl = open(O + "/publisher.log", errors="replace").read()
m = re.search(r"READY draft=(\d+)", log)
neg = int(m.group(1)) if m else None
ends = re.findall(r"^END (\S+) cause=(\d+) disposition=(\d+)", log, re.M)
complete = "finite: COMPLETE" in log
try:
    chk = json.load(open(O + "/check.json"))
except (OSError, ValueError):
    chk = {}
units = {t: int(u) for t, u in re.findall(r"^track (\S+): .*?units=(\d+)", log, re.M)}
events = dict(re.findall(r"(\w+)=(\d+)", (re.search(r"^events: (.*)$", log, re.M) or [""])[0]))
stats = dict(re.findall(r"(\w+)=(\d+)", (re.search(r"^player: (.*)$", log, re.M) or [""])[0]))
fatal = re.search(r"receiver failed: (\d+)", log)
pubfail = re.search(r"ERROR: from element [^\n]*?: ([^\n]*)", pl)
sender = re.search(r"sender stats: ([^\n]*)", pl) or re.search(r"done: ([^\n]*)", pl)
v = "FAIL"
why = []
timing = {}
draft_ok = draft == "auto" or neg == int(draft)
if not draft_ok:
    why.append("negotiated draft %s" % neg)
if complete and not chk.get("ok"):
    why.append("COMPLETE without a passing dump check")
if life in ("finite", "join"):
    if complete and chk.get("ok") and draft_ok:
        v = "PASS"
elif life == "interrupt":
    lag = int(t1) - int(fault_at)
    if "interrupted" in log and not complete and lag <= 2:
        v = "PASS"
    why.append("stopped %ds after SIGINT" % lag)
elif life == "pubkill":
    lag = int(t1) - int(fault_at)
    # On the player's monotonic clock (its CLOCK line places the wall-clock
    # kill time, approximately): READY, the receiver's sampled terminal
    # state and END against the kill. The kill time is taken just after the
    # signal is sent.
    clk = re.search(r"^CLOCK t_ms=([\d.]+) realtime_us=(\d+)", log, re.M)
    try:
        kill_us = int(open(O + "/fault_realtime_us").read())
    except (OSError, ValueError):
        kill_us = None
    kill_ms = (float(clk.group(1)) + (kill_us - int(clk.group(2))) / 1000.0
               if clk and kill_us is not None else None)
    ready_t = re.search(r"^READY[^\n]*? t_ms=([\d.]+)", log, re.M)
    fatal_t = re.search(r"receiver failed: \d+ t_ms=([\d.]+)", log)
    ready_before = bool(ready_t) and kill_ms is not None and float(ready_t.group(1)) < kill_ms
    fatal_after = (None if not fatal_t or kill_ms is None
                   else float(fatal_t.group(1)) > kill_ms)
    try:
        not_sent = open(O + "/fault_not_sent").read().strip()
    except OSError:
        not_sent = None
    v, notes = verdicts.pubkill_verdict(ends, int(ntracks), complete, bool(fatal), lag,
                                        ready=ready_before,
                                        killed=kill_us is not None and not not_sent,
                                        fatal_after_kill=fatal_after)
    why += notes
    if not_sent:
        why.append("kill not sent: " + not_sent)
    why.append("ended %ds after SIGKILL" % lag)
    notices = [float(x) for x in re.findall(r"^NOTICE \S+ state=\S+ t_ms=([\d.]+)", log, re.M)]
    end_ts = [float(x) for x in re.findall(r"^END \S+ cause=\d+ disposition=\d+ t_ms=([\d.]+)", log, re.M)]
    if kill_ms is not None:
        if notices:
            timing["notice_ms_after_kill"] = round(max(notices) - kill_ms, 1)
        if end_ts:
            timing["end_ms_after_kill"] = round(max(end_ts) - kill_ms, 1)
    if notices and end_ts:
        timing["end_ms_after_notice"] = round(max(end_ts) - max(notices), 1)
elif life == "stall":
    if "deadline reached" not in log and (not complete or chk.get("ok")):
        v = "PASS"
if fatal:
    why.append("receiver failed %s" % fatal.group(1))
if pubfail:
    why.append("publisher: " + pubfail.group(1)[:160])
print(json.dumps(dict(case=name, run=tag, verdict=v, publisher=pub, relay=relay,
                      transport=tr, draft=draft, negotiated_draft=neg, select=sel,
                      lifecycle=life, input=inp, namespace=ns, player_exit=int(prc),
                      seconds=int(dur), complete=complete, ends=ends, units=units,
                      events=events, stats=stats, sender=sender.group(1) if sender else None,
                      check_ok=chk.get("ok"), av_offset_difference_us=chk.get("av_offset_difference_us"),
                      timing=timing, notes=why)))
PY
    tail -1 "$RESULTS" | python3 -I -c 'import json,sys; r=json.loads(sys.stdin.read()); print("%-20s %-4s draft=%s units=%s ends=%s %s" % (r["run"], r["verdict"], r["negotiated_draft"], r["units"], r["ends"], "; ".join(r["notes"])))'
}

while read -r name file; do
    [ -n "$name" ] || continue
    prepare_input "$name" "$file" || echo "input $name: preparation failed" >&2
done <<<"$INPUTS"

# The case loop reads the table on its own descriptor, so nothing a case
# runs can consume it from stdin.
want=" $* "
while read -r -u 3 name pub relay tr draft sel life input; do
    [ -n "$name" ] || continue
    [ $# -eq 0 ] || [[ "$want" == *" $name "* ]] || continue
    for rep in $(seq 1 "$REPEAT"); do
        run_case "$name" "$pub" "$relay" "$tr" "$draft" "$sel" "$life" "$input" "$rep"
    done
done 3<<<"$CASES"
