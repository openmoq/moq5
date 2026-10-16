#!/usr/bin/env bash
# Two-binary perturbation gate: does the instrumented relay binary perturb
# performance vs the pre-instrumentation binary? Runs both at ONE fixed
# operating point (resolver defaults, the wake-campaign workload) in
# randomized interleaved blocks, then compares BLAST-REPORT JSON only via
# perturbation_compare.py (the old binary emits none of the strict relay
# records, so relay logs are never parsed — the production paired-log
# parser stays strict). Incomplete blocks fail the gate closed.
#
#   OLD_RELAY_BIN=... OLD_RELAY_SHA=... OLD_RELAY_TREE=... \
#   NEW_RELAY_BIN=... NEW_RELAY_SHA=... NEW_RELAY_TREE=... BLAST_BIN=... \
#     run_perturbation_check.sh --blocks 7 --out DIR [--tolerance 0.02]
#
# Provenance is REQUIRED input, not caller homework: each binary's git SHA
# and build tree are mandatory and land in DIR/manifest.txt, so every
# verdict is traceable to the exact sources that produced both binaries.
# --selftest exercises the prerequisite validation and the dry-run
# manifest (no relay/blast runs).
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
blocks=7
tolerance=0.02
outdir=""
seed=1447
dry_run=0
selftest=0
# Operating point (defaults = the single-point wake workload; callers may
# sweep subs/lanes across points).
lanes=2
subs=16
groups=16
objects=64
payload=512
require_improvement=0
min_gain=""
while [ $# -gt 0 ]; do
    case "$1" in
        --blocks) blocks="$2"; shift 2 ;;
        --tolerance) tolerance="$2"; shift 2 ;;
        --out) outdir="$2"; shift 2 ;;
        --seed) seed="$2"; shift 2 ;;
        --lanes) lanes="$2"; shift 2 ;;
        --subs) subs="$2"; shift 2 ;;
        --groups) groups="$2"; shift 2 ;;
        --objects) objects="$2"; shift 2 ;;
        --payload) payload="$2"; shift 2 ;;
        --require-improvement) require_improvement=1; shift ;;
        --min-gain) min_gain="$2"; shift 2 ;;
        --dry-run) dry_run=1; shift ;;
        --selftest) selftest=1; shift ;;
        *) echo "unknown flag: $1" >&2; exit 2 ;;
    esac
done

# validate arguments BEFORE any relay starts (or any selftest recursion):
# a bad comparator argument must never be discovered after the campaign
case "$blocks" in
    ''|*[!0-9]*) echo "--blocks must be a positive integer" >&2; exit 2 ;;
esac
[ "$blocks" -ge 1 ] || { echo "--blocks must be >= 1" >&2; exit 2; }
case "$seed" in
    ''|*[!0-9]*) echo "--seed must be a nonnegative integer" >&2; exit 2 ;;
esac
# strict lexical form FIRST (awk would coerce "0.5junk"/nan/inf), then
# the open-interval range check on the now-known-clean decimal
case "$tolerance" in
    0.[0-9]*|.[0-9]*) : ;;
    *) echo "--tolerance must be a plain decimal with 0 < t < 1" >&2
       exit 2 ;;
esac
case "$tolerance" in
    *[!0-9.]*|*.*.*) echo "--tolerance must be a plain decimal" >&2
                     exit 2 ;;
esac
awk -v t="$tolerance" 'BEGIN { exit !(t > 0 && t < 1) }' || {
    echo "--tolerance must satisfy 0 < t < 1" >&2; exit 2; }
# operating-point arguments: canonical positive decimal integers ONLY —
# no signs, junk suffixes, leading zeros, or underscore separators (a bad
# value must never be discovered after processes have started)
for opt in lanes subs groups objects payload; do
    eval "v=\$$opt"
    case "$v" in
        [1-9]|[1-9][0-9]|[1-9][0-9][0-9]|[1-9][0-9][0-9][0-9]|[1-9][0-9][0-9][0-9][0-9]) : ;;
        *) echo "--$opt must be a canonical positive integer, got '$v'" >&2
           exit 2 ;;
    esac
done
# --min-gain (optional) mirrors the comparator's canonical-fraction rule
if [ -n "$min_gain" ]; then
    case "$min_gain" in
        0.[0-9]*|.[0-9]*) : ;;
        *) echo "--min-gain must be a plain decimal with 0 < g < 1" >&2
           exit 2 ;;
    esac
    case "$min_gain" in
        *[!0-9.]*|*.*.*) echo "--min-gain must be a plain decimal" >&2
                         exit 2 ;;
    esac
    awk -v g="$min_gain" 'BEGIN { exit !(g > 0 && g < 1) }' || {
        echo "--min-gain must satisfy 0 < g < 1" >&2; exit 2; }
    if [ "$require_improvement" -ne 1 ]; then
        echo "--min-gain requires --require-improvement" >&2; exit 2
    fi
fi

if [ "$selftest" -eq 1 ]; then
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' EXIT
    # (1) missing provenance is refused with a naming message
    if env -u OLD_RELAY_SHA OLD_RELAY_BIN=/bin/ls NEW_RELAY_BIN=/bin/ls \
        OLD_RELAY_TREE=x NEW_RELAY_SHA=y NEW_RELAY_TREE=z BLAST_BIN=/bin/ls \
        "$0" --blocks 2 --out "$tmp/a" --dry-run \
        > "$tmp/o" 2> "$tmp/e"; then
        echo "FAIL: missing OLD_RELAY_SHA accepted" >&2; exit 1
    fi
    grep -q "OLD_RELAY_SHA" "$tmp/e" || {
        echo "FAIL: refusal does not name OLD_RELAY_SHA" >&2; exit 1; }
    # (2) the dry-run manifest records both SHAs, trees, and the schedule
    OLD_RELAY_BIN=/bin/ls OLD_RELAY_SHA=oldsha OLD_RELAY_TREE=/tmp/old \
    NEW_RELAY_BIN=/bin/ls NEW_RELAY_SHA=newsha NEW_RELAY_TREE=/tmp/new \
    BLAST_BIN=/bin/ls "$0" --blocks 3 --out "$tmp/b" --dry-run \
        > /dev/null
    for want in oldsha newsha /tmp/old /tmp/new "block 3:"; do
        grep -q "$want" "$tmp/b/manifest.txt" || {
            echo "FAIL: manifest missing $want" >&2; exit 1; }
    done
    # (3) the block schedule is deterministic for one seed
    OLD_RELAY_BIN=/bin/ls OLD_RELAY_SHA=oldsha OLD_RELAY_TREE=/tmp/old \
    NEW_RELAY_BIN=/bin/ls NEW_RELAY_SHA=newsha NEW_RELAY_TREE=/tmp/new \
    BLAST_BIN=/bin/ls "$0" --blocks 3 --out "$tmp/c" --dry-run \
        > /dev/null
    diff <(grep "^block" "$tmp/b/manifest.txt") \
         <(grep "^block" "$tmp/c/manifest.txt") > /dev/null || {
        echo "FAIL: schedule not deterministic" >&2; exit 1; }
    # (4) a rep wrapper that writes a VALID passed report but exits nonzero
    # must invalidate the block and fail the gate closed
    cat > "$tmp/stub.sh" <<'STUB'
#!/usr/bin/env bash
out=""
while [ $# -gt 0 ]; do
    if [ "$1" = "--out" ]; then out="$2"; shift 2; else shift; fi
done
mkdir -p "$out"
cat > "$out/blast-l2-r1.json" <<'JSON'
{"correctness":{"passed":true},
 "ergonomics_notes":["retrieval=subscribe (live ABSOLUTE_START fan-out)"],
 "fairness":{"alpn":"moqt-16","namespace_count":1,"tracks_per_namespace":1,
  "publishers_per_namespace":1,"subscribers_per_track":16,"groups":16,
  "objects_per_group":64,"first_object_size":512,"other_object_size":512},
 "metrics":{"measurement_basis":"relay_e2e_wall",
  "wall_objects_per_sec":100000.0,"latency_p99_ns":150000000}}
JSON
exit 3
STUB
    chmod +x "$tmp/stub.sh"
    if OLD_RELAY_BIN=/bin/ls OLD_RELAY_SHA=oldsha OLD_RELAY_TREE=/tmp/old \
        NEW_RELAY_BIN=/bin/ls NEW_RELAY_SHA=newsha NEW_RELAY_TREE=/tmp/new \
        BLAST_BIN=/bin/ls PERTURB_REPRO_BIN="$tmp/stub.sh" \
        "$0" --blocks 2 --out "$tmp/d" > "$tmp/d.out" 2>&1; then
        echo "FAIL: nonzero rep wrapper did not fail the gate" >&2; exit 1
    fi
    grep -q "block invalidated" "$tmp/d/manifest.txt" || {
        echo "FAIL: manifest missing the invalidation record" >&2; exit 1; }
    grep -q "PERTURBATION GATE: FAIL" "$tmp/d/verdict.txt" || {
        echo "FAIL: verdict missing" >&2; exit 1; }
    # (5) bad arguments are refused BEFORE any campaign machinery
    if "$0" --blocks 0 --out "$tmp/e" --dry-run > /dev/null 2>&1; then
        echo "FAIL: --blocks 0 accepted" >&2; exit 1
    fi
    for bad_tol in 1.5 0.5junk nan inf; do
        if "$0" --blocks 2 --tolerance "$bad_tol" --out "$tmp/e" \
            --dry-run > /dev/null 2>&1; then
            echo "FAIL: --tolerance $bad_tol accepted" >&2; exit 1
        fi
    done
    # (6) operating-point arguments: zero, negative, junk, underscores,
    # and leading zeros are refused for every point flag
    for flag in lanes subs groups objects payload; do
        for bad in 0 -3 1_6 24x 016 ""; do
            if "$0" --blocks 2 --"$flag" "$bad" --out "$tmp/f" \
                --dry-run > /dev/null 2>&1; then
                echo "FAIL: --$flag '$bad' accepted" >&2; exit 1
            fi
        done
    done
    for bad_gain in 10 1_0 -0.1 1e-1 0 junk; do
        if "$0" --blocks 2 --require-improvement --min-gain "$bad_gain" \
            --out "$tmp/g" --dry-run > /dev/null 2>&1; then
            echo "FAIL: --min-gain '$bad_gain' accepted" >&2; exit 1
        fi
    done
    # --min-gain without --require-improvement is refused
    if "$0" --blocks 2 --min-gain 0.05 --out "$tmp/g2" \
        --dry-run > /dev/null 2>&1; then
        echo "FAIL: --min-gain without --require-improvement accepted" >&2
        exit 1
    fi
    echo "# ALL PASS"
    exit 0
fi

[ -n "$outdir" ] || { echo "--out is required" >&2; exit 2; }
: "${OLD_RELAY_BIN:?set OLD_RELAY_BIN}" "${NEW_RELAY_BIN:?set NEW_RELAY_BIN}"
: "${OLD_RELAY_SHA:?set OLD_RELAY_SHA (git SHA the old binary was built from)}"
: "${NEW_RELAY_SHA:?set NEW_RELAY_SHA (git SHA the new binary was built from)}"
: "${OLD_RELAY_TREE:?set OLD_RELAY_TREE (build tree of the old binary)}"
: "${NEW_RELAY_TREE:?set NEW_RELAY_TREE (build tree of the new binary)}"
: "${BLAST_BIN:?set BLAST_BIN}"
mkdir -p "$outdir"

{
    echo "argv: --blocks $blocks --tolerance $tolerance --seed $seed" \
         "--lanes $lanes --subs $subs --groups $groups" \
         "--objects $objects --payload $payload"
    echo "old: $OLD_RELAY_BIN sha=$OLD_RELAY_SHA tree=$OLD_RELAY_TREE"
    echo "new: $NEW_RELAY_BIN sha=$NEW_RELAY_SHA tree=$NEW_RELAY_TREE"
    echo "blast: $BLAST_BIN"
} > "$outdir/manifest.txt"

# PERTURB_REPRO_BIN overrides the rep runner for the structural selftest
# stub ONLY (documented test plumbing, like RELAY_BIN/BLAST_BIN).
repro="${PERTURB_REPRO_BIN:-$here/run_relay_blast_repro.sh}"
arm_failed=0

# deterministic per-block order from the seed (xorshift-ish LCG is enough
# for a coin flip; the SAME seed reproduces the SAME schedule)
rng=$seed
compare_args=(--blocks "$blocks" --tolerance "$tolerance"
              --expect-subs "$subs" --expect-lanes "$lanes"
              --expect-groups "$groups" --expect-objects "$objects"
              --expect-payload "$payload")
if [ "$require_improvement" -eq 1 ]; then
    compare_args+=(--require-improvement)
fi
if [ -n "$min_gain" ]; then
    compare_args+=(--min-gain "$min_gain")
fi
for b in $(seq 1 "$blocks"); do
    rng=$(( (rng * 6364136223846793005 + 1442695040888963407) % 4294967296 ))
    if [ $((rng % 2)) -eq 0 ]; then order="old new"; else order="new old"; fi
    echo "block $b: $order" | tee -a "$outdir/manifest.txt"
    if [ "$dry_run" -eq 1 ]; then
        continue
    fi
    for tag in $order; do
        bin="$OLD_RELAY_BIN"; [ "$tag" = new ] && bin="$NEW_RELAY_BIN"
        rep_out="$outdir/b$b-$tag"
        arm_rc=0
        RELAY_BIN="$bin" BLAST_BIN="$BLAST_BIN" \
            "$repro" --lanes "$lanes" --subs "$subs" --groups "$groups" \
            --objects-per-group "$objects" --payload "$payload" \
            --reps 1 --out "$rep_out" > "$rep_out.summary" 2>&1 || arm_rc=$?
        # A nonzero wrapper status invalidates the ARM even when a passed
        # report file exists: launcher/teardown/strict-record failures must
        # never feed the CI through a surviving JSON.
        if [ "$arm_rc" -ne 0 ]; then
            echo "block $b ($tag): repro exit $arm_rc — block invalidated" \
                | tee -a "$outdir/manifest.txt"
            arm_failed=1
        fi
        if [ ! -f "$rep_out/blast-l$lanes-r1.json" ]; then
            echo "block $b ($tag): no report — the gate will fail closed" \
                | tee -a "$outdir/manifest.txt"
        fi
    done
    compare_args+=(--block "$outdir/b$b-old/blast-l$lanes-r1.json"
                   "$outdir/b$b-new/blast-l$lanes-r1.json")
done

if [ "$dry_run" -eq 1 ]; then
    echo "dry run: schedule + manifest only, no verdict"
    exit 0
fi
if [ "$arm_failed" -ne 0 ]; then
    echo "PERTURBATION GATE: FAIL (a rep wrapper exited nonzero — the " \
         "gate fails closed regardless of surviving reports)" \
        | tee "$outdir/verdict.txt"
    exit 1
fi
python3 "$here/perturbation_compare.py" "${compare_args[@]}" \
    | tee "$outdir/verdict.txt"
