#!/usr/bin/env bash
# Capacity must describe the process `serve` will actually run.
#
# A dual config with one lane on each listener runs the MULTI-shard
# composition: two shards, a cross-shard plane, and a serve context sized for
# both. Anything that still classifies the process from the raw lane count
# alone reports a smaller relay than the one that will start — which is exactly
# the number an operator sizes a host from.
#
# Command-facing on purpose: these run the shipped binaries, so a predicate
# left raw-only inside main.c cannot pass this.
set -u
relay=${1:?usage: check_dual_capacity.sh <moq5-relay> [<moq-relay-verify>]}
verify=${2:-}
fail=0
note() { printf '  %s\n' "$*"; }

# The single-lane refusal contract, shared with its own selftest so the
# classifier's decisions are pinned by captured text rather than only by
# whatever the real binary happens to emit today.
. "$(dirname "$0")/verify_refusal.sh"

work=$(mktemp -d); trap 'rm -rf "$work"' EXIT
cat > "$work/dual.json" <<'JSON'
{ "listener":     { "port": 4433, "cert": "c", "key": "k", "lanes": 1 },
  "webtransport": { "port": 4443, "cert": "c", "key": "k", "lanes": 1 } }
JSON
cat > "$work/raw2.json" <<'JSON'
{ "listener": { "port": 4433, "cert": "c", "key": "k", "lanes": 2 } }
JSON
cat > "$work/raw1.json" <<'JSON'
{ "listener": { "port": 4433, "cert": "c", "key": "k", "lanes": 1 } }
JSON

cap() { "$relay" capacity --config "$1" 2>&1; }
field() { printf '%s\n' "$2" | sed -n "s/.*$1[: ]*\([0-9][0-9]*\).*/\1/p" | head -1; }

d=$(cap "$work/dual.json") || { echo "FAIL: capacity refused the dual config"; exit 1; }
r2=$(cap "$work/raw2.json") || { echo "FAIL: capacity refused the raw 2-lane config"; exit 1; }
r1=$(cap "$work/raw1.json") || { echo "FAIL: capacity refused the raw 1-lane config"; exit 1; }

# 1. the dual config must use the multi-shard composition at all
d_cross=$(field 'cross-shard' "$d")
[ -n "$d_cross" ] && [ "$d_cross" -gt 0 ] 2>/dev/null || {
    echo "FAIL: dual 1+1 reports no cross-shard plane (got '${d_cross:-none}')"
    fail=1; }

# 2. and it must match an equivalent raw 2-lane relay term for term
for f in 'cross-shard' 'cli runtime' 'usable client bindings per shard' \
         'relay-state allocation-request ceiling'; do
    a=$(field "$f" "$d"); b=$(field "$f" "$r2")
    if [ "$a" != "$b" ]; then
        echo "FAIL: dual 1+1 and raw 2-lane disagree on '$f' ($a vs $b)"; fail=1
    fi
done
[ "$fail" -eq 0 ] && note "dual 1+1 == raw 2-lane on every shared capacity term"

# 3. and it must NOT look like the direct one-lane model
r1_cross=$(field 'cross-shard' "$r1")
if [ "$(field 'cli runtime' "$d")" = "$(field 'cli runtime' "$r1")" ]; then
    echo "FAIL: dual 1+1 reports the one-lane serve context"; fail=1
fi
if [ -n "$r1_cross" ] && [ "$d_cross" = "$r1_cross" ]; then
    echo "FAIL: dual 1+1 reports the one-lane cross-shard term"; fail=1
fi
note "one-lane model rejected for dual 1+1 (raw1 cross-shard '${r1_cross:-0}')"

# 4. the verify seam must treat dual 1+1 as multi-lane
#
# A refusal is the tool exiting nonzero and saying so on its own diagnostic
# line. It is NOT the word "lanes" appearing somewhere in the output: the
# capacity report itself describes a term as applying "at lanes > 1", so
# scanning the whole text would read a successful report as a refusal.
if [ -n "$verify" ] && [ -x "$verify" ]; then
    out=$(MOQR_VERIFY_BIND_MAX_OPEN_SUBGROUPS=4 \
          MOQR_VERIFY_SESSION_MAX_OPEN_SUBGROUPS=4 \
          "$verify" capacity --config "$work/dual.json" 2>&1)
    rc=$?
    if verify_refused "$rc" "$out"; then
        echo "FAIL: the verify seam refuses dual 1+1 as single-lane"
        printf '%s\n' "$out" | grep '^moq-relay-verify: ' | head -2 \
            | sed 's/^/    /'
        fail=1
    elif verify_no_wt_support "$rc" "$out"; then
        # This verify binary is compiled without WebTransport support, so it
        # refuses ANY webtransport object -- for capacity exactly as for serve.
        # That is a capability answer, not the single-lane misclassification
        # this arm is about, and it is the only other refusal accepted here.
        note "verify seam: dual 1+1 refused for build capability, not lane count"
    elif [ "$rc" -ne 0 ]; then
        echo "FAIL: the verify seam failed on dual 1+1 (rc $rc)"
        printf '%s\n' "$out" | head -3 | sed 's/^/    /'
        fail=1
    else
        note "verify seam: dual 1+1 accepted as multi-lane"
    fi
    # and it must still refuse a genuinely single-lane relay, by the same
    # definition of a refusal
    out1=$(MOQR_VERIFY_BIND_MAX_OPEN_SUBGROUPS=4 \
           MOQR_VERIFY_SESSION_MAX_OPEN_SUBGROUPS=4 \
           "$verify" capacity --config "$work/raw1.json" 2>&1)
    rc1=$?
    if verify_refused "$rc1" "$out1"; then
        note "verify seam: a true single-lane relay is refused, by that refusal"
    else
        echo "FAIL: the verify seam no longer refuses a true single-lane relay"
        echo "    wanted status $MOQR_SINGLE_LANE_STATUS and:"
        printf '    %s\n' "$MOQR_SINGLE_LANE_REFUSAL"
        echo "    got status $rc1 and:"
        printf '%s\n' "$out1" | head -3 | sed 's/^/    /'
        fail=1
    fi
else
    note "verify seam: binary not built in this tree, skipped"
fi

[ "$fail" -eq 0 ] && echo "PASS: dual capacity composition"
exit "$fail"
