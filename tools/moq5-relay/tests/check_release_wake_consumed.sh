#!/usr/bin/env bash
# Each serve loop must consume the release-wake its coordinator returns.
#
# `out_wake_lanes` is the only notice that retiring a generation opened
# deferred work. A loop that receives it and drops it leaves those lanes
# unwoken until the next poll — which the dedicated admin thread in Step 3 has
# no equivalent of. The two call sites differ (one managed lane vs every global
# shard), so each is pinned at its OWN call site rather than by a file-wide
# grep that a neighbouring wake could satisfy.
set -u
main_c="${1:-}"
[ -n "$main_c" ] || { echo "usage: $0 <cli/main.c>" >&2; exit 2; }
[ -r "$main_c" ] || { echo "FAIL: cannot read $main_c" >&2; exit 1; }

failures=0
fail() { echo "FAIL: $*" >&2; failures=$((failures + 1)); }

code=$(sed -e 's://.*::' "$main_c" | tr '\n' '\001' |
       sed -e 's:/\*[^\*]*\*\+\([^/\*][^\*]*\*\+\)*/: :g' | tr '\001' '\n')

# For each coordinator call that passes &wake_after_release, require a
# consumption branch on that same variable within the following few lines, and
# require that branch to contain the wake call this composition uses.
check_site() {
    fn="$1"; wake_call="$2"
    body=$(printf '%s\n' "$code" |
           awk -v f="^$fn\\\\(" 'BEGIN{on=0} $0 ~ f {on=1} on {print} on && /^}/ {exit}')
    [ -n "$body" ] || { fail "$fn not found"; return; }

    sites=$(printf '%s\n' "$body" | grep -c '&wake_after_release')
    [ "$sites" -gt 0 ] || { fail "$fn never asks for the release wake"; return; }

    # Every request for the flag must be followed by a guarded consumption.
    n=0
    printf '%s\n' "$body" | grep -n '&wake_after_release);' | while IFS=: read -r ln _; do
        hi=$((ln + 12))
        window=$(printf '%s\n' "$body" | sed -n "${ln},${hi}p")
        printf '%s\n' "$window" | grep -q 'if (wake_after_release)' ||
            { echo "FAIL: $fn does not test wake_after_release after its call at body line $ln" >&2; exit 1; }
        printf '%s\n' "$window" | grep -q "$wake_call" ||
            { echo "FAIL: $fn does not call $wake_call when the release wake is set (body line $ln)" >&2; exit 1; }
        n=$((n + 1))
    done || fail "$fn does not consume the release wake at every call site"
}

# K=1 wakes its single managed lane; K>1 routes every global shard by plan.
check_site cmd_serve       'moq_msquic_lane_wake'
check_site cmd_serve_lanes 'serve_wake_shard'

# The K>1 consumption must cover EVERY shard, not just one.
lanes_body=$(printf '%s\n' "$code" |
    awk 'BEGIN{on=0} /^cmd_serve_lanes\(/ {on=1} on {print} on && /^}/ {exit}')
blk=$(printf '%s\n' "$lanes_body" | grep -A6 'if (wake_after_release)')
printf '%s\n' "$blk" | grep -q 'plan.total_shards' ||
    fail "cmd_serve_lanes wakes only some shards on the release wake"

if [ "$failures" -ne 0 ]; then
    echo "FAIL: $failures release-wake consumption violation(s)" >&2
    exit 1
fi
echo "PASS: every serve loop consumes the release wake at its own call site"
exit 0
