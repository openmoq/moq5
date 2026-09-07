#!/usr/bin/env bash
# Dual-listener wiring, pinned at the source.
#
# A running relay cannot show you these properties: an all-or-nothing startup
# looks like a clean start, and a teardown that frees shards while a lane is
# still stepping them usually looks like nothing at all. Each is an ordering
# inside one function, so each is pinned here.
set -u
main_c=${1:?usage: check_dual_listener_wiring.sh <cli/main.c>}
fail=0
note() { printf '  %s\n' "$*"; }

full=$(mktemp); code=$(mktemp); labels=$(mktemp)
trap 'rm -f "$full" "$code" "$labels"' EXIT
sed 's|//.*||' "$main_c" | sed 's|/\*.*\*/||' > "$full"

# Every ordering below belongs to the multi-shard composition, so the checks
# read THAT function's body. Scanning the whole file would let the single-facade
# path's own stop/destroy/readiness satisfy a check the dual path fails.
awk '
  /^cmd_serve_lanes\(/ { grab = 1 }
  grab {
      print
      n = length($0)
      for (i = 1; i <= n; i++) {
          c = substr($0, i, 1)
          if (c == "{") { depth++; open = 1 }
          else if (c == "}") { depth--; if (open && depth == 0) exit }
      }
  }' "$full" > "$code"
[ -s "$code" ] || { echo "FAIL: could not locate cmd_serve_lanes"; exit 1; }

# line number of the FIRST match of a pattern, or empty
at() { grep -n "$1" "$code" | head -1 | cut -d: -f1; }

# -- 1. no shard is destroyed while a facade could still be stepping it ----
# Scoped to the paths where both facades are known to be UP: the relay only
# announces itself once every listener has been created, so any shard destroy
# after that line is a live-facade teardown and must stop (and therefore join)
# both facades first. Paths before it are create-failure arms, where the facade
# in question does not exist; those are check 2's job.
ready_l=$(at 'moqr_cli_serve_log_readiness(&log')
[ -n "$ready_l" ] || { echo "FAIL: no readiness line in the dual composition"; fail=1; }
live_bad=0
for d in $(grep -n 'moqr_shards_destroy[[:space:]]*(' "$code" | cut -d: -f1); do
    [ -n "$ready_l" ] && [ "$d" -gt "$ready_l" ] || continue
    awk -v a="$ready_l" -v b="$d" 'NR>a && NR<b' "$code" \
        | grep -q 'moq_msquic_managed_stop[[:space:]]*(' || {
            echo "FAIL: live-path shard destroy at +$d does not stop the raw facade"
            live_bad=1; }
    awk -v a="$ready_l" -v b="$d" 'NR>a && NR<b' "$code" \
        | grep -q 'moq_wtquic_msquic_managed_stop[[:space:]]*(' || {
            echo "FAIL: live-path shard destroy at +$d does not stop the WebTransport facade"
            live_bad=1; }
done
# Ordering alone would let ONE stop, in an earlier branch, stand in for every
# teardown below it. Each live-path teardown owns its own stop, so the counts
# have to keep up with the destroys.
n_destroy=0
for d in $(grep -n 'moqr_shards_destroy[[:space:]]*(' "$code" | cut -d: -f1); do
    [ -n "$ready_l" ] && [ "$d" -gt "$ready_l" ] && n_destroy=$(( n_destroy + 1 ))
done
n_wt_stop=$(awk -v a="$ready_l" 'NR>a' "$code" \
            | grep -c 'moq_wtquic_msquic_managed_stop[[:space:]]*(')
n_raw_stop=$(awk -v a="$ready_l" 'NR>a' "$code" \
             | grep -c 'moq_msquic_managed_stop[[:space:]]*(')
[ "$n_wt_stop" -ge "$n_destroy" ] || {
    echo "FAIL: $n_destroy live-path teardowns but only $n_wt_stop WebTransport stops"
    live_bad=1; }
[ "$n_raw_stop" -ge "$n_destroy" ] || {
    echo "FAIL: $n_destroy live-path teardowns but only $n_raw_stop raw stops"
    live_bad=1; }
[ "$live_bad" -eq 0 ] && note "teardown: every live-path shard destroy stops both facades first"
[ "$live_bad" -eq 0 ] || fail=1

# -- 2. startup is all-or-nothing -------------------------------------------
# The WebTransport create failure branch must tear the raw facade down and
# return, and it must do so BEFORE the readiness line is printed: a half-open
# relay must never announce itself as serving.
ready=$(at 'moqr_cli_serve_log_readiness(&log')
# The listener is built and created in one step, so this is the single call
# the composition makes; removing it leaves nothing for the checks below to
# find and the gate fails rather than passing vacuously.
wt_create=$(at 'moqr_cli_wt_listener_create[[:space:]]*(')
if [ -z "$ready" ] || [ -z "$wt_create" ]; then
    echo "FAIL: missing the readiness line or the WebTransport create"; fail=1
else
    [ "$wt_create" -lt "$ready" ] || {
        echo "FAIL: WebTransport listener is created after the relay announces itself"
        fail=1; }
    # the failure arm, bounded to the 20 lines after the create
    arm=$(awk -v s="$wt_create" 'NR>=s && NR<s+20' "$code")
    printf '%s\n' "$arm" | grep -q 'moq_msquic_managed_stop[[:space:]]*(' || {
        echo "FAIL: WebTransport create failure does not stop the raw facade"; fail=1; }
    printf '%s\n' "$arm" | grep -q 'moqr_shards_destroy[[:space:]]*(' || {
        echo "FAIL: WebTransport create failure does not destroy the shards"; fail=1; }
    printf '%s\n' "$arm" | grep -qE 'return[[:space:]]+(serve_done\(&log,[[:space:]]*)?1' || {
        echo "FAIL: WebTransport create failure does not return an error"; fail=1; }
    note "all-or-nothing: create@$wt_create tears down and returns before ready@$ready"
fi

# -- 3. each facade's lanes carry its own transport label -------------------
# The construction lives in serve_lanes_fill_labels, which the multi-shard
# composition calls and the admin endpoint is handed the result of, so the
# checks read THAT function's body -- and require the dual path to use it.
awk '
  /^serve_lanes_fill_labels\(/ { grab = 1 }
  grab {
      print
      n = length($0)
      for (i = 1; i <= n; i++) {
          c = substr($0, i, 1)
          if (c == "{") { depth++; open = 1 }
          else if (c == "}") { depth--; if (open && depth == 0) exit }
      }
  }' "$full" > "$labels"
[ -s "$labels" ] || {
    echo "FAIL: could not locate serve_lanes_fill_labels"; fail=1; }
grep -q 'serve_lanes_fill_labels[[:space:]]*(' "$code" || {
    echo "FAIL: the multi-shard composition does not build its lane labels";
    fail=1; }
grep -q '\.transport = "msquic"' "$labels" || {
    echo "FAIL: raw lanes are not labelled msquic"; fail=1; }
grep -q '\.transport = "wtquic-msquic"' "$labels" || {
    echo "FAIL: WebTransport lanes are not labelled wtquic-msquic"; fail=1; }
# and they are labelled at their GLOBAL shard index, not a per-facade one
grep -q 'labels\[plan->wt_first + i\]' "$labels" || {
    echo "FAIL: WebTransport labels are not indexed by global shard"; fail=1; }
grep -q 'labels\[plan->raw_first + i\]' "$labels" || {
    echo "FAIL: raw labels are not indexed by global shard"; fail=1; }
note "labels: raw=msquic, wt=wtquic-msquic, indexed by global shard"

# -- 4. each pump addresses only its own shard range ------------------------
grep -q 'moqr_cli_shard_of_wt_lane[[:space:]]*(' "$full" || {
    echo "FAIL: the WebTransport pump does not map through the plan"; fail=1; }
grep -q 'moqr_cli_shard_of_raw_lane[[:space:]]*(' "$full" || {
    echo "FAIL: the raw pump does not map through the plan"; fail=1; }
note "shard mapping: both pumps go through the plan"

# -- 5. a WebTransport config never takes the single-facade path ------------
# The composition is chosen from the shards the process will allocate, across
# both listeners -- so a WebTransport listener can never land on the
# single-facade path, whatever the raw lane count is.
if grep -qE 'return \(?cfg\.lanes > 1\)? \? cmd_serve_lanes' "$full"; then
    echo "FAIL: serve dispatch classifies the process from raw lanes alone"
    fail=1
else
    grep -q 'moqr_cli_total_lanes(&cfg) > 1 ? cmd_serve_lanes' "$full" || {
        echo "FAIL: serve dispatch does not use the total-lane authority"; fail=1; }
    note "dispatch: composition chosen from total lanes"
fi

# -- 11. no process-composition decision is left on raw lanes ---------------
# Transport-local uses stay raw (the raw facade's lane count, its banner, its
# own admission cap, and the plan's raw range); a process-wide predicate must
# not be.
for pat in 'moqr_cli_snapshot_bytes\(cfg->lanes\)' \
           'if \(cfg->lanes <= 1\)' \
           'cfg\.lanes <= 1 &&' \
           'print_capacity\(&cfg, cfg\.lanes > 1'; do
    if grep -qE "$pat" "$full"; then
        echo "FAIL: a process-composition decision still reads raw lanes: $pat"
        fail=1
    fi
done
note "audit: process-composition decisions use total lanes"

# -- 6. the coordinator addresses shards through the plan ------------------
# It iterates GLOBAL shards, so taking a raw lane handle for each one would
# index the raw facade with a WebTransport shard number.
if grep -q 'moq_msquic_managed_lane(t, d)' "$code"; then
    echo "FAIL: the coordinator wake takes a raw lane for every global shard"
    fail=1
else
    grep -q 'serve_wake_shard(ctx, t, d)' "$code" || {
        echo "FAIL: the coordinator wake does not route through the plan"; fail=1; }
    note "coordinator wake: routed by plan"
fi

# -- 7. a terminal on EITHER facade ends the relay --------------------------
grep -q 'moq_wtquic_msquic_managed_wait' "$code" || {
    echo "FAIL: the coordinator never observes the WebTransport facade's terminal"
    fail=1; }
note "terminal: both facades observed"

# -- 8. every live-path teardown destroys the WebTransport facade -----------
# A count alone is too weak: one destroy on an early-return path would stand in
# for the normal shutdown. Each live-path shard destroy needs its own, ordered
# before it, exactly as the stops are checked above.
wtd_bad=0
for d in $(grep -n 'moqr_shards_destroy[[:space:]]*(' "$code" | cut -d: -f1); do
    [ -n "$ready_l" ] && [ "$d" -gt "$ready_l" ] || continue
    awk -v a="$ready_l" -v b="$d" 'NR>a && NR<b' "$code" \
        | grep -q 'moq_wtquic_msquic_managed_destroy[[:space:]]*(' || {
            echo "FAIL: live-path shard destroy at +$d does not destroy the WebTransport facade"
            wtd_bad=1; }
done
n_wt_destroy=$(awk -v a="$ready_l" 'NR>a' "$code" \
               | grep -c 'moq_wtquic_msquic_managed_destroy[[:space:]]*(')
[ "$n_wt_destroy" -ge "$n_destroy" ] || {
    echo "FAIL: $n_destroy live-path teardowns but only $n_wt_destroy WebTransport destroys"
    wtd_bad=1; }
# and never after the shards it may still reference
last_wt_destroy=$(grep -n 'moq_wtquic_msquic_managed_destroy[[:space:]]*(' "$code" \
                  | tail -1 | cut -d: -f1)
last_destroy=$(grep -n 'moqr_shards_destroy[[:space:]]*(' "$code" | tail -1 | cut -d: -f1)
if [ -n "$last_wt_destroy" ] && [ -n "$last_destroy" ] &&
   [ "$last_wt_destroy" -gt "$last_destroy" ]; then
    echo "FAIL: the WebTransport facade is destroyed after the shards"; wtd_bad=1
fi
[ "$wtd_bad" -eq 0 ] && note "wt destroy: one per live-path teardown, all before shard destroy"
[ "$wtd_bad" -eq 0 ] || fail=1

# -- 9. each facade admits only against the shards it owns ------------------
# The raw facade fills its own config here; the WebTransport one is built and
# created in a single step, so its share is read off that call's arguments.
if grep -qE 'tcfg\.max_connections = serve_max_conns' "$code"; then
    echo "FAIL: the raw facade is handed the COMBINED admission cap"
    fail=1
else
    grep -q 'tcfg.max_connections = raw_max_conns' "$code" || {
        echo "FAIL: the raw facade does not use its own cap"; fail=1; }
    if [ -n "$wt_create" ]; then
        # the call's arguments, bounded to the 6 lines it can span
        wt_args=$(awk -v s="$wt_create" 'NR>=s && NR<s+6' "$code")
        printf '%s\n' "$wt_args" | grep -q 'serve_max_conns' && {
            echo "FAIL: the WebTransport facade is handed the COMBINED cap"
            fail=1; }
        printf '%s\n' "$wt_args" | grep -q 'wt_max_conns' || {
            echo "FAIL: the WebTransport facade does not use its own cap"
            fail=1; }
        printf '%s\n' "$wt_args" | grep -q 'plan.wt_count' || {
            echo "FAIL: the WebTransport facade does not use its own lane count"
            fail=1; }
        printf '%s\n' "$wt_args" | grep -q 'relay_wt_lanes_pump' || {
            echo "FAIL: the WebTransport facade is not given its lane pump"
            fail=1; }
    fi
    note "admission: per-facade caps and lane counts"
fi

# -- 10. WebTransport rows name the set THAT listener offers ----------------
grep -q 'version = cfg->wt.alpn_set' "$labels" || {
    echo "FAIL: WebTransport labels do not use the WebTransport version set"
    fail=1; }
note "labels: WebTransport rows carry their own ordered set"

[ "$fail" -eq 0 ] && echo "PASS: dual listener wiring"
exit "$fail"
