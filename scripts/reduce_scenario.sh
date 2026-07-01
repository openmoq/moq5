#!/usr/bin/env bash
#
# Reduce a failing scenario to minimal step count.
#
# Binary searches MOQ_SCENARIO_STEPS to find the smallest value
# that still reproduces the failure.
#
# Assumes prefix-monotonic failures: if step N fails, step N+1 also
# fails. If a failure only occurs at exactly N steps, binary search
# may not find it.
#
# Usage:
#   scripts/reduce_scenario.sh --runner scenario_combined_faults \
#     --seed 0x36 --steps 50 --alloc-fault-permille 25
#
#   scripts/reduce_scenario.sh --self-test

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scenario_common.sh
source "$SCRIPT_DIR/scenario_common.sh"

usage() {
    cat <<EOF
Usage:
  $(basename "$0") [ENV=val...] ./path/to/test_scenario_*
  $(basename "$0") --runner NAME --seed SEED --steps N [options]
  $(basename "$0") --self-test

Options:
  --runner NAME           Scenario runner name
  --seed SEED             Seed value (decimal or 0x hex)
  --steps N               Starting step count (required)
  --fault-permille N      MOQ_SCENARIO_FAULT_PERMILLE
  --alloc-fault-permille N    MOQ_SCENARIO_ALLOC_FAULT_PERMILLE
  --transport-fault-permille N MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE
  --mutate-permille N     MOQ_SCENARIO_MUTATE_PERMILLE (seeded media runners)
  --build-dir DIR         CMake build root (default: auto-detect)
  --dry-run               Print original command only (no reduction)
  --verbose               Print each probe result
  --self-test             Run internal validation with a stub runner

Valid runners: ${VALID_RUNNERS[*]}
EOF
    exit "${1:-0}"
}

DRY_RUN=false
VERBOSE=false
LOG_DIR=""

# -- Build env var array for a given step count ----------------------

build_probe_env() {
    local steps="$1"
    PROBE_ENV=()
    if [[ -n "$SC_SEED_START" ]]; then
        PROBE_ENV+=("MOQ_SCENARIO_SEED_START=$SC_SEED_START")
    fi
    PROBE_ENV+=("MOQ_SCENARIO_SEEDS=${SC_SEEDS:-1}")
    PROBE_ENV+=("MOQ_SCENARIO_STEPS=$steps")
    if [[ -n "$SC_FAULT_PERMILLE" ]]; then
        PROBE_ENV+=("MOQ_SCENARIO_FAULT_PERMILLE=$SC_FAULT_PERMILLE")
    fi
    if [[ -n "$SC_ALLOC_FAULT_PERMILLE" ]]; then
        PROBE_ENV+=("MOQ_SCENARIO_ALLOC_FAULT_PERMILLE=$SC_ALLOC_FAULT_PERMILLE")
    fi
    if [[ -n "$SC_TRANSPORT_FAULT_PERMILLE" ]]; then
        PROBE_ENV+=("MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE=$SC_TRANSPORT_FAULT_PERMILLE")
    fi
    if [[ -n "$SC_MUTATE_PERMILLE" ]]; then
        PROBE_ENV+=("MOQ_SCENARIO_MUTATE_PERMILLE=$SC_MUTATE_PERMILLE")
    fi
}

# -- Run with a given step count, log output, return exit code -------

run_with_steps() {
    local steps="$1"
    local log_file="$2"
    build_probe_env "$steps"

    local rc=0
    env "${PROBE_ENV[@]}" "$SC_BINARY_PATH" > "$log_file" 2>&1 || rc=$?
    return $rc
}

# -- Print command for a given step count ----------------------------

print_command() {
    local steps="$1"
    build_probe_env "$steps"
    echo "${PROBE_ENV[*]} $SC_BINARY_PATH"
}

# -- Self-test with stub runner --------------------------------------

run_self_test() {
    local stub
    stub="$(mktemp /tmp/reduce_stub_XXXXXX)"
    cat > "$stub" <<'STUBEOF'
#!/usr/bin/env bash
steps="${MOQ_SCENARIO_STEPS:-50}"
if (( steps >= 5 )); then
    echo "FAIL: step $steps" >&2
    exit 1
fi
echo "PASS" >&2
exit 0
STUBEOF
    chmod +x "$stub"

    local test_log_dir
    test_log_dir="$(mktemp -d /tmp/reduce_selftest_XXXXXX)"
    local fail=0

    # Test 1: reduce from 50 → 5
    SC_BINARY_PATH="$stub"
    SC_SEED_START="0x0"
    SC_SEEDS="1"
    SC_STEPS="50"
    SC_FAULT_PERMILLE=""
    SC_ALLOC_FAULT_PERMILLE=""
    SC_TRANSPORT_FAULT_PERMILLE=""
    SC_MUTATE_PERMILLE=""

    local rc=0
    run_with_steps 50 "$test_log_dir/t1_orig.log" || rc=$?
    if [[ $rc -eq 0 ]]; then
        echo "FAIL: self-test stub should fail at 50 steps"
        fail=1
    else
        local lo=0 hi=50 mid
        while (( lo < hi )); do
            mid=$(( (lo + hi) / 2 ))
            rc=0
            run_with_steps "$mid" "$test_log_dir/t1_step_${mid}.log" || rc=$?
            if [[ $rc -ne 0 ]]; then hi=$mid; else lo=$(( mid + 1 )); fi
        done
        if [[ $lo -ne 5 ]]; then
            echo "FAIL: self-test expected minimal steps=5, got $lo"
            fail=1
        else
            echo "PASS: self-test reduced 50 → 5"
        fi
    fi

    # Test 2: original passes → error
    SC_STEPS="4"
    rc=0
    run_with_steps 4 "$test_log_dir/t2_orig.log" || rc=$?
    if [[ $rc -ne 0 ]]; then
        echo "FAIL: self-test stub should pass at 4 steps"
        fail=1
    else
        echo "PASS: self-test detects passing original"
    fi

    # Test 3: verify logs were created
    if [[ -f "$test_log_dir/t1_orig.log" ]]; then
        echo "PASS: self-test logs created"
    else
        echo "FAIL: self-test logs not created"
        fail=1
    fi

    # Test 4: step-count arithmetic-injection guard. A crafted step count
    # must be rejected by sc_validate_uint and must NOT be evaluated. The
    # same class of value used unguarded in (( )) IS an injection primitive
    # even under `set -u` (a set scalar used as a fake array name dodges the
    # nounset error), so it touches a separate proof marker. The script's
    # binary-search arithmetic (hi=$SC_STEPS; (( lo < hi ))) runs with lo
    # set, so the guard is what prevents code execution there.
    local marker="$test_log_dir/pwn_guard"
    local proof="$test_log_dir/pwn_raw"
    rm -f "$marker" "$proof"
    # shellcheck disable=SC2016  # MUST stay literal: the guard must reject it
    local crafted='lo[$(touch '"$marker"')+0]'
    local guard_rc=0
    ( sc_validate_uint "--steps/MOQ_SCENARIO_STEPS" "$crafted" ) \
        >/dev/null 2>&1 || guard_rc=$?
    # shellcheck disable=SC2016,SC2034  # value is evaluated by (( )), not read
    ( lo=0; raw='lo[$(touch '"$proof"')+0]'; (( lo < raw )) ) \
        >/dev/null 2>&1 || true
    if [[ $guard_rc -ne 0 && ! -e "$marker" && -e "$proof" ]]; then
        echo "PASS: self-test rejects step-count injection"
    else
        echo "FAIL: self-test step-count injection not blocked" \
             "(guard_rc=$guard_rc)"
        fail=1
    fi
    rm -f "$marker" "$proof"

    # Test 5: the Format A replay-text path funnels the same SC_STEPS into the
    # same guard, so a crafted MOQ_SCENARIO_STEPS token is rejected too.
    local fa_marker="$test_log_dir/pwn_fa"
    rm -f "$fa_marker"
    SC_STEPS=""
    sc_parse_env_token "MOQ_SCENARIO_STEPS=a[\$(touch $fa_marker)+0]"
    local fa_rc=0
    ( sc_validate_uint "--steps/MOQ_SCENARIO_STEPS" "$SC_STEPS" ) \
        >/dev/null 2>&1 || fa_rc=$?
    if [[ $fa_rc -ne 0 && ! -e "$fa_marker" ]]; then
        echo "PASS: self-test rejects Format A step injection"
    else
        echo "FAIL: self-test Format A step injection not blocked" \
             "(fa_rc=$fa_rc)"
        fail=1
    fi
    rm -f "$fa_marker"

    # Test 6: an in-^[0-9]+$ but out-of-range step count (above the signed
    # 64-bit domain) is rejected. Otherwise it wraps negative in (( lo < hi ))
    # and the reducer silently reports a 0-step, 0-iteration "reduction".
    # The exact boundary (SC_UINT_MAX) is pinned: max accepted, max+1 not.
    local big_rc=0 ok_rc=0 atmax_rc=0 overmax_rc=0
    ( sc_validate_uint "--steps" "9223372036854775808" ) >/dev/null 2>&1 \
        || big_rc=$?
    ( sc_validate_uint "--steps" "50" ) >/dev/null 2>&1 || ok_rc=$?
    ( sc_validate_uint "--steps" "1000000000" ) >/dev/null 2>&1 || atmax_rc=$?
    ( sc_validate_uint "--steps" "1000000001" ) >/dev/null 2>&1 || overmax_rc=$?
    if [[ $big_rc -ne 0 && $ok_rc -eq 0 && $atmax_rc -eq 0 && $overmax_rc -ne 0 ]]; then
        echo "PASS: self-test rejects out-of-range step count"
    else
        echo "FAIL: self-test step-count range" \
             "(big_rc=$big_rc ok_rc=$ok_rc atmax_rc=$atmax_rc overmax_rc=$overmax_rc)"
        fail=1
    fi

    rm -rf "$test_log_dir" "$stub"
    exit $fail
}

# -- Parse arguments -------------------------------------------------

if [[ $# -eq 0 ]]; then usage 1; fi

if [[ "$1" == "--self-test" ]]; then
    run_self_test
fi

if sc_is_format_a "$@"; then
    sc_parse_format_a "$@"
else
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --runner)       SC_RUNNER="$2"; shift 2 ;;
            --seed|--seed-start) SC_SEED_START="$2"; shift 2 ;;
            --seeds)        SC_SEEDS="$2"; shift 2 ;;
            --steps)        SC_STEPS="$2"; shift 2 ;;
            --fault-permille) SC_FAULT_PERMILLE="$2"; shift 2 ;;
            --alloc-fault-permille) SC_ALLOC_FAULT_PERMILLE="$2"; shift 2 ;;
            --transport-fault-permille) SC_TRANSPORT_FAULT_PERMILLE="$2"; shift 2 ;;
            --mutate-permille) SC_MUTATE_PERMILLE="$2"; shift 2 ;;
            --build-dir)    SC_BUILD_DIR="$2"; shift 2 ;;
            --dry-run)      DRY_RUN=true; shift ;;
            --verbose)      VERBOSE=true; shift ;;
            --help|-h)      usage 0 ;;
            *)              sc_die "unknown flag: $1" ;;
        esac
    done
    if [[ -z "$SC_RUNNER" ]]; then sc_die "missing --runner"; fi
    sc_validate_runner "$SC_RUNNER"
    SC_BINARY_PATH="$(sc_find_binary "$(sc_runner_to_binary "$SC_RUNNER")")"
fi

SC_SEEDS="${SC_SEEDS:-1}"
SC_STEPS="${SC_STEPS:-50}"

# SC_STEPS feeds bash arithmetic (hi=$SC_STEPS; (( lo < hi ))), which would
# execute a command substitution in a crafted value. Validate it as a plain
# non-negative integer here, after both the Format A and --steps paths have
# set it and the default is applied, before any arithmetic or dry-run use.
sc_validate_uint "--steps/MOQ_SCENARIO_STEPS" "$SC_STEPS"

# -- Dry-run: print command only, no reduction -----------------------

if $DRY_RUN; then
    echo "Would verify failure at $SC_STEPS steps:"
    echo "  $(print_command "$SC_STEPS")"
    echo ""
    echo "Then binary-search steps in [0, $SC_STEPS]."
    echo "Probe results depend on actual pass/fail outcomes."
    exit 0
fi

# -- Create log directory --------------------------------------------

LOG_DIR="$(mktemp -d /tmp/reduce_scenario_XXXXXX)"

# -- Verify original fails -------------------------------------------

echo "Verifying original fails at $SC_STEPS steps..."
orig_rc=0
run_with_steps "$SC_STEPS" "$LOG_DIR/original.log" || orig_rc=$?
if [[ $orig_rc -eq 0 ]]; then
    echo "ERROR: original command does not fail at $SC_STEPS steps"
    echo "Command: $(print_command "$SC_STEPS")"
    echo "Log: $LOG_DIR/original.log"
    exit 1
fi
if $VERBOSE; then echo "  original fails (rc=$orig_rc)"; fi

# -- Binary search ---------------------------------------------------

lo=0
hi=$SC_STEPS
iters=0

while (( lo < hi )); do
    mid=$(( (lo + hi) / 2 ))
    iters=$(( iters + 1 ))
    probe_rc=0
    run_with_steps "$mid" "$LOG_DIR/step_${mid}.log" || probe_rc=$?
    if $VERBOSE; then
        if [[ $probe_rc -ne 0 ]]; then
            echo "  step $mid: FAIL"
        else
            echo "  step $mid: pass"
        fi
    fi
    if [[ $probe_rc -ne 0 ]]; then
        hi=$mid
    else
        lo=$(( mid + 1 ))
    fi
done

# -- Save final minimized failure log --------------------------------

run_with_steps "$lo" "$LOG_DIR/minimized.log" || true

# -- Summary ---------------------------------------------------------

echo ""
echo "Reduced: steps $SC_STEPS → $lo ($iters iterations)"
if [[ $lo -eq 0 ]]; then
    echo "Note: failure reproduces at step 0 (setup/prelude)"
fi
echo ""
echo "Minimized command:"
echo "  $(print_command "$lo")"
echo ""
echo "Logs: $LOG_DIR/"
