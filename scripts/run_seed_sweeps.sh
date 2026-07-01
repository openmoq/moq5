#!/usr/bin/env bash
#
# Run deterministic seed sweeps across scenario runners.
#
# Profiles:
#   quick     ~5s   1k seeds, normal build only
#   standard  ~30s  10k seeds + ASan 1k subset
#   nightly   ~5min 100k seeds + ASan 10k subset
#
# Usage:
#   scripts/run_seed_sweeps.sh --profile quick
#   scripts/run_seed_sweeps.sh --profile standard --fail-fast
#   scripts/run_seed_sweeps.sh --runner scenario_auth --seeds 50000
#   scripts/run_seed_sweeps.sh --runner scenario_combined_faults --seeds 100 \
#     --alloc-fault-permille 25 --transport-fault-permille 40
#   scripts/run_seed_sweeps.sh --profile quick --dry-run

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scenario_common.sh
source "$SCRIPT_DIR/scenario_common.sh"

usage() {
    cat <<EOF
Usage:
  $(basename "$0") --profile NAME [options]
  $(basename "$0") --runner NAME --seeds N [options]

Profiles: quick, standard, nightly

Options:
  --profile NAME                Sweep profile
  --runner NAME                 Run only this runner
  --seeds N                     Override seed count
  --steps N                     Override step count (default: 50)
  --build-dir DIR               Normal build root (default: build)
  --asan-dir DIR                ASan build root (default: build/asan)
  --fault-permille N            Fault rate for scenario_faults / scenario_transport_faults
  --alloc-fault-permille N      Allocation fault rate for scenario_combined_faults
  --transport-fault-permille N  Transport fault rate for scenario_combined_faults
  --fail-fast                   Stop on first failure
  --dry-run                     Print commands without executing
  --verbose                     Print each runner invocation
  --help                        Show this help

In --runner/--seeds mode, fault-rate flags are passed to the runner.
Profiles use hardcoded fault rates; the flags above are ignored.
EOF
    exit "${1:-0}"
}

PROFILE=""
RUNNER_FILTER=""
SEEDS_OVERRIDE=""
STEPS="50"
BUILD_DIR=""
ASAN_DIR=""
FAULT_PERMILLE=""
ALLOC_FAULT_PERMILLE=""
TRANSPORT_FAULT_PERMILLE=""
FAIL_FAST=false
DRY_RUN=false
VERBOSE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile)                  PROFILE="$2"; shift 2 ;;
        --runner)                   RUNNER_FILTER="$2"; shift 2 ;;
        --seeds)                    SEEDS_OVERRIDE="$2"; shift 2 ;;
        --steps)                    STEPS="$2"; shift 2 ;;
        --build-dir)                BUILD_DIR="$2"; shift 2 ;;
        --asan-dir)                 ASAN_DIR="$2"; shift 2 ;;
        --fault-permille)           FAULT_PERMILLE="$2"; shift 2 ;;
        --alloc-fault-permille)     ALLOC_FAULT_PERMILLE="$2"; shift 2 ;;
        --transport-fault-permille) TRANSPORT_FAULT_PERMILLE="$2"; shift 2 ;;
        --fail-fast)                FAIL_FAST=true; shift ;;
        --dry-run)                  DRY_RUN=true; shift ;;
        --verbose)                  VERBOSE=true; shift ;;
        --help|-h)                  usage 0 ;;
        *)                          sc_die "unknown flag: $1" ;;
    esac
done

BUILD_DIR="${BUILD_DIR:-$SCENARIO_ROOT/build}"
ASAN_DIR="${ASAN_DIR:-$SCENARIO_ROOT/build/asan}"

LOG_DIR=""
if ! $DRY_RUN; then
    LOG_DIR="$(mktemp -d /tmp/seed_sweep_XXXXXX)"
fi

# -- Resolve binary for a runner + build dir -------------------------

find_runner_binary() {
    local runner="$1" dir="$2"
    local bin="$dir/tests/test_${runner}"
    if [[ -x "$bin" ]]; then echo "$bin"; return 0; fi
    return 1
}

# -- Convert extra_env to replay CLI flags ---------------------------

env_to_replay_flags() {
    local flags=""
    for e in "$@"; do
        case "$e" in
            MOQ_SCENARIO_FAULT_PERMILLE=*)
                flags="$flags --fault-permille ${e#*=}" ;;
            MOQ_SCENARIO_ALLOC_FAULT_PERMILLE=*)
                flags="$flags --alloc-fault-permille ${e#*=}" ;;
            MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE=*)
                flags="$flags --transport-fault-permille ${e#*=}" ;;
            MOQ_SCENARIO_MUTATE_PERMILLE=*)
                flags="$flags --mutate-permille ${e#*=}" ;;
        esac
    done
    echo "$flags"
}

# -- Run one sweep entry ---------------------------------------------

run_entry() {
    local runner="$1" seeds="$2" build="$3" label="$4"
    shift 4
    local extra_env=("$@")

    if [[ -n "$RUNNER_FILTER" && "$runner" != "$RUNNER_FILTER" ]]; then
        return 0
    fi

    # Skip (return 2), pass (0), or fail (1) — distinct so the summary does
    # not count a skipped entry as passed. Skips warn unconditionally (the
    # docs promise a warning), not only under --verbose.
    local binary
    if ! binary="$(find_runner_binary "$runner" "$build")"; then
        if [[ "$build" == "$ASAN_DIR" ]]; then
            echo "  SKIP $runner ($label): ASan build not found" >&2
            return 2
        fi
        # Seeded media runners depend on optional media libs (MOQ_BUILD_MSF/
        # CMAF/LOC). Skip gracefully when a build omits them, rather than
        # failing the whole sweep.
        case "$runner" in
            scenario_media_*)
                echo "  SKIP $runner ($label): media runner not built" >&2
                return 2 ;;
        esac
        echo "ERROR: binary not found: $build/tests/test_${runner}"
        return 1
    fi

    if [[ -n "$SEEDS_OVERRIDE" ]]; then seeds="$SEEDS_OVERRIDE"; fi

    local env_vars=(
        "MOQ_SCENARIO_SEED_START=0"
        "MOQ_SCENARIO_SEEDS=$seeds"
        "MOQ_SCENARIO_STEPS=$STEPS"
    )
    env_vars+=("${extra_env[@]}")

    if $DRY_RUN; then
        echo "  ${env_vars[*]} $binary"
        return 0
    fi

    if $VERBOSE; then echo "  $runner ($label, $seeds seeds)..."; fi

    local log_file="$LOG_DIR/${runner}_${label}.log"
    local rc=0
    env "${env_vars[@]}" "$binary" >"$log_file" 2>&1 || rc=$?

    if [[ $rc -eq 0 ]]; then
        local pass_line
        pass_line="$(grep '^PASS:' "$log_file" || true)"
        if [[ -n "$pass_line" ]]; then
            echo "  PASS: $runner ($label) $pass_line"
        else
            echo "  PASS: $runner ($label, $seeds seeds)"
        fi
        return 0
    fi

    echo "  FAIL: $runner ($label, $seeds seeds, rc=$rc)"
    echo "    Log: $log_file"

    local fail_seed
    fail_seed="$(grep -o 'seed=0x[0-9a-fA-F]*' "$log_file" | head -1 || true)"
    if [[ -n "$fail_seed" ]]; then
        local seed_val="${fail_seed#seed=}"
        local fault_flags
        fault_flags="$(env_to_replay_flags "${extra_env[@]}")"
        echo "    Replay: scripts/replay_scenario.sh --runner $runner --seed $seed_val --steps $STEPS --build-dir $build$fault_flags"
        echo "    Reduce: scripts/reduce_scenario.sh --runner $runner --seed $seed_val --steps $STEPS --build-dir $build$fault_flags"
    fi
    tail -5 "$log_file" | sed 's/^/    /'
    return 1
}

# -- Profile definitions ---------------------------------------------

run_profile() {
    local profile="$1"
    local total=0 passed=0 failed=0 skipped=0
    local stopped=false

    run_one() {
        local runner="$1"
        if $stopped; then return 0; fi
        if [[ -n "$RUNNER_FILTER" && "$runner" != "$RUNNER_FILTER" ]]; then
            return 0
        fi
        total=$((total + 1))
        local rc=0
        run_entry "$@" || rc=$?
        if [[ $rc -eq 0 ]]; then
            passed=$((passed + 1))
        elif [[ $rc -eq 2 ]]; then
            skipped=$((skipped + 1))
        else
            failed=$((failed + 1))
            if $FAIL_FAST; then
                echo ""
                echo "FAIL-FAST: stopping after first failure"
                stopped=true
            fi
        fi
        return 0
    }

    echo "=== Seed Sweep: $profile ==="
    echo ""

    local extra=()
    case "$profile" in
    quick)
        run_one scenario_auth           1000 "$BUILD_DIR" "normal"
        run_one scenario_lifecycle      1000 "$BUILD_DIR" "normal"
        run_one scenario_object         1000 "$BUILD_DIR" "normal"
        run_one scenario_streaming_object 1000 "$BUILD_DIR" "normal"
        run_one scenario_streaming_faults 1000 "$BUILD_DIR" "normal" \
            "MOQ_SCENARIO_FAULT_PERMILLE=50"
        run_one scenario_publisher      1000 "$BUILD_DIR" "normal"
        run_one scenario_goaway         1000 "$BUILD_DIR" "normal"
        run_one scenario_backpressure   1000 "$BUILD_DIR" "normal"
        run_one scenario_request_credit 1000 "$BUILD_DIR" "normal"
        run_one scenario_namespace_sub  1000 "$BUILD_DIR" "normal"
        run_one scenario_fetch          1000 "$BUILD_DIR" "normal"
        run_one scenario_publish 1000 "$BUILD_DIR" "normal"
        run_one scenario_track_status 1000 "$BUILD_DIR" "normal"
        run_one scenario_object_datagram 1000 "$BUILD_DIR" "normal"
        run_one scenario_faults         1000 "$BUILD_DIR" "normal" \
            "MOQ_SCENARIO_FAULT_PERMILLE=30"
        run_one scenario_transport_faults 1000 "$BUILD_DIR" "normal" \
            "MOQ_SCENARIO_FAULT_PERMILLE=50"
        run_one scenario_combined_faults 1000 "$BUILD_DIR" "normal" \
            "MOQ_SCENARIO_ALLOC_FAULT_PERMILLE=25" \
            "MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE=40"
        run_one scenario_delay          1000 "$BUILD_DIR" "normal"
        run_one scenario_crossed        1000 "$BUILD_DIR" "normal"
        run_one scenario_delay_backpressure 1000 "$BUILD_DIR" "normal"
        # Seeded media conformance: generate-only in quick (fast, no mutation).
        run_one scenario_media_loc      1000 "$BUILD_DIR" "normal"
        run_one scenario_media_msf      1000 "$BUILD_DIR" "normal"
        run_one scenario_media_cmaf     1000 "$BUILD_DIR" "normal"
        ;;
    standard)
        run_one scenario_auth           10000 "$BUILD_DIR" "normal"
        run_one scenario_lifecycle      10000 "$BUILD_DIR" "normal"
        run_one scenario_object         10000 "$BUILD_DIR" "normal"
        run_one scenario_streaming_object 10000 "$BUILD_DIR" "normal"
        run_one scenario_streaming_faults 10000 "$BUILD_DIR" "normal" \
            "MOQ_SCENARIO_FAULT_PERMILLE=50"
        run_one scenario_publisher      10000 "$BUILD_DIR" "normal"
        run_one scenario_goaway         10000 "$BUILD_DIR" "normal"
        run_one scenario_backpressure   10000 "$BUILD_DIR" "normal"
        run_one scenario_request_credit 10000 "$BUILD_DIR" "normal"
        run_one scenario_namespace_sub  10000 "$BUILD_DIR" "normal"
        run_one scenario_fetch          10000 "$BUILD_DIR" "normal"
        run_one scenario_publish 1000 "$BUILD_DIR" "normal"
        run_one scenario_track_status 1000 "$BUILD_DIR" "normal"
        run_one scenario_object_datagram 10000 "$BUILD_DIR" "normal"
        run_one scenario_faults         10000 "$BUILD_DIR" "normal" \
            "MOQ_SCENARIO_FAULT_PERMILLE=30"
        run_one scenario_transport_faults 10000 "$BUILD_DIR" "normal" \
            "MOQ_SCENARIO_FAULT_PERMILLE=50"
        run_one scenario_combined_faults 10000 "$BUILD_DIR" "normal" \
            "MOQ_SCENARIO_ALLOC_FAULT_PERMILLE=25" \
            "MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE=40"
        run_one scenario_delay          1000 "$BUILD_DIR" "normal"
        run_one scenario_crossed        1000 "$BUILD_DIR" "normal"
        run_one scenario_delay_backpressure 1000 "$BUILD_DIR" "normal"
        # Seeded media conformance: larger generate-only + modest mutation.
        run_one scenario_media_loc      10000 "$BUILD_DIR" "normal" \
            "MOQ_SCENARIO_MUTATE_PERMILLE=200"
        run_one scenario_media_msf      10000 "$BUILD_DIR" "normal" \
            "MOQ_SCENARIO_MUTATE_PERMILLE=200"
        run_one scenario_media_cmaf     10000 "$BUILD_DIR" "normal" \
            "MOQ_SCENARIO_MUTATE_PERMILLE=200"
        # ASan subset
        run_one scenario_auth           1000 "$ASAN_DIR" "asan"
        run_one scenario_publisher      1000 "$ASAN_DIR" "asan"
        run_one scenario_transport_faults 1000 "$ASAN_DIR" "asan" \
            "MOQ_SCENARIO_FAULT_PERMILLE=50"
        run_one scenario_combined_faults 1000 "$ASAN_DIR" "asan" \
            "MOQ_SCENARIO_ALLOC_FAULT_PERMILLE=25" \
            "MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE=40"
        # Mutation has teeth under ASan (catches over-reads, not just rejects).
        run_one scenario_media_loc      1000 "$ASAN_DIR" "asan" \
            "MOQ_SCENARIO_MUTATE_PERMILLE=500"
        run_one scenario_media_msf      1000 "$ASAN_DIR" "asan" \
            "MOQ_SCENARIO_MUTATE_PERMILLE=500"
        run_one scenario_media_cmaf     1000 "$ASAN_DIR" "asan" \
            "MOQ_SCENARIO_MUTATE_PERMILLE=500"
        ;;
    nightly)
        run_one scenario_auth           100000 "$BUILD_DIR" "normal"
        run_one scenario_lifecycle      100000 "$BUILD_DIR" "normal"
        run_one scenario_object         100000 "$BUILD_DIR" "normal"
        run_one scenario_streaming_object 100000 "$BUILD_DIR" "normal"
        run_one scenario_streaming_faults 100000 "$BUILD_DIR" "normal" \
            "MOQ_SCENARIO_FAULT_PERMILLE=50"
        run_one scenario_publisher      100000 "$BUILD_DIR" "normal"
        run_one scenario_goaway         100000 "$BUILD_DIR" "normal"
        run_one scenario_backpressure   100000 "$BUILD_DIR" "normal"
        run_one scenario_request_credit 100000 "$BUILD_DIR" "normal"
        run_one scenario_namespace_sub  100000 "$BUILD_DIR" "normal"
        run_one scenario_faults         100000 "$BUILD_DIR" "normal" \
            "MOQ_SCENARIO_FAULT_PERMILLE=30"
        run_one scenario_transport_faults 100000 "$BUILD_DIR" "normal" \
            "MOQ_SCENARIO_FAULT_PERMILLE=50"
        run_one scenario_combined_faults 100000 "$BUILD_DIR" "normal" \
            "MOQ_SCENARIO_ALLOC_FAULT_PERMILLE=25" \
            "MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE=40"
        run_one scenario_delay          10000 "$BUILD_DIR" "normal"
        run_one scenario_crossed        10000 "$BUILD_DIR" "normal"
        run_one scenario_delay_backpressure 10000 "$BUILD_DIR" "normal"
        # Seeded media conformance: large mutation-enabled sweeps.
        run_one scenario_media_loc      100000 "$BUILD_DIR" "normal" \
            "MOQ_SCENARIO_MUTATE_PERMILLE=500"
        run_one scenario_media_msf      100000 "$BUILD_DIR" "normal" \
            "MOQ_SCENARIO_MUTATE_PERMILLE=500"
        run_one scenario_media_cmaf     100000 "$BUILD_DIR" "normal" \
            "MOQ_SCENARIO_MUTATE_PERMILLE=500"
        # ASan full set (media runners get mutation, where ASan has the most to
        # catch; they skip when the ASan build omits the media libs).
        for r in "${VALID_RUNNERS[@]}"; do
            extra=()
            case "$r" in
                scenario_faults) extra=("MOQ_SCENARIO_FAULT_PERMILLE=30") ;;
                scenario_transport_faults) extra=("MOQ_SCENARIO_FAULT_PERMILLE=50") ;;
                scenario_combined_faults) extra=("MOQ_SCENARIO_ALLOC_FAULT_PERMILLE=25" "MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE=40") ;;
                scenario_media_*) extra=("MOQ_SCENARIO_MUTATE_PERMILLE=500") ;;
            esac
            run_one "$r" 10000 "$ASAN_DIR" "asan" "${extra[@]}"
        done
        ;;
    *)
        sc_die "unknown profile: $profile (valid: quick, standard, nightly)"
        ;;
    esac

    # A runner filter that matches no entry in this profile selects nothing.
    # Report it as an error rather than a misleading 0-of-0 success.
    if [[ $total -eq 0 ]]; then
        if [[ -n "$RUNNER_FILTER" ]]; then
            sc_die "runner '$RUNNER_FILTER' is not part of profile '$profile'"
        fi
        sc_die "profile '$profile' selected no entries"
    fi

    echo ""
    echo "=== Summary: $passed passed, $failed failed, $skipped skipped (of $total selected) ==="
    if [[ -n "$LOG_DIR" ]]; then
        echo "Logs: $LOG_DIR"
    fi
    if [[ $failed -gt 0 ]]; then exit 1; fi
}

# -- Main ------------------------------------------------------------

if [[ -n "$PROFILE" ]]; then
    # Validate the runner filter up front so an unknown runner fails loudly
    # instead of silently selecting nothing and reporting 0-of-0 success.
    if [[ -n "$RUNNER_FILTER" ]]; then
        sc_validate_runner "$RUNNER_FILTER"
    fi
    run_profile "$PROFILE"
elif [[ -n "$RUNNER_FILTER" && -n "$SEEDS_OVERRIDE" ]]; then
    sc_validate_runner "$RUNNER_FILTER"
    local_extra=()
    if [[ -n "$FAULT_PERMILLE" ]]; then
        local_extra+=("MOQ_SCENARIO_FAULT_PERMILLE=$FAULT_PERMILLE")
    fi
    if [[ -n "$ALLOC_FAULT_PERMILLE" ]]; then
        local_extra+=("MOQ_SCENARIO_ALLOC_FAULT_PERMILLE=$ALLOC_FAULT_PERMILLE")
    fi
    if [[ -n "$TRANSPORT_FAULT_PERMILLE" ]]; then
        local_extra+=("MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE=$TRANSPORT_FAULT_PERMILLE")
    fi
    run_entry "$RUNNER_FILTER" "$SEEDS_OVERRIDE" "$BUILD_DIR" "custom" "${local_extra[@]}"
    if [[ -n "$LOG_DIR" ]]; then
        echo "Logs: $LOG_DIR"
    fi
else
    usage 1
fi
