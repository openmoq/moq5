#!/usr/bin/env bash
#
# Replay a deterministic scenario failure.
#
# Usage:
#   # Format A: paste the replay line (quoted or unquoted)
#   scripts/replay_scenario.sh MOQ_SCENARIO_SEED_START=0x36 \
#     MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=50 \
#     ./build/tests/test_scenario_auth
#
#   # Format B: explicit arguments
#   scripts/replay_scenario.sh --runner scenario_auth --seed 0x36
#
#   # With debugger
#   scripts/replay_scenario.sh --runner scenario_auth --seed 0x36 \
#     --debugger lldb

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scenario_common.sh
source "$SCRIPT_DIR/scenario_common.sh"

usage() {
    cat <<EOF
Usage:
  $(basename "$0") [ENV=val...] ./path/to/test_scenario_*
  $(basename "$0") --runner NAME --seed SEED [options]

Options:
  --runner NAME           Scenario runner name
  --seed SEED             Seed value (decimal or 0x hex)
  --seed-start SEED       Alias for --seed
  --seeds COUNT           Number of seeds (default: 1)
  --steps N               Steps per seed (default: 50)
  --fault-permille N      MOQ_SCENARIO_FAULT_PERMILLE
  --alloc-fault-permille N    MOQ_SCENARIO_ALLOC_FAULT_PERMILLE
  --transport-fault-permille N MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE
  --mutate-permille N     MOQ_SCENARIO_MUTATE_PERMILLE (seeded media runners)
  --build-dir DIR         CMake build root (default: auto-detect)
  --debugger lldb|gdb     Run under debugger
  --dry-run               Print command without executing
  --verbose               Print parsed settings before running

Valid runners: ${VALID_RUNNERS[*]}
EOF
    exit "${1:-0}"
}

DEBUGGER=""
DRY_RUN=false
VERBOSE=false

if [[ $# -eq 0 ]]; then usage 1; fi

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
            --debugger)     DEBUGGER="$2"; shift 2 ;;
            --dry-run)      DRY_RUN=true; shift ;;
            --verbose)      VERBOSE=true; shift ;;
            --help|-h)      usage 0 ;;
            *)              sc_die "unknown flag: $1" ;;
        esac
    done
    [[ -z "$SC_RUNNER" ]] && sc_die "missing --runner"
    sc_validate_runner "$SC_RUNNER"
    SC_BINARY_PATH="$(sc_find_binary "$(sc_runner_to_binary "$SC_RUNNER")")"
fi

SC_SEEDS="${SC_SEEDS:-1}"
SC_STEPS="${SC_STEPS:-50}"
sc_build_env_vars

if $VERBOSE; then
    echo "Runner:    ${SC_RUNNER:-$(basename "$SC_BINARY_PATH")}"
    echo "Binary:    $SC_BINARY_PATH"
    echo "Build dir: ${SC_BUILD_DIR:-auto}"
    echo "Seed:      ${SC_SEED_START:-0}"
    echo "Seeds:     $SC_SEEDS"
    echo "Steps:     $SC_STEPS"
    if [[ -n "$SC_FAULT_PERMILLE" ]]; then echo "Fault ‰:   $SC_FAULT_PERMILLE"; fi
    if [[ -n "$SC_ALLOC_FAULT_PERMILLE" ]]; then echo "Alloc ‰:   $SC_ALLOC_FAULT_PERMILLE"; fi
    if [[ -n "$SC_TRANSPORT_FAULT_PERMILLE" ]]; then echo "Xport ‰:   $SC_TRANSPORT_FAULT_PERMILLE"; fi
    if [[ -n "$SC_MUTATE_PERMILLE" ]]; then echo "Mutate ‰:  $SC_MUTATE_PERMILLE"; fi
    echo "---"
fi

CMD=()
if [[ -n "$DEBUGGER" ]]; then
    case "$DEBUGGER" in
        lldb) CMD+=(lldb --) ;;
        gdb)  CMD+=(gdb --args) ;;
        *)    sc_die "unknown debugger: $DEBUGGER (use lldb or gdb)" ;;
    esac
fi
CMD+=("$SC_BINARY_PATH")

if $DRY_RUN; then
    echo "${SC_ENV_VARS[*]} ${CMD[*]}"
    exit 0
fi

export "${SC_ENV_VARS[@]}"
exec "${CMD[@]}"
