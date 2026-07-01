# Shared helpers for scenario replay/reduce tools.
# Source this file; do not execute directly.

SCENARIO_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCENARIO_ROOT="$(cd "$SCENARIO_SCRIPT_DIR/.." && pwd)"

VALID_RUNNERS=(
    scenario_auth
    scenario_lifecycle
    scenario_object
    scenario_streaming_object
    scenario_streaming_faults
    scenario_publisher
    scenario_faults
    scenario_transport_faults
    scenario_combined_faults
    scenario_goaway
    scenario_backpressure
    scenario_request_credit
    scenario_namespace_sub
    scenario_fetch
    scenario_publish
    scenario_track_status
    scenario_object_datagram
    scenario_delay
    scenario_crossed
    scenario_delay_backpressure
    # Seeded media-conformance runners (generate-only by default; mutation is
    # opt-in via MOQ_SCENARIO_MUTATE_PERMILLE). Built only when the media libs
    # are enabled, so sweeps skip them gracefully when absent.
    scenario_media_loc
    scenario_media_msf
    scenario_media_cmaf
)

sc_die() { echo "ERROR: $*" >&2; exit 1; }

sc_is_format_a() {
    for arg in "$@"; do
        case "$arg" in
            --*) return 1 ;;
        esac
    done
    return 0
}

# Globals set by sc_parse_env_token / sc_parse_format_b:
SC_SEED_START=""
SC_SEEDS=""
SC_STEPS=""
SC_FAULT_PERMILLE=""
SC_ALLOC_FAULT_PERMILLE=""
SC_TRANSPORT_FAULT_PERMILLE=""
SC_MUTATE_PERMILLE=""
SC_BUILD_DIR=""
SC_RUNNER=""
SC_BINARY_PATH=""

sc_parse_env_token() {
    local token="$1"
    case "$token" in
        MOQ_SCENARIO_SEED_START=*)
            SC_SEED_START="${token#*=}" ;;
        MOQ_SCENARIO_SEEDS=*)
            SC_SEEDS="${token#*=}" ;;
        MOQ_SCENARIO_STEPS=*)
            SC_STEPS="${token#*=}" ;;
        MOQ_SCENARIO_FAULT_PERMILLE=*)
            SC_FAULT_PERMILLE="${token#*=}" ;;
        MOQ_SCENARIO_ALLOC_FAULT_PERMILLE=*)
            SC_ALLOC_FAULT_PERMILLE="${token#*=}" ;;
        MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE=*)
            SC_TRANSPORT_FAULT_PERMILLE="${token#*=}" ;;
        MOQ_SCENARIO_MUTATE_PERMILLE=*)
            SC_MUTATE_PERMILLE="${token#*=}" ;;
        */test_scenario_*)
            SC_BINARY_PATH="$token" ;;
        *)
            sc_die "unrecognized token: $token" ;;
    esac
}

sc_parse_format_a() {
    local tokens=()
    for arg in "$@"; do
        # shellcheck disable=SC2206
        tokens+=($arg)
    done
    for token in "${tokens[@]}"; do
        case "$token" in
            Replay:|"\\") continue ;;
            *) sc_parse_env_token "$token" ;;
        esac
    done
    if [[ -z "$SC_BINARY_PATH" ]]; then
        sc_die "no runner binary path found in arguments"
    fi
    if [[ ! -x "$SC_BINARY_PATH" ]]; then
        if [[ -x "$SCENARIO_ROOT/$SC_BINARY_PATH" ]]; then
            SC_BINARY_PATH="$SCENARIO_ROOT/$SC_BINARY_PATH"
        else
            sc_die "binary not found: $SC_BINARY_PATH"
        fi
    fi
}

sc_runner_to_binary() { echo "test_${1}"; }

sc_validate_runner() {
    local name="$1"
    for r in "${VALID_RUNNERS[@]}"; do
        if [[ "$r" == "$name" ]]; then return 0; fi
    done
    echo "ERROR: unknown runner '$name'" >&2
    echo "Valid runners: ${VALID_RUNNERS[*]}" >&2
    exit 1
}

# Largest value the scenario scripts accept where it later feeds bash
# arithmetic. Step counts are tiny in practice (default 50); 10^9 is far
# beyond any real scenario yet sits well inside Bash's signed 64-bit
# arithmetic domain, so (( lo < hi )) etc. can never wrap.
SC_UINT_MAX=1000000000

# Validate a non-negative decimal integer before it is used in bash
# arithmetic. Without this, a crafted value such as
# 'x[$(rm -rf ~)+0]' would execute its command substitution inside (( )).
# Rejects empty, signed, hex, expressions, array syntax, and whitespace.
# Also bounds the magnitude: a value above the signed-64-bit domain (e.g.
# 9223372036854775808) passes ^[0-9]+$ but wraps negative in (( )), which
# would silently corrupt a binary search. The digit-length check runs
# before any arithmetic so a huge value cannot wrap during the check itself.
# $1 = caller-facing name (for the error), $2 = value.
sc_validate_uint() {
    local name="$1" value="$2"
    if [[ ! "$value" =~ ^[0-9]+$ ]]; then
        sc_die "$name must be a non-negative decimal integer"
    fi
    if (( ${#value} > 10 )) || (( 10#$value > SC_UINT_MAX )); then
        sc_die "$name must be between 0 and $SC_UINT_MAX"
    fi
}

sc_find_binary() {
    local binary_name="$1"
    if [[ -n "${SC_BUILD_DIR:-}" ]]; then
        local path="$SC_BUILD_DIR/tests/$binary_name"
        if [[ -x "$path" ]]; then echo "$path"; return 0; fi
        sc_die "binary not found: $path (run cmake --build $SC_BUILD_DIR first)"
    fi
    local search_dirs=("$SCENARIO_ROOT/build" "$SCENARIO_ROOT/build/asan" "$SCENARIO_ROOT/build/fuzz-check")
    for dir in "${search_dirs[@]}"; do
        local path="$dir/tests/$binary_name"
        if [[ -x "$path" ]]; then echo "$path"; return 0; fi
    done
    sc_die "binary '$binary_name' not found in build/tests/, build/asan/tests/, or build/fuzz-check/tests/. Run cmake --build first."
}

sc_build_env_vars() {
    SC_ENV_VARS=()
    if [[ -n "$SC_SEED_START" ]]; then
        SC_ENV_VARS+=("MOQ_SCENARIO_SEED_START=$SC_SEED_START")
    fi
    SC_ENV_VARS+=("MOQ_SCENARIO_SEEDS=${SC_SEEDS:-1}")
    SC_ENV_VARS+=("MOQ_SCENARIO_STEPS=${SC_STEPS:-50}")
    if [[ -n "$SC_FAULT_PERMILLE" ]]; then
        SC_ENV_VARS+=("MOQ_SCENARIO_FAULT_PERMILLE=$SC_FAULT_PERMILLE")
    fi
    if [[ -n "$SC_ALLOC_FAULT_PERMILLE" ]]; then
        SC_ENV_VARS+=("MOQ_SCENARIO_ALLOC_FAULT_PERMILLE=$SC_ALLOC_FAULT_PERMILLE")
    fi
    if [[ -n "$SC_TRANSPORT_FAULT_PERMILLE" ]]; then
        SC_ENV_VARS+=("MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE=$SC_TRANSPORT_FAULT_PERMILLE")
    fi
    if [[ -n "$SC_MUTATE_PERMILLE" ]]; then
        SC_ENV_VARS+=("MOQ_SCENARIO_MUTATE_PERMILLE=$SC_MUTATE_PERMILLE")
    fi
}
