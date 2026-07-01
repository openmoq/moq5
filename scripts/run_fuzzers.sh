#!/usr/bin/env bash
#
# Run every libFuzzer target for a bounded time each.
#
# Used by the nightly long-fuzz workflow (.github/workflows/fuzz-nightly.yml)
# and for local short runs. This script builds nothing - point it at a build
# tree that already has the fuzz targets:
#
#   CC=clang cmake -B build/fuzz-ci -DMOQ_BUILD_FUZZ=ON -DMOQ_REQUIRE_FUZZ=ON
#   cmake --build build/fuzz-ci
#   scripts/run_fuzzers.sh --build-dir build/fuzz-ci --max-total-time 60
#
# Each target fuzzes for --max-total-time seconds, seeded from its committed
# corpus where one exists. New inputs, per-target logs, and any crash / oom /
# timeout reproducers land under --artifacts so CI can upload them on failure.
# Every target runs even if an earlier one crashes; the script exits non-zero
# if any target found a problem.
#
# Usage:
#   scripts/run_fuzzers.sh [--build-dir DIR] [--max-total-time SECS]
#                          [--artifacts DIR] [--help]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BUILD_DIR="build/fuzz-ci"
MAX_TOTAL_TIME="60"
ARTIFACTS_DIR="fuzz-artifacts"

usage() {
    cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --build-dir DIR        Build tree with the fuzz targets (default: build/fuzz-ci)
  --max-total-time SECS  Wall-clock seconds to fuzz each target (default: 60)
  --artifacts DIR        Where new corpus, logs, and reproducers go
                         (default: fuzz-artifacts)
  --help                 Show this help

Relative paths are resolved against the repo root ($REPO_ROOT).
EOF
    exit "${1:-0}"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --build-dir)      BUILD_DIR="$2"; shift 2;;
        --max-total-time) MAX_TOTAL_TIME="$2"; shift 2;;
        --artifacts)      ARTIFACTS_DIR="$2"; shift 2;;
        --help|-h)        usage 0;;
        *) echo "unknown argument: $1" >&2; usage 1;;
    esac
done

# Defense-in-depth: --max-total-time is forwarded to libFuzzer as
# -max_total_time="$MAX_TOTAL_TIME". Reject anything that is not a plain
# non-negative integer so a bad (or hostile) value fails fast here rather than
# reaching the fuzzer, and so callers cannot smuggle shell-looking text through.
case "$MAX_TOTAL_TIME" in
    ''|*[!0-9]*)
        echo "invalid --max-total-time: '$MAX_TOTAL_TIME' (expected a non-negative integer)" >&2
        exit 2;;
esac

# Resolve relative paths against the repo root so the script works from anywhere.
case "$BUILD_DIR"     in /*) ;; *) BUILD_DIR="$REPO_ROOT/$BUILD_DIR";; esac
case "$ARTIFACTS_DIR" in /*) ;; *) ARTIFACTS_DIR="$REPO_ROOT/$ARTIFACTS_DIR";; esac

FUZZ_BIN_DIR="$BUILD_DIR/fuzz"
CORPUS_ROOT="$REPO_ROOT/fuzz/corpus"

# All libFuzzer targets defined in fuzz/CMakeLists.txt.
TARGETS=(
    fuzz_quic_varint
    fuzz_kvp_decode
    fuzz_control_d16
    fuzz_session_control
    fuzz_session_data
)

# Committed seed corpus subdir for a target, or empty if none exists.
seed_for() {
    case "$1" in
        fuzz_session_control) echo "session_control";;
        fuzz_session_data)    echo "session_data";;
        *)                    echo "";;
    esac
}

mkdir -p "$ARTIFACTS_DIR"
failed=()

for t in "${TARGETS[@]}"; do
    bin="$FUZZ_BIN_DIR/$t"
    if [ ! -x "$bin" ]; then
        echo "MISSING: $bin" >&2
        echo "  (configure with -DMOQ_BUILD_FUZZ=ON -DMOQ_REQUIRE_FUZZ=ON and build first)" >&2
        failed+=("$t")
        continue
    fi

    out_corpus="$ARTIFACTS_DIR/corpus/$t"
    art_dir="$ARTIFACTS_DIR/$t"
    log="$ARTIFACTS_DIR/$t.log"
    mkdir -p "$out_corpus" "$art_dir"

    seed_args=()
    seed="$(seed_for "$t")"
    if [ -n "$seed" ] && [ -d "$CORPUS_ROOT/$seed" ]; then
        seed_args=("$CORPUS_ROOT/$seed")
    fi

    echo "=== fuzzing $t for ${MAX_TOTAL_TIME}s ==="
    # libFuzzer: the first dir collects new inputs; any later dirs are
    # read-only seeds. artifact_prefix must end in '/' so crash-*/oom-*/
    # timeout-* reproducers are written inside the per-target artifact dir.
    if "$bin" "$out_corpus" "${seed_args[@]}" \
            -max_total_time="$MAX_TOTAL_TIME" \
            -print_final_stats=1 \
            -artifact_prefix="$art_dir/" 2>&1 | tee "$log"; then
        echo "--- $t: ok"
    else
        echo "--- $t: FAILED (see $log and $art_dir)" >&2
        failed+=("$t")
    fi
done

if [ "${#failed[@]}" -gt 0 ]; then
    echo "Fuzzing found problems in: ${failed[*]}" >&2
    exit 1
fi

echo "All ${#TARGETS[@]} fuzz targets completed cleanly."
