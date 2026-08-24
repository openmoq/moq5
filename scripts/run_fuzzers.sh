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
SELF_TEST=0

usage() {
    cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --build-dir DIR        Build tree with the fuzz targets (default: build/fuzz-ci)
  --max-total-time SECS  Wall-clock seconds to fuzz each target (default: 60)
  --artifacts DIR        Where new corpus, logs, and reproducers go
                         (default: fuzz-artifacts)
  --self-test            Run the script's own regression (stub binaries, no
                         real fuzzing) and exit; must pass on Bash 3.2
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
        --self-test)      SELF_TEST=1; shift;;
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

# Deterministic regression: drive the real per-target loop against stub
# binaries and prove every target actually runs - including the seedless ones,
# whose empty seed_args expansion aborted under `set -u` on Bash 3.2 before the
# fix. Runs no real fuzzing. Kept in-script (the reduce_scenario.sh --self-test
# convention) so `bash scripts/run_fuzzers.sh --self-test` is the whole check,
# and it must pass on the same Bash 3.2 that the bug was invisible to on Linux.
run_self_test() {
    local tmp stubdir art t rc fail out seed a1 a2 exp1 n c opt
    tmp="$(mktemp -d "${TMPDIR:-/tmp}/run_fuzzers_selftest_XXXXXX")"
    # Never leave the temporary tree behind on interrupt or failure.
    trap 'rm -rf "$tmp"' EXIT INT TERM
    stubdir="$tmp/build/fuzz"
    art="$tmp/artifacts"
    mkdir -p "$stubdir"
    fail=0

    # Each stub records its COMPLETE argv (one word per line - fuzzer paths and
    # options contain no newlines) plus one line per invocation, so the harness
    # can assert exactly what each target was invoked with, not merely that it
    # ran.
    for t in "${TARGETS[@]}"; do
        cat > "$stubdir/$t" <<STUBEOF
#!/bin/sh
printf '%s\n' "\$@" > "$tmp/argv_$t"
echo 1 >> "$tmp/count_$t"
exit 0
STUBEOF
        chmod +x "$stubdir/$t"
    done

    # Re-invoke under the SAME interpreter running this self-test, not via the
    # shebang - `#!/usr/bin/env bash` would resolve to whatever bash is first
    # on PATH (often a newer one), hiding the very Bash 3.2 regression this
    # guards. `$BASH` is the running shell's own path.
    rc=0
    out="$tmp/out.log"
    "$BASH" "$0" --build-dir "$tmp/build" --max-total-time 0 --artifacts "$art" \
        >"$out" 2>&1 || rc=$?

    if [ "$rc" -eq 0 ]; then
        echo "PASS: self-test run exited 0"
    else
        echo "FAIL: self-test run exited $rc (expected 0)"
        echo "  --- captured output ---"
        sed 's/^/  /' "$out"
        fail=1
    fi

    if grep -q "unbound variable" "$out"; then
        echo "FAIL: 'unbound variable' abort (empty seed_args regression)"
        fail=1
    else
        echo "PASS: no unbound-variable abort"
    fi

    # Per-target argv assertions. The sub-invocation resolved --artifacts to an
    # absolute path, so the target's own out_corpus is "$art/corpus/$t" and its
    # artifact_prefix is "$art/$t/"; --max-total-time was 0.
    for t in "${TARGETS[@]}"; do
        # Executed exactly once.
        n=0
        [ -f "$tmp/count_$t" ] && n="$(wc -l < "$tmp/count_$t" | tr -d ' ')"
        if [ "$n" != 1 ]; then
            echo "FAIL: $t executed $n time(s) (expected 1)"
            fail=1
            continue
        fi
        if [ ! -f "$tmp/argv_$t" ]; then
            echo "FAIL: $t recorded no argv"
            fail=1
            continue
        fi

        # Argument 1 is always the output corpus.
        exp1="$art/corpus/$t"
        a1="$(sed -n '1p' "$tmp/argv_$t")"
        if [ "$a1" != "$exp1" ]; then
            echo "FAIL: $t arg1='$a1' (expected output corpus '$exp1')"
            fail=1
        fi

        # Argument 2: seeded targets get exactly their seed dir as the second
        # positional corpus; seedless targets get an option, no second corpus.
        seed="$(seed_for "$t")"
        a2="$(sed -n '2p' "$tmp/argv_$t")"
        if [ -n "$seed" ]; then
            if [ "$a2" = "$CORPUS_ROOT/$seed" ]; then
                echo "PASS: $t seeded with '$seed' as arg2"
            else
                echo "FAIL: $t arg2='$a2' (expected seed '$CORPUS_ROOT/$seed')"
                fail=1
            fi
        else
            case "$a2" in
                -*) echo "PASS: $t seedless (arg2 is an option, no 2nd corpus)";;
                *)  echo "FAIL: $t seedless but arg2='$a2' is a positional corpus"
                    fail=1;;
            esac
        fi

        # The three libFuzzer options appear exactly once with declared values.
        for opt in "-max_total_time=0" "-print_final_stats=1" \
                   "-artifact_prefix=$art/$t/"; do
            c="$(grep -cxF -- "$opt" "$tmp/argv_$t" || true)"
            if [ "$c" != 1 ]; then
                echo "FAIL: $t option '$opt' count=$c (expected 1)"
                fail=1
            fi
        done
    done

    if [ "$fail" -ne 0 ]; then
        echo "SELF-TEST FAILED"
        exit 1
    fi
    echo "SELF-TEST PASSED (Bash ${BASH_VERSINFO[0]}.${BASH_VERSINFO[1]}," \
         "${#TARGETS[@]} targets, argv verified)"
    exit 0
}

if [ "$SELF_TEST" -eq 1 ]; then
    run_self_test
fi

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
    #
    # Build the invocation explicitly per seeded/unseeded case. Expanding an
    # empty "${seed_args[@]}" aborts under `set -u` on Bash 3.2 (the macOS
    # system shell), where a zero-element array reads as unset; Bash 4.4+
    # expands it to nothing. Branching keeps nounset intact on both, and every
    # target - not just the two with a committed corpus - is actually fuzzed.
    if [ "${#seed_args[@]}" -gt 0 ]; then
        run_cmd=("$bin" "$out_corpus" "${seed_args[@]}")
    else
        run_cmd=("$bin" "$out_corpus")
    fi
    if "${run_cmd[@]}" \
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
