#!/usr/bin/env bash
#
# Build with coverage instrumentation, run tests, and produce a
# line-coverage summary for core/src/session/.
#
# Usage:
#   scripts/run_coverage.sh                # auto-detect toolchain
#   scripts/run_coverage.sh --toolchain llvm   # force Clang/LLVM
#   scripts/run_coverage.sh --toolchain gcov   # force GCC/gcov
#   scripts/run_coverage.sh --report       # skip build/test, regenerate report
#
# Requires:
#   LLVM path: clang + llvm-profdata + llvm-cov
#   GCC path:  gcc + gcov, optionally lcov + genhtml
#
# Output: build/coverage/coverage-report/
#
# This is a local development tool, not a CI gate.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$ROOT/build/coverage"
REPORT_DIR="$BUILD_DIR/coverage-report"

# -- Argument parsing --------------------------------------------------

REPORT_ONLY=false
FORCE_TOOLCHAIN=""

while [ $# -gt 0 ]; do
    case "$1" in
        --report)     REPORT_ONLY=true; shift ;;
        --toolchain)
            if [ $# -lt 2 ]; then
                echo "ERROR: --toolchain requires an argument (llvm or gcov)."
                exit 1
            fi
            FORCE_TOOLCHAIN="$2"
            if [ "$FORCE_TOOLCHAIN" != "llvm" ] && [ "$FORCE_TOOLCHAIN" != "gcov" ]; then
                echo "ERROR: --toolchain must be 'llvm' or 'gcov', got '$FORCE_TOOLCHAIN'."
                exit 1
            fi
            shift 2 ;;
        *)            echo "Unknown option: $1"; exit 1 ;;
    esac
done

# -- Tool detection ----------------------------------------------------

find_tool() {
    local name="$1"
    if command -v xcrun >/dev/null 2>&1; then
        local path
        path="$(xcrun -f "$name" 2>/dev/null)" && [ -n "$path" ] && echo "$path" && return
    fi
    command -v "$name" 2>/dev/null && return
    echo ""
}

detect_toolchain() {
    if [ -n "$FORCE_TOOLCHAIN" ]; then
        echo "$FORCE_TOOLCHAIN"
        return
    fi
    # Check what compiler CMake would use
    local cc="${CC:-$(command -v cc 2>/dev/null || echo "")}"
    if [ -n "$cc" ]; then
        local id
        id="$($cc --version 2>&1 | head -1)" || true
        case "$id" in
            *clang*|*Clang*|*LLVM*)
                if [ -n "$(find_tool llvm-profdata)" ] && [ -n "$(find_tool llvm-cov)" ]; then
                    echo "llvm"
                    return
                fi
                ;;
            *gcc*|*GCC*)
                if command -v gcov >/dev/null 2>&1; then
                    echo "gcov"
                    return
                fi
                ;;
        esac
    fi
    # Fallback: try LLVM tools, then gcov
    if [ -n "$(find_tool llvm-profdata)" ] && [ -n "$(find_tool llvm-cov)" ]; then
        echo "llvm"
    elif command -v gcov >/dev/null 2>&1; then
        echo "gcov"
    else
        echo ""
    fi
}

TOOLCHAIN="$(detect_toolchain)"

if [ -z "$TOOLCHAIN" ]; then
    echo "ERROR: No coverage tools found."
    echo "Need either clang + llvm-profdata + llvm-cov, or gcc + gcov."
    echo "Use --toolchain llvm|gcov to force."
    exit 1
fi

echo "Toolchain: $TOOLCHAIN"

# -- Session source filter (used by both paths) ------------------------

# Only count session core + profile files for the primary report.
SESSION_FILTER='src/session/'

# -- LLVM path ---------------------------------------------------------

if [ "$TOOLCHAIN" = "llvm" ]; then
    LLVM_PROFDATA="${LLVM_PROFDATA:-$(find_tool llvm-profdata)}"
    LLVM_COV="${LLVM_COV:-$(find_tool llvm-cov)}"
    PROFDATA="$BUILD_DIR/merged.profdata"

    if [ -z "$LLVM_PROFDATA" ] || [ -z "$LLVM_COV" ]; then
        echo "ERROR: llvm-profdata and llvm-cov required for LLVM toolchain."
        exit 1
    fi

    # Find clang for the build
    CLANG_CC="${CC:-$(find_tool clang)}"
    CLANG_CXX="${CXX:-$(find_tool clang++)}"
    if [ -z "$CLANG_CC" ] || [ -z "$CLANG_CXX" ]; then
        echo "ERROR: clang/clang++ required for LLVM coverage build."
        exit 1
    fi

    if [ "$REPORT_ONLY" = false ]; then
        echo "=== Configuring coverage build (Clang/LLVM) ==="
        cmake -S "$ROOT" -B "$BUILD_DIR" \
            -DCMAKE_C_COMPILER="$CLANG_CC" \
            -DCMAKE_CXX_COMPILER="$CLANG_CXX" \
            -DCMAKE_BUILD_TYPE=Debug \
            -DCMAKE_C_FLAGS="-fprofile-instr-generate -fcoverage-mapping" \
            -DCMAKE_CXX_FLAGS="-fprofile-instr-generate -fcoverage-mapping" \
            -DCMAKE_EXE_LINKER_FLAGS="-fprofile-instr-generate" \
            -DMOQ_BUILD_TESTS=ON \
            -DMOQ_BUILD_SIM=ON \
            2>&1 | tail -5

        echo ""
        echo "=== Building ==="
        cmake --build "$BUILD_DIR" 2>&1 | tail -5

        echo ""
        echo "=== Running tests ==="
        export LLVM_PROFILE_FILE="$BUILD_DIR/profiles/%p-%m.profraw"
        mkdir -p "$BUILD_DIR/profiles"

        ctest --test-dir "$BUILD_DIR" --output-on-failure 2>&1 | tail -5

        echo ""
        echo "=== Merging profiles ==="
        "$LLVM_PROFDATA" merge -sparse "$BUILD_DIR/profiles"/*.profraw \
            -o "$PROFDATA"
    fi

    if [ ! -f "$PROFDATA" ]; then
        echo "ERROR: $PROFDATA not found. Run without --report first."
        exit 1
    fi

    OBJECTS=()
    for exe in "$BUILD_DIR"/tests/test_*; do
        [ -x "$exe" ] || continue
        OBJECTS+=("-object=$exe")
    done

    echo ""
    echo "=== Coverage report ==="
    mkdir -p "$REPORT_DIR"

    echo "--- session core line coverage ---"
    "$LLVM_COV" report \
        "${OBJECTS[@]}" \
        -instr-profile="$PROFDATA" \
        -ignore-filename-regex='tests/|sim/|examples/|_deps/|wire/|base/|facade/|include/|internal/|types\.c|version\.c' \
        2>/dev/null | tee "$REPORT_DIR/session-summary.txt"

    echo ""
    echo "--- full core/ line coverage ---"
    "$LLVM_COV" report \
        "${OBJECTS[@]}" \
        -instr-profile="$PROFDATA" \
        -ignore-filename-regex='tests/|sim/|examples/|_deps/' \
        2>/dev/null | tee "$REPORT_DIR/core-summary.txt"

    "$LLVM_COV" show \
        "${OBJECTS[@]}" \
        -instr-profile="$PROFDATA" \
        -format=html \
        -output-dir="$REPORT_DIR/html" \
        -ignore-filename-regex='tests/|_deps/' \
        2>/dev/null

    echo ""
    echo "=== Done ==="
    echo "Session summary: $REPORT_DIR/session-summary.txt"
    echo "Full core summary: $REPORT_DIR/core-summary.txt"
    echo "HTML report: $REPORT_DIR/html/index.html"

# -- GCC/gcov path -----------------------------------------------------

elif [ "$TOOLCHAIN" = "gcov" ]; then
    if [ "$REPORT_ONLY" = false ]; then
        echo "=== Configuring coverage build (GCC/gcov) ==="
        cmake -S "$ROOT" -B "$BUILD_DIR" \
            -DCMAKE_BUILD_TYPE=Debug \
            -DCMAKE_C_FLAGS="--coverage -fprofile-arcs -ftest-coverage" \
            -DCMAKE_CXX_FLAGS="--coverage -fprofile-arcs -ftest-coverage" \
            -DCMAKE_EXE_LINKER_FLAGS="--coverage" \
            -DMOQ_BUILD_TESTS=ON \
            -DMOQ_BUILD_SIM=ON \
            2>&1 | tail -5

        echo ""
        echo "=== Building ==="
        cmake --build "$BUILD_DIR" 2>&1 | tail -5

        echo ""
        echo "=== Running tests ==="
        ctest --test-dir "$BUILD_DIR" --output-on-failure 2>&1 | tail -5
    fi

    echo ""
    echo "=== Coverage report ==="
    mkdir -p "$REPORT_DIR"

    if command -v lcov >/dev/null 2>&1; then
        lcov --capture --directory "$BUILD_DIR" \
            --output-file "$REPORT_DIR/coverage.info" \
            --ignore-errors source 2>/dev/null
        lcov --remove "$REPORT_DIR/coverage.info" \
            '*/tests/*' '*/sim/*' '*/_deps/*' '*/examples/*' \
            --output-file "$REPORT_DIR/coverage-filtered.info" 2>/dev/null

        echo "--- core/ line coverage ---"
        lcov --list "$REPORT_DIR/coverage-filtered.info" 2>/dev/null \
            | tee "$REPORT_DIR/core-summary.txt"

        if command -v genhtml >/dev/null 2>&1; then
            genhtml "$REPORT_DIR/coverage-filtered.info" \
                --output-directory "$REPORT_DIR/html" 2>/dev/null
            echo ""
            echo "HTML report: $REPORT_DIR/html/index.html"
        fi
    else
        echo "lcov not found — showing gcov summary only."
        find "$BUILD_DIR" -name '*.gcno' -path '*session*' \
            -exec gcov -n {} \; 2>/dev/null \
            | grep -E 'File|Lines' | tee "$REPORT_DIR/session-summary.txt"
    fi

    echo ""
    echo "=== Done ==="
    echo "Reports in: $REPORT_DIR/"
fi
