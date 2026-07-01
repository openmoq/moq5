#!/usr/bin/env bash
#
# Run LibMoQ sans-I/O benchmarks and report results.
#
# Assumes benchmark binaries are already built.
#
# Usage:
#   scripts/run_benchmarks.sh [build-dir]
#
# Environment variables:
#   MOQ_BENCH_OBJECTS      objects per benchmark (default 10000)
#   MOQ_BENCH_OBJECT_SIZE  payload bytes per object (default 1200)
#   MOQ_BENCH_JSON_DIR     if set, write JSON results to this directory
#   MOQ_BENCH_WARMUP       warmup objects (default 100)

set -euo pipefail

BUILD_DIR="${1:-build}"
BENCH_DIR="$BUILD_DIR/benchmarks"

OBJECTS="${MOQ_BENCH_OBJECTS:-10000}"
OBJECT_SIZE="${MOQ_BENCH_OBJECT_SIZE:-1200}"
WARMUP="${MOQ_BENCH_WARMUP:-100}"
JSON_DIR="${MOQ_BENCH_JSON_DIR:-}"

SUB_COUNTS="1 4 16"

# Single-session subscription counts for the alias-lookup scaling curve. Larger
# than SUB_COUNTS on purpose: the O(n) receiver-side alias scan only shows up
# with many subscriptions on one session.
LOOKUP_COUNTS="${MOQ_BENCH_LOOKUP_COUNTS:-64 256 1024}"

# Capture git commit if available.
GIT_COMMIT=""
if command -v git >/dev/null 2>&1 && git rev-parse --short HEAD >/dev/null 2>&1; then
    GIT_COMMIT="$(git rev-parse --short HEAD)"
fi

FAILED=0

run_bench() {
    local name="$1"
    shift
    local bin="$BENCH_DIR/$name"

    if [ ! -x "$bin" ]; then
        echo "SKIP: $bin not found" >&2
        return
    fi

    local json_flag=""
    local json_file=""
    if [ -n "$JSON_DIR" ]; then
        json_flag="--json"
        # Sanitize the whole arg suffix to filename-safe characters: args such
        # as --cert /path/to/cert.pem contain slashes that would otherwise be
        # read as directory separators and fail the redirect.
        json_file="$JSON_DIR/${name}$(printf ' %s' "$@" | tr -c 'A-Za-z0-9._-' '_').json"
    fi

    echo "=== $name $* ==="
    if [ -n "$json_flag" ]; then
        if ! "$bin" "$@" $json_flag > "$json_file"; then
            echo "FAIL: $name $*" >&2
            FAILED=1
        else
            cat "$json_file"
        fi
    else
        if ! "$bin" "$@"; then
            echo "FAIL: $name $*" >&2
            FAILED=1
        fi
    fi
    echo ""
}

# Validate build dir.
if [ ! -d "$BENCH_DIR" ]; then
    echo "error: benchmark directory not found: $BENCH_DIR" >&2
    echo "Build with: cmake -B $BUILD_DIR -DMOQ_BUILD_BENCHMARKS=ON && cmake --build $BUILD_DIR" >&2
    exit 1
fi

# Create JSON output directory if requested.
if [ -n "$JSON_DIR" ]; then
    mkdir -p "$JSON_DIR"
    # Write metadata alongside results.
    cat > "$JSON_DIR/metadata.json" <<METADATA
{
  "commit": "${GIT_COMMIT:-unknown}",
  "objects": $OBJECTS,
  "object_size": $OBJECT_SIZE,
  "warmup": $WARMUP,
  "subscriber_counts": [$(echo $SUB_COUNTS | tr ' ' ',')],
  "lookup_counts": [$(echo $LOOKUP_COUNTS | tr ' ' ',')]
}
METADATA
fi

# Print header.
if [ -n "$GIT_COMMIT" ]; then
    echo "LibMoQ benchmarks @ $GIT_COMMIT"
    echo ""
fi

# -- Core 1:1 throughput --
run_bench moq_bench_core \
    --objects "$OBJECTS" --object-size "$OBJECT_SIZE" --warmup "$WARMUP"

# -- Session scaling --
for N in $SUB_COUNTS; do
    run_bench moq_bench_session_scale \
        --subscribers "$N" --objects "$OBJECTS" --object-size "$OBJECT_SIZE" \
        --warmup "$WARMUP"
done

# -- Process fanout --
for N in $SUB_COUNTS; do
    run_bench moq_bench_process_fanout \
        --subscribers "$N" --objects "$OBJECTS" --object-size "$OBJECT_SIZE" \
        --warmup "$WARMUP"
done

# -- Single-session alias-lookup scaling (O(n) receiver alias scan) --
for N in $LOOKUP_COUNTS; do
    run_bench moq_bench_session_lookup \
        --mode alias --subscriptions "$N" \
        --objects "$OBJECTS" --object-size "$OBJECT_SIZE" --warmup "$WARMUP"
done

# -- Steady-state subgroup receive (one subgroup, N objects) --
run_bench moq_bench_session_subgroup \
    --objects "$OBJECTS" --object-size "$OBJECT_SIZE" --warmup "$WARMUP"

# -- Zero-allocation relay fan-out (1 upstream -> N downstream, pooled) --
for N in $SUB_COUNTS; do
    run_bench moq_bench_relay_fanout \
        --fanout "$N" --objects "$OBJECTS" --object-size "$OBJECT_SIZE" \
        --warmup "$WARMUP"
done

# -- Transport: picoquic loopback (optional) --
PQ_BIN="$BENCH_DIR/moq_bench_picoquic_loopback"
if [ -x "$PQ_BIN" ]; then
    PQ_TMPDIR=$(mktemp -d)
    pq_cleanup() { rm -rf "$PQ_TMPDIR"; }
    trap pq_cleanup EXIT

    if command -v openssl >/dev/null 2>&1; then
        openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
            -keyout "$PQ_TMPDIR/key.pem" -out "$PQ_TMPDIR/cert.pem" \
            -days 1 -nodes -subj '/CN=localhost' 2>/dev/null

        PQ_PORT=$((14400 + (RANDOM % 1000)))
        run_bench moq_bench_picoquic_loopback \
            --cert "$PQ_TMPDIR/cert.pem" --key "$PQ_TMPDIR/key.pem" \
            --objects "$OBJECTS" --object-size "$OBJECT_SIZE" --warmup "$WARMUP" \
            --port "$PQ_PORT"

        rm -rf "$PQ_TMPDIR"
        trap - EXIT
    else
        echo "SKIP: openssl not found, skipping picoquic loopback" >&2
    fi
fi

if [ "$FAILED" -ne 0 ]; then
    echo "FAIL: one or more benchmarks failed" >&2
    exit 1
fi

echo "All benchmarks passed."
