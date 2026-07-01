#!/usr/bin/env bash
#
# Smoke test for moq_bench_picoquic_loopback.
# Generates ephemeral TLS certs and runs a tiny benchmark.
#
# Usage: smoke_bench_picoquic.sh <build-dir> [port]

set -euo pipefail

BUILD_DIR="${1:?usage: smoke_bench_picoquic.sh <build-dir> [port]}"
PORT="${2:-$((14400 + (RANDOM % 1000)))}"

BIN="$BUILD_DIR/benchmarks/moq_bench_picoquic_loopback"

if [ ! -x "$BIN" ]; then
    echo "SKIP: $BIN not found"
    exit 0
fi

TMPDIR=$(mktemp -d)
cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -keyout "$TMPDIR/key.pem" -out "$TMPDIR/cert.pem" \
    -days 1 -nodes -subj '/CN=localhost' 2>/dev/null

"$BIN" --cert "$TMPDIR/cert.pem" --key "$TMPDIR/key.pem" \
    --objects 8 --object-size 64 --warmup 0 --port "$PORT"
