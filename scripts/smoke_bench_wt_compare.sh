#!/usr/bin/env bash
#
# Tiny correctness/availability run of the WebTransport transport
# comparison benchmark (not a performance gate). Generates throwaway
# self-signed certs and runs both backends with a small workload on a
# randomized port.
#
# Usage: smoke_bench_wt_compare.sh <path-to-moq_bench_wt_compare> [port]

set -euo pipefail

BENCH="${1:?usage: smoke_bench_wt_compare.sh <path-to-moq_bench_wt_compare> [port]}"
PORT="${2:-$((24400 + (RANDOM % 1000)))}"
DIR="$(mktemp -d)"
trap 'rm -rf "$DIR"' EXIT

openssl req -x509 -newkey rsa:2048 -sha256 -days 2 -nodes \
    -keyout "$DIR/key.pem" -out "$DIR/cert.pem" \
    -subj /CN=localhost 2>/dev/null

"$BENCH" --backend both --objects 8 --object-size 64 --warmup 0 \
    --port "$PORT" --cert "$DIR/cert.pem" --key "$DIR/key.pem"
echo "smoke_bench_wt_compare: OK"
