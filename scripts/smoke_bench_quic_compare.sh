#!/usr/bin/env bash
#
# Tiny correctness/availability run of the raw MoQ-over-QUIC transport
# comparison benchmark (not a performance gate). Generates throwaway
# self-signed certs and runs both backends with a small workload; the
# pq_threaded listener uses a randomized port (msquic_managed always
# binds port 0).
#
# Usage: smoke_bench_quic_compare.sh <path-to-moq_bench_quic_compare> [port]
# MOQ_BENCH_BACKEND overrides the backend selection (default: both).

set -euo pipefail

BENCH="${1:?usage: smoke_bench_quic_compare.sh <path-to-moq_bench_quic_compare> [port]}"
PORT="${2:-$((25400 + (RANDOM % 1000)))}"
DIR="$(mktemp -d)"
trap 'rm -rf "$DIR"' EXIT

openssl req -x509 -newkey rsa:2048 -sha256 -days 2 -nodes \
    -keyout "$DIR/key.pem" -out "$DIR/cert.pem" \
    -subj /CN=localhost 2>/dev/null

"$BENCH" --backend "${MOQ_BENCH_BACKEND:-both}" \
    --objects 8 --object-size 64 --warmup 0 \
    --port "$PORT" --cert "$DIR/cert.pem" --key "$DIR/key.pem"
echo "smoke_bench_quic_compare: OK"
