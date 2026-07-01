#!/usr/bin/env bash
#
# Smoke test for picoquic publisher/subscriber examples.
#
# Usage:
#   scripts/smoke_picoquic_examples.sh [build-dir] [port]
#
# Defaults: build-dir=build/pq, port=4433.
# Requires OpenSSL for temporary certificate generation.
# Not part of default ctest — run manually or in CI after picoquic build.

set -euo pipefail

BUILD_DIR="${1:-build/pq}"
PORT="${2:-4433}"

PUB="$BUILD_DIR/examples/picoquic/moq_publisher_example"
SUB="$BUILD_DIR/examples/picoquic/moq_subscriber_example"

if [ ! -x "$PUB" ] || [ ! -x "$SUB" ]; then
    echo "SKIP: publisher or subscriber binary not found in $BUILD_DIR"
    echo "Build with -DMOQ_BUILD_ADAPTER_PICOQUIC=ON first."
    exit 0
fi

TMPDIR=$(mktemp -d)
PUB_PID=""
cleanup() {
    [ -n "$PUB_PID" ] && kill "$PUB_PID" 2>/dev/null || true
    wait 2>/dev/null || true
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

# Generate ephemeral test certificates.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -keyout "$TMPDIR/key.pem" -out "$TMPDIR/cert.pem" \
    -days 1 -nodes -subj '/CN=localhost' 2>/dev/null

# Start publisher on the specified port.
"$PUB" "$TMPDIR/cert.pem" "$TMPDIR/key.pem" "$PORT" 2>"$TMPDIR/pub.log" &
PUB_PID=$!
sleep 1

# Start subscriber with short timeout.
timeout 5 "$SUB" localhost "$PORT" 2>"$TMPDIR/sub.log" || true

# Stop publisher.
kill "$PUB_PID" 2>/dev/null || true
wait "$PUB_PID" 2>/dev/null || true
PUB_PID=""

# Check results.
FAILURES=0

if ! grep -q 'subscribed - track active' "$TMPDIR/sub.log"; then
    echo "FAIL: subscriber never reached active state"
    FAILURES=$((FAILURES + 1))
fi

OBJ_COUNT=$(grep -c 'object: g=' "$TMPDIR/sub.log" || true)
if [ "$OBJ_COUNT" -lt 2 ]; then
    echo "FAIL: expected at least 2 objects, got $OBJ_COUNT"
    FAILURES=$((FAILURES + 1))
fi

if ! grep -q '\[props=[1-9]' "$TMPDIR/sub.log"; then
    echo "FAIL: no objects received with properties"
    FAILURES=$((FAILURES + 1))
fi

if ! grep -q 'sent priority update to 200' "$TMPDIR/sub.log"; then
    echo "FAIL: subscriber did not send priority update"
    FAILURES=$((FAILURES + 1))
fi

if ! grep -q 'subscriber update: priority=200' "$TMPDIR/pub.log"; then
    echo "FAIL: publisher did not observe subscriber update"
    FAILURES=$((FAILURES + 1))
fi

if [ "$FAILURES" -gt 0 ]; then
    echo ""
    echo "--- subscriber log ---"
    cat "$TMPDIR/sub.log"
    echo "--- publisher log ---"
    cat "$TMPDIR/pub.log"
    echo ""
    echo "FAILED: $FAILURES check(s)"
    exit 1
fi

echo "PASS: smoke_picoquic_examples ($OBJ_COUNT objects, props ok, update ok)"
