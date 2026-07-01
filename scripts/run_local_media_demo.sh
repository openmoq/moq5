#!/usr/bin/env bash
#
# Run the local media demo: publisher → relay → client.
#
# Topology (per pass):
#   publisher (server, PUB_PORT)
#     ← relay (client upstream, server downstream on RELAY_PORT)
#       ← client (client, connects to RELAY_PORT)
#
# Usage:
#   scripts/run_local_media_demo.sh [build-dir] [passes]
#
# Defaults: build-dir=build/pq, passes=3.
# Ports are auto-assigned from a PID-based high range to avoid
# collisions in parallel CI.
# Requires OpenSSL for temporary certificate generation.

set -euo pipefail

BUILD_DIR="${1:-build/pq}"
PASSES="${2:-3}"
FRAMES=60
MIN_CLIENT=20
CLIENT_TIMEOUT=20

PUB_BIN="$BUILD_DIR/examples/local-media/moq_demo_publisher"
RELAY_BIN="$BUILD_DIR/examples/local-media/moq_demo_relay"
CLIENT_BIN="$BUILD_DIR/examples/local-media/moq_demo_client"

for bin in "$PUB_BIN" "$RELAY_BIN" "$CLIENT_BIN"; do
    if [ ! -x "$bin" ]; then
        echo "FAIL: $bin not found in $BUILD_DIR"
        echo "Build with -DMOQ_BUILD_ADAPTER_PICOQUIC=ON -DMOQ_BUILD_LOC=ON -DMOQ_BUILD_MSF=ON -DMOQ_BUILD_CMAF=ON first."
        exit 1
    fi
done

BASE_PORT=$(( ($$ % 8000) + 10000 ))

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -keyout "$TMPDIR/key.pem" -out "$TMPDIR/cert.pem" \
    -days 1 -nodes -subj '/CN=localhost' 2>/dev/null

echo "=== Local Media Demo ($PASSES passes, $FRAMES frames/pass) ==="
echo ""

TOTAL_FAILURES=0

for PASS in $(seq 1 "$PASSES"); do
    PUB_PORT=$((BASE_PORT + PASS * 2))
    RELAY_PORT=$((BASE_PORT + PASS * 2 + 1))
    LOGDIR="$TMPDIR/pass$PASS"
    mkdir -p "$LOGDIR"

    echo "--- Pass $PASS (pub=$PUB_PORT relay=$RELAY_PORT) ---"

    PUB_PID=""
    RELAY_PID=""
    CLIENT_PID=""
    PASS_OK=true

    # 1. Publisher (QUIC server).
    "$PUB_BIN" "$TMPDIR/cert.pem" "$TMPDIR/key.pem" "$PUB_PORT" "$FRAMES" \
        2>"$LOGDIR/pub.log" &
    PUB_PID=$!
    sleep 1

    if ! kill -0 "$PUB_PID" 2>/dev/null; then
        echo "FAIL: publisher exited early"
        cat "$LOGDIR/pub.log"
        PASS_OK=false
    fi

    # 2. Relay.
    if $PASS_OK; then
        "$RELAY_BIN" "$TMPDIR/cert.pem" "$TMPDIR/key.pem" \
            localhost "$PUB_PORT" "$RELAY_PORT" \
            2>"$LOGDIR/relay.log" &
        RELAY_PID=$!
        sleep 0.5

        if ! kill -0 "$RELAY_PID" 2>/dev/null; then
            echo "FAIL: relay exited early"
            cat "$LOGDIR/relay.log"
            PASS_OK=false
        fi
    fi

    # 3. Client.
    CLIENT_RC=0
    if $PASS_OK; then
        "$CLIENT_BIN" localhost "$RELAY_PORT" "$MIN_CLIENT" \
            2>"$LOGDIR/client.log" &
        CLIENT_PID=$!

        ELAPSED=0
        while kill -0 "$CLIENT_PID" 2>/dev/null; do
            sleep 1
            ELAPSED=$((ELAPSED + 1))
            if [ "$ELAPSED" -ge "$CLIENT_TIMEOUT" ]; then
                echo "FAIL: client timed out after ${CLIENT_TIMEOUT}s"
                kill "$CLIENT_PID" 2>/dev/null || true
                CLIENT_RC=124
                break
            fi
        done
        if [ "$CLIENT_RC" -eq 0 ]; then
            wait "$CLIENT_PID" 2>/dev/null || CLIENT_RC=$?
        fi
        CLIENT_PID=""
    fi

    # Stop processes.
    [ -n "$RELAY_PID" ] && { kill "$RELAY_PID" 2>/dev/null || true; wait "$RELAY_PID" 2>/dev/null || true; }
    [ -n "$PUB_PID" ] && { kill "$PUB_PID" 2>/dev/null || true; wait "$PUB_PID" 2>/dev/null || true; }
    RELAY_PID=""
    PUB_PID=""

    # Validate.
    if $PASS_OK; then
        if [ "$CLIENT_RC" -ne 0 ]; then
            echo "FAIL: client exited with code $CLIENT_RC"
            PASS_OK=false
        fi

        if ! grep -q 'PASS:' "$LOGDIR/client.log"; then
            echo "FAIL: client did not report PASS"
            PASS_OK=false
        fi

        if ! grep -q 'relay: fwd g=' "$LOGDIR/relay.log"; then
            echo "FAIL: relay did not forward any objects"
            PASS_OK=false
        fi

        FWD_COUNT=$(grep -c 'relay: fwd g=' "$LOGDIR/relay.log" || true)
        if [ "$FWD_COUNT" -lt "$MIN_CLIENT" ]; then
            echo "FAIL: relay forwarded $FWD_COUNT, expected >= $MIN_CLIENT"
            PASS_OK=false
        fi

        if ! grep -q 'KEY' "$LOGDIR/client.log"; then
            echo "FAIL: client did not observe any keyframes"
            PASS_OK=false
        fi

        if ! grep -q 'catalog retained for Joining FETCH' "$LOGDIR/pub.log"; then
            echo "FAIL: publisher did not emit catalog"
            PASS_OK=false
        fi

        if ! grep -q 'catalog.*parsed\|catalog: discovered' "$LOGDIR/client.log"; then
            echo "FAIL: client did not parse catalog"
            PASS_OK=false
        fi

        if ! grep -q 'discovered video track' "$LOGDIR/client.log"; then
            echo "FAIL: client did not discover video track"
            PASS_OK=false
        fi

        if ! grep -q 'initData decoded' "$LOGDIR/client.log"; then
            echo "FAIL: client did not decode initData"
            PASS_OK=false
        fi

        if ! grep -q 'packaging=cmaf' "$LOGDIR/client.log"; then
            echo "FAIL: client did not validate CMAF packaging"
            PASS_OK=false
        fi

        if ! grep -q 'playback pipeline created' "$LOGDIR/client.log"; then
            echo "FAIL: client did not create playback pipeline"
            PASS_OK=false
        fi

        if ! grep -q 'playback configure:' "$LOGDIR/client.log"; then
            echo "FAIL: client did not receive playback CONFIGURE_VIDEO"
            PASS_OK=false
        fi

        if ! grep -q 'playback decode:' "$LOGDIR/client.log"; then
            echo "FAIL: client did not receive playback DECODE_CMAF"
            PASS_OK=false
        fi

        if ! grep -q 'playback errors:.*0' "$LOGDIR/client.log"; then
            echo "FAIL: client reported playback errors"
            PASS_OK=false
        fi
    fi

    if $PASS_OK; then
        RX_COUNT=$(grep -c 'playback decode:' "$LOGDIR/client.log" || true)
        echo "PASS: pass $PASS (relay_fwd=$FWD_COUNT, client_decode=$RX_COUNT)"
        # Surface the relay's optional single-shard pool report (MOQ_DEMO_RELAY_POOL=1).
        # The env is inherited by the backgrounded relay; its report is on stderr.
        if [ "${MOQ_DEMO_RELAY_POOL:-}" = "1" ]; then
            grep -A2 'pool ENABLED' "$LOGDIR/relay.log" | sed 's/^/  /' || true
        fi
    else
        echo ""
        [ -f "$LOGDIR/client.log" ] && { echo "--- client log ---"; cat "$LOGDIR/client.log"; }
        [ -f "$LOGDIR/pub.log" ] && { echo "--- publisher log ---"; cat "$LOGDIR/pub.log"; }
        [ -f "$LOGDIR/relay.log" ] && { echo "--- relay log ---"; cat "$LOGDIR/relay.log"; }
        echo ""
        echo "FAILED: pass $PASS"
        TOTAL_FAILURES=$((TOTAL_FAILURES + 1))
    fi
done

echo ""
if [ "$TOTAL_FAILURES" -gt 0 ]; then
    echo "FAILED: $TOTAL_FAILURES/$PASSES passes failed"
    exit 1
fi

echo "PASS: local_media_demo ($PASSES/$PASSES passes)"
