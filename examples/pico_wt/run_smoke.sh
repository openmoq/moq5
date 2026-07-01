#!/usr/bin/env bash
#
# Localhost smoke for the pico WT examples: start moq_pico_wt_server,
# run moq_pico_wt_client against it, and assert the client received the
# published object end to end over real UDP. The server is always
# cleaned up (trap on EXIT). Both server and client output are printed
# on failure.
#
# Args: <server-bin> <client-bin> <cert> <key> <base-port>
set -u

SERVER="$1"; CLIENT="$2"; CERT="$3"; KEY="$4"; BASE_PORT="$5"

SRV_PID=""
SRV_OUT="$(mktemp)"
CLI_OUT="$(mktemp)"
cleanup() {
    if [ -n "$SRV_PID" ]; then
        kill -INT "$SRV_PID" 2>/dev/null
        wait "$SRV_PID" 2>/dev/null
    fi
    rm -f "$SRV_OUT" "$CLI_OUT"
}
trap cleanup EXIT

dump_logs() {
    echo "=== server output ==="; cat "$SRV_OUT"
    echo "=== client output ==="; cat "$CLI_OUT"
    echo "====================="
}

# Find a high port the server can actually bind. A few candidates make
# the smoke robust on a busy dev machine. The server keeps running once
# bound, so "still alive after a short wait" means the bind succeeded
# (a failed bind makes picoquic_packet_loop return immediately).
PORT=""
for off in 0 1000 2000 3000; do
    cand=$((BASE_PORT + off))
    : > "$SRV_OUT"
    "$SERVER" --cert "$CERT" --key "$KEY" --port "$cand" \
        > "$SRV_OUT" 2>&1 &
    SRV_PID=$!
    sleep 0.5
    if kill -0 "$SRV_PID" 2>/dev/null; then
        PORT="$cand"
        break
    fi
    # Bind failed (server already exited); reap and try the next port.
    wait "$SRV_PID" 2>/dev/null
    SRV_PID=""
done

if [ -z "$PORT" ]; then
    echo "SMOKE FAIL: server could not bind any candidate port"
    dump_logs
    exit 1
fi

"$CLIENT" --host 127.0.0.1 --port "$PORT" --path /moq --timeout-sec 10 \
    > "$CLI_OUT" 2>&1
CRC=$?

if [ "$CRC" -ne 0 ]; then
    echo "SMOKE FAIL: client exited $CRC (port $PORT)"
    dump_logs
    exit 1
fi
if ! grep -q "object received" "$CLI_OUT"; then
    echo "SMOKE FAIL: client did not report 'object received'"
    dump_logs
    exit 1
fi
if ! grep -q "hello-pico-wt" "$CLI_OUT"; then
    echo "SMOKE FAIL: expected payload 'hello-pico-wt' not found"
    dump_logs
    exit 1
fi

echo "SMOKE PASS (port $PORT)"
cat "$CLI_OUT"
exit 0
