#!/usr/bin/env bash
# Live SIGTERM against a relay running BOTH listeners.
#
# The source guard pins the stop/join-before-destroy ORDER; this proves the
# real process actually honours it: a relay that destroyed shards while either
# facade's lanes were still stepping them would be tearing the memory those
# lanes are reading, which is a crash or an ASan report, not a clean exit.
#
# The relay is stopped only once BOTH listeners have announced themselves, so
# the signal always arrives with two live facades.
set -u
relay=${1:?usage: check_dual_sigterm.sh <moq5-relay> <cert> <key>}
cert=${2:?}; key=${3:?}
[ -x "$relay" ] || { echo "FAIL: no relay binary at $relay"; exit 1; }

work=$(mktemp -d); trap 'rm -rf "$work"; [ -n "${pid:-}" ] && kill -9 "$pid" 2>/dev/null' EXIT

# two free UDP ports, taken and released just before use
read -r p1 p2 <<<"$(python3 - <<'PY'
import socket
def free():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p
print(free(), free())
PY
)"
cat > "$work/relay.json" <<JSON
{
  "listener":     { "host": "127.0.0.1", "port": $p1, "cert": "$cert",
                    "key": "$key", "lanes": 2, "versions": [18, 16] },
  "webtransport": { "host": "127.0.0.1", "port": $p2, "cert": "$cert",
                    "key": "$key", "lanes": 2, "versions": [18, 16],
                    "path": "/moq", "profile": "current" }
}
JSON

"$relay" serve --config "$work/relay.json" > "$work/out" 2>&1 &
pid=$!

# wait for BOTH listeners, so the signal cannot land on a half-started relay
ready=0
for _ in $(seq 1 300); do
    if grep -q 'listening on' "$work/out" 2>/dev/null &&
       grep -q 'WebTransport on' "$work/out" 2>/dev/null; then
        ready=1; break
    fi
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.1
done
if [ "$ready" -ne 1 ]; then
    echo "FAIL: both listeners never came up"; sed 's/^/    /' "$work/out"; exit 1
fi
echo "  both listeners up (raw :$p1, WebTransport :$p2)"

kill -TERM "$pid"
exited=0
for _ in $(seq 1 300); do
    kill -0 "$pid" 2>/dev/null || { exited=1; break; }
    sleep 0.1
done
if [ "$exited" -ne 1 ]; then
    echo "FAIL: relay did not exit within 30s of SIGTERM"
    kill -9 "$pid" 2>/dev/null; sed 's/^/    /' "$work/out"; exit 1
fi
wait "$pid"; status=$?
pid=

# 128+15 is a relay killed BY the signal rather than one that handled it.
if [ "$status" -eq 143 ]; then
    echo "FAIL: relay was terminated by SIGTERM instead of shutting down"
    exit 1
fi
if [ "$status" -ne 0 ]; then
    echo "FAIL: relay exited $status after SIGTERM"; sed 's/^/    /' "$work/out"; exit 1
fi
if grep -qiE 'sanitizer|use-after-free|heap-buffer-overflow|SEGV|assert' "$work/out"; then
    echo "FAIL: teardown produced a fault report"
    grep -iE 'sanitizer|use-after-free|heap-buffer-overflow|SEGV|assert' "$work/out" | head -3 | sed 's/^/    /'
    exit 1
fi
# Both facades must have reported their final per-lane rows: a facade that was
# never joined cannot have published its post-join attribution.
raw_rows=$(grep -c 'RELAY_LANE_STATS_V3' "$work/out" || true)
if [ "$raw_rows" -lt 4 ]; then
    echo "FAIL: expected a final row for all 4 lanes, saw $raw_rows"
    sed 's/^/    /' "$work/out"; exit 1
fi
echo "  clean exit after SIGTERM; $raw_rows final lane rows published"
echo "PASS: dual listener SIGTERM"
