#!/bin/sh
#
# Smoke test for moq-interop-client. No relay required.
#
# Usage: smoke_interop_client.sh <build-dir>

set -e

BUILD_DIR="${1:?usage: smoke_interop_client.sh <build-dir>}"
BIN="$BUILD_DIR/tools/moq-interop-client/moq-interop-client"

if [ ! -x "$BIN" ]; then
    echo "SKIP: $BIN not found"
    exit 0
fi

FAILURES=0
fail() { echo "FAIL: $1"; FAILURES=$((FAILURES + 1)); }

TMPOUT=$(mktemp)
TMPERR=$(mktemp)
cleanup() { rm -f "$TMPOUT" "$TMPERR"; }
trap cleanup EXIT

# -- 1: unknown test → TAP skip + exit 127 --

RC=0
"$BIN" --relay moqt://127.0.0.1:9 --test unknown-xyz >"$TMPOUT" 2>"$TMPERR" || RC=$?
if [ "$RC" -ne 127 ]; then
    fail "unknown test exit code $RC, expected 127"
fi
if ! grep -q 'ok 1 - unknown-xyz # SKIP' "$TMPOUT"; then
    fail "unknown test missing TAP skip line"
fi

# -- 2: bad URL → exit 1 --

RC=0
"$BIN" --relay bad --test setup-only >"$TMPOUT" 2>"$TMPERR" || RC=$?
if [ "$RC" -ne 1 ]; then
    fail "bad URL exit code $RC, expected 1"
fi

# -- 3: announce-subscribe against unreachable → exit 1, 5 results --

RC=0
"$BIN" --relay moqt://127.0.0.1:9 --test announce-subscribe \
    --tls-disable-verify >"$TMPOUT" 2>"$TMPERR" || RC=$?
if [ "$RC" -ne 1 ]; then
    fail "announce-subscribe exit code $RC, expected 1"
fi
if ! grep -q '^TAP version 14$' "$TMPOUT"; then
    fail "announce-subscribe missing TAP version"
fi
if ! grep -q '^1\.\.5$' "$TMPOUT"; then
    fail "announce-subscribe missing 1..5 plan"
fi
RESULT_LINES=$(grep -c '^\(ok\|not ok\) [1-5] - ' "$TMPOUT" || true)
if [ "$RESULT_LINES" -ne 5 ]; then
    fail "announce-subscribe has $RESULT_LINES result lines, expected 5"
fi
# TAP must not leak to stderr
if grep -q '^TAP version\|^1\.\.\|^\(ok\|not ok\) ' "$TMPERR"; then
    fail "announce-subscribe TAP leaked to stderr"
fi

# -- 4: subscribe-before-announce against unreachable → exit 1, 5 results --

RC=0
"$BIN" --relay moqt://127.0.0.1:9 --test subscribe-before-announce \
    --tls-disable-verify >"$TMPOUT" 2>"$TMPERR" || RC=$?
if [ "$RC" -ne 1 ]; then
    fail "subscribe-before-announce exit code $RC, expected 1"
fi
if ! grep -q '^TAP version 14$' "$TMPOUT"; then
    fail "subscribe-before-announce missing TAP version"
fi
if ! grep -q '^1\.\.5$' "$TMPOUT"; then
    fail "subscribe-before-announce missing 1..5 plan"
fi
RESULT_LINES=$(grep -c '^\(ok\|not ok\) [1-5] - ' "$TMPOUT" || true)
if [ "$RESULT_LINES" -ne 5 ]; then
    fail "subscribe-before-announce has $RESULT_LINES result lines, expected 5"
fi
if grep -q '^TAP version\|^1\.\.\|^\(ok\|not ok\) ' "$TMPERR"; then
    fail "subscribe-before-announce TAP leaked to stderr"
fi

# -- 5: invalid --draft (trailing junk) → exit 1, validation error --

RC=0
"$BIN" --relay moqt://127.0.0.1:9 --draft 16junk >"$TMPOUT" 2>"$TMPERR" || RC=$?
if [ "$RC" -ne 1 ]; then
    fail "--draft 16junk exit code $RC, expected 1"
fi
if ! grep -q 'must be exactly 16 or 18' "$TMPERR"; then
    fail "--draft 16junk missing validation error"
fi

# -- 6: invalid MOQT_DRAFT (non-numeric) → exit 1, validation error --

RC=0
MOQT_DRAFT=abc "$BIN" --relay moqt://127.0.0.1:9 --test setup-only \
    >"$TMPOUT" 2>"$TMPERR" || RC=$?
if [ "$RC" -ne 1 ]; then
    fail "MOQT_DRAFT=abc exit code $RC, expected 1"
fi
if ! grep -q 'must be exactly 16 or 18' "$TMPERR"; then
    fail "MOQT_DRAFT=abc missing validation error"
fi

# -- 7: --help → exit 0, usage shape (mentions --draft) --

RC=0
"$BIN" --help >"$TMPOUT" 2>"$TMPERR" || RC=$?
if [ "$RC" -ne 0 ]; then
    fail "--help exit code $RC, expected 0"
fi
if ! grep -q 'Usage:' "$TMPERR"; then
    fail "--help missing usage text"
fi
if ! grep -q -- '--draft' "$TMPERR"; then
    fail "--help usage missing --draft"
fi

# -- 8: bare "wtquic" backend is rejected (flag) --

RC=0
"$BIN" --backend wtquic --relay https://127.0.0.1:9/ --test setup-only \
    >"$TMPOUT" 2>"$TMPERR" || RC=$?
if [ "$RC" -ne 1 ]; then
    fail "--backend wtquic exit code $RC, expected 1"
fi
if ! grep -q 'bare "wtquic" is not accepted' "$TMPERR"; then
    fail "--backend wtquic missing rejection message"
fi

# -- 9: bare "wtquic" is rejected via MOQT_BACKEND too --

RC=0
MOQT_BACKEND=wtquic "$BIN" --relay https://127.0.0.1:9/ --test setup-only \
    >"$TMPOUT" 2>"$TMPERR" || RC=$?
if [ "$RC" -ne 1 ]; then
    fail "MOQT_BACKEND=wtquic exit code $RC, expected 1"
fi

# -- 10: WebTransport-only backends reject a raw-QUIC (moqt://) URL --

for BE in wtquic-network wtquic-msquic proxygen; do
    RC=0
    "$BIN" --backend "$BE" --relay moqt://127.0.0.1:9 --test setup-only \
        >"$TMPOUT" 2>"$TMPERR" || RC=$?
    if [ "$RC" -ne 1 ]; then
        fail "--backend $BE moqt:// exit code $RC, expected 1"
    fi
    if ! grep -q 'WebTransport only' "$TMPERR"; then
        fail "--backend $BE moqt:// missing WebTransport-only error"
    fi
done

# -- 11: raw-QUIC-only backend rejects a WebTransport (https://) URL --

RC=0
"$BIN" --backend msquic --relay https://127.0.0.1:9/ --test setup-only \
    >"$TMPOUT" 2>"$TMPERR" || RC=$?
if [ "$RC" -ne 1 ]; then
    fail "--backend msquic https:// exit code $RC, expected 1"
fi
if ! grep -q 'raw-QUIC only' "$TMPERR"; then
    fail "--backend msquic https:// missing raw-QUIC-only error"
fi

# -- 12: unknown backend token → exit 1 --

RC=0
"$BIN" --backend bogus --relay https://127.0.0.1:9/ --test setup-only \
    >"$TMPOUT" 2>"$TMPERR" || RC=$?
if [ "$RC" -ne 1 ]; then
    fail "--backend bogus exit code $RC, expected 1"
fi

# -- 13: --backend wins over MOQT_BACKEND (proven via scheme cross-check) --
# env picks a WT-only backend, the flag picks a raw-QUIC-only one, the URL is
# https://; if the flag wins we get a raw-QUIC-only rejection, if the env won we
# would connect. Instant, no relay round-trip.

RC=0
MOQT_BACKEND=wtquic-network "$BIN" --backend msquic --relay https://127.0.0.1:9/ \
    --test setup-only >"$TMPOUT" 2>"$TMPERR" || RC=$?
if [ "$RC" -ne 1 ] || ! grep -q 'raw-QUIC only' "$TMPERR"; then
    fail "--backend did not override MOQT_BACKEND"
fi

# -- 14: --help lists the new backend tokens and the bare-wtquic rejection --

"$BIN" --help >"$TMPOUT" 2>"$TMPERR" || true
if ! grep -q 'wtquic-msquic' "$TMPERR"; then
    fail "--help missing wtquic-msquic backend"
fi
if ! grep -q 'wtquic-network' "$TMPERR"; then
    fail "--help missing wtquic-network backend"
fi
if ! grep -q 'bare "wtquic" is not accepted' "$TMPERR"; then
    fail "--help missing bare-wtquic rejection note"
fi

# -- 15: --wt-profile is rejected for a backend that cannot select a dialect --

RC=0
"$BIN" --backend picoquic --wt-profile d13-14 --relay https://127.0.0.1:9/ \
    --test setup-only >"$TMPOUT" 2>"$TMPERR" || RC=$?
if [ "$RC" -ne 1 ]; then
    fail "--wt-profile on picoquic exit code $RC, expected 1"
fi
if ! grep -q 'applies only to the wtquic-msquic backend' "$TMPERR"; then
    fail "--wt-profile on picoquic missing rejection message"
fi

# -- 16: an unknown --wt-profile value is rejected (on wtquic-msquic) --

RC=0
"$BIN" --backend wtquic-msquic --wt-profile bogus --relay https://127.0.0.1:9/ \
    --test setup-only >"$TMPOUT" 2>"$TMPERR" || RC=$?
if [ "$RC" -ne 1 ] || ! grep -q 'must be' "$TMPERR"; then
    fail "--wt-profile bogus not rejected"
fi

# -- 17: TAP identity is TRUTHFUL. wtquic-msquic reports the selected dialect;
# a fixed-dialect WebTransport backend reports native; raw QUIC reports n/a. The
# request line is emitted before the (failing) dial, so a dead port is fine. --

"$BIN" --backend wtquic-msquic --wt-profile d13-14 --relay https://127.0.0.1:9/ \
    --test setup-only >"$TMPOUT" 2>"$TMPERR" || true
if ! grep -q '^# request: backend=wtquic-msquic wt-profile=d13-14 draft=' "$TMPOUT"; then
    fail "wtquic-msquic d13-14 identity not reported truthfully"
fi
"$BIN" --backend picoquic --relay https://127.0.0.1:9/ \
    --test setup-only >"$TMPOUT" 2>"$TMPERR" || true
if ! grep -q '^# request: backend=picoquic wt-profile=native draft=' "$TMPOUT"; then
    fail "picoquic WebTransport identity should be native"
fi
"$BIN" --backend msquic --relay moqt://127.0.0.1:9 \
    --test setup-only >"$TMPOUT" 2>"$TMPERR" || true
if ! grep -q '^# request: backend=msquic wt-profile=n/a draft=' "$TMPOUT"; then
    fail "raw-QUIC msquic identity should be n/a"
fi

# -- Summary --

if [ "$FAILURES" -gt 0 ]; then
    echo "FAILED: $FAILURES check(s)"
    exit 1
fi

echo "PASS: smoke_interop_client (17 checks)"
