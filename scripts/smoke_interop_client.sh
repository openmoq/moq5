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

# -- Summary --

if [ "$FAILURES" -gt 0 ]; then
    echo "FAILED: $FAILURES check(s)"
    exit 1
fi

echo "PASS: smoke_interop_client (4 checks)"
