#!/usr/bin/env bash
# The snapshot_cap oracle must actually discriminate.
#
# Its CMake registration can stay byte-for-byte correct while the oracle's own
# semantics stop checking anything -- a matcher pointed at the ordinary success
# line, a dropped status check, or a loosened cardinality all leave the wiring
# guard green. So the oracle is exercised here against synthetic subjects with
# known output and exit status, and every discrimination it claims is proven.
#
# No clock, no sleeps, no network.
set -u
oracle="${1:-}"
cmake_bin="${2:-cmake}"
[ -n "$oracle" ] || { echo "usage: $0 <run_snapshot_cap.cmake> [cmake]" >&2; exit 2; }
[ -r "$oracle" ] || { echo "FAIL: cannot read $oracle" >&2; exit 1; }

failures=0
fail() { echo "FAIL: $*" >&2; failures=$((failures + 1)); }

work=$(mktemp -d) || { echo "FAIL: no temp dir" >&2; exit 1; }
trap 'rm -rf "$work"' EXIT

RECEIPT='RECEIPT concurrent_pumps completed gens=16 threads=4'
OKLINE='test_relay_snapshot_cap: OK'

# Build a synthetic subject: $1 name, $2 exit status, rest = lines to print.
subject() {
    name="$1"; status="$2"; shift 2
    path="$work/$name"
    {
        echo '#!/bin/sh'
        for line in "$@"; do printf "echo '%s'\n" "$line"; done
        echo "exit $status"
    } > "$path"
    chmod +x "$path"
    echo "$path"
}

# Run the oracle over a subject; echo accept|reject.
verdict() {
    if "$cmake_bin" -DMOQR_BIN="$1" -P "$oracle" >/dev/null 2>&1; then
        echo accept
    else
        echo reject
    fi
}

expect() {
    want="$1"; name="$2"; path="$3"
    got=$(verdict "$path")
    [ "$got" = "$want" ] ||
        fail "oracle $got the '$name' subject, expected $want"
}

expect accept "well-formed run" \
    "$(subject good 0 "$RECEIPT" "$OKLINE")"
expect reject "success without receipt (the early-return shape)" \
    "$(subject no_receipt 0 "$OKLINE")"
expect reject "receipt without success record" \
    "$(subject no_ok 0 "$RECEIPT")"
expect reject "duplicate exact receipt" \
    "$(subject dup_receipt 0 "$RECEIPT" "$RECEIPT" "$OKLINE")"
expect reject "duplicate exact success record" \
    "$(subject dup_ok 0 "$RECEIPT" "$OKLINE" "$OKLINE")"
expect reject "both records but nonzero status" \
    "$(subject bad_status 1 "$RECEIPT" "$OKLINE")"
expect reject "no output at all" \
    "$(subject silent 0)"

# Records must be whole lines. An unanchored search accepts every one of these.
expect reject "prefix on the receipt" \
    "$(subject pre_receipt 0 "prefix $RECEIPT" "$OKLINE")"
expect reject "suffix on the receipt" \
    "$(subject suf_receipt 0 "$RECEIPT suffix" "$OKLINE")"
expect reject "prefix on the success record" \
    "$(subject pre_ok 0 "$RECEIPT" "prefix $OKLINE")"
expect reject "suffix on the success record" \
    "$(subject suf_ok 0 "$RECEIPT" "$OKLINE suffix")"
expect reject "both records embedded in one malformed line" \
    "$(subject one_line 0 "$RECEIPT $OKLINE")"
expect reject "one exact line plus a substring-only impostor" \
    "$(subject impostor 0 "$RECEIPT" "$OKLINE" "x $OKLINE x")"
expect reject "exact receipt plus a prefixed impostor receipt" \
    "$(subject impostor2 0 "$RECEIPT" "zz $RECEIPT" "$OKLINE")"

# CRLF is the explicitly chosen policy: a trailing carriage return is stripped,
# so the same records are accepted on a platform that emits CRLF.
crlf="$work/crlf"
{
    echo '#!/bin/sh'
    printf "printf '%%s\\r\\n' '%s'\n" "$RECEIPT"
    printf "printf '%%s\\r\\n' '%s'\n" "$OKLINE"
    echo 'exit 0'
} > "$crlf"
chmod +x "$crlf"
expect accept "CRLF line endings" "$crlf"

if [ "$failures" -ne 0 ]; then
    echo "FAIL: $failures oracle-discrimination violation(s)" >&2
    exit 1
fi
echo "PASS: the snapshot_cap oracle discriminates on status, receipt and success"
exit 0
