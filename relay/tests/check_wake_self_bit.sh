#!/usr/bin/env bash
# The wake set returned by moqr_shards_step_shard includes the stepped shard's
# OWN bit when the step left local work no external event re-signals. A caller
# that treats that bit as illegal drops the continuation, which is how a
# namespace advertised after its subscription goes undelivered.
#
# This guard pins the callers that inspect the mask for legality: the
# saturation benchmark must derive its legal set from the shard count alone,
# never by subtracting the stepped shard. Argument 1 is the benchmark source.
set -u

src="${1:-}"
if [ ! -f "$src" ]; then
    echo "FAIL: usage: $0 <bench_relay_saturation.c>" >&2
    exit 2
fi

failures=0
fail() { echo "FAIL: $1" >&2; failures=$((failures + 1)); }

# Strip comments and string literals so prose cannot satisfy or trip a check.
# Same lexical pass the other relay source guards use: same-line block
# comments first, then whole multi-line block bodies, then line comments and
# string literals.
strip() {
    sed -e 's|/\*.*\*/||g' "$1" |
    awk '/\/\*/{inc=1} !inc{print} /\*\//{inc=0}' |
    sed -e 's|//.*||' -e 's|"[^"]*"||g'
}

body=$(strip "$src")

# 1. No legality expression may subtract the stepped shard's own bit.
if printf '%s\n' "$body" | grep -qE '~\(1ull << (d|shard|0)\)'; then
    fail "a wake-legality mask subtracts the stepped shard's own bit"
fi
if printf '%s\n' "$body" | grep -qE 'mask & \(1ull << 0\)\) != 0'; then
    fail "an owner loop rejects its own continuation bit"
fi

# 2. Every legality test must go through the one shared helper, so a new call
#    site cannot quietly reintroduce a hand-rolled exclusion.
checks=$(printf '%s\n' "$body" | grep -cE 'mask & ~' || true)
viahelper=$(printf '%s\n' "$body" | grep -cE 'mask & ~sat_legal_wake\(' || true)
if [ "$checks" -eq 0 ]; then
    fail "no wake-legality check found; the guard would pass vacuously"
fi
if [ "$checks" -ne "$viahelper" ]; then
    fail "$((checks - viahelper)) wake-legality check(s) bypass sat_legal_wake"
fi

# 3. The helper itself must not exclude anything.
helper=$(printf '%s\n' "$body" |
         sed -n '/^sat_legal_wake(uint16_t K)$/,/^}/p')
if [ -z "$helper" ]; then
    fail "sat_legal_wake is gone; the shared legality helper is the contract"
elif printf '%s\n' "$helper" | grep -qE '~\(1ull <<|& ~'; then
    fail "sat_legal_wake excludes an in-range bit; every one of them is legal"
fi

if [ "$failures" -ne 0 ]; then
    echo "FAIL: $failures wake self-bit violation(s)" >&2
    exit 1
fi
echo "PASS: every wake-legality check accepts the local continuation bit"
exit 0
