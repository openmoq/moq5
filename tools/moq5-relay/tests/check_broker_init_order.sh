#!/usr/bin/env bash
# The broker must be fully constructed before any transport facade exists.
#
# A managed facade starts its doorbell threads before its create call returns,
# and every lane pump reads the broker. Initialising the broker afterwards
# leaves a window in which a live callback locks an uninitialised mutex, or
# races the memset that clears it. Ordering is the whole defence, so it is
# pinned here rather than left to review.
set -u
main_c="${1:-}"
[ -n "$main_c" ] || { echo "usage: $0 <cli/main.c>" >&2; exit 2; }
[ -r "$main_c" ] || { echo "FAIL: cannot read $main_c" >&2; exit 1; }

failures=0
fail() { echo "FAIL: $*" >&2; failures=$((failures + 1)); }

# Strip comments and string literals so a mention in prose cannot satisfy the
# gate, then work per serve function.
code=$(sed -e 's://.*::' "$main_c" | tr '\n' '\001' |
       sed -e 's:/\*[^\*]*\*\+\([^/\*][^\*]*\*\+\)*/: :g' | tr '\001' '\n')

check_fn() {
    fn="$1"
    body=$(printf '%s\n' "$code" |
           awk -v f="^$fn\\\\(" 'BEGIN{on=0} $0 ~ f {on=1} on {print} on && /^}/ {exit}')
    if [ -z "$body" ]; then
        fail "$fn not found"
        return
    fi
    init=$(printf '%s\n' "$body" | grep -n 'moqr_broker_init' | head -1 | cut -d: -f1)
    if [ -z "$init" ]; then
        fail "$fn does not initialise the metrics broker"
        return
    fi
    # Every facade create in this function must come AFTER the broker init.
    printf '%s\n' "$body" | grep -n 'managed_create' | while IFS=: read -r ln _; do
        if [ "$ln" -lt "$init" ]; then
            echo "FAIL: $fn creates a facade (line $ln of body) before broker init (line $init)" >&2
            exit 1
        fi
    done || fail "$fn creates a transport facade before the broker exists"
}

check_fn cmd_serve
check_fn cmd_serve_lanes

if [ "$failures" -ne 0 ]; then
    echo "FAIL: $failures broker-ordering violation(s)" >&2
    exit 1
fi
echo "PASS: every serve path constructs the broker before any facade"
exit 0
