#!/usr/bin/env bash
# Every exit after the broker exists must destroy it.
#
# The broker owns a pthread mutex, so a serve path that constructs it and then
# returns early on a later setup failure leaks that mutex and its state. There
# are several such arms (transport create, capacity refusal, snapshot
# allocation), and each is only reachable under a different failure, so the
# ordering is pinned structurally rather than left to one runtime path.
set -u
main_c="${1:-}"
[ -n "$main_c" ] || { echo "usage: $0 <cli/main.c>" >&2; exit 2; }
[ -r "$main_c" ] || { echo "FAIL: cannot read $main_c" >&2; exit 1; }

failures=0
fail() { echo "FAIL: $*" >&2; failures=$((failures + 1)); }

# Strip comments and blank lines so prose cannot satisfy the gate.
code=$(sed -e 's://.*::' "$main_c" | tr '\n' '\001' |
       sed -e 's:/\*[^\*]*\*\+\([^/\*][^\*]*\*\+\)*/: :g' | tr '\001' '\n')

check_fn() {
    fn="$1"
    body=$(printf '%s\n' "$code" |
           awk -v f="^$fn\\\\(" 'BEGIN{on=0} $0 ~ f {on=1} on {print} on && /^}/ {exit}')
    [ -n "$body" ] || { fail "$fn not found"; return; }

    init=$(printf '%s\n' "$body" | grep -n 'moqr_broker_init' | head -1 | cut -d: -f1)
    [ -n "$init" ] || { fail "$fn does not initialise the metrics broker"; return; }

    # Every `return` after the broker exists must have a broker destroy within
    # the preceding few lines of its own unwind block.
    printf '%s\n' "$body" | grep -n '^\s*return ' | while IFS=: read -r ln _; do
        [ "$ln" -gt "$init" ] || continue
        lo=$((ln - 14)); [ "$lo" -lt "$init" ] && lo="$init"
        window=$(printf '%s\n' "$body" | sed -n "${lo},${ln}p")
        # The broker-init failure arm itself never constructed a broker, so it
        # must NOT destroy one; it is the single legitimate exception. The
        # exemption window is deliberately tight -- a wider one would let an
        # unrelated arm a few lines below borrow this arm's diagnostic and
        # inherit the exemption with it.
        xlo=$((ln - 4)); [ "$xlo" -lt "$init" ] && xlo="$init"
        if printf '%s\n' "$body" | sed -n "${xlo},${ln}p" |
           grep -q 'metrics broker init failed'; then
            continue
        fi
        if ! printf '%s\n' "$window" | grep -q 'moqr_broker_destroy'; then
            echo "FAIL: $fn returns at body line $ln after broker init without destroying the broker" >&2
            exit 1
        fi
    done || fail "$fn has an exit that leaks the constructed broker"
}

check_fn cmd_serve
check_fn cmd_serve_lanes

if [ "$failures" -ne 0 ]; then
    echo "FAIL: $failures broker-unwind violation(s)" >&2
    exit 1
fi
echo "PASS: every exit after broker construction destroys it"
exit 0
