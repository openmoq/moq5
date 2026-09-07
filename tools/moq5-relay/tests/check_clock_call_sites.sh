#!/usr/bin/env bash
# The test clock can only be driven through operations that take the expected
# outcome, and there is no raw operation left to reach around them.
#
# NOTHING HERE PARSES C. A scanner can be defeated by a comment, a string
# literal, a line splice, a parenthesized function designator or a function
# pointer, so every fact below is decided by the compiler, the linker, or the
# object file:
#
#   1. the raw operations are not callable at all      (private to the
#      -- not parenthesized, not via a pointer          implementation)
#   2. an expectation cannot be consumed and ignored   (the operation is void
#      -- nor stated with no ledger to answer to        and the ledger is not
#                                                       an optional argument)
#   3. both expected outcomes are expressible          (the boundary's shape)
#   4. the governed unit really uses that boundary     (its object's symbols)
set -u
hdr="${1:-}"
src="${2:-}"
cc="${3:-cc}"
[ -n "$hdr" ] && [ -n "$src" ] || {
    echo "usage: $0 <admin_listen.h> <governed.c> <cc> [-I...]" >&2
    exit 2
}
shift 3
for f in "$hdr" "$src"; do
    [ -r "$f" ] || { echo "FAIL: cannot read $f" >&2; exit 1; }
done

failures=0
fail() { echo "FAIL: $*" >&2; failures=$((failures + 1)); }

tmp=$(mktemp -d) || { echo "FAIL: no temporary directory" >&2; exit 1; }
trap 'rm -rf "$tmp"' EXIT

raw_arm='moqr_admin_listen_test_clock_arm'
raw_adv='moqr_admin_listen_test_clock_advance'
exp_arm="${raw_arm}_expect"
exp_adv="${raw_adv}_expect"

cc_fixture() {   # cc_fixture <name> [flags...] ; the compiler's verdict
    local name="$1"
    shift
    "$cc" -std=c11 -Wall -Wextra -Wpedantic -Werror \
        -DMOQR_ADMIN_LISTEN_TESTING "$@" -c "$tmp/$name.c" -o "$tmp/$name.o" \
        >"$tmp/cc.log" 2>&1
}

# --- 1. the raw operations are unreachable, in every spelling -------------
for raw in "$raw_arm" "$raw_adv"; do
    # plain call
    cat > "$tmp/raw.c" <<EOF
#include "admin_listen.h"
int probe(moqr_admin_listen_t *l);
int probe(moqr_admin_listen_t *l) { return $raw(l, 1) ? 0 : 1; }
EOF
    if cc_fixture raw "$@"; then
        fail "$raw is still callable: a future call site can bypass the \
expectation boundary"
    fi
    # parenthesized function designator -- the form that defeats a macro
    cat > "$tmp/paren.c" <<EOF
#include "admin_listen.h"
int probe(moqr_admin_listen_t *l);
int probe(moqr_admin_listen_t *l) { return ($raw)(l, 1) ? 0 : 1; }
EOF
    if cc_fixture paren "$@"; then
        fail "a parenthesized designator reaches $raw"
    fi
    # address-of / function-pointer alias
    cat > "$tmp/alias.c" <<EOF
#include "admin_listen.h"
typedef bool (*clock_fn)(moqr_admin_listen_t *, uint64_t);
int probe(moqr_admin_listen_t *l);
int probe(moqr_admin_listen_t *l) { clock_fn f = &$raw; return f(l, 1) ? 0 : 1; }
EOF
    if cc_fixture alias "$@"; then
        fail "a function-pointer alias reaches $raw"
    fi
done

# --- 2. an expectation cannot be consumed and then ignored ---------------
# The exact shape a returning boundary could not reject: the result is
# syntactically consumed and semantically dropped. A void operation that owns
# the verdict cannot be written this way at all.
for api in "$exp_arm" "$exp_adv"; do
    cat > "$tmp/ignored.c" <<EOF
#include "admin_listen.h"
int probe(moqr_admin_listen_t *l);
int probe(moqr_admin_listen_t *l) {
    int failures = 0;
    if ($api(l, 1, true, "ignored", &failures)) {
    }
    return failures;
}
EOF
    if cc_fixture ignored "$@"; then
        fail "an expectation from $api was consumed and ignored: the verdict \
is still the caller's to drop"
    fi
    # And the ledger is not optional.
    cat > "$tmp/noledger.c" <<EOF
#include "admin_listen.h"
void probe(moqr_admin_listen_t *l);
void probe(moqr_admin_listen_t *l) { $api(l, 1, true, "who"); }
EOF
    if cc_fixture noledger "$@"; then
        fail "$api accepted a call with no failure ledger argument"
    fi
done

# --- 3. both expected outcomes are expressible ---------------------------
cat > "$tmp/pos.c" <<EOF
#include "admin_listen.h"
int probe(moqr_admin_listen_t *l);
int probe(moqr_admin_listen_t *l) {
    int failures = 0;
    $exp_arm(l, 1, true, "who", &failures);
    $exp_arm(l, 1, false, "who", &failures);
    $exp_adv(l, 1, true, "who", &failures);
    $exp_adv(l, 1, false, "who", &failures);
    return failures;
}
EOF
if ! cc_fixture pos "$@"; then
    fail "the expectation boundary rejected an ordinary call:"
    sed -n '1,6p' "$tmp/cc.log" >&2
fi

# --- 4. the governed unit really uses the boundary ------------------------
cp "$src" "$tmp/governed.c"
if ! cc_fixture governed "$@" -I"$(cd "$(dirname "$src")" && pwd)"; then
    fail "the governed translation unit does not compile under this contract:"
    sed -n '1,6p' "$tmp/cc.log" >&2
else
    syms=$(nm "$tmp/governed.o" 2>/dev/null)
    for api in "$exp_arm" "$exp_adv"; do
        if ! printf '%s\n' "$syms" | grep -q "$api"; then
            fail "$src never calls $api: its object references no such \
symbol, so this contract would be about nothing"
        fi
    done
    # And it reaches nothing else: a raw reference would mean a symbol the
    # implementation no longer provides.
    for raw in "$raw_arm" "$raw_adv"; do
        if printf '%s\n' "$syms" | grep -E "[ 	]_?${raw}$" >/dev/null; then
            fail "$src references the raw $raw"
        fi
    done
fi

if [ "$failures" -ne 0 ]; then
    echo "FAIL: check_clock_call_sites ($failures)" >&2
    exit 1
fi
echo "PASS: check_clock_call_sites"
