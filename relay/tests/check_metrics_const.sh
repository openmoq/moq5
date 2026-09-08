#!/usr/bin/env bash
# The metrics writer hands `mv_family` a read-only view of the label bases.
#
# Qualification through the array-pointer level is not an implicit conversion
# in ISO C before C23 — `char (*)[N]` does not become `const char (*)[N]` the
# way `char *` becomes `const char *`. The file therefore takes ONE explicit
# add-const alias and every family call uses it. A new call that passes the
# mutable array directly still compiles under clang and only fails on a
# pedantic GCC build, so it is pinned here as well: the source is the contract,
# not the compiler that happens to run.
#
# Argument 1 is relay/src/obs/metrics.c.
set -u

src="${1:-}"
if [ ! -f "$src" ]; then
    echo "FAIL: usage: $0 <obs/metrics.c>" >&2
    exit 2
fi

failures=0
fail() { echo "FAIL: $1" >&2; failures=$((failures + 1)); }

# Comments and string literals stripped: prose about `bases` must not satisfy
# — or trip — anything below.
code=$(sed -e 's|//.*||' "$src" |
       awk '/\/\*/{c=1} !c{print} /\*\//{c=0; print ""}' |
       sed -e 's/"[^"]*"//g')

# 1. the callee keeps its read-only contract
if ! printf '%s\n' "$code" | grep -qE 'const char[[:space:]]+bases\[\]\[MV_BASE_CAP\]'; then
    fail "mv_family no longer takes 'const char bases[][MV_BASE_CAP]'"
fi

# 2. exactly one add-const alias exists, and it is itself const
if ! printf '%s\n' "$code" |
     grep -qE 'const char[[:space:]]*\(\*const cbases\)\[MV_BASE_CAP\]'; then
    fail "the single add-const alias 'cbases' is missing or no longer const"
fi
n_alias=$(printf '%s\n' "$code" | grep -cE '\(\*const cbases\)\[MV_BASE_CAP\][[:space:]]*=')
if [ "$n_alias" -ne 1 ]; then
    fail "expected exactly one cbases definition, found $n_alias"
fi

# 3. the capacity is named once, not duplicated as a literal
if ! printf '%s\n' "$code" | grep -qE '#define MV_BASE_CAP[[:space:]]+192'; then
    fail "MV_BASE_CAP is no longer defined as 192"
fi

# 4. every mv_family call passes the alias; none passes the mutable array.
#    Calls span lines, so the argument lists are joined before matching.
# `mv_family(&w, ...)` is a call; `mv_family(mw_t *w, ...)` is the definition.
joined=$(printf '%s\n' "$code" | tr '\n' ' ' | sed 's/mv_family(/\n&/g' |
         grep '^mv_family([[:space:]]*&w[[:space:]]*,')
calls=$(printf '%s\n' "$joined" | grep -c '^mv_family(' || true)
if [ "$calls" -lt 1 ]; then
    fail "no mv_family calls found — the guard would pass vacuously"
fi
bad=$(printf '%s\n' "$joined" | grep -cE ',[[:space:]]*bases[[:space:]]*,' || true)
if [ "$bad" -ne 0 ]; then
    fail "$bad mv_family call(s) pass the mutable 'bases' instead of 'cbases'"
fi
good=$(printf '%s\n' "$joined" | grep -cE ',[[:space:]]*cbases[[:space:]]*,' || true)
if [ "$good" -ne "$calls" ]; then
    fail "only $good of $calls mv_family calls use the alias"
fi

if [ "$failures" -ne 0 ]; then
    echo "FAIL: $failures metrics-const violation(s)" >&2
    exit 1
fi
echo "PASS: $calls mv_family calls all take the read-only alias"
exit 0
