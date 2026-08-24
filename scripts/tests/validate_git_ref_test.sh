#!/usr/bin/env bash
#
# Test for scripts/validate_git_ref.sh (security finding #11). Runs the validator
# against practical-valid and malicious refs and asserts accept/reject. Pure
# shell, no build deps.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
validator="$here/../validate_git_ref.sh"

fails=0

expect_accept() {
    if "$validator" "$1" ref >/dev/null 2>&1; then
        :
    else
        printf 'FAIL: expected ACCEPT but rejected: %q\n' "$1" >&2
        fails=$((fails + 1))
    fi
}

expect_reject() {
    if "$validator" "$1" ref >/dev/null 2>&1; then
        printf 'FAIL: expected REJECT but accepted: %q\n' "$1" >&2
        fails=$((fails + 1))
    fi
}

# --- Valid refs must still work -------------------------------------------
expect_accept "master"
expect_accept "main"
expect_accept "v1.2.3"
expect_accept "feature/foo"
expect_accept "release/v2.0"
expect_accept "0123456789abcdef0123456789abcdef01234567"   # full commit SHA
expect_accept "a1b2c3d"                                     # short SHA
expect_accept "foo.bar_baz-qux"

# --- Malicious / malformed refs must be rejected --------------------------
expect_reject ""                                            # empty
expect_reject "-rf"                                         # leading '-'
expect_reject "--upload-pack=evil"                          # option injection
expect_reject 'master"; rm -rf /'                           # quote + metachar
expect_reject "master'"                                     # single quote
expect_reject 'a$(id)'                                      # command substitution
expect_reject 'a`id`'                                       # backtick substitution
expect_reject 'a;id'                                        # semicolon
expect_reject 'a|id'                                        # pipe
expect_reject 'a&id'                                        # background
expect_reject 'a>b'                                         # redirect
expect_reject 'a<b'                                         # redirect
expect_reject 'a(b)'                                        # subshell parens
expect_reject 'a b'                                         # whitespace
expect_reject $'master\n; rm -rf /'                         # embedded newline
expect_reject $'ok\ttab'                                    # embedded tab
expect_reject 'a\b'                                         # backslash
expect_reject '@'                                           # bare @ (whitelist)

# --- Shell-safe but NOT valid git refnames (git check-ref-format rejects) ---
expect_reject "/bad"                                        # leading slash
expect_reject "bad/"                                        # trailing slash
expect_reject "bad..ref"                                    # double dot
expect_reject ".bad"                                        # leading dot
expect_reject "bad.lock"                                    # .lock suffix
expect_reject "foo//bar"                                    # consecutive slashes

if [ "$fails" -ne 0 ]; then
    printf '%d case(s) failed\n' "$fails" >&2
    exit 1
fi
echo "ok"
