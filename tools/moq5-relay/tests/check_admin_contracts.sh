#!/usr/bin/env bash
# Source oracle for the admin tier's public contracts.
#
# Two classes of defect no behavioural test can see:
#
#   1. A comment that states the opposite of what the code does. Every phrase
#      below was a real contradiction found in review, not a hypothetical.
#   2. Casting away const to mutate through a pointer whose signature promised
#      not to. The behaviour may be right; the API is a lie.
#
# Portability: BSD grep -E has no \s or \b, so every pattern here uses POSIX
# character classes and explicit boundary characters. A pattern that silently
# matches nothing is a check that silently passes.
#
# Usage: check_admin_contracts.sh <moqr_admin.h> <admin_sm.c> <http_parse.c>
set -u

hdr=${1:-}
sm=${2:-}
hp=${3:-}
for f in "$hdr" "$sm" "$hp"; do
    [ -r "$f" ] || { echo "usage: $0 <moqr_admin.h> <admin_sm.c> <http_parse.c>" >&2; exit 2; }
done

failures=0
fail() { echo "FAIL: $1" >&2; failures=$((failures + 1)); }

# The cast pattern must be proven to match a known-bad line on THIS grep, or a
# portability difference would turn the whole check into a no-op.
cast_re='[(][[:space:]]*moqr_admin[a-z_]*_t[[:space:]]*[*][[:space:]]*[)][[:space:]]*a[^[:alnum:]_]'
printf '((moqr_admin_t *)a)->gen[i].state = 1;\n' | grep -qE "$cast_re" ||
    fail "the cast-away-const pattern does not match a known cast on this grep"

for f in "$sm" "$hp"; do
    grep -qE "$cast_re" "$f" &&
        fail "$(basename "$f") casts away const on an admin pointer"
done

# The claiming operation must take a MUTABLE admin: it performs a transition.
grep -q 'bool moqr_admin_claim_abandon(moqr_admin_t \*a,' "$hdr" ||
    fail "moqr_admin_claim_abandon does not take a mutable admin"
grep -q 'moqr_admin_peek_abandon' "$hdr" &&
    fail "the retired peek_abandon name is still present"

# -- retired vocabulary, one decisive check per phrase -----------------------
# Each described a mechanism that no longer exists; left in place it would send
# a Step-4 reader down a path the code does not implement.
check_absent() {   # file, phrase, message
    grep -qF "$2" "$1" && fail "$3"
}
check_present() {  # file, phrase, message
    # Single-line matching: a phrase that wraps across a comment line break can
    # never match, which would make the check silently pass or silently fail.
    grep -qF "$2" "$1" || fail "$3"
}

check_absent "$hdr" "Accept extensions, which follow q, are skipped" \
    "the header still states the removed accept-ext rule"
check_absent "$hdr" "which this queues" \
    "the header still says a release is queued"
check_absent "$hdr" "is simply retried" \
    "the header still promises an unconditionally retryable release"
check_present "$hdr" "SUCCESSFUL broker release is NOT" \
    "the header does not state the post-release boundary for bank releases"
check_present "$hdr" "phase 1, before take" \
    "the header does not state the retirement phase contract"
grep -q 'moqr_result_t moqr_admin_record_take(' "$hdr" ||
    fail "the header does not declare the retained-token operation"
# The DECLARATION, not merely a mention: the name also appears in the phase
# contract comment, so matching anywhere would pass with the function gone.
grep -q 'moqr_result_t moqr_admin_settle_abandon(' "$hdr" ||
    fail "the header does not declare the coupled settle transaction"
check_absent "$hdr" "moqr_admin_ack_abandon" \
    "a separate abandonment acknowledgement is still declared"
# Retirement vocabulary: the removed peek/ack shape must not survive in prose.
check_absent "$hdr" "between a peek and its acknowledgement" \
    "the header still describes the removed abandonment peek/ack race"
check_absent "$hdr" "the caller acknowledged the broker retired it" \
    "the transition table still implies a separate abandonment acknowledgement"
check_present "$hdr" "the coupled settlement retired it in the broker" \
    "the transition table does not name the coupled settlement"
# ONE policy for a failing settle callback. The header said both "the owner can
# retry from phase 2" and "a refusal is an invariant failure, not a retry".
check_absent "$hdr" "the owner can retry from phase 2" \
    "the header still offers a retry policy for a failing settle callback"
check_present "$hdr" "INVARIANT FAILURE, not a retry" \
    "the header does not state the single failing-callback policy"
check_absent "$hdr" "the retirement stays claimable" \
    "the header still says a failed settle leaves the retirement claimable"
check_absent "$sm" "resumes from phase 2" \
    "admin_sm.c still says the owner resumes from phase 2 after a failure"

# accept-ext must never appear as a rule in force, in EITHER file, and the
# prose that stated it did so without using the hyphenated spelling at all --
# so scanning for "accept-ext" alone let it through. Ban the claim itself.
for f in "$hdr" "$hp"; do
    check_absent "$f" "Accept extensions, which follow q" \
        "$(basename "$f") still says accept extensions after q are skipped"
    check_absent "$f" "follow q, are a different thing" \
        "$(basename "$f") still exempts parameters that follow q"
    if grep -q 'accept-ext' "$f"; then
        grep 'accept-ext' "$f" | grep -qv 'removed accept-ext' &&
            fail "$(basename "$f") states an accept-ext rule"
    fi
done
check_present "$hp" "narrows wherever it appears" \
    "http_parse.c does not state that an unknown parameter always narrows"

check_absent "$sm" "strictly greater" \
    "admin_sm.c still describes the predicate as strictly greater"
check_absent "$sm" "Strictly greater" \
    "admin_sm.c still describes the predicate as Strictly greater"
check_present "$sm" "Inequality, never" \
    "admin_sm.c does not state the predicate as inequality"
check_present "$hp" "project policy for a closed surface" \
    "http_parse.c does not label the Host narrowing as project policy"
# The IPvFuture comment claims the real production; the code must admit
# sub-delims, or the comment is a false quotation.
grep -q "case '!': case '\$': case '&':" "$hp" ||
    fail "http_parse.c claims the IPvFuture production but omits sub-delims"

if [ "$failures" -ne 0 ]; then
    echo "FAIL: $failures admin contract violation(s)" >&2
    exit 1
fi
echo "PASS: admin contracts, const honesty and retired vocabulary"
