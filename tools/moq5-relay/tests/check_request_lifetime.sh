#!/usr/bin/env bash
# B's lifetime is proved by a mutex handoff, and the proof is load-bearing.
#
# The protocol is factored into structurally narrow operations so that each
# property is a fact about one small body rather than about token order in a
# long function:
#
#   poll_req_fail_stop            the ONE terminal action of every failure arm
#   poll_req_publish_completion   checked acquire / seam wait / set /
#                                 checked broadcast / checked unlock
#   poll_req_publish_release      checked acquire / set / checked broadcast /
#                                 checked unlock -- B's last context access
#   poll_req_await_release_and_detach_or_stop
#                                 the only path to the detach
#
# Nothing here discards diagnostics or filters lines to manufacture a clean
# shape: failure arms are extracted as brace-balanced bodies and required to
# consist of the fail-stop call alone; the successful path of poll_req_main is
# required to END with the release publication followed by the return.
set -u
src="${1:-}"
[ -n "$src" ] || { echo "usage: $0 <tests/test_relay_admin_listen.c>" >&2; exit 2; }
[ -r "$src" ] || { echo "FAIL: cannot read $src" >&2; exit 1; }

failures=0
fail() { echo "FAIL: $*" >&2; failures=$((failures + 1)); }

fn_body() {
    awk -v n="$1" '
        index($0, n "(") == 1 { on = 1 }
        on { print }
        on && /^}$/ { exit }' "$src"
}

# The brace-balanced body of the `if (...) {` at line N of a body.
arm_body() {   # arm_body <body> <line-number-of-if>
    printf '%s\n' "$1" | awk -v s="$2" '
        NR < s { next }
        NR == s { depth = gsub(/\{/, "{") - gsub(/\}/, "}"); next }
        {
            line = $0
            n = gsub(/\{/, "{", line); m = gsub(/\}/, "}", line)
            if (depth + n - m <= 0) { exit }
            print $0
            depth += n - m
        }'
}

# Every failure arm of <call> in <body> must consist of exactly one statement:
# a call to the fail-stop helper.
require_failstop_arms() {   # <label> <body> <call-regex>
    local label="$1" body="$2" call="$3" lines ln arm stmts
    lines=$(printf '%s\n' "$body" | grep -nE "if[[:space:]]*\\([[:space:]]*${call}[[:space:]]*!=[[:space:]]*0[[:space:]]*\\)" | cut -d: -f1)
    if [ -z "$lines" ]; then
        fail "$label: no checked call to ${call} was found"
        return
    fi
    for ln in $lines; do
        arm=$(arm_body "$body" "$ln")
        stmts=$(printf '%s\n' "$arm" | grep -vE '^[[:space:]]*$' |
                tr '\n' ' ' | sed -E 's/[[:space:]]+/ /g')
        # Joined continuation lines of one call are one statement.
        if ! printf '%s\n' "$stmts" | grep -qE '^ ?poll_req_fail_stop\("[^;]*\); ?$'; then
            fail "$label: the failure arm of ${call} does not stop; it reads: \
${stmts}"
        fi
    done
}

# --- the fail-stop helper is what it claims ------------------------------
fs=$(fn_body poll_req_fail_stop)
if [ -z "$fs" ]; then
    fail "poll_req_fail_stop was not found: this contract would be vacuous"
else
    if ! grep -qE '^_Noreturn static void$' "$src" ||
       ! grep -qE '^poll_req_fail_stop\(const char \*what\)$' "$src"; then
        fail "poll_req_fail_stop is not declared _Noreturn"
    fi
    if ! printf '%s\n' "$fs" | grep -q '_exit(90);'; then
        fail "poll_req_fail_stop does not stop the process"
    fi
    if printf '%s\n' "$fs" | grep -qE '^[[:space:]]*return'; then
        fail "poll_req_fail_stop can return"
    fi
fi

# --- completion publication ----------------------------------------------
pc=$(fn_body poll_req_publish_completion)
if [ -z "$pc" ]; then
    fail "poll_req_publish_completion was not found"
else
    require_failstop_arms "completion" "$pc" 'pthread_mutex_lock\(&c->mu\)'
    require_failstop_arms "completion" "$pc" 'pthread_cond_wait\(&c->cv_done, &c->mu\)'
    require_failstop_arms "completion" "$pc" 'pthread_cond_broadcast\(&c->cv_done\)'
    require_failstop_arms "completion" "$pc" 'pthread_mutex_unlock\(&c->mu\)'
    # Order and interval: lock < set < broadcast < unlock, one unlock, and no
    # unlock before the set.
    l1=$(printf '%s\n' "$pc" | grep -n 'pthread_mutex_lock(&c->mu)' | head -1 | cut -d: -f1)
    st=$(printf '%s\n' "$pc" | grep -n 'c->done = 1;' | head -1 | cut -d: -f1)
    bc=$(printf '%s\n' "$pc" | grep -n 'pthread_cond_broadcast(&c->cv_done)' | head -1 | cut -d: -f1)
    ul=$(printf '%s\n' "$pc" | grep -n 'pthread_mutex_unlock(&c->mu)' | cut -d: -f1)
    if [ -z "$l1" ] || [ -z "$st" ] || [ -z "$bc" ] || [ -z "$ul" ]; then
        fail "completion publication is missing a step"
    else
        if [ "$(printf '%s\n' "$ul" | wc -l | tr -d ' ')" -ne 1 ]; then
            fail "completion publication releases its mutex more than once"
        elif [ "$l1" -ge "$st" ] || [ "$st" -ge "$bc" ] || [ "$bc" -ge "$ul" ]; then
            fail "completion publication is not acquire, set, broadcast, unlock \
in that order"
        fi
    fi
    if ! printf '%s\n' "$pc" | grep -q 'while (c->hold_done && !c->waiter_in_wait)'; then
        fail "the completion seam is missing"
    fi
fi

# --- release publication -------------------------------------------------
pr=$(fn_body poll_req_publish_release)
if [ -z "$pr" ]; then
    fail "poll_req_publish_release was not found"
else
    require_failstop_arms "release" "$pr" 'pthread_mutex_lock\(&c->mu\)'
    require_failstop_arms "release" "$pr" 'pthread_cond_broadcast\(&c->cv_released\)'
    require_failstop_arms "release" "$pr" 'pthread_mutex_unlock\(&c->mu\)'
    l1=$(printf '%s\n' "$pr" | grep -n 'pthread_mutex_lock(&c->mu)' | head -1 | cut -d: -f1)
    st=$(printf '%s\n' "$pr" | grep -n 'c->released = 1;' | head -1 | cut -d: -f1)
    bc=$(printf '%s\n' "$pr" | grep -n 'pthread_cond_broadcast(&c->cv_released)' | head -1 | cut -d: -f1)
    ul=$(printf '%s\n' "$pr" | grep -n 'pthread_mutex_unlock(&c->mu)' | cut -d: -f1)
    if [ -z "$l1" ] || [ -z "$st" ] || [ -z "$bc" ] || [ -z "$ul" ]; then
        fail "release publication is missing a step"
    else
        if [ "$(printf '%s\n' "$ul" | wc -l | tr -d ' ')" -ne 1 ]; then
            fail "release publication releases its mutex more than once"
        elif [ "$l1" -ge "$st" ] || [ "$st" -ge "$bc" ] || [ "$bc" -ge "$ul" ]; then
            fail "release publication is not acquire, set, broadcast, unlock \
in that order"
        fi
    fi
    # The release operation touches the context only through the four
    # primitives and the assignment.
    if printf '%s\n' "$pr" | grep -E 'c->' |
       grep -vE 'pthread_mutex_lock\(&c->mu\)|pthread_mutex_unlock\(&c->mu\)|pthread_cond_broadcast\(&c->cv_released\)|c->released = 1;' |
       grep -q .; then
        fail "release publication touches the context beyond its own protocol"
    fi
    # After the closing unlock the waiter may already have destroyed the
    # context, so the successful tail contains NO context access -- not even
    # a repeat of the assignment made under the lock.
    if [ -n "$ul" ] && [ "$(printf '%s\n' "$ul" | wc -l | tr -d ' ')" -eq 1 ] &&
       printf '%s\n' "$pr" | awk -v ul="$ul" 'NR > ul' | grep -q 'c->'; then
        fail "release publication touches the context after its unlock"
    fi
fi

# --- poll_req_main: release is penultimate, return is last -----------------
pm=$(fn_body poll_req_main)
if [ -z "$pm" ]; then
    fail "poll_req_main was not found"
else
    # Statements only: strip blank lines, comment lines and the braces.
    stmts=$(printf '%s\n' "$pm" | grep -vE '^[[:space:]]*$|^[[:space:]]*/?\*|^[[:space:]]*\*/|^\{$|^\}$|^poll_req_main\(|^static void \*$')
    last2=$(printf '%s\n' "$stmts" | tail -2 | tr '\n' '|')
    if [ "$last2" != "    poll_req_publish_release(c);|    return NULL;|" ]; then
        fail "poll_req_main does not end with the release publication followed \
by the return; its last statements are: ${last2}"
    fi
    if [ "$(printf '%s\n' "$stmts" | grep -c 'poll_req_publish_release(c);')" -ne 1 ] ||
       [ "$(printf '%s\n' "$stmts" | grep -c 'poll_req_publish_completion(c);')" -ne 1 ]; then
        fail "poll_req_main does not publish completion and release exactly once each"
    fi
    if printf '%s\n' "$pm" | grep -qE 'c->(done|released) ='; then
        fail "poll_req_main publishes a fact directly instead of through its \
operation"
    fi
fi

# --- the waits are strict and on the right conditions --------------------
w=$(fn_body await_request_released)
if [ -z "$w" ]; then
    fail "await_request_released was not found"
else
    if ! printf '%s\n' "$w" | grep -q 'pthread_cond_timedwait(&c->cv_released, &c->mu'; then
        fail "the release is not awaited on B's own condition"
    fi
    if printf '%s\n' "$w" | grep -qE 'moqr_admin_listen_test_(wait_)?view'; then
        fail "the release wait consults the view publication"
    fi
    if ! printf '%s\n' "$w" | awk '/pthread_cond_timedwait\(&c->cv_/ { on = 1; next } on { print; if (/^[[:space:]]*}/) { exit } }' |
         grep -q 'return false;'; then
        fail "a refused or timed-out wait does not return a non-event: an \
expired deadline could authorise teardown"
    fi
fi
d=$(fn_body await_request_done)
if [ -z "$d" ]; then
    fail "await_request_done was not found"
else
    if ! printf '%s\n' "$d" | grep -q 'pthread_cond_timedwait(&c->cv_done, &c->mu'; then
        fail "completion is not awaited on the worker's own condition"
    fi
    if ! printf '%s\n' "$d" | awk '/pthread_cond_timedwait\(&c->cv_/ { on = 1; next } on { print; if (/^[[:space:]]*}/) { exit } }' |
         grep -q 'return false;'; then
        fail "a refused or timed-out completion wait does not return a \
non-event: an expired deadline could stand in for being told"
    fi
fi

# --- one operation owns the transition, and nobody else detaches ---------
o=$(fn_body poll_req_await_release_and_detach_or_stop)
if [ -z "$o" ]; then
    fail "the integrated release/detach operation was not found"
else
    if ! printf '%s\n' "$o" | grep -qE 'if[[:space:]]*\([[:space:]]*!await_request_released\(c, deadline\)[[:space:]]*\)'; then
        fail "the transition does not make the detach depend on the release event"
    fi
    if ! printf '%s\n' "$o" | grep -q '_exit(90);'; then
        fail "a missing release does not stop the process before teardown"
    fi
    stop_ln=$(printf '%s\n' "$o" | grep -n '_exit(90);' | head -1 | cut -d: -f1)
    det_ln=$(printf '%s\n' "$o" | grep -n 'owned_thread_release_detach(' | head -1 | cut -d: -f1)
    if [ -z "$det_ln" ] || [ "$det_ln" -lt "$stop_ln" ]; then
        fail "the detach is not on the branch the release event reaches"
    fi
fi
calls=$(grep -c 'owned_thread_release_detach(' "$src")
if [ "$calls" -ne 2 ]; then
    fail "owned_thread_release_detach appears $calls time(s): expected its \
definition and exactly one call, inside the integrated operation"
fi
for case_fn in test_poll_ack_names_what_it_took test_completion_signal_is_required; do
    c=$(fn_body "$case_fn")
    if [ -z "$c" ]; then
        fail "$case_fn was not found"
        continue
    fi
    if printf '%s\n' "$c" | grep -qE 'owned_thread_join(_or_stop)?\(&th(_b)?,'; then
        fail "$case_fn joins the worker; a join after completion is unbounded"
    fi
    if ! printf '%s\n' "$c" | grep -q 'poll_req_await_release_and_detach_or_stop('; then
        fail "$case_fn does not relinquish the worker through the integrated \
operation"
    fi
done

if [ "$failures" -ne 0 ]; then
    echo "FAIL: check_request_lifetime ($failures)" >&2
    exit 1
fi
echo "PASS: check_request_lifetime"
