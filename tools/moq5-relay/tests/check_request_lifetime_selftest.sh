#!/usr/bin/env bash
# The lifetime authority is itself under test.
#
# A source authority that accepts a broken source is worse than none, because
# it lends the break a green tick. So the real checker is driven against
# temporary copies of the real source, each carrying exactly one mutation, and
# it must reject every one for the rule the mutation breaks -- and accept the
# pristine source. A mutation that fails to apply, or applies more than once,
# is a failed self-test: it proves nothing about the authority.
#
# No network, no sleeps, no elapsed-time verdicts: every case is a file and a
# checker run.
set -u
checker="${1:-}"
src="${2:-}"
[ -n "$checker" ] && [ -n "$src" ] || {
    echo "usage: $0 <check_request_lifetime.sh> <tests/test_relay_admin_listen.c>" >&2
    exit 2
}
[ -r "$checker" ] && [ -r "$src" ] || { echo "FAIL: cannot read inputs" >&2; exit 1; }

failures=0
cases=0
fail() { echo "FAIL: $*" >&2; failures=$((failures + 1)); }

tmp=$(mktemp -d) || { echo "FAIL: no temporary directory" >&2; exit 1; }
trap 'rm -rf "$tmp"' EXIT

# mutate <name> <old> <new> : writes $tmp/<name>.c, or fails the self-test.
mutate() {
    local name="$1" old="$2" new="$3" n
    n=$(python3 -B - "$src" "$old" <<'PY'
import sys
s = open(sys.argv[1]).read()
print(s.count(sys.argv[2]))
PY
)
    if [ "$n" != "1" ]; then
        fail "self-test mutation '$name' matches the source $n time(s), expected exactly once"
        return 1
    fi
    python3 -B - "$src" "$old" "$new" "$tmp/$name.c" <<'PY'
import sys
s = open(sys.argv[1]).read()
open(sys.argv[4], "w").write(s.replace(sys.argv[2], sys.argv[3], 1))
PY
}

# expect_reject <name> <rule-fragment>
expect_reject() {
    local name="$1" rule="$2" out
    cases=$((cases + 1))
    [ -f "$tmp/$name.c" ] || return
    out=$(bash "$checker" "$tmp/$name.c" 2>&1)
    if [ $? -eq 0 ]; then
        fail "authority accepted mutant '$name'"
    elif ! printf '%s\n' "$out" | grep -q -- "$rule"; then
        fail "authority rejected mutant '$name' but not for its rule ('$rule'); it said: $(printf '%s\n' "$out" | grep -m1 'FAIL:')"
    fi
}

# --- pristine must be accepted -------------------------------------------
cases=$((cases + 1))
if ! bash "$checker" "$src" >/dev/null 2>&1; then
    fail "authority rejects the pristine source"
fi

# --- failure arms that return, and a context access after the release -----
mutate V1_release_unlock_arm_returns \
'    if (pthread_mutex_unlock(&c->mu) != 0) {
        poll_req_fail_stop("could not release the mutex the release was "
                           "published under");
    }
}' \
'    if (pthread_mutex_unlock(&c->mu) != 0) {
        return;
    }
}' && expect_reject V1_release_unlock_arm_returns "release: the failure arm of pthread_mutex_unlock"

mutate V2_release_broadcast_arm_returns \
'    if (pthread_cond_broadcast(&c->cv_released) != 0) {
        poll_req_fail_stop("could not signal the release");
    }' \
'    if (pthread_cond_broadcast(&c->cv_released) != 0) {
        return;
    }' && expect_reject V2_release_broadcast_arm_returns "release: the failure arm of pthread_cond_broadcast"

mutate V3_completion_acquire_arm_empty \
'    if (pthread_mutex_lock(&c->mu) != 0) {
        poll_req_fail_stop("could not take the mutex to publish completion");
    }' \
'    if (pthread_mutex_lock(&c->mu) != 0) {
        ;
    }' && expect_reject V3_completion_acquire_arm_empty "completion: the failure arm of pthread_mutex_lock"

mutate V4_context_access_after_release \
'    poll_req_publish_release(c);
    return NULL;' \
'    poll_req_publish_release(c);
    fprintf(stderr, "%d", c->done);
    return NULL;' && expect_reject V4_context_access_after_release "does not end with the release publication followed by the return"

mutate S9_release_context_after_unlock \
'    if (pthread_mutex_unlock(&c->mu) != 0) {
        poll_req_fail_stop("could not release the mutex the release was "
                           "published under");
    }
}' \
'    if (pthread_mutex_unlock(&c->mu) != 0) {
        poll_req_fail_stop("could not release the mutex the release was "
                           "published under");
    }
    c->released = 1;
}' && expect_reject S9_release_context_after_unlock "touches the context after its unlock"

# --- the completion seam may not publish after a refused wait -------------
mutate S1_seam_wait_breaks \
'        if (pthread_cond_wait(&c->cv_done, &c->mu) != 0) {
            poll_req_fail_stop("the completion seam'"'"'s wait refused");
        }' \
'        if (pthread_cond_wait(&c->cv_done, &c->mu) != 0) {
            break;
        }' && expect_reject S1_seam_wait_breaks "completion: the failure arm of pthread_cond_wait"

# --- release interval ----------------------------------------------------
mutate S2_release_early_unlock \
'    c->released = 1;' \
'    (void)pthread_mutex_unlock(&c->mu);
    c->released = 1;' && expect_reject S2_release_early_unlock "release publication releases its mutex more than once"

mutate S3_release_unchecked_unlock \
'    if (pthread_mutex_unlock(&c->mu) != 0) {
        poll_req_fail_stop("could not release the mutex the release was "
                           "published under");
    }' \
'    (void)pthread_mutex_unlock(&c->mu);' && expect_reject S3_release_unchecked_unlock "release: no checked call to pthread_mutex_unlock"

# --- completion interval --------------------------------------------------
mutate S4_completion_missing_broadcast \
'    if (pthread_cond_broadcast(&c->cv_done) != 0) {
        poll_req_fail_stop("could not signal completion");
    }' \
'' && expect_reject S4_completion_missing_broadcast "completion: no checked call to pthread_cond_broadcast"

mutate S5_completion_break_and_reread \
'        if (pthread_cond_timedwait(&c->cv_done, &c->mu, &at) != 0) {
            (void)pthread_mutex_unlock(&c->mu);
            printf("  %s: the second request never returned\n", who);
            return false;
        }' \
'        if (pthread_cond_timedwait(&c->cv_done, &c->mu, &at) != 0) {
            break;
        }' && expect_reject S5_completion_break_and_reread "timed-out completion wait does not return a non-event"

# --- the transition ------------------------------------------------------
mutate S6_direct_detach \
'        poll_req_await_release_and_detach_or_stop(&ctx_b, &th_b, &txn,
                                                  "pollseam B");' \
'        owned_thread_release_detach(&th_b, "pollseam B");' && expect_reject S6_direct_detach "owned_thread_release_detach appears 3 time(s)"

mutate S7_release_result_ignored \
'    if (!await_request_released(c, deadline)) {' \
'    (void)await_request_released(c, deadline);
    if (0) {' && expect_reject S7_release_result_ignored "does not make the detach depend on the release event"

mutate S8_release_wait_break_and_reread \
'        if (pthread_cond_timedwait(&c->cv_released, &c->mu, &at) != 0) {
            (void)pthread_mutex_unlock(&c->mu);
            return false;
        }' \
'        if (pthread_cond_timedwait(&c->cv_released, &c->mu, &at) != 0) {
            break;
        }' && expect_reject S8_release_wait_break_and_reread "timed-out wait does not return a non-event"

if [ "$failures" -ne 0 ]; then
    echo "FAIL: check_request_lifetime_selftest ($failures of $cases cases)" >&2
    exit 1
fi
echo "PASS: check_request_lifetime_selftest ($cases cases)"
