#!/usr/bin/env bash
#
# Source authority for the admin listener's structural contracts.
#
# Three of the listener's properties cannot be decided by the boundary test,
# because reaching them needs a scheduling state the test cannot force: whether
# a wake is gated on terminality, whether a generation is frozen only when a
# bank exists, and whether the broker is held by reference. A property that
# cannot be observed at runtime is still worth pinning -- at the source, and
# honestly labelled as such rather than implied by a green suite.
#
# Every pattern here is plain POSIX ERE. \s and \b are GNU extensions that BSD
# grep does not honour, and a pattern that silently matches nothing is a guard
# that silently passes.
set -u

lst=${1:-}
lay=${2:-}
mn=${3:-}
cc=${4:-}
shift 4 2>/dev/null || true
cc_incs=("$@")
[ -f "$lst" ] || { echo "FAIL: listener source not found: $lst" >&2; exit 1; }
[ -f "$lay" ] || { echo "FAIL: layout header not found: $lay" >&2; exit 1; }
[ -f "$mn" ] || { echo "FAIL: main not found: $mn" >&2; exit 1; }

failures=0
fail() { echo "FAIL: $1" >&2; failures=$((failures + 1)); }

# Occurrences, not matching lines: two calls on one line are two calls.
count_occurrences() { # file, ERE
    grep -oE "$2" "$1" 2>/dev/null | wc -l | tr -d ' '
}

# -- the patterns must discriminate ----------------------------------------
# Each is proven against a known-bad line before it is trusted. A guard whose
# pattern cannot match is indistinguishable from a guard that passes.
probe=$(mktemp) || { echo "FAIL: no temp file" >&2; exit 1; }
trap 'rm -f "$probe"' EXIT

printf 'moqr_broker_t broker;\n' > "$probe"
if [ "$(count_occurrences "$probe" 'moqr_broker_t[ ]+[a-z_]+;')" != "1" ]; then
    fail "the embedded-broker pattern does not match a known-bad line"
fi
printf '    moqr_admin_destroy(&l->admin);\n' > "$probe"
if [ "$(count_occurrences "$probe" 'moqr_admin_destroy[ ]*\(')" != "1" ]; then
    fail "the destroy pattern does not match a known-bad line"
fi

# -- one broker, held by reference -----------------------------------------
# A listener-private broker is a second identity space: its serials, banks and
# releases would be unrelated to the ones the signal path uses, so a generation
# opened for a scrape would be invisible to the lanes.
if [ "$(count_occurrences "$lay" 'moqr_broker_t[ ]+[a-z_]+;')" != "0" ]; then
    fail "the layout embeds a moqr_broker_t by value -- a second identity space"
fi
if [ "$(count_occurrences "$lay" 'moqr_broker_t[ ]*\*[ ]*broker;')" != "1" ]; then
    fail "the layout does not hold exactly one broker BY REFERENCE"
fi
if [ "$(count_occurrences "$lst" 'moqr_broker_init[ ]*\(')" != "0" ]; then
    fail "the listener initialises a broker of its own"
fi
if [ "$(count_occurrences "$lst" 'moqr_broker_destroy[ ]*\(')" != "0" ]; then
    fail "the listener destroys a broker it does not own"
fi

# -- the state machine is destroyed exactly once ---------------------------
if [ "$(count_occurrences "$lst" 'moqr_admin_destroy[ ]*\(')" != "1" ]; then
    fail "moqr_admin_destroy is not called exactly once"
fi

# -- no lane wake after terminality ----------------------------------------
# The wake callback reaches the lanes. Once the facade is terminal they will
# never pump again, so a wake issued then is work nobody can complete.
if ! grep -A6 'consume_wake(struct moqr_admin_listen' "$lst" |
     grep -qE 'atomic_load\(&l->terminal\)'; then
    fail "consume_wake does not gate on terminality before invoking the sink"
fi

# -- a generation is frozen only when a bank exists ------------------------
# Freezing a generation with nowhere to render it strands it: it is READY, has
# no storage, and nothing can move it until an unrelated pin happens to free.
if ! grep -B4 -F 'rc = collect(' "$lst" |
     grep -qE 'moqr_admin_bank_available'; then
    fail "owner_produce collects without first checking bank availability"
fi

# -- cancellation belongs to the owner -------------------------------------
# stop() is called by another thread. If it cancelled the state machine itself,
# two threads would mutate it at once -- which is exactly the defect the
# structural review found.
if grep -n 'moqr_admin_listen_stop' -A20 "$lst" |
   grep -qE 'moqr_admin_(cancel|close|tick|refuse|commit|abort)[ ]*\('; then
    fail "moqr_admin_listen_stop mutates the state machine from the caller's thread"
fi

# -- one owner, in the process that wires it ------------------------------
# When the endpoint exists it owns every broker transaction. The serve loops
# must therefore stand down: both the latched-signal conversion and the
# coordinator call are gated on the endpoint being absent. Neither gate can be
# reached by a test -- they need a real transport -- so they are pinned here.
if [ "$(count_occurrences "$mn" 'admin_l == NULL && atomic_exchange\(&g_signal_metrics_pending')" != "1" ]; then
    fail "the multi-lane loop converts the signal latch without standing down for the endpoint"
fi
# `state_owed` is itself gated on the endpoint being absent, so the coordinator
# and its lane wakes are both owned by exactly one thread.
if [ "$(count_occurrences "$mn" 'admin_l == NULL && moqr_broker_busy')" != "1" ]; then
    fail "the multi-lane loop runs the coordinator alongside the endpoint's owner"
fi
if [ "$(count_occurrences "$mn" 'if \(admin_l != NULL\) \{')" -lt 1 ]; then
    fail "the single-lane loop does not stand down for the endpoint"
fi
# The endpoint is joined before the facade stops, the snapshot is destroyed or
# the broker is torn down -- everything its owner thread reads.
if [ "$(count_occurrences "$mn" 'admin_endpoint_stop\(admin_l')" != "2" ]; then
    fail "the endpoint is not shut down in exactly the two serve compositions"
fi
# Each shutdown must PRECEDE its facade stop: the owner thread has to have
# settled every bank while the facade, the snapshot and the broker it reads are
# all still alive. Proximity is the testable form of "before" here -- a stop
# moved below the facade's own stop no longer has one following it.
sites_ok=$(awk '
    /admin_endpoint_stop\(admin_l/ { window = 10; seen = 0; next }
    window > 0 {
        if ($0 ~ /moq(_wtquic)?_msquic_managed_stop\(/) { seen = 1 }
        window--
        if (window == 0) { total += seen }
    }
    END { print total + 0 }' "$mn")
if [ "$sites_ok" != "2" ]; then
    fail "an endpoint shutdown does not precede its facade stop ($sites_ok of 2 do)"
fi

# -- the endpoint is not built where it would change SIGUSR1 ---------------
# The verify composition's signal projection is RELAY_BLOCKED_V0. Enabling the
# endpoint there would replace one operator-facing document with another
# without anyone asking, so that build refuses the section instead.
if [ "$(count_occurrences "$mn" 'MOQR_ADMIN_ENDPOINT_AVAILABLE')" -lt 2 ]; then
    fail "the verify build's SIGUSR1 contract is not protected from the endpoint"
fi

# -- disabled means nothing is owned ---------------------------------------
# With the section absent there must be no socket, no thread, no bank and no
# owner rows or views -- which is what makes the disabled ceiling exact.
if ! grep -A8 'admin_owner_init(admin_owner_t' "$mn" |
     grep -qE 'cfg->admin.enabled'; then
    fail "the admin owner context is built without checking that admin is enabled"
fi

# -- the ceiling gate precedes the endpoint and every readiness line --------
cap_line=$(grep -n 'print_capacity(cfg, sizeof(serve_lanes_ctx_t),' "$mn" |
           head -1 | cut -d: -f1)
start_line=$(grep -n 'admin_endpoint_start(cfg' "$mn" | tail -1 | cut -d: -f1)
ready_line=$(grep -n 'moqr_cli_serve_log_readiness(&log, cfg, MOQR_CLI_LOG_COMP_LANES' "$mn" |
             head -1 | cut -d: -f1)
if [ -z "$cap_line" ] || [ -z "$start_line" ] || [ -z "$ready_line" ]; then
    fail "the multi-lane startup order cannot be located"
elif [ "$cap_line" -gt "$start_line" ] || [ "$cap_line" -gt "$ready_line" ]; then
    fail "the multi-lane ceiling gate runs after the endpoint or its readiness line"
fi

# -- the publication notification comes from the publish point --------------
# Not from the wake callback, which only ASKS lanes to publish later.
if [ "$(count_occurrences "$mn" 'admin_notify_published\(&ctx')" != "2" ]; then
    fail "lane publication does not notify the endpoint in both compositions"
fi
for fn in admin_wake_all serve_wake_every_shard serve_wake_lane0; do
    if grep -A12 "^$fn(void \*" "$mn" | grep -q 'moqr_admin_listen_notify'; then
        fail "$fn notifies the owner -- a wake asks lanes to publish, it does not publish"
    fi
done

# -- broker-driven wakes belong to the owner --------------------------------
if [ "$(count_occurrences "$mn" 'admin_l == NULL && moqr_broker_busy')" != "1" ]; then
    fail "the multi-lane loop wakes lanes for broker work the endpoint owner already owns"
fi

# -- no destructive take without a durable carrier --------------------------
# The poisoned arm must not take a broker token: the state machine accepts a
# recorded take only once a retirement has been claimed, so a take here would
# consume an identity nothing can carry.
if grep -A14 'POISONED. Nothing is taken here.' "$lst" |
   grep -qE 'moqr_admin_record_take'; then
    fail "the poisoned arm records a take before a retirement is claimed"
fi

# -- SIGPIPE is suppressed per socket, never process-wide -------------------
# Pinned here because it cannot be pinned by behaviour on every platform: a
# vanished loopback peer is reported as ECONNRESET on this host, which does not
# raise SIGPIPE, so removing the guard is not observable locally. It is still
# the difference between a scraper being able to end the relay and not, on the
# platforms where a write to a departed peer is EPIPE.
if ! grep -A6 'accept(l->listen_fd' "$lst" | grep -qE 'set_nosigpipe\(fd\)'; then
    fail "an accepted admin socket is admitted without SIGPIPE suppression"
fi
if grep -qE 'signal\(SIGPIPE' "$lst"; then
    fail "the listener changes the process-global SIGPIPE disposition"
fi
# The self-pipe is not a socket; the client and refusal descriptors are.
for sock in 'refuse_fd' 'client_fd\[s\]'; do
    if [ "$(count_occurrences "$lst" "[^_a-z]write\($sock")" != "0" ]; then
        fail "an admin socket is written with an unprotected write()"
    fi
done

# -- the endpoint is inert until the caller says otherwise ------------------
# Both serve paths must install the lane-visible pointer BEFORE activating the
# endpoint, and must not print readiness until activation reports the owner is
# serving. Behaviour proves activation itself; what only the source can show is
# that both compositions do it in that order.
for ctxref in 'ctx.admin_listen' 'ctx->admin_listen'; do
    store=$(grep -n "atomic_store_explicit(&$ctxref, admin_l" "$mn" | head -1 |
            cut -d: -f1)
    act=$(grep -n "admin_endpoint_activate(admin_l)" "$mn" |
          sed -n "$( [ "$ctxref" = 'ctx.admin_listen' ] && echo 1 || echo 2 )p" |
          cut -d: -f1)
    if [ -z "$store" ] || [ -z "$act" ]; then
        fail "a serve path does not install the lane pointer and activate"
    elif [ "$store" -gt "$act" ]; then
        fail "a serve path activates the endpoint before installing the lane pointer"
    fi
done
if [ "$(count_occurrences "$mn" 'admin_endpoint_activate\(admin_l\)')" != "2" ]; then
    fail "the endpoint is not activated in exactly the two serve compositions"
fi

# -- an unprovable owner halts instead of unwinding -------------------------
# A live owner reaches the snapshot, the broker and the lane facades. There is
# no correct partial teardown, so the only handled outcome is to stop.
if ! grep -A10 'rc = moqr_admin_listen_stop(l);' "$mn" |
     grep -qE 'MOQR_ERR_WOULD_BLOCK'; then
    fail "the unprovable-join outcome is not handled where the endpoint stops"
fi
# The halt is one named operation. It must be reached from the stop branch, it
# must be declared _Noreturn, and it must stop the process itself: a halt that
# could return would hand the caller back into the teardown it exists to
# prevent. Its report may not be a prerequisite for stopping -- nothing in it
# may go through the blocking standard streams on the stopping thread.
if ! grep -A16 'rc = moqr_admin_listen_stop(l);' "$mn" | grep -qE 'admin_halt\('; then
    fail "an unprovable owner does not halt the process before teardown"
fi
if ! grep -B1 '^admin_halt(const char \*msg)' "$mn" | grep -qE '^_Noreturn static void'; then
    fail "admin_halt is not declared _Noreturn"
fi
halt_body=$(awk '/^admin_halt\(const char \*msg\)/ { on = 1; next } on { print; if (/^}/) { exit } }' "$mn")
if ! printf '%s\n' "$halt_body" | grep -qE '_exit\('; then
    fail "admin_halt does not stop the process"
fi
if printf '%s\n' "$halt_body" | grep -qE '\breturn\b'; then
    fail "admin_halt can return"
fi
if printf '%s\n' "$halt_body" | grep -qE 'fprintf\(|fflush\(|fputs\(|printf\('; then
    fail "admin_halt reports through blocking stdio on the stopping thread"
fi
# The halting thread shares nothing with its reporter that could hold it: no
# mutex, no condition, no join. Its grace is one MONOTONIC deadline.
if printf '%s\n' "$halt_body" | grep -qE 'pthread_mutex_|pthread_cond_|pthread_join\('; then
    fail "admin_halt waits on a reporter-shared lock, condition or join"
fi
if ! printf '%s\n' "$halt_body" | grep -q 'CLOCK_MONOTONIC'; then
    fail "admin_halt's grace is not measured on the monotonic clock"
fi
if printf '%s\n' "$halt_body" | grep -q 'CLOCK_REALTIME'; then
    fail "admin_halt's grace is measured on the wall clock"
fi
if ! grep -A12 'rc = moqr_admin_listen_activate(l);' "$mn" | grep -qE 'admin_halt\('; then
    fail "a failed activation with a live owner does not take the same halt"
fi

# -- the callback wiring exists in exactly one place ------------------------
if [ "$(count_occurrences "$mn" 'admin_fill_listen_cfg\(')" != "3" ]; then
    fail "the endpoint's callback wiring is not a single shared function"
fi

# -- no state-machine result is discarded -----------------------------------
# Two of these branches are unreachable while the broker and the state machine
# agree, so behaviour cannot reach them; what the source CAN show is that the
# result is consumed at all rather than cast away. The classification for each
# is stated beside the call.
for call in 'moqr_admin_tick' 'moqr_admin_close' 'moqr_admin_on_bytes' \
            'moqr_admin_on_written' 'moqr_admin_refuse' \
            'moqr_admin_bind_serial' 'moqr_admin_fail_serial'; do
    if [ "$(count_occurrences "$lst" "\(void\)$call")" != "0" ]; then
        fail "$call has a discarded result"
    fi
done
# The wrappers are what the code actually calls, so the guard has to see THOSE
# too. Exactly four discards are classified best-effort: the storage-failure
# and commit-failure cleanups, where an unconditional named terminal follows on
# the very next line and the cleanup exists only for balanced teardown. Any
# other discarded wrapper result is a classification nobody wrote down.
n_disc=$(count_occurrences "$lst" '\(void\)admin_[a-z_]*\(')
if [ "$n_disc" != "4" ]; then
    fail "expected exactly 4 classified best-effort wrapper discards, found $n_disc"
fi
for d in $(grep -n '(void)admin_[a-z_]*(' "$lst" | cut -d: -f1); do
    if ! awk -v s="$d" 'NR>=s && NR<=s+3' "$lst" | grep -q 'owner_fail('; then
        fail "a discarded wrapper result at line $d is not followed by a named terminal"
    fi
done

# -- activation failure tears down in the SAME order as a normal stop ------
# Clearing the lane-visible pointer is not a grace period: a lane may already
# have loaded it. Every facade that could have loaded it is joined first, then
# the pointer is cleared, and only then can the endpoint object be released.
activation_order=$(awk '
    /if \(admin_endpoint_activate\(admin_l\) != MOQR_OK\)/ {
        arm++; active=1; raw=0; wt=0; cleared=0; destroyed=0; next
    }
    active {
        if (/moq_wtquic_msquic_managed_destroy\(ctx->wt\)/ && !cleared)
            wt=1
        if (/moq_msquic_managed_destroy\(t\)/ && !cleared)
            raw=1
        if (/atomic_store_explicit\(&ctx(->|\.)admin_listen, NULL/) {
            if (!raw || (arm == 2 && !wt)) bad=1
            cleared=1
        }
        if (/admin_endpoint_destroy\(admin_l/) {
            if (!cleared) bad=1
            destroyed=1
        }
        if (/return 1;/ && destroyed) {
            if (!raw || !cleared || (arm == 2 && !wt)) bad=1
            active=0
        }
    }
    END { printf "%d %d", arm + 0, bad + 0 }
' "$mn")
if [ "$activation_order" != "2 0" ]; then
    fail "activation-failure reader-grace order is incomplete ($activation_order)"
fi

# -- the owner spans the full resolved dual-facade shard domain -------------
# Labels alone are not enough: passing cfg->lanes here would make the owner
# render only the raw rows even though the label builder produced WT rows too.
if ! grep -A3 -F 'admin_owner_init(&admin_owner, cfg, &snap, ctx->labels,' "$mn" |
     grep -qF 'plan.total_shards'; then
    fail "the multi-lane admin owner is not sized to plan.total_shards"
fi

# --- the clock operations are one checked critical section each -----------
#
# Choosing which clock to read and reading it must happen under one hold of
# the view lock, and arming must take THE SAME lock so it cannot land between
# the choice and the sample. Two edits defeat any runtime probe while leaving
# it satisfied: release the real mutex early and leave a second, unreachable
# release later in the text, or arm without taking the lock at all. Neither is
# a state a runtime oracle can see.
#
# So the shape is pinned semantically rather than by token order: each of these
# functions takes the checked boundary exactly ONCE, releases it exactly ONCE,
# and has no `return` between the two. An implementation that released early
# would need a second release; one that returned early would leave the lock
# held. Dead text cannot satisfy a count of one.
clock_fn_body() {
    awk -v fn="$1" '
        index($0, fn "(struct moqr_admin_listen") == 1 { on = 1 }
        on { print }
        on && /^}$/ { exit }' "$lst"
}

for fn in clock_now_us clock_arm_raw clock_advance_raw; do
    body=$(clock_fn_body "$fn")
    if [ -z "$body" ]; then
        fail "$fn was not found: this contract would be vacuous"
        continue
    fi
    if printf '%s\n' "$body" | grep -qE 'pthread_mutex_(lock|unlock)\(&l->view_mu\)'; then
        fail "$fn takes or releases view_mu directly instead of through the \
checked boundary"
    fi
    takes=$(printf '%s\n' "$body" | grep -c 'view_lock_for_clock(l)')
    rels=$(printf '%s\n' "$body" | grep -c 'view_unlock_for_clock(l)')
    if [ "$takes" -ne 1 ]; then
        fail "$fn takes the checked boundary $takes time(s), expected exactly one"
    fi
    if [ "$rels" -ne 1 ]; then
        fail "$fn releases the checked boundary $rels time(s), expected exactly \
one; a second release lets an early one hide behind it"
    fi
    if printf '%s\n' "$body" | grep -qE 'view_held[[:space:]]*=|view_holder[[:space:]]*='; then
        fail "$fn writes the ownership marker itself instead of letting the \
checked boundary do it"
    fi
    take_ln=$(printf '%s\n' "$body" | grep -n 'view_lock_for_clock(l)' | head -1 | cut -d: -f1)
    rel_ln=$(printf '%s\n' "$body" | grep -n 'view_unlock_for_clock(l)' | head -1 | cut -d: -f1)
    if [ -z "$take_ln" ] || [ -z "$rel_ln" ] || [ "$rel_ln" -le "$take_ln" ]; then
        fail "$fn releases the boundary before it takes it"
        continue
    fi
    # The take is guarded, and its refusal arm returns without the lock. That
    # arm is named exactly so it can be excluded here rather than blindly
    # skipped: everything after it runs with the lock held.
    guard=$(printf '%s\n' "$body" | sed -n "$take_ln,$((take_ln + 2))p")
    if [ "$(printf '%s\n' "$guard" | tr -d ' \n')" != \
         "if(!view_lock_for_clock(l)){returnfalse;}" ]; then
        fail "$fn does not take the checked boundary in the guarded form the \
rest of this contract depends on"
        continue
    fi
    # Nothing may leave the function while the lock is held.
    inner=$(printf '%s\n' "$body" | sed -n "$((take_ln + 3)),$((rel_ln - 1))p")
    if printf '%s\n' "$inner" | grep -qE '^[[:space:]]*return'; then
        fail "$fn returns while holding the checked boundary"
    fi
done

# The probe's release goes through the failpoint wrapper, and the wrapper both
# performs the real release and reports its result.
#
# This PINS THE SHAPE only. The effect -- that a refused release increments the
# counter the verdict reads -- is proved at runtime by
# `probe_records_a_refused_release`, which exercises the real arm through a
# one-shot injected refusal. A source check cannot establish that control flow
# reaches a statement, and this one does not claim to.
probe_fn=$(awk '
    index($0, "clock_lock_probe(struct moqr_admin_listen") == 1 { on = 1 }
    on { print }
    on && /^}$/ { exit }' "$lst")
rel_fn=$(awk '
    index($0, "probe_release(struct moqr_admin_listen") == 1 { on = 1 }
    on { print }
    on && /^}$/ { exit }' "$lst")
if [ -z "$probe_fn" ] || [ -z "$rel_fn" ]; then
    fail "clock_lock_probe or probe_release was not found: this contract \
would be vacuous"
else
    if printf '%s\n' "$probe_fn" | grep -qE 'pthread_mutex_unlock\(&l->view_mu\)'; then
        fail "clock_lock_probe releases the view lock directly instead of \
through the failpoint wrapper"
    fi
    if ! printf '%s\n' "$probe_fn" | grep -qE 'if[[:space:]]*\([[:space:]]*probe_release\(l\)[[:space:]]*!=[[:space:]]*0[[:space:]]*\)'; then
        fail "clock_lock_probe does not check the release it performs"
    fi
    if ! printf '%s\n' "$rel_fn" | grep -qE 'pthread_mutex_unlock\(&l->view_mu\)'; then
        fail "probe_release does not perform the real release"
    fi
fi

# A counter accessor may only release what it acquired.
#
# Ignoring the acquisition and unlocking anyway hands back a hold the caller
# never took -- on the error-checking view lock, an accessor called from inside
# a critical section would release that section's own lock and heal the very
# state a test is about to measure.
acc_fn=$(awk '
    index($0, "moqr_admin_listen_test_clock_probe(struct moqr_admin_listen") == 1 { on = 1 }
    on { print }
    on && /^}$/ { exit }' "$lst")
if [ -z "$acc_fn" ]; then
    fail "the clock-probe accessor was not found: this contract would be vacuous"
else
    if printf '%s\n' "$acc_fn" | grep -qE '\(void\)[[:space:]]*pthread_mutex_lock\(&l->view_mu\)'; then
        fail "the clock-probe accessor discards its acquisition and would \
release a lock it may not hold"
    fi
    if ! printf '%s\n' "$acc_fn" |
         grep -qE 'if[[:space:]]*\([[:space:]]*pthread_mutex_lock\(&l->view_mu\)[[:space:]]*!=[[:space:]]*0[[:space:]]*\)'; then
        fail "the clock-probe accessor does not check its acquisition"
    fi
fi

# The poll adoption block holds ONE lock across capture, drain and
# acknowledgement, through the checked boundary. A raw release there is the
# exact shape that lets a newer request's wake be drained while the request
# stays pending, and no runtime oracle can see a lock that was never taken.
poll_region=$(awk '
    /ADOPT A REQUESTED POLL MODE, THEN SAY SO/ { on = 1 }
    on { print }
    on && /if \(poll\(p, n, poll_ms\)/ { exit }' "$lst")
if [ -z "$poll_region" ]; then
    fail "the poll adoption block was not found: this contract would be vacuous"
else
    if printf '%s\n' "$poll_region" |
       grep -qE 'pthread_mutex_(lock|unlock)\(&l->view_mu\)'; then
        fail "the poll adoption block takes or releases view_mu directly \
instead of through the checked boundary"
    fi
    if [ "$(printf '%s\n' "$poll_region" | grep -c 'view_lock_for_clock(l)')" -ne 1 ] ||
       [ "$(printf '%s\n' "$poll_region" | grep -c 'view_unlock_for_clock(l)')" -ne 1 ]; then
        fail "the poll adoption block does not take and release the checked \
boundary exactly once"
    fi
    if printf '%s\n' "$poll_region" | grep -qE '^[[:space:]]*wake_drain\(l\);'; then
        fail "the poll adoption block drains without measuring the lock \
around the drain"
    fi
    if [ "$(printf '%s\n' "$poll_region" | grep -c 'poll_drain_measured(l)')" -ne 1 ]; then
        fail "the poll adoption block does not take its drain fact from the \
measured drain"
    fi
    drain_ln=$(printf '%s\n' "$poll_region" | grep -n 'poll_drain_measured(l)' | head -1 | cut -d: -f1)
    rel_ln=$(printf '%s\n' "$poll_region" | grep -n 'view_unlock_for_clock(l)' | head -1 | cut -d: -f1)
    if [ -z "$drain_ln" ] || [ -z "$rel_ln" ] || [ "$drain_ln" -gt "$rel_ln" ]; then
        fail "the wake drain is not inside the poll adoption block's hold"
    fi
fi

# The real sample must be inside clock_now_us's critical section.
body=$(clock_fn_body clock_now_us)
take_ln=$(printf '%s\n' "$body" | grep -n 'view_lock_for_clock(l)' | head -1 | cut -d: -f1)
rel_ln=$(printf '%s\n' "$body" | grep -n 'view_unlock_for_clock(l)' | head -1 | cut -d: -f1)
sample_ln=$(printf '%s\n' "$body" | grep -n 'clock_gettime(' | head -1 | cut -d: -f1)
probe_ln=$(printf '%s\n' "$body" | grep -n 'clock_lock_probe(l)' | head -1 | cut -d: -f1)
if [ -z "$sample_ln" ] || [ -z "$probe_ln" ]; then
    fail "clock_now_us no longer samples the real clock or no longer probes"
elif [ "$sample_ln" -lt "$take_ln" ] || [ "$sample_ln" -gt "$rel_ln" ] ||
     [ "$probe_ln" -lt "$take_ln" ] || [ "$probe_ln" -gt "$rel_ln" ]; then
    fail "the real clock sample or its probe sits outside the checked boundary"
fi

# The arm's sample must be inside its critical section too.
body=$(clock_fn_body clock_arm_raw)
take_ln=$(printf '%s\n' "$body" | grep -n 'view_lock_for_clock(l)' | head -1 | cut -d: -f1)
rel_ln=$(printf '%s\n' "$body" | grep -n 'view_unlock_for_clock(l)' | head -1 | cut -d: -f1)
sample_ln=$(printf '%s\n' "$body" | grep -n 'clock_gettime(' | head -1 | cut -d: -f1)
if [ -z "$sample_ln" ]; then
    fail "the arm no longer samples a floor"
elif [ "$sample_ln" -lt "$take_ln" ] || [ "$sample_ln" -gt "$rel_ln" ]; then
    fail "the arm samples its floor outside the checked boundary"
fi

if [ "$failures" -ne 0 ]; then
    echo "FAIL: $failures listener contract violation(s)" >&2
    exit 1
fi
echo "PASS: listener structural contracts"
