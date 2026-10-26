#!/bin/sh
# Is the direct release publication bound to the native inputs it ships?
#
# The topology audit recomputes the LibMoQ input digest and refuses images that
# do not correspond to the current sources. This driver asks whether the
# PUBLICATION honours that refusal, by running the real sequence:
#
#   1. baseline   the audit passes; the input manifest and digest are computed
#                 through lib-inputs.sh and checked against BOTH receipts; a
#                 publication into a fresh repository is inventoried against the
#                 version authority and the audited images
#   2. change     alter ONE declared native input -- a comment in a hashed
#                 source -- leaving every native OUTPUT untouched
#   3. refusal    the recomputed digest must MOVE, and the audit must reject
#                 BOTH required ABIs with the exact refusal line
#   4. publish    invoke the real publication task BY ITSELF, warm outputs,
#                 ordinary up-to-date behaviour, into a second fresh, disjoint
#                 repository. No aggregate check first
#   5. verdict    a correct refusal carries the complete expected diagnostic
#                 line for both ABIs, no unknown diagnostic of any kind, and an
#                 empty repository. Anything else is a failure or is
#                 inconclusive -- never a satisfied contract
#
# It is a RED driver: on the current product it FAILS by name, and that failure
# is the finding. When the contract exists it passes without modification.
#
# WHAT THIS DRIVER OWNS, AND WHAT IT DOES NOT
#
# Every command runs in its own session, so a bound or a signal reaches the
# whole tree it started -- the wrapper, its shell, the Gradle client and tee --
# and nothing outside it. Escalation is TERM, then KILL, each with its own
# deadline, and the perturbed input is restored only after the group is
# observably quiescent. The membership of every owned group is recorded.
#
# The Gradle build JVM is INSIDE that boundary. `--no-daemon` forks a single-use
# daemon, and it is forked by this driver's own launcher, so it is a member of
# the owned group -- which is asserted by MEASUREMENT, from the recorded group
# membership of each publication, not inferred from JAVA_HOME. Requiring a JDK
# that satisfies the project's pin is only what stops Gradle from having to
# provision one; membership is what the record shows.
#
# A long-lived SHARED daemon is a different thing: it starts as its own session
# leader, so it can never be a member of an owned group, and nothing here
# signals one. If Gradle ever ran this build on such a daemon, that work would
# be outside this driver's reach, and the recorded membership is what would show
# it.
#
# The exclusion lock is keyed on the file this driver mutates and is observed by
# THIS DRIVER ONLY. It stops two invocations from perturbing the same input at
# once. It does not and cannot exclude Gradle, the topology producer, or any
# other tool: the workflow's one-producer-at-a-time discipline still applies.
#
# `--selftest` runs the harness controls instead: disposable subjects, scripted
# logs, synthetic repositories and fixture children that exercise the live
# supervision, classification, digest and evidence-ownership decisions. Those
# controls run no real Gradle and never touch a product file; their output is
# headed as harness evidence and is not the real Gradle RED.
#
# EVIDENCE IS KEPT. Commands, exit statuses, logs, input manifests, receipt
# hashes, artifact inventories, the quiescence record and the classifier verdict
# are written to a per-invocation evidence directory that is never deleted and
# whose path is printed. An evidence directory that already exists is REFUSED,
# not reused and not cleared.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd) || { echo "$_self: cannot resolve bindings/kotlin" >&2; exit 2; }
repo=$(cd "$root/../.." && pwd) || { echo "$_self: cannot resolve the worktree root" >&2; exit 2; }

# shellcheck source=/dev/null
. "$here/lib-declaration.sh" || { echo "$_self: cannot source the declaration helper" >&2; exit 2; }
# shellcheck source=/dev/null
. "$here/lib-inputs.sh" || { echo "$_self: cannot source the input inventory helper" >&2; exit 2; }

# Independent literals, not read back from the build script under test.
GROUP='org.openmoq'
ARTIFACT='moq5-android'
ABIS="arm64-v8a x86_64"
MEMBERS="jni/arm64-v8a/libmoq5_jni.so jni/x86_64/libmoq5_jni.so"
VERSION_HEADER="$repo/core/include/moq/version.h"

# Bounds. Deadlines, not timing assertions: a command that finishes sooner is
# never penalised, and only a command that overruns is stopped.
CMD_TIMEOUT=${MOQ5_PUBBIND_TIMEOUT:-1800}   # per owned command
TERM_GRACE=8                                # TERM to KILL
KILL_GRACE=8                                # KILL to observed quiescence

usage() {
    echo "usage: $_self [--selftest [--section <name>]] [--harness] [--subject <file>] [--evidence <new-dir>] [--evidence-parent <dir>]" >&2
    exit 2
}

mode=real
section=""
harness=0
subject_override=""
evidence_arg=""
evidence_parent=""
while [ $# -gt 0 ]; do
    case $1 in
        --selftest) mode=selftest; shift ;;
        --section)  [ $# -ge 2 ] || usage; section=$2; shift 2 ;;
        --harness)  harness=1; shift ;;
        --subject)  [ $# -ge 2 ] || usage; subject_override=$2; shift 2 ;;
        --evidence) [ $# -ge 2 ] || usage; evidence_arg=$2; shift 2 ;;
        --evidence-parent) [ $# -ge 2 ] || usage; evidence_parent=$2; shift 2 ;;
        *) usage ;;
    esac
done

# Test-only injections exist so the harness can drive the LIVE decisions. They
# are refused outside harness mode, so a stray environment variable can never
# quietly alter a real measurement.
if [ "$harness" -ne 1 ]; then
    for _v in MOQ5_PUBBIND_FAULT MOQ5_PUBBIND_FIXTURE MOQ5_PUBBIND_STUB_PUBLISH \
              MOQ5_PUBBIND_TIMEOUT MOQ5_PUBBIND_MARKER \
              MOQ5_PUBBIND_STUB_AUDIT MOQ5_PUBBIND_STUB_RECEIPTS \
              MOQ5_PUBBIND_LOCK_BARRIER \
              MOQ5_PUBBIND_BREAK_SETSID MOQ5_PUBBIND_BREAK_FIXTURE \
              MOQ5_PUBBIND_PS MOQ5_PUBBIND_BLIND_AFTER MOQ5_PUBBIND_LOCK_ROOT \
              MOQ5_PUBBIND_HARNESS_PS MOQ5_PUBBIND_BARRIER_BOUND MOQ5_PUBBIND_NO_ARM \
              MOQ5_PUBBIND_CHILD_PS MOQ5_PUBBIND_READER MOQ5_PUBBIND_INVENTORY_APPEND \
              MOQ5_PUBBIND_CLEANUP_RECORDER MOQ5_PUBBIND_PARSER_PROBE \
              MOQ5_PUBBIND_SETTLE_RECORDER MOQ5_PUBBIND_ACK_CLEANUP_PROBE \
              MOQ5_PUBBIND_ACK_STOP_BEFORE_CLEANUP; do
        eval "_x=\${$_v-}"
        [ -z "$_x" ] || { echo "$_self: $_v is set without --harness; refusing to run" >&2; exit 2; }
    done
    [ -z "$subject_override" ] || { echo "$_self: --subject requires --harness" >&2; exit 2; }
fi
FAULT=${MOQ5_PUBBIND_FAULT-}
fault_is() {
    for _fw in $FAULT; do [ "$_fw" = "$1" ] && return 0; done
    return 1
}
FIXTURE=${MOQ5_PUBBIND_FIXTURE-}
STUB=${MOQ5_PUBBIND_STUB_PUBLISH-}

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

# ------------------------------------------------------------ evidence, fresh

if [ -n "$evidence_arg" ] && [ -n "$evidence_parent" ]; then
    echo "$_self: give --evidence or --evidence-parent, not both" >&2; exit 2
fi
[ -n "$evidence_arg" ] || evidence_arg=${MOQ5_PUBBIND_EVIDENCE-}
if [ -n "$evidence_arg" ]; then
    if [ -e "$evidence_arg" ]; then
        echo "$_self: the evidence directory already exists: $evidence_arg" >&2
        echo "$_self: refusing to reuse or clear it; name a new directory" >&2
        exit 2
    fi
    evidence=$evidence_arg
else
    [ -n "$evidence_parent" ] || evidence_parent=${TMPDIR:-/tmp}
    mkdir -p "$evidence_parent" || { echo "$_self: cannot use $evidence_parent" >&2; exit 2; }
    evidence="$evidence_parent/pubbind-$(date -u +%Y%m%dT%H%M%SZ)-$$"
    [ ! -e "$evidence" ] || { echo "$_self: $evidence already exists" >&2; exit 2; }
fi
mkdir -p "$(dirname "$evidence")" || { echo "$_self: cannot create the evidence parent" >&2; exit 2; }
mkdir "$evidence" || { echo "$_self: cannot create $evidence" >&2; exit 2; }
# The ownership inventory exists from the outset, so a later absence is a fault
# rather than an ambiguity, and a run that owns nothing still has a complete,
# provably empty inventory to settle.
# The marker is what makes this file an OWNERSHIP authority rather than a file
# that happens to be called ownership.txt: synthetic parser inputs never carry
# it, so no consumer can mistake test data for a group this driver owns.
printf 'owned-by=moq5-pubbind-driver\n' > "$evidence/ownership.txt" \
    || { echo "$_self: cannot initialise the ownership inventory" >&2; exit 2; }

# ------------------------------------------------------- bounded supervision

# One inline supervisor. It becomes a session leader FIRST and proves it, so the
# process group it leads is exactly the tree this driver started; if that cannot
# be established it starts no worker at all. Completion is then read from the
# STATUS FILE, never from `kill -0`, which a zombie answers.
SUPERVISOR='
import os, signal, subprocess, sys
status_path, log_path = sys.argv[1], sys.argv[2]
def refuse(reason, code):
    with open(status_path, "w") as f:
        f.write(reason + "\n")
    sys.exit(code)
if os.environ.get("MOQ5_PUBBIND_BREAK_SETSID") == "1":
    refuse("ownership-setup-failed", 70)
try:
    os.setsid()
except OSError:
    try:
        os.setpgrp()
    except OSError:
        refuse("ownership-setup-failed", 70)
if os.getpgrp() != os.getpid():
    refuse("ownership-setup-failed", 70)
held = {}
def _stop(signum, frame):
    p = held.get("proc")
    if p is not None:
        try: p.terminate()
        except Exception: pass
signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)
with open(log_path, "wb") as log:
    held["proc"] = subprocess.Popen(sys.argv[3:], stdout=log, stderr=subprocess.STDOUT)
    rc = held["proc"].wait()
if rc < 0:
    rc = 128 - rc
with open(status_path, "w") as f:
    f.write("%d\n" % rc)
sys.exit(rc)
'

# The ownership state. `owned_group` is the group this driver is answerable for
# and is cleared ONLY when that group is observed empty. `ownership_failed` is
# sticky: once cleanup has failed or become unobservable, no later call can turn
# it back into success, and nothing that depends on a clean tree may proceed.
owned_pid=""
owned_group=""
ownership_failed=0
quiescent=1

now() { date +%s; }

ownership_ok() {
    [ "$ownership_failed" -eq 0 ] && [ -z "$owned_group" ] && [ "$quiescent" -eq 1 ]
}

# Observation fails CLOSED. An inspection that cannot be made is reported as
# `unobservable`, never as `empty`: absent output is not evidence of absence.
# The inspection command itself, so a control can take the driver's sight away
# by giving it one that fails -- and the REAL status and emptiness decisions are
# what then have to answer for it.
ps_cmd() {
    if [ "$harness" -eq 1 ] && [ -n "${MOQ5_PUBBIND_PS-}" ]; then
        printf '%s' "${MOQ5_PUBBIND_PS}"
    else
        printf 'ps'
    fi
}
proc_probe() {
    _pp=$("$(ps_cmd)" -Ao pid=,stat= 2>/dev/null) || { printf 'unobservable'; return 0; }
    [ -n "$_pp" ] || { printf 'unobservable'; return 0; }
    _pr=$(printf '%s\n' "$_pp" | awk -v p="$1" '
        { gsub(/^ +/, "", $1) }
        $1 == p && $2 !~ /^Z/ { found = 1 }
        END { print found ? "alive" : "gone" }') || { printf 'unobservable'; return 0; }
    case "$_pr" in alive|gone) printf '%s' "$_pr" ;; *) printf 'unobservable' ;; esac
}

group_probe() {
    _gp=$("$(ps_cmd)" -Ao pgid=,stat= 2>/dev/null) || { printf 'unobservable'; return 0; }
    [ -n "$_gp" ] || { printf 'unobservable'; return 0; }
    _gr=$(printf '%s\n' "$_gp" | awk -v g="$1" '
        { gsub(/^ +/, "", $1) }
        $1 == g && $2 !~ /^Z/ { found = 1 }
        END { print found ? "live" : "empty" }') || { printf 'unobservable'; return 0; }
    case "$_gr" in live|empty) printf '%s' "$_gr" ;; *) printf 'unobservable' ;; esac
}

# Everything in an owned group, recorded untruncated so the boundary can be
# audited: a shared Gradle daemon must never appear in one.
group_members() {
    ps -Ao pid=,pgid=,command= 2>/dev/null \
        | awk -v g="$1" '{ gsub(/^ +/, "", $1) } $2 == g { print }'
}

# KILL every member of an owned group except its supervisor, which is asked to
# stand down instead: a supervisor killed outright would leave the shell to
# announce the job, and the announcement is not a diagnostic anyone should read.
kill_group_except() {
    ps -Ao pid=,pgid= 2>/dev/null \
        | awk -v g="$1" -v me="$2" '{ gsub(/^ +/, "", $1) } $2 == g && $1 != me { print $1 }' \
        | while read -r _kp; do kill -KILL "$_kp" 2>/dev/null || true; done
}

# Reap the direct child only when it is provably gone, so no path can block on
# an unbounded `wait` for a process that outlived every deadline.
reap_if_gone() {
    [ -n "$owned_pid" ] || return 0
    [ "$(proc_probe "$owned_pid")" = gone ] || return 1
    wait "$owned_pid" 2>/dev/null || true
    return 0
}

# Establish quiescence for the outstanding group, or record that it could not be
# established. Ownership is released ONLY on proof.
stop_owned() {
    if [ -n "$owned_group" ]; then
        _sg=$owned_group
        group_members "$_sg" >> "$evidence/stopped-groups.txt"
        kill -TERM "-$_sg" 2>/dev/null || kill -TERM "$owned_pid" 2>/dev/null || true
        _d=$(( $(now) + TERM_GRACE ))
        while [ "$(group_probe "$_sg")" != empty ]; do
            [ "$(now)" -lt "$_d" ] || break
            sleep 1
        done
        if [ "$(group_probe "$_sg")" != empty ]; then
            kill_group_except "$_sg" "$owned_pid"
            kill -TERM "$owned_pid" 2>/dev/null || true
            _d=$(( $(now) + KILL_GRACE ))
            while [ "$(group_probe "$_sg")" != empty ]; do
                [ "$(now)" -lt "$_d" ] || break
                sleep 1
            done
        fi
        reap_if_gone || true
        if [ "$(group_probe "$_sg")" != empty ]; then
            kill -KILL "$owned_pid" 2>/dev/null || true
            printf 'escalated to the supervisor pid %s\n' "$owned_pid" >> "$evidence/quiescence.txt"
            _d=$(( $(now) + KILL_GRACE ))
            while [ "$(group_probe "$_sg")" != empty ]; do
                [ "$(now)" -lt "$_d" ] || break
                sleep 1
            done
            reap_if_gone || true
        fi
        _final=$(group_probe "$_sg")
        {
            printf 'group=%s final=%s\n' "$_sg" "$_final"
            if [ -n "${MOQ5_PUBBIND_MARKER-}" ] && [ -f "${MOQ5_PUBBIND_MARKER}" ]; then
                printf 'marker_bytes_at_quiescence=%s\n' "$(wc -c < "${MOQ5_PUBBIND_MARKER}" | tr -d ' ')"
            fi
        } >> "$evidence/quiescence.txt"
        printf 'quiescence:%s\n' "$_final" >> "$evidence/order.txt"
        if [ "$_final" = empty ] && reap_if_gone; then
            owned_group=""
            owned_pid=""
        else
            ownership_failed=1
        fi
    fi
    if [ "$ownership_failed" -eq 1 ] || [ -n "$owned_group" ]; then
        quiescent=0
        return 1
    fi
    quiescent=1
    return 0
}

# run_owned <name> <cmd...> -- bounded, owned, recorded. 124 means it overran its
# deadline and was stopped; 125 that it never started; 126 that its ownership
# could not be established, in which case no worker was started at all.
run_owned() {
    _rn=$1; shift
    printf '%s\n' "$*" > "$evidence/$_rn.cmd"
    rm -f "$evidence/$_rn.status"
    python3 -c "$SUPERVISOR" "$evidence/$_rn.status" "$evidence/$_rn.log" "$@" &
    owned_pid=$!
    owned_group=$owned_pid
    quiescent=0
    # Observe the owned session before judging anything by it.
    _d=$(( $(now) + 10 ))
    while [ "$(group_probe "$owned_pid")" != live ]; do
        [ -f "$evidence/$_rn.status" ] && break
        [ "$(proc_probe "$owned_pid")" = alive ] || break
        [ "$(now)" -lt "$_d" ] || break
        sleep 1
    done
    printf 'session=%s observed=%s\n' "$owned_pid" "$(group_probe "$owned_pid")" >> "$evidence/ownership.txt"
    group_members "$owned_pid" > "$evidence/$_rn.group.raw"
    _d=$(( $(now) + CMD_TIMEOUT ))
    while :; do
        # Membership is sampled throughout and several times a second, not once
        # at the start: a build JVM forked later, and living only briefly, must
        # still appear in the record.
        group_members "$owned_pid" >> "$evidence/$_rn.group.raw"
        [ -f "$evidence/$_rn.status" ] && break
        [ "$(proc_probe "$owned_pid")" = alive ] || break
        if [ "$(now)" -ge "$_d" ]; then
            printf 'timeout after %ss\n' "$CMD_TIMEOUT" > "$evidence/$_rn.status"
            stop_owned || true
            return 124
        fi
        _n=0
        while [ "$_n" -lt 4 ]; do
            sleep 0.25 2>/dev/null || sleep 1
            group_members "$owned_pid" >> "$evidence/$_rn.group.raw"
            _n=$((_n + 1))
        done
    done
    sort -u "$evidence/$_rn.group.raw" > "$evidence/$_rn.group" 2>/dev/null || true
    _rc=$(cat "$evidence/$_rn.status" 2>/dev/null) || _rc=""
    # A command that exits is not a quiet tree: anything it left in its own group
    # is still this driver's to stop before the tree may be touched.
    if [ "$(group_probe "$owned_group")" = empty ] && reap_if_gone; then
        owned_group=""
        owned_pid=""
        quiescent=1
        printf 'quiescence:empty\n' >> "$evidence/order.txt"
    else
        printf 'survivors after exit: %s\n' "$_rn" >> "$evidence/quiescence.txt"
        stop_owned || true
    fi
    case "$_rc" in
        ownership-setup-failed) return 126 ;;
        ''|*[!0-9]*) printf 'launch-failure\n' > "$evidence/$_rn.status"; return 125 ;;
    esac
    return "$_rc"
}
# -------------------------------------------------------------- exclusion

# Keyed on the file this driver mutates, so two invocations cannot perturb the
# same input at once. Observed by this driver only -- see the header.
lock_dir=""
lock_held=0
lock_retained=0
# Read by release_lock, which runs from the traps, so they exist from the start.
restored=0
perturbed=0

# EXCLUSIVE CREATION, AND NO TAKEOVER. `mkdir` fails if anything already exists
# at the path -- a directory, a file, a symlink, dangling or not -- so a lock
# that is already there is refused whatever it looks like. The owner record
# written inside afterwards is DIAGNOSTIC ONLY: this driver never reads someone
# else's record to decide it may unlink their lock, because a read/check/unlink
# sequence is not atomic and because a dead pid does not mean the work it
# started has stopped.
#
# A lock left behind therefore means a previous invocation could not prove
# cleanup or restoration. Clearing it is an operator step -- verify the tree,
# put the source back from the retained backup, then remove the directory --
# not machinery here.
acquire_lock() {
    _lk=$(printf '%s' "$(cd "$(dirname "$1")" && pwd)/$(basename "$1")" | shasum -a 256 | cut -c1-16) || return 1
    _lroot=${TMPDIR:-/tmp}
    # A harness case that deliberately ends in quarantine keeps that quarantine
    # inside its OWN evidence, so it can neither outlive the run nor be
    # inherited by another.
    if [ "$harness" -eq 1 ] && [ -n "${MOQ5_PUBBIND_LOCK_ROOT-}" ]; then
        mkdir -p "${MOQ5_PUBBIND_LOCK_ROOT}" 2>/dev/null || true
        _lroot=${MOQ5_PUBBIND_LOCK_ROOT}
    fi
    lock_dir="$_lroot/moq5-pubbind-lock-$_lk"
    if ! mkdir "$lock_dir" 2>/dev/null; then
        fail "pubbind.exclusion" "$lock_dir already exists, so another invocation of this driver either holds $1 or left it unresolved; refusing. Resolve it by hand -- confirm nothing is still running, restore the source from that run's retained backup, then remove the directory"
        lock_dir=""
        return 1
    fi
    lock_held=1
    {
        printf 'pid=%s\n' "$$"
        printf 'evidence=%s\n' "$evidence"
        printf 'subject=%s\n' "$1"
        printf 'end\n'
    } > "$lock_dir/owner.record" 2>/dev/null || true
    [ -z "${MOQ5_PUBBIND_LOCK_BARRIER-}" ] || {
        : > "${MOQ5_PUBBIND_LOCK_BARRIER}.taken"
        _bd=$(( $(now) + 60 ))
        while [ ! -f "${MOQ5_PUBBIND_LOCK_BARRIER}" ]; do
            [ "$(now)" -lt "$_bd" ] || break
            sleep 1
        done
    }
    return 0
}

# Exclusion outlives the process when the source is not provably safe. The lock
# is given up only when cleanup is proved AND the source is back, so a run that
# leaves a perturbed input behind also leaves the next owner locked out.
release_lock() {
    [ "$lock_held" -eq 1 ] || return 0
    if ownership_ok && { [ "$perturbed" -eq 0 ] || [ "$restored" -eq 1 ]; }; then
        rm -rf "$lock_dir" 2>/dev/null || true
        lock_held=0
        return 0
    fi
    lock_retained=1
    printf 'retained=%s reason=cleanup_or_restoration_unproved\n' "$lock_dir" >> "$evidence/ownership.txt"
    fail "pubbind.exclusion" "the source is not provably back and the tree is not provably quiet, so the exclusion lock $lock_dir is RETAINED and no further invocation may take this subject; the backup is at ${backup:-(none taken)}"
    return 1
}

# ------------------------------------------------------------- restoration

PAIR_REPORT=1
pair_pass() { [ "$PAIR_REPORT" -eq 1 ] && pass "$1" "$2" || printf 'PASS[%s]: %s\n' "$1" "$2"; }
pair_fail() { [ "$PAIR_REPORT" -eq 1 ] && fail "$1" "$2" || printf 'FAIL[%s]: %s\n' "$1" "$2"; }

# restore_pair <file> <backup> <expected-sha256> <label>
# Puts the original back and PROVES it. A backup that is gone, or bytes that
# still differ, is a named failure -- and the backup is deliberately kept,
# because on that path it is the only original left.
restore_pair() {
    _rf=$1; _rb=$2; _rw=$3; _rl=$4
    if [ ! -f "$_rb" ]; then
        pair_fail "$_rl" "the backup is gone; the original cannot be restored"
        return 1
    fi
    cp "$_rb" "$_rf" 2>/dev/null || true
    _rn=$(decl_sha256 "$_rf" 2>/dev/null) || _rn=""
    if [ "$_rn" = "$_rw" ]; then
        pair_pass "$_rl" "the perturbed input is byte-identical again ($(printf '%s' "$_rw" | cut -c1-16))"
        return 0
    fi
    pair_fail "$_rl" "the perturbed input was NOT restored ($(printf '%s' "${_rn:-unreadable}" | cut -c1-16) != $(printf '%s' "$_rw" | cut -c1-16)); the recoverable backup is RETAINED at $_rb"
    return 1
}

# ------------------------------------------------------- diagnostic policy

# The one refusal line this contract may be recognised by.
expected_line() {
    printf 'FAIL[%s.provenance]: the recomputed LibMoQ input digest %s does not match the declared %s: the audited DSO does not correspond to these sources' \
        "$1" "$3" "$2"
}
expected_file() {
    _ef=$1; _ed=$2; _er=$3
    : > "$_ef" || return 1
    for _ea in $ABIS; do expected_line "$_ea" "$_ed" "$_er" >> "$_ef"; printf '\n' >> "$_ef"; done
}

# named_refusal <log> <expected-file>
# Every required ABI's COMPLETE line must appear as a whole line. Quoted,
# prefixed or embedded copies of the text are not observed refusals.
named_refusal() {
    [ -f "$1" ] && [ -f "$2" ] || return 1
    awk -v allow="$2" '
        BEGIN { while ((getline line < allow) > 0) { want[line] = 1; n++ } close(allow) }
        ($0 in want) { seen[$0] = 1 }
        END { c = 0; for (k in seen) c++; exit !(n > 0 && c == n) }' "$1"
}

# forbidden_diagnostic <log> <expected-file>
# Echoes the first line that must not accompany a refusal: any warning form,
# including a promoted GCC/Clang warning, any compiler-style error, and any
# named FAIL[...] diagnostic that is not the expected contract refusal. Normal
# Gradle failure framing carries none of those markers and is left alone.
forbidden_diagnostic() {
    [ -f "$1" ] || { printf 'INSPECTION-ERROR: the log %s cannot be read' "$1"; return 0; }
    [ -f "$2" ] || { printf 'INSPECTION-ERROR: the allowlist %s cannot be read' "$2"; return 0; }
    if ! _fd=$(awk -v allow="$2" '
        BEGIN { while ((getline line < allow) > 0) want[line] = 1; close(allow) }
        ($0 in want) { next }
        /^w: / || /[Ww]arning:/ || /WARNING/ || /-Werror/ || /\[-W/ { print; exit }
        /^FAIL\[/ { print; exit }
        /error:/ { print; exit }' "$1"); then
        printf 'INSPECTION-ERROR: the log %s could not be classified' "$1"
        return 0
    fi
    printf '%s' "$_fd"
}

# clean_log <log> -- the same policy with NOTHING allowed. The allowlist is read
# explicitly rather than by input-order, so an empty one permits nothing instead
# of turning every record of the log into an allowlist entry.
clean_log() {
    : > "$evidence/.empty-expected"
    forbidden_diagnostic "$1" "$evidence/.empty-expected"
}

# classify_publish <status> <log> <repo> <expected-file>
classify_publish() {
    _cs=$1; _cl=$2; _cr=$3; _ce=$4
    if [ "$_cs" -eq 0 ]; then
        printf 'published the publication succeeded'
        return 0
    fi
    if [ "$_cs" -eq 124 ]; then
        printf 'inconclusive the publication overran its deadline and was stopped'
        return 0
    fi
    if ! named_refusal "$_cl" "$_ce"; then
        printf 'inconclusive the failure does not carry the complete expected refusal line for both required ABIs with the measured digests'
        return 0
    fi
    _cd=$(forbidden_diagnostic "$_cl" "$_ce")
    if [ -n "$_cd" ]; then
        printf 'inconclusive the refusal is accompanied by a diagnostic this contract does not name: %s' "$_cd"
        return 0
    fi
    _cf=$(([ -d "$_cr" ] && find "$_cr" -type f 2>/dev/null || true) | sed -n '1,3p' | tr '\n' ' ')
    if [ -n "$_cf" ]; then
        printf 'wrote the refusal still wrote to the repository: %s' "$_cf"
        return 0
    fi
    printf 'refused the publication refused with the named provenance mismatch and wrote nothing'
}

# task_state <log> <task-path> -- Gradle's own header lines only. A task NAME in
# prose, a command line or a dependency report is not execution.
task_state() {
    awk -v t="> Task $2" '
        index($0, t) == 1 {
            rest = substr($0, length(t) + 1)
            if (rest == "") { print "executed"; found = 1; exit }
            if (rest == " UP-TO-DATE" || rest == " FROM-CACHE") { print "up-to-date"; found = 1; exit }
            if (rest == " SKIPPED") { print "skipped"; found = 1; exit }
            if (rest == " NO-SOURCE") { print "no-source"; found = 1; exit }
        }
        END { if (!found) print "absent" }
    ' "$1"
}

# --------------------------------------------------------------- authority

# The ONE version authority: the C library's own header. The published
# coordinate is pinned to it rather than inferred from whatever was written.
authority_version() {
    [ -f "$VERSION_HEADER" ] || return 1
    _av=$(sed -nE 's/^#define[[:space:]]+MOQ_VERSION_STRING[[:space:]]+"([^"]+)".*/\1/p' "$VERSION_HEADER" | head -1)
    [ -n "$_av" ] || return 1
    printf '%s' "$_av"
}

declared_digest() {
    # The harness stands in for the receipts so a control can exercise the digest
    # decision itself on a tree whose declaration this very script has moved. The
    # receipts row is exercised for real by the real sequence.
    if [ "$harness" -eq 1 ] && [ "${MOQ5_PUBBIND_STUB_RECEIPTS-}" = 1 ]; then
        printf '%s' "$d_base"
        return 0
    fi
    _dp="$root/.native/_topology/$1/topology.provenance"
    [ -f "$_dp" ] || return 1
    _dv=$(sed -n 's/^libmoq\.inputs\.sha256=//p' "$_dp")
    [ "$(printf '%s\n' "$_dv" | grep -c .)" -eq 1 ] || return 1
    decl_is_sha256 "$_dv" || return 1
    printf '%s' "$_dv"
}

audited_images() {
    for _ia in $ABIS; do
        _ih=$(decl_sha256 "$root/.native/_topology/$_ia/package/libmoq5_jni.so") || return 1
        printf 'jni/%s/libmoq5_jni.so=%s\n' "$_ia" "$_ih"
    done
}

# digest_of <label> -- the full manifest and its digest through lib-inputs.sh,
# retained as evidence. A failure-message scrape cannot represent success and so
# cannot be used here. The harness may fault the RESULT to drive the live guard.
digest_of() {
    _gs="$evidence/inputs-$1"
    mkdir -p "$_gs" || return 1
    if [ "$harness" -eq 1 ]; then
        case "$FAULT" in
            *baseline-digest-missing*) [ "$1" != before ] || { printf ''; return 0; } ;;
        esac
        case "$FAULT" in
            *digest-unchanged*) [ "$1" != after ] || { printf '%s' "$d_base"; return 0; } ;;
        esac
    fi
    inputs_digest "$repo" "$_gs"
}

# --------------------------------------------------------- artifact identity

# inventory <repo-dir> <out-file> <version> <images...>
# The exact GAV pinned by the version authority, exactly one product AAR,
# exactly the two required members, and every member's bytes equal to the
# audited image. Each refusal carries its own code so a control cannot be
# satisfied by a parser that merely failed.
inventory() {
    _iv_repo=$1; _iv_out=$2; _iv_ver=$3; shift 3
    python3 - "$_iv_repo" "$_iv_out" "$GROUP" "$ARTIFACT" "$_iv_ver" "$MEMBERS" "$@" <<'PY'
import hashlib, os, re, sys, zipfile

repo_dir, out, group, artifact, version, members_spec = sys.argv[1:7]
want = sorted(members_spec.split())
expected = {}
for pair in sys.argv[7:]:
    k, _, v = pair.partition("=")
    expected[k] = v

def die(code, detail):
    print("ERROR %s %s" % (code, detail))
    sys.exit(1)

if not re.match(r"^[0-9]+(\.[0-9]+)*$", version or ""):
    die("version-authority", "the version authority did not yield a version: %r" % version)
for name, value in sorted(expected.items()):
    if not re.match(r"^[0-9a-f]{64}$", value or ""):
        die("expected-image-invalid", "no valid audited digest for %s: %r" % (name, value))
if sorted(expected) != want:
    die("expected-image-invalid", "audited images %r do not cover the required members %r"
        % (sorted(expected), want))

base = os.path.join(repo_dir, *group.split("."), artifact)
if not os.path.isdir(base):
    die("coordinate-absent", "no artifact at %s:%s in %s" % (group, artifact, repo_dir))
versions = sorted(d for d in os.listdir(base) if os.path.isdir(os.path.join(base, d)))
if versions != [version]:
    die("wrong-version", "the authority pins %s; the repository holds %r" % (version, versions))

aars = []
for dirpath, _, filenames in os.walk(base):
    for fn in filenames:
        if fn.endswith(".aar"):
            aars.append(os.path.relpath(os.path.join(dirpath, fn), base))
expected_aar = "%s-%s.aar" % (artifact, version)
expected_rel = os.path.join(version, expected_aar)
if expected_rel not in aars:
    die("aar-absent", "no artifact at the exact coordinate path %s: %r"
        % (os.path.join(base, expected_rel), sorted(aars)))
extra = [a for a in aars if a != expected_rel]
if extra:
    die("extra-aar", "the repository holds product archives away from %s: %r" % (expected_rel, sorted(extra)))
if len(aars) != 1:
    die("extra-aar", "more than one product archive: %r" % sorted(aars))

aar = os.path.join(base, expected_rel)
try:
    z = zipfile.ZipFile(aar)
except Exception as exc:
    die("unreadable", "the artifact is not a readable archive: %s" % exc)
names = [n for n in z.namelist() if n.endswith(".so")]
if len(names) != len(set(names)):
    die("duplicate-member", "duplicate native members: %r" % sorted(names))
if sorted(names) != want:
    die("members", "native members are not exactly %r: %r" % (want, sorted(names)))

with open(aar, "rb") as f:
    lines = ["aar %s" % hashlib.sha256(f.read()).hexdigest()]
mismatched = []
for n in want:
    h = hashlib.sha256(z.read(n)).hexdigest()
    if h != expected[n]:
        mismatched.append("%s carries %s, not the audited %s" % (n, h, expected[n]))
    lines.append("%s %s" % (n, h))
if mismatched:
    die("image-mismatch", "; ".join(mismatched))

with open(out, "w") as f:
    f.write("\n".join(lines) + "\n")
print("\n".join(lines))
PY
}

# ------------------------------------------------------------- publication

# The JDK this project pins, read from its single source of truth. Nothing here
# names an installation path: WHERE a matching JDK lives is the caller's
# `JAVA_HOME`, exactly as tools/verify-toolchain.sh discovers it.
pinned_jdk_major() {
    _pj=$(sed -n 's/^jdk\.major=//p' "$root/gradle/toolchain.properties" | head -1)
    [ -n "$_pj" ] || return 1
    printf '%s' "$_pj"
}

# A build JVM this driver can own has to be forked from a JDK that satisfies the
# pin; otherwise Gradle would have to provision one, which is where the
# single-use daemon fails. The blocker is named rather than worked around by
# handing the build to a shared daemon nothing here owns.
check_build_jvm() {
    if [ "$harness" -eq 1 ] && [ -n "$STUB" ]; then
        printf 'build-jvm check skipped: the publication is stubbed\n' >> "$evidence/ownership.txt"
        return 0
    fi
    _want=$(pinned_jdk_major) || {
        fail "pubbind.build-jvm" "gradle/toolchain.properties declares no jdk.major"
        return 1
    }
    if [ -z "${JAVA_HOME-}" ]; then
        fail "pubbind.build-jvm" "JAVA_HOME is unset, so the publication would run on a shared daemon this driver does not own; set it to a JDK $_want"
        return 1
    fi
    if [ ! -x "$JAVA_HOME/bin/java" ]; then
        fail "pubbind.build-jvm" "JAVA_HOME=$JAVA_HOME has no executable bin/java"
        return 1
    fi
    _have=$("$JAVA_HOME/bin/java" -version 2>&1 | head -1 | sed 's/.*version "\([0-9][0-9]*\).*/\1/')
    if [ "$_have" != "$_want" ]; then
        fail "pubbind.build-jvm" "the pinned jdk.major is $_want but JAVA_HOME reports $_have; a single-use daemon would have to provision a JDK $_want and cannot"
        return 1
    fi
    printf 'java_home=%s major=%s\n' "$JAVA_HOME" "$_have" >> "$evidence/ownership.txt"
    pass "pubbind.build-jvm" "the build JVM is a JDK $_have from JAVA_HOME, so the no-daemon fork lands inside this driver's own session"
    return 0
}

# The real publication, alone, through the checked-in wrapper and the ordinary
# task graph, into a repository the caller names. `--no-daemon` so the build JVM
# is forked as a member of this driver's owned session rather than handed to a
# shared daemon.
gradle_publish() {
    if [ "$harness" -eq 1 ] && [ -n "$STUB" ]; then
        printf '%s\n' "$1" >> "$STUB/invocations"
        python3 - "$2" "$GROUP" "$ARTIFACT" "$(authority_version)" "$root" "$ABIS" <<'PY'
import os, sys, zipfile
repo_dir, group, artifact, version, root, abis = sys.argv[1:7]
d = os.path.join(repo_dir, *group.split("."), artifact, version)
os.makedirs(d, exist_ok=True)
with zipfile.ZipFile(os.path.join(d, "%s-%s.aar" % (artifact, version)), "w") as z:
    z.writestr("AndroidManifest.xml", "<manifest/>")
    for abi in abis.split():
        p = os.path.join(root, ".native", "_topology", abi, "package", "libmoq5_jni.so")
        z.write(p, "jni/%s/libmoq5_jni.so" % abi)
PY
        printf 'stubbed publication into %s\n' "$2" > "$evidence/$1.log"
        printf '0\n' > "$evidence/$1.status"
        return 0
    fi
    run_owned "$1" "$root/tools/gradlew-verified" \
        ":moq5-android:publishReleasePublicationToProbeRepository" \
        "-PmoqPublicationRepo=$2" --console=plain --no-daemon
}

# The topology audit, or -- in harness mode -- a fixture standing in for it, so
# the live supervision path can be driven with a child that ignores TERM or
# spawns a descendant.
run_audit() {
    if [ "$harness" -eq 1 ] && [ -n "$FIXTURE" ]; then
        if { fault_is fixture-audit && [ "$1" = audit-baseline ]; } \
           || { fault_is fixture-audit-late && [ "$1" = audit-perturbed ]; }; then
            run_owned "$1" /bin/sh "$FIXTURE"
            return $?
        fi
    fi
    if [ "$harness" -eq 1 ] && [ -n "${MOQ5_PUBBIND_STUB_AUDIT-}" ]; then
        printf 'stub audit for %s\n' "$1" > "$evidence/$1.cmd"
        if [ "$1" = audit-baseline ]; then
            printf 'PASS[stub.topology]: a harness stand-in for the topology audit\n' > "$evidence/$1.log"
            [ "${MOQ5_PUBBIND_STUB_AUDIT}" != diagnostic ] \
                || printf 'ld: warning: object file was built for a newer OS version\n' >> "$evidence/$1.log"
            printf '0\n' > "$evidence/$1.status"
            return 0
        fi
        cp "$evidence/expected-refusal.txt" "$evidence/$1.log"
        printf '1\n' > "$evidence/$1.status"
        return 1
    fi
    run_owned "$1" /bin/sh "$here/check-jni-topology.sh"
}

# ------------------------------------------------------------------ selftest

if [ "$mode" = selftest ]; then
    printf 'harness controls for the publication source-binding driver\n'
    printf '(no real Gradle runs and no product file is touched)\n\n'
    sx="$evidence/fixtures"; mkdir -p "$sx"
    driver="$here/$_self"
    ver=$(authority_version) || ver=""

    barrier() {   # bounded wait for a fixture to arm itself
        _bd=$(( $(now) + ${MOQ5_PUBBIND_BARRIER_BOUND:-90} ))
        while [ ! -f "$1" ]; do
            [ "$(now)" -lt "$_bd" ] || return 1
            sleep 1
        done
        return 0
    }
    cat > "$sx/stubborn.sh" <<'FIX'
trap '' TERM INT
: > "$MOQ5_PUBBIND_ARMED"
n=0
while [ "$n" -lt 180 ]; do printf 'x' >> "$MOQ5_PUBBIND_MARKER"; n=$((n + 1)); sleep 1; done
FIX
    cat > "$sx/descendant.sh" <<'FIX'
trap '' TERM INT
( trap '' TERM INT; n=0; while [ "$n" -lt 180 ]; do printf 'y' >> "$MOQ5_PUBBIND_MARKER.child"; n=$((n + 1)); sleep 1; done ) &
: > "$MOQ5_PUBBIND_ARMED"
n=0
while [ "$n" -lt 180 ]; do printf 'x' >> "$MOQ5_PUBBIND_MARKER"; n=$((n + 1)); sleep 1; done
FIX
    cat > "$sx/prompt.sh" <<'FIX'
: > "$MOQ5_PUBBIND_ARMED"
exit 7
FIX
    cat > "$sx/ps-blind" <<'FIX'
#!/bin/sh
exit 71
FIX
    cat > "$sx/ps-blind-after" <<'FIX'
#!/bin/sh
[ -f "$MOQ5_PUBBIND_BLIND_AFTER" ] && exit 71
exec /bin/ps "$@"
FIX
    cat > "$sx/ps-fail" <<'FIX'
#!/bin/sh
exit 71
FIX
    cat > "$sx/never-arms.sh" <<'FIX'
exit 0
FIX
    cat > "$sx/ps-pid-only" <<'FIX'
#!/bin/sh
case "$*" in
    *pgid*) exit 71 ;;
    *) exec /bin/ps "$@" ;;
esac
FIX
    cat > "$sx/reader-refuses" <<'FIX'
#!/bin/sh
exit 71
FIX
    cat > "$sx/reader-partial" <<'FIX'
#!/bin/sh
head -c 12 "$1" 2>/dev/null
exit 71
FIX
    cat > "$sx/reader-output-then-error" <<'FIX'
#!/bin/sh
cat "$1"
exit 71
FIX
    # The real observation plus one named live row, so a cleanup decision can be
    # driven without creating a process to decide about.
    cat > "$sx/ps-says-alive" <<'FIX'
#!/bin/sh
/bin/ps "$@" || exit 71
printf '%s S\n' "$MOQ5_PUBBIND_ALIVE_PID"
FIX
    # The real observation plus one extra live, session-leading row that belongs
    # to nothing this driver owns.
    cat > "$sx/ps-plus-unrelated" <<'FIX'
#!/bin/sh
/bin/ps "$@" || exit 71
printf '%s\n' '2147483645 S'
FIX
    # A fixture that owns TWO groups and records the second itself.
    cat > "$sx/two-groups.sh" <<'FIX'
trap '' TERM INT
# Ownership of the forked worker is established BEFORE that worker can become
# one. The child waits on a pipe and stands down on EOF, so it only ever reaches
# `exec` after its identity has been written to the cleanup LEDGER -- a separate,
# append-only authority the lifecycle logic never rewrites. A failed handoff is
# therefore not an abandoned worker: the child either never starts, or is stopped
# and reaped here. The receipts file is evidence about that lifecycle; the ledger
# is what a cleanup may act on.
#
# A failed acknowledgment is a SETUP FAILURE -- nothing is recorded, nothing is
# armed, no work starts. Only the direct child is ever signalled: an
# unacknowledged session is not an established group. An outcome the helper
# cannot establish is named unresolved and keeps the retained identity, so it is
# never read as a completed reap.
[ -z "$MOQ5_PUBBIND_ACK_LEDGER" ] \
    || printf 'ledger=moq5-pubbind-ack\n' > "$MOQ5_PUBBIND_ACK_LEDGER" \
    || exit 71
_second=$(python3 -c '
import errno, os, signal, sys, time

receipts = os.environ.get("MOQ5_PUBBIND_ACK_RECEIPTS", "")
ledger = os.environ.get("MOQ5_PUBBIND_ACK_LEDGER", "")
fault = os.environ.get("MOQ5_PUBBIND_ACK_FAULT", "")
skip_cleanup = os.environ.get("MOQ5_PUBBIND_ACK_SKIP_CLEANUP") == "1"
if fault not in ("", "signal-error", "wait-error", "bounds"):
    sys.stderr.write("unknown MOQ5_PUBBIND_ACK_FAULT: " + fault + "\n")
    os._exit(2)

def note(line):
    if not receipts:
        return
    fh = open(receipts, "a")
    fh.write(line + "\n")
    fh.close()

# The ledger is opened BEFORE the fork, so a ledger that cannot be written is a
# refusal rather than an owned child nobody recorded.
ledger_fd = -1
if ledger and not fault:
    try:
        ledger_fd = os.open(ledger, os.O_WRONLY | os.O_APPEND)
    except OSError as exc:
        sys.stderr.write("cannot open the ack ledger: %d\n" % (exc.errno or 0))
        os._exit(6)

def reap_now(p):
    """Bounded stop-and-reap of a child that must not be left behind."""
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.kill(p, sig)
        except OSError:
            pass
        for _ in range(40):
            try:
                if os.waitpid(p, os.WNOHANG)[0] == p:
                    return True
            except OSError:
                return False
            time.sleep(0.05)
    return False

# A mechanism probe exercises the failure control flow with no worker at all: in
# that mode nothing is forked, no signal is sent, no wait is made, and nothing is
# written to the cleanup ledger -- a probe that created nothing owns nothing.
owner_fd = os.environ.get("MOQ5_PUBBIND_OWNER_FD", "")
owner_support = os.environ.get("MOQ5_PUBBIND_OWNER_SUPPORT", "")
owner_reply = os.environ.get("MOQ5_PUBBIND_OWNER_REPLY", "")
if not fault and owner_reply and owner_fd.isdigit() and owner_support:
    # The worker is created by the RETAINED OWNER, over the channel this run
    # inherited, before it exists: the owner holds its wait authority and this
    # fixture never becomes its parent, so nothing here may signal or reap it.
    # The reply is handed back whole for the canonical writer to record; this
    # helper writes no identity of its own into the cleanup ledger.
    import subprocess
    call = subprocess.run(
        [sys.executable, "-B", os.path.join(owner_support, "owner_call.py"),
         "create", os.environ.get("MOQ5_PUBBIND_OWNER_SCOPE", "ack-owned"),
         os.environ.get("MOQ5_PUBBIND_OWNER_KEY", "ack-owned-key")],
        capture_output=True, universal_newlines=True,
        pass_fds=(int(owner_fd),))
    reply = call.stdout.strip()
    if call.returncode != 0 or len(reply.split()) != 4:
        sys.stderr.write("the retained owner did not create a worker: %s\n"
                         % (reply or call.stderr.strip()))
        os._exit(6)
    fh = open(owner_reply, "w")
    fh.write(reply + "\n")
    fh.close()
    note("ack=ok group=%s" % reply.split()[3])
    sys.stdout.write(reply.split()[3]); sys.stdout.flush()
    os._exit(0)

if fault:
    pid = int(os.environ.get("MOQ5_PUBBIND_ACK_FAKE_PID", "2147483646"))
else:
    gate_r, gate_w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(gate_w)
        go = os.read(gate_r, 1)
        os.close(gate_r)
        # EOF: the parent could not record this child, so it never becomes a
        # worker and there is nothing to own.
        if go != b"1":
            os._exit(0)
        os.setsid()
        _null = os.open(os.devnull, os.O_RDWR)
        os.dup2(_null, 0); os.dup2(_null, 1); os.dup2(_null, 2)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        try:
            os.execvp("sleep", ["sleep", "180"])
        finally:
            os._exit(127)
    os.close(gate_r)
    try:
        if os.environ.get("MOQ5_PUBBIND_ACK_HANDOFF_FAIL") == "1":
            raise OSError(errno.EIO, "injected handoff failure")
        if ledger_fd >= 0:
            os.write(ledger_fd, ("child=%d\n" % pid).encode())
        note("child=%d" % pid)
    except (OSError, IOError) as exc:
        # The child exists but its identity could not be handed over. Close the
        # gate so it stands down, then reap it here; this is an ownership exit.
        os.close(gate_w)
        done = reap_now(pid)
        sys.stderr.write("ack handoff failed for child %d (errno %d, reaped=%s)\n"
                         % (pid, exc.errno or 0, done))
        os._exit(5)
    os.write(gate_w, b"1")
    os.close(gate_w)

if fault:
    note("child=%d" % pid)
    note("probe=%s" % fault)
ok = False
if not fault and os.environ.get("MOQ5_PUBBIND_ACK_FAIL") != "1":
    for _ in range(100):
        try:
            if os.getpgid(pid) == pid:
                ok = True
                break
        except OSError:
            pass
        time.sleep(0.05)
if ok:
    note("ack=ok group=%d" % pid)
    sys.stdout.write(str(pid)); sys.stdout.flush(); os._exit(0)
note("ack=failed child=%d" % pid)
if skip_cleanup:
    note("outcome=unresolved reason=cleanup-skipped child=%d" % pid)
    os._exit(4)
for sig, name in ((signal.SIGTERM, "TERM"), (signal.SIGKILL, "KILL")):
    try:
        if fault == "signal-error":
            raise OSError(errno.EPERM, "injected signal failure")
        if not fault:
            os.kill(pid, sig)
    except ProcessLookupError:
        note("signal=%s target-already-gone" % name)
    except OSError as exc:
        note("outcome=unresolved reason=signal-%s-failed errno=%d child=%d"
             % (name, exc.errno or 0, pid))
        os._exit(4)
    else:
        note("signal=%s %s" % (name, "not-sent-probe" if fault else "sent"))
    for _ in range(40):
        try:
            if fault == "wait-error":
                raise OSError(errno.EIO, "injected wait failure")
            done, status = (0, 0) if fault else os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            note("outcome=unresolved reason=child-not-waitable child=%d" % pid)
            os._exit(4)
        except OSError as exc:
            note("outcome=unresolved reason=wait-failed errno=%d child=%d"
                 % (exc.errno or 0, pid))
            os._exit(4)
        if done == pid:
            note("wait=reaped child=%d status=%d" % (pid, status))
            note("outcome=reaped child=%d" % pid)
            os._exit(3)
        time.sleep(0.05)
note("outcome=unresolved reason=bounds-exhausted child=%d" % pid)
os._exit(4)
')
_helper=$?
[ -z "$MOQ5_PUBBIND_ACK_LEDGER" ] \
    || printf 'helper=%s\n' "$_helper" >> "$MOQ5_PUBBIND_ACK_LEDGER"
[ -z "$MOQ5_PUBBIND_ACK_RECEIPTS" ] \
    || printf 'helper=%s\n' "$_helper" >> "$MOQ5_PUBBIND_ACK_RECEIPTS"
if [ "$_helper" -ne 0 ]; then
    exit 70
fi
case "$_second" in
    ''|*[!0-9]*) exit 70 ;;
esac
# An owner-created worker is settled by its owner through the driver's cleanup
# path, so this helper arms nothing and adds no session of its own.
if [ -n "$MOQ5_PUBBIND_OWNER_REPLY" ] && [ -s "$MOQ5_PUBBIND_OWNER_REPLY" ]; then
    exit 0
fi
[ -z "$MOQ5_PUBBIND_INVENTORY_APPEND" ] \
    || printf 'session=%s observed=fixture-second-group\n' "$_second" >> "$MOQ5_PUBBIND_INVENTORY_APPEND"
: > "$MOQ5_PUBBIND_ARMED"
n=0
while [ "$n" -lt 180 ]; do printf 'x' >> "$MOQ5_PUBBIND_MARKER"; n=$((n + 1)); sleep 1; done
FIX
    chmod +x "$sx/ps-blind" "$sx/ps-blind-after" "$sx/ps-fail" "$sx/ps-pid-only" \
        "$sx/ps-plus-unrelated" "$sx/ps-says-alive" "$sx/reader-refuses" \
        "$sx/reader-partial" "$sx/reader-output-then-error"

    want() { [ -z "$section" ] || [ "$section" = "$1" ]; }
    run_section() {   # run_section <name> <log> [extra env...]
        # The evidence directory is named for the LOG, so two cases that drive
        # the same section keep separate, never-overwritten evidence.
        _rs=$1; _rl=$2; shift 2
        _rev="$sx/ev-$(basename "$_rl" .log)"
        rm -rf "$_rev"
        env MOQ5_PUBBIND_NO_ARM=1 MOQ5_PUBBIND_BARRIER_BOUND=3 "$@" \
            /bin/sh "$driver" --selftest --harness --section "$_rs" \
            --evidence "$_rev" > "$_rl" 2>&1
        _rrc=$?
        # Each identity is routed ONCE, against the inventory that declares it:
        # the child section's own evidence, or the fixture directory it left.
        for _rg in $(sed -n 's/^session=\([0-9]*\).*/\1/p' "$_rev/ownership.txt" 2>/dev/null); do
            cleanup_or_abort "$_rg" "selftest.stop-boundary-$_rs" \
                "the child section's fixture group" "$_rev"
        done
        for _rgd in "$_rev"/fixtures/ev-*; do
            [ -d "$_rgd" ] || continue
            for _rg in $(sed -n 's/^session=\([0-9]*\).*/\1/p' "$_rgd/ownership.txt" 2>/dev/null); do
                cleanup_or_abort "$_rg" "selftest.stop-boundary-$_rs" \
                    "a fixture the child section left" "$_rgd"
            done
        done
        return $_rrc
    }
    # Nothing may be reported after the abort line.
    nothing_after_abort() {
        awk '/HARNESS ABORTED/ { seen = 1; next }
             seen && (/^PASS\[selftest\./ || /^FAIL\[selftest\./) { bad = 1 }
             END { exit bad }' "$1"
    }

    child_ps_env() {
        [ -z "${MOQ5_PUBBIND_CHILD_PS-}" ] || printf 'MOQ5_PUBBIND_PS=%s' "${MOQ5_PUBBIND_CHILD_PS}"
    }
    # A control may ask for a fixture that never arms, so the barrier-failure
    # path is reached in seconds instead of after a real timeout.
    arming_fixture() {
        if [ -n "${MOQ5_PUBBIND_NO_ARM-}" ]; then printf '%s' "$sx/never-arms.sh"
        elif [ -n "${MOQ5_PUBBIND_INVENTORY_APPEND-}" ]; then printf '%s' "$sx/two-groups.sh"
        else printf '%s' "$sx/stubborn.sh"; fi
    }

    reaped_status=""
    # A `wait` whose duration nothing bounds is entered ONLY on proof that the
    # child is already gone. An inspection that cannot be made refuses; it never
    # falls through into that wait.
    reap_bounded() {
        _rd=$(( $(now) + ${2:-40} ))
        while :; do
            case "$(harness_proc_live "$1")" in
                gone)  break ;;
                alive) : ;;
                *)     reaped_status=unobservable; return 1 ;;
            esac
            [ "$(now)" -lt "$_rd" ] || { reaped_status=timeout; return 1; }
            sleep 1
        done
        wait "$1" 2>/dev/null
        reaped_status=$?
        return 0
    }
    # Terminate an owned child, then reap it under the same proof.
    stop_and_reap() {
        kill -TERM "$1" 2>/dev/null || true
        reap_bounded "$1" "${2:-60}" && return 0
        kill -KILL "$1" 2>/dev/null || true
        reap_bounded "$1" 20
    }
    # A cleanup that cannot be PROVED stops the harness where it stands. It is
    # not a flag a later launch might forget to consult: nothing after this
    # point runs, so no worker starts, nothing is restored and nothing is
    # unlocked. A case that needs to observe this behaviour runs it in an
    # isolated child, never by clearing the parent's unresolved state.
    # settle_or_abort <direct-child-pid|""> <group|""> <control> <what>
    #
    # An owned direct child and a separately owned fixture group are different
    # facts: waiting on the child says nothing about the group, and an empty
    # group says nothing about a child still to be reaped. Both are settled
    # here, and a settlement that cannot be PROVED stops the harness where it
    # stands -- no later command, no later worker, no restoration, no unlock.
    # An expected non-zero exit from the child is not a failure to reap.
    settle_or_abort() {
        # The inventory that DECLARES the identity is named by the caller; a
        # group recorded in another run's evidence is not declared by this
        # one's, and absence is never read as ownership.
        _sp=$1; _sg=$2; _sc=$3; _sw=$4; _sev=${5:-${evidence:-/nonexistent}}
        _swhy=""
        if [ -n "$_sp" ]; then
            stop_and_reap "$_sp" 60 \
                || _swhy="the owned child $_sp could not be reaped (${reaped_status:-unknown})"
        fi
        if [ -n "$_sg" ]; then
            typed_dispatch "$(route_identity "$_sev" "$_sg")" \
                "$_sg" "$_sc" "$_sw" harness_reap "$_sev" \
                || _swhy="${_swhy:+$_swhy; }the owned group $_sg did not resolve to owned authority"
        fi
        [ -z "$_swhy" ] && return 0
        abort_harness "$_sc" "$_sw" "$_swhy"
    }
    abort_harness() {   # abort_harness <control> <what> <why>
        fail "$1" "the independent coordinator could not settle $2: $3; refusing to restore, unlock or start another fixture"
        printf 'HARNESS ABORTED: cleanup of %s is unproved\n' "$2"
        printf '\nharness controls: %d failure(s)\n' "$fails"
        printf 'evidence: %s\n' "$evidence"
        exit 1
    }
    cleanup_or_abort() {   # cleanup_or_abort <group> <control> <what> [evidence]
        settle_or_abort "" "$1" "$2" "$3" "${4:-${evidence:-/nonexistent}}"
    }
    # settle_recorded_groups <evidence-dir> <control> <what>
    #
    # Read only AFTER the owned child has been reaped, so the inventory cannot
    # grow while it is being read, and settle EVERY session that child recorded
    # -- a successful wait on the child says nothing about what the child
    # started. The child's own pid is deliberately never signalled again: it has
    # been waited on, and that number no longer carries the ownership it did.
    # inventory_read <file> <out>
    #
    # Acquires the COMPLETE inventory, or fails. A reader that errors, and a
    # reader that manages a prefix before erroring, both fail: a partial read is
    # not an inventory. An inventory with no sessions is a complete inventory --
    # that is the legitimate no-work case, and it is distinguished from a file
    # that could not be read at all.
    inventory_read() {
        _ir_cmd=cat
        [ "$harness" -ne 1 ] || _ir_cmd=${MOQ5_PUBBIND_READER:-cat}
        : > "$2" || return 1
        [ -e "$1" ] || return 1
        "$_ir_cmd" "$1" > "$2" 2>/dev/null || return 1
        [ -s "$2" ] || return 0
        # A complete record set ends at a record boundary; a truncated tail does
        # not, so a reader that stopped mid-line is refused too.
        case "$(tail -c 1 "$2" | od -An -tx1 | tr -d ' \n')" in
            0a) return 0 ;;
            *)  return 1 ;;
        esac
    }
    # The identities this driver is allowed to hand to a cleanup dispatch. The
    # WHOLE token must be a plain decimal process-group id: a numeric prefix of
    # a malformed token is an invented identity, and 0 and 1 are not owned
    # groups at all -- 0 selects the caller's own group and 1 is init. Nothing
    # is converted arithmetically; the token is validated as text.
    valid_owned_group() {
        case "$1" in
            ''|*[!0-9]*) return 1 ;;
            0*) return 1 ;;
            1) return 1 ;;
        esac
        return 0
    }
    # A whole non-negative decimal, with no leading zeros beyond zero itself.
    is_uint() {
        case "$1" in
            ''|*[!0-9]*) return 1 ;;
            0|[1-9]*) return 0 ;;
            *) return 1 ;;
        esac
    }
    # The identities this driver may hand to a signal aimed at ONE process: the
    # same whole-decimal, non-special domain the group cleanup enforces, so a
    # malformed token and the selectors 0 and 1 are refused at both boundaries.
    valid_owned_pid() { valid_owned_group "$1"; }
    # The one place a validated identity is handed on. A control may replace the
    # dispatch with a recorder, so a malformed or special selector can be proved
    # never to reach the signalling layer WITHOUT any signal being sent.
    dispatch_group_cleanup() {
        if [ "$harness" -eq 1 ] && [ -n "${MOQ5_PUBBIND_CLEANUP_RECORDER-}" ]; then
            printf '%s\n' "$1" >> "${MOQ5_PUBBIND_CLEANUP_RECORDER}"
            return 0
        fi
        cleanup_or_abort "$1" "$2" "$3" "${4:-}"
    }
    # The complete set of non-session records this driver writes into an
    # ownership inventory. Anything outside it is not a record these parsers
    # understand, and is refused rather than skipped.
    declared_inventory_metadata() {
        case "$1" in
            managed=*) valid_managed_record "$1" ;;
            owned-by=*|java_home=*|retained=*|"replaced a lock whose recorded owner pid "*|\
            "build-jvm check skipped: the publication is stubbed"|'') return 0 ;;
            *) return 1 ;;
        esac
    }
    # The frozen carrier grammar, whole-record and in order:
    #   managed=<generation> scope=<scope> ref=<opaque> child=<pid>
    # Bounds: generation 32, scope 64, ref 32, child 10; whole record 166 bytes
    # INCLUDING its newline, so 165 without. A longer record is REFUSED, never
    # truncated, and the cap is checked before any field is trusted.
    valid_managed_record() {
        [ "${#1}" -le 165 ] || return 1
        _mr=$1
        case "$_mr" in managed=*) ;; *) return 1 ;; esac
        _mg=${_mr#managed=}; _mg=${_mg%% *}
        _rest=${_mr#managed=* }
        case "$_rest" in scope=*) ;; *) return 1 ;; esac
        _ms=${_rest#scope=}; _ms=${_ms%% *}
        _rest=${_rest#scope=* }
        case "$_rest" in ref=*) ;; *) return 1 ;; esac
        _mf=${_rest#ref=}; _mf=${_mf%% *}
        _rest=${_rest#ref=* }
        case "$_rest" in child=*) ;; *) return 1 ;; esac
        _mc=${_rest#child=}
        case "$_mc" in *" "*) return 1 ;; esac
        [ -n "$_mg" ] && [ "${#_mg}" -le 32 ] || return 1
        [ -n "$_ms" ] && [ "${#_ms}" -le 64 ] || return 1
        [ -n "$_mf" ] && [ "${#_mf}" -le 32 ] || return 1
        [ -n "$_mc" ] && [ "${#_mc}" -le 10 ] || return 1
        case "$_mg$_mf" in *[!A-Za-z0-9_-]*) return 1 ;; esac
        case "$_ms" in *[!A-Za-z0-9_-]*) return 1 ;; esac
        valid_owned_pid "$_mc" || return 1
        return 0
    }
    # The ledger form: an ACK ledger carries the managed record for its child.
    route_ledger_identity() {
        _rl_led=$1; _rl_tok=$2
        [ -f "$_rl_led" ] || { printf 'unknown no-ledger'; return 0; }
        _rl_first=$(head -n 1 "$_rl_led" 2>/dev/null)
        case "$_rl_first" in
            ledger=moq5-pubbind-ack|ledger=moq5-pubbind-ack-probe) ;;
            *) printf 'unknown unattested-ledger'; return 0 ;;
        esac
        _rl_hit=""; _rl_n=0
        while IFS= read -r _rl_l; do
            case "$_rl_l" in managed=*) ;; *) continue ;; esac
            valid_managed_record "$_rl_l" \
                || { printf 'unknown malformed-managed-record'; return 0; }
            _rl_c=${_rl_l##*child=}
            [ "$_rl_c" = "$_rl_tok" ] || continue
            _rl_n=$((_rl_n + 1)); _rl_hit=$_rl_l
        done < "$_rl_led"
        [ "$_rl_n" -le 1 ] \
            || { printf 'unknown conflicting-managed-records'; return 0; }
        if [ "$_rl_n" -eq 1 ]; then
            _rl_g=${_rl_hit#managed=}; _rl_g=${_rl_g%% *}
            _rl_s=${_rl_hit#*scope=}; _rl_s=${_rl_s%% *}
            _rl_r=${_rl_hit##*ref=}; _rl_r=${_rl_r%% *}
            printf 'managed %s %s %s' "$_rl_g" "$_rl_s" "$_rl_r"; return 0
        fi
        # No managed identity. The same attested ledger is what declares a
        # legacy child, so an identity IT records keeps its existing path; an
        # identity nothing declared is unknown, not legacy by default.
        if grep -q "^child=$_rl_tok\$" "$_rl_led" 2>/dev/null; then
            printf 'unrelated'; return 0
        fi
        printf 'unknown not-declared'
    }
    # The canonical WRITER: emit exactly one valid managed record for a child
    # the retained owner created, from the values its create reply returned.
    # Nothing here invents a ref: `ref` is the owner-minted opaque token, and
    # the record is refused rather than written if it does not validate.
    #   write_managed_record <ledger> <generation> <scope> <ref> <child>
    write_managed_record() {
        _wm_led=$1; _wm_g=$2; _wm_s=$3; _wm_r=$4; _wm_c=$5
        _wm_line=$(printf 'managed=%s scope=%s ref=%s child=%s' \
                          "$_wm_g" "$_wm_s" "$_wm_r" "$_wm_c")
        valid_managed_record "$_wm_line" || return 1
        [ -f "$_wm_led" ] || return 1
        _wm_first=$(head -n 1 "$_wm_led" 2>/dev/null)
        case "$_wm_first" in
            ledger=moq5-pubbind-ack|ledger=moq5-pubbind-ack-probe) ;;
            *) return 1 ;;
        esac
        # Exactly one record per child: a second write for the same child is
        # refused rather than appended, so a conflict cannot be created here.
        if grep -q " child=$_wm_c\$" "$_wm_led" 2>/dev/null; then
            return 1
        fi
        printf '%s\n' "$_wm_line" >> "$_wm_led"
    }
    # The managed creation path: the fixture's child is created BY the retained
    # owner, and the record this driver writes is built from that create reply.
    # No identity here is invented locally, and a failure to record the handoff
    # is an ownership exit rather than a silently unmanaged child.
    #   owner_create_managed <ledger> <scope> <key> <control> <what>
    owner_create_managed() {
        _oc_led=$1; _oc_scope=$2; _oc_key=$3; _oc_ctl=$4; _oc_what=$5
        owner_connected || {
            abort_harness "$_oc_ctl" "$_oc_what" \
                "no retained owner connection: a managed child cannot be created"
            return 1
        }
        _oc_reply=$(owner_call create "$_oc_scope" "$_oc_key"); _oc_rc=$?
        case "$_oc_reply" in
            'refused: '*)
                abort_harness "$_oc_ctl" "$_oc_what" \
                    "the owner refused the create: ${_oc_reply#refused: }"
                return 1 ;;
        esac
        [ "$_oc_rc" -eq 0 ] || {
            abort_harness "$_oc_ctl" "$_oc_what" \
                "the owner create ended with status $_oc_rc"
            return 1
        }
        # generation scope ref child, exactly as the owner reported them
        _oc_g=$(printf '%s' "$_oc_reply" | cut -d" " -f1)
        _oc_s=$(printf '%s' "$_oc_reply" | cut -d" " -f2)
        _oc_r=$(printf '%s' "$_oc_reply" | cut -d" " -f3)
        _oc_c=$(printf '%s' "$_oc_reply" | cut -d" " -f4)
        write_managed_record "$_oc_led" "$_oc_g" "$_oc_s" "$_oc_r" "$_oc_c" || {
            abort_harness "$_oc_ctl" "$_oc_what" \
                "the create reply for $_oc_r could not be recorded in $_oc_led"
            return 1
        }
        printf '%s %s %s %s' "$_oc_g" "$_oc_s" "$_oc_r" "$_oc_c"
    }
    # The other half of the same handoff: a fixture that created its child
    # through the owner hands back the reply whole, and the canonical writer --
    # not the fixture -- is what records it. A reply that does not carry the
    # complete identity is an ownership exit, never an unmanaged child.
    #   record_owner_reply <ledger> <reply-file> <control> <what>
    record_owner_reply() {
        _rr_led=$1; _rr_file=$2; _rr_ctl=$3; _rr_what=$4
        [ -s "$_rr_file" ] || {
            abort_harness "$_rr_ctl" "$_rr_what" \
                "the fixture recorded no create reply in $_rr_file"
            return 1
        }
        _rr_reply=$(head -n 1 "$_rr_file")
        _rr_g=$(printf '%s' "$_rr_reply" | cut -d" " -f1)
        _rr_s=$(printf '%s' "$_rr_reply" | cut -d" " -f2)
        _rr_r=$(printf '%s' "$_rr_reply" | cut -d" " -f3)
        _rr_c=$(printf '%s' "$_rr_reply" | cut -d" " -f4)
        if [ -z "$_rr_g" ] || [ -z "$_rr_s" ] || [ -z "$_rr_r" ] || [ -z "$_rr_c" ]; then
            abort_harness "$_rr_ctl" "$_rr_what" \
                "the create reply is incomplete: $_rr_reply"
            return 1
        fi
        write_managed_record "$_rr_led" "$_rr_g" "$_rr_s" "$_rr_r" "$_rr_c" || {
            abort_harness "$_rr_ctl" "$_rr_what" \
                "the create reply for $_rr_r could not be recorded in $_rr_led"
            return 1
        }
        printf '%s %s %s %s' "$_rr_g" "$_rr_s" "$_rr_r" "$_rr_c"
    }
    # THE one dispatch path. Every caller category routes here first, and each
    # outcome has exactly one action. A managed reference is settled through the
    # owner by its ref; a positively declared legacy target keeps its existing
    # path; retired dispatches nothing; unknown refuses by name.
    #   typed_dispatch <outcome-text> <token> <ctx> <why> <legacy-cmd> [evidence]
    #
    # The optional evidence directory is the inventory that DECLARED this
    # identity. It is handed to the legacy command so a caller that routes
    # again cannot look for the declaration in some other run's inventory.
    typed_dispatch() {
        _td_out=$1; _td_tok=$2; _td_ctx=$3; _td_why=$4; _td_legacy=$5
        _td_ev=${6:-}
        case "$_td_out" in
            managed\ *)
                _td_g=$(printf '%s' "$_td_out" | cut -d" " -f2)
                _td_s=$(printf '%s' "$_td_out" | cut -d" " -f3)
                _td_r=$(printf '%s' "$_td_out" | cut -d" " -f4)
                # The whole identity must be present. A missing field means the
                # record did not carry an owner identity, which is unknown.
                if [ -z "$_td_g" ] || [ -z "$_td_s" ] || [ -z "$_td_r" ]; then
                    abort_harness "$_td_ctx" "$_td_why" \
                        "the managed routing value is incomplete: $_td_out"
                    return 1
                fi
                # The owner itself decides whether this generation is its own:
                # an environment string is not an owner identity, and a value
                # compared against itself proves nothing.
                owner_settle_ref "$_td_g" "$_td_s" "$_td_r" "$_td_ctx" "$_td_why"
                return $? ;;
            unrelated)
                $_td_legacy "$_td_tok" "$_td_ctx" "$_td_why" "$_td_ev"
                return $? ;;
            retired)
                return 0 ;;                       # addressable, dispatches nothing
            unknown\ *|unknown)
                abort_harness "$_td_ctx" "$_td_why" \
                    "the identity $_td_tok did not resolve to owned authority: ${_td_out#unknown }"
                return 1 ;;
        esac
        abort_harness "$_td_ctx" "$_td_why" "an unroutable outcome: $_td_out"
        return 1
    }
    # The retained owner connection. `owner_connect` is the only place the
    # driver learns where its owner is; every managed boundary goes through it.
    # A missing channel is a REFUSAL, never a settled child.
    owner_support() {
        printf '%s' "${MOQ5_PUBBIND_OWNER_SUPPORT:-$(dirname "$0")/publication_owner}"
    }
    # The owner channel is INHERITED, never discovered. The launcher creates the
    # socketpair, retains its end for its own lifetime, and passes this driver
    # one explicit alias at a fixed descriptor number. There is no pathname to
    # reconnect to and no way to find another owner: if the alias is absent, the
    # driver has no owner, and that is a refusal rather than a settled child.
    owner_connected() {
        [ -n "${MOQ5_PUBBIND_OWNER_FD:-}" ] || return 1
        case "$MOQ5_PUBBIND_OWNER_FD" in ''|*[!0-9]*) return 1 ;; esac
        return 0
    }
    owner_call() {
        owner_connected || {
            printf 'refused: no-owner-channel: no owner channel was inherited'
            return 3
        }
        "${MOQ5_PUBBIND_PYTHON:-python3}" -B "$(owner_support)/owner_call.py" "$@"
    }
    # Settlement through the retained owner, by the FULL identity -- never by
    # number, and never satisfied by a log write. Only the owner's own
    # authoritative outcome retires a child.
    #   owner_settle_ref <generation> <scope> <ref> <control> <what>
    owner_settle_ref() {
        _os_g=$1; _os_s=$2; _os_r=$3; _os_c=$4; _os_w=$5
        _os_out=$(owner_call settle "$_os_g" "$_os_s" "$_os_r"); _os_rc=$?
        case "$_os_out" in
            'retired'|'retired already')
                [ "$_os_rc" -eq 0 ] || {
                    abort_harness "$_os_c" "$_os_w" \
                        "the owner reported '$_os_out' with status $_os_rc"
                    return 1
                }
                return 0 ;;
        esac
        abort_harness "$_os_c" "$_os_w" \
            "the retained owner did not settle $_os_r: ${_os_out:-no reply} (status $_os_rc)"
        return 1
    }
    # The four SIGNED routing outcomes. Exactly one is printed, and a managed
    # answer carries the generation and the opaque ref as SEPARATE fields.
    #   managed <gen> <ref> | unrelated | retired | unknown <why>
    route_identity() {
        _ri_ev=$1; _ri_tok=$2
        [ -n "$_ri_tok" ] || { printf 'unknown no-identity'; return 0; }
        [ -f "$_ri_ev/ownership.txt" ] || { printf 'unknown no-inventory'; return 0; }
        _ri_first=$(head -n 1 "$_ri_ev/ownership.txt" 2>/dev/null)
        [ "$_ri_first" = "owned-by=moq5-pubbind-driver" ] \
            || { printf 'unknown unattested-inventory'; return 0; }
        _ri_hit=""; _ri_n=0; _ri_bad=0
        while IFS= read -r _ri_l; do
            case "$_ri_l" in managed=*) ;; *) continue ;; esac
            valid_managed_record "$_ri_l" || { _ri_bad=1; break; }
            _ri_c=${_ri_l##*child=}
            [ "$_ri_c" = "$_ri_tok" ] || continue
            _ri_n=$((_ri_n + 1))
            _ri_hit=$_ri_l
        done < "$_ri_ev/ownership.txt"
        [ "$_ri_bad" -eq 0 ] || { printf 'unknown malformed-managed-record'; return 0; }
        if [ "$_ri_n" -gt 1 ]; then
            printf 'unknown conflicting-managed-records'; return 0
        fi
        if [ "$_ri_n" -eq 1 ]; then
            _ri_g=${_ri_hit#managed=}; _ri_g=${_ri_g%% *}
            _ri_s=${_ri_hit#*scope=}; _ri_s=${_ri_s%% *}
            _ri_r=${_ri_hit##*ref=}; _ri_r=${_ri_r%% *}
            # Retirement is the OWNER's state, never a file claiming it. A
            # record cannot skip an owed settle; the owner answers
            # `retired already` for a ref it has already retired.
            printf 'managed %s %s %s' "$_ri_g" "$_ri_s" "$_ri_r"; return 0
        fi
        # No managed record names it. An attested session= record is a
        # positively declared LEGACY target; absence alone is never authority.
        if grep -qxF "session=$_ri_tok observed=live" "$_ri_ev/ownership.txt" \
                2>/dev/null \
           || grep -qxF "session=$_ri_tok observed=direct-child" \
                "$_ri_ev/ownership.txt" 2>/dev/null \
           || grep -q "^session=$_ri_tok " "$_ri_ev/ownership.txt" 2>/dev/null; then
            printf 'unrelated'; return 0
        fi
        # This driver's own quiescence record is the other positive
        # declaration of a legacy group in the same evidence directory.
        if grep -q "^group=$_ri_tok final=" "$_ri_ev/quiescence.txt" 2>/dev/null; then
            printf 'unrelated'; return 0
        fi
        printf 'unknown not-declared'
    }
    settle_recorded_groups() {
        _srg_inv="$evidence/.inventory.$$"
        if ! inventory_read "$1/ownership.txt" "$_srg_inv"; then
            abort_harness "$2" "$3" \
                "its recorded ownership inventory at $1/ownership.txt could not be read completely, so the set of groups to settle is unknown"
        fi
        while IFS= read -r _srg_line; do
            case "$_srg_line" in
                session=*) ;;
                # The metadata this driver itself writes, and nothing else: an
                # unrecognised record means the inventory is not the one this
                # parser understands.
                *) declared_inventory_metadata "$_srg_line" && continue
                   abort_harness "$2" "$3" \
                       "an unrecognised record in the ownership inventory: $_srg_line" ;;
            esac
            # The whole token, not a numeric prefix of it.
            _srg=${_srg_line#session=}
            _srg=${_srg%% *}
            if ! valid_owned_group "$_srg"; then
                abort_harness "$2" "$3" \
                    "a recorded session token is not a usable owned process group: '$_srg' (from: $_srg_line)"
            fi
            typed_dispatch "$(route_identity "$1" "$_srg")" "$_srg" "$2" \
                "$3 (recorded group $_srg)" dispatch_group_cleanup "$1"
        done < "$_srg_inv"
        # The witness of this caller's OWN settlement, written at its
        # continuation boundary -- before any independent coordinator cleanup
        # can repair an omission and make the caller look complete.
        _srg_wit="$evidence/settled-$(basename "$1").txt"
        : > "$_srg_wit"
        while IFS= read -r _srg_line; do
            case "$_srg_line" in session=*) ;; *) continue ;; esac
            _srg=${_srg_line#session=}
            _srg=${_srg%% *}
            valid_owned_group "$_srg" || continue
            printf 'group=%s state=%s\n' "$_srg" "$(harness_group_live "$_srg")" >> "$_srg_wit"
        done < "$_srg_inv"
        rm -f "$_srg_inv"
    }
    # The recorded session a child driver owned, from its own evidence.
    recorded_group() {
        sed -n 's/^session=\([0-9]*\).*/\1/p' "$1/ownership.txt" 2>/dev/null | tail -1
    }

    # An INDEPENDENT reaper for the coordinator. It shares no code with the
    # supervision under test, so a control can neuter that supervision and this
    # still cleans up. Bounded at every step.
    harness_ps() {
        if [ -n "${MOQ5_PUBBIND_HARNESS_PS-}" ]; then
            printf '%s' "${MOQ5_PUBBIND_HARNESS_PS}"
        else
            printf '/bin/ps'
        fi
    }
    harness_proc_live() {
        _hp=$("$(harness_ps)" -Ao pid=,stat= 2>/dev/null) || { printf 'unknown'; return 0; }
        [ -n "$_hp" ] || { printf 'unknown'; return 0; }
        _hq=$(printf '%s\n' "$_hp" | awk -v p="$1" '
            { gsub(/^ +/, "", $1) }
            $1 == p && $2 !~ /^Z/ { found = 1 }
            END { print found ? "alive" : "gone" }') || { printf 'unknown'; return 0; }
        case "$_hq" in alive|gone) printf '%s' "$_hq" ;; *) printf 'unknown' ;; esac
    }
    harness_group_live() {
        _hs=$("$(harness_ps)" -Ao pgid=,stat= 2>/dev/null) || { printf 'unknown'; return 0; }
        [ -n "$_hs" ] || { printf 'unknown'; return 0; }
        _hr=$(printf '%s\n' "$_hs" | awk -v g="$1" '
            { gsub(/^ +/, "", $1) }
            $1 == g && $2 !~ /^Z/ { found = 1 }
            END { print found ? "live" : "empty" }') || { printf 'unknown'; return 0; }
        case "$_hr" in live|empty) printf '%s' "$_hr" ;; *) printf 'unknown' ;; esac
    }
    harness_reap() {
        [ -n "${1:-}" ] || return 0
        kill -TERM "-$1" 2>/dev/null || true
        _hd=$(( $(now) + 6 ))
        while [ "$(harness_group_live "$1")" != empty ]; do
            [ "$(now)" -lt "$_hd" ] || break
            sleep 1
        done
        if [ "$(harness_group_live "$1")" != empty ]; then
            "$(harness_ps)" -Ao pid=,pgid= 2>/dev/null \
                | awk -v g="$1" '{ gsub(/^ +/, "", $1) } $2 == g { print $1 }' \
                | while read -r _hk; do kill -KILL "$_hk" 2>/dev/null || true; done
            _hd=$(( $(now) + 6 ))
            while [ "$(harness_group_live "$1")" != empty ]; do
                [ "$(now)" -lt "$_hd" ] || break
                sleep 1
            done
        fi
        # An inspection that cannot be made is not proof of an empty group.
        [ "$(harness_group_live "$1")" = empty ]
    }

    # settle_named_process <pid> <control> <what>
    #   Settle one process this driver's own fixture created and recorded in its
    #   cleanup ledger. It is not a direct child, so it cannot be waited on: it
    #   is stopped by that exact identity and its absence is PROVED by the
    #   checked observer. An inspection that cannot be made is not proof of an
    #   absence, an empty identity is not the same fact as having no child, and
    #   a token outside the owned domain is never signalled.
    settle_named_process() {
        [ -n "${1:-}" ] \
            || abort_harness "$2" "$3" "a cleanup was asked for with no identity, which is not the same fact as having no child"
        valid_owned_pid "$1" \
            || abort_harness "$2" "$3" "'$1' is not an identity this driver may signal"
        case "$(harness_proc_live "$1")" in
            gone)  return 0 ;;
            alive) : ;;
            *) abort_harness "$2" "$3" "process $1 could not be observed" ;;
        esac
        # A control may replace the dispatch with a recorder, so what WOULD have
        # been signalled is observed without any signal being sent.
        if [ "$harness" -eq 1 ] && [ -n "${MOQ5_PUBBIND_SETTLE_RECORDER-}" ]; then
            printf '%s\n' "$1" >> "${MOQ5_PUBBIND_SETTLE_RECORDER}"
            return 0
        fi
        for _sn_sig in TERM KILL; do
            kill "-$_sn_sig" "$1" 2>/dev/null || true
            _sn_d=$(( $(now) + 6 ))
            while [ "$(harness_proc_live "$1")" = alive ]; do
                [ "$(now)" -lt "$_sn_d" ] || break
                sleep 1
            done
            [ "$(harness_proc_live "$1")" = alive ] || break
        done
        [ "$(harness_proc_live "$1")" = gone ] \
            || abort_harness "$2" "$3" "process $1 was not observed gone after being stopped"
    }

    # ack_receipts_parse <receipts>
    #   The receipt contract the acknowledgment helper emits, validated as a
    #   whole history rather than a bag of last-value-wins fields. Records are
    #   parsed complete and in their declared order, each lifecycle fact appears
    #   with its declared multiplicity, every child-bearing record is bound to
    #   the one created child, and an unresolved history cannot also claim a
    #   completed reap. Echoes nothing when the history is coherent and the named
    #   reason otherwise; on success the validated facts are left in
    #   _ar_child / _ar_ack / _ar_outcome / _ar_wait / _ar_status / _ar_helper.
    #
    #   The declared history:
    #     child=<pid>                                   at most one, first
    #     probe=<word>                                  at most one
    #     ack=ok group=<pid> | ack=failed child=<pid>   one, when a child exists
    #     signal=<TERM|KILL> <sent|not-sent-probe|target-already-gone>
    #                                                   TERM then KILL, once each
    #     wait=reaped child=<pid> status=<n>            at most one
    #     outcome=reaped child=<pid>
    #     outcome=unresolved reason=<word> [errno=<n>] child=<pid>
    #     helper=<status>                               exactly one, last
    ack_receipts_parse() {
        _ar_f="$sx/.ack-parse.$$"
        inventory_read "$1" "$_ar_f" \
            || { _ar_why=$(printf 'the helper receipts at %s could not be read completely' "$1"); return 0; }
        _ar_why=""
        _ar_child=""; _ar_ack=""; _ar_outcome=""; _ar_reason=""; _ar_wait=""
        _ar_status=""; _ar_helper=""
        _ar_stage=0; _ar_nchild=0; _ar_nprobe=0; _ar_nack=0; _ar_nwait=0
        _ar_nout=0; _ar_nhelper=0; _ar_sigs=" "
        while IFS= read -r _ar_l; do
            case "$_ar_l" in
                child=*)   _ar_st=1 ;;
                probe=*)   _ar_st=2 ;;
                ack=*)     _ar_st=3 ;;
                signal=*)  _ar_st=4 ;;
                wait=*)    _ar_st=5 ;;
                outcome=*) _ar_st=6 ;;
                helper=*)  _ar_st=7 ;;
                '')        continue ;;
                *) _ar_why=$(printf 'an unrecognised helper receipt: %s' "$_ar_l"); return 0 ;;
            esac
            [ "$_ar_st" -ge "$_ar_stage" ] \
                || { _ar_why=$(printf 'a helper receipt out of its declared order: %s' "$_ar_l"); return 0; }
            _ar_stage=$_ar_st
            case "$_ar_l" in
                child=*)
                    _ar_nchild=$((_ar_nchild + 1))
                    [ "$_ar_nchild" -eq 1 ] \
                        || { _ar_why=$(printf 'the receipts name more than one created child'); return 0; }
                    _ar_child=${_ar_l#child=}
                    valid_owned_pid "$_ar_child" \
                        || { _ar_why=$(printf 'the created child identity is not a usable process id: %s' "$_ar_l"); return 0; } ;;
                probe=*)
                    _ar_nprobe=$((_ar_nprobe + 1))
                    [ "$_ar_nprobe" -eq 1 ] \
                        || { _ar_why=$(printf 'a repeated probe receipt: %s' "$_ar_l"); return 0; }
                    case "${_ar_l#probe=}" in
                        ''|*' '*) _ar_why=$(printf 'a malformed probe receipt: %s' "$_ar_l"); return 0 ;;
                    esac ;;
                ack=*)
                    _ar_nack=$((_ar_nack + 1))
                    [ "$_ar_nack" -eq 1 ] \
                        || { _ar_why=$(printf 'a repeated acknowledgment receipt: %s' "$_ar_l"); return 0; }
                    case "$_ar_l" in
                        'ack=ok group='*)     _ar_ack=ok;     _ar_id=${_ar_l#ack=ok group=} ;;
                        'ack=failed child='*) _ar_ack=failed; _ar_id=${_ar_l#ack=failed child=} ;;
                        *) _ar_why=$(printf 'a malformed acknowledgment receipt: %s' "$_ar_l"); return 0 ;;
                    esac
                    [ "$_ar_id" = "$_ar_child" ] \
                        || { _ar_why=$(printf 'the acknowledgment receipt names %s, not the created child %s' "$_ar_id" "${_ar_child:-nothing}"); return 0; } ;;
                signal=*)
                    _ar_r=${_ar_l#signal=}
                    _ar_nm=${_ar_r%% *}
                    case "$_ar_r" in "$_ar_nm "?*) ;; *) _ar_why=$(printf 'a malformed signal receipt: %s' "$_ar_l"); return 0 ;; esac
                    _ar_dt=${_ar_r#"$_ar_nm "}
                    case "$_ar_nm" in TERM|KILL) ;; *) _ar_why=$(printf 'an unrepresentable signal name: %s' "$_ar_l"); return 0 ;; esac
                    case "$_ar_dt" in
                        sent|not-sent-probe|target-already-gone) ;;
                        *) _ar_why=$(printf 'an unrepresentable signal outcome: %s' "$_ar_l"); return 0 ;;
                    esac
                    case "$_ar_sigs" in
                        *" $_ar_nm "*) _ar_why=$(printf 'a repeated signal receipt: %s' "$_ar_l"); return 0 ;;
                    esac
                    if [ "$_ar_nm" = TERM ]; then
                        case "$_ar_sigs" in
                            *" KILL "*) _ar_why=$(printf 'a TERM receipt after KILL: %s' "$_ar_l"); return 0 ;;
                        esac
                    fi
                    _ar_sigs="$_ar_sigs$_ar_nm " ;;
                wait=*)
                    _ar_nwait=$((_ar_nwait + 1))
                    [ "$_ar_nwait" -eq 1 ] \
                        || { _ar_why=$(printf 'a repeated wait receipt: %s' "$_ar_l"); return 0; }
                    case "$_ar_l" in 'wait=reaped child='*) ;; *) _ar_why=$(printf 'a malformed wait receipt: %s' "$_ar_l"); return 0 ;; esac
                    _ar_r=${_ar_l#wait=reaped child=}
                    _ar_wait=${_ar_r%% *}
                    case "$_ar_r" in "$_ar_wait status="?*) ;; *) _ar_why=$(printf 'a wait receipt with no status: %s' "$_ar_l"); return 0 ;; esac
                    _ar_status=${_ar_r#"$_ar_wait status="}
                    is_uint "$_ar_status" \
                        || { _ar_why=$(printf 'a wait receipt with a malformed status: %s' "$_ar_l"); return 0; }
                    valid_owned_pid "$_ar_wait" \
                        || { _ar_why=$(printf 'the wait receipt names an identity that is not a usable process id: %s' "$_ar_l"); return 0; }
                    [ "$_ar_wait" = "$_ar_child" ] \
                        || { _ar_why=$(printf 'the wait receipt names %s, not the created child %s' "$_ar_wait" "${_ar_child:-nothing}"); return 0; } ;;
                outcome=*)
                    _ar_nout=$((_ar_nout + 1))
                    [ "$_ar_nout" -eq 1 ] \
                        || { _ar_why=$(printf 'a repeated outcome receipt: %s' "$_ar_l"); return 0; }
                    case "$_ar_l" in
                        'outcome=reaped child='*)
                            _ar_outcome=reaped; _ar_id=${_ar_l#outcome=reaped child=} ;;
                        'outcome=unresolved reason='*)
                            _ar_outcome=unresolved
                            _ar_r=${_ar_l#outcome=unresolved reason=}
                            _ar_reason=${_ar_r%% *}
                            _ar_t=${_ar_r#"$_ar_reason"}
                            case "$_ar_t" in
                                " child="*) _ar_id=${_ar_t#" child="} ;;
                                " errno="*)
                                    _ar_e=${_ar_t#" errno="}
                                    _ar_en=${_ar_e%% *}
                                    case "$_ar_e" in "$_ar_en child="?*) ;; *) _ar_why=$(printf 'a malformed unresolved outcome: %s' "$_ar_l"); return 0 ;; esac
                                    is_uint "$_ar_en" \
                                        || { _ar_why=$(printf 'an unresolved outcome with a malformed errno: %s' "$_ar_l"); return 0; }
                                    _ar_id=${_ar_e#"$_ar_en child="} ;;
                                *) _ar_why=$(printf 'a malformed unresolved outcome: %s' "$_ar_l"); return 0 ;;
                            esac
                            case "$_ar_reason" in
                                ''|*' '*) _ar_why=$(printf 'a malformed unresolved reason: %s' "$_ar_l"); return 0 ;;
                            esac ;;
                        *) _ar_why=$(printf 'a malformed outcome receipt: %s' "$_ar_l"); return 0 ;;
                    esac
                    [ "$_ar_id" = "$_ar_child" ] \
                        || { _ar_why=$(printf 'the outcome receipt names %s, not the created child %s' "$_ar_id" "${_ar_child:-nothing}"); return 0; } ;;
                helper=*)
                    _ar_nhelper=$((_ar_nhelper + 1))
                    [ "$_ar_nhelper" -eq 1 ] \
                        || { _ar_why=$(printf 'a repeated helper status: %s' "$_ar_l"); return 0; }
                    _ar_helper=${_ar_l#helper=}
                    is_uint "$_ar_helper" \
                        || { _ar_why=$(printf 'a malformed helper status: %s' "$_ar_l"); return 0; } ;;
            esac
        done < "$_ar_f"
        rm -f "$_ar_f"
        [ "$_ar_nhelper" -eq 1 ] \
            || { _ar_why=$(printf 'the receipts record no helper status'); return 0; }
        if [ "$_ar_nchild" -eq 0 ]; then
            # A run that recorded no child may not describe one either.
            [ "$_ar_nack" -eq 0 ] && [ "$_ar_nwait" -eq 0 ] && [ "$_ar_nout" -eq 0 ] \
                && [ "$_ar_sigs" = " " ] \
                || { _ar_why=$(printf 'the receipts describe a child they never recorded'); return 0; }
            return 0
        fi
        [ "$_ar_nack" -eq 1 ] \
            || { _ar_why=$(printf 'the receipts record no acknowledgment outcome'); return 0; }
        if [ "$_ar_ack" = ok ]; then
            [ "$_ar_nwait" -eq 0 ] && [ "$_ar_nout" -eq 0 ] && [ "$_ar_sigs" = " " ] \
                || { _ar_why=$(printf 'a successful acknowledgment cannot also report cleanup'); return 0; }
            [ "$_ar_helper" = 0 ] \
                || { _ar_why=$(printf 'a successful acknowledgment whose helper status is %s, not 0' "$_ar_helper"); return 0; }
            return 0
        fi
        [ "$_ar_nout" -eq 1 ] \
            || { _ar_why=$(printf 'a failed acknowledgment with no outcome receipt'); return 0; }
        case "$_ar_outcome" in
            reaped)
                [ "$_ar_nwait" -eq 1 ] \
                    || { _ar_why=$(printf 'a completed reap with no wait receipt'); return 0; }
                [ "$_ar_helper" = 3 ] \
                    || { _ar_why=$(printf 'a completed reap whose helper status is %s, not 3' "$_ar_helper"); return 0; } ;;
            unresolved)
                [ "$_ar_nwait" -eq 0 ] \
                    || { _ar_why=$(printf 'an unresolved outcome that also claims a completed wait'); return 0; }
                [ "$_ar_helper" = 4 ] \
                    || { _ar_why=$(printf 'an unresolved outcome whose helper status is %s, not 4' "$_ar_helper"); return 0; } ;;
        esac
        return 0
    }

    # ack_reap_verdict <receipts> <caller-status> <armed-path> <recorded-inventory>
    #   Echoes nothing when the validated history says this exact child was
    #   created, its acknowledgment failed, it was reaped, and the checked
    #   observer agrees it is gone and leads no group -- and the caller refused
    #   to arm or record anything. Otherwise it echoes the named reason. This
    #   function never signals: the receipts are evidence, not authority.
    ack_reap_verdict() {
        ack_receipts_parse "$1"; _av_s=$?
        [ "$_av_s" -eq 0 ] \
            || { printf 'the receipt parser ended with status %s' "$_av_s"; return 0; }
        [ -z "$_ar_why" ] || { printf '%s' "$_ar_why"; return 0; }
        [ -n "$_ar_child" ] || { printf 'the helper recorded no created child'; return 0; }
        [ "$_ar_ack" = failed ] \
            || { printf 'the failed-acknowledgment path was not reached (ack=%s)' "${_ar_ack:-none}"; return 0; }
        [ "$_ar_outcome" = reaped ] \
            || { printf 'the helper reported no completed reap (outcome=%s)' "${_ar_outcome:-none}"; return 0; }
        case "$(harness_proc_live "$_ar_child")" in
            gone)  ;;
            alive) printf 'the created child %s is still alive' "$_ar_child"; return 0 ;;
            *)     printf 'the created child %s could not be observed' "$_ar_child"; return 0 ;;
        esac
        case "$(harness_group_live "$_ar_child")" in
            empty) ;;
            live)  printf 'the group %s the created child would lead is not empty' "$_ar_child"; return 0 ;;
            *)     printf 'the group %s could not be observed' "$_ar_child"; return 0 ;;
        esac
        [ "$2" -ne 0 ] \
            || { printf 'the caller accepted a failed acknowledgment (exit %s)' "$2"; return 0; }
        [ ! -f "$3" ] || { printf 'the fixture armed after a failed acknowledgment'; return 0; }
        [ ! -s "$4" ] \
            || { printf 'a session was recorded after a failed acknowledgment: %s' "$(tr '\n' ' ' < "$4")"; return 0; }
        printf ''
    }

    # ack_ledger_read <ledger>
    #   The CLEANUP authority, kept separate from the receipts on purpose: it is
    #   written at fork time, before any of the lifecycle logic the receipts
    #   describe, and it is never rewritten, so a fixture that misreports its own
    #   cleanup cannot also hide the worker it created. Prints `none` when the
    #   ledger is complete and records no child, the created pid when it records
    #   one, and `refused: <reason>` when the authority cannot be established --
    #   which is NOT the same fact as knowing there was no child.
    #
    #   Two headers share this grammar. `ledger=moq5-pubbind-ack` is written by a
    #   fixture run that really forked, and is the only header an outside
    #   coordinator may treat as ownership; `ledger=moq5-pubbind-ack-probe` marks
    #   a control's synthetic input, so a valid-looking identity in test data can
    #   never be mistaken for a worker some run created.
    ack_ledger_read() {
        _al_f="$sx/.ack-ledger.$$"
        inventory_read "$1" "$_al_f" \
            || { printf 'refused: the cleanup ledger at %s could not be read completely\n' "$1"; return 0; }
        _al_head=0; _al_child=""; _al_n=0; _al_st=0
        _al_m=0; _al_mline=""; _al_mchild=""
        while IFS= read -r _al_l; do
            case "$_al_l" in
                ledger=moq5-pubbind-ack|ledger=moq5-pubbind-ack-probe)
                    _al_head=$((_al_head + 1)) ;;
                managed=*)
                    # The carrier this driver's own writer emits. It is
                    # validated by the same grammar the writer used, and it
                    # carries the managed identity the child is settled by.
                    valid_managed_record "$_al_l" \
                        || { printf 'refused: a malformed managed record: %s\n' "$_al_l"; return 0; }
                    _al_m=$((_al_m + 1))
                    _al_mline=$_al_l
                    _al_mchild=${_al_l##*child=} ;;
                child=*)
                    _al_n=$((_al_n + 1))
                    _al_child=${_al_l#child=}
                    valid_owned_pid "$_al_child" \
                        || { printf 'refused: the ledger records an identity that is not a usable process id: %s\n' "$_al_l"; return 0; } ;;
                helper=*)
                    _al_st=$((_al_st + 1))
                    is_uint "${_al_l#helper=}" \
                        || { printf 'refused: a malformed ledger status: %s\n' "$_al_l"; return 0; } ;;
                '') continue ;;
                *) printf 'refused: an unrecognised cleanup-ledger record: %s\n' "$_al_l"; return 0 ;;
            esac
        done < "$_al_f"
        rm -f "$_al_f"
        [ "$_al_head" -eq 1 ] \
            || { printf 'refused: the cleanup ledger carries no ownership header\n'; return 0; }
        [ "$_al_n" -le 1 ] \
            || { printf 'refused: the cleanup ledger records more than one created child\n'; return 0; }
        [ "$_al_m" -le 1 ] \
            || { printf 'refused: the cleanup ledger records more than one managed identity\n'; return 0; }
        if [ "$_al_m" -eq 1 ] && [ -n "$_al_child" ] \
                && [ "$_al_mchild" != "$_al_child" ]; then
            printf 'refused: the managed record names child %s, not the created %s\n' \
                "$_al_mchild" "$_al_child"; return 0
        fi
        [ "$_al_st" -eq 1 ] \
            || { printf 'refused: the cleanup ledger records no helper status\n'; return 0; }
        if [ "$_al_m" -eq 1 ]; then
            printf 'managed %s\n' "$_al_mline"
            return 0
        fi
        printf '%s\n' "${_al_child:-none}"
    }

    # ack_cleanup_of <ledger> <control> <what>
    #   The single cleanup handoff for one fixture run, made once, at the moment
    #   the fixture returns. The identity comes from the ledger and the decision
    #   from the checked observer: a child observed gone here releases the
    #   authority for good, a child observed alive is settled by that exact
    #   identity, and an authority that cannot be established stops the harness
    #   rather than continuing as though there were no child.
    ack_cleanup_of() {
        _ac_led=$1
        _ac_v=$(ack_ledger_read "$_ac_led"); _ac_s=$?
        [ "$_ac_s" -eq 0 ] \
            || abort_harness "$2" "$3" "the cleanup ledger reader ended with status $_ac_s"
        case "$_ac_v" in
            'refused: '*) abort_harness "$2" "$3" "${_ac_v#refused: }" ;;
            none) return 0 ;;
            'managed '*)
                # The reader already resolved the managed identity from THIS
                # ledger; route it without consulting any other path.
                _ac_line=${_ac_v#managed }
                _ac_g=${_ac_line#managed=}; _ac_g=${_ac_g%% *}
                _ac_sc=${_ac_line#*scope=}; _ac_sc=${_ac_sc%% *}
                _ac_r=${_ac_line##*ref=}; _ac_r=${_ac_r%% *}
                _ac_c=${_ac_line##*child=}
                typed_dispatch "managed $_ac_g $_ac_sc $_ac_r" \
                    "$_ac_c" "$2" "$3" settle_named_process
                return $? ;;
        esac
        # The same ledger argument is preserved through cleanup: an
        # environment-selected path is not the file this reader validated.
        typed_dispatch "$(route_ledger_identity "$_ac_led" "$_ac_v")" \
            "$_ac_v" "$2" "$3" settle_named_process
    }
    ack_verdict_of() {   # ack_verdict_of <receipts> <caller-status> <armed> <inventory>
        _avo=$(ack_reap_verdict "$1" "$2" "$3" "$4"); _avs=$?
        if [ "$_avs" -ne 0 ]; then
            printf 'the reap oracle ended with status %s' "$_avs"
        else
            printf '%s' "$_avo"
        fi
    }

    artifact_section() {
    # ---- F3: the artifact oracle ------------------------------------------
    if ! python3 - "$sx" "$GROUP" "$ARTIFACT" "$ver" 2> "$sx/fixture-build.err" <<'PY'
import hashlib, os, sys, zipfile
sx, group, artifact, version = sys.argv[1:5]
A = "so-bytes-arm64"; X = "so-bytes-x86"
def mk(tag, versions, members, extra=None, bytes_map=None):
    for v in versions:
        d = os.path.join(sx, "inv-" + tag, *group.split("."))
        d = os.path.join(d, artifact, v)
        os.makedirs(d, exist_ok=True)
        if members is None:
            open(os.path.join(d, "%s-%s.aar" % (artifact, v)), "w").write("not an archive")
            continue
        with zipfile.ZipFile(os.path.join(d, "%s-%s.aar" % (artifact, v)), "w") as z:
            z.writestr("AndroidManifest.xml", "<manifest/>")
            for m in members:
                z.writestr(m, (bytes_map or {}).get(m, A if "arm64" in m else X))
        if extra:
            with zipfile.ZipFile(os.path.join(d, extra), "w") as z:
                z.writestr("AndroidManifest.xml", "<manifest/>")
both = ["jni/arm64-v8a/libmoq5_jni.so", "jni/x86_64/libmoq5_jni.so"]
mk("good", [version], both)
mk("wrong-version", ["9.9.9"], both)
mk("extra-aar", [version], both, extra="unexpected.aar")
mk("image-mismatch", [version], both, bytes_map={"jni/arm64-v8a/libmoq5_jni.so": "tampered"})
mk("members", [version], ["jni/arm64-v8a/libmoq5_jni.so"])
mk("unreadable", [version], None)
# The same archive, byte for byte, at a path that is not the Maven coordinate.
_src = os.path.join(sx, "inv-good", *group.split("."))
_src = os.path.join(_src, artifact, version, "%s-%s.aar" % (artifact, version))
_dst = os.path.join(sx, "inv-misplaced", *group.split("."))
_dst = os.path.join(_dst, artifact, version, "not-a-maven-coordinate")
os.makedirs(_dst, exist_ok=True)
with open(_src, "rb") as _f, open(os.path.join(_dst, "%s-%s.aar" % (artifact, version)), "wb") as _g:
    _g.write(_f.read())
os.makedirs(os.path.join(sx, "inv-coordinate-absent"), exist_ok=True)
# A duplicate member IS produced by writing the same name twice. The writer
# warns about it; that warning is expected, and the control below asserts it
# precisely rather than silencing every warning this interpreter might raise.
src = os.path.join(sx, "inv-good", *group.split("."))
src = os.path.join(src, artifact, version, "%s-%s.aar" % (artifact, version))
dst_dir = os.path.join(sx, "inv-duplicate-member", *group.split("."))
dst_dir = os.path.join(dst_dir, artifact, version)
os.makedirs(dst_dir, exist_ok=True)
zin = zipfile.ZipFile(src)
with zipfile.ZipFile(os.path.join(dst_dir, "%s-%s.aar" % (artifact, version)), "w") as z:
    for n in zin.namelist():
        z.writestr(n, zin.read(n))
    z.writestr("jni/x86_64/libmoq5_jni.so", X)
with open(os.path.join(sx, "inv-expected-images.txt"), "w") as f:
    f.write("jni/arm64-v8a/libmoq5_jni.so=%s\njni/x86_64/libmoq5_jni.so=%s\n"
            % (hashlib.sha256(A.encode()).hexdigest(), hashlib.sha256(X.encode()).hexdigest()))
if os.environ.get("MOQ5_PUBBIND_BREAK_FIXTURE") == "1":
    sys.exit(73)
PY
    then
        fail "selftest.fixture-build" "the artifact fixture producer failed; its output is not fit to test against: $(tail -2 "$sx/fixture-build.err" | tr '\n' ' ')"
        _fixtures_ready=0
    else
        pass "selftest.fixture-build" "the artifact fixture producer completed, so its archives may be consumed"
        _fixtures_ready=1
    fi
    if [ "$_fixtures_ready" -ne 1 ]; then
        fail "selftest.fixture-consumed" "the artifact checks are not run against a fixture set whose producer failed"
    else
        pass "selftest.fixture-consumed" "the artifact checks run only against a completed fixture set"
    _imgs=$(cat "$sx/inv-expected-images.txt" 2>/dev/null)
    _dupname='jni/x86_64/libmoq5_jni.so'
    _wl=$(grep -c 'UserWarning:' "$sx/fixture-build.err" 2>/dev/null || true)
    _other=$(grep -E '[Ww]arning' "$sx/fixture-build.err" 2>/dev/null | grep -vc 'UserWarning: Duplicate name' || true)
    _dupcount=$(python3 - "$sx" "$GROUP" "$ARTIFACT" "$ver" "$_dupname" <<'DUP'
import os, sys, zipfile
sx, group, artifact, version, name = sys.argv[1:6]
d = os.path.join(sx, "inv-duplicate-member", *group.split("."))
z = zipfile.ZipFile(os.path.join(d, artifact, version, "%s-%s.aar" % (artifact, version)))
print(sum(1 for n in z.namelist() if n == name))
DUP
)
    if [ "$_wl" = 1 ] && [ "$_other" = 0 ] && [ "$_dupcount" = 2 ] \
       && grep -q "UserWarning: Duplicate name: '$_dupname'" "$sx/fixture-build.err"; then
        pass "selftest.fixture-duplicate-warning" "the duplicate-member fixture really holds two '$_dupname' entries, and its one expected writer warning is asserted rather than silenced"
    else
        fail "selftest.fixture-duplicate-warning" "expected exactly one Duplicate-name warning and two duplicate entries; got warnings=$_wl other=$_other entries=$_dupcount"
    fi
    # shellcheck disable=SC2086  # the image list is two whitespace-separated pairs
    for case in good:0: wrong-version:1:wrong-version extra-aar:1:extra-aar \
                image-mismatch:1:image-mismatch members:1:members \
                unreadable:1:unreadable coordinate-absent:1:coordinate-absent \
                misplaced:1:aar-absent duplicate-member:1:duplicate-member; do
        _tag=${case%%:*}; _rest=${case#*:}; _want=${_rest%%:*}; _code=${_rest#*:}
        if inventory "$sx/inv-$_tag" "$sx/inv-$_tag.txt" "$ver" $_imgs > "$sx/inv-$_tag.log" 2>&1; then _got=0; else _got=1; fi
        if [ "$_got" -ne "$_want" ]; then
            fail "selftest.inventory-$_tag" "expected exit $_want, got $_got: $(head -1 "$sx/inv-$_tag.log")"
        elif [ "$_want" -eq 0 ]; then
            pass "selftest.inventory-$_tag" "the pinned GAV, both members and both audited images are accepted"
        elif [ "$(awk 'NR==1 {print $2}' "$sx/inv-$_tag.log")" = "$_code" ]; then
            pass "selftest.inventory-$_tag" "refused by its own reason: $(head -1 "$sx/inv-$_tag.log" | cut -c1-96)"
        else
            fail "selftest.inventory-$_tag" "refused, but not by '$_code': $(head -1 "$sx/inv-$_tag.log")"
        fi
    done
    if inventory "$sx/inv-good" "$sx/inv-bad-expect.txt" "$ver" "jni/arm64-v8a/libmoq5_jni.so=" "jni/x86_64/libmoq5_jni.so=" \
        > "$sx/inv-bad-expect.log" 2>&1; then
        fail "selftest.inventory-expected-image-invalid" "an empty audited digest was accepted as an image identity"
    else
        [ "$(awk 'NR==1 {print $2}' "$sx/inv-bad-expect.log")" = expected-image-invalid ] \
            && pass "selftest.inventory-expected-image-invalid" "an empty audited digest cannot stand in for an image identity" \
            || fail "selftest.inventory-expected-image-invalid" "refused for the wrong reason: $(head -1 "$sx/inv-bad-expect.log")"
    fi
    if inventory "$sx/inv-good" "$sx/inv-bad-ver.txt" "" $_imgs > "$sx/inv-bad-ver.log" 2>&1; then
        fail "selftest.inventory-version-authority" "an empty version was accepted as the pinned coordinate"
    else
        [ "$(awk 'NR==1 {print $2}' "$sx/inv-bad-ver.log")" = version-authority ] \
            && pass "selftest.inventory-version-authority" "the coordinate is pinned by the authority, never inferred" \
            || fail "selftest.inventory-version-authority" "refused for the wrong reason: $(head -1 "$sx/inv-bad-ver.log")"
    fi
    fi
    }

    supervision_section() {

    _saved_timeout=$CMD_TIMEOUT
    CMD_TIMEOUT=3
    MOQ5_PUBBIND_ARMED="$sx/armed-1"; MOQ5_PUBBIND_MARKER="$sx/marker-1"
    export MOQ5_PUBBIND_ARMED MOQ5_PUBBIND_MARKER
    run_owned sup-stubborn /bin/sh "$sx/stubborn.sh"; _rc=$?
    if [ "$_rc" -eq 124 ] && [ "$quiescent" -eq 1 ]; then
        pass "selftest.supervision-timeout" "a TERM-ignoring child is stopped at the deadline and escalated to quiescence"
    else
        fail "selftest.supervision-timeout" "expected a bounded stop (124, quiescent); got $_rc, quiescent=$quiescent"
    fi
    _al=$(sed -n 's/^session=\([0-9]*\).*/\1/p' "$evidence/ownership.txt" | tail -1)
    typed_dispatch "$(route_identity "$evidence" "$_al")" "$_al" \
        "selftest.supervision-timeout" "the fixture group" cleanup_or_abort "$evidence"
    ownership_failed=0; owned_group=""; owned_pid=""; quiescent=1

    MOQ5_PUBBIND_ARMED="$sx/armed-2"; MOQ5_PUBBIND_MARKER="$sx/marker-2"
    run_owned sup-descendant /bin/sh "$sx/descendant.sh"; _rc=$?
    _before=$(wc -c < "$sx/marker-2.child" 2>/dev/null | tr -d ' ')
    sleep 2
    _after=$(wc -c < "$sx/marker-2.child" 2>/dev/null | tr -d ' ')
    if [ "$_rc" -eq 124 ] && [ "$quiescent" -eq 1 ] && [ "$_before" = "$_after" ]; then
        pass "selftest.supervision-descendant" "a descendant of an owned child is stopped too (marker frozen at $_after bytes)"
    else
        fail "selftest.supervision-descendant" "rc=$_rc quiescent=$quiescent descendant marker $_before -> $_after"
    fi
    _al=$(sed -n 's/^session=\([0-9]*\).*/\1/p' "$evidence/ownership.txt" | tail -1)
    typed_dispatch "$(route_identity "$evidence" "$_al")" "$_al" \
        "selftest.supervision-descendant" "the fixture group" cleanup_or_abort "$evidence"
    ownership_failed=0; owned_group=""; owned_pid=""; quiescent=1

    MOQ5_PUBBIND_ARMED="$sx/armed-3"; MOQ5_PUBBIND_MARKER="$sx/marker-3"
    run_owned sup-prompt /bin/sh "$sx/prompt.sh"; _rc=$?
    if [ "$_rc" -eq 7 ] && [ "$(cat "$evidence/sup-prompt.status")" = 7 ]; then
        pass "selftest.supervision-prompt" "a command that finishes inside its bound returns its own status, unstopped"
    else
        fail "selftest.supervision-prompt" "expected status 7, got $_rc / $(cat "$evidence/sup-prompt.status" 2>/dev/null)"
    fi
    CMD_TIMEOUT=$_saved_timeout
    unset MOQ5_PUBBIND_ARMED MOQ5_PUBBIND_MARKER
    }

    # witness_matches_inventory <inventory> <witness>
    #   Prints `ok` when the witness accounts for EXACTLY the two distinct owned
    #   identities the inventory records -- each matched once, each observed
    #   empty -- and `refused: <reason>` otherwise. Both files are acquired
    #   through the same checked reader the settlement authority uses, so a
    #   reader that errors, with or without producing its output first, and a
    #   tail that stops mid-record are reasons rather than an empty set. Every
    #   record is parsed whole against the shape this driver writes: a
    #   malformed, foreign or unrepresentable record is refused by name, never
    #   filtered away, and an identity must lie in the same owned-group domain
    #   the cleanup dispatch accepts. Printing nothing is not a verdict --
    #   witness_verdict is how a caller tells acceptance from a helper that
    #   stopped early.
    witness_matches_inventory() {
        _wmi_i="$sx/.wmi-inventory.$$"; _wmi_w="$sx/.wmi-witness.$$"
        inventory_read "$1" "$_wmi_i" \
            || { printf 'refused: the inventory %s could not be read completely\n' "$1"; return 0; }
        inventory_read "$2" "$_wmi_w" \
            || { printf 'refused: the witness %s could not be read completely\n' "$2"; return 0; }
        _wmi_inv=" "; _wmi_n=0
        while IFS= read -r _wmi_l; do
            case "$_wmi_l" in
                session=*) ;;
                *) declared_inventory_metadata "$_wmi_l" && continue
                   printf 'refused: an unrecognised inventory record: %s\n' "$_wmi_l"; return 0 ;;
            esac
            _wmi_r=${_wmi_l#session=}
            _wmi_id=${_wmi_r%% *}
            case "$_wmi_r" in
                "$_wmi_id observed="?*) ;;
                *) printf 'refused: a malformed inventory record: %s\n' "$_wmi_l"; return 0 ;;
            esac
            case "${_wmi_r#"$_wmi_id observed="}" in
                *' '*) printf 'refused: a malformed inventory record: %s\n' "$_wmi_l"; return 0 ;;
            esac
            valid_owned_group "$_wmi_id" \
                || { printf 'refused: the inventory records an identity that is not a usable owned group: %s\n' "$_wmi_l"; return 0; }
            case "$_wmi_inv" in
                *" $_wmi_id "*) printf 'refused: the inventory records the same group twice: %s\n' "$_wmi_id"; return 0 ;;
            esac
            _wmi_inv="$_wmi_inv$_wmi_id "
            _wmi_n=$((_wmi_n + 1))
        done < "$_wmi_i"
        if [ "$_wmi_n" -ne 2 ]; then
            printf 'refused: the inventory records %s distinct session(s), not two\n' "$_wmi_n"; return 0
        fi
        _wmi_seen=" "; _wmi_m=0
        while IFS= read -r _wmi_l; do
            case "$_wmi_l" in
                group=*) ;;
                '') continue ;;
                *) printf 'refused: an unrecognised witness record: %s\n' "$_wmi_l"; return 0 ;;
            esac
            _wmi_r=${_wmi_l#group=}
            _wmi_id=${_wmi_r%% *}
            case "$_wmi_r" in
                "$_wmi_id state="?*) ;;
                *) printf 'refused: a malformed witness record: %s\n' "$_wmi_l"; return 0 ;;
            esac
            _wmi_st=${_wmi_r#"$_wmi_id state="}
            case "$_wmi_st" in
                live|empty|unknown) ;;
                *) printf 'refused: an unrepresentable observation: %s\n' "$_wmi_l"; return 0 ;;
            esac
            valid_owned_group "$_wmi_id" \
                || { printf 'refused: the witness names an identity that is not a usable owned group: %s\n' "$_wmi_l"; return 0; }
            case "$_wmi_inv" in
                *" $_wmi_id "*) ;;
                *) printf 'refused: the witness accounts for %s, which the inventory does not record\n' "$_wmi_id"; return 0 ;;
            esac
            case "$_wmi_seen" in
                *" $_wmi_id "*) printf 'refused: the witness accounts for %s twice\n' "$_wmi_id"; return 0 ;;
            esac
            if [ "$_wmi_st" != empty ]; then
                printf 'refused: the witness does not show the recorded group %s empty: %s\n' "$_wmi_id" "$_wmi_st"; return 0
            fi
            _wmi_seen="$_wmi_seen$_wmi_id "
            _wmi_m=$((_wmi_m + 1))
        done < "$_wmi_w"
        rm -f "$_wmi_i" "$_wmi_w"
        if [ "$_wmi_m" -ne "$_wmi_n" ]; then
            printf 'refused: the witness accounts for %s of the %s recorded groups\n' "$_wmi_m" "$_wmi_n"; return 0
        fi
        printf 'ok\n'
    }
    # witness_verdict <status> <output>
    #   The comparison's result, read from BOTH how the helper ended and what it
    #   said. Silence is not acceptance: a helper that stopped before printing a
    #   verdict did not run to completion, and that is a reason of its own.
    witness_verdict() {
        if [ "$1" -ne 0 ]; then
            printf 'the witness comparison ended with status %s' "$1"; return 0
        fi
        case "$2" in
            ok) printf '' ;;
            'refused: '*) printf '%s' "${2#refused: }" ;;
            *) printf 'the witness comparison printed no verdict: %s' "$(printf '%s' "$2" | tr '\n' ' ')" ;;
        esac
    }

    lock_path_for() {
        printf '%s/moq5-pubbind-lock-%s' "$sx/locks" \
            "$(printf '%s' "$(cd "$(dirname "$1")" && pwd)/$(basename "$1")" | shasum -a 256 | cut -c1-16)"
    }
    drive() {   # drive <subject> <evidence> <log> [extra env assignments...]
        _dsub=$1; _dev=$2; _dlog=$3; shift 3
        env MOQ5_PUBBIND_STUB_PUBLISH="$sx" MOQ5_PUBBIND_STUB_AUDIT=1 MOQ5_PUBBIND_LOCK_ROOT="$sx/locks" \
            MOQ5_PUBBIND_STUB_RECEIPTS=1 MOQ5_PUBBIND_TIMEOUT=300 "$@" \
            /bin/sh "$driver" --harness --subject "$_dsub" --evidence "$_dev" > "$_dlog" 2>&1
    }

    case "${section:-}" in
        ""|rest|settlement|witness|witnessprobe|ackfixture|ackcleanup|parser|artifact|supervision|directchild|negctl|barriers-interrupt|barriers-race|barriers-own) : ;;
        *) echo "$_self: unknown --section '$section'" >&2; exit 2 ;;
    esac

    want supervision && supervision_section

    if want barriers-interrupt; then
    # ---- F1: interruption stops owned work BEFORE restoration -------------
    cp "$root/jni/moq5_registry.h" "$sx/subject-int.h"
    _int_before=$(decl_sha256 "$sx/subject-int.h") \
        || abort_harness "selftest.interrupt-quiescence" "the interrupt subject" \
               "its digest could not be produced before the run"
    env MOQ5_PUBBIND_FAULT=fixture-audit-late \
        MOQ5_PUBBIND_FIXTURE="$(arming_fixture)" \
        MOQ5_PUBBIND_STUB_PUBLISH="$sx" MOQ5_PUBBIND_STUB_AUDIT=1 MOQ5_PUBBIND_STUB_RECEIPTS=1 MOQ5_PUBBIND_TIMEOUT=300 MOQ5_PUBBIND_LOCK_ROOT="$sx/locks" \
        MOQ5_PUBBIND_ARMED="$sx/armed-int" \
        MOQ5_PUBBIND_MARKER="$sx/marker-int" \
        /bin/sh "$driver" --harness --subject "$sx/subject-int.h" --evidence "$sx/ev-int" \
        > "$sx/int.log" 2>&1 &
    _dpid=$!
    if barrier "$sx/armed-int"; then
        kill -TERM "$_dpid" 2>/dev/null || true
        reap_bounded "$_dpid" 90 \
            || settle_or_abort "$_dpid" "$(recorded_group "$sx/ev-int")" \
                   "selftest.interrupt-quiescence" "the interrupted driver" \
                   "$sx/ev-int"
        _st=$reaped_status
        settle_recorded_groups "$sx/ev-int" "selftest.interrupt-quiescence" \
            "the interrupted driver's own fixtures"
        _qgrp=$(sed -n 's/^group=\([0-9]*\) final=.*/\1/p' "$sx/ev-int/quiescence.txt" 2>/dev/null | tail -1)
        _qfin=$(sed -n 's/^group=[0-9]* final=//p' "$sx/ev-int/quiescence.txt" 2>/dev/null | tail -1)
        _qm=$(sed -n 's/^marker_bytes_at_quiescence=//p' "$sx/ev-int/quiescence.txt" 2>/dev/null | tail -1)
        _mk=$(wc -c < "$sx/marker-int" 2>/dev/null | tr -d ' ')
        _order=$(tr '\n' ' ' < "$sx/ev-int/order.txt" 2>/dev/null)
        _now=$(decl_sha256 "$sx/subject-int.h") \
            || abort_harness "selftest.interrupt-quiescence" "the interrupt subject" \
                   "its digest could not be produced after the run"
        # The group being EMPTY is what makes the marker final: nothing is left
        # that could write to it. No elapsed-time sample is involved.
        if [ "$_st" = 130 ] && [ "$_qfin" = empty ] && [ "$(harness_group_live "$_qgrp")" = empty ] \
           && [ -n "$_qm" ] && [ "$_qm" = "$_mk" ] && [ "$_now" = "$_int_before" ]; then
            pass "selftest.interrupt-quiescence" "an interrupted driver proved its owned group empty at $_qm marker bytes, which nothing can now change, then restored the perturbed subject"
        else
            fail "selftest.interrupt-quiescence" "exit=$_st final=$_qfin group=$(harness_group_live "$_qgrp") marker=$_mk quiescence=$_qm subject=$([ "$_now" = "$_int_before" ] && echo restored || echo PERTURBED)"
        fi
        case "$_order" in
            *"quiescence:empty restore:ok"*)
                pass "selftest.interrupt-order" "the recorded order is quiescence before restoration: $_order" ;;
            *)
                fail "selftest.interrupt-order" "restoration is not recorded after proved quiescence: $_order" ;;
        esac
        grep -q 'PASS\[pubbind.restore\]' "$sx/int.log" \
            && pass "selftest.interrupt-reports" "the interrupted exit reports its restoration" \
            || fail "selftest.interrupt-reports" "the interrupted exit reported no restoration verdict"
        cleanup_or_abort "$_qgrp" "selftest.interrupt-quiescence" \
            "the interrupted driver's fixture" "$sx/ev-int"
    else
        fail "selftest.interrupt-quiescence" "the fixture never armed; see $sx/int.log"
        settle_or_abort "$_dpid" "$(recorded_group "$sx/ev-int")" \
            "selftest.interrupt-quiescence" "the interrupted driver and its fixture" \
            "$sx/ev-int"
        fail "selftest.interrupt-order" "not reached"
        fail "selftest.interrupt-reports" "not reached"
    fi

    fi

    # Driven only by the cleanup controls, never as part of an ordinary run.
    if [ "${section:-}" = ackcleanup ]; then
        ack_cleanup_of "${MOQ5_PUBBIND_ACK_CLEANUP_PROBE:?the cleanup section needs a probe}" \
            "selftest.ack-cleanup-probe" "a synthetic cleanup ledger"
        pass "selftest.ack-cleanup-probe" "the synthetic ledger was settled without refusal"
    fi

    # Driven only by the witness controls, never as part of an ordinary run.
    #
    # The checked reader is a HARNESS-ONLY seam: `inventory_read` consults
    # MOQ5_PUBBIND_READER only when the driver was launched with --harness, so
    # an ordinary run can never have its reads redirected. A control that wants
    # to prove what a failing reader does must therefore run the comparison in
    # a harness child, exactly as the cleanup and parser controls do; setting
    # the variable around an in-process call proves nothing, because the
    # variable is not read there.
    if [ "${section:-}" = witnessprobe ]; then
        # Deliberately induced control failures, so the CALLER's oracle can be
        # proved to reject a child that did not do what it was asked. Each is
        # named, exactly asserted by a control below, and reachable only in a
        # harness child of this section.
        case "${MOQ5_PUBBIND_WITNESS_FAULT-}" in
            '') ;;
            silent-73)
                exit 73 ;;
            wrong-exit)
                printf 'witness-probe-refused: the inventory %s could not be read completely\n' \
                    "${MOQ5_PUBBIND_WITNESS_INVENTORY-}"
                exit 0 ;;
            *)
                echo "$_self: unknown MOQ5_PUBBIND_WITNESS_FAULT '$MOQ5_PUBBIND_WITNESS_FAULT'" >&2
                exit 2 ;;
        esac
        _wp_out=$(witness_matches_inventory \
            "${MOQ5_PUBBIND_WITNESS_INVENTORY:?the witness section needs an inventory}" \
            "${MOQ5_PUBBIND_WITNESS_WITNESS:?the witness section needs a witness}")
        _wp_st=$?
        _wp_why=$(witness_verdict "$_wp_st" "$_wp_out")
        if [ -n "$_wp_why" ]; then
            printf 'witness-probe-refused: %s\n' "$_wp_why"
            fail "selftest.witness-probe" "$_wp_why"
        else
            pass "selftest.witness-probe" "the witness accounts for exactly the recorded identities"
        fi
    fi

    # Driven only by the parser controls, never as part of an ordinary run.
    if [ "${section:-}" = parser ]; then
        settle_recorded_groups "${MOQ5_PUBBIND_PARSER_PROBE:?the parser section needs a probe}" \
            "selftest.parser-probe" "a synthetic inventory"
        pass "selftest.parser-probe" "the synthetic inventory was settled without refusal"
    fi

    if want settlement; then
    # ---- a reaped child is not a settled fixture --------------------------
    # Each case drives the REAL reached-barrier caller with independent process
    # and group outcomes: the child driver is given a blinded inspector, so it
    # leaves its own fixture running and exits with a status the parent really
    # observes; the parent's inspector answers about processes and refuses about
    # groups. The root is therefore genuinely reaped while its separately
    # recorded fixture cannot be proved settled.
    for _sec in barriers-race barriers-own; do
        run_section "$_sec" "$sx/settle-$_sec.log" \
            MOQ5_PUBBIND_HARNESS_PS="$sx/ps-pid-only" \
            MOQ5_PUBBIND_CHILD_PS="$sx/ps-blind" \
            MOQ5_PUBBIND_NO_ARM= MOQ5_PUBBIND_BARRIER_BOUND=15
        _ssc=$?
        if [ "$_ssc" -ne 0 ] \
           && grep -q "HARNESS ABORTED: cleanup of the first invocation's own fixtures" "$sx/settle-$_sec.log" \
           && ! grep -q '^PASS\[selftest.lock-two-owner\]' "$sx/settle-$_sec.log" \
           && ! grep -q '^PASS\[selftest.ownership-exclusion\]' "$sx/settle-$_sec.log" \
           && nothing_after_abort "$sx/settle-$_sec.log"; then
            pass "selftest.settled-group-$_sec" "a reaped root is not a settled fixture: $(grep -m1 'HARNESS ABORTED' "$sx/settle-$_sec.log")"
        else
            fail "selftest.settled-group-$_sec" "exit=$_ssc aborted=$(grep -c 'HARNESS ABORTED' "$sx/settle-$_sec.log") ownership-pass=$(grep -cE '^PASS\[selftest.(lock-two-owner|ownership-exclusion)\]' "$sx/settle-$_sec.log")"
        fi
    done

    # The matched half: with a working inspection the SAME separate fixture is
    # settled and the case continues, still reporting the child's real exit
    # status rather than calling a known exit an unobserved wait.
    for _sec in barriers-race barriers-own; do
        run_section "$_sec" "$sx/settled-ok-$_sec.log" \
            MOQ5_PUBBIND_CHILD_PS="$sx/ps-blind" \
            MOQ5_PUBBIND_NO_ARM= MOQ5_PUBBIND_BARRIER_BOUND=15
        _ssc=$?
        if ! grep -q 'HARNESS ABORTED' "$sx/settled-ok-$_sec.log" \
           && grep -qE '^PASS\[selftest.(lock-two-owner|ownership-exclusion)\]' "$sx/settled-ok-$_sec.log" \
           && grep -qE 'first exit (1|130|[0-9]+)' "$sx/settled-ok-$_sec.log" \
           && grep -q 'harness controls: ' "$sx/settled-ok-$_sec.log"; then
            pass "selftest.settled-group-continue-$_sec" "the separate fixture is settled and the case continues, keeping the child's observed status: $(grep -oE 'first exit [0-9]+' "$sx/settled-ok-$_sec.log" | head -1)"
        else
            fail "selftest.settled-group-continue-$_sec" "exit=$_ssc aborted=$(grep -c 'HARNESS ABORTED' "$sx/settled-ok-$_sec.log") ownership-pass=$(grep -cE '^PASS\[selftest.(lock-two-owner|ownership-exclusion)\]' "$sx/settled-ok-$_sec.log")"
        fi
    done

    # ---- the inventory is acquired, not assumed ---------------------------
    # A reader that refuses, and one that manages a prefix first, must both stop
    # the run: the set of groups to settle is unknown, which is not the same as
    # knowing there are none.
    for _rc in "refuses:$sx/reader-refuses:barriers-race" \
               "refuses:$sx/reader-refuses:barriers-own" \
               "partial:$sx/reader-partial:barriers-own"; do
        _rk=${_rc%%:*}; _rrest=${_rc#*:}; _rprog=${_rrest%%:*}; _rsec=${_rrest#*:}
        run_section "$_rsec" "$sx/read-$_rk-$_rsec.log" \
            MOQ5_PUBBIND_READER="$_rprog" \
            MOQ5_PUBBIND_CHILD_PS="$sx/ps-blind" \
            MOQ5_PUBBIND_NO_ARM= MOQ5_PUBBIND_BARRIER_BOUND=15
        _rrc=$?
        if [ "$_rrc" -ne 0 ] \
           && grep -q 'could not be read completely' "$sx/read-$_rk-$_rsec.log" \
           && grep -q 'HARNESS ABORTED' "$sx/read-$_rk-$_rsec.log" \
           && ! grep -qE '^PASS\[selftest.(lock-two-owner|ownership-exclusion)\]' "$sx/read-$_rk-$_rsec.log" \
           && nothing_after_abort "$sx/read-$_rk-$_rsec.log"; then
            pass "selftest.ownership-inventory-$_rk-$_rsec" "an inventory that cannot be read completely stops the run instead of reading as empty"
        else
            fail "selftest.ownership-inventory-$_rk-$_rsec" "exit=$_rrc aborted=$(grep -c 'HARNESS ABORTED' "$sx/read-$_rk-$_rsec.log") ownership-pass=$(grep -cE '^PASS\[selftest.(lock-two-owner|ownership-exclusion)\]' "$sx/read-$_rk-$_rsec.log")"
        fi
    done

    # Readable, and TWO recorded groups: the proof is the caller's OWN witness,
    # written at its continuation boundary before the coordinator's independent
    # cleanup could repair an omission.
    _twolog="$sx/read-two-groups.log"
    _twoev="$sx/ev-read-two-groups"
    _twoinv="$_twoev/fixtures/ev-own-a/ownership.txt"
    run_section barriers-own "$_twolog" \
        MOQ5_PUBBIND_CHILD_PS="$sx/ps-blind" \
        MOQ5_PUBBIND_INVENTORY_APPEND="$_twoinv" \
        MOQ5_PUBBIND_NO_ARM= MOQ5_PUBBIND_BARRIER_BOUND=15
    _twrc=$?
    _twowit="$_twoev/settled-ev-own-a.txt"
    _twoout=$(witness_matches_inventory "$_twoinv" "$_twowit"); _twost=$?
    _twowhy=$(witness_verdict "$_twost" "$_twoout")
    if [ -z "$_twowhy" ] \
       && ! grep -q 'HARNESS ABORTED' "$_twolog" \
       && grep -q '^PASS\[selftest.ownership-exclusion\]' "$_twolog"; then
        pass "selftest.ownership-inventory-every-group-settled" "the child recorded exactly two distinct groups and the caller's own witness accounts for each of them, empty, at its continuation boundary"
    else
        fail "selftest.ownership-inventory-every-group-settled" "${_twowhy:-the section aborted or lost its ownership row} (exit=$_twrc)"
    fi



    fi

    if want witness; then
    # ---- the witness must account for the exact recorded identities -------
    # Component checks, with no worker and no signal: synthetic inventory and
    # witness pairs fed to the same comparison the two-group control uses.
    # wmi_case <name> <inventory lines> <witness lines> <expect: ok|reason> [reader]
    #   The optional reader is the program the checked acquisition uses, so a
    #   reader that fails after producing its whole output can be pinned without
    #   damaging either file.
    # wmi_child <name> <inventory> <witness> <reader> [fault]
    #   Runs ONE witness comparison in a harness child -- the only mode in
    #   which the checked reader is consulted at all -- and leaves its status
    #   in _wch_rc, its named refusal in _wch_why and its log at _wch_log.
    wmi_child() {
        _wch_d="$sx/wmi-$1"
        mkdir -p "$_wch_d"
        rm -rf "$_wch_d/run"
        _wch_log="$_wch_d/log"
        env MOQ5_PUBBIND_READER="$4" \
            MOQ5_PUBBIND_WITNESS_INVENTORY="$2" \
            MOQ5_PUBBIND_WITNESS_WITNESS="$3" \
            ${5:+MOQ5_PUBBIND_WITNESS_FAULT=$5} \
            /bin/sh "$driver" --selftest --harness --section witnessprobe \
            --evidence "$_wch_d/run" > "$_wch_log" 2>&1
        _wch_rc=$?
        _wch_n=$(grep -c '^witness-probe-refused: ' "$_wch_log")
        _wch_why=$(sed -n 's/^witness-probe-refused: //p' "$_wch_log" | sed -n '1p')
        return 0
    }

    # wmi_child_verdict <expected-status> <expected-reason|"">
    #   Echoes nothing when the child did EXACTLY what was declared, and the
    #   named reason it did not otherwise. A child that failed for its own
    #   reasons is never the expected refusal: an unexpected status, a missing
    #   reason, a second reason or different text each has its own name, so a
    #   broken control cannot be counted as a successful negative.
    wmi_child_verdict() {
        _wcv_st=$1; _wcv_want=$2
        if [ "$_wch_rc" -ne "$_wcv_st" ]; then
            printf 'the control exited %s, not the declared %s' "$_wch_rc" "$_wcv_st"
            return 0
        fi
        if [ -z "$_wcv_want" ]; then
            [ "$_wch_n" -eq 0 ] \
                || { printf 'the control refused when it was declared to accept: %s' "$_wch_why"; return 0; }
            grep -q '^PASS\[selftest.witness-probe\]' "$_wch_log" \
                || { printf 'the control neither refused nor recorded its acceptance'; return 0; }
            printf ''
            return 0
        fi
        [ "$_wch_n" -ne 0 ] \
            || { printf 'the control exited %s but named no refusal' "$_wch_rc"; return 0; }
        [ "$_wch_n" -eq 1 ] \
            || { printf 'the control named %s refusals; one is expected' "$_wch_n"; return 0; }
        [ "$_wch_why" = "$_wcv_want" ] \
            || { printf 'the control refused with %s' "$_wch_why"; return 0; }
        printf ''
    }

    wmi_case() {
        _wc_n=$1
        mkdir -p "$sx/wmi-$_wc_n"
        printf '%b' "$2" > "$sx/wmi-$_wc_n/inv.txt"
        printf '%b' "$3" > "$sx/wmi-$_wc_n/wit.txt"
        if [ -n "${5-}" ]; then
            # A reader case runs in a HARNESS child, and the child is held to
            # the WHOLE declaration: the exact status AND the exact refusal
            # this inventory path must produce. A control that failed for its
            # own reasons -- a bad section, a missing fixture, an early exit --
            # names a different defect and can never stand in for the read
            # refusal under test.
            wmi_child "$_wc_n" "$sx/wmi-$_wc_n/inv.txt" "$sx/wmi-$_wc_n/wit.txt" "$5"
            _wc_want="the inventory $sx/wmi-$_wc_n/inv.txt could not be read completely"
            _wc_bad=$(wmi_child_verdict 1 "$_wc_want")
            if [ -z "$_wc_bad" ]; then
                pass "selftest.witness-$_wc_n" "the checked reader's failure is refused by name, with the declared status: $_wch_why"
            else
                fail "selftest.witness-$_wc_n" "$_wc_bad"
            fi
            return 0
        fi
        _wc_out=$(witness_matches_inventory "$sx/wmi-$_wc_n/inv.txt" "$sx/wmi-$_wc_n/wit.txt"); _wc_st=$?
        _wc_why=$(witness_verdict "$_wc_st" "$_wc_out")
        if [ "$4" = ok ]; then
            [ -z "$_wc_why" ] \
                && pass "selftest.witness-$_wc_n" "the witness accounts for exactly the recorded identities" \
                || fail "selftest.witness-$_wc_n" "unexpectedly refused: $_wc_why"
        else
            [ -n "$_wc_why" ] \
                && pass "selftest.witness-$_wc_n" "refused by name: $_wc_why" \
                || fail "selftest.witness-$_wc_n" "accepted a witness it should have refused"
        fi
    }

    # The caller's own oracle, proved to REJECT. Each control drives the same
    # child boundary and asserts that `wmi_child_verdict` names the defect --
    # so a broken control can never be counted as a successful negative, which
    # is exactly what it did before (chats/libmoq/1740 F1).
    #   wmi_oracle_control <name> <expected-status> <expected-reason|""> <why>
    wmi_oracle_control() {
        _woc_n=$1; _woc_st=$2; _woc_want=$3; _woc_why=$4
        _woc_bad=$(wmi_child_verdict "$_woc_st" "$_woc_want")
        if [ -n "$_woc_bad" ]; then
            pass "selftest.witness-oracle-$_woc_n" "$_woc_why: $_woc_bad"
        else
            fail "selftest.witness-oracle-$_woc_n" "the caller's oracle accepted $_woc_why"
        fi
    }
    wmi_case exact-match      'session=4242 observed=live\nsession=4343 observed=live\n' \
                              'group=4242 state=empty\ngroup=4343 state=empty\n' ok
    # The inventory a real run writes opens with its ownership marker and may
    # carry the other records this driver declares; those are preserved.
    wmi_case metadata-kept    'owned-by=moq5-pubbind-driver\nsession=4242 observed=live\njava_home=/x major=21\nsession=4343 observed=direct-child\n' \
                              'group=4242 state=empty\ngroup=4343 state=empty\n' ok
    wmi_case wrong-identities 'session=4242 observed=live\nsession=4343 observed=live\n' \
                              'group=5252 state=empty\ngroup=5353 state=empty\n' reason
    wmi_case duplicate-group  'session=4242 observed=live\nsession=4242 observed=live\n' \
                              'group=4242 state=empty\ngroup=4242 state=empty\n' reason
    wmi_case duplicate-row    'session=4242 observed=live\nsession=4343 observed=live\n' \
                              'group=4242 state=empty\ngroup=4242 state=empty\n' reason
    wmi_case witness-short    'session=4242 observed=live\nsession=4343 observed=live\n' \
                              'group=4242 state=empty\n' reason
    wmi_case witness-live     'session=4242 observed=live\nsession=4343 observed=live\n' \
                              'group=4242 state=empty\ngroup=4343 state=live\n' reason
    # A record outside the shapes this driver writes is refused, not skipped --
    # in either file, and whether it is foreign, malformed or unrepresentable.
    wmi_case foreign-inventory-record 'session=4242 observed=live\nsession=4343 observed=live\nnot-a-record\n' \
                              'group=4242 state=empty\ngroup=4343 state=empty\n' reason
    wmi_case malformed-session 'session=4242 observed=live\nsession=4343 observed=live\nsession=555junk observed=live\n' \
                              'group=4242 state=empty\ngroup=4343 state=empty\n' reason
    wmi_case session-unobserved 'session=4242 observed=live\nsession=4343 observed=live\nsession=4444\n' \
                              'group=4242 state=empty\ngroup=4343 state=empty\n' reason
    wmi_case special-ids      'session=0 observed=live\nsession=1 observed=live\n' \
                              'group=0 state=empty\ngroup=1 state=empty\n' reason
    wmi_case foreign-witness-record 'session=4242 observed=live\nsession=4343 observed=live\n' \
                              'group=4242 state=empty\ngroup=4343 state=empty\ngroup=555junk state=empty\n' reason
    wmi_case unrepresentable-state 'session=4242 observed=live\nsession=4343 observed=live\n' \
                              'group=4242 state=empty\ngroup=4343 state=settled\n' reason
    # A tail that stops mid-record is not a complete set of records.
    wmi_case unterminated-inventory 'session=4242 observed=live\nsession=4343 observed=live\nsession=4444 observed=live' \
                              'group=4242 state=empty\ngroup=4343 state=empty\n' reason
    wmi_case unterminated-witness 'session=4242 observed=live\nsession=4343 observed=live\n' \
                              'group=4242 state=empty\ngroup=4343 state=empty\ngroup=4444 state=empty' reason
    # A reader that produces its whole output and THEN fails is a failed read.
    wmi_case reader-output-then-error 'session=4242 observed=live\nsession=4343 observed=live\n' \
                              'group=4242 state=empty\ngroup=4343 state=empty\n' reason \
                              "$sx/reader-output-then-error"
    wmi_case reader-partial   'session=4242 observed=live\nsession=4343 observed=live\n' \
                              'group=4242 state=empty\ngroup=4343 state=empty\n' reason \
                              "$sx/reader-partial"

    # ---- the child boundary's own controls ---------------------------------
    # A complete, successful read through the SAME child path, so the positive
    # behaviour of this boundary is measured and not merely assumed.
    mkdir -p "$sx/wmi-child-ok"
    printf 'session=4242 observed=live\nsession=4343 observed=live\n' > "$sx/wmi-child-ok/inv.txt"
    printf 'group=4242 state=empty\ngroup=4343 state=empty\n' > "$sx/wmi-child-ok/wit.txt"
    wmi_child child-ok "$sx/wmi-child-ok/inv.txt" "$sx/wmi-child-ok/wit.txt" /bin/cat
    _wc_bad=$(wmi_child_verdict 0 "")
    [ -z "$_wc_bad" ] \
        && pass "selftest.witness-child-complete-read" "a complete read through the child path is accepted, status 0 and no refusal" \
        || fail "selftest.witness-child-complete-read" "$_wc_bad"

    # A control that exits non-zero having done nothing must NOT satisfy a
    # reader case. This is the exact false pass 1740 F1 reported.
    wmi_child child-silent "$sx/wmi-child-ok/inv.txt" "$sx/wmi-child-ok/wit.txt" /bin/cat silent-73
    wmi_oracle_control silent-error 1 \
        "the inventory $sx/wmi-child-ok/inv.txt could not be read completely" \
        "a control that exited 73 having compared nothing"

    # The RIGHT refusal text accompanying the WRONG exit status is not the
    # declared outcome either: the reason alone is not the receipt.
    wmi_child child-wrongexit "$sx/wmi-child-ok/inv.txt" "$sx/wmi-child-ok/wit.txt" /bin/cat wrong-exit
    wmi_oracle_control wrong-exit 1 \
        "the inventory $sx/wmi-child-ok/inv.txt could not be read completely" \
        "the expected refusal carried by an exit status that was not declared"

    # A DIFFERENT named refusal -- a real one, from a malformed record rather
    # than from the reader -- must not satisfy a reader case either.
    mkdir -p "$sx/wmi-child-other"
    printf 'session=4242 observed=live\nnot-a-record\n' > "$sx/wmi-child-other/inv.txt"
    printf 'group=4242 state=empty\n' > "$sx/wmi-child-other/wit.txt"
    wmi_child child-other "$sx/wmi-child-other/inv.txt" "$sx/wmi-child-other/wit.txt" /bin/cat
    wmi_oracle_control other-reason 1 \
        "the inventory $sx/wmi-child-other/inv.txt could not be read completely" \
        "a different named refusal standing in for the read failure"
    rm -rf "$sx/wmi-missing"; mkdir -p "$sx/wmi-missing"
    _wc_out=$(witness_matches_inventory "$sx/wmi-missing/absent-inv.txt" "$sx/wmi-missing/absent-wit.txt"); _wc_st=$?
    _wc_why=$(witness_verdict "$_wc_st" "$_wc_out")
    [ -n "$_wc_why" ] \
        && pass "selftest.witness-unreadable" "an unreadable inventory is a named reason, not an empty set: $_wc_why" \
        || fail "selftest.witness-unreadable" "an unreadable inventory was accepted"

    # A caller reads the verdict, not the silence: a comparison that stopped
    # before saying anything has not accepted the witness.
    verdict_case() {   # verdict_case <name> <status> <output> <expect: ok|reason>
        _vc_why=$(witness_verdict "$2" "$3")
        if [ "$4" = ok ]; then
            [ -z "$_vc_why" ] \
                && pass "selftest.witness-$1" "a completed comparison that says ok is an acceptance" \
                || fail "selftest.witness-$1" "unexpectedly refused: $_vc_why"
        else
            [ -n "$_vc_why" ] \
                && pass "selftest.witness-$1" "refused by name: $_vc_why" \
                || fail "selftest.witness-$1" "silence was read as acceptance"
        fi
    }
    verdict_case verdict-ok             0 'ok'          ok
    verdict_case verdict-silent-success 0 ''            reason
    verdict_case verdict-silent-failure 1 ''            reason
    verdict_case verdict-unnamed        0 'refused'     reason
    verdict_case verdict-ok-after-failure 1 'ok'         reason

    fi

    if want ackfixture; then
    # ---- a failed acknowledgment arms nothing ------------------------------
    # No worker: `python3` is replaced by a stand-in that exits 3.
    mkdir -p "$sx/ackfail/bin"
    printf '#!/bin/sh\nexit 3\n' > "$sx/ackfail/bin/python3"
    chmod +x "$sx/ackfail/bin/python3"
    rm -f "$sx/ackfail/armed" "$sx/ackfail/inv.txt" "$sx/ackfail/ledger"
    ( PATH="$sx/ackfail/bin:$PATH"; export PATH
      MOQ5_PUBBIND_ACK_LEDGER="$sx/ackfail/ledger" \
      MOQ5_PUBBIND_ARMED="$sx/ackfail/armed" \
      MOQ5_PUBBIND_MARKER="$sx/ackfail/marker" \
      MOQ5_PUBBIND_INVENTORY_APPEND="$sx/ackfail/inv.txt" \
      /bin/sh "$sx/two-groups.sh" ) > "$sx/ackfail/out" 2>&1
    _afrc=$?
    ack_cleanup_of "$sx/ackfail/ledger" "selftest.fixture-ack-failure-arms-nothing" \
        "the stand-in helper's fixture"
    if [ "$_afrc" -ne 0 ] && [ ! -f "$sx/ackfail/armed" ] && [ ! -s "$sx/ackfail/inv.txt" ]; then
        pass "selftest.fixture-ack-failure-arms-nothing" "a failed acknowledgment exits $_afrc, records no session and arms nothing"
    else
        fail "selftest.fixture-ack-failure-arms-nothing" "exit=$_afrc armed=$([ -f "$sx/ackfail/armed" ] && echo yes || echo no) recorded='$(cat "$sx/ackfail/inv.txt" 2>/dev/null)'"
    fi

    # ---- and it really reaps the exact worker it created -------------------
    # The oracle is bound to the child the helper names in its OWN receipts: a
    # child must have been created, the failed-acknowledgment path reached, that
    # exact child reaped, and the group it would lead observed empty by the
    # checked observer. Nothing is concluded from a global command-name scan or
    # a process count, and nothing found by such a scan is ever signalled.
    #

    # One bounded REAL owned-child failed acknowledgment, run once.
    rm -rf "$sx/ackreal"; mkdir -p "$sx/ackreal"
    ( MOQ5_PUBBIND_ACK_FAIL=1 \
      MOQ5_PUBBIND_ACK_RECEIPTS="$sx/ackreal/receipts" \
      MOQ5_PUBBIND_ACK_LEDGER="$sx/ackreal/ledger" \
      MOQ5_PUBBIND_ARMED="$sx/ackreal/armed" \
      MOQ5_PUBBIND_MARKER="$sx/ackreal/marker" \
      MOQ5_PUBBIND_INVENTORY_APPEND="$sx/ackreal/inv.txt" \
      /bin/sh "$sx/two-groups.sh" ) > "$sx/ackreal/out" 2>&1
    _arrc=$?
    _arwhy=$(ack_verdict_of "$sx/ackreal/receipts" "$_arrc" "$sx/ackreal/armed" "$sx/ackreal/inv.txt")
    if [ -z "$_arwhy" ]; then
        pass "selftest.fixture-ack-failure-reaps-its-child" "a real failed acknowledgment reaped the exact worker it created and left its group empty: $(tr '\n' ' ' < "$sx/ackreal/receipts")"
    else
        fail "selftest.fixture-ack-failure-reaps-its-child" "$_arwhy (caller exit $_arrc)"
    fi
    if [ -n "${MOQ5_PUBBIND_ACK_STOP_BEFORE_CLEANUP-}" ]; then
        # A forced stop at the moment the fixture returns, so a control outside
        # this run can ask whether the child it created is still accounted for
        # when this run's own cleanup never executes.
        printf 'STOPPED BEFORE ACK CLEANUP\n'
        printf '\nharness controls: %d failure(s)\n' "$fails"
        printf 'evidence: %s\n' "$evidence"
        exit 1
    fi
    # Whatever the verdict says, the cleanup handoff happens here, once, from
    # the ledger rather than from the receipts under test -- so a fixture that
    # misreports its own cleanup cannot also leave a worker behind.
    ack_cleanup_of "$sx/ackreal/ledger" \
        "selftest.fixture-ack-failure-reaps-its-child" \
        "the worker the failed-acknowledgment fixture created"

    # ---- a worker the RETAINED OWNER created, settled through it ----------
    # The whole chain in one executable path: this run holds only the alias its
    # launcher inherited to it; the fixture creates its worker through that
    # owner before the worker exists; the create reply reaches the canonical
    # writer; and the ACTUAL cleanup caller is what settles it, by identity,
    # through the owner. Nothing here signals a number, and no file is read as
    # proof of retirement. Without an inherited channel the path cannot be
    # executed, and that is reported rather than passed.
    if owner_connected; then
        rm -rf "$sx/ackowned"; mkdir -p "$sx/ackowned"
        _owled="$sx/ackowned/ledger"
        ( MOQ5_PUBBIND_ACK_RECEIPTS="$sx/ackowned/receipts" \
          MOQ5_PUBBIND_ACK_LEDGER="$_owled" \
          MOQ5_PUBBIND_ARMED="$sx/ackowned/armed" \
          MOQ5_PUBBIND_MARKER="$sx/ackowned/marker" \
          MOQ5_PUBBIND_INVENTORY_APPEND="$sx/ackowned/inv.txt" \
          MOQ5_PUBBIND_OWNER_REPLY="$sx/ackowned/reply" \
          MOQ5_PUBBIND_OWNER_SCOPE=ack-owned \
          MOQ5_PUBBIND_OWNER_KEY="ack-owned-$$" \
          /bin/sh "$sx/two-groups.sh" ) > "$sx/ackowned/out" 2>&1
        _owrc=$?
        if [ "$_owrc" -ne 0 ]; then
            fail "selftest.owner-managed-child-is-settled" \
                "the fixture could not create a worker through the retained owner (exit $_owrc): $(tr '\n' ' ' < "$sx/ackowned/out")"
        else
            _owid=$(record_owner_reply "$_owled" "$sx/ackowned/reply" \
                "selftest.owner-managed-child-is-settled" \
                "the worker the retained owner created")
            _owr=$(printf '%s' "$_owid" | cut -d" " -f3)
            _owc=$(printf '%s' "$_owid" | cut -d" " -f4)
            ack_cleanup_of "$_owled" "selftest.owner-managed-child-is-settled" \
                "the worker the retained owner created"
            # The owner itself is asked; the ledger and the process table are
            # observations, never the authority for this answer.
            _owstate=$(owner_call state "$_owr")
            case "$_owstate" in
                *'"state": "retired"'*)
                    pass "selftest.owner-managed-child-is-settled" \
                        "the retained owner reports $_owr retired after the actual cleanup, and its worker $_owc is $(harness_proc_live "$_owc")" ;;
                *)
                    fail "selftest.owner-managed-child-is-settled" \
                        "the owner does not report $_owr retired: $_owstate" ;;
            esac
        fi
    else
        printf 'SKIP[selftest.owner-managed-child-is-settled]: no owner channel was inherited; the managed path did not execute\n'
    fi

    # A helper that never created a child cannot have reaped one.
    rm -rf "$sx/acknochild"; mkdir -p "$sx/acknochild/bin"
    printf '#!/bin/sh\nexit 3\n' > "$sx/acknochild/bin/python3"
    chmod +x "$sx/acknochild/bin/python3"
    ( PATH="$sx/acknochild/bin:$PATH"; export PATH
      MOQ5_PUBBIND_ACK_RECEIPTS="$sx/acknochild/receipts" \
      MOQ5_PUBBIND_ACK_LEDGER="$sx/acknochild/ledger" \
      MOQ5_PUBBIND_ARMED="$sx/acknochild/armed" \
      MOQ5_PUBBIND_MARKER="$sx/acknochild/marker" \
      MOQ5_PUBBIND_INVENTORY_APPEND="$sx/acknochild/inv.txt" \
      /bin/sh "$sx/two-groups.sh" ) > "$sx/acknochild/out" 2>&1
    _ancrc=$?
    _ancwhy=$(ack_verdict_of "$sx/acknochild/receipts" "$_ancrc" "$sx/acknochild/armed" "$sx/acknochild/inv.txt")
    [ -n "$_ancwhy" ] \
        && pass "selftest.fixture-ack-no-child-is-refused" "refused by name: $_ancwhy" \
        || fail "selftest.fixture-ack-no-child-is-refused" "an absent child was read as a completed reap"
    ack_cleanup_of "$sx/acknochild/ledger" "selftest.fixture-ack-no-child-is-refused" \
        "a helper that created nothing"

    # The same receipts, read through an observer that cannot answer: an
    # inspection that cannot be made is not proof that the child is gone.
    _apsaved=${MOQ5_PUBBIND_HARNESS_PS-}
    MOQ5_PUBBIND_HARNESS_PS="$sx/ps-fail"
    _ablwhy=$(ack_verdict_of "$sx/ackreal/receipts" "$_arrc" "$sx/ackreal/armed" "$sx/ackreal/inv.txt")
    # And through one that reports an extra live, session-leading process that
    # this driver does not own: the exact-owned result must not move.
    MOQ5_PUBBIND_HARNESS_PS="$sx/ps-plus-unrelated"
    _aunwhy=$(ack_verdict_of "$sx/ackreal/receipts" "$_arrc" "$sx/ackreal/armed" "$sx/ackreal/inv.txt")
    if [ -z "$_apsaved" ]; then unset MOQ5_PUBBIND_HARNESS_PS; else MOQ5_PUBBIND_HARNESS_PS=$_apsaved; fi
    [ -n "$_ablwhy" ] \
        && pass "selftest.fixture-ack-blind-observer-is-refused" "refused by name: $_ablwhy" \
        || fail "selftest.fixture-ack-blind-observer-is-refused" "an unanswerable inspection was read as an absence"
    [ -z "$_aunwhy" ] \
        && pass "selftest.fixture-ack-unrelated-process-is-ignored" "an unrelated live session leader does not change the exact-owned result" \
        || fail "selftest.fixture-ack-unrelated-process-is-ignored" "an unrelated process changed the verdict: $_aunwhy"

    # The oracle's own record boundaries, on synthetic receipts. The identities
    # here name nothing on this host, nothing is created and nothing is
    # signalled: only the verdict is read.
    mkdir -p "$sx/ackrec"
    printf 'child=2147483646\nack=failed child=2147483646\nwait=reaped child=2147483645 status=0\noutcome=reaped child=2147483645\nhelper=3\n' \
        > "$sx/ackrec/mismatched.txt"
    printf 'child=2147483646\nack=failed child=2147483646\nnot-a-receipt\nwait=reaped child=2147483646 status=0\noutcome=reaped child=2147483646\nhelper=3\n' \
        > "$sx/ackrec/foreign.txt"
    for _ar in mismatched:"a reap receipt that names another process" \
               foreign:"an unrecognised receipt record" \
               absent:"receipts that cannot be read"; do
        _ark=${_ar%%:*}; _ard=${_ar#*:}
        _arw=$(ack_verdict_of "$sx/ackrec/$_ark.txt" 70 "$sx/ackrec/no-armed" "$sx/ackrec/no-inventory")
        [ -n "$_arw" ] \
            && pass "selftest.fixture-ack-receipt-$_ark" "$_ard is refused by name: $_arw" \
            || fail "selftest.fixture-ack-receipt-$_ark" "$_ard was read as a completed reap"
    done

    # The receipt contract itself, on synthetic histories. Nothing is created
    # and nothing is signalled: only the parser's verdict is read. Each
    # contradictory history carries an otherwise complete, coherent record set,
    # so a parser that dropped or overwrote the offending record would accept it.
    receipt_case() {   # receipt_case <name> <records> <expect: ok|reason>
        mkdir -p "$sx/ackrec"
        printf '%b' "$2" > "$sx/ackrec/rc-$1.txt"
        ack_receipts_parse "$sx/ackrec/rc-$1.txt"; _rc_s=$?
        if [ "$_rc_s" -ne 0 ]; then
            fail "selftest.receipt-$1" "the parser ended with status $_rc_s"
        elif [ "$3" = ok ]; then
            [ -z "$_ar_why" ] \
                && pass "selftest.receipt-$1" "a coherent history is accepted" \
                || fail "selftest.receipt-$1" "unexpectedly refused: $_ar_why"
        else
            [ -n "$_ar_why" ] \
                && pass "selftest.receipt-$1" "refused by name: $_ar_why" \
                || fail "selftest.receipt-$1" "a contradictory history was accepted"
        fi
    }
    _rc_ok='child=4242\nack=failed child=4242\nsignal=TERM sent\nsignal=KILL sent\nwait=reaped child=4242 status=9\noutcome=reaped child=4242\nhelper=3\n'
    receipt_case reaped-history   "$_rc_ok" ok
    receipt_case term-only        'child=4242\nack=failed child=4242\nsignal=TERM sent\nwait=reaped child=4242 status=15\noutcome=reaped child=4242\nhelper=3\n' ok
    receipt_case unresolved-history 'child=4242\nack=failed child=4242\nsignal=TERM sent\nsignal=KILL sent\noutcome=unresolved reason=bounds-exhausted child=4242\nhelper=4\n' ok
    receipt_case acknowledged     'child=4242\nack=ok group=4242\nhelper=0\n' ok
    receipt_case no-child         'helper=3\n' ok
    receipt_case wrong-ack-child  'child=4242\nack=failed child=4343\nsignal=TERM sent\nsignal=KILL sent\nwait=reaped child=4242 status=9\noutcome=reaped child=4242\nhelper=3\n' reason
    receipt_case wrong-outcome-child 'child=4242\nack=failed child=4242\nsignal=TERM sent\nsignal=KILL sent\nwait=reaped child=4242 status=9\noutcome=reaped child=4343\nhelper=3\n' reason
    receipt_case wait-no-status   'child=4242\nack=failed child=4242\nsignal=TERM sent\nwait=reaped child=4242\noutcome=reaped child=4242\nhelper=3\n' reason
    receipt_case wait-bad-status  'child=4242\nack=failed child=4242\nsignal=TERM sent\nwait=reaped child=4242 status=nine\noutcome=reaped child=4242\nhelper=3\n' reason
    receipt_case unresolved-then-reaped 'child=4242\nack=failed child=4242\nsignal=TERM sent\nwait=reaped child=4242 status=9\noutcome=unresolved reason=bounds-exhausted child=4242\noutcome=reaped child=4242\nhelper=3\n' reason
    receipt_case unresolved-claims-wait 'child=4242\nack=failed child=4242\nsignal=TERM sent\nwait=reaped child=4242 status=9\noutcome=unresolved reason=bounds-exhausted child=4242\nhelper=4\n' reason
    receipt_case duplicate-wait   'child=4242\nack=failed child=4242\nsignal=TERM sent\nwait=reaped child=4242 status=9\nwait=reaped child=4242 status=9\noutcome=reaped child=4242\nhelper=3\n' reason
    receipt_case duplicate-signal 'child=4242\nack=failed child=4242\nsignal=TERM sent\nsignal=TERM sent\nwait=reaped child=4242 status=9\noutcome=reaped child=4242\nhelper=3\n' reason
    receipt_case signal-out-of-order 'child=4242\nack=failed child=4242\nsignal=KILL sent\nsignal=TERM sent\nwait=reaped child=4242 status=9\noutcome=reaped child=4242\nhelper=3\n' reason
    receipt_case foreign-signal   'child=4242\nack=failed child=4242\nsignal=TERM whatever\nwait=reaped child=4242 status=9\noutcome=reaped child=4242\nhelper=3\n' reason
    receipt_case zero-child       'child=0\nack=failed child=0\nsignal=TERM sent\nwait=reaped child=0 status=9\noutcome=reaped child=0\nhelper=3\n' reason
    receipt_case init-child       'child=1\nack=failed child=1\nsignal=TERM sent\nwait=reaped child=1 status=9\noutcome=reaped child=1\nhelper=3\n' reason
    receipt_case out-of-order     'child=4242\nack=failed child=4242\nhelper=3\nsignal=TERM sent\nwait=reaped child=4242 status=9\noutcome=reaped child=4242\n' reason
    receipt_case no-helper        'child=4242\nack=failed child=4242\nsignal=TERM sent\nwait=reaped child=4242 status=9\noutcome=reaped child=4242\n' reason
    receipt_case wrong-helper     'child=4242\nack=failed child=4242\nsignal=TERM sent\nwait=reaped child=4242 status=9\noutcome=reaped child=4242\nhelper=4\n' reason
    receipt_case orphan-outcome   'ack=failed child=4242\noutcome=reaped child=4242\nhelper=3\n' reason

    # ---- the cleanup authority, with the dispatch replaced by a recorder ----
    # Every case runs in an isolated child, because an authority that cannot be
    # established must STOP the harness where it stands. No signal is sent: the
    # dispatch records what it WOULD have settled.
    cleanup_case() {   # cleanup_case <name> <ledger lines|""> <expect: refuse|""|pid> [ps] [reader]
        _cl_n=$1
        rm -rf "$sx/acl-$_cl_n"; mkdir -p "$sx/acl-$_cl_n"
        [ -z "$2" ] || printf '%b' "$2" > "$sx/acl-$_cl_n/ledger"
        # An empty recorder, so a case that settles nothing reads as nothing
        # rather than as a missing file the shell would complain about.
        : > "$sx/aclrec-$_cl_n"
        env MOQ5_PUBBIND_SETTLE_RECORDER="$sx/aclrec-$_cl_n" \
            MOQ5_PUBBIND_ACK_CLEANUP_PROBE="$sx/acl-$_cl_n/ledger" \
            MOQ5_PUBBIND_HARNESS_PS="${4:-/bin/ps}" \
            MOQ5_PUBBIND_ALIVE_PID=2147483646 \
            MOQ5_PUBBIND_READER="${5:-cat}" \
            /bin/sh "$driver" --selftest --harness --section ackcleanup \
            --evidence "$sx/acl-$_cl_n/run" > "$sx/acllog-$_cl_n" 2>&1
        _cl_rc=$?
        _cl_got=$(tr '\n' ' ' < "$sx/aclrec-$_cl_n")
        case "$3" in
            refuse)
                if [ "$_cl_rc" -ne 0 ] && grep -q 'HARNESS ABORTED' "$sx/acllog-$_cl_n" \
                   && [ -z "$_cl_got" ]; then
                    pass "selftest.ack-cleanup-$_cl_n" "an authority that cannot be established stops the run and settles nothing: $(grep -m1 'HARNESS ABORTED' "$sx/acllog-$_cl_n")"
                else
                    fail "selftest.ack-cleanup-$_cl_n" "exit=$_cl_rc aborted=$(grep -c 'HARNESS ABORTED' "$sx/acllog-$_cl_n") settled='$_cl_got'"
                fi ;;
            "")
                if [ "$_cl_rc" -eq 0 ] && [ -z "$_cl_got" ]; then
                    pass "selftest.ack-cleanup-$_cl_n" "nothing is settled and the run continues"
                else
                    fail "selftest.ack-cleanup-$_cl_n" "exit=$_cl_rc settled='$_cl_got'"
                fi ;;
            *)
                if [ "$_cl_rc" -eq 0 ] && [ "$_cl_got" = "$3 " ]; then
                    pass "selftest.ack-cleanup-$_cl_n" "exactly $3 is settled"
                else
                    fail "selftest.ack-cleanup-$_cl_n" "exit=$_cl_rc settled='$_cl_got' wanted='$3 '"
                fi ;;
        esac
    }
    _cl_hdr='ledger=moq5-pubbind-ack-probe\n'
    cleanup_case missing-ledger    ''                                             refuse
    cleanup_case unreadable-ledger "$_cl_hdr"'child=2147483646\nhelper=4\n'        refuse /bin/ps "$sx/reader-refuses"
    cleanup_case partial-ledger    "$_cl_hdr"'child=2147483646\nhelper=4\n'        refuse /bin/ps "$sx/reader-partial"
    cleanup_case no-header         'child=2147483646\nhelper=4\n'                  refuse
    cleanup_case malformed-child   "$_cl_hdr"'child=4242junk\nhelper=4\n'          refuse
    cleanup_case zero-selector     "$_cl_hdr"'child=0\nhelper=4\n'                 refuse
    cleanup_case init-selector     "$_cl_hdr"'child=1\nhelper=4\n'                 refuse
    cleanup_case two-children      "$_cl_hdr"'child=2147483646\nchild=2147483645\nhelper=4\n' refuse
    cleanup_case no-status         "$_cl_hdr"'child=2147483646\n'                  refuse
    cleanup_case foreign-record    "$_cl_hdr"'child=2147483646\nstray\nhelper=4\n' refuse
    cleanup_case known-no-child    "$_cl_hdr"'helper=3\n'                          ""
    cleanup_case gone-child        "$_cl_hdr"'child=2147483646\nhelper=3\n'        ""
    cleanup_case live-child        "$_cl_hdr"'child=2147483646\nhelper=4\n'        2147483646 "$sx/ps-says-alive"

    # ---- a handoff that cannot be recorded stands the child down -----------
    # A real fork whose identity cannot be handed to the cleanup authority. The
    # child is gated on that handoff, so it never becomes a worker: it stands
    # down on EOF and is reaped here. The helper names it on stderr, so the
    # control can check that exact identity rather than scanning for a command.
    rm -rf "$sx/ackhandoff"; mkdir -p "$sx/ackhandoff"
    ( MOQ5_PUBBIND_ACK_HANDOFF_FAIL=1 \
      MOQ5_PUBBIND_ACK_RECEIPTS="$sx/ackhandoff/receipts" \
      MOQ5_PUBBIND_ACK_LEDGER="$sx/ackhandoff/ledger" \
      MOQ5_PUBBIND_ARMED="$sx/ackhandoff/armed" \
      MOQ5_PUBBIND_MARKER="$sx/ackhandoff/marker" \
      MOQ5_PUBBIND_INVENTORY_APPEND="$sx/ackhandoff/inv.txt" \
      /bin/sh "$sx/two-groups.sh" ) > "$sx/ackhandoff/out" 2>&1
    _ahrc=$?
    _ahpid=$(sed -n 's/^ack handoff failed for child \([0-9][0-9]*\) .*/\1/p' "$sx/ackhandoff/out" | head -1)
    if [ -n "$_ahpid" ]; then _ahlive=$(harness_proc_live "$_ahpid"); else _ahlive=unknown; fi
    if [ "$_ahrc" -ne 0 ] && [ -n "$_ahpid" ] && [ "$_ahlive" = gone ] \
       && [ ! -f "$sx/ackhandoff/armed" ] && [ ! -s "$sx/ackhandoff/inv.txt" ] \
       && ! grep -q '^child=' "$sx/ackhandoff/ledger"; then
        pass "selftest.fixture-ack-handoff-failure-stands-down" "a child whose identity could not be recorded never became a worker and is gone ($_ahpid), and nothing was armed or recorded"
    else
        fail "selftest.fixture-ack-handoff-failure-stands-down" "exit=$_ahrc child='$_ahpid' observed=$_ahlive armed=$([ -f "$sx/ackhandoff/armed" ] && echo yes || echo no) ledger='$(tr '\n' ' ' < "$sx/ackhandoff/ledger" 2>/dev/null)'"
    fi
    # The identity is settled here whatever the verdict, so a child that did NOT
    # stand down cannot be left behind by this control.
    if [ -n "$_ahpid" ]; then
        settle_named_process "$_ahpid" "selftest.fixture-ack-handoff-failure-stands-down" \
            "a child whose ownership handoff failed"
    fi

    # ---- an interrupted run still accounts for the child it created --------
    # One bounded real owned child: the child run's helper skips its own cleanup
    # and the run is forced to stop before its cleanup call, so the worker is
    # alive and its own driver never settles it. The ledger it wrote at fork time
    # is what makes that worker recoverable from outside, and this control then
    # settles it by that exact identity and proves it gone.
    run_section ackfixture "$sx/ackstop.log" \
        MOQ5_PUBBIND_ACK_SKIP_CLEANUP=1 MOQ5_PUBBIND_ACK_STOP_BEFORE_CLEANUP=1
    _asrc=$?
    _asled="$sx/ev-ackstop/fixtures/ackreal/ledger"
    _aspid=$(ack_ledger_read "$_asled"); _asst=$?
    case "$_asst:$_aspid" in
        0:none|0:refused:*|0:) _aswhy="the interrupted run left no usable cleanup authority: ${_aspid:-nothing}" ;;
        0:*) _aswhy="" ;;
        *)   _aswhy="the ledger reader ended with status $_asst" ;;
    esac
    if [ -z "$_aswhy" ]; then
        _asalive=$(harness_proc_live "$_aspid")
    else
        _asalive=unknown
    fi
    if [ "$_asrc" -ne 0 ] && [ -z "$_aswhy" ] \
       && grep -q 'STOPPED BEFORE ACK CLEANUP' "$sx/ackstop.log" \
       && [ "$_asalive" = alive ]; then
        pass "selftest.fixture-ack-interrupted-run-is-accounted-for" "the run stopped before its own cleanup, and the child $_aspid it recorded at fork time is still accounted for and observably alive"
    else
        fail "selftest.fixture-ack-interrupted-run-is-accounted-for" "exit=$_asrc ledger='$_aspid' observed=$_asalive stopped=$(grep -c 'STOPPED BEFORE ACK CLEANUP' "$sx/ackstop.log") ${_aswhy}"
    fi
    # The recovery this control exists to prove: the same ledger settles it.
    ack_cleanup_of "$_asled" "selftest.fixture-ack-interrupted-run-is-accounted-for" \
        "the worker an interrupted run left behind"
    if [ -n "$_aspid" ] && [ "$_aspid" != none ]; then
        case "$_aspid" in
            'refused: '*) : ;;
            *) [ "$(harness_proc_live "$_aspid")" = gone ] \
                   && pass "selftest.fixture-ack-interrupted-run-is-recovered" "the recovered worker $_aspid is observed gone" \
                   || fail "selftest.fixture-ack-interrupted-run-is-recovered" "the recovered worker $_aspid is $(harness_proc_live "$_aspid")" ;;
        esac
    else
        fail "selftest.fixture-ack-interrupted-run-is-recovered" "no identity to recover"
    fi

    # The helper's exceptional cleanup paths, exercised as mechanism probes:
    # no fork, no signal and no wait reaches the operating system. Each must
    # name an unresolved outcome, keep the retained identity, claim no reap,
    # and still arm and record nothing.
    for _af in signal-error:signal-TERM-failed wait-error:wait-failed bounds:bounds-exhausted; do
        _afk=${_af%%:*}; _afr=${_af#*:}
        rm -rf "$sx/ackfault-$_afk"; mkdir -p "$sx/ackfault-$_afk"
        ( MOQ5_PUBBIND_ACK_FAULT="$_afk" \
          MOQ5_PUBBIND_ACK_RECEIPTS="$sx/ackfault-$_afk/receipts" \
          MOQ5_PUBBIND_ACK_LEDGER="$sx/ackfault-$_afk/ledger" \
          MOQ5_PUBBIND_ARMED="$sx/ackfault-$_afk/armed" \
          MOQ5_PUBBIND_MARKER="$sx/ackfault-$_afk/marker" \
          MOQ5_PUBBIND_INVENTORY_APPEND="$sx/ackfault-$_afk/inv.txt" \
          /bin/sh "$sx/two-groups.sh" ) > "$sx/ackfault-$_afk/out" 2>&1
        _afrc=$?
        _afwhy=$(ack_verdict_of "$sx/ackfault-$_afk/receipts" "$_afrc" \
                    "$sx/ackfault-$_afk/armed" "$sx/ackfault-$_afk/inv.txt")
        if [ "$_afrc" -ne 0 ] \
           && grep -q "^outcome=unresolved reason=$_afr .*child=2147483646\$" "$sx/ackfault-$_afk/receipts" \
           && ! grep -q '^outcome=reaped' "$sx/ackfault-$_afk/receipts" \
           && grep -q '^helper=4$' "$sx/ackfault-$_afk/receipts" \
           && [ ! -f "$sx/ackfault-$_afk/armed" ] && [ ! -s "$sx/ackfault-$_afk/inv.txt" ] \
           && [ -n "$_afwhy" ]; then
            pass "selftest.fixture-ack-$_afk-is-unresolved" "an unestablished cleanup outcome is named unresolved, keeps its identity and stops the caller: $(grep -m1 '^outcome=' "$sx/ackfault-$_afk/receipts")"
        else
            fail "selftest.fixture-ack-$_afk-is-unresolved" "exit=$_afrc receipts='$(tr '\n' ' ' < "$sx/ackfault-$_afk/receipts" 2>/dev/null)' oracle='$_afwhy'"
        fi
        # A probe that forked nothing owns nothing: its ledger must record no
        # child, so its synthetic identity can never reach a signal.
        ack_cleanup_of "$sx/ackfault-$_afk/ledger" "selftest.fixture-ack-$_afk-is-unresolved" \
            "a mechanism probe that created no worker"
    done

    fi

    if want settlement; then
    # ---- no malformed identity reaches the signalling layer ---------------
    # The dispatch is replaced by a recorder, so what WOULD have been signalled
    # is observed without any signal being sent. No `kill` is invoked here.
    parser_case() {   # parser_case <name> <record> <expect: record|refuse>
        _pc_n=$1; _pc_rec=$2; _pc_want=$3
        rm -rf "$sx/pev-$_pc_n"; mkdir -p "$sx/pev-$_pc_n"
        # The synthetic inventory carries this driver's ownership marker: these
        # cases are about the RECORD grammar, and an inventory without the
        # marker is refused for attestation instead, which its own control
        # pins.
        printf 'owned-by=moq5-pubbind-driver\n%s\n' "$_pc_rec" \
            > "$sx/pev-$_pc_n/ownership.txt"
        rm -f "$sx/prec-$_pc_n"
        env MOQ5_PUBBIND_CLEANUP_RECORDER="$sx/prec-$_pc_n" \
            MOQ5_PUBBIND_PARSER_PROBE="$sx/pev-$_pc_n" \
            /bin/sh "$driver" --selftest --harness --section parser \
            --evidence "$sx/pev-$_pc_n/run" > "$sx/plog-$_pc_n" 2>&1
        _pc_rc=$?
        _pc_dispatched=$(cat "$sx/prec-$_pc_n" 2>/dev/null | tr '\n' ' ')
        case "$_pc_want" in
            "")
                if [ "$_pc_rc" -eq 0 ] && [ -z "$_pc_dispatched" ]; then
                    pass "selftest.parser-$_pc_n" "'$_pc_rec' is accepted metadata and dispatches nothing"
                else
                    fail "selftest.parser-$_pc_n" "exit=$_pc_rc dispatched='$_pc_dispatched'"
                fi ;;
            refuse)
                if [ "$_pc_rc" -ne 0 ] && grep -q 'HARNESS ABORTED' "$sx/plog-$_pc_n" \
                   && [ -z "$_pc_dispatched" ]; then
                    pass "selftest.parser-$_pc_n" "'$_pc_rec' is refused by name and nothing is dispatched"
                else
                    fail "selftest.parser-$_pc_n" "exit=$_pc_rc dispatched='$_pc_dispatched' aborted=$(grep -c 'HARNESS ABORTED' "$sx/plog-$_pc_n")"
                fi ;;
            *)
                if [ "$_pc_rc" -eq 0 ] && [ "$_pc_dispatched" = "$_pc_want " ]; then
                    pass "selftest.parser-$_pc_n" "'$_pc_rec' dispatches exactly $_pc_want"
                else
                    fail "selftest.parser-$_pc_n" "exit=$_pc_rc dispatched='$_pc_dispatched' wanted='$_pc_want '"
                fi ;;
        esac
    }
    parser_case malformed-suffix 'session=123junk observed=live' refuse
    parser_case zero-selector    'session=0 observed=live'       refuse
    parser_case init-selector    'session=1 observed=live'       refuse
    parser_case leading-zero     'session=0123 observed=live'    refuse
    parser_case empty-token      'session= observed=live'        refuse
    parser_case unknown-record   'sessionish=99 observed=live'   refuse
    parser_case valid-record     'session=4242 observed=live'    4242
    parser_case metadata-kept    'java_home=/x major=21'         ''

    # A complete inventory that records nothing is the legitimate no-work case,
    # and it is not the same fact as a missing or unreadable one.
    mkdir -p "$sx/ev-empty-inventory"
    : > "$sx/ev-empty-inventory/ownership.txt"
    settle_recorded_groups "$sx/ev-empty-inventory" "selftest.ownership-inventory-empty-is-not-unreadable" \
        "a run that owned nothing"
    if inventory_read "$sx/ev-empty-inventory/ownership.txt" "$sx/inv-empty.out" \
       && ! inventory_read "$sx/ev-empty-inventory/absent.txt" "$sx/inv-missing.out"; then
        pass "selftest.ownership-inventory-empty-is-not-unreadable" "a complete empty inventory settles nothing and continues, while a missing one is refused"
    else
        fail "selftest.ownership-inventory-empty-is-not-unreadable" "the empty and missing inventories are not distinguished"
    fi

    fi

    if want rest; then
    # ---- F1: every exit reports a failed restore and keeps the backup -----
    cp "$root/jni/moq5_registry.h" "$sx/subject-abort.h"
    env MOQ5_PUBBIND_FAULT=abort-after-perturb \
        MOQ5_PUBBIND_STUB_PUBLISH="$sx" MOQ5_PUBBIND_STUB_AUDIT=1 MOQ5_PUBBIND_STUB_RECEIPTS=1 MOQ5_PUBBIND_TIMEOUT=300 MOQ5_PUBBIND_LOCK_ROOT="$sx/locks" \
        /bin/sh "$driver" --harness --subject "$sx/subject-abort.h" --evidence "$sx/ev-abort" \
        > "$sx/abort.log" 2>&1
    _st=$?
    # Both digests are taken in checked assignment form BEFORE the decision, so
    # a hasher that failed cannot leave two empty strings comparing equal.
    _abort_now=$(decl_sha256 "$sx/subject-abort.h") \
        || abort_harness "selftest.abort-restores" "the aborted subject" \
               "its digest could not be produced"
    _abort_want=$(decl_sha256 "$root/jni/moq5_registry.h") \
        || abort_harness "selftest.abort-restores" "the pristine registry header" \
               "its digest could not be produced"
    if [ "$_st" -eq 3 ] && grep -q 'PASS\[pubbind.perturb\]' "$sx/abort.log" \
       && grep -q 'PASS\[pubbind.restore\]' "$sx/abort.log" \
       && ! grep -q 'publication source binding:' "$sx/abort.log" \
       && [ "$_abort_now" = "$_abort_want" ]; then
        pass "selftest.abort-restores" "an abrupt exit restores through the trap, reports it, and keeps its own status ($_st)"
    else
        fail "selftest.abort-restores" "exit=$_st (expected 3); restoration not reported or not performed"
    fi

    cp "$root/jni/moq5_registry.h" "$sx/subject-lost.h"
    env MOQ5_PUBBIND_FAULT=abort-after-perturb-lost-backup \
        MOQ5_PUBBIND_STUB_PUBLISH="$sx" MOQ5_PUBBIND_STUB_AUDIT=1 MOQ5_PUBBIND_STUB_RECEIPTS=1 MOQ5_PUBBIND_TIMEOUT=300 MOQ5_PUBBIND_LOCK_ROOT="$sx/locks" \
        /bin/sh "$driver" --harness --subject "$sx/subject-lost.h" --evidence "$sx/ev-lost" \
        > "$sx/lost.log" 2>&1
    _st=$?
    if [ "$_st" -ne 0 ] && grep -q 'PASS\[pubbind.perturb\]' "$sx/lost.log" \
       && grep -q 'FAIL\[pubbind.restore\]' "$sx/lost.log" \
       && ! grep -q 'publication source binding:' "$sx/lost.log"; then
        pass "selftest.abort-reports-failed-restore" "a failed restoration turns an otherwise-successful exit non-zero ($_st) instead of being discarded"
    else
        fail "selftest.abort-reports-failed-restore" "exit=$_st; a failed restoration was not reported: $(tail -2 "$sx/lost.log" | tr '\n' ' ')"
    fi

    # A cleanup that cannot be OBSERVED must stop the run: no restoration claim,
    # no false success, the backup retained, and nothing published. The real
    # topology audit runs here, so there is a genuine owned group to lose sight
    # of; only the observation is broken.
    cp "$root/jni/moq5_registry.h" "$sx/subject-blind.h"
    _blind_before=$(decl_sha256 "$sx/subject-blind.h") \
        || abort_harness "selftest.unobservable-cleanup" "the blinded subject" \
               "its digest could not be produced before the run"
    rm -f "$sx/invocations"; : > "$sx/invocations"
    env MOQ5_PUBBIND_PS="$sx/ps-blind" \
        MOQ5_PUBBIND_STUB_PUBLISH="$sx" MOQ5_PUBBIND_STUB_RECEIPTS=1 MOQ5_PUBBIND_TIMEOUT=300 \
        MOQ5_PUBBIND_LOCK_ROOT="$sx/locks" \
        /bin/sh "$driver" --harness --subject "$sx/subject-blind.h" --evidence "$sx/ev-blind" \
        > "$sx/blind.log" 2>&1
    _st=$?
    _n=$(awk 'END { print NR + 0 }' "$sx/invocations")
    _blind_after=$(decl_sha256 "$sx/subject-blind.h") \
        || abort_harness "selftest.unobservable-cleanup" "the blinded subject" \
               "its digest could not be produced after the run"
    if [ "$_st" -ne 0 ] && grep -q 'FAIL\[pubbind.ownership\]' "$sx/blind.log" \
       && grep -q 'FAIL\[pubbind.restore\]' "$sx/blind.log" \
       && ! grep -q 'PASS\[pubbind.restore\]' "$sx/blind.log" \
       && [ "$_n" -eq 0 ] && [ -f "$sx/ev-blind/subject.orig" ] \
       && [ "$_blind_after" = "$_blind_before" ]; then
        pass "selftest.unobservable-cleanup" "an unobservable cleanup stops the run by name, refuses to claim a restoration, keeps the backup and publishes nothing (exit $_st)"
    else
        fail "selftest.unobservable-cleanup" "exit=$_st ownership=$(grep -c 'FAIL\[pubbind.ownership\]' "$sx/blind.log") restore-claimed=$(grep -c 'PASS\[pubbind.restore\]' "$sx/blind.log") publications=$_n backup=$([ -f "$sx/ev-blind/subject.orig" ] && echo kept || echo GONE)"
    fi
    _al=$(sed -n 's/^session=\([0-9]*\).*/\1/p' "$sx/ev-blind/ownership.txt" 2>/dev/null | tail -1)
    typed_dispatch "$(route_identity "$sx/ev-blind" "$_al")" "$_al" \
        "selftest.unobservable-cleanup" "the blinded driver's audit worker" cleanup_or_abort "$sx/ev-blind"

    # An audit that exits ZERO while carrying a diagnostic must stop the run
    # before anything is published.
    cp "$root/jni/moq5_registry.h" "$sx/subject-diag.h"
    rm -f "$sx/invocations"; : > "$sx/invocations"
    env MOQ5_PUBBIND_STUB_PUBLISH="$sx" MOQ5_PUBBIND_STUB_AUDIT=diagnostic MOQ5_PUBBIND_STUB_RECEIPTS=1 \
        MOQ5_PUBBIND_TIMEOUT=300        /bin/sh "$driver" --harness --subject "$sx/subject-diag.h" --evidence "$sx/ev-diag" \
        > "$sx/diag.log" 2>&1
    _st=$?
    _n=$(awk 'END { print NR + 0 }' "$sx/invocations")
    if [ "$_st" -ne 0 ] && grep -q 'FAIL\[pubbind.baseline\]' "$sx/diag.log" && [ "$_n" -eq 0 ]; then
        pass "selftest.baseline-diagnostic" "a zero-exit audit carrying a diagnostic refuses the run before any publication"
    else
        fail "selftest.baseline-diagnostic" "exit=$_st publications=$_n: $(grep -m1 'FAIL\[' "$sx/diag.log")"
    fi

    fi

    if want barriers-race; then
    # Two owners cannot both take the lock. The first is held at the instant its
    # lock becomes visible, which is exactly where a two-step lock would let a
    # contender in.
    cp "$root/jni/moq5_registry.h" "$sx/subject-race.h"
    _race_before=$(decl_sha256 "$sx/subject-race.h") \
        || abort_harness "selftest.lock-two-owner" "the contended subject" \
               "its digest could not be produced before the run"
    rm -f "$sx/race-barrier" "$sx/race-barrier.taken"
    env $(child_ps_env) MOQ5_PUBBIND_LOCK_BARRIER="$sx/race-barrier" \
        ${MOQ5_PUBBIND_CHILD_PS:+MOQ5_PUBBIND_FAULT=fixture-audit} \
        ${MOQ5_PUBBIND_CHILD_PS:+MOQ5_PUBBIND_FIXTURE=$sx/stubborn.sh} \
        ${MOQ5_PUBBIND_CHILD_PS:+MOQ5_PUBBIND_ARMED=$sx/armed-race} \
        ${MOQ5_PUBBIND_INVENTORY_APPEND:+MOQ5_PUBBIND_INVENTORY_APPEND=$MOQ5_PUBBIND_INVENTORY_APPEND} \
        ${MOQ5_PUBBIND_CHILD_PS:+MOQ5_PUBBIND_MARKER=$sx/marker-race} \
        MOQ5_PUBBIND_STUB_PUBLISH="$sx" MOQ5_PUBBIND_STUB_AUDIT=1 MOQ5_PUBBIND_STUB_RECEIPTS=1 MOQ5_PUBBIND_TIMEOUT=300 MOQ5_PUBBIND_LOCK_ROOT="$sx/locks" \
        /bin/sh "$driver" --harness --subject "$sx/subject-race.h" --evidence "$sx/ev-race-a" \
        > "$sx/race-a.log" 2>&1 &
    _rpid=$!
    if barrier "$sx/race-barrier.taken"; then
        env MOQ5_PUBBIND_STUB_PUBLISH="$sx" MOQ5_PUBBIND_STUB_AUDIT=1 MOQ5_PUBBIND_STUB_RECEIPTS=1 MOQ5_PUBBIND_TIMEOUT=300 MOQ5_PUBBIND_LOCK_ROOT="$sx/locks" \
            /bin/sh "$driver" --harness --subject "$sx/subject-race.h" --evidence "$sx/ev-race-b" \
            > "$sx/race-b.log" 2>&1
        _bst=$?
        : > "$sx/race-barrier"
        reap_bounded "$_rpid" 120 \
            || settle_or_abort "$_rpid" "$(recorded_group "$sx/ev-race-a")" \
                   "selftest.lock-two-owner" "the first invocation" "$sx/ev-race-a"
        _ast=$reaped_status
        settle_recorded_groups "$sx/ev-race-a" "selftest.lock-two-owner" \
            "the first invocation's own fixtures"
        _race_after=$(decl_sha256 "$sx/subject-race.h") \
            || abort_harness "selftest.lock-two-owner" "the contended subject" \
                   "its digest could not be produced after the run"
        if [ "$_bst" -ne 0 ] && grep -q 'FAIL\[pubbind.exclusion\]' "$sx/race-b.log" \
           && [ ! -f "$sx/ev-race-b/subject.orig" ] \
           && [ "$_race_after" = "$_race_before" ]; then
            pass "selftest.lock-two-owner" "the second invocation is refused at the moment the lock becomes visible; it took no backup and mutated nothing (first exit $_ast)"
        else
            fail "selftest.lock-two-owner" "second exit=$_bst backup=$([ -f "$sx/ev-race-b/subject.orig" ] && echo TAKEN || echo none) source=$([ "$_race_after" = "$_race_before" ] && echo unchanged || echo MUTATED)"
        fi
    else
        : > "$sx/race-barrier"
        fail "selftest.lock-two-owner" "the first invocation never reached the lock barrier; see $sx/race-a.log"
        settle_or_abort "$_rpid" "$(recorded_group "$sx/ev-race-a")" \
            "selftest.lock-two-owner" "the first invocation and its fixture" \
            "$sx/ev-race-a"
    fi

    fi

    if want rest; then
    # ---- F2: classification against the live functions --------------------
    D="1111111111111111111111111111111111111111111111111111111111111111"
    R="2222222222222222222222222222222222222222222222222222222222222222"
    expected_file "$sx/expected" "$D" "$R"
    named_log() { cp "$sx/expected" "$1"; }
    verdict() { classify_publish "$1" "$2" "$3" "$sx/expected" | cut -d' ' -f1; }
    mkdir -p "$sx/empty-repo"
    named_log "$sx/clean.log"

    v=$(verdict 1 "$sx/clean.log" "$sx/empty-repo")
    [ "$v" = refused ] && pass "selftest.class-clean" "a complete named refusal for both ABIs with no output is the satisfied contract" \
        || fail "selftest.class-clean" "the clean control classified '$v'"

    for c in "gcc error: unused variable x [-Werror=unused-variable]" \
             "clang cc1: unused variable 'x' [-Werror,-Wunused-variable]" \
             "bare cc1plus: all warnings being treated as errors [-Werror]" \
             "unknown FAIL[arm64-v8a.readelf]: llvm-readelf could not start"; do
        _tag=${c%% *}; _line=${c#* }
        { cat "$sx/clean.log"; printf '%s\n' "$_line"; } > "$sx/log-$_tag"
        v=$(verdict 1 "$sx/log-$_tag" "$sx/empty-repo")
        [ "$v" = inconclusive ] \
            && pass "selftest.class-$_tag" "refused with an unnamed diagnostic alongside it is inconclusive" \
            || fail "selftest.class-$_tag" "'$_line' classified '$v'"
    done

    sed 's/^/DEBUG expected log text, not an observed refusal: /' "$sx/clean.log" > "$sx/log-quoted"
    v=$(verdict 1 "$sx/log-quoted" "$sx/empty-repo")
    [ "$v" = inconclusive ] \
        && pass "selftest.class-quoted" "quoted diagnostic text is not an observed refusal" \
        || fail "selftest.class-quoted" "quoted text classified '$v'"

    head -1 "$sx/clean.log" > "$sx/log-oneabi"
    v=$(verdict 1 "$sx/log-oneabi" "$sx/empty-repo")
    [ "$v" = inconclusive ] && pass "selftest.class-one-abi" "one ABI is not both" \
        || fail "selftest.class-one-abi" "a single-ABI refusal classified '$v'"

    sed "s/$R/3333333333333333333333333333333333333333333333333333333333333333/" "$sx/clean.log" > "$sx/log-digest"
    v=$(verdict 1 "$sx/log-digest" "$sx/empty-repo")
    [ "$v" = inconclusive ] && pass "selftest.class-wrong-digest" "a refusal naming digests that were not measured is inconclusive" \
        || fail "selftest.class-wrong-digest" "a wrong-digest refusal classified '$v'"

    mkdir -p "$sx/wrote-repo/$GROUP"; : > "$sx/wrote-repo/$GROUP/maven-metadata.xml"
    v=$(verdict 1 "$sx/clean.log" "$sx/wrote-repo")
    [ "$v" = wrote ] && pass "selftest.class-wrote" "repository metadata counts as output, so the refusal did not hold" \
        || fail "selftest.class-wrote" "a refusal that wrote metadata classified '$v'"

    v=$(verdict 0 "$sx/clean.log" "$sx/empty-repo")
    [ "$v" = published ] && pass "selftest.class-success" "a zero exit is a publication, whatever the log says" \
        || fail "selftest.class-success" "a successful publication classified '$v'"

    v=$(verdict 124 "$sx/clean.log" "$sx/empty-repo")
    [ "$v" = inconclusive ] && pass "selftest.class-timeout" "a publication stopped at its deadline proves nothing" \
        || fail "selftest.class-timeout" "a timed-out publication classified '$v'"

    printf 'BUILD SUCCESSFUL in 3s\n> Task :moq5-android:publishReleasePublicationToProbeRepository\n' > "$sx/cl-benign"
    _cl=$(clean_log "$sx/cl-benign")
    [ -z "$_cl" ] && pass "selftest.clean-log-benign" "ordinary success framing is clean under an empty allowlist" \
        || fail "selftest.clean-log-benign" "benign framing was rejected: $_cl"
    for cc in "linker:ld: warning: object file was built for a newer OS version" \
              "gcc:cc1: error: unused variable x [-Werror=unused-variable]" \
              "clang:cc1: unused variable 'x' [-Werror,-Wunused-variable]" \
              "named:FAIL[arm64-v8a.readelf]: llvm-readelf could not start" \
              "compiler:e: Registry.kt: error: unresolved reference"; do
        _ct=${cc%%:*}; _cline=${cc#*:}
        { printf 'BUILD SUCCESSFUL in 3s\n'; printf '%s\n' "$_cline"; } > "$sx/cl-$_ct"
        _cl=$(clean_log "$sx/cl-$_ct")
        [ -n "$_cl" ] \
            && pass "selftest.clean-log-$_ct" "an empty allowlist rejects it too: $_cl" \
            || fail "selftest.clean-log-$_ct" "'$_cline' passed clean_log unchallenged"
    done
    _cl=$(clean_log "$sx/no-such-log-file")
    case "$_cl" in
        INSPECTION-ERROR*) pass "selftest.clean-log-unreadable" "a log that cannot be inspected is an error, not proof of cleanliness" ;;
        *) fail "selftest.clean-log-unreadable" "an unreadable log classified as [$_cl]" ;;
    esac

    printf '> Task :moq5-android:verifyJniTopology\n' > "$sx/t1"
    printf '> Task :moq5-android:verifyJniTopology UP-TO-DATE\n' > "$sx/t2"
    printf 'the publication does not depend on :moq5-android:verifyJniTopology\n' > "$sx/t3"
    _ts="$(task_state "$sx/t1" ':moq5-android:verifyJniTopology')/$(task_state "$sx/t2" ':moq5-android:verifyJniTopology')/$(task_state "$sx/t3" ':moq5-android:verifyJniTopology')"
    [ "$_ts" = "executed/up-to-date/absent" ] \
        && pass "selftest.task-state" "execution, up-to-date and a name in prose are distinguished" \
        || fail "selftest.task-state" "task states read '$_ts'"


    # ---- the coordinator's own controls ----------------------------------

    fi

    if want directchild; then
    # An inspection it cannot make must refuse inside its bound, never fall
    # through into a wait nothing bounds -- and the subject is a REAL direct
    # child of this shell, so `wait` on it would genuinely block.
    ( exec 2> "$sx/direct-child.err"
      python3 -c 'import os, signal; os.setsid(); signal.signal(signal.SIGTERM, signal.SIG_IGN); os.execvp("sleep", ["sleep", "45"])' &
      _p=$!
      _pd=$(( $(now) + 10 ))
      while [ "$(harness_proc_live "$_p")" != alive ]; do
          [ "$(now)" -lt "$_pd" ] || break
          sleep 1
      done
      # Recorded while it can still be proved, so any caller can settle it.
      printf 'session=%s observed=direct-child\n' "$_p" >> "$evidence/ownership.txt"
      _ppid=$(/bin/ps -o ppid= -p "$_p" 2>/dev/null | tr -d ' ')
      _pcmd=$(/bin/ps -o command= -p "${_ppid:-1}" 2>/dev/null)
      _t0=$(now)
      _saved_hps=${MOQ5_PUBBIND_HARNESS_PS-}
      MOQ5_PUBBIND_HARNESS_PS="$sx/ps-fail"
      if reap_bounded "$_p" 1; then _r=success; else _r=$reaped_status; fi
      if [ -n "$_saved_hps" ]; then MOQ5_PUBBIND_HARNESS_PS=$_saved_hps
      else unset MOQ5_PUBBIND_HARNESS_PS; fi
      _el=$(( $(now) - _t0 ))
      _al=$(harness_proc_live "$_p")
      if stop_and_reap "$_p" 30; then _cl=reaped; else _cl=${reaped_status:-none}; fi
      _af=$(harness_proc_live "$_p")
      case "$_pcmd" in
          *red-publication-source-binding*) _pk=yes ;;
          *) _pk="no" ;;
      esac
      printf 'result=%s elapsed=%s alive_during=%s parent_is_driver=%s cleanup=%s alive_after=%s\n' \
          "$_r" "$_el" "$_al" "$_pk" "$_cl" "$_af" > "$sx/direct-child.result" )
    _dc=$(cat "$sx/direct-child.result" 2>/dev/null)
    case "$_dc" in
        "result=unobservable elapsed="[0-9]" alive_during=alive parent_is_driver=yes cleanup=reaped alive_after=gone"|\
        "result=unobservable elapsed="[0-9][0-9]" alive_during=alive parent_is_driver=yes cleanup=reaped alive_after=gone")
            pass "selftest.coordinator-unobservable-no-wait" "against a real direct child of this shell, an inspection the coordinator cannot make refuses at once rather than waiting on it, and bounded cleanup then reaps it ($_dc)" ;;
        *)
            fail "selftest.coordinator-unobservable-no-wait" "$_dc" ;;
    esac
    case "$_dc" in
        *"cleanup=reaped"*) : ;;
        *) abort_harness "selftest.coordinator-unobservable-no-wait" \
               "the direct-child oracle's own subject" \
               "its final cleanup reported ${_dc:-nothing}" ;;
    esac
    # The only thing that shell was allowed to say is a job notice about the
    # child this control killed on purpose.
    if [ ! -s "$sx/direct-child.err" ] || ! grep -qvE '^.*(Killed|Terminated)' "$sx/direct-child.err"; then
        pass "selftest.coordinator-direct-child-quiet" "the direct-child oracle emitted nothing but the job notice for the child it stopped"
    else
        fail "selftest.coordinator-direct-child-quiet" "unexpected output: $(head -2 "$sx/direct-child.err" | tr '\n' ' ')"
    fi

    fi

    if want negctl; then
    # The stop boundary, exercised at a REAL cleanup call site in an isolated
    # child: with the coordinator's inspection broken, the first cleanup must
    # abort the run before the next command is even composed.
    if true; then
        rm -rf "$sx/ev-abort"
        env MOQ5_PUBBIND_HARNESS_PS="$sx/ps-fail" \
            /bin/sh "$driver" --selftest --harness --section supervision --evidence "$sx/ev-abort" \
            > "$sx/abort-section.log" 2>&1
        _ast=$?
        if [ "$_ast" -ne 0 ] \
           && grep -q 'FAIL\[selftest.supervision-timeout\]' "$sx/abort-section.log" \
           && grep -q 'HARNESS ABORTED' "$sx/abort-section.log" \
           && [ ! -f "$sx/ev-abort/sup-descendant.cmd" ] \
           && [ ! -f "$sx/ev-abort/fixtures/armed-2" ] \
           && ! grep -q 'PASS\[selftest.supervision-descendant\]' "$sx/abort-section.log"; then
            pass "selftest.coordinator-abort-stops-next-launch" "an unproved cleanup aborts at the call site: no next command, no next worker, exit $_ast"
        else
            fail "selftest.coordinator-abort-stops-next-launch" "exit=$_ast next-command=$([ -f "$sx/ev-abort/sup-descendant.cmd" ] && echo COMPOSED || echo none) next-worker=$([ -f "$sx/ev-abort/fixtures/armed-2" ] && echo STARTED || echo none) aborted=$(grep -c 'HARNESS ABORTED' "$sx/abort-section.log")"
        fi
        # Whatever that child left is stopped by this coordinator, not by it.
        for _g in $(sed -n 's/^session=\([0-9]*\).*/\1/p' "$sx/ev-abort/ownership.txt" 2>/dev/null); do
            cleanup_or_abort "$_g" "selftest.coordinator-abort-stops-next-launch" \
                "the aborted child's fixture group" "$sx/ev-abort"
        done

        # The matched success control reaches that same next launch.
        rm -rf "$sx/ev-continue"
        /bin/sh "$driver" --selftest --harness --section supervision --evidence "$sx/ev-continue" \
            > "$sx/continue-section.log" 2>&1
        _cst=$?
        if [ "$_cst" -eq 0 ] && [ -f "$sx/ev-continue/sup-descendant.cmd" ] \
           && grep -q 'PASS\[selftest.supervision-descendant\]' "$sx/continue-section.log" \
           && ! grep -q 'HARNESS ABORTED' "$sx/continue-section.log"; then
            pass "selftest.coordinator-success-reaches-next-launch" "with a working inspection the same call site proves its cleanup and the next launch happens"
        else
            fail "selftest.coordinator-success-reaches-next-launch" "exit=$_cst next-command=$([ -f "$sx/ev-continue/sup-descendant.cmd" ] && echo yes || echo NONE)"
        fi
    fi

    fi

    want artifact && artifact_section

    if want rest; then
    # A command that EXITS SUCCESSFULLY while leaving a worker in its own group.
    # A finished command is not a quiet tree.
    cat > "$sx/leaver.sh" <<'FIX'
( trap '' TERM INT; n=0; while [ "$n" -lt 180 ]; do printf 'z' >> "$MOQ5_PUBBIND_MARKER.worker"; n=$((n + 1)); sleep 1; done ) &
: > "$MOQ5_PUBBIND_ARMED"
exit 0
FIX
    MOQ5_PUBBIND_ARMED="$sx/armed-leave"; MOQ5_PUBBIND_MARKER="$sx/marker-leave"
    export MOQ5_PUBBIND_ARMED MOQ5_PUBBIND_MARKER
    run_owned sup-leaver /bin/sh "$sx/leaver.sh"; _rc=$?
    _grp=$(sed -n 's/^session=\([0-9]*\).*/\1/p' "$evidence/ownership.txt" | tail -1)
    if [ "$_rc" -eq 0 ] && ownership_ok && [ "$(harness_group_live "$_grp")" = empty ]; then
        pass "selftest.supervision-live-descendant" "a command that exits 0 leaving a same-group worker still ends in proved quiescence, and the worker is gone"
    else
        fail "selftest.supervision-live-descendant" "rc=$_rc ownership_ok=$(ownership_ok && echo yes || echo no) group=$(harness_group_live "$_grp")"
    fi
    cleanup_or_abort "$_grp" "selftest.supervision-live-descendant" "the leaver's worker"

    # An ownership setup that cannot be established starts no worker at all.
    MOQ5_PUBBIND_ARMED="$sx/armed-nosetup"; MOQ5_PUBBIND_MARKER="$sx/marker-nosetup"
    MOQ5_PUBBIND_BREAK_SETSID=1; export MOQ5_PUBBIND_BREAK_SETSID
    run_owned sup-nosetup /bin/sh "$sx/prompt.sh"; _rc=$?
    unset MOQ5_PUBBIND_BREAK_SETSID
    if [ "$_rc" -eq 126 ] && [ ! -f "$sx/armed-nosetup" ] \
       && [ "$(cat "$evidence/sup-nosetup.status" 2>/dev/null)" = ownership-setup-failed ]; then
        pass "selftest.supervision-setup-failure" "an ownership setup that cannot be established starts no worker and is refused by name"
    else
        fail "selftest.supervision-setup-failure" "rc=$_rc armed=$([ -f "$sx/armed-nosetup" ] && echo yes || echo no) status=$(cat "$evidence/sup-nosetup.status" 2>/dev/null)"
    fi

    # A cleanup that cannot be OBSERVED is a failure, and no later call may turn
    # that failure back into success.
    ownership_failed=1; owned_group=""; quiescent=1
    if stop_owned; then
        fail "selftest.ownership-sticky" "a second cleanup call erased an earlier ownership failure"
    else
        ownership_ok \
            && fail "selftest.ownership-sticky" "ownership reports itself clean after a recorded failure" \
            || pass "selftest.ownership-sticky" "an ownership failure is sticky: a later cleanup call cannot report success"
    fi
    ownership_failed=0; owned_group=""; quiescent=1


    # A fixture producer that fails must not be consumed, and nothing downstream
    # of it may report a pass. Driven through the artifact section alone, so the
    # guard is what is measured rather than the rest of the suite.
    if [ -z "${MOQ5_PUBBIND_BREAK_FIXTURE-}" ]; then
        env MOQ5_PUBBIND_BREAK_FIXTURE=1 \
            /bin/sh "$driver" --selftest --harness --section artifact --evidence "$sx/ev-fixturefail" \
            > "$sx/fixturefail.log" 2>&1
        _st=$?
        if [ "$_st" -ne 0 ] \
           && grep -q 'FAIL\[selftest.fixture-build\]' "$sx/fixturefail.log" \
           && grep -q 'FAIL\[selftest.fixture-consumed\]' "$sx/fixturefail.log" \
           && ! grep -q 'PASS\[selftest.inventory-' "$sx/fixturefail.log"; then
            pass "selftest.fixture-build-failure" "a producer that exits non-zero is named as a fixture-build failure and no artifact check reports a pass"
        else
            fail "selftest.fixture-build-failure" "exit=$_st build=$(grep -c 'FAIL\[selftest.fixture-build\]' "$sx/fixturefail.log") inventory-passes=$(grep -c 'PASS\[selftest.inventory-' "$sx/fixturefail.log")"
        fi
    fi

    # ---- F3: the REAL digest guards, with their effects observed ----------
    for g in baseline-digest-missing:pubbind.baseline-digest:0 digest-unchanged:pubbind.moved:1; do
        _f=${g%%:*}; _rest=${g#*:}; _row=${_rest%%:*}; _pubs=${_rest#*:}
        cp "$root/jni/moq5_registry.h" "$sx/subject-$_f.h"
        _sb=$(decl_sha256 "$sx/subject-$_f.h") \
            || abort_harness "selftest.guard-$_f" "the guarded subject" \
                   "its digest could not be produced before the run"
        rm -f "$sx/invocations"; : > "$sx/invocations"
        env MOQ5_PUBBIND_FAULT="$_f" MOQ5_PUBBIND_STUB_PUBLISH="$sx" MOQ5_PUBBIND_STUB_AUDIT=1 MOQ5_PUBBIND_STUB_RECEIPTS=1 MOQ5_PUBBIND_TIMEOUT=300 MOQ5_PUBBIND_LOCK_ROOT="$sx/locks" \
            /bin/sh "$driver" --harness --subject "$sx/subject-$_f.h" --evidence "$sx/ev-$_f" \
            > "$sx/$_f.log" 2>&1
        _st=$?
        _n=$(awk 'END { print NR + 0 }' "$sx/invocations")
        _stale=$(awk '/^publish-stale$/ { n++ } END { print n + 0 }' "$sx/invocations")
        _sa=$(decl_sha256 "$sx/subject-$_f.h") \
            || abort_harness "selftest.guard-$_f" "the guarded subject" \
                   "its digest could not be produced after the run"
        if [ "$_st" -ne 0 ] && grep -q "FAIL\[$_row\]" "$sx/$_f.log" \
           && [ "$_n" -eq "$_pubs" ] && [ "$_stale" -eq 0 ] \
           && [ ! -f "$sx/ev-$_f/audit-perturbed.cmd" ] \
           && [ "$_sa" = "$_sb" ]; then
            pass "selftest.guard-$_f" "the live guard refuses by $_row, the rejected-input publication never starts ($_n publication(s) recorded), and the subject is restored"
        else
            fail "selftest.guard-$_f" "exit=$_st row=$(grep -c "FAIL\[$_row\]" "$sx/$_f.log") publications=$_n stale=$_stale perturbed-audit=$([ -f "$sx/ev-$_f/audit-perturbed.cmd" ] && echo ran || echo no)"
        fi
    done

    # ---- F1: exclusion lasts as long as the source is unsafe ---------------
    # Anything already at the lock path is refused, whatever it is.
    cp "$root/jni/moq5_registry.h" "$sx/subject-lockdir.h"
    _lp=$(lock_path_for "$sx/subject-lockdir.h"); rm -rf "$_lp"; mkdir -p "$_lp"
    drive "$sx/subject-lockdir.h" "$sx/ev-lockdir" "$sx/lockdir.log"
    _st=$?
    if [ "$_st" -ne 0 ] && grep -q 'FAIL\[pubbind.exclusion\]' "$sx/lockdir.log" \
       && [ ! -f "$sx/ev-lockdir/subject.orig" ] && [ -d "$_lp" ]; then
        pass "selftest.lock-existing-directory" "an empty directory already at the lock path is refused before any backup is taken"
    else
        fail "selftest.lock-existing-directory" "exit=$_st backup=$([ -f "$sx/ev-lockdir/subject.orig" ] && echo TAKEN || echo none): $(grep -m1 'FAIL\[' "$sx/lockdir.log")"
    fi
    rm -rf "$_lp"

    # A record naming a long-dead pid is diagnostic, never permission to unlink.
    cp "$root/jni/moq5_registry.h" "$sx/subject-stale.h"
    _lp=$(lock_path_for "$sx/subject-stale.h"); rm -rf "$_lp"; mkdir -p "$_lp"
    printf 'pid=%s\nevidence=/nonexistent\nsubject=%s\nend\n' "999999" "$sx/subject-stale.h" > "$_lp/owner.record"
    drive "$sx/subject-stale.h" "$sx/ev-stale" "$sx/stale.log"
    _st=$?
    if [ "$_st" -ne 0 ] && grep -q 'FAIL\[pubbind.exclusion\]' "$sx/stale.log" \
       && [ ! -f "$sx/ev-stale/subject.orig" ] && [ -f "$_lp/owner.record" ]; then
        pass "selftest.lock-no-takeover" "a lock whose recorded owner is long gone is still refused; the record is not permission to unlink"
    else
        fail "selftest.lock-no-takeover" "exit=$_st record=$([ -f "$_lp/owner.record" ] && echo kept || echo REMOVED): $(grep -m1 'FAIL\[' "$sx/stale.log")"
    fi
    rm -rf "$_lp"

    # A run that cannot prove cleanup keeps the subject quarantined AFTER it
    # exits: the source stays perturbed, its backup is kept, and no second owner
    # may take it -- refused before any backup of its own.
    cp "$root/jni/moq5_registry.h" "$sx/subject-quar.h"
    _quar_before=$(decl_sha256 "$sx/subject-quar.h") \
        || abort_harness "selftest.lock-retained-after-unproved-cleanup" "the quarantined subject" \
               "its digest could not be produced before the run"
    _lp=$(lock_path_for "$sx/subject-quar.h"); rm -rf "$_lp"
    rm -f "$sx/blind-after"
    drive "$sx/subject-quar.h" "$sx/ev-quar" "$sx/quar.log" \
        MOQ5_PUBBIND_FAULT="fixture-audit-late break-probe-after-perturb" \
        MOQ5_PUBBIND_FIXTURE="$(arming_fixture)" \
        MOQ5_PUBBIND_PS="$sx/ps-blind-after" MOQ5_PUBBIND_BLIND_AFTER="$sx/blind-after" \
        MOQ5_PUBBIND_ARMED="$sx/armed-quar" MOQ5_PUBBIND_MARKER="$sx/marker-quar" \
        MOQ5_PUBBIND_TIMEOUT=8
    _st=$?
    _quar_after=$(decl_sha256 "$sx/subject-quar.h") \
        || abort_harness "selftest.lock-retained-after-unproved-cleanup" "the quarantined subject" \
               "its digest could not be produced after the run"
    _order=$(tr '\n' ' ' < "$sx/ev-quar/order.txt" 2>/dev/null)
    if [ "$_st" -ne 0 ] && [ -d "$_lp" ] && [ -f "$sx/ev-quar/subject.orig" ] \
       && [ "$_quar_after" != "$_quar_before" ] \
       && grep -q 'restore:refused' "$sx/ev-quar/order.txt" \
       && grep -q 'RETAINED' "$sx/quar.log"; then
        pass "selftest.lock-retained-after-unproved-cleanup" "an unproved cleanup leaves the source quarantined, its backup kept and the lock retained past exit (order: $_order)"
    else
        fail "selftest.lock-retained-after-unproved-cleanup" "exit=$_st lock=$([ -d "$_lp" ] && echo kept || echo RELEASED) backup=$([ -f "$sx/ev-quar/subject.orig" ] && echo kept || echo GONE) subject=$([ "$_quar_after" != "$_quar_before" ] && echo perturbed || echo RESTORED) order=$_order"
    fi
    drive "$sx/subject-quar.h" "$sx/ev-quar-b" "$sx/quar-b.log"
    _st=$?
    _quar_b_now=$(decl_sha256 "$sx/subject-quar.h") \
        || abort_harness "selftest.lock-quarantine-blocks-next-owner" "the quarantined subject" \
               "its digest could not be produced after the second invocation"
    if [ "$_st" -ne 0 ] && grep -q 'FAIL\[pubbind.exclusion\]' "$sx/quar-b.log" \
       && [ ! -f "$sx/ev-quar-b/subject.orig" ] \
       && [ "$_quar_b_now" = "$_quar_after" ]; then
        pass "selftest.lock-quarantine-blocks-next-owner" "a quarantined subject admits no second owner, and the refused invocation took no backup and changed nothing"
    else
        fail "selftest.lock-quarantine-blocks-next-owner" "exit=$_st backup=$([ -f "$sx/ev-quar-b/subject.orig" ] && echo TAKEN || echo none): $(grep -m1 'FAIL\[' "$sx/quar-b.log")"
    fi
    # The coordinator, not the driver, resolves the quarantine it created -- and
    # only once it can PROVE the worker is gone. Otherwise the quarantine, the
    # backup and the perturbed source stay exactly as they are.
    _al=$(sed -n 's/^session=\([0-9]*\).*/\1/p' "$sx/ev-quar/ownership.txt" 2>/dev/null | tail -1)
    typed_dispatch "$(route_identity "$sx/ev-quar" "$_al")" "$_al" \
        "selftest.lock-retained-after-unproved-cleanup" "the quarantined run's fixture" cleanup_or_abort "$sx/ev-quar"
    cp "$sx/ev-quar/subject.orig" "$sx/subject-quar.h" 2>/dev/null || true
    rm -rf "$_lp"

    # An ordinary run that does prove cleanup and restoration hands the subject on.
    cp "$root/jni/moq5_registry.h" "$sx/subject-hand.h"
    _lp=$(lock_path_for "$sx/subject-hand.h"); rm -rf "$_lp"
    drive "$sx/subject-hand.h" "$sx/ev-hand-a" "$sx/hand-a.log" MOQ5_PUBBIND_FAULT=digest-unchanged
    drive "$sx/subject-hand.h" "$sx/ev-hand-b" "$sx/hand-b.log" MOQ5_PUBBIND_FAULT=digest-unchanged
    if grep -q 'PASS\[pubbind.restore\]' "$sx/hand-a.log" && [ ! -d "$_lp" ] \
       && ! grep -q 'FAIL\[pubbind.exclusion\]' "$sx/hand-b.log" \
       && [ -f "$sx/ev-hand-b/subject.orig" ]; then
        pass "selftest.lock-released-after-success" "a run that proves cleanup and restoration releases the lock, and the next owner takes it"
    else
        fail "selftest.lock-released-after-success" "lock=$([ -d "$_lp" ] && echo RETAINED || echo released) second=$(grep -c 'FAIL\[pubbind.exclusion\]' "$sx/hand-b.log") backup=$([ -f "$sx/ev-hand-b/subject.orig" ] && echo taken || echo NONE)"
    fi
    rm -rf "$_lp"

    # ---- F4: evidence is never reused, and one owner at a time ------------
    mkdir -p "$sx/ev-prepopulated"; printf 'an older witness\n' > "$sx/ev-prepopulated/witness.txt"
    _w=$(decl_sha256 "$sx/ev-prepopulated/witness.txt") \
        || abort_harness "selftest.evidence-prepopulated" "the pre-existing witness" \
               "its digest could not be produced before the run"
    cp "$root/jni/moq5_registry.h" "$sx/subject-ev.h"
    env MOQ5_PUBBIND_STUB_PUBLISH="$sx" MOQ5_PUBBIND_STUB_AUDIT=1 MOQ5_PUBBIND_STUB_RECEIPTS=1 MOQ5_PUBBIND_TIMEOUT=300 MOQ5_PUBBIND_LOCK_ROOT="$sx/locks" \
        /bin/sh "$driver" --harness --subject "$sx/subject-ev.h" --evidence "$sx/ev-prepopulated" \
        > "$sx/ev.log" 2>&1
    _st=$?
    _w_now=$(decl_sha256 "$sx/ev-prepopulated/witness.txt") \
        || abort_harness "selftest.evidence-prepopulated" "the pre-existing witness" \
               "its digest could not be produced after the run"
    if [ "$_st" -ne 0 ] && grep -q 'already exists' "$sx/ev.log" \
       && [ "$_w_now" = "$_w" ] \
       && [ "$(ls "$sx/ev-prepopulated" | wc -l | tr -d ' ')" -eq 1 ]; then
        pass "selftest.evidence-refuses-reuse" "an existing evidence directory is refused, not reused or cleared; the older witness is byte-identical"
    else
        fail "selftest.evidence-refuses-reuse" "exit=$_st; the driver did not refuse a prepopulated evidence directory"
    fi

    fi

    if want barriers-own; then
    cp "$root/jni/moq5_registry.h" "$sx/subject-own.h"
    _ob=$(decl_sha256 "$sx/subject-own.h") \
        || abort_harness "selftest.ownership-exclusion" "the owned subject" \
               "its digest could not be produced before the run"
    MOQ5_PUBBIND_ARMED="$sx/armed-own"; export MOQ5_PUBBIND_ARMED
    env $(child_ps_env) MOQ5_PUBBIND_FAULT=fixture-audit MOQ5_PUBBIND_FIXTURE="$(arming_fixture)" \
        MOQ5_PUBBIND_STUB_PUBLISH="$sx" MOQ5_PUBBIND_STUB_AUDIT=1 MOQ5_PUBBIND_STUB_RECEIPTS=1 MOQ5_PUBBIND_TIMEOUT=300 MOQ5_PUBBIND_LOCK_ROOT="$sx/locks" \
        MOQ5_PUBBIND_ARMED="$sx/armed-own" MOQ5_PUBBIND_MARKER="$sx/marker-own" \
        ${MOQ5_PUBBIND_INVENTORY_APPEND:+MOQ5_PUBBIND_INVENTORY_APPEND=$MOQ5_PUBBIND_INVENTORY_APPEND} \
        /bin/sh "$driver" --harness --subject "$sx/subject-own.h" --evidence "$sx/ev-own-a" \
        > "$sx/own-a.log" 2>&1 &
    _apid=$!
    if barrier "$sx/armed-own"; then
        env MOQ5_PUBBIND_STUB_PUBLISH="$sx" MOQ5_PUBBIND_STUB_AUDIT=1 MOQ5_PUBBIND_STUB_RECEIPTS=1 MOQ5_PUBBIND_TIMEOUT=300 MOQ5_PUBBIND_LOCK_ROOT="$sx/locks" \
            /bin/sh "$driver" --harness --subject "$sx/subject-own.h" --evidence "$sx/ev-own-b" \
            > "$sx/own-b.log" 2>&1
        _st=$?
        kill -TERM "$_apid" 2>/dev/null || true
        reap_bounded "$_apid" 90 \
            || settle_or_abort "$_apid" "$(recorded_group "$sx/ev-own-a")" \
                   "selftest.ownership-exclusion" "the first invocation" \
                   "$sx/ev-own-a"
        _ast=$reaped_status
        settle_recorded_groups "$sx/ev-own-a" "selftest.ownership-exclusion" \
            "the first invocation's own fixtures"
        _oa=$(decl_sha256 "$sx/subject-own.h") \
            || abort_harness "selftest.ownership-exclusion" "the owned subject" \
                   "its digest could not be produced after the run"
        if [ "$_st" -ne 0 ] && grep -q 'FAIL\[pubbind.exclusion\]' "$sx/own-b.log" \
           && [ "$_oa" = "$_ob" ]; then
            pass "selftest.ownership-exclusion" "a second invocation is refused while the first owns the subject; the source is byte-identical (first exit $_ast)"
        else
            fail "selftest.ownership-exclusion" "exit=$_st; the competing invocation was not refused: $(tail -2 "$sx/own-b.log" | tr '\n' ' ')"
        fi
    else
        fail "selftest.ownership-exclusion" "the first invocation never armed; see $sx/own-a.log"
        settle_or_abort "$_apid" "$(recorded_group "$sx/ev-own-a")" \
            "selftest.ownership-exclusion" "the first invocation and its fixture" \
            "$sx/ev-own-a"
    fi
    unset MOQ5_PUBBIND_ARMED
    fi

    if want rest; then

    # ---- restoration primitives -------------------------------------------
    printf 'original\n' > "$sx/subj"
    w=$(decl_sha256 "$sx/subj") \
        || abort_harness "selftest.restore-ok" "the restoration fixture" \
               "its digest could not be produced"
    cp "$sx/subj" "$sx/subj.bak"; printf 'perturbed\n' >> "$sx/subj"
    # The restored digest is taken AFTER the restoration and in checked
    # assignment form, so its own failure cannot be read as a match.
    restore_pair "$sx/subj" "$sx/subj.bak" "$w" "selftest.restore-inner" > "$sx/restore-ok.log" 2>&1
    _rp_st=$?
    _w_restored=$(decl_sha256 "$sx/subj") \
        || abort_harness "selftest.restore-ok" "the restoration fixture" \
               "its digest could not be produced after the restore"
    if [ "$_rp_st" -eq 0 ] && [ "$_w_restored" = "$w" ]; then
        pass "selftest.restore-ok" "a recoverable perturbation is restored and proved"
    else
        fail "selftest.restore-ok" "the restoration control did not restore the file"
    fi
    printf 'perturbed\n' >> "$sx/subj"; rm -f "$sx/subj.bak"
    PAIR_REPORT=0
    if restore_pair "$sx/subj" "$sx/subj.bak" "$w" "selftest.restore-inner" > "$sx/restore-lost.log" 2>&1; then
        PAIR_REPORT=1; fail "selftest.restore-lost-backup" "a lost backup was reported as a successful restoration"
    else
        PAIR_REPORT=1
        grep -q 'FAIL\[selftest.restore-inner\]' "$sx/restore-lost.log" \
            && pass "selftest.restore-lost-backup" "a failed restoration fails by name and claims nothing" \
            || fail "selftest.restore-lost-backup" "a failed restoration did not report its own named failure"
    fi
    printf 'kept\n' > "$sx/subj2"; cp "$sx/subj2" "$sx/subj2.bak"; printf 'wrong\n' > "$sx/subj2"
    PAIR_REPORT=0
    if restore_pair "$sx/subj2" "$sx/subj2.bak" \
        "0000000000000000000000000000000000000000000000000000000000000000" \
        "selftest.restore-inner" > "$sx/retain.log" 2>&1; then
        PAIR_REPORT=1; fail "selftest.restore-retains-backup" "a mismatched restoration was reported as successful"
    else
        PAIR_REPORT=1
        if [ -f "$sx/subj2.bak" ] && grep -q 'RETAINED' "$sx/retain.log"; then
            pass "selftest.restore-retains-backup" "on a failed restoration the recoverable backup is retained and reported"
        else
            fail "selftest.restore-retains-backup" "the backup was not retained or not reported"
        fi
    fi


    # ---- the stop boundary at every remaining cleanup caller --------------
    # Each case runs the REAL call site in an isolated child section, with the
    # barrier made to fail (or be reached) in seconds rather than waited out.
    for _sec in barriers-interrupt barriers-race barriers-own directchild negctl; do
        run_section "$_sec" "$sx/stop-$_sec.log" MOQ5_PUBBIND_HARNESS_PS="$sx/ps-fail"
        _src=$?
        if [ "$_src" -ne 0 ] && grep -q 'HARNESS ABORTED' "$sx/stop-$_sec.log" \
           && nothing_after_abort "$sx/stop-$_sec.log"; then
            pass "selftest.stop-boundary-$_sec" "an unproved cleanup at this call site stops the run: $(grep -m1 'HARNESS ABORTED' "$sx/stop-$_sec.log")"
        else
            fail "selftest.stop-boundary-$_sec" "exit=$_src aborted=$(grep -c 'HARNESS ABORTED' "$sx/stop-$_sec.log") continued=$(nothing_after_abort "$sx/stop-$_sec.log" && echo no || echo YES)"
        fi
    done

    # The matched half: with a working inspection the same call sites settle
    # their cleanup and the section carries on, keeping the barrier failure as
    # an ordinary test failure.
    for _sec in barriers-interrupt barriers-race barriers-own; do
        run_section "$_sec" "$sx/cont-$_sec.log"
        _src=$?
        if ! grep -q 'HARNESS ABORTED' "$sx/cont-$_sec.log" \
           && grep -q 'harness controls: ' "$sx/cont-$_sec.log"; then
            pass "selftest.stop-boundary-continue-$_sec" "a proved cleanup lets the section finish, keeping $(grep -c '^FAIL\[selftest\.' "$sx/cont-$_sec.log") recorded failure(s) as ordinary test failures"
        else
            fail "selftest.stop-boundary-continue-$_sec" "exit=$_src aborted=$(grep -c 'HARNESS ABORTED' "$sx/cont-$_sec.log")"
        fi
    done


    # The isolated-negative control's PARENT must never reach its matched
    # success case on an unproved cleanup of the child it just aborted.
    if grep -q 'HARNESS ABORTED' "$sx/stop-negctl.log" \
       && ! grep -q 'PASS\[selftest.coordinator-success-reaches-next-launch\]' "$sx/stop-negctl.log" \
       && [ ! -d "$sx/ev-negctl/fixtures/ev-continue" ]; then
        pass "selftest.negctl-parent-cleanup-stops" "the parent's own cleanup failure stops it before the matched success case, which never ran"
    else
        fail "selftest.negctl-parent-cleanup-stops" "the parent continued to its success case after an unproved cleanup"
    fi
    fi

    printf '\nharness controls: %d failure(s)\nevidence: %s\n' "$fails" "$evidence"
    [ "$fails" -eq 0 ] || exit 1
    exit 0
fi

# ---------------------------------------------------------------- the real RED

# The declared native input this driver perturbs: a hashed source whose comment
# text cannot change any machine code.
subject=${subject_override:-$root/jni/moq5_registry.h}
[ -f "$subject" ] || { echo "$_self: no perturbation subject at $subject" >&2; exit 2; }

acquire_lock "$subject" || { printf '\npublication source binding: %d failure(s)\nevidence: %s\n' "$fails" "$evidence"; exit 1; }

backup="$evidence/subject.orig"
cp "$subject" "$backup" || { release_lock; echo "$_self: cannot back up the subject" >&2; exit 2; }
before=$(decl_sha256 "$subject") || { release_lock; echo "$_self: cannot hash the subject" >&2; exit 2; }

exiting=0
restore() {
    [ "$restored" -eq 1 ] && return 0
    if ! ownership_ok; then
        printf 'restore:refused\n' >> "$evidence/order.txt"
        pair_fail "pubbind.restore" "cleanup is unproved (failed=$ownership_failed, outstanding group '${owned_group:-none}', quiescent=$quiescent); refusing to restore the perturbed input while anything may still read it, and the recoverable backup is RETAINED at $backup"
        return 1
    fi
    if restore_pair "$subject" "$backup" "$before" "pubbind.restore"; then
        printf 'restore:ok\n' >> "$evidence/order.txt"
        restored=1
        return 0
    fi
    printf 'restore:failed\n' >> "$evidence/order.txt"
    return 1
}

# Nothing that reads or mutates the tree may proceed on unproved cleanup.
require_ownership() {
    ownership_ok && return 0
    fail "pubbind.ownership" "$1 left cleanup unproved (failed=$ownership_failed, outstanding group '${owned_group:-none}', quiescent=$quiescent); refusing to go on"
    return 1
}
# shellcheck disable=SC2329  # invoked from the traps below
on_signal() {
    stop_owned || true
    restore || true
    release_lock
    printf 'evidence: %s\n' "$evidence"
    exiting=1
    exit 130
}
trap on_signal HUP INT TERM
# shellcheck disable=SC2329  # invoked from the traps below
on_exit() {
    _xr=$?
    [ "$exiting" -eq 1 ] && return
    exiting=1
    stop_owned || true
    restore || _xr=1
    release_lock
    printf 'evidence: %s\n' "$evidence"
    exit "$_xr"
}
trap on_exit EXIT

finish() {
    stop_owned || true
    restore || true
    release_lock
    printf '\npublication source binding: %d failure(s)\n' "$fails"
    printf 'evidence: %s\n' "$evidence"
    exiting=1
    [ "$fails" -eq 0 ] || exit 1
    exit 0
}

: > "$evidence/.empty-expected"

# -- 1. the baseline ----------------------------------------------------------
run_audit audit-baseline; audit_status=$?
require_ownership "the baseline audit" || finish
if [ "$audit_status" -ne 0 ]; then
    fail "pubbind.baseline" "the topology audit does not pass before the perturbation: $(grep -m1 '^FAIL' "$evidence/audit-baseline.log")"
    finish
fi
_d=$(clean_log "$evidence/audit-baseline.log")
if [ -n "$_d" ]; then
    fail "pubbind.baseline" "the baseline audit carries a diagnostic this contract does not name: $_d"
    finish
fi
pass "pubbind.baseline" "the topology audit passes cleanly on the unperturbed tree"

d_base=$(digest_of before) || d_base=""
if ! decl_is_sha256 "$d_base"; then
    fail "pubbind.baseline-digest" "the baseline input digest could not be computed through lib-inputs.sh; a driver with no measured baseline cannot judge a refusal"
    finish
fi
_bad=""
for abi in $ABIS; do
    _rd=$(declared_digest "$abi") || _rd=""
    _rcpt=$(decl_sha256 "$root/.native/_topology/$abi/topology.provenance") \
        || _rcpt=unreadable
    printf '%s receipt=%s declared=%s\n' "$abi" "$_rcpt" "$_rd" >> "$evidence/receipts.txt"
    [ "$_rd" = "$d_base" ] || _bad="$_bad $abi(${_rd:-unreadable})"
done
if [ -n "$_bad" ]; then
    fail "pubbind.baseline-digest" "the recomputed baseline digest $d_base is not what the receipts declare for:$_bad"
    finish
fi
check_build_jvm || finish

pass "pubbind.baseline-digest" "baseline input digest $(printf '%s' "$d_base" | cut -c1-16) over $(awk 'END { print NR }' "$evidence/inputs-before/libmoq-inputs.manifest") declared inputs, matching both receipts"

version=$(authority_version) || version=""
images=$(audited_images) || images=""
if [ -z "$version" ] || [ -z "$images" ]; then
    fail "pubbind.baseline-publish" "no version authority at core/include/moq/version.h, or no audited image to compare against"
    finish
fi

repo_base="$evidence/repo-baseline"
if ! gradle_publish publish-baseline "$repo_base"; then
    fail "pubbind.baseline-publish" "the baseline publication failed: $(tail -3 "$evidence/publish-baseline.log" | tr '\n' ' ')"
    finish
fi
require_ownership "the baseline publication" || finish
_d=$(clean_log "$evidence/publish-baseline.log")
if [ -n "$_d" ]; then
    fail "pubbind.baseline-publish" "the baseline publication carries a diagnostic this contract does not name: $_d"
    finish
fi
# shellcheck disable=SC2086  # the audited image list is one pair per required ABI
if ! inventory "$repo_base" "$evidence/members-baseline.txt" "$version" $images > "$evidence/inventory-baseline.log" 2>&1; then
    fail "pubbind.baseline-publish" "the baseline artifact is not the pinned coordinate carrying the audited images: $(head -1 "$evidence/inventory-baseline.log")"
    finish
fi
pass "pubbind.baseline-publish" "published $GROUP:$ARTIFACT:$version as $(awk '/^aar /{print substr($2,1,16)}' "$evidence/members-baseline.txt"), carrying exactly the two audited images"

# -- 2. change one declared native input --------------------------------------
printf '\n/* A declared-input perturbation. Comment only: no machine code changes. */\n' >> "$subject"
after=$(decl_sha256 "$subject") || after=""
if [ "$after" = "$before" ] || [ -z "$after" ]; then
    fail "pubbind.perturb" "the subject did not change"
    finish
fi
perturbed=1
pass "pubbind.perturb" "the declared input changed: $(printf '%s' "$before" | cut -c1-16) -> $(printf '%s' "$after" | cut -c1-16)"

if [ "$harness" -eq 1 ]; then
    fault_is break-probe-after-perturb && : > "${MOQ5_PUBBIND_BLIND_AFTER}"
    fault_is abort-after-perturb && exit 3
    fault_is abort-after-perturb-lost-backup && { rm -f "$backup"; exit 0; }
fi

if [ "$harness" -eq 1 ] && fault_is fixture-audit-late; then
    # Harness mechanics: the digest move is asserted so the LIVE supervision and
    # restoration ordering can be exercised with the subject genuinely perturbed.
    d_pert="44444444444444444444444444444444444444444444444444444444444444dd"
else
    d_pert=$(digest_of after) || d_pert=""
fi
if ! decl_is_sha256 "$d_pert"; then
    fail "pubbind.moved" "the perturbed input digest could not be computed"
    finish
fi
if [ "$d_pert" = "$d_base" ]; then
    fail "pubbind.moved" "the recomputed input digest did not move: the perturbation is not covered by the input inventory, so no refusal could be expected"
    finish
fi
pass "pubbind.moved" "the input digest moved: $(printf '%s' "$d_base" | cut -c1-16) -> $(printf '%s' "$d_pert" | cut -c1-16)"

# -- 3. the authoritative checker must refuse BOTH ABIs, by name --------------
expected_file "$evidence/expected-refusal.txt" "$d_base" "$d_pert" || { fail "pubbind.refusal" "cannot compose the expected refusal"; finish; }
run_audit audit-perturbed; perturbed_status=$?
require_ownership "the perturbed audit" || finish
if [ "$perturbed_status" -eq 0 ]; then
    fail "pubbind.refusal" "the topology audit still passes after a declared input changed; there is no refusal for the publication to honour"
    finish
fi
if ! named_refusal "$evidence/audit-perturbed.log" "$evidence/expected-refusal.txt"; then
    fail "pubbind.refusal" "the audit did not carry the complete refusal line for both required ABIs with the declared $d_base and recomputed $d_pert digests"
    finish
fi
_d=$(forbidden_diagnostic "$evidence/audit-perturbed.log" "$evidence/expected-refusal.txt")
if [ -n "$_d" ]; then
    fail "pubbind.refusal" "the audit's refusal is accompanied by a diagnostic this contract does not name: $_d"
    finish
fi
pass "pubbind.refusal" "both ABIs are rejected with the complete expected line and nothing else"

# -- 4. the real publication, alone, into a disjoint repository ---------------
repo_stale="$evidence/repo-stale"
gradle_publish publish-stale "$repo_stale"; status=$?
verdict=$(classify_publish "$status" "$evidence/publish-stale.log" "$repo_stale" "$evidence/expected-refusal.txt")
kind=${verdict%% *}; detail=${verdict#* }
printf '%s\n' "$verdict" > "$evidence/verdict.txt"
require_ownership "the rejected-input publication" || finish

case "$kind" in
    published)
        state=$(task_state "$evidence/publish-stale.log" ":moq5-android:verifyJniTopology")
        if [ "$state" = executed ]; then
            pass "pubbind.audited" "the publication executed the topology audit"
        else
            fail "pubbind.audited" "the publication did not execute verifyJniTopology (task state: $state); nothing bound it to the sources it ships"
        fi
        # shellcheck disable=SC2086  # the audited image list is one pair per required ABI
        if inventory "$repo_stale" "$evidence/members-stale.txt" "$version" $images > "$evidence/inventory-stale.log" 2>&1; then
            if cmp -s "$evidence/members-baseline.txt" "$evidence/members-stale.txt"; then
                fail "pubbind.published" "the publication SUCCEEDED with rejected inputs and wrote the same artifact and native bytes: $(awk '/^aar /{print substr($2,1,16)}' "$evidence/members-stale.txt")"
            else
                fail "pubbind.published" "the publication succeeded with rejected inputs and its artifact differs from the baseline, so it shipped unaudited bytes"
            fi
        else
            fail "pubbind.published" "the publication succeeded with rejected inputs but wrote no valid artifact: $(head -1 "$evidence/inventory-stale.log")"
        fi
        ;;
    wrote)         fail "pubbind.published" "$detail" ;;
    inconclusive)  fail "pubbind.inconclusive" "$detail (exit $status); this is not a satisfied contract" ;;
    refused)       pass "pubbind.published" "$detail" ;;
esac

finish
