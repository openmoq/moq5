#!/bin/sh
# The JNI diagnostic gate's own ledger.
#
# The gate is only as good as its vocabulary and its identity rules, and both
# have already been wrong once: it matched two phrases while the selected
# runtime emits more, and it accepted stale results beside a fresh log
# (chats/libmoq/1226). Every case below is a KILL the gate must make, or a
# near-miss it must NOT make, run against disposable fixtures.
#
# The positive forms are those the selected runtime's libjvm actually exposes;
# they are listed in check-jni-diagnostics.sh with their provenance.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
gate="$here/check-jni-diagnostics.sh"
[ -f "$gate" ] || { echo "$_self: no gate at $gate" >&2; exit 2; }

scratch=$(mktemp -d "${TMPDIR:-/tmp}/moq5-jnidiag-selftest-XXXXXXXX") \
    || { echo "$_self: cannot create a scratch" >&2; exit 2; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() { rm -rf -- "$scratch"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

kills=0
misses=0
kill_ok() { kills=$((kills + 1)); printf 'KILL[%s]: %s\n' "$1" "$2"; }
miss()    { misses=$((misses + 1)); printf 'MISS[%s]: %s\n' "$1" "$2"; }

# fixture <dir> <text> -- a disposable result set carrying <text> in a fork's
# captured output, with a matching identity so only the vocabulary is under test.
fixture() {
    _d=$1
    mkdir -p "$_d" || return 1
    printf 'moq5-run-token: selftest-token\n' > "$_d/run.log"
    printf 'selftest-token' > "$_d/token"
    mkdir -p "$_d/results" || return 1
    {
        printf '<?xml version="1.0" encoding="UTF-8"?>\n'
        printf '<testsuite name="s" tests="1" failures="0" errors="0" skipped="0">\n'
        printf '  <testcase name="p" classname="s"/>\n'
        printf '  <system-err><![CDATA[%s]]></system-err>\n' "$2"
        printf '</testsuite>\n'
    } > "$_d/results/TEST-s.xml"
}

run_gate() {
    _d=$1
    sh "$gate" "$_d/results" "$_d/token" selftest-token "$_d/run.log" > "$_d/out" 2>&1
    echo $?
}

# expect_fail <case> <text>   -- the gate must reject this output
expect_fail() {
    _c=$1
    _d="$scratch/$_c"
    fixture "$_d" "$2" || { miss "$_c" "fixture could not be built"; return; }
    if [ "$(run_gate "$_d")" -ne 0 ] && grep -q 'FAIL\[jni-diagnostics.clean\]' "$_d/out"; then
        _ev=$(sed -n 's/.*diagnostic(s) in this run.s output: //p' "$_d/out" | sed -n '1p')
        if [ -n "$_ev" ]; then
            kill_ok "$_c" "rejected, quoting: $(printf '%s' "$_ev" | cut -c1-60)"
        else
            miss "$_c" "rejected but quoted no evidence"
        fi
    else
        miss "$_c" "ACCEPTED a real checked-JNI diagnostic"
    fi
}

# expect_pass <case> <text>   -- ordinary prose the gate must NOT reject
expect_pass() {
    _c=$1
    _d="$scratch/$_c"
    fixture "$_d" "$2" || { miss "$_c" "fixture could not be built"; return; }
    if [ "$(run_gate "$_d")" -eq 0 ]; then
        kill_ok "$_c" "accepted ordinary prose"
    else
        miss "$_c" "rejected ordinary prose: $(grep '^FAIL' "$_d/out" | sed -n '1p' | cut -c1-70)"
    fi
}

echo "== the runtime's checked-JNI warning and fatal forms =="
expect_fail warning-generic \
    'WARNING in native method: JNI call made with exception pending'
expect_fail warning-unchecked \
    'WARNING in native method: JNI call made without checking exceptions when required to from CallVoidMethod'
expect_fail fatal-generic \
    'FATAL ERROR in native method: something went wrong'
expect_fail fatal-nonjava-thread \
    'FATAL ERROR in native method: Using JNIEnv in non-Java thread'
expect_fail warning-critical-scope \
    'Warning: Calling other JNI functions in the scope of Get/ReleasePrimitiveArrayCritical or Get/ReleaseStringCritical'

echo "== near misses: ordinary prose must not be mistaken for a diagnostic =="
expect_pass prose-counter 'jni-warnings=0 jni-diagnostics=0 all clean'
expect_pass prose-word    'This test warns about native methods in general.'
expect_pass prose-name    'org.example.WarningInNativeMethodTest passed'

echo "== a results directory whose path contains a space =="
_sp="$scratch/with space"
fixture "$_sp" 'WARNING in native method: JNI call made with exception pending" ' \
    || miss whitespace-path "fixture could not be built"
if [ "$(run_gate "$_sp")" -ne 0 ] && grep -q 'FAIL\[jni-diagnostics.clean\]' "$_sp/out"; then
    kill_ok whitespace-path "a result path containing a space is still read and judged"
else
    miss whitespace-path "a result path containing a space was not judged"
fi
_spc="$scratch/with space clean"
fixture "$_spc" 'nothing of interest here' || miss whitespace-path-clean "fixture failed"
if [ "$(run_gate "$_spc")" -eq 0 ]; then
    kill_ok whitespace-path-clean "a clean result path containing a space passes"
else
    miss whitespace-path-clean "a clean spaced path was rejected"
fi

echo "== identity: evidence must belong to THIS invocation =="
_id="$scratch/identity"
fixture "$_id" 'nothing of interest here'
if [ "$(sh "$gate" "$_id/results" "$_id/token" "" "$_id/run.log" >"$_id/o1" 2>&1; echo $?)" -ne 0 ] &&
    grep -q 'FAIL\[jni-diagnostics.run-identity\]' "$_id/o1"; then
    kill_ok identity-no-token "a run with no launcher identity is refused"
else
    miss identity-no-token "a run with no launcher identity was accepted"
fi
printf 'moq5-run-token: some-other-run\n' > "$_id/foreign.log"
if [ "$(sh "$gate" "$_id/results" "$_id/token" selftest-token "$_id/foreign.log" >"$_id/o2" 2>&1; echo $?)" -ne 0 ] &&
    grep -q 'FAIL\[jni-diagnostics.run-identity\]' "$_id/o2"; then
    kill_ok identity-foreign-log "a log from another invocation is refused"
else
    miss identity-foreign-log "a foreign log was accepted"
fi
printf 'an-older-run' > "$_id/stale-token"
if [ "$(sh "$gate" "$_id/results" "$_id/stale-token" selftest-token "$_id/run.log" >"$_id/o3" 2>&1; echo $?)" -ne 0 ] &&
    grep -q 'FAIL\[jni-diagnostics.run-identity\]' "$_id/o3"; then
    kill_ok identity-inherited-results "inherited results are refused"
else
    miss identity-inherited-results "inherited results were accepted"
fi
if [ "$(sh "$gate" "$_id/results" "$_id/absent-token" selftest-token "$_id/run.log" >"$_id/o4" 2>&1; echo $?)" -ne 0 ] &&
    grep -q 'FAIL\[jni-diagnostics.run-identity\]' "$_id/o4"; then
    kill_ok identity-unstamped "results with no stamped token are refused"
else
    miss identity-unstamped "unstamped results were accepted"
fi

echo "== a fork's diagnostic is still judged when ledger lines share the log =="
_mix="$scratch/mixed-log"
fixture "$_mix" 'nothing of interest here'
{
    printf 'KILL[some-case]: rejected, quoting: WARNING in native method: quoted evidence\n'
    printf 'PASS[other]: FATAL ERROR in native method: also quoted\n'
} >> "$_mix/run.log"
if [ "$(run_gate "$_mix")" -eq 0 ]; then
    kill_ok ledger-lines-ignored "this gate's own ledger lines are not read back as findings"
else
    miss ledger-lines-ignored "the gate failed on its own ledger output"
fi
printf 'WARNING in native method: a real fork wrote this\n' >> "$_mix/run.log"
if [ "$(run_gate "$_mix")" -ne 0 ] && grep -q 'FAIL\[jni-diagnostics.clean\]' "$_mix/out"; then
    kill_ok ledger-lines-not-a-loophole "a real fork line beside ledger lines is still caught"
else
    miss ledger-lines-not-a-loophole "a real fork line was missed because ledger lines were present"
fi

echo "== --recorder: the host image's native recorder reports fail a caller that expects none =="
_rec='MOQ5-TRUST-PROBE: recorder: more codec calls than the log holds'
run_gate_rec() {
    _d=$1
    sh "$gate" "$_d/results" "$_d/token" selftest-token "${2:-$_d/run.log}" --recorder > "$_d/out" 2>&1
    echo $?
}
_r="$scratch/recorder-run-log"
fixture "$_r" 'nothing of interest here'
printf '%s\n' "$_rec" >> "$_r/run.log"
if [ "$(run_gate_rec "$_r")" -ne 0 ] && grep -q 'FAIL\[jni-diagnostics.recorder\]' "$_r/out"; then
    kill_ok recorder-run-log "a recorder failure in the run log only is rejected"
else
    miss recorder-run-log "a recorder failure in the run log was accepted"
fi
_r="$scratch/recorder-captured"
fixture "$_r" "$_rec"
if [ "$(run_gate_rec "$_r")" -ne 0 ] && grep -q 'FAIL\[jni-diagnostics.recorder\]' "$_r/out"; then
    kill_ok recorder-captured "a recorder failure in a fork's captured output only is rejected"
else
    miss recorder-captured "a recorder failure in captured output was accepted"
fi
_r="$scratch/recorder-clean"
fixture "$_r" 'nothing of interest here'
if [ "$(run_gate_rec "$_r")" -eq 0 ] && grep -q 'PASS\[jni-diagnostics.recorder\]' "$_r/out"; then
    kill_ok recorder-clean "clean output passes the recorder check"
else
    miss recorder-clean "clean output was rejected by the recorder check"
fi
_r="$scratch/recorder-stale"
fixture "$_r" 'nothing of interest here'
printf 'moq5-run-token: some-other-run\n%s\n' "$_rec" > "$_r/foreign.log"
if [ "$(run_gate_rec "$_r" "$_r/foreign.log")" -ne 0 ] && grep -q 'FAIL\[jni-diagnostics.run-identity\]' "$_r/out"; then
    kill_ok recorder-stale-identity "a foreign log is still refused under --recorder"
else
    miss recorder-stale-identity "a foreign log was accepted under --recorder"
fi
_r="$scratch/recorder-ledger"
fixture "$_r" 'nothing of interest here'
printf 'KILL[recorder-run-log]: rejected, quoting: %s\n' "$_rec" >> "$_r/run.log"
if [ "$(run_gate_rec "$_r")" -eq 0 ]; then
    kill_ok recorder-ledger-lines-ignored "a ledger line quoting a recorder report is not a finding"
else
    miss recorder-ledger-lines-ignored "the recorder check failed on a ledger line"
fi
_r="$scratch/recorder-opt-in"
fixture "$_r" 'nothing of interest here'
printf '%s\n' "$_rec" >> "$_r/run.log"
if [ "$(run_gate "$_r")" -eq 0 ] && ! grep -q 'jni-diagnostics.recorder' "$_r/out"; then
    kill_ok recorder-opt-in "without --recorder the classification is unchanged for callers that assert recorder reports"
else
    miss recorder-opt-in "the recorder check ran without --recorder"
fi

printf '\njni diagnostic self-test: %s kill(s), %s miss(es)\n' "$kills" "$misses"
[ "$misses" -eq 0 ] || exit 1
exit 0
