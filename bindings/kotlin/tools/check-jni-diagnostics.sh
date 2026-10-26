#!/bin/sh
# No checked-JNI diagnostic may survive a JNI test run, and the evidence must
# belong to THIS invocation.
#
# usage: check-jni-diagnostics.sh RESULTS-DIR TOKEN-FILE RUN-TOKEN RUN-LOG [--recorder]
#
# Two independent failure modes are guarded, both of which were measured as
# false passes before this revision (chats/libmoq/1226):
#
#  1. IDENTITY. A fixed log path let a launcher that died before scheduling any
#     test rewrite the log while stale JUnit XML from an earlier run remained --
#     and that combination read as a clean run. So the launcher's token, the
#     token stamped in the log's first line, and the token the JNI producer
#     stamped beside its results must all be the same string. There is no
#     fallback path and no "missing means skip": a run with no launcher identity
#     fails closed.
#
#  2. VOCABULARY. The classifier previously matched only two phrases. The
#     selected runtime's libjvm exposes more, including a checked-JNI warning
#     that does not contain "in native method" at all. The forms below are
#     derived from that library rather than guessed:
#
#       WARNING in native method: %s
#       WARNING in native method: JNI call made without checking exceptions ...
#       FATAL ERROR in native method: %s
#       FATAL ERROR in native method: Using JNIEnv in non-Java thread
#       Warning: Calling other JNI functions in the scope of
#           Get/ReleasePrimitiveArrayCritical or Get/ReleaseStringCritical
#
#     The patterns are anchored on those literal prefixes, so ordinary prose such
#     as "jni-warnings=0" is not mistaken for a diagnostic, and no real
#     diagnostic is exempted.
#
#  3. RECORDER (only with --recorder). The host bridge image carries a native
#     recorder (jni/testfixtures/moq5_trust_route_probe.c) whose every report
#     reaches stderr through one emitter prefix, "MOQ5-TRUST-PROBE: ": its
#     latched instrumentation failures (moq5_latch -> moq5_probe_say, e.g.
#     "recorder: more codec calls than the log holds") and its two direct
#     fprintf reports. Those are native, not JVM, output, so the checked-JNI
#     vocabulary never sees them, and a run whose recorder lost part of its
#     inventory still exits 0. A caller that expects NO recorder output passes
#     --recorder and any such line fails the run. It is opt-in because other
#     lanes deliberately assert recorder reports (the library's
#     TrustBindUnwindTest and TrustedCreateTest require "load refused" lines);
#     without the flag this gate's classification is exactly as before.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
for t in find grep printf sed head; do
    command -v "$t" >/dev/null 2>&1 \
        || { echo "$_self: required tool not found: $t" >&2; exit 2; }
done

usage() {
    echo "usage: $_self RESULTS-DIR TOKEN-FILE RUN-TOKEN RUN-LOG" >&2
    exit 2
}
[ $# -eq 4 ] || [ $# -eq 5 ] || usage
results=$1
token_file=$2
run_token=$3
run_log=$4
recorder=0
if [ $# -eq 5 ]; then
    [ "$5" = "--recorder" ] || usage
    recorder=1
fi

scratch=$(mktemp -d "${TMPDIR:-/tmp}/moq5-jnidiag-XXXXXXXX") \
    || { echo "$_self: cannot create a task-owned scratch" >&2; exit 2; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() { rm -rf -- "$scratch"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

# -- identity ------------------------------------------------------------------
_identity_ok=0
if [ -z "$run_token" ]; then
    fail jni-diagnostics.run-identity "this Gradle run carries no launcher run token; \
the forks' own output is observable only through tools/gradlew-verified, so a run \
started outside it cannot report a clean result"
elif [ -z "$run_log" ] || [ ! -f "$run_log" ]; then
    fail jni-diagnostics.run-identity "no run log for token $run_token at '${run_log:-<unset>}'"
elif [ ! -f "$token_file" ]; then
    fail jni-diagnostics.run-identity "the JNI producer stamped no run token at $token_file; \
its results cannot be shown to belong to this invocation"
else
    _log_token=$(sed -n '1s/^moq5-run-token: //p' "$run_log")
    _res_token=$(head -n 1 "$token_file")
    if [ "$_log_token" != "$run_token" ]; then
        fail jni-diagnostics.run-identity "the run log at $run_log carries token \
'${_log_token:-<none>}', not this invocation's '$run_token'; it is a stale or foreign log"
    elif [ "$_res_token" != "$run_token" ]; then
        fail jni-diagnostics.run-identity "the JNI results were produced under token \
'${_res_token:-<none>}', not this invocation's '$run_token'; they are inherited, not fresh"
    else
        _identity_ok=1
        pass jni-diagnostics.run-identity "launcher, run log and JNI results all carry token $run_token"
    fi
fi

# -- the evidence itself -------------------------------------------------------
if [ "$_identity_ok" -eq 1 ]; then
    if [ ! -d "$results" ]; then
        fail jni-diagnostics.results "no results directory at $results"
    else
        # A NUL-free manifest, one path per line, so paths containing spaces
        # survive: `for f in $(find ...)` splits them into nonexistent pieces.
        find "$results" -name 'TEST-*.xml' -print > "$scratch/manifest" 2>/dev/null
        _n=$(grep -c . "$scratch/manifest" 2>/dev/null) || _n=0
        if [ "$_n" -eq 0 ]; then
            fail jni-diagnostics.results "no JUnit result files under $results"
        else
            : > "$scratch/hits"
            _re='WARNING in native method|FATAL ERROR in native method|Warning: Calling other JNI functions in the scope of'
            while IFS= read -r f; do
                [ -n "$f" ] || continue
                grep -hoE "($_re)[^<]*" "$f" >> "$scratch/hits" 2>/dev/null
            done < "$scratch/manifest"
            # The run log carries the WHOLE invocation, including this gate's
            # own ledger lines and the self-test's, which quote diagnostic text
            # as evidence. Those are OUR reporting, not fork output, and reading
            # them back as findings would make the gate fail on its own
            # correctness proof. Only the ledger PREFIXES are excluded -- no
            # diagnostic form is exempted, and a fork line is still judged even
            # when ledger lines sit beside it in the same log.
            grep -vE '^(KILL|MISS|PASS|FAIL)\[' "$run_log" \
                | grep -hoE "($_re)[^<]*" >> "$scratch/hits" 2>/dev/null

            if [ -s "$scratch/hits" ]; then
                fail jni-diagnostics.clean "checked-JNI diagnostic(s) in this run's output: \
$(sed -n '1p' "$scratch/hits" | sed 's/]]\>*$//' | cut -c1-200)"
            else
                pass jni-diagnostics.clean "no checked-JNI diagnostic in $_n result file(s) \
or the run log of token $run_token"
            fi

            if [ "$recorder" -eq 1 ]; then
                # The recorder's one emitter prefix, in any fork's captured
                # output or the run log, with the same ledger-prefix exclusion.
                : > "$scratch/recorder"
                _rre='MOQ5-TRUST-PROBE: '
                while IFS= read -r f; do
                    [ -n "$f" ] || continue
                    grep -hoE "$_rre[^<]*" "$f" >> "$scratch/recorder" 2>/dev/null
                done < "$scratch/manifest"
                grep -vE '^(KILL|MISS|PASS|FAIL)\[' "$run_log" \
                    | grep -hoE "$_rre[^<]*" >> "$scratch/recorder" 2>/dev/null
                if [ -s "$scratch/recorder" ]; then
                    fail jni-diagnostics.recorder "$(grep -c . "$scratch/recorder") native recorder \
report(s) in this run's output: $(sed -n '1p' "$scratch/recorder" | sed 's/]]\>*$//' | cut -c1-200)"
                else
                    pass jni-diagnostics.recorder "no native recorder report in $_n result file(s) \
or the run log of token $run_token"
                fi
            fi
        fi
    fi
fi

printf '\n'
if [ "$fails" -eq 0 ]; then
    printf 'jni diagnostics: 0 failure(s)\n'
    exit 0
fi
printf 'jni diagnostics: %s failure(s)\n' "$fails"
exit 1
