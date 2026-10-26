#!/bin/sh
# The registry's stack frames must stay small on every compiler that builds it.
#
# The first teardown built fixed-capacity inventories on the stack and cost a
# ~98 KiB frame: a routine endpoint close must not reserve roughly 100 KiB
# merely because the registry's maximum is 4096 (chats/libmoq/1232 F3).
#
# The gate carries a POSITIVE CONTROL. A flag a compiler silently ignores would
# make this pass by absence, so a deliberately oversized function is compiled
# first and MUST be rejected; only then is the registry's own result trusted.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)  || { echo "$_self: cannot resolve bindings/kotlin" >&2; exit 2; }
repo=$(cd "$root/../.." && pwd) || { echo "$_self: cannot resolve the worktree root" >&2; exit 2; }

MAX_FRAME=4096
src="$root/jni/moq5_registry.c"
[ -f "$src" ] || { echo "$_self: missing $src" >&2; exit 2; }

scratch=$(mktemp -d "${TMPDIR:-/tmp}/moq5-frame-XXXXXXXX") || { echo "$_self: no scratch" >&2; exit 2; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() { rm -rf -- "$scratch"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

# A function whose frame is unmistakably over the limit.
# The buffer's address ESCAPES through an external call, so the frame cannot be
# optimized away. Two earlier forms were wrong: a `volatile` array was elided at
# -O1 (the control then "passed" while proving nothing), and storing the address
# in a global is itself diagnosable -- GCC reports -Wdangling-pointer, which
# would make the control fail for an unrelated reason under the project's
# warning policy. An undefined external is fine here: these are -c compiles.
cat > "$scratch/oversized.c" <<'EOC'
#include <string.h>
void moq5_frame_use(char *p);
int moq5_frame_positive_control(void);
int moq5_frame_positive_control(void)
{
    char big[8192];
    memset(big, 1, sizeof big);
    moq5_frame_use(big);
    return (int)big[0] + (int)big[sizeof big - 1];
}
EOC

# The diagnostic each compiler emits for an oversized frame names the function.
# Requiring that NAME is what makes the control causal: a syntax error or any
# unrelated diagnostic would otherwise be accepted as proof that the flag fired
# (chats/libmoq/1234 F3).
CONTROL_FN=moq5_frame_positive_control

# classify_control_log <log> <function>
#
# The ONE classifier used by the real controls and by this gate's own
# self-tests. A presence test -- "a frame phrase somewhere and the name
# somewhere" -- accepted a log that carried the expected diagnostic PLUS an
# unrelated warning or error (chats/libmoq/1236 F3). This instead requires the
# expected diagnostic AND rejects any other warning/error class.
#
# Echoes one of: ok | no-frame-diag | wrong-function | mixed:<line>
classify_control_log() {
    _log=$1
    _fn=$2

    if ! grep -qE "frame size|frame larger" "$_log"; then
        printf 'no-frame-diag'
        return
    fi
    if ! grep -q "$_fn" "$_log"; then
        printf 'wrong-function'
        return
    fi

    # Every diagnostic LINE is examined. Frame diagnostics are expected; any
    # other warning: or error: line is an unrelated diagnostic and disqualifies
    # the control. Compiler context is preserved: GCC's "In function '...':",
    # source echoes, carets and note: lines are not diagnostics of their own.
    _other=$(grep -E '(warning|error|fatal error):' "$_log" \
        | grep -vE "frame size|frame larger" \
        | grep -vE '^[^:]*: (In function|In file included from)' \
        | sed -n '1p')
    if [ -n "$_other" ]; then
        printf 'mixed:%s' "$(printf '%s' "$_other" | cut -c1-90)"
        return
    fi
    printf 'ok'
}

# lane <name> <mode-label> <opt-flags> <compiler> [extra flags...]
lane() {
    _name=$1
    _mode=$2
    _opt=$3
    _cc=$4
    shift 4
    if [ ! -x "$_cc" ] && ! command -v "$_cc" >/dev/null 2>&1; then
        fail "frame.$_name.compiler" "the declared compiler is not available: $_cc"
        return
    fi

    # CONTROL 1: an oversized frame must be rejected, AND the diagnostic must
    # name the control function.
    # shellcheck disable=SC2086  # _opt must split into separate arguments
    if "$_cc" "$@" -std=c11 $_opt -c "-Wframe-larger-than=$MAX_FRAME" -Werror -Wall -Wextra \
        -o "$scratch/ctl.$_name.o" "$scratch/oversized.c" > "$scratch/ctl.$_name.log" 2>&1; then
        fail "frame.$_name.control" \
            "an 8 KiB frame was ACCEPTED in $_mode, so -Wframe-larger-than is not in force"
        return
    fi
    _verdict=$(classify_control_log "$scratch/ctl.$_name.log" "$CONTROL_FN")
    if [ "$_verdict" != "ok" ]; then
        fail "frame.$_name.control" "the control did not fail cleanly by its frame: $_verdict"
        return
    fi
    pass "frame.$_name.control" "an oversized frame is rejected by name in $_mode"

    # shellcheck disable=SC2086
    if "$_cc" "$@" -std=c11 $_opt -c "-Wframe-larger-than=$MAX_FRAME" -Werror -Wall -Wextra \
        -o "$scratch/reg.$_name.o" "$src" > "$scratch/reg.$_name.log" 2>&1; then
        pass "frame.$_name" "every registry frame is within $MAX_FRAME bytes in $_mode"
    else
        fail "frame.$_name" "$(grep -m1 'frame size' "$scratch/reg.$_name.log" \
            || sed -n '1p' "$scratch/reg.$_name.log")"
    fi
}

# -- the gate's own self-tests ------------------------------------------------
# A control that fails for an UNRELATED reason must not be read as the flag
# firing, and the oversized control must pass when the flag is absent.
selftest() {
    _cc=$(command -v cc) || { fail frame.selftest.compiler "no cc for the self-test"; return; }

    printf 'int broken(void) { return ; }\n' > "$scratch/broken.c"
    "$_cc" -std=c11 -O0 -c "-Wframe-larger-than=$MAX_FRAME" -Werror -Wall -Wextra \
        -o "$scratch/broken.o" "$scratch/broken.c" > "$scratch/broken.log" 2>&1
    _v=$(classify_control_log "$scratch/broken.log" "$CONTROL_FN")
    if [ "$_v" = "ok" ]; then
        fail frame.selftest.unrelated "an unrelated compile error was accepted as a frame diagnostic"
    else
        pass frame.selftest.unrelated "an unrelated error is refused ($_v)"
    fi

    if "$_cc" -std=c11 -O0 -c -Werror -Wall -Wextra -o "$scratch/noflag.o" "$scratch/oversized.c" \
        > "$scratch/noflag.log" 2>&1; then
        pass frame.selftest.no-flag "without the flag the oversized frame compiles, as expected"
    else
        fail frame.selftest.no-flag \
            "the oversized control failed even WITHOUT the frame flag, so the control is not specific"
    fi

    # MIXED: the expected frame diagnostic AND an unrelated warning in one log.
    # A presence-only matcher accepts this; the classifier must not.
    cat > "$scratch/mixed.c" <<'EOM'
#include <string.h>
void moq5_frame_use(char *p);
int moq5_frame_positive_control(void);
int moq5_frame_positive_control(void)
{
    char big[8192];
    int unused_variable_here = 1;
    memset(big, 1, sizeof big);
    moq5_frame_use(big);
    return (int)big[0];
}
EOM
    # Deliberately WITHOUT -Werror: under it the unrelated diagnostic becomes an
    # error and compilation stops before codegen emits the frame diagnostic, so
    # the log would never contain both. The classifier's job is to refuse a log
    # carrying an unrelated diagnostic whatever its severity.
    "$_cc" -std=c11 -O0 -c "-Wframe-larger-than=$MAX_FRAME" -Wall -Wextra \
        -o "$scratch/mixed.o" "$scratch/mixed.c" > "$scratch/mixed.log" 2>&1
    _v=$(classify_control_log "$scratch/mixed.log" "$CONTROL_FN")
    case $_v in
        mixed:*) pass frame.selftest.mixed "a log carrying an unrelated diagnostic is refused" ;;
        ok)      fail frame.selftest.mixed \
                     "a mixed log was ACCEPTED; the classifier is presence-only" ;;
        *)       fail frame.selftest.mixed "the mixed probe did not produce a frame diagnostic: $_v" ;;
    esac
}

selftest

# The PRODUCTION mode first: build-jni-topology.sh compiles the registry object
# with no optimization flag, and the host oracle is likewise unoptimized. An
# optimized lane is added as well, but may not stand in for the real mode.
lane clang "unoptimized (production mode)" "" "$(command -v cc || echo cc)"
lane clang_o1 "-O1" "-O1" "$(command -v cc || echo cc)"
lane gcc15 "unoptimized (production mode)" "" /opt/homebrew/bin/gcc-15
lane gcc15_o1 "-O1" "-O1" /opt/homebrew/bin/gcc-15

# Both Android ABIs, using the pinned NDK compilers the topology stage declares.
dep="$root/.native"
for abi in arm64-v8a x86_64; do
    prov="$dep/$abi/provenance.properties"
    if [ ! -f "$prov" ]; then
        fail "frame.$abi.provenance" "no Phase 0.2 provenance at $prov"
        continue
    fi
    ndkcc=$(sed -n 's/^ndk\.cc=//p' "$prov" | sed -n '1p')
    if [ -z "$ndkcc" ] || [ ! -x "$ndkcc" ]; then
        fail "frame.$abi.compiler" "the declared NDK compiler is unusable: ${ndkcc:-<unset>}"
        continue
    fi
    lane "$abi" "unoptimized (production mode)" "" "$ndkcc"
    lane "$abi-O1" "-O1" "-O1" "$ndkcc"
done
: "$repo"

printf '\n'
if [ "$fails" -eq 0 ]; then
    printf 'registry frames: 0 failure(s)\n'
    exit 0
fi
printf 'registry frames: %s failure(s)\n' "$fails"
exit 1
