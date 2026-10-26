#!/bin/sh
# The handle registry's behavioral oracle, in every lane it must survive.
#
# It compiles the PRODUCTION registry source together with the oracle: a model
# would prove only that the model is self-consistent. The sanitizer lanes are
# not optional decoration -- the whole point of the pin protocol is that a
# retirement cannot destroy an entry a caller still holds, and use-after-free is
# exactly what ASan sees. The thread lane covers the lease/retire interleaving,
# which is driven by barriers rather than sleeps.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)  || { echo "$_self: cannot resolve bindings/kotlin" >&2; exit 2; }

command -v cc >/dev/null 2>&1 || { echo "$_self: no cc" >&2; exit 2; }

src="$root/jni/moq5_registry.c"
oracle="$root/jni/testfixtures/moq5_registry_oracle.c"
[ -f "$src" ] || { echo "$_self: missing $src" >&2; exit 2; }
[ -f "$oracle" ] || { echo "$_self: missing $oracle" >&2; exit 2; }

scratch=$(mktemp -d "${TMPDIR:-/tmp}/moq5-regoracle-XXXXXXXX") \
    || { echo "$_self: cannot create a scratch" >&2; exit 2; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() { rm -rf -- "$scratch"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

WARN="-std=c11 -Wall -Wextra -Werror"

# lane <name> <compiler> [extra flags...]
#
# The compiler is EXPLICIT. This always invoked `cc`, so the report's claim of a
# gcc-15 warnings-as-errors build was not actually made by this gate
# (chats/libmoq/1234 F4). A missing compiler is a named failure, never a skip.
lane() {
    _name=$1
    _cc=$2
    shift 2
    if [ ! -x "$_cc" ] && ! command -v "$_cc" >/dev/null 2>&1; then
        fail "registry.$_name.compiler" "the declared compiler is not available: $_cc"
        return
    fi
    _bin="$scratch/oracle_$_name"
    # shellcheck disable=SC2086  # WARN and the sanitizer flags must split
    if ! "$_cc" $WARN "$@" -DMOQ5_REGISTRY_TESTING -o "$_bin" "$oracle" "$src" \
        > "$scratch/build.$_name" 2>&1; then
        fail "registry.$_name" "did not compile warning-free: $(sed -n '1p' "$scratch/build.$_name")"
        return
    fi
    if ASAN_OPTIONS=abort_on_error=1 UBSAN_OPTIONS=halt_on_error=1 \
        TSAN_OPTIONS=halt_on_error=1 "$_bin" > "$scratch/run.$_name" 2>&1; then
        pass "registry.$_name" "$(sed -n '$p' "$scratch/run.$_name")"
    else
        fail "registry.$_name" "$(grep -m1 '^FAIL' "$scratch/run.$_name" \
            || sed -n '$p' "$scratch/run.$_name")"
    fi
    # A sanitizer can report while the process still exits 0 in some builds, so
    # the output is inspected as well as the status.
    if grep -qE 'ERROR: |runtime error|WARNING: ThreadSanitizer' "$scratch/run.$_name"; then
        fail "registry.$_name.sanitizer" \
            "$(grep -m1 -E 'ERROR: |runtime error|WARNING: ThreadSanitizer' "$scratch/run.$_name" | cut -c1-120)"
    fi
}

_clang=$(command -v cc || echo cc)

lane clang_normal "$_clang"
lane clang_asan_ubsan "$_clang" -g -fsanitize=address,undefined
lane clang_tsan "$_clang" -g -fsanitize=thread

# gcc-15 with warnings as errors. No sanitizer lane is claimed for it, because
# none is run here.
lane gcc15_normal /opt/homebrew/bin/gcc-15

printf '\n'
if [ "$fails" -eq 0 ]; then
    printf 'registry oracle: 0 failure(s)\n'
    exit 0
fi
printf 'registry oracle: %s failure(s)\n' "$fails"
exit 1
