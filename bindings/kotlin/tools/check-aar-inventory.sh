#!/bin/sh
# The shipped AARs must not contain test-only classes.
#
# Kotlin `internal` is not JVM encapsulation: an `internal object` compiles to a
# public final class with public methods, and one that sits in main source ships
# inside the AAR whatever the Kotlin visibility says (chats/libmoq/1232 F4). So
# the assertion is made against the ACTUAL archives, by name and by method.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)  || { echo "$_self: cannot resolve bindings/kotlin" >&2; exit 2; }

for t in unzip find grep printf; do
    command -v "$t" >/dev/null 2>&1 || { echo "$_self: required tool not found: $t" >&2; exit 2; }
done
javap=""
if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/javap" ]; then
    javap="$JAVA_HOME/bin/javap"
elif command -v javap >/dev/null 2>&1; then
    javap=$(command -v javap)
fi

scratch=$(mktemp -d "${TMPDIR:-/tmp}/moq5-aar-XXXXXXXX") || { echo "$_self: no scratch" >&2; exit 2; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() { rm -rf -- "$scratch"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

# Classes that must NEVER appear in a shipped archive, with the methods that
# would prove a stale copy slipped through.
FORBIDDEN_CLASS="org/openmoq/moq5/jni/RegistryRoundTripBridge"
FORBIDDEN_METHODS="mintToken resolves echo retire"

# The EXACT archives expected, so a run that happened to build only one -- or
# none -- cannot report a clean inventory (chats/libmoq/1234 F6).
EXPECTED_AARS="moq5-android-debug.aar moq5-android-release.aar"
_missing=""
for want in $EXPECTED_AARS; do
    [ -f "$root/moq5-android/build/outputs/aar/$want" ] || _missing="$_missing $want"
done
if [ -n "$_missing" ]; then
    fail aar.expected-set "expected archive(s) not built:$_missing"
else
    pass aar.expected-set "both expected archives are present"
fi

_seen=0
for aar in "$root"/moq5-android/build/outputs/aar/*.aar; do
    [ -f "$aar" ] || continue
    _seen=$((_seen + 1))
    _name=$(basename "$aar")
    _d="$scratch/$_name.d"
    mkdir -p "$_d"
    unzip -o -q "$aar" -d "$_d" || { fail aar.extract "cannot extract $_name"; continue; }

    # classes.jar carries the compiled Kotlin.
    if [ ! -f "$_d/classes.jar" ]; then
        fail aar.classes "$_name has no classes.jar"
        continue
    fi
    unzip -o -q "$_d/classes.jar" -d "$_d/classes" || { fail aar.classes "cannot extract classes.jar of $_name"; continue; }

    if [ -f "$_d/classes/$FORBIDDEN_CLASS.class" ]; then
        fail aar.test-class-absent "$_name ships the test-only class $FORBIDDEN_CLASS"
        if [ -n "$javap" ]; then
            for m in $FORBIDDEN_METHODS; do
                if "$javap" -p "$_d/classes/$FORBIDDEN_CLASS.class" 2>/dev/null | grep -q "$m"; then
                    fail aar.test-method-absent "$_name ships $FORBIDDEN_CLASS.$m"
                fi
            done
        fi
    else
        pass aar.test-class-absent "$_name does not ship $FORBIDDEN_CLASS"
    fi
done

_want_count=0
for want in $EXPECTED_AARS; do _want_count=$((_want_count + 1)); done
if [ "$_seen" -ne "$_want_count" ]; then
    fail aar.built "inspected $_seen archive(s), expected exactly $_want_count"
else
    pass aar.built "inspected exactly $_seen expected archive(s)"
fi

printf '\n'
if [ "$fails" -eq 0 ]; then
    printf 'aar inventory: 0 failure(s)\n'
    exit 0
fi
printf 'aar inventory: %s failure(s)\n' "$fails"
exit 1
