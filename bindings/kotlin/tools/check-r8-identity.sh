#!/bin/sh
# The bridge's registration identity must survive a consuming app's R8 pass.
#
# Static evidence from a real minified release build: the test-only fixture
# application consumes the library's OWN packaged consumer rules (AGP applies an
# AAR's rules automatically; nothing here copies them), and this gate reads the
# resulting DEX with dexdump plus R8's mapping file.
#
# SCOPE: this proves the identity the native side looks up still exists in the
# minified artefact. It is NOT a device execution claim -- actually loading the
# library and running JNI_OnLoad on a device remains held under Phase 0.4C.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)  || { echo "$_self: cannot resolve bindings/kotlin" >&2; exit 2; }

for t in grep sed awk unzip find mktemp rm printf; do
    command -v "$t" >/dev/null 2>&1 \
        || { echo "$_self: required tool not found: $t" >&2; exit 2; }
done

scratch=$(mktemp -d "${TMPDIR:-/tmp}/moq5-r8-XXXXXXXX") \
    || { echo "$_self: cannot create a task-owned scratch" >&2; exit 2; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() { rm -rf -- "$scratch"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

abi_h="$root/jni/moq5_abi.h"
apk=$(find "$root/bridge-r8-fixture/build/outputs/apk/release" -name '*.apk' 2>/dev/null | sed -n '1p')
mapping="$root/bridge-r8-fixture/build/outputs/mapping/release/mapping.txt"
[ -f "$abi_h" ] || { echo "$_self: missing the authority $abi_h" >&2; exit 2; }
[ -n "$apk" ] || { echo "$_self: no minified APK; run :bridge-r8-fixture:assembleRelease" >&2; exit 2; }
[ -f "$mapping" ] || { echo "$_self: no R8 mapping at $mapping" >&2; exit 2; }

# The pinned build-tools' dexdump, discovered from the SDK the toolchain names.
sdk=${ANDROID_HOME:-${ANDROID_SDK_ROOT:-}}
if [ -z "$sdk" ] && [ -f "$root/local.properties" ]; then
    sdk=$(sed -n 's/^sdk\.dir=\(.*\)$/\1/p' "$root/local.properties" | sed -n '1p')
fi
bt=$(sed -n 's/^sdk\.buildTools=\(.*\)$/\1/p' "$root/gradle/toolchain.properties")
dexdump="$sdk/build-tools/$bt/dexdump"
[ -x "$dexdump" ] || { echo "$_self: no dexdump at $dexdump" >&2; exit 2; }

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

class=$(sed -n 's/^#define MOQ5_BRIDGE_CLASS "\(.*\)"$/\1/p' "$abi_h")
[ -n "$class" ] || { echo "$_self: the authority names no bound class" >&2; exit 2; }
dotted=$(printf '%s' "$class" | tr '/' '.')
want_static=$(sed -n 's/^#define MOQ5_BRIDGE_METHOD_STATIC \(.*\)$/\1/p' "$abi_h")

# An APK with no DEX at all is a RESULT, not a tooling failure: it is what R8
# emits when nothing is kept. Report it as the named class failure rather than
# exiting on it, so a stripped keep rule is attributable instead of looking like
# a broken environment. A dex that IS present but cannot be extracted stays a
# hard error.
if unzip -Z1 "$apk" 2>/dev/null | grep -q '^classes.*\.dex$'; then
    unzip -o -q "$apk" 'classes*.dex' -d "$scratch" \
        || { echo "$_self: cannot extract dex from $apk" >&2; exit 2; }
else
    fail r8.class "the minified APK contains no DEX at all -- R8 kept nothing, so L$class; is gone with it"
    printf '\nR8 bridge identity: %s failure(s)\n' "$fails"
    exit 1
fi

: > "$scratch/block"
for d in "$scratch"/classes*.dex; do
    "$dexdump" -d "$d" 2>/dev/null |
        awk -v c="Class descriptor  : 'L$class;'" '
            index($0, c) { inb = 1 }
            inb { print }
            inb && index($0, "source_file_idx") { inb = 0 }
        ' >> "$scratch/block"
done

if [ ! -s "$scratch/block" ]; then
    fail r8.class "the minified APK contains no class L$class; -- R8 removed or renamed the bound class"
    printf '\nR8 bridge identity: %s failure(s)\n' "$fails"
    exit 1
fi
pass r8.class "the minified APK still contains L$class;"

# R8 must not have renamed it: the mapping's left and right sides must agree.
if grep -q "^$dotted -> $dotted:" "$mapping"; then
    pass r8.mapping "the R8 mapping keeps $dotted unrenamed"
else
    _to=$(grep "^$dotted -> " "$mapping" | sed -n '1s/.*-> //p')
    fail r8.mapping "R8 renamed $dotted to '${_to:-<absent from mapping>}'"
fi

# Every authority row must survive with its exact name, descriptor and shape.
sed -n 's/^ *X(\([A-Za-z0-9_]*\), *"\([^"]*\)").*$/\1 \2/p' "$abi_h" > "$scratch/authority"
rows=$(grep -c . "$scratch/authority")
missing=""
badshape=""
while read -r m d; do
    [ -n "$m" ] || continue
    # Literal matching throughout: a method descriptor such as "()I" is not a
    # regular expression, and treating it as one silently matches the wrong row.
    _acc=$(awk -v n="name          : '$m'" -v t="type          : '$d'" '
        index($0, n) { seen = 1; next }
        seen && index($0, t) { want = 1; next }
        seen && index($0, "type          :") { seen = 0; next }
        want && index($0, "access        :") { print; want = 0; seen = 0 }
    ' "$scratch/block" | sed -n '1p')
    if [ -z "$_acc" ]; then
        missing="$missing $m$d"
        continue
    fi
    # The EXACT flag set dexdump prints, not a NATIVE membership test plus a
    # STATIC probe. The consumer rule names `public final native`, so that is
    # what the minified artefact is held to (chats/libmoq/1212 F3).
    _flags=$(printf '%s' "$_acc" | sed -n 's/.*(\(.*\)).*/\1/p')
    if [ "$want_static" = "0" ]; then
        _want_flags="PUBLIC FINAL NATIVE"
    else
        _want_flags="PUBLIC STATIC FINAL NATIVE"
    fi
    [ "$_flags" = "$_want_flags" ] || badshape="$badshape ${m}[$_flags]"
done < "$scratch/authority"

if [ -n "$missing" ]; then
    fail r8.methods "the minified APK lost:$missing"
else
    pass r8.methods "all $rows registered method(s) survive with their exact descriptors"
fi

# The trusted-create contract row (chats/libmoq/1365), named DIRECTLY rather
# than through the authority: the loop above is silent while the row is absent
# from the authority, and the minified artefact must carry it in the exact
# instance shape once it lands.
_tr_name=nativeEndpointCreateTrusted
_tr_desc='(Ljava/lang/String;II[IJILjava/lang/String;[J)I'
_tr_acc=$(awk -v n="name          : '$_tr_name'" -v t="type          : '$_tr_desc'" '
    index($0, n) { seen = 1; next }
    seen && index($0, t) { want = 1; next }
    seen && index($0, "type          :") { seen = 0; next }
    want && index($0, "access        :") { print; want = 0; seen = 0 }
' "$scratch/block" | sed -n '1p')
if [ -z "$_tr_acc" ]; then
    fail r8.trusted.row "the minified APK carries no $_tr_name$_tr_desc; the trusted-create row is absent after R8"
else
    _tr_flags=$(printf '%s' "$_tr_acc" | sed -n 's/.*(\(.*\)).*/\1/p')
    if [ "$_tr_flags" = "PUBLIC FINAL NATIVE" ]; then
        pass r8.trusted.row "$_tr_name$_tr_desc survives R8 as (PUBLIC FINAL NATIVE)"
    else
        fail r8.trusted.row "$_tr_name survives R8 as ($_tr_flags), expected (PUBLIC FINAL NATIVE)"
    fi
fi
if [ -n "$badshape" ]; then
    fail r8.methods.shape "after R8, methods must be exactly ($_want_flags):$badshape"
else
    pass r8.methods.shape "every surviving method is exactly ($_want_flags)"
fi

# The Kotlin object's INSTANCE field, which an instance call goes through and
# which the shipped consumer rule keeps by name and type.
if [ "$want_static" = "0" ]; then
    _ifld=$(awk '
        index($0, "name          : '\''INSTANCE'\''") { seen = 1; next }
        seen && index($0, "type          :") { t = $0; sub(/.*: /, "", t); next }
        seen && index($0, "access        :") {
            a = $0; sub(/.*\(/, "", a); sub(/\).*/, "", a); print a "|" t; exit }
    ' "$scratch/block")
    if [ "$_ifld" = "PUBLIC STATIC FINAL|'L$class;'" ]; then
        pass r8.instance-field "INSTANCE survives as PUBLIC STATIC FINAL L$class;"
    else
        fail r8.instance-field \
            "the singleton's INSTANCE field is '$_ifld', expected \"PUBLIC STATIC FINAL|'L$class;'\""
    fi
fi

# -- the SECOND native lookup: the library-owned trust bridge -----------------
# JNI_OnLoad resolves a second class by string -- the trust bridge, its
# INSTANCE field and its checkServerTrusted method -- so a consumer's R8 pass
# must preserve exactly that shape too. The authority is the product
# translation unit's own #defines, read here rather than restated.
jni_c="$root/jni/moq5_jni.c"
tclass=$(sed -n 's/^#define MOQ5_TRUST_CLASS  *"\(.*\)"$/\1/p' "$jni_c" | sed -n '1p')
tfield=$(sed -n 's/^#define MOQ5_TRUST_INSTANCE  *"\(.*\)"$/\1/p' "$jni_c" | sed -n '1p')
tfsig=$(sed -n 's/^#define MOQ5_TRUST_INSTANCE_SIG *"\(.*\)"$/\1/p' "$jni_c" | sed -n '1p')
tmethod=$(sed -n 's/^#define MOQ5_TRUST_METHOD  *"\(.*\)"$/\1/p' "$jni_c" | sed -n '1p')
tdesc=$(sed -n 's/^#define MOQ5_TRUST_DESCRIPTOR  *"\(.*\)"$/\1/p' "$jni_c" | sed -n '1p')
if [ -z "$tclass" ] || [ -z "$tfield" ] || [ -z "$tfsig" ] || [ -z "$tmethod" ] || [ -z "$tdesc" ]; then
    fail r8.trust.authority "moq5_jni.c does not declare the trust bridge lookup (class, INSTANCE, signature, method, descriptor) as single #defines"
else
    tdotted=$(printf '%s' "$tclass" | tr '/' '.')
    : > "$scratch/tblock"
    for d in "$scratch"/classes*.dex; do
        "$dexdump" -d "$d" 2>/dev/null |
            awk -v c="Class descriptor  : 'L$tclass;'" '
                index($0, c) { inb = 1 }
                inb { print }
                inb && index($0, "source_file_idx") { inb = 0 }
            ' >> "$scratch/tblock"
    done
    if [ ! -s "$scratch/tblock" ]; then
        fail r8.trust.class "the minified APK contains no class L$tclass; -- R8 removed or renamed the trust bridge JNI_OnLoad resolves"
    else
        pass r8.trust.class "the minified APK still contains L$tclass;"
        if grep -q "^$tdotted -> $tdotted:" "$mapping"; then
            pass r8.trust.mapping "the R8 mapping keeps $tdotted unrenamed"
        else
            _tto=$(grep "^$tdotted -> " "$mapping" | sed -n '1s/.*-> //p')
            fail r8.trust.mapping "R8 renamed $tdotted to '${_tto:-<absent from mapping>}'"
        fi
        _tfld=$(awk -v n="name          : '$tfield'" '
            index($0, n) { seen = 1; next }
            seen && index($0, "type          :") { t = $0; sub(/.*: /, "", t); next }
            seen && index($0, "access        :") {
                a = $0; sub(/.*\(/, "", a); sub(/\).*/, "", a); print a "|" t; exit }
        ' "$scratch/tblock")
        if [ "$_tfld" = "PUBLIC STATIC FINAL|'$tfsig'" ]; then
            pass r8.trust.instance-field "$tfield survives as PUBLIC STATIC FINAL $tfsig"
        else
            fail r8.trust.instance-field "the trust bridge's $tfield field is '$_tfld', expected \"PUBLIC STATIC FINAL|'$tfsig'\""
        fi
        _tacc=$(awk -v n="name          : '$tmethod'" -v t="type          : '$tdesc'" '
            index($0, n) { seen = 1; next }
            seen && index($0, t) { want = 1; next }
            seen && index($0, "type          :") { seen = 0; next }
            want && index($0, "access        :") { print; want = 0; seen = 0 }
        ' "$scratch/tblock" | sed -n '1p')
        if [ -z "$_tacc" ]; then
            fail r8.trust.method "the minified APK carries no $tmethod$tdesc on L$tclass;"
        else
            _tflags=$(printf '%s' "$_tacc" | sed -n 's/.*(\(.*\)).*/\1/p')
            if [ "$_tflags" = "PUBLIC FINAL" ]; then
                pass r8.trust.method "$tmethod$tdesc survives R8 as (PUBLIC FINAL)"
            else
                fail r8.trust.method "$tmethod survives R8 as ($_tflags), expected (PUBLIC FINAL): an instance method the C side resolves with GetMethodID"
            fi
        fi
    fi
fi

printf '\n'
if [ "$fails" -eq 0 ]; then
    printf 'R8 bridge identity: 0 failure(s) (static evidence; device load remains held)\n'
    exit 0
fi
printf 'R8 bridge identity: %s failure(s)\n' "$fails"
exit 1
