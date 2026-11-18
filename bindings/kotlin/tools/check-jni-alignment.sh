#!/bin/sh
# Gates the 16 KiB ELF load alignment of the packaged JNI libraries.
#
# Android requires every native shared library to have LOAD segments aligned to
# at least 2**14, so a device with 16 KiB pages can map them
# (developer.android.com/guide/practices/page-sizes). NDK r28 produces that by
# default, but the ARTIFACT is gated here rather than the tool version: a
# toolchain change, an added linker flag, or a hand-built object could each
# lower it without any other oracle noticing.
#
# This is ARTIFACT evidence. Linker flags such as -Wl,-z,max-page-size=16384 are
# a RECIPE obligation and are proven separately, in the receipt.
#
# Every LOAD segment is checked, not just the first: a library whose first
# segment is aligned and whose later segment is not is exactly as unmappable.
# Both the packaged and the diagnostic image are checked, because the diagnostic
# twin is what the strip oracle reproduces the package from.
#
# Fail-closed throughout: a producer that fails, output that does not parse, an
# unreadable file, or a file with no LOAD rows at all is a named failure, never
# an empty pass.
set -u
LC_ALL=C; export LC_ALL

here=$(cd "$(dirname "$0")" && pwd) || { echo "cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)        || { echo "cannot resolve bindings/kotlin" >&2; exit 2; }
. "$here/lib-declaration.sh"

for t in awk sed grep dirname basename printf mktemp; do
    command -v "$t" >/dev/null 2>&1 || { echo "check-jni-alignment: required tool not found: $t" >&2; exit 2; }
done

# The Android floor, as a power of two, stated once.
MIN_ALIGN_SHIFT=14
MIN_ALIGN=$(( 1 << MIN_ALIGN_SHIFT ))
ABIS="arm64-v8a x86_64"
IMAGES="package diagnostic"

usage() { echo "usage: $(basename "$0") [--readelf PATH] [STAGE]" >&2; exit 2; }
readelf_override=""
stage=""
while [ $# -gt 0 ]; do
    case $1 in
        --readelf) [ $# -ge 2 ] || { echo "--readelf requires a path" >&2; usage; }
                   [ -z "$readelf_override" ] || { echo "duplicate --readelf" >&2; usage; }
                   readelf_override=$2; shift 2 ;;
        -*) echo "unknown option: $1" >&2; usage ;;
        *) [ -z "$stage" ] || { echo "more than one stage given" >&2; usage; }
           stage=$1; shift ;;
    esac
done
dep="$root/.native"
[ -n "$stage" ] || stage="$dep/_topology"

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

# The readelf that reads the artifact is the PINNED one from the toolchain the
# artifact was built with, resolved the same way the topology checker resolves
# it, so the evidence source cannot silently become a host tool.
if [ -n "$readelf_override" ]; then
    readelf=$readelf_override
else
    _cc=""
    for _a in $ABIS; do
        _p="$dep/$_a/provenance.properties"
        [ -f "$_p" ] || continue
        _n=$(props_count "$_p" ndk.cc) || continue
        [ "$_n" = "1" ] || continue
        _cc=$(props_value "$_p" ndk.cc) || continue
        [ -n "$_cc" ] && break
    done
    [ -n "$_cc" ] || { echo "check-jni-alignment: cannot resolve the pinned toolchain from $dep/*/provenance.properties" >&2; exit 2; }
    readelf="$(dirname "$_cc")/llvm-readelf"
fi
[ -x "$readelf" ] || { echo "check-jni-alignment: the pinned readelf is not executable: $readelf" >&2; exit 2; }

scratch=$(mktemp -d "${TMPDIR:-/tmp}/moq5-align-XXXXXXXX") \
    || { echo "check-jni-alignment: cannot create a task-owned scratch" >&2; exit 2; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() { rm -rf -- "$scratch"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

# gate_one <tag> <file>
gate_one() {
    _tag=$1; _f=$2
    if [ ! -f "$_f" ]; then fail "$_tag" "no such image: $_f"; return; fi
    if [ ! -r "$_f" ]; then fail "$_tag" "image is not readable: $_f"; return; fi
    if ! "$readelf" --wide -l "$_f" > "$scratch/ph" 2>"$scratch/pherr"; then
        fail "$_tag" "the program-header producer failed on $_f, so alignment is UNMEASURED -- a missing row is never an aligned row: $(sed -n '1p' "$scratch/pherr" | cut -c1-100)"
        return
    fi
    # Every LOAD row's last field is p_align, printed by llvm-readelf as hex.
    # A row whose alignment does not parse is a failure, not a skip.
    if ! awk '$1 == "LOAD" { print $NF }' "$scratch/ph" > "$scratch/al" 2>/dev/null; then
        fail "$_tag" "the program-header parser failed on $_f"
        return
    fi
    _n=$(awk 'NF { n++ } END { print n + 0 }' "$scratch/al")
    if [ "$_n" -eq 0 ]; then
        fail "$_tag" "$_f reports no LOAD segments at all; an image with nothing to map cannot be declared aligned"
        return
    fi
    _bad=""; _min=""
    while IFS= read -r _a; do
        [ -n "$_a" ] || continue
        case $_a in
            0x*|0X*) _v=$(( _a )) ;;
            [0-9]*)  _v=$(( _a )) ;;
            *) _bad="an unparseable alignment field '$_a'"; break ;;
        esac
        [ -n "$_min" ] || _min=$_v
        [ "$_v" -lt "$_min" ] && _min=$_v
        if [ "$_v" -lt "$MIN_ALIGN" ]; then
            _bad="a LOAD segment aligned to $_a ($_v), below the required 2**$MIN_ALIGN_SHIFT ($MIN_ALIGN)"
            break
        fi
    done < "$scratch/al"
    if [ -n "$_bad" ]; then
        fail "$_tag" "$_f has $_bad -- Android cannot map it on a 16 KiB page-size device"
        return
    fi
    pass "$_tag" "$_n LOAD segment(s), every alignment >= 2**$MIN_ALIGN_SHIFT (minimum observed $_min)"
}

[ -d "$stage" ] || { fail stage "no topology stage at $stage"; printf '\n16 KiB alignment gate: %s failure(s)\n' "$fails"; exit 1; }
for abi in $ABIS; do
    for img in $IMAGES; do
        gate_one "$abi.$img.alignment" "$stage/$abi/$img/libmoq5_jni.so"
    done
done

if [ "$fails" -eq 0 ]; then
    printf '\n16 KiB alignment gate: 0 failure(s)\n'
    exit 0
fi
printf '\n16 KiB alignment gate: %s failure(s)\n' "$fails"
exit 1
