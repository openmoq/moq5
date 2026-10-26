#!/bin/sh
# Gates the PACKAGE-level native-library contract for a zip-shaped Android
# artifact: 16 KiB page alignment, uncompressed native libraries, and the
# one-DSO closure preserved at package level.
#
# Three rules, and the second is the one that makes this more than a zipalign
# wrapper:
#
#  1. `zipalign -c -P 16 4` must accept the archive. This is the Android
#     page-alignment check (developer.android.com/tools/zipalign), run with the
#     PINNED build-tools binary rather than whatever is first on PATH.
#
#  2. Every lib/<abi>/*.so entry must be STORED, not deflated. zipalign reports
#     a compressed .so as "OK - compressed" and exits 0, because a compressed
#     entry has no alignment to check -- so zipalign alone cannot enforce this.
#     A compressed native library cannot be mapped directly out of the archive,
#     which is precisely what 16 KiB page-size support requires, so it is
#     refused by name here.
#
#  3. The native payload is exactly one DSO per ABI, named libmoq5_jni.so. The
#     one-DSO closure is a topology contract; it is re-checked at package level
#     because a second native library could be introduced by packaging alone,
#     long after the topology audit passed.
#
# Fail-closed: a missing archive, a missing or unusable zipalign, an entry
# listing that will not parse, or an archive with no native libraries at all is
# a named failure, never an empty pass.
set -u
LC_ALL=C; export LC_ALL

here=$(cd "$(dirname "$0")" && pwd) || { echo "cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)        || { echo "cannot resolve bindings/kotlin" >&2; exit 2; }
. "$here/lib-declaration.sh"

for t in awk sed grep basename printf mktemp unzip; do
    command -v "$t" >/dev/null 2>&1 || { echo "check-package-alignment: required tool not found: $t" >&2; exit 2; }
done

PAGE_KB=16
WANT_SO=libmoq5_jni.so
ABIS="arm64-v8a x86_64"

usage() { echo "usage: $(basename "$0") [--zipalign PATH] [--abis \"a b\"] PACKAGE.zip" >&2; exit 2; }
za_override=""
pkg=""
abis_override=""
while [ $# -gt 0 ]; do
    case $1 in
        --zipalign) [ $# -ge 2 ] || { echo "--zipalign requires a path" >&2; usage; }
                    [ -z "$za_override" ] || { echo "duplicate --zipalign" >&2; usage; }
                    za_override=$2; shift 2 ;;
        --abis) [ $# -ge 2 ] || { echo "--abis requires a value" >&2; usage; }
                [ -z "$abis_override" ] || { echo "duplicate --abis" >&2; usage; }
                abis_override=$2; shift 2 ;;
        -*) echo "unknown option: $1" >&2; usage ;;
        *) [ -z "$pkg" ] || { echo "more than one package given" >&2; usage; }
           pkg=$1; shift ;;
    esac
done
[ -n "$pkg" ] || usage
[ -n "$abis_override" ] && ABIS=$abis_override

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }
finish() {
    if [ "$fails" -eq 0 ]; then printf '\npackage native-library contract: 0 failure(s)\n'; exit 0; fi
    printf '\npackage native-library contract: %s failure(s)\n' "$fails"; exit 1
}

# The PINNED zipalign, from the same toolchain declaration Phase 0.1 pins, so
# the gate cannot silently run a different build-tools revision.
if [ -n "$za_override" ]; then
    zipalign=$za_override
else
    tc="$root/gradle/toolchain.properties"
    [ -f "$tc" ] || { echo "check-package-alignment: no toolchain declaration at $tc" >&2; exit 2; }
    _n=$(props_count "$tc" sdk.buildTools) \
        || { echo "check-package-alignment: cannot read sdk.buildTools from $tc" >&2; exit 2; }
    [ "$_n" = "1" ] || { echo "check-package-alignment: $tc declares no single sdk.buildTools" >&2; exit 2; }
    _bt=$(props_value "$tc" sdk.buildTools) \
        || { echo "check-package-alignment: cannot read sdk.buildTools from $tc" >&2; exit 2; }
    _sdk=${ANDROID_SDK_ROOT:-${ANDROID_HOME:-$HOME/Library/Android/sdk}}
    zipalign="$_sdk/build-tools/$_bt/zipalign"
fi
[ -x "$zipalign" ] || { echo "check-package-alignment: the pinned zipalign is not executable: $zipalign" >&2; exit 2; }

[ -f "$pkg" ] || { fail package "no such package: $pkg"; finish; }
[ -r "$pkg" ] || { fail package "package is not readable: $pkg"; finish; }

scratch=$(mktemp -d "${TMPDIR:-/tmp}/moq5-pkgalign-XXXXXXXX") \
    || { echo "check-package-alignment: cannot create a task-owned scratch" >&2; exit 2; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() { rm -rf -- "$scratch"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

# ---- rule 1: the pinned zipalign page-alignment check ------------------------
# Run WITHOUT a pipe: zipalign's own status is the evidence, and a pipeline
# would report the last command's status instead.
if "$zipalign" -c -P "$PAGE_KB" -v 4 "$pkg" > "$scratch/za" 2>&1; then
    pass package.zipalign "zipalign -c -P $PAGE_KB -v 4 accepts the archive ($(basename "$zipalign") from the pinned build-tools)"
else
    fail package.zipalign "zipalign -c -P $PAGE_KB -v 4 rejects the archive: $(grep -m1 'BAD' "$scratch/za" | sed 's/^ *//' | cut -c1-120)"
fi

# ---- entry listing, parsed once ---------------------------------------------
if ! unzip -v "$pkg" > "$scratch/lv" 2>"$scratch/lverr"; then
    fail package.entries "cannot list the archive, so no entry rule can be decided: $(sed -n '1p' "$scratch/lverr" | cut -c1-100)"
    finish
fi
# unzip -v columns: Length Method Size Cmpr Date Time CRC-32 Name
awk 'NR > 3 && NF >= 8 { m = $2; n = $NF; if (n ~ /\.so$/) print m "\t" n }' "$scratch/lv" \
    > "$scratch/so" 2>/dev/null \
    || { fail package.entries "the archive entry parser failed"; finish; }

_son=$(awk 'NF { n++ } END { print n + 0 }' "$scratch/so")
if [ "$_son" -eq 0 ]; then
    fail package.entries "the archive contains no .so entries at all; a package with no native library cannot satisfy a native-library contract"
    finish
fi

# ---- rule 2: every native library is STORED ---------------------------------
_comp=$(awk -F'\t' '$1 != "Stored" { print $2 " (" $1 ")"; exit }' "$scratch/so")
if [ -n "$_comp" ]; then
    fail package.compression "a native library is compressed: $_comp -- zipalign reports a compressed .so as OK because there is nothing to align, but a compressed library cannot be mapped directly out of the archive, which is what 16 KiB page-size support requires"
else
    pass package.compression "all $_son native librar(ies) are STORED, so they can be mapped directly out of the archive"
fi

# ---- rule 3: the one-DSO closure, at package level ---------------------------
_bad=""
for abi in $ABIS; do
    _n=$(awk -F'\t' -v w="lib/$abi/$WANT_SO" '$2 == w { n++ } END { print n + 0 }' "$scratch/so")
    [ "$_n" -eq 1 ] || { _bad="lib/$abi/$WANT_SO appears $_n time(s); exactly one is required"; break; }
done
if [ -z "$_bad" ]; then
    _extra=$(awk -F'\t' -v want="$ABIS" -v so="$WANT_SO" '
        BEGIN { split(want, a, " "); for (i in a) ok["lib/" a[i] "/" so] = 1 }
        { if (!($2 in ok)) { print $2; exit } }' "$scratch/so")
    [ -z "$_extra" ] || _bad="an unexpected native library is packaged: $_extra -- the one-DSO closure permits only lib/<abi>/$WANT_SO"
fi
if [ -n "$_bad" ]; then
    fail package.onedso "$_bad"
else
    pass package.onedso "exactly one native library per ABI ($ABIS), each lib/<abi>/$WANT_SO, and no other .so entry"
fi

finish
