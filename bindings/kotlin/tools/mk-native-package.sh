#!/bin/sh
# Builds the native payload of an Android package from the durable topology
# stage: lib/<abi>/libmoq5_jni.so, STORED and 16 KiB page-aligned.
#
# WHAT THIS IS. These are exactly the bytes and the layout an APK carries in its
# lib/ tree, produced with the PINNED build-tools zipalign. It is the artifact
# check-package-alignment.sh gates.
#
# WHAT THIS IS NOT. It is not an APK, and gating it is not an install gate. It
# carries no manifest, no dex, no resources, no signature, and no AGP packaging
# decision -- in particular nothing here proves what useLegacyPackaging or
# extractNativeLibs would be set to in a real module, and nothing here proves
# the platform will map the library at install time. Those need a real Android
# application module, which Phase 0.4 deliberately does not create.
set -u
LC_ALL=C; export LC_ALL

here=$(cd "$(dirname "$0")" && pwd) || { echo "cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)        || { echo "cannot resolve bindings/kotlin" >&2; exit 2; }
. "$here/lib-declaration.sh"

for t in awk sed grep cp rm mkdir basename printf zip; do
    command -v "$t" >/dev/null 2>&1 || { echo "mk-native-package: required tool not found: $t" >&2; exit 2; }
done

ABIS="arm64-v8a x86_64"
WANT_SO=libmoq5_jni.so
PAGE_KB=16

usage() { echo "usage: $(basename "$0") [--stage DIR] [OUT.zip]" >&2; exit 2; }
stage=""; out=""
while [ $# -gt 0 ]; do
    case $1 in
        --stage) [ $# -ge 2 ] || { echo "--stage requires a directory" >&2; usage; }
                 [ -z "$stage" ] || { echo "duplicate --stage" >&2; usage; }
                 stage=$2; shift 2 ;;
        -*) echo "unknown option: $1" >&2; usage ;;
        *) [ -z "$out" ] || { echo "more than one output given" >&2; usage; }
           out=$1; shift ;;
    esac
done
dep="$root/.native"
[ -n "$stage" ] || stage="$dep/_topology"
[ -n "$out" ] || out="$dep/_topology.package/moq5-native.zip"
[ -d "$stage" ] || { echo "mk-native-package: no topology stage at $stage" >&2; exit 2; }

# The pinned zipalign, resolved from the same declaration Phase 0.1 pins.
tc="$root/gradle/toolchain.properties"
[ -f "$tc" ] || { echo "mk-native-package: no toolchain declaration at $tc" >&2; exit 2; }
_n=$(props_count "$tc" sdk.buildTools) || { echo "mk-native-package: cannot read sdk.buildTools" >&2; exit 2; }
[ "$_n" = "1" ] || { echo "mk-native-package: $tc declares no single sdk.buildTools" >&2; exit 2; }
_bt=$(props_value "$tc" sdk.buildTools) || { echo "mk-native-package: cannot read sdk.buildTools" >&2; exit 2; }
_sdk=${ANDROID_SDK_ROOT:-${ANDROID_HOME:-$HOME/Library/Android/sdk}}
zipalign="$_sdk/build-tools/$_bt/zipalign"
[ -x "$zipalign" ] || { echo "mk-native-package: the pinned zipalign is not executable: $zipalign" >&2; exit 2; }

outdir=$(dirname "$out")
mkdir -p "$outdir" || { echo "mk-native-package: cannot create $outdir" >&2; exit 2; }
work="$outdir/.staging"
rm -rf -- "$work" || { echo "mk-native-package: cannot clear $work" >&2; exit 2; }
mkdir -p "$work" || { echo "mk-native-package: cannot create $work" >&2; exit 2; }

for abi in $ABIS; do
    _src="$stage/$abi/package/$WANT_SO"
    [ -f "$_src" ] || { echo "mk-native-package: the stage has no packaged image for $abi: $_src" >&2; exit 2; }
    mkdir -p "$work/lib/$abi" || { echo "mk-native-package: cannot create lib/$abi" >&2; exit 2; }
    cp "$_src" "$work/lib/$abi/$WANT_SO" || { echo "mk-native-package: cannot stage $abi" >&2; exit 2; }
done

rm -f "$out" "$out.unaligned" || { echo "mk-native-package: cannot clear the outputs" >&2; exit 2; }
# -0 stores every entry: a native library must be uncompressed to be mapped
# directly out of the archive. -X drops extra attributes so the input is
# reproducible from the same bytes.
( cd "$work" && zip -q -0 -X -r "$out.unaligned" lib ) \
    || { echo "mk-native-package: cannot build the archive" >&2; exit 2; }
"$zipalign" -P "$PAGE_KB" -f 4 "$out.unaligned" "$out" \
    || { echo "mk-native-package: zipalign failed" >&2; exit 2; }
rm -f "$out.unaligned" || { echo "mk-native-package: cannot tidy the intermediate" >&2; exit 2; }
rm -rf -- "$work" || { echo "mk-native-package: cannot tidy the staging tree" >&2; exit 2; }

printf '%s\n' "$out"
