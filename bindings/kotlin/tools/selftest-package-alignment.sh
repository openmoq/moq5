#!/bin/sh
# Non-vacuity evidence for the package-level native-library contract.
#
# Every fixture is built in a task-owned scratch from the real packaged images.
# The durable stage and the durable package are only read, so this suite needs
# no mutation lock.
#
# The decisive case is `compressed`: zipalign accepts a deflated .so and exits
# 0, so a gate that only ran zipalign would pass an archive whose native library
# the platform cannot map. Each case therefore records what zipalign ALONE said
# about the same bytes, which is what shows the compression rule is doing work
# rather than restating the alignment one.
set -u
LC_ALL=C; export LC_ALL

here=$(cd "$(dirname "$0")" && pwd) || { echo "cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)        || { echo "cannot resolve bindings/kotlin" >&2; exit 2; }
. "$here/lib-declaration.sh"

for t in awk sed grep cp rm mkdir basename printf mktemp zip unzip; do
    command -v "$t" >/dev/null 2>&1 || { echo "selftest-package-alignment: required tool not found: $t" >&2; exit 2; }
done

dep="$root/.native"
STAGE="$dep/_topology"
GATE="$here/check-package-alignment.sh"
MKPKG="$here/mk-native-package.sh"
ABIS="arm64-v8a x86_64"
WANT_SO=libmoq5_jni.so
[ -r "$GATE" ] || { echo "selftest-package-alignment: no gate at $GATE" >&2; exit 2; }
[ -d "$STAGE" ] || { echo "selftest-package-alignment: no durable stage at $STAGE -- run build-jni-topology.sh first" >&2; exit 2; }

tc="$root/gradle/toolchain.properties"
_n=$(props_count "$tc" sdk.buildTools) || { echo "cannot read sdk.buildTools" >&2; exit 2; }
[ "$_n" = "1" ] || { echo "$tc declares no single sdk.buildTools" >&2; exit 2; }
_bt=$(props_value "$tc" sdk.buildTools) || { echo "cannot read sdk.buildTools" >&2; exit 2; }
_sdk=${ANDROID_SDK_ROOT:-${ANDROID_HOME:-$HOME/Library/Android/sdk}}
ZIPALIGN="$_sdk/build-tools/$_bt/zipalign"
[ -x "$ZIPALIGN" ] || { echo "the pinned zipalign is not executable: $ZIPALIGN" >&2; exit 2; }

work=$(mktemp -d "${TMPDIR:-/tmp}/moq5-pkg-selftest-XXXXXXXX") \
    || { echo "selftest-package-alignment: cannot create a task-owned scratch" >&2; exit 2; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() { rm -rf -- "$work"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

kills=0; misses=0; cases=0
kill_() { cases=$((cases + 1)); kills=$((kills + 1)); echo "KILL[$1]: $2"; }
miss_() { cases=$((cases + 1)); misses=$((misses + 1)); echo "MISS[$1]: $2"; }

# stage the real native payload once
tree="$work/tree"
for abi in $ABIS; do
    mkdir -p "$tree/lib/$abi" || { echo "cannot stage $abi" >&2; exit 2; }
    cp "$STAGE/$abi/package/$WANT_SO" "$tree/lib/$abi/$WANT_SO" || { echo "cannot stage $abi" >&2; exit 2; }
done

# what zipalign ALONE says, so each case shows whether the gate adds anything
zipalign_alone() {
    if "$ZIPALIGN" -c -P 16 4 "$1" > "$work/za.out" 2>&1; then
        printf 'accepts (exit 0)'
    else
        printf 'rejects (exit 1)'
    fi
}

expect_fail() {
    _c=$1; _p=$2; _sub=$3
    if sh "$GATE" --zipalign "$ZIPALIGN" "$_p" > "$work/out" 2>&1; then
        miss_ "$_c" "the gate passed a package it must reject"
        return
    fi
    if grep -q -- "$_sub" "$work/out"; then
        kill_ "$_c" "$(grep -m1 -- "$_sub" "$work/out" | cut -c1-150)"
    else
        miss_ "$_c" "the gate failed, but not for the expected reason: $(grep -m1 '^FAIL' "$work/out" | cut -c1-150)"
    fi
}

echo "== the produced package passes, so every mutant below is attributable =="
good="$work/good.zip"
( cd "$tree" && zip -q -0 -X -r "$work/good.unaligned" lib ) || { echo "cannot build the baseline" >&2; exit 2; }
"$ZIPALIGN" -P 16 -f 4 "$work/good.unaligned" "$good" || { echo "cannot align the baseline" >&2; exit 2; }
if sh "$GATE" --zipalign "$ZIPALIGN" "$good" > "$work/base.out" 2>&1; then
    echo "baseline: $(grep -c '^PASS' "$work/base.out") rule(s) pass"
else
    echo "selftest-package-alignment: the produced package does not pass:" >&2
    grep '^FAIL' "$work/base.out" >&2; exit 1
fi
# and the durable package the recipe writes passes under the same gate
if [ -r "$MKPKG" ]; then
    _dp=$(sh "$MKPKG") || { echo "cannot produce the durable package" >&2; exit 2; }
    if sh "$GATE" --zipalign "$ZIPALIGN" "$_dp" > "$work/dp.out" 2>&1; then
        echo "durable package: $_dp passes"
    else
        echo "the durable package does not pass:" >&2; grep '^FAIL' "$work/dp.out" >&2; exit 1
    fi
fi

echo "== RED 1: STORED native libraries that are not 16 KiB aligned =="
unal="$work/unaligned.zip"
( cd "$tree" && zip -q -0 -X -r "$unal" lib ) || { echo "cannot build fixture 1" >&2; exit 2; }
echo "   zipalign alone: $(zipalign_alone "$unal")"
expect_fail red1-unaligned "$unal" "rejects the archive"

echo "== RED 2: correctly aligned, but an unexpected second native DSO =="
tree2="$work/tree2"
mkdir -p "$tree2/lib/arm64-v8a" "$tree2/lib/x86_64" || { echo "cannot build fixture 2" >&2; exit 2; }
for abi in $ABIS; do cp "$tree/lib/$abi/$WANT_SO" "$tree2/lib/$abi/$WANT_SO" || exit 2; done
cp "$tree/lib/arm64-v8a/$WANT_SO" "$tree2/lib/arm64-v8a/libmoq5_extra.so" || { echo "cannot add the extra DSO" >&2; exit 2; }
( cd "$tree2" && zip -q -0 -X -r "$work/extra.unaligned" lib ) || { echo "cannot build fixture 2" >&2; exit 2; }
extra="$work/extra.zip"
"$ZIPALIGN" -P 16 -f 4 "$work/extra.unaligned" "$extra" || { echo "cannot align fixture 2" >&2; exit 2; }
echo "   zipalign alone: $(zipalign_alone "$extra")  <- alignment is fine; only the closure rule can catch this"
expect_fail red2-extra-dso "$extra" "unexpected native library is packaged"

echo "== RED 3: COMPRESSED native libraries =="
comp="$work/compressed.zip"
( cd "$tree" && zip -q -9 -X -r "$comp" lib ) || { echo "cannot build fixture 3" >&2; exit 2; }
echo "   zipalign alone: $(zipalign_alone "$comp")  <- a compressed .so has nothing to align, so zipalign is satisfied"
expect_fail red3-compressed "$comp" "native library is compressed"

echo "== RED 4: an archive with no native library at all =="
tree4="$work/tree4"; mkdir -p "$tree4/lib" || exit 2
printf 'not a library\n' > "$tree4/lib/README.txt" || exit 2
none="$work/none.zip"
( cd "$tree4" && zip -q -0 -X -r "$none" lib ) || { echo "cannot build fixture 4" >&2; exit 2; }
echo "   zipalign alone: $(zipalign_alone "$none")"
expect_fail red4-no-native "$none" "no .so entries at all"

echo "== RED 5: one ABI's library missing =="
tree5="$work/tree5"; mkdir -p "$tree5/lib/arm64-v8a" || exit 2
cp "$tree/lib/arm64-v8a/$WANT_SO" "$tree5/lib/arm64-v8a/$WANT_SO" || exit 2
( cd "$tree5" && zip -q -0 -X -r "$work/one.unaligned" lib ) || exit 2
oneabi="$work/oneabi.zip"
"$ZIPALIGN" -P 16 -f 4 "$work/one.unaligned" "$oneabi" || exit 2
echo "   zipalign alone: $(zipalign_alone "$oneabi")"
expect_fail red5-missing-abi "$oneabi" "appears 0 time(s)"

echo "== RED 6: a missing package is a failure, not an absent measurement =="
expect_fail red6-missing-package "$work/does-not-exist.zip" "no such package"

printf '\npackage-alignment self-test: %s kill(s), %s miss(es), %s case(s)\n' "$kills" "$misses" "$cases"
[ "$misses" -eq 0 ] || exit 1
exit 0
