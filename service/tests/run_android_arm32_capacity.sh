#!/bin/sh
# Compile the sender capacity fixture for Android armeabi-v7a: a REAL 32-bit
# size_t, so the fixture's checked-multiplication branch is compiled by a
# target on which ring_cap * sizeof(entry) actually wraps, and the product's
# arithmetic is compiled under the same width. The host cannot execute the
# result; this lane proves compilation and links the test binary against the
# service tier built for that ABI, warning-clean. A missing NDK is a named
# environment failure (exit 3), never a pass.
#
#   run_android_arm32_capacity.sh <libmoq-root> <build-dir> [ndk-root]
set -u
root=${1:?libmoq root}; bdir=${2:?build dir}; ndk=${3:-}
if [ -z "$ndk" ]; then
    for c in "${ANDROID_NDK_ROOT:-}" "${ANDROID_NDK_HOME:-}" \
             "$HOME/Library/Android/sdk/ndk/28.2.13676358"; do
        [ -n "$c" ] && [ -f "$c/build/cmake/android.toolchain.cmake" ] && { ndk=$c; break; }
    done
fi
[ -n "$ndk" ] && [ -f "$ndk/build/cmake/android.toolchain.cmake" ] \
    || { echo "ENVIRONMENT: no Android NDK with build/cmake/android.toolchain.cmake" >&2; exit 3; }
for t in cmake file; do command -v "$t" >/dev/null 2>&1 || { echo "ENVIRONMENT: missing $t" >&2; exit 3; }; done

rm -rf "$bdir"
cmake -S "$root" -B "$bdir" -G Ninja \
    -DCMAKE_TOOLCHAIN_FILE="$ndk/build/cmake/android.toolchain.cmake" \
    -DANDROID_ABI=armeabi-v7a -DANDROID_PLATFORM=android-21 \
    -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF \
    -DMOQ_BUILD_TESTS=ON -DMOQ_BUILD_SIM=OFF -DMOQ_BUILD_EXAMPLES=OFF \
    -DMOQ_BUILD_SERVICE=ON -DMOQ_BUILD_MSF=ON -DMOQ_BUILD_MEDIA_OBJECT=ON \
    -DMOQ_BUILD_LOC=ON -DMOQ_BUILD_CMAF=ON -DMOQ_WARNINGS_AS_ERRORS=ON \
    > "$bdir.configure.log" 2>&1 || { echo "FAIL[arm32.configure]: see $bdir.configure.log" >&2; exit 1; }
cmake --build "$bdir" --target test_media_sender_capacity > "$bdir.build.log" 2>&1 \
    || { echo "FAIL[arm32.build]: see $bdir.build.log" >&2; exit 1; }
w=$(grep -ci 'warning' "$bdir.configure.log" "$bdir.build.log" | awk -F: '{s+=$2} END{print s+0}')
[ "$w" -eq 0 ] || { echo "FAIL[arm32.warnings]: $w warning line(s)" >&2; exit 1; }
bin="$bdir/service/test_media_sender_capacity"
desc=$(file "$bin")
case $desc in
    *"ELF 32-bit"*ARM*) echo "PASS[arm32.binary]: $desc" ;;
    *) echo "FAIL[arm32.binary]: not an ELF32 ARM executable: $desc" >&2; exit 1 ;;
esac
echo "PASS[arm32.lane]: test_media_sender_capacity compiled and linked for armeabi-v7a, 0 warnings"
