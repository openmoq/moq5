#!/bin/sh
# Proves check-sample-debug-trust.sh refuses what it exists to refuse.
#
# Each case copies the samples' sources and built artifacts into a fresh
# fixture, breaks exactly one thing, and requires the gate to fail by the named
# row -- not merely to exit nonzero. The unbroken fixture must pass. Build both
# samples' debug and release variants first (see check-sample-debug-trust.sh).
#
# usage: selftest-sample-debug-trust.sh [bindings/kotlin root]
set -eu

_self=$(basename "$0")
here=$(cd "$(dirname "$0")" && pwd)
root=${1:-$(cd "$here/.." && pwd)}
gate="$here/check-sample-debug-trust.sh"
[ -x "$gate" ] || { echo "$_self: $gate is missing or not executable" >&2; exit 2; }

work=$(mktemp -d "${TMPDIR:-/tmp}/sample-debug-trust.XXXXXX")
cleanup() { rm -rf -- "$work"; }
trap cleanup EXIT INT TERM

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

# A fixture: just what the gate reads.
fixture() {
    _f="$work/$1"
    mkdir -p "$_f/gradle"
    cp "$root/settings.gradle.kts" "$_f/"
    cp "$root/gradle/toolchain.properties" "$_f/gradle/"
    [ -f "$root/local.properties" ] && cp "$root/local.properties" "$_f/"
    for s in player publisher; do
        mkdir -p "$_f/samples/$s/build/intermediates" "$_f/samples/$s/build/outputs"
        cp -R "$root/samples/$s/src" "$_f/samples/$s/"
        cp -R "$root/samples/$s/build/intermediates/merged_manifest" "$_f/samples/$s/build/intermediates/"
        cp -R "$root/samples/$s/build/outputs/apk" "$_f/samples/$s/build/outputs/"
    done
    printf '%s' "$_f"
}

# expect <case> <row> <break...>: the broken fixture must fail by <row>.
# CASE_READER, when set, is the aapt2 the gate uses for that case.
CASE_READER=
expect() {
    _case=$1; _row=$2; shift 2
    _f=$(fixture "$_case")
    ( cd "$_f" && eval "$*" )
    if env ${CASE_READER:+MOQ5_AAPT2="$CASE_READER"} "$gate" "$_f" > "$work/$_case.out" 2>&1; then
        fail "selftest.$_case" "the gate passed; it must fail by $_row"
    elif grep -q "^FAIL\[$_row\]" "$work/$_case.out"; then
        pass "selftest.$_case" "the gate fails by name: $_row"
    else
        fail "selftest.$_case" "the gate failed, but not by $_row: $(grep '^FAIL' "$work/$_case.out" | tr '\n' ' ')"
    fi
}

P=samples/player
CFG=src/debug/res/xml/network_security_config.xml

_f=$(fixture baseline)
if "$gate" "$_f" > "$work/baseline.out" 2>&1; then
    pass "selftest.baseline" "the unbroken fixture passes"
else
    fail "selftest.baseline" "the unbroken fixture fails: $(grep '^FAIL' "$work/baseline.out" | tr '\n' ' ')"
fi

expect player-overlay-removed trust.player.source-debug "rm -r $P/src/debug"
expect publisher-overlay-removed trust.publisher.source-debug "rm -r samples/publisher/src/debug"
expect player-config-removed trust.player.source-config "rm $P/$CFG"
expect config-cleartext-base trust.player.source-config \
    "sed -i.bak 's|<debug-overrides>|<base-config cleartextTrafficPermitted=\"true\" /><debug-overrides>|' $P/$CFG"
expect config-bundled-anchor trust.player.source-config \
    "sed -i.bak 's|src=\"user\"|src=\"@raw/local_ca\"|' $P/$CFG"
expect config-pin-override trust.player.source-config \
    "sed -i.bak 's|src=\"user\"|src=\"user\" overridePins=\"true\"|' $P/$CFG"
expect config-namespaced-src trust.player.source-config \
    "sed -i.bak 's|<certificates src=\"user\" />|<certificates xmlns:x=\"urn:evil\" x:src=\"user\" />|' $P/$CFG && grep -q 'x:src' $P/$CFG"
expect config-malformed trust.player.source-config \
    "sed -i.bak 's|</network-security-config>||' $P/$CFG"
expect debug-manifest-twice trust.player.source-debug \
    "sed -i.bak 's|tools:targetApi=\"24\"|tools:targetApi=\"24\" android:networkSecurityConfig=\"@xml/other\"|' $P/src/debug/AndroidManifest.xml"
expect release-source-attribute trust.player.source-release \
    "sed -i.bak 's|<application|<application android:networkSecurityConfig=\"@xml/network_security_config\"|' $P/src/main/AndroidManifest.xml"
expect release-source-resource trust.player.source-release \
    "mkdir -p $P/src/main/res/xml && cp $P/$CFG $P/src/main/res/xml/"
expect merged-release-leak trust.player.merged-release \
    "cp $P/build/intermediates/merged_manifest/debug/processDebugMainManifest/AndroidManifest.xml $P/build/intermediates/merged_manifest/release/processReleaseMainManifest/AndroidManifest.xml"
expect merged-debug-missing trust.player.merged-debug \
    "cp $P/build/intermediates/merged_manifest/release/processReleaseMainManifest/AndroidManifest.xml $P/build/intermediates/merged_manifest/debug/processDebugMainManifest/AndroidManifest.xml"
expect apk-release-leak trust.player.apk-release \
    "cp $P/build/outputs/apk/debug/player-debug.apk $P/build/outputs/apk/release/player-release-unsigned.apk"
expect apk-debug-missing trust.player.apk-debug \
    "cp $P/build/outputs/apk/release/player-release-unsigned.apk $P/build/outputs/apk/debug/player-debug.apk"

# An APK the reader cannot read proves nothing about what it lacks.
expect apk-release-corrupt trust.player.apk-release \
    ": > $P/build/outputs/apk/release/player-release-unsigned.apk"
expect apk-debug-corrupt trust.player.apk-debug \
    ": > $P/build/outputs/apk/debug/player-debug.apk"

# Nor does a reader that answers nothing, or answers with a diagnostic.
real_reader=$(sed -n 's/^sdk\.buildTools=//p' "$root/gradle/toolchain.properties")
if [ -n "${ANDROID_SDK_ROOT:-${ANDROID_HOME:-}}" ]; then
    real_reader="${ANDROID_SDK_ROOT:-$ANDROID_HOME}/build-tools/$real_reader/aapt2"
else
    real_reader="$(sed -n 's/^sdk\.dir=//p' "$root/local.properties")/build-tools/$real_reader/aapt2"
fi
printf '#!/bin/sh\nexit 0\n' > "$work/silent-aapt2"
# The mock's own "$@" and $_rc are written literally, for it to expand.
# shellcheck disable=SC2016
printf '#!/bin/sh\n"%s" "$@"; _rc=$?\necho "W: a diagnostic the gate must not hide" >&2\nexit $_rc\n' "$real_reader" > "$work/noisy-aapt2"
chmod +x "$work/silent-aapt2" "$work/noisy-aapt2"
CASE_READER="$work/silent-aapt2"
expect reader-silent trust.player.apk-release ":"
CASE_READER="$work/noisy-aapt2"
expect reader-diagnostic trust.player.apk-release ":"
CASE_READER=

# The attribute as text in a comment is not the attribute.
expect source-attribute-in-comment trust.player.source-debug \
    "sed -i.bak 's| android:networkSecurityConfig=\"@xml/network_security_config\"||' $P/src/debug/AndroidManifest.xml && printf '%s\n' '<!-- android:networkSecurityConfig=\"@xml/network_security_config\" -->' >> $P/src/debug/AndroidManifest.xml"
expect merged-attribute-in-comment trust.player.merged-debug \
    "M=$P/build/intermediates/merged_manifest/debug/processDebugMainManifest/AndroidManifest.xml && sed -i.bak 's| android:networkSecurityConfig=\"@xml/network_security_config\"||' \$M && printf '%s\n' '<!-- android:networkSecurityConfig=\"@xml/network_security_config\" -->' >> \$M"

if [ "$fails" -ne 0 ]; then
    echo "$_self: $fails case(s) failed" >&2
    exit 1
fi
echo "$_self: every case fails by its named row, and the unbroken fixture passes"
