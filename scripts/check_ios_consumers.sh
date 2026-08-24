#!/usr/bin/env bash
#
# check_ios_consumers.sh - build-only iOS consumer link checks.
#
# Proves an app importing MoQService compiles AND LINKS against the
# locally built xcframeworks for BOTH iOS slices, using SimplePlayerXcode
# as the consumer -- the env-free iOS/Xcode consumer that depends on the
# MoQServiceApple wrapper package (NOT the root MOQ5 env-gated lane), so
# this check runs with NO MOQ_SERVICE* environment, exactly as Xcode
# resolves it. It reuses SimplePlayer's sources, so it compiles the full
# backend-selection mapping, including .wtquicNetwork; a stale
# LibMoQ.xcframework whose headers predate a backend fails here. No
# simulator boot, no device, no network -- link checks only.
#
# Prerequisites (built locally first):
#   scripts/build_ios_openssl.sh          (both slices)
#   scripts/build_ios_xcframework.sh      (both slices)
#
# Usage: scripts/check_ios_consumers.sh [simulator|device|all]  (default: all)

set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
player="$repo_root/bindings/swift/Examples/SimplePlayerXcode"

log() { printf '[check_ios_consumers] %s\n' "$*" >&2; }
die() { printf '[check_ios_consumers] ERROR: %s\n' "$*" >&2; exit 1; }

what="${1:-all}"

[ -d "$repo_root/build-ios/dist/LibMoQ.xcframework" ] \
    || die "build-ios/dist/LibMoQ.xcframework missing; run scripts/build_ios_xcframework.sh"
[ -d "$repo_root/build-ios-openssl/dist/OpenSSL.xcframework" ] \
    || die "build-ios-openssl/dist/OpenSSL.xcframework missing; run scripts/build_ios_openssl.sh"

build_for() {
    local name=$1 triple=$2 sdk=$3
    local slice_dir
    case "$name" in
    simulator) slice_dir="ios-arm64-simulator" ;;
    device)    slice_dir="ios-arm64" ;;
    esac
    [ -d "$repo_root/build-ios/dist/LibMoQ.xcframework/$slice_dir" ] \
        || die "LibMoQ.xcframework lacks the $slice_dir slice"
    [ -d "$repo_root/build-ios-openssl/dist/OpenSSL.xcframework/$slice_dir" ] \
        || die "OpenSSL.xcframework lacks the $slice_dir slice"
    # A DEDICATED scratch path, removed first: a previously linked
    # product in SimplePlayer's own .build could otherwise let the
    # check pass without proving the regenerated xcframework (stale
    # incremental output has produced false results before).
    local scratch="$repo_root/build-ios/consumer-$name"
    rm -rf "$scratch"
    log "[$name] SimplePlayerXcode build ($triple; scratch $scratch; no env)"
    # Explicitly UNSET MOQ_SERVICE* so a stray shell env can never mask an
    # accidental reintroduction of the root MOQ5 dependency: the MoQServiceApple
    # wrapper vends MoQService unconditionally, which is the whole point of the
    # iOS/Xcode lane, and this check is load-bearing on that.
    (cd "$player" && \
     env -u MOQ_SERVICE -u MOQ_SERVICE_IOS -u MOQ_TRANSPORT swift build \
        --scratch-path "$scratch" \
        --triple "$triple" \
        --sdk "$(xcrun --sdk "$sdk" --show-sdk-path)" >&2)
    log "[$name] OK"
}

case "$what" in
simulator) build_for simulator arm64-apple-ios16.0-simulator iphonesimulator ;;
device)    build_for device    arm64-apple-ios16.0           iphoneos ;;
all)
    build_for simulator arm64-apple-ios16.0-simulator iphonesimulator
    build_for device    arm64-apple-ios16.0           iphoneos
    ;;
*) die "unknown selector '$what' (want: simulator | device | all)" ;;
esac
log "done"
