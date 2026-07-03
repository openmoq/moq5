#!/usr/bin/env bash
#
# build_ios_openssl.sh - pinned iOS OpenSSL 3.x static build.
#
# The picoquic adapter's certificate verifier and libpicotls-openssl.a
# compile against OpenSSL, and an app linking LibMoQ.xcframework must link
# an OpenSSL built FOR iOS (host-Mac OpenSSL is not linkable there). This
# script materializes that dependency from a KNOWN-GOOD pinned OpenSSL
# release and builds static libcrypto.a + libssl.a for both iOS slices:
#
#     <build-root>/prefix-ios-device/     ios-arm64 device   (platform 2)
#     <build-root>/prefix-ios-simulator/  ios-arm64 simulator (platform 7)
#
# Each prefix carries iOS-configured headers (include/openssl, including
# the per-config opensslconf.h) plus lib/libcrypto.a + lib/libssl.a. It
# also assembles:
#
#     <out>/OpenSSL.xcframework   (libssl+libcrypto merged per slice)
#
# for the app link, and prints the value to feed the LibMoQ xcframework
# build:
#
#     MOQ_IOS_OPENSSL_ROOT=<build-root>/prefix-ios-device
#
# (Device and simulator arm64 headers are identical, so either prefix
# supplies the correct headers when compiling LibMoQ.xcframework; the
# per-slice archives differ and are linked from OpenSSL.xcframework.)
#
# Usage:
#     scripts/build_ios_openssl.sh [output-dir]   (default: <build-root>/dist)
#
# Env overrides (all optional):
#   MOQ_OPENSSL_REF            OpenSSL git tag/commit (default: pinned below)
#   MOQ_OPENSSL_REPO           OpenSSL remote (default: github.com/openssl/openssl)
#   MOQ_OPENSSL_SOURCE_DIR     pre-fetched OpenSSL checkout (skips clone)
#   MOQ_IOS_OPENSSL_BUILD_ROOT build trees + prefixes (default: <repo>/build-ios-openssl)
#   MOQ_IOS_DEPLOYMENT_TARGET  min iOS version (default: 16.0)
#   MOQ_IOS_JOBS               parallel build jobs (default: hw.ncpu)
#
# Requires: git (unless MOQ_OPENSSL_SOURCE_DIR), perl, make, xcodebuild +
# iOS SDKs, libtool.

set -euo pipefail

# -- Pinned, known-good OpenSSL release --------------------------------
# openssl-3.5.7 (3.5.x LTS). A git tag is mutable, so the default path is
# verified against the tag's exact commit; any override (ref, repo, or a
# pre-supplied source dir) is built but logged as UNVERIFIED.
OPENSSL_REF="${MOQ_OPENSSL_REF:-openssl-3.5.7}"
OPENSSL_REPO="${MOQ_OPENSSL_REPO:-https://github.com/openssl/openssl}"
OPENSSL_PINNED_COMMIT="8cf17aaeb4599f8af87fefd810b5b5fee90fe69e"

# The default pin is trusted only when nothing about the source is
# overridden (ref, repo, or an externally supplied checkout).
if [ -z "${MOQ_OPENSSL_REF:-}" ] && [ -z "${MOQ_OPENSSL_REPO:-}" ] \
   && [ -z "${MOQ_OPENSSL_SOURCE_DIR:-}" ]; then
    openssl_verify_pin=1
else
    openssl_verify_pin=0
fi

script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)

log() { printf '[build_ios_openssl] %s\n' "$*" >&2; }
die() { printf '[build_ios_openssl] ERROR: %s\n' "$*" >&2; exit 1; }

command -v perl       >/dev/null 2>&1 || die "perl not found"
command -v make       >/dev/null 2>&1 || die "make not found"
command -v xcodebuild >/dev/null 2>&1 || die "xcodebuild not found"
command -v libtool    >/dev/null 2>&1 || die "libtool not found"

build_root="${MOQ_IOS_OPENSSL_BUILD_ROOT:-$repo_root/build-ios-openssl}"
out_dir="${1:-$build_root/dist}"
deploy_target="${MOQ_IOS_DEPLOYMENT_TARGET:-16.0}"
jobs="${MOQ_IOS_JOBS:-$(sysctl -n hw.ncpu)}"

# -- Source ------------------------------------------------------------
src="${MOQ_OPENSSL_SOURCE_DIR:-}"
if [ -n "$src" ]; then
    [ -f "$src/Configure" ] || die "MOQ_OPENSSL_SOURCE_DIR has no Configure: $src"
    src=$(cd "$src" && pwd)
else
    command -v git >/dev/null 2>&1 || die "git not found (or set MOQ_OPENSSL_SOURCE_DIR)"
    src="$repo_root/.deps/openssl-ci/openssl"
    if [ ! -f "$src/Configure" ]; then
        log "fetching $OPENSSL_REF from $OPENSSL_REPO"
        mkdir -p "$src"
        git -C "$src" init -q
        git -C "$src" remote add origin "$OPENSSL_REPO" 2>/dev/null || \
            git -C "$src" remote set-url origin "$OPENSSL_REPO"
        git -C "$src" fetch --depth 1 origin "refs/tags/$OPENSSL_REF" >&2 \
            || git -C "$src" fetch --depth 1 origin "$OPENSSL_REF" >&2 \
            || die "could not fetch $OPENSSL_REF"
        git -C "$src" checkout -q FETCH_HEAD
    fi
fi

# -- Verify the commit pin --------------------------------------------
# Enforce the documented commit for the default path (a fetched tag can
# be re-pointed upstream); a re-used checkout is verified too, not only a
# fresh fetch. Overrides are built but flagged unverified.
head_commit=$(git -C "$src" rev-parse HEAD 2>/dev/null || echo "")
if [ "$openssl_verify_pin" = "1" ]; then
    [ -n "$head_commit" ] || die "cannot resolve HEAD of $src to verify the pin"
    [ "$head_commit" = "$OPENSSL_PINNED_COMMIT" ] || die \
        "OpenSSL commit pin mismatch: $src is at $head_commit, expected $OPENSSL_PINNED_COMMIT ($OPENSSL_REF)"
    log "commit pin OK   : $head_commit ($OPENSSL_REF)"
else
    log "WARNING: UNVERIFIED OpenSSL override (ref/repo/source-dir set);"
    log "WARNING: building ${head_commit:-<non-git source>} without pin enforcement."
fi

log "OpenSSL source  : $src"
log "pinned ref      : $OPENSSL_REF"
log "deployment tgt  : iOS $deploy_target"
log "build root      : $build_root"

# -- Per-slice out-of-tree configure/build/install ---------------------
# OpenSSL 3.x supports out-of-source builds: run <src>/Configure from an
# empty build dir. no-shared -> static only; no-tests/no-apps/no-docs
# trims everything an app does not link. Default algorithms are kept
# (picotls/picoquic pull a broad set - do NOT no-legacy/no-* them).
build_slice() {
    local slice=$1 target=$2 version_flag=$3 want_platform=$4
    local bdir="$build_root/$slice"
    local prefix="$build_root/prefix-$slice"

    log "[$slice] configure ($target)"
    rm -rf "$bdir" "$prefix"
    mkdir -p "$bdir"
    ( cd "$bdir" && perl "$src/Configure" "$target" \
        no-shared no-tests no-apps no-docs \
        --prefix="$prefix" --openssldir="$prefix/ssl" \
        "$version_flag" >&2 )

    log "[$slice] build"
    make -C "$bdir" -j"$jobs" >&2

    log "[$slice] install -> $prefix"
    # install_dev = headers + static libs + pkgconfig (no runtime/man).
    make -C "$bdir" install_dev >/dev/null

    [ -f "$prefix/include/openssl/ssl.h" ] || die "[$slice] no ssl.h installed"
    [ -f "$prefix/lib/libcrypto.a" ]       || die "[$slice] no libcrypto.a"
    [ -f "$prefix/lib/libssl.a" ]          || die "[$slice] no libssl.a"

    local platform
    platform=$(otool -l "$prefix/lib/libcrypto.a" 2>/dev/null \
        | awk '/platform/ && !found {print $2; found=1} END {if (!found) print "none"}')
    [ "$platform" = "$want_platform" ] \
        || die "[$slice] libcrypto.a platform $platform != $want_platform"

    log "[$slice] merge libssl+libcrypto"
    libtool -static -no_warning_for_no_symbols \
        -o "$bdir/libopenssl_bundle.a" "$prefix/lib/libssl.a" "$prefix/lib/libcrypto.a"
    log "[$slice] OK (platform $platform)"
}

build_slice ios-device    ios64-xcrun               -miphoneos-version-min="$deploy_target"        2
build_slice ios-simulator iossimulator-arm64-xcrun  -mios-simulator-version-min="$deploy_target"   7

# -- Assemble OpenSSL.xcframework -------------------------------------
# Headers are the device prefix's (arm64 device == arm64 sim config).
mkdir -p "$out_dir"
rm -rf "$out_dir/OpenSSL.xcframework"
xcodebuild -create-xcframework \
    -library "$build_root/ios-device/libopenssl_bundle.a" \
        -headers "$build_root/prefix-ios-device/include" \
    -library "$build_root/ios-simulator/libopenssl_bundle.a" \
        -headers "$build_root/prefix-ios-simulator/include" \
    -output "$out_dir/OpenSSL.xcframework" >&2

log "done: $out_dir/OpenSSL.xcframework"
log "feed the LibMoQ xcframework build with:"
log "    MOQ_IOS_OPENSSL_ROOT=$build_root/prefix-ios-device"
