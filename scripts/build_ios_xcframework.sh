#!/usr/bin/env bash
#
# build_ios_xcframework.sh - build LibMoQ.xcframework for iOS.
#
# Produces a single static xcframework containing the full MoQService
# C stack (core + media + service + picoquic raw/threaded + pico_wt
# raw/managed) with its transport dependencies (picoquic, picohttp,
# picotls) merged into one archive per platform slice:
#
#     <out>/LibMoQ.xcframework
#         ios-arm64/            device slice   (LC_BUILD_VERSION platform 2)
#         ios-arm64-simulator/  simulator slice (platform 7)
#
# Each slice's Headers/ carries the installed public headers plus a
# module.modulemap defining the clang module `CMoQService` with the same
# shim contents as bindings/swift/SystemModules/CMoQService, so Swift
# code written against the pkg-config system module compiles unchanged
# against the xcframework.
#
# Dependencies are consumed in source-tree mode: picoquic via
# MOQ_PICOQUIC_SOURCE_DIR, picotls via PICOQUIC_FETCH_PTLS pointed at a
# LOCAL checkout through FETCHCONTENT_SOURCE_DIR_PICOTLS (no network
# fetch; picotls is configured inside the same iOS toolchain, which is
# the only layout that survives CMAKE_FIND_ROOT_PATH re-rooting when
# cross-compiling).
#
# OpenSSL: the picoquic adapter's certificate verifier and
# libpicotls-openssl.a get compiled into the bundle, so this build
# REQUIRES an iOS-built OpenSSL 3.x prefix via MOQ_IOS_OPENSSL_ROOT -
# otherwise the resulting archives would target the host and the
# xcframework, though it looks shippable, could not link on iOS. For a
# compile-only smoke check (output stamped NOT_LINKABLE.txt) you may set
# MOQ_IOS_ALLOW_HOST_OPENSSL_HEADERS=1 to fall back to host OpenSSL
# headers; never ship that artifact.
#
# Usage:
#     scripts/build_ios_xcframework.sh [output-dir]   (default: <build-root>/dist)
#
# Env overrides:
#   MOQ_IOS_OPENSSL_ROOT       iOS-built OpenSSL 3.x prefix (REQUIRED unless
#                              MOQ_IOS_ALLOW_HOST_OPENSSL_HEADERS=1)
#   MOQ_IOS_ALLOW_HOST_OPENSSL_HEADERS  =1 -> compile-only host-header fallback,
#                              output marked NOT LINKABLE (default: unset)
#   MOQ_IOS_BUILD_ROOT         build trees + prefixes (default: <repo>/build-ios)
#   MOQ_PICOQUIC_SOURCE_DIR    picoquic checkout (default: <repo>/.deps/picoquic-ci/picoquic,
#                              else <repo>/../picoquic)
#   MOQ_PICOTLS_SOURCE_DIR     picotls checkout  (default: <repo>/.deps/picoquic-ci/picotls,
#                              else <repo>/../picotls)
#   MOQ_IOS_DEPLOYMENT_TARGET  min iOS version   (default: 16.0)
#   MOQ_IOS_JOBS               parallel build jobs (default: hw.ncpu)
#
# Requires: cmake, xcodebuild + iOS SDKs, libtool, git-free (no network).

set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)

log() { printf '[build_ios_xcframework] %s\n' "$*" >&2; }
die() { printf '[build_ios_xcframework] ERROR: %s\n' "$*" >&2; exit 1; }

command -v cmake      >/dev/null 2>&1 || die "cmake not found"
command -v xcodebuild >/dev/null 2>&1 || die "xcodebuild not found"
command -v libtool    >/dev/null 2>&1 || die "libtool not found"

# -- Inputs -----------------------------------------------------------
build_root="${MOQ_IOS_BUILD_ROOT:-$repo_root/build-ios}"
out_dir="${1:-$build_root/dist}"
deploy_target="${MOQ_IOS_DEPLOYMENT_TARGET:-16.0}"
jobs="${MOQ_IOS_JOBS:-$(sysctl -n hw.ncpu)}"

resolve_dep() {
    local name=$1 override=$2 var=$3
    if [ -n "$override" ]; then
        [ -d "$override" ] || die "$name: '$override' is not a directory"
        echo "$override"
    elif [ -d "$repo_root/.deps/picoquic-ci/$name" ]; then
        echo "$repo_root/.deps/picoquic-ci/$name"
    elif [ -d "$repo_root/../$name" ]; then
        cd "$repo_root/../$name" && pwd
    else
        die "$name checkout not found; run scripts/setup_picoquic_deps.sh or set $var"
    fi
}

picoquic_src=$(resolve_dep picoquic "${MOQ_PICOQUIC_SOURCE_DIR:-}" MOQ_PICOQUIC_SOURCE_DIR)
picotls_src=$(resolve_dep picotls "${MOQ_PICOTLS_SOURCE_DIR:-}" MOQ_PICOTLS_SOURCE_DIR)

# OpenSSL prefix. Default: REQUIRE an iOS-built OpenSSL 3.x via
# MOQ_IOS_OPENSSL_ROOT, so the xcframework this produces is actually
# linkable in an app. The picoquic adapter's certificate verifier and
# libpicotls-openssl.a get compiled into the bundle, so host-Mac OpenSSL
# headers would yield an artifact that looks shippable but cannot link on
# iOS. Building against host headers is a compile-only convenience gated
# behind an explicit opt-in (MOQ_IOS_ALLOW_HOST_OPENSSL_HEADERS=1) that
# stamps the output as NOT LINKABLE.
compile_only_openssl=0
openssl_root="${MOQ_IOS_OPENSSL_ROOT:-}"
if [ -z "$openssl_root" ]; then
    if [ "${MOQ_IOS_ALLOW_HOST_OPENSSL_HEADERS:-0}" != "1" ]; then
        die "MOQ_IOS_OPENSSL_ROOT is required (an iOS-built OpenSSL 3.x prefix)."$'\n'\
"  Build one with the pinned iOS OpenSSL script, or - for a COMPILE-ONLY"$'\n'\
"  check whose output is NOT LINKABLE on iOS - set"$'\n'\
"  MOQ_IOS_ALLOW_HOST_OPENSSL_HEADERS=1 to fall back to host OpenSSL headers."
    fi
    openssl_root=$(brew --prefix openssl@3 2>/dev/null) \
        || die "MOQ_IOS_ALLOW_HOST_OPENSSL_HEADERS=1 set but 'brew --prefix openssl@3' failed"
    compile_only_openssl=1
    log "WARNING: COMPILE-ONLY build against host OpenSSL headers ($openssl_root)."
    log "WARNING: the resulting xcframework is NOT LINKABLE in an iOS app -"
    log "WARNING: its OpenSSL objects target the host. Set MOQ_IOS_OPENSSL_ROOT"
    log "WARNING: to an iOS-built OpenSSL 3.x prefix for a shippable artifact."
fi
[ -f "$openssl_root/include/openssl/ssl.h" ] \
    || die "no openssl/ssl.h under $openssl_root/include"

log "picoquic source : $picoquic_src"
log "picotls source  : $picotls_src"
log "OpenSSL prefix  : $openssl_root"
log "deployment tgt  : iOS $deploy_target"
log "build root      : $build_root"

# -- Per-slice configure/build/install/merge ---------------------------
# The archives that make up the bundle: everything libmoq installs, plus
# the dependency archives that stay in the build tree (picoquic installs
# nothing here and picotls is a FetchContent subbuild).
DEPS_ARCHIVES=(
    _deps/picoquic/libpicoquic-core.a
    _deps/picoquic/libpicoquic-log.a
    _deps/picoquic/libpicohttp-core.a
    _deps/picotls-build/libpicotls-core.a
    _deps/picotls-build/libpicotls-openssl.a
    _deps/picotls-build/libpicotls-minicrypto.a
)

build_slice() {
    local slice=$1 sysroot=$2 want_platform=$3
    local bdir="$build_root/$slice"
    local prefix="$build_root/prefix-$slice"

    log "[$slice] configure"
    cmake -B "$bdir" -S "$repo_root" \
        -DCMAKE_SYSTEM_NAME=iOS \
        -DCMAKE_OSX_SYSROOT="$sysroot" \
        -DCMAKE_OSX_ARCHITECTURES=arm64 \
        -DCMAKE_OSX_DEPLOYMENT_TARGET="$deploy_target" \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS=OFF \
        -DMOQ_BUILD_TESTS=OFF \
        -DMOQ_BUILD_EXAMPLES=OFF \
        -DMOQ_BUILD_MSF=ON \
        -DMOQ_BUILD_MEDIA_OBJECT=ON \
        -DMOQ_BUILD_PLAYBACK=ON \
        -DMOQ_BUILD_SERVICE=ON \
        -DMOQ_BUILD_ADAPTER_PICOQUIC=ON \
        -DMOQ_BUILD_PQ_THREADED=ON \
        -DMOQ_BUILD_ADAPTER_PICO_WT=ON \
        -DMOQ_BUILD_PICO_WT_MANAGED=ON \
        -DMOQ_PICOQUIC_SOURCE_DIR="$picoquic_src" \
        -DPICOQUIC_FETCH_PTLS=ON \
        -DFETCHCONTENT_SOURCE_DIR_PICOTLS="$picotls_src" \
        -DMOQ_PICOTLS_PREFIX="$picotls_src" \
        -DWITH_FUSION=OFF \
        -DBUILD_DEMO=OFF -DBUILD_PQBENCH=OFF -DBUILD_LOGREADER=OFF \
        -DOPENSSL_ROOT_DIR="$openssl_root" \
        "-DCMAKE_FIND_ROOT_PATH=$openssl_root;$picotls_src" \
        >&2

    log "[$slice] build"
    cmake --build "$bdir" -j"$jobs" >&2

    log "[$slice] install -> $prefix"
    rm -rf "$prefix"
    cmake --install "$bdir" --prefix "$prefix" >/dev/null

    log "[$slice] merge archives"
    local merge=("$prefix"/lib/libmoq-*.a)
    local dep
    for dep in "${DEPS_ARCHIVES[@]}"; do
        [ -f "$bdir/$dep" ] || die "[$slice] expected archive missing: $bdir/$dep"
        merge+=("$bdir/$dep")
    done
    libtool -static -no_warning_for_no_symbols \
        -o "$bdir/libmoq_bundle.a" "${merge[@]}"

    # Sanity: the anchor symbol is present and the slice targets the
    # right platform (2 = iOS device, 7 = iOS simulator). No early-exit
    # consumers on the left of a pipe (grep -q / awk-exit would SIGPIPE
    # nm/otool and trip pipefail).
    nm "$bdir/libmoq_bundle.a" 2>/dev/null | grep "T _moq_endpoint_connect" >/dev/null \
        || die "[$slice] _moq_endpoint_connect missing from bundle"
    local platform
    platform=$(otool -l "$bdir/libmoq_bundle.a" 2>/dev/null \
        | awk '/platform/ && !found {print $2; found=1} END {if (!found) print "none"}')
    [ "$platform" = "$want_platform" ] \
        || die "[$slice] LC_BUILD_VERSION platform $platform != $want_platform"
    log "[$slice] bundle OK ($(du -h "$bdir/libmoq_bundle.a" | cut -f1) platform $platform)"
}

build_slice ios-device    iphoneos        2
build_slice ios-simulator iphonesimulator 7

# -- Headers + clang module -------------------------------------------
# Installed headers are platform-neutral; stage once (from the device
# prefix) and reuse for both slices. The module mirrors
# bindings/swift/SystemModules/CMoQService so `import CMoQService`
# resolves identically against pkg-config (macOS) and this xcframework.
headers="$build_root/headers"
rm -rf "$headers"
mkdir -p "$headers"
cp -R "$build_root/prefix-ios-device/include/moq" "$headers/moq"

cat > "$headers/shim.h" <<'EOF'
#ifndef MOQ_SERVICE_SHIM_H
#define MOQ_SERVICE_SHIM_H

/* The installed C service tier (pkg-config: libmoq-service). The service
 * headers pull types.h/session.h/media_object.h/cmaf.h/msf.h transitively;
 * rcbuf.h is included for the sender's payload buffers. */
#include <moq/endpoint.h>
#include <moq/media_receiver.h>
#include <moq/media_sender.h>
#include <moq/rcbuf.h>

#endif
EOF

cat > "$headers/module.modulemap" <<'EOF'
module CMoQService [system] {
    header "shim.h"
    export *
}
EOF

# -- Assemble ----------------------------------------------------------
mkdir -p "$out_dir"
rm -rf "$out_dir/LibMoQ.xcframework"
xcodebuild -create-xcframework \
    -library "$build_root/ios-device/libmoq_bundle.a"    -headers "$headers" \
    -library "$build_root/ios-simulator/libmoq_bundle.a" -headers "$headers" \
    -output "$out_dir/LibMoQ.xcframework" >&2

# Stamp compile-only builds so the artifact cannot masquerade as
# shippable once it leaves this shell's warnings behind.
if [ "$compile_only_openssl" = "1" ]; then
    cat > "$out_dir/LibMoQ.xcframework/NOT_LINKABLE.txt" <<EOF
This LibMoQ.xcframework was built COMPILE-ONLY against host OpenSSL
headers (MOQ_IOS_ALLOW_HOST_OPENSSL_HEADERS=1). Its OpenSSL objects
target the host, not iOS, so it WILL NOT LINK in an iOS app. Rebuild
with MOQ_IOS_OPENSSL_ROOT pointing at an iOS-built OpenSSL 3.x prefix
for a shippable artifact.
EOF
    log "done (COMPILE-ONLY, NOT LINKABLE): $out_dir/LibMoQ.xcframework"
else
    log "done: $out_dir/LibMoQ.xcframework"
fi
ls "$out_dir/LibMoQ.xcframework" >&2
