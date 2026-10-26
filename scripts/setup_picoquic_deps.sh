#!/usr/bin/env bash
#
# setup_picoquic_deps.sh - pinned picoquic + picotls setup for the
# picoquic adapter (CI and reproducible local dev).
#
# libmoq's picoquic adapter resolves picoquic in "source-tree mode" (see
# cmake/FindPicoquic.cmake): it needs a picoquic source checkout and a
# built picotls. This script materializes both at KNOWN-GOOD pinned
# commits so CI and developers consume the same dependency input instead
# of relying on whatever happens to be checked out next to the repo.
#
# It clones (shallow, by exact commit) picoquic and picotls into a
# deterministic deps dir, builds picotls, and prints the two values the
# libmoq configure needs:
#
#     MOQ_PICOQUIC_SOURCE_DIR=<deps>/picoquic
#     MOQ_PICOTLS_PREFIX=<deps>/picotls/build
#
# Then configure libmoq, e.g.:
#
#     # Sets MOQ_PICOQUIC_SOURCE_DIR / MOQ_PICOTLS_PREFIX as shell
#     # variables in the CURRENT shell (KEY=VALUE, not `export`); pass
#     # them explicitly to cmake in the same shell as below.
#     eval "$(scripts/setup_picoquic_deps.sh)"
#     cmake -B build/pq-ci \
#         -DMOQ_BUILD_ADAPTER_PICOQUIC=ON \
#         -DMOQ_BUILD_PQ_THREADED=ON \
#         -DMOQ_BUILD_TESTS=ON \
#         -DMOQ_PICOQUIC_SOURCE_DIR="$MOQ_PICOQUIC_SOURCE_DIR" \
#         -DMOQ_PICOTLS_PREFIX="$MOQ_PICOTLS_PREFIX"
#
# The two vars are also written to <deps>/picoquic_deps.env (source-able)
# and, under GitHub Actions, appended to $GITHUB_ENV so later steps see
# them. Because the printed lines are KEY=VALUE (not `export`), `eval`
# only sets them for the current shell - it does not export them to child
# processes, which is why the example passes them via -D... explicitly.
#
# Env overrides (all optional):
#   MOQ_DEPS_DIR    deps root            (default: <repo>/.deps/picoquic-ci)
#   PICOQUIC_REPO   picoquic remote      (default: private-octopus/picoquic)
#   PICOQUIC_REF    picoquic commit      (default: pinned below)
#   PICOTLS_REPO    picotls remote       (default: h2o/picotls)
#   PICOTLS_REF     picotls commit       (default: pinned below)
#   OPENSSL_ROOT_DIR  passed to picotls' cmake if set (macOS/brew)
#   MOQ_PICOTLS_BUILD_CONFIG  dependency configuration (default: Release)
#
# The tracked dependency patches (patches/picoquic, patches/picotls) are
# applied after checkout, in the order their `series` manifests declare (see
# scripts/provider_patches.sh, which other recipes run directly).
# Already-patched inputs are accepted; conflicting inputs fail before any
# dependency build.
#   --check-patch-consumers
#       offline check that those other recipes apply patches only through
#       scripts/provider_patches.sh and use the picotls build-directory layout.
# --self-test checks patching, the patch consumers and the compiler policy
# with offline fixtures.
#
# Requires: git, cmake, a C compiler, and OpenSSL dev headers
# (Ubuntu: apt-get install -y libssl-dev cmake).
# --self-test additionally requires Ninja and CMake 3.20 or newer.
# Multi-config LibMoQ build-tree exports remain separate portability work.
#
# Windows (Git Bash + Visual Studio 2022): picotls also needs pkg-config, and
# neither it nor OpenSSL ships with the toolchain. E.g. with vcpkg:
#   vcpkg install openssl pkgconf --triplet x64-windows-static-md
#   export OPENSSL_ROOT_DIR=<vcpkg>/installed/x64-windows-static-md
#   export PKG_CONFIG=$OPENSSL_ROOT_DIR/tools/pkgconf/pkgconf.exe

set -euo pipefail

# -- Pinned, known-good dependency commits -----------------------------
PICOQUIC_REPO="${PICOQUIC_REPO:-https://github.com/private-octopus/picoquic}"
PICOQUIC_REF="${PICOQUIC_REF:-70b5fd5aafdd2d6c349ff783202ece9d778bcaf2}"
PICOTLS_REPO="${PICOTLS_REPO:-https://github.com/h2o/picotls.git}"
PICOTLS_REF="${PICOTLS_REF:-7f0d38c1c54c920d56b4e13ceb1c2dfa2b1325e1}"

script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)

# shellcheck source=provider_patches.sh
. "$script_dir/provider_patches.sh"

# The patch lists are the providers' series manifests.
PICOQUIC_PATCHES=()
while IFS= read -r _moq_patch; do PICOQUIC_PATCHES+=("$_moq_patch"); done \
    < <(read_patch_series "$repo_root/patches/picoquic/series")
PICOTLS_PATCHES=()
while IFS= read -r _moq_patch; do PICOTLS_PATCHES+=("$_moq_patch"); done \
    < <(read_patch_series "$repo_root/patches/picotls/series")
unset _moq_patch
MOQ_DEPS_DIR="${MOQ_DEPS_DIR:-$repo_root/.deps/picoquic-ci}"

picoquic_dir="$MOQ_DEPS_DIR/picoquic"
picotls_dir="$MOQ_DEPS_DIR/picotls"
picotls_build="$picotls_dir/build"

log() { printf '[setup_picoquic_deps] %s\n' "$*" >&2; }
die() { printf '[setup_picoquic_deps] ERROR: %s\n' "$*" >&2; exit 1; }

build_picotls() {
    local source_dir=$1 build_dir=$2
    local config=${MOQ_PICOTLS_BUILD_CONFIG:-Release}
    # picotls's configure writes a scratch file into the process's working
    # directory (its USDT probe check runs `dtrace -o .tmp.dprobes.h`) and
    # removes it relative to its own source directory instead. Configure and
    # build therefore run from inside the build directory, so nothing lands
    # in the caller's tree; the paths are made absolute first.
    mkdir -p "$build_dir" || return $?
    source_dir=$(cd "$source_dir" && pwd) || return $?
    build_dir=$(cd "$build_dir" && pwd) || return $?
    local cmake_args=(-S "$source_dir" -B "$build_dir" "-DCMAKE_BUILD_TYPE=$config"
        "-DCMAKE_PROJECT_picotls_INCLUDE=$repo_root/cmake/MoqPicotlsBuildPolicy.cmake")
    # Same Apple Clang ABI-probe correction as the LibMoQ root (see
    # docs/cmake-abi-probes.md), registered before picotls enables C/C++.
    # A toolchain selected through the CMAKE_TOOLCHAIN_FILE environment
    # variable owns its rules hooks, so it is left alone.
    if [ -z "${CMAKE_TOOLCHAIN_FILE:-}" ]; then
        cmake_args+=("-DCMAKE_PROJECT_picotls_INCLUDE_BEFORE=$repo_root/cmake/MoqAppleAbiProbe.cmake")
    fi
    if [ -n "${OPENSSL_ROOT_DIR:-}" ]; then
        cmake_args+=(-DOPENSSL_ROOT_DIR="$OPENSSL_ROOT_DIR")
    fi
    # Preserve both streams for the caller's log (normal mode routes stdout
    # to stderr below so the two result assignments remain machine-readable).
    (
        cd "$build_dir" || exit $?
        cmake "${cmake_args[@]}" || exit $?
        # Only the library targets used by FindPTLS; not dependency CLI/test tools.
        cmake --build "$build_dir" --config "$config" \
            --target picotls-core picotls-openssl picotls-minicrypto \
            -j"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)"
    )
}

# build_picotls must leave the caller's working directory and the LibMoQ
# checkout untouched, even though picotls's configure writes a scratch file
# into the process's working directory. The fixture reproduces that write.
build_cwd_selftest() (
    set -eu
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    local stray=.tmp.dprobes.h repo_had=0
    [ -e "$repo_root/$stray" ] && repo_had=1
    mkdir -p "$tmp/source" "$tmp/caller"
    cat > "$tmp/source/CMakeLists.txt" <<'CMAKE'
cmake_minimum_required(VERSION 3.13)
project(picotls C)
# Same shape as picotls's USDT check: output relative to the working directory.
execute_process(COMMAND "${CMAKE_COMMAND}" -E touch .tmp.dprobes.h)
foreach(target picotls-core picotls-openssl picotls-minicrypto)
    add_library(${target} STATIC probe.c)
endforeach()
CMAKE
    printf 'int moq_cwd_probe(void) { return 0; }\n' > "$tmp/source/probe.c"
    (cd "$tmp/caller" && build_picotls ../source ../build) > "$tmp/build.log" 2>&1 || {
        cat "$tmp/build.log" >&2; die "self-test: working-directory fixture build failed"
    }
    [ -z "$(ls -A "$tmp/caller")" ] \
        || die "self-test: building picotls wrote into the caller's directory: $(ls -A "$tmp/caller")"
    [ -e "$tmp/build/$stray" ] \
        || die "self-test: the fixture's working-directory write did not happen in the build directory"
    if [ "$repo_had" = 0 ] && [ -e "$repo_root/$stray" ]; then
        die "self-test: building picotls left $stray in the LibMoQ checkout"
    fi
    printf 'picotls build working-directory self-test: caller directory and checkout untouched PASS\n'
)

compiler_policy_selftest() (
    set -eu
    # Each call captures both streams, including Ninja's stdout diagnostics.
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    mkdir -p "$tmp/source"
    cat > "$tmp/source/CMakeLists.txt" <<'CMAKE'
cmake_minimum_required(VERSION 3.13)
project(picotls C)
foreach(target picotls-core picotls-openssl picotls-minicrypto)
    add_library(${target} STATIC probe.c)
    target_compile_options(${target} PRIVATE -Wall)
endforeach()
CMAKE
    cat > "$tmp/source/probe.c" <<'C'
#include <assert.h>
#ifdef NDEBUG
#error picotls_assertions_must_remain_enabled
#endif
#ifdef MOQ_EXPECT_CACHED
#if MOQ_CACHE_FLAG != 17
#error picotls_cached_flags_must_be_preserved
#endif
#elif MOQ_ENV_FLAG != 23
#error picotls_environment_flags_must_be_preserved
#endif
int picotls_policy_probe(void) { int value = 1; assert(value); return 0; }
C
    CFLAGS='-DMOQ_ENV_FLAG=23 -fPIC' build_picotls "$tmp/source" "$tmp/build" > "$tmp/positive.log" 2>&1 || {
        cat "$tmp/positive.log" >&2; die "self-test: assertion-enabled build failed"
    }
    for target in core openssl minicrypto; do
        test -f "$tmp/build/libpicotls-$target.a" || die "self-test: missing $target archive"
    done
    cmake -S "$tmp/source" -B "$tmp/cached-build" \
        '-DCMAKE_C_FLAGS=-DMOQ_CACHE_FLAG=17 -fPIC' \
        '-DCMAKE_C_FLAGS_RELEASE=-O2 -DNDEBUG -DMOQ_EXPECT_CACHED=1' > "$tmp/cache.log" 2>&1 || {
        cat "$tmp/cache.log" >&2; die "self-test: cache fixture configure failed"
    }
    build_picotls "$tmp/source" "$tmp/cached-build" >> "$tmp/cache.log" 2>&1 || {
        cat "$tmp/cache.log" >&2; die "self-test: cached flags build failed"
    }
    mkdir -p "$tmp/multi-source" "$tmp/msvc-source"
    cat > "$tmp/multi-source/CMakeLists.txt" <<'CMAKE'
cmake_minimum_required(VERSION 3.20)
project(picotls C)
if(DEFINED CMAKE_BUILD_TYPE)
    message(STATUS "Single-config selection: ${CMAKE_BUILD_TYPE}")
endif()
foreach(target picotls-core picotls-openssl picotls-minicrypto)
    add_library(${target} STATIC probe.c)
    target_compile_options(${target} PRIVATE -Wall)
    target_compile_definitions(${target} PRIVATE
        "$<$<CONFIG:Debug>:MOQ_ACTUAL_DEBUG=1>"
        "$<$<CONFIG:Release>:MOQ_ACTUAL_RELEASE=1>")
endforeach()
CMAKE
    cat > "$tmp/multi-source/probe.c" <<'C'
#include <assert.h>
#ifdef NDEBUG
#error picotls_multiconfig_assertions_must_remain_enabled
#endif
#if MOQ_ENV_FLAG != 23 || MOQ_CACHE_FLAG != 17
#error picotls_multiconfig_caller_flags_must_be_preserved
#endif
#if defined(MOQ_EXPECT_DEBUG) && !defined(MOQ_ACTUAL_DEBUG)
#error picotls_wrong_debug_configuration
#endif
#if defined(MOQ_EXPECT_RELEASE) && !defined(MOQ_ACTUAL_RELEASE)
#error picotls_wrong_release_configuration
#endif
int picotls_policy_probe(void) { int value = 1; assert(value); return 0; }
C
    for config in Debug Release; do
        expected=$(printf '%s' "$config" | tr '[:lower:]' '[:upper:]')
        build="$tmp/multi-$config"
        CFLAGS='-DMOQ_ENV_FLAG=23 -fPIC' cmake \
            -G 'Ninja Multi-Config' -S "$tmp/multi-source" -B "$build" \
            "-DCMAKE_C_FLAGS_${expected}=-DMOQ_CACHE_FLAG=17 -DMOQ_EXPECT_${expected}=1 -DNDEBUG" \
            > "$tmp/multi-$config.log" 2>&1 || {
            cat "$tmp/multi-$config.log" >&2; die "self-test: multi-config configure failed"
        }
        MOQ_PICOTLS_BUILD_CONFIG=$config build_picotls "$tmp/multi-source" "$build" \
            >> "$tmp/multi-$config.log" 2>&1 || {
            cat "$tmp/multi-$config.log" >&2; die "self-test: selected $config build failed"
        }
        for target in core openssl minicrypto; do
            test -f "$build/$config/libpicotls-$target.a" \
                || die "self-test: selected $config $target archive missing"
            for other in Debug Release RelWithDebInfo; do
                [ "$other" = "$config" ] || test ! -f "$build/$other/libpicotls-$target.a" \
                    || die "self-test: unselected $other archive built"
            done
        done
    done
    printf '\nstatic int moq_multi_unused_function(void) { return 0; }\n' >> "$tmp/multi-source/probe.c"
    for config in Debug Release; do
        if MOQ_PICOTLS_BUILD_CONFIG=$config build_picotls "$tmp/multi-source" \
            "$tmp/multi-$config" > "$tmp/multi-warning-$config.log" 2>&1; then
            die "self-test: $config multi-config warning was accepted"
        fi
        grep -q 'error:.*moq_multi_unused_function' "$tmp/multi-warning-$config.log" || {
            cat "$tmp/multi-warning-$config.log" >&2; die "self-test: wrong $config warning refusal"
        }
    done
    # Exercise policy selection only: no host compiler pretends to be MSVC.
    cat > "$tmp/msvc-source/CMakeLists.txt" <<'CMAKE'
cmake_minimum_required(VERSION 3.20)
project(policy_branch NONE)
set(MSVC TRUE)
add_compile_options(/DMOQ_CALLER_FLAG=17)
include("${MOQ_POLICY}")
get_directory_property(actual COMPILE_OPTIONS)
if(NOT "${actual}" STREQUAL "/DMOQ_CALLER_FLAG=17;/UNDEBUG")
    message(FATAL_ERROR "MSVC policy options changed: ${actual}")
endif()
CMAKE
    cmake -S "$tmp/msvc-source" -B "$tmp/msvc-build" \
        "-DMOQ_POLICY=$repo_root/cmake/MoqPicotlsBuildPolicy.cmake" \
        > "$tmp/msvc.log" 2>&1 || {
        cat "$tmp/msvc.log" >&2; die "self-test: MSVC policy branch control failed"
    }
    printf '\nstatic int moq_setup_unused_function(void) { return 0; }\n' >> "$tmp/source/probe.c"
    if CFLAGS='-DMOQ_ENV_FLAG=23' build_picotls "$tmp/source" "$tmp/warning-build" > "$tmp/warning.log" 2>&1; then
        die "self-test: compiler warning was accepted"
    fi
    grep -q 'error:.*moq_setup_unused_function' "$tmp/warning.log" || {
        cat "$tmp/warning.log" >&2; die "self-test: wrong warning refusal"
    }
    printf 'picotls compiler policy self-test: assertions, caller flags, three archives, warning refusal, Debug/Release selection, MSVC source control PASS\n'
)

# Patching itself lives in scripts/provider_patches.sh; the lists and the
# patch directory follow repo_root, which the self-tests point at fixtures.
apply_picoquic_patch() { provider_apply_patch picoquic "$repo_root/patches/picoquic" "$@"; }
apply_picoquic_patches() {
    provider_apply_series picoquic "$repo_root/patches/picoquic" "$1" "${PICOQUIC_PATCHES[@]}"
}
apply_picotls_patch() { provider_apply_patch picotls "$repo_root/patches/picotls" "$@"; }
apply_picotls_patches() {
    provider_apply_series picotls "$repo_root/patches/picotls" "$1" "${PICOTLS_PATCHES[@]}"
}

# Write the minimal upstream context of a patch under <root>: each file holds
# only the pre-image of its hunks (context and removed lines), which the patch
# then applies to at an offset.
patch_preimage() {
    local patch=$1 root=$2
    awk -v root="$root" '
        /^diff --git / { file = ""; hunk = 0; next }
        /^--- / && !hunk { next }
        /^\+\+\+ b\// && !hunk {
            file = root "/" substr($0, 7)
            dir = file; sub(/\/[^\/]*$/, "", dir)
            system("mkdir -p \"" dir "\"")
            next
        }
        /^@@ / { hunk = (file != ""); next }
        hunk && /^[ -]/ { print substr($0, 2) > file; next }
        hunk && /^$/ { print "" > file; next }
        hunk && /^\+/ { next }
        { hunk = 0 }
    ' "$patch"
}

patch_selftest() (
    set -eu
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    for name in "${PICOQUIC_PATCHES[@]}"; do
        [ -f "$repo_root/patches/picoquic/$name" ] || die "self-test: missing $name"
    done
    local close_patch=0005-own-application-close-reason.patch
    local patch="$repo_root/patches/picoquic/$close_patch"
    mkdir -p "$tmp/tree"
    git -C "$tmp/tree" init -q
    # Minimal upstream context. The behavioural regression is
    # test_pq_close_reason.
    patch_preimage "$patch" "$tmp/tree"
    local sender="$tmp/tree/picoquic/sender.c"
    [ -f "$sender" ] || die "self-test: no fixture for $close_patch"
    apply_picoquic_patch "$tmp/tree" "$close_patch"
    grep -qF '#define PICOQUIC_CLOSE_REASON_COPIED 1' "$tmp/tree/picoquic/picoquic.h"
    grep -qF 'cnx->local_error_reason = reason_copy;' "$sender"
    cp -R "$tmp/tree/picoquic" "$tmp/expected"
    apply_picoquic_patch "$tmp/tree" "$close_patch"
    diff -r "$tmp/expected" "$tmp/tree/picoquic" >/dev/null
    printf 'conflicting source\n' > "$sender"
    if (apply_picoquic_patch "$tmp/tree" "$close_patch") > "$tmp/log" 2>&1; then
        die "self-test: conflicting source was accepted"
    fi
    grep -qF "picoquic patch $close_patch conflicts" "$tmp/log"
    test "$(cat "$sender")" = 'conflicting source'
    if (repo_root="$tmp/missing"; apply_picoquic_patch "$tmp/tree" "$close_patch") > "$tmp/log" 2>&1; then
        die "self-test: missing patch was accepted"
    fi
    grep -qF 'missing picoquic patch' "$tmp/log"
    mkdir -p "$tmp/tree/child"
    if (apply_picoquic_patch "$tmp/tree/child" "$close_patch") > "$tmp/log" 2>&1; then
        die "self-test: non-root patch input was accepted"
    fi
    grep -qF 'picoquic patch input is not its own git root' "$tmp/log"
    printf 'close-reason patch self-test: apply, repeat, conflict, missing PASS\n'
)

picotls_patch_selftest() (
    set -eu
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    local scratch_patch=0001-initialize-compiler-visible-scratch.patch
    local member_patch=0002-minicrypto-distinct-chacha20-object.patch
    local name
    for name in "$scratch_patch" "$member_patch"; do
        [ -f "$repo_root/patches/picotls/$name" ] || die "self-test: missing $name"
    done
    mkdir -p "$tmp/tree"
    git -C "$tmp/tree" init -q
    patch_preimage "$repo_root/patches/picotls/$scratch_patch" "$tmp/tree"
    patch_preimage "$repo_root/patches/picotls/$member_patch" "$tmp/tree"
    # The rename carries no hunk; any content stands in for the wrapper.
    mkdir -p "$tmp/tree/lib/cifra"
    printf '/* picotls chacha20 wrappers */\n' > "$tmp/tree/lib/cifra/chacha20.c"
    cp "$tmp/tree/lib/cifra/chacha20.c" "$tmp/wrapper"
    local uecc="$tmp/tree/deps/micro-ecc/uECC.c" test_src="$tmp/tree/t/picotls.c"
    local cmake="$tmp/tree/CMakeLists.txt"
    [ -f "$uecc" ] && [ -f "$test_src" ] && [ -f "$cmake" ] \
        || die "self-test: no fixture for the picotls patches"
    # micro-ecc is a submodule of picotls: 0001 crosses that boundary.
    git -C "$tmp/tree/deps/micro-ecc" init -q
    apply_picotls_patches "$tmp/tree"
    test "$(grep -cF 't5[uECC_MAX_WORDS] = {0};' "$uecc")" = 2
    grep -qF 'ptls_cipher_suite_t *selected = NULL;' "$test_src"
    # The wrapper moved byte for byte and every reference followed it.
    test ! -e "$tmp/tree/lib/cifra/chacha20.c"
    cmp "$tmp/wrapper" "$tmp/tree/lib/cifra/minicrypto-chacha20.c"
    test "$(grep -cF 'lib/cifra/minicrypto-chacha20.c' "$cmake")" = 3
    ! grep -qF 'lib/cifra/chacha20.c' "$cmake"
    mkdir -p "$tmp/expected-dir"
    cp -R "$tmp/tree/deps" "$tmp/tree/t" "$tmp/tree/lib" "$cmake" "$tmp/expected-dir/"
    apply_picotls_patches "$tmp/tree"
    cmp "$tmp/expected-dir/deps/micro-ecc/uECC.c" "$uecc"
    cmp "$tmp/expected-dir/t/picotls.c" "$test_src"
    cmp "$tmp/expected-dir/CMakeLists.txt" "$cmake"
    cmp "$tmp/wrapper" "$tmp/tree/lib/cifra/minicrypto-chacha20.c"
    printf 'conflicting source\n' > "$test_src"
    if (apply_picotls_patch "$tmp/tree" "$scratch_patch") > "$tmp/log" 2>&1; then
        die "self-test: conflicting picotls source was accepted"
    fi
    grep -qF "picotls patch $scratch_patch conflicts" "$tmp/log"
    test "$(cat "$test_src")" = 'conflicting source'
    cmp "$tmp/expected-dir/deps/micro-ecc/uECC.c" "$uecc"
    printf 'conflicting source\n' > "$cmake"
    if (apply_picotls_patch "$tmp/tree" "$member_patch") > "$tmp/log" 2>&1; then
        die "self-test: conflicting picotls build description was accepted"
    fi
    grep -qF "picotls patch $member_patch conflicts" "$tmp/log"
    test "$(cat "$cmake")" = 'conflicting source'
    cmp "$tmp/wrapper" "$tmp/tree/lib/cifra/minicrypto-chacha20.c"
    if (repo_root="$tmp/missing"; apply_picotls_patch "$tmp/tree" "$scratch_patch") > "$tmp/log" 2>&1; then
        die "self-test: missing picotls patch was accepted"
    fi
    grep -qF 'missing picotls patch' "$tmp/log"
    if (apply_picotls_patch "$tmp/tree/t" "$scratch_patch") > "$tmp/log" 2>&1; then
        die "self-test: non-root picotls patch input was accepted"
    fi
    grep -qF 'picotls patch input is not its own git root' "$tmp/log"
    if (PICOTLS_PATCHES=("$scratch_patch" "$member_patch" "$scratch_patch"); \
        apply_picotls_patches "$tmp/tree") > "$tmp/log" 2>&1; then
        die "self-test: a picotls patch listed twice was accepted"
    fi
    grep -qF "picotls patch series lists $scratch_patch twice" "$tmp/log"
    printf 'picotls patch self-test: apply, repeat, submodule path, rename, conflict, missing, list PASS\n'
)

patch_list_selftest() (
    set -eu
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    repo_root=$tmp/root
    PICOQUIC_PATCHES=(0001-one.patch 0002-two.patch)
    mkdir -p "$repo_root/patches/picoquic" "$tmp/tree"
    git -C "$tmp/tree" init -q
    for pair in 0001-one 0002-two; do
        name=${pair#*-}
        printf 'old\n' > "$tmp/tree/$name.txt"
        printf '%s\n' "diff --git a/$name.txt b/$name.txt" \
            "--- a/$name.txt" "+++ b/$name.txt" '@@ -1 +1 @@' '-old' '+new' \
            > "$repo_root/patches/picoquic/$pair.patch"
    done
    apply_picoquic_patches "$tmp/tree"
    test "$(cat "$tmp/tree/one.txt")" = new
    test "$(cat "$tmp/tree/two.txt")" = new
    apply_picoquic_patches "$tmp/tree"
    test "$(cat "$tmp/tree/one.txt")" = new
    test "$(cat "$tmp/tree/two.txt")" = new
    printf 'old\n' > "$tmp/tree/one.txt"
    printf 'old\n' > "$tmp/tree/two.txt"
    mv "$repo_root/patches/picoquic/0002-two.patch" "$tmp/two.patch"
    if (apply_picoquic_patches "$tmp/tree") > "$tmp/log" 2>&1; then
        die "self-test: missing second patch was accepted"
    fi
    grep -qF 'missing picoquic patch:' "$tmp/log"
    test "$(cat "$tmp/tree/one.txt")" = old
    mv "$tmp/two.patch" "$repo_root/patches/picoquic/0002-two.patch"
    if (PICOQUIC_PATCHES=(0001-one.patch 0001-one.patch); \
        apply_picoquic_patches "$tmp/tree") > "$tmp/log" 2>&1; then
        die "self-test: a patch listed twice was accepted"
    fi
    grep -qF 'picoquic patch series lists 0001-one.patch twice' "$tmp/log"
    test "$(cat "$tmp/tree/one.txt")" = old
    printf 'conflicting source\n' > "$tmp/tree/two.txt"
    if (apply_picoquic_patches "$tmp/tree") > "$tmp/log" 2>&1; then
        die "self-test: conflicting second patch was accepted"
    fi
    grep -qF 'picoquic patch 0002-two.patch conflicts' "$tmp/log"
    test "$(cat "$tmp/tree/two.txt")" = 'conflicting source'
    printf 'patch list self-test: all members, repeat, missing, duplicate, conflict PASS\n'
)

# Recipes outside this script that prepare picoquic/picotls checkouts. Each
# must apply the tracked patches with `provider_patches.sh apply` against the
# providers' series manifests (never a list of its own) and hand LibMoQ the
# picotls build-directory layout this script produces: picotls configured
# into <picotls>/build and that directory passed as MOQ_PICOTLS_PREFIX.
PATCH_CONSUMERS=(
    tools/moq-interop-client/Dockerfile
)

# check_picoquic_series_contracts <patch-dir> <series names...>
# The picoquic adapters gate behaviour on contract macros a tracked patch
# introduces (PICOQUIC_*_CONTRACT_VERSION); D22 attach is refused without
# them. Every such macro the adapter sources test must be defined by a
# patch in the series.
check_picoquic_series_contracts() (
    set -eu
    local patch_dir=$1 macro name found
    shift
    local macros
    macros=$(grep -rhoE 'PICOQUIC_[A-Z0-9_]+_CONTRACT_VERSION' \
        "$repo_root/adapters/picoquic" "$repo_root/adapters/pico_wt" \
        "$repo_root/adapters/common" --include='*.c' --include='*.h' 2>/dev/null | sort -u)
    for macro in $macros; do
        found=0
        for name in "$@"; do
            if grep -qE "^\+[[:space:]]*#[[:space:]]*define[[:space:]]+$macro([[:space:]]|$)" "$patch_dir/$name"; then
                found=1
                break
            fi
        done
        [ "$found" = 1 ] \
            || die "the picoquic series defines no $macro, which the picoquic adapters require"
    done
)

check_patch_consumers() (
    set -eu
    local recipe path name provider build prefix joined tmp
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    for recipe in "${PATCH_CONSUMERS[@]}"; do
        path="$repo_root/$recipe"
        [ -f "$path" ] || die "patch consumer missing: $recipe"
        # One logical line per instruction: join backslash continuations.
        joined=$tmp/joined
        awk '{ if (sub(/\\$/, "")) printf "%s ", $0; else print }' "$path" > "$joined"
        for provider in picoquic picotls; do
            grep -qE "provider_patches\.sh[[:space:]]+apply[[:space:]]+[^[:space:]]*patches/$provider[[:space:]]" "$joined" \
                || die "$recipe does not apply the $provider series through provider_patches.sh apply"
        done
        # Neither a patch file path nor a tracked patch name may appear:
        # either would be a second, divergent declaration of the list.
        if grep -qE 'patches/(picoquic|picotls)/[^[:space:]]' "$joined"; then
            die "$recipe names a patch file instead of applying the series"
        fi
        for provider in picoquic picotls; do
            for name in "$repo_root/patches/$provider/"*.patch; do
                [ -f "$name" ] || continue
                name=${name##*/}
                if grep -qF "${name%.patch}" "$joined"; then
                    die "$recipe names tracked patch $name"
                fi
            done
        done
        build=$(sed -nE 's/.*cmake[[:space:]]+-B[[:space:]]+([^[:space:]]+)[[:space:]]+-S[[:space:]]+([^[:space:]]*picotls)([[:space:]].*)?$/\2|\1/p' "$joined")
        prefix=$(sed -nE 's/.*-DMOQ_PICOTLS_PREFIX=([^[:space:]\\]+).*/\1/p' "$joined")
        [ -n "$build" ] || die "$recipe: no picotls configure step found"
        [ -n "$prefix" ] || die "$recipe: no MOQ_PICOTLS_PREFIX found"
        case "${build#*|}" in
            "${build%%|*}/build"|*"/${build%%|*}/build") ;;
            *) die "$recipe builds picotls into ${build#*|}, not <picotls>/build" ;;
        esac
        case "$prefix" in
            "${build#*|}"|*"/${build#*|}") ;;
            *) die "$recipe passes MOQ_PICOTLS_PREFIX=$prefix, not its picotls build directory ${build#*|}" ;;
        esac
    done
    check_picoquic_series_contracts "$repo_root/patches/picoquic" "${PICOQUIC_PATCHES[@]}"
    printf 'patch consumers check: %s apply the series manifests and use the picotls build layout; the picoquic series provides the adapter contracts PASS\n' \
        "${PATCH_CONSUMERS[*]}"
)

overlap_selftest() (
    set -eu
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    repo_root=$tmp/root
    PICOQUIC_PATCHES=(0001-add.patch 0002-rewrite.patch)
    mkdir -p "$repo_root/patches/picoquic" "$tmp/tree"
    git -C "$tmp/tree" init -q
    printf 'a\nb\n' > "$tmp/tree/f.txt"
    # The second patch rewrites the line the first one adds, so once both
    # are in, the first no longer reverse-applies on its own.
    printf '%s\n' 'diff --git a/f.txt b/f.txt' '--- a/f.txt' '+++ b/f.txt' \
        '@@ -1,2 +1,3 @@' ' a' '+x' ' b' > "$repo_root/patches/picoquic/0001-add.patch"
    printf '%s\n' 'diff --git a/f.txt b/f.txt' '--- a/f.txt' '+++ b/f.txt' \
        '@@ -1,3 +1,3 @@' ' a' '-x' '+y' ' b' > "$repo_root/patches/picoquic/0002-rewrite.patch"
    apply_picoquic_patches "$tmp/tree"
    test "$(cat "$tmp/tree/f.txt")" = "$(printf 'a\ny\nb')"
    apply_picoquic_patches "$tmp/tree" 2> "$tmp/log"
    grep -qF 'picoquic patch 0001-add.patch already applied' "$tmp/log"
    grep -qF 'picoquic patch 0002-rewrite.patch already applied' "$tmp/log"
    test "$(cat "$tmp/tree/f.txt")" = "$(printf 'a\ny\nb')"
    # First patch only: the second is applied on top.
    printf 'a\nx\nb\n' > "$tmp/tree/f.txt"
    apply_picoquic_patches "$tmp/tree" 2> "$tmp/log"
    grep -qF 'picoquic patch 0001-add.patch already applied' "$tmp/log"
    test "$(cat "$tmp/tree/f.txt")" = "$(printf 'a\ny\nb')"
    printf 'a\nz\nb\n' > "$tmp/tree/f.txt"
    if (apply_picoquic_patches "$tmp/tree") > "$tmp/log" 2>&1; then
        die "self-test: conflicting overlapping series was accepted"
    fi
    grep -qF 'conflicts' "$tmp/log"
    printf 'overlap self-test: fresh, fully applied, prefix applied, conflict PASS\n'
)

series_selftest() (
    set -eu
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    local dir=$tmp/patches/picoquic helper=$script_dir/provider_patches.sh
    mkdir -p "$dir" "$tmp/tree"
    git -C "$tmp/tree" init -q
    printf 'old\n' > "$tmp/tree/one.txt"
    printf 'old\n' > "$tmp/tree/two.txt"
    for pair in 0001-one 0002-two; do
        name=${pair#*-}
        printf '%s\n' "diff --git a/$name.txt b/$name.txt" \
            "--- a/$name.txt" "+++ b/$name.txt" '@@ -1 +1 @@' '-old' '+new' \
            > "$dir/$pair.patch"
    done
    # The real series name existing patches once each and provide the
    # picoquic contracts the adapters require.
    provider_check_series picoquic "$repo_root/patches/picoquic" "${PICOQUIC_PATCHES[@]}"
    provider_check_series picotls "$repo_root/patches/picotls" "${PICOTLS_PATCHES[@]}"
    check_picoquic_series_contracts "$repo_root/patches/picoquic" "${PICOQUIC_PATCHES[@]}"
    # Without the entry that defines the D22 stream-terminal contract
    # (found by content, wherever it sits in the series), the real picoquic
    # series no longer provides it.
    local short=() contract_patch="" name
    for name in "${PICOQUIC_PATCHES[@]}"; do
        if grep -qE '^\+[[:space:]]*#[[:space:]]*define[[:space:]]+PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION([[:space:]]|$)' \
            "$repo_root/patches/picoquic/$name"; then
            contract_patch=$name
        else
            short+=("$name")
        fi
    done
    [ -n "$contract_patch" ] \
        || die "self-test: no picoquic series entry defines PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION"
    if (check_picoquic_series_contracts "$repo_root/patches/picoquic" "${short[@]}") > "$tmp/log" 2>&1; then
        die "self-test: a picoquic series without $contract_patch passed the contract check"
    fi
    grep -qF 'PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION' "$tmp/log"
    printf '# comment\n0001-one.patch\n\n0002-two.patch\n' > "$dir/series"
    bash "$helper" apply "$dir" "$tmp/tree" 2> "$tmp/log"
    test "$(cat "$tmp/tree/one.txt")" = new
    test "$(cat "$tmp/tree/two.txt")" = new
    bash "$helper" apply "$dir" "$tmp/tree" 2> "$tmp/log"
    grep -qF 'picoquic patch 0002-two.patch already applied' "$tmp/log"
    printf 'old\n' > "$tmp/tree/one.txt"
    printf 'old\n' > "$tmp/tree/two.txt"
    # The series is the selection: an unlisted patch file is not applied.
    printf '0001-one.patch\n' > "$dir/series"
    bash "$helper" apply "$dir" "$tmp/tree" 2> "$tmp/log"
    test "$(cat "$tmp/tree/one.txt")" = new
    test "$(cat "$tmp/tree/two.txt")" = old
    grep -qF 'picoquic patch 0002-two.patch is not in the series; not applied' "$tmp/log"
    printf 'old\n' > "$tmp/tree/one.txt"
    printf '0001-one.patch\n0001-one.patch\n' > "$dir/series"
    if bash "$helper" apply "$dir" "$tmp/tree" > "$tmp/log" 2>&1; then
        die "self-test: a series listing a patch twice was accepted"
    fi
    grep -qF 'lists 0001-one.patch twice' "$tmp/log"
    printf '0001-one.patch\n0002-two.patch\n0003-three.patch\n' > "$dir/series"
    if bash "$helper" apply "$dir" "$tmp/tree" > "$tmp/log" 2>&1; then
        die "self-test: a series naming a missing patch was accepted"
    fi
    grep -qF 'missing picoquic patch' "$tmp/log"
    test "$(cat "$tmp/tree/one.txt")" = old
    printf '0001-one.patch\n0002-two.patch\n' > "$dir/series"
    printf 'conflicting source\n' > "$tmp/tree/two.txt"
    if bash "$helper" apply "$dir" "$tmp/tree" > "$tmp/log" 2>&1; then
        die "self-test: a conflicting checkout was accepted"
    fi
    grep -qF 'picoquic patch 0002-two.patch conflicts' "$tmp/log"
    test "$(cat "$tmp/tree/one.txt")" = old
    printf 'series self-test: real series and contracts, apply, repeat, unlisted patch, duplicate, missing, conflict PASS\n'
)

# The series trial must never act outside a scratch directory it created
# and verified, and a failed preparation must stop the apply before the
# checkout changes. Both are driven from a strict parent through command
# substitution, as provider_apply_series calls the trial, with rm, mkdir, cp
# and git replaced by recording functions; nothing outside $tmp is touched.
trial_safety_selftest() (
    set -eu
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    local helper=$script_dir/provider_patches.sh dir=$tmp/patches/picoquic
    mkdir -p "$dir" "$tmp/tree" "$tmp/scratch-root"
    git -C "$tmp/tree" init -q
    printf 'old\n' > "$tmp/tree/one.txt"
    printf '%s\n' 'diff --git a/one.txt b/one.txt' '--- a/one.txt' '+++ b/one.txt' \
        '@@ -1 +1 @@' '-old' '+new' > "$dir/0001-one.patch"
    cp "$tmp/tree/one.txt" "$tmp/one.before"

    # mktemp fails: no further filesystem or git operation, no result.
    : > "$tmp/rec"
    (
        set -euo pipefail
        . "$helper"
        mktemp() { printf 'mktemp %s\n' "$*" >> "$tmp/rec"; return 1; }
        rm() { printf 'rm %s\n' "$*" >> "$tmp/rec"; }
        mkdir() { printf 'mkdir %s\n' "$*" >> "$tmp/rec"; }
        cp() { printf 'cp %s\n' "$*" >> "$tmp/rec"; }
        git() { printf 'git %s\n' "$*" >> "$tmp/rec"; }
        status=0
        out=$(provider_applied_prefix "$dir" "$tmp/tree" 0001-one.patch) || status=$?
        printf 'status=%s out=<%s>\n' "$status" "$out" > "$tmp/result"
    )
    grep -qxE 'status=[1-9][0-9]* out=<>' "$tmp/result" \
        || die "self-test: a failed mktemp produced a trial result: $(cat "$tmp/result")"
    [ "$(grep -cv '^mktemp ' "$tmp/rec")" = 0 ] \
        || die "self-test: the trial acted after a failed mktemp: $(grep -v '^mktemp ' "$tmp/rec" | head -3)"

    # The scratch copy fails: the apply stops before touching the checkout,
    # and every path the trial removed, created or wrote is in its scratch.
    : > "$tmp/rec"
    if (
        set -euo pipefail
        . "$helper"
        rm() { printf 'rm %s\n' "$*" >> "$tmp/rec"; command rm "$@"; }
        mkdir() { printf 'mkdir %s\n' "$*" >> "$tmp/rec"; command mkdir "$@"; }
        cp() { printf 'cp %s\n' "$*" >> "$tmp/rec"; return 1; }
        TMPDIR=$tmp/scratch-root
        provider_apply_series picoquic "$dir" "$tmp/tree" 0001-one.patch
    ) > "$tmp/log" 2>&1; then
        die "self-test: a failed trial copy still applied the series"
    fi
    grep -qF 'could not trial the picoquic series' "$tmp/log" \
        || die "self-test: a failed trial copy was not reported as such: $(tail -1 "$tmp/log")"
    cmp -s "$tmp/one.before" "$tmp/tree/one.txt" \
        || die "self-test: the checkout changed after a failed trial copy"
    grep -q '^cp ' "$tmp/rec" || die "self-test: the trial copy was never attempted"
    local line word last
    while IFS= read -r line; do
        last=${line##* }
        for word in ${line#* }; do
            case "$word" in
                /*) ;;
                *) continue ;;
            esac
            # cp reads from the checkout; only its destination must be scratch.
            if [ "${line%% *}" = cp ] && [ "$word" != "$last" ]; then
                continue
            fi
            case "$word" in
                "$tmp/scratch-root/moq-patch-trial."*) ;;
                *) die "self-test: the trial reached outside its scratch: $line" ;;
            esac
        done
    done < "$tmp/rec"
    [ -z "$(ls -A "$tmp/scratch-root")" ] \
        || die "self-test: the trial left its scratch directory behind"
    printf 'trial safety self-test: failed mktemp, failed scratch copy PASS\n'
)

case "${1:-}" in
    --self-test)
        patch_selftest
        picotls_patch_selftest
        patch_list_selftest
        overlap_selftest
        series_selftest
        trial_safety_selftest
        build_cwd_selftest
        check_patch_consumers
        compiler_policy_selftest
        exit $?
        ;;
    --check-patch-consumers)
        check_patch_consumers
        exit $?
        ;;
esac

# Only the two KEY=VALUE result lines may reach real stdout (so callers
# can `eval "$(...)"`). Route everything else - including git/cmake and
# git-submodule chatter, which print to stdout - to stderr via fd 3.
exec 3>&1 1>&2

command -v git   >/dev/null 2>&1 || die "git not found"
command -v cmake >/dev/null 2>&1 || die "cmake not found"

# Without OpenSSL picotls silently drops its OpenSSL backend and the build then
# fails on the missing picotls-openssl target; on Windows neither OpenSSL nor
# pkg-config comes with the toolchain, so check up front.
case "$(uname -s)" in
MINGW* | MSYS* | CYGWIN*)
    if [ -z "${PKG_CONFIG:-}" ] && ! command -v pkg-config >/dev/null 2>&1; then
        die "pkg-config not found (picotls requires it); install one (e.g. vcpkg pkgconf) and set PKG_CONFIG=/path/to/pkgconf.exe"
    fi
    [ -n "${OPENSSL_ROOT_DIR:-}" ] \
        || die "OPENSSL_ROOT_DIR not set; point it at an OpenSSL install (e.g. vcpkg x64-windows-static-md)"
    ;;
esac

# Fetch a repo at an exact commit into $dir (idempotent, shallow when
# the server allows fetch-by-SHA, which GitHub does).
fetch_at() {
    local repo=$1 ref=$2 dir=$3
    if [ ! -d "$dir/.git" ]; then
        log "init $dir"
        mkdir -p "$dir"
        git -C "$dir" init -q
        git -C "$dir" remote add origin "$repo"
    fi
    git -C "$dir" remote set-url origin "$repo"
    if git -C "$dir" cat-file -e "${ref}^{commit}" 2>/dev/null; then
        log "$dir already has $ref"
    elif git -C "$dir" fetch --depth 1 origin "$ref" 2>/dev/null; then
        log "fetched $ref (shallow) from $repo"
    else
        log "shallow fetch unavailable; full fetch from $repo"
        git -C "$dir" fetch origin
    fi
    git -C "$dir" checkout -q --detach "$ref"
    local got
    got=$(git -C "$dir" rev-parse HEAD)
    [ "$got" = "$ref" ] || die "checkout mismatch in $dir: want $ref got $got"
}

mkdir -p "$MOQ_DEPS_DIR"
log "deps dir: $MOQ_DEPS_DIR"

fetch_at "$PICOQUIC_REPO" "$PICOQUIC_REF" "$picoquic_dir"
apply_picoquic_patches "$picoquic_dir"
fetch_at "$PICOTLS_REPO"  "$PICOTLS_REF"  "$picotls_dir"

# picotls carries minicrypto backends as submodules pinned by the parent
# commit - deterministic with the pinned ref.
log "picotls submodules"
git -C "$picotls_dir" submodule update --init --recursive --depth 1
# The tracked picotls patches may touch submodule sources, so they follow it.
apply_picotls_patches "$picotls_dir"

log "building picotls -> $picotls_build"
build_picotls "$picotls_dir" "$picotls_build"

# Sanity: picotls-core archive must exist where FindPicoquic expects it.
ls "$picotls_build"/libpicotls-core.* >/dev/null 2>&1 \
    || [ -f "$picotls_build/picotls-core.lib" ] \
    || die "picotls build produced no libpicotls-core in $picotls_build"

# -- Emit the two configure inputs -------------------------------------
# stdout (fd 3) and the env file carry Bash-source/eval-safe assignments:
# the values are %q-escaped so paths with spaces or shell metacharacters
# survive `eval "$(setup_picoquic_deps.sh)"` and `. picoquic_deps.env`.
# Nothing else reaches fd 3, so the eval consumes exactly these two lines.
env_file="$MOQ_DEPS_DIR/picoquic_deps.env"
{
    printf 'MOQ_PICOQUIC_SOURCE_DIR=%q\n' "$picoquic_dir"
    printf 'MOQ_PICOTLS_PREFIX=%q\n'      "$picotls_build"
} | tee "$env_file" >&3

# GitHub Actions: make them available to subsequent steps. $GITHUB_ENV is
# NOT a shell file - Actions parses it as raw NAME=value env-file lines,
# so these stay unescaped (unlike the eval/source output above).
if [ -n "${GITHUB_ENV:-}" ]; then
    gh_picoquic_dir=$picoquic_dir
    gh_picotls_build=$picotls_build
    if command -v cygpath >/dev/null 2>&1; then
        gh_picoquic_dir=$(cygpath -m "$picoquic_dir")
        gh_picotls_build=$(cygpath -m "$picotls_build")
    fi
    {
        printf 'MOQ_PICOQUIC_SOURCE_DIR=%s\n' "$gh_picoquic_dir"
        printf 'MOQ_PICOTLS_PREFIX=%s\n'      "$gh_picotls_build"
    } >> "$GITHUB_ENV"
fi

log "done. env written to $env_file"
