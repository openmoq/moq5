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
# applied after checkout. Already-patched inputs are accepted; conflicting
# inputs fail before any dependency build.
# --self-test checks patching and the compiler policy with offline fixtures.
#
# Requires: git, cmake, a C compiler, and OpenSSL dev headers
# (Ubuntu: apt-get install -y libssl-dev cmake).
# --self-test additionally requires Ninja and CMake 3.20 or newer.
# Configuration selection alone does not provide Windows archive discovery or
# multi-config consumer exports; those remain separate portability work.

set -euo pipefail

# -- Pinned, known-good dependency commits -----------------------------
PICOQUIC_REPO="${PICOQUIC_REPO:-https://github.com/private-octopus/picoquic}"
PICOQUIC_REF="${PICOQUIC_REF:-a602e60babea48da7849169d716aaf68684e66d6}"
PICOTLS_REPO="${PICOTLS_REPO:-https://github.com/h2o/picotls.git}"
PICOTLS_REF="${PICOTLS_REF:-f06553b877ada93c860dd1db9002eabf7365292d}"

PICOQUIC_PATCHES=(
    0002-reset-stream-after-fin.patch
    0006-own-application-close-reason.patch
)
PICOTLS_PATCHES=(
    0001-initialize-compiler-visible-scratch.patch
    0002-minicrypto-distinct-chacha20-object.patch
)

script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
MOQ_DEPS_DIR="${MOQ_DEPS_DIR:-$repo_root/.deps/picoquic-ci}"

picoquic_dir="$MOQ_DEPS_DIR/picoquic"
picotls_dir="$MOQ_DEPS_DIR/picotls"
picotls_build="$picotls_dir/build"

log() { printf '[setup_picoquic_deps] %s\n' "$*" >&2; }
die() { printf '[setup_picoquic_deps] ERROR: %s\n' "$*" >&2; exit 1; }

build_picotls() {
    local source_dir=$1 build_dir=$2
    local config=${MOQ_PICOTLS_BUILD_CONFIG:-Release}
    local cmake_args=(-S "$source_dir" -B "$build_dir" "-DCMAKE_BUILD_TYPE=$config"
        "-DCMAKE_PROJECT_picotls_INCLUDE=$repo_root/cmake/MoqPicotlsBuildPolicy.cmake")
    if [ -n "${OPENSSL_ROOT_DIR:-}" ]; then
        cmake_args+=(-DOPENSSL_ROOT_DIR="$OPENSSL_ROOT_DIR")
    fi
    cmake "${cmake_args[@]}" >/dev/null || return $?
    # Only the library targets used by FindPTLS; not dependency CLI/test tools.
    cmake --build "$build_dir" --config "$config" \
        --target picotls-core picotls-openssl picotls-minicrypto \
        -j"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)" >/dev/null
}

compiler_policy_selftest() (
    set -eu
    # Ninja prints compiler diagnostics on stdout, discarded by build_picotls.
    cmake() { command cmake "$@" 1>&2; }
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
if(NOT "${actual}" STREQUAL "/DMOQ_CALLER_FLAG=17;/WX;/UNDEBUG")
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

# Tracked provider patches live in patches/<provider>/ and are applied in the
# declared order. Each one is applied, accepted when already applied, or
# refused when it conflicts, before any dependency build.
apply_provider_patch() {
    local provider=$1 dir=$2 name=$3
    local patch="$repo_root/patches/$provider/$name"
    [ -f "$patch" ] || die "missing $provider patch: $patch"
    # A non-repository child of libmoq can silently skip every patch path.
    [ "$(git -C "$dir" rev-parse --show-toplevel 2>/dev/null)" = "$(cd "$dir" && pwd -P)" ] \
        || die "$provider patch input is not its own git root: $dir"
    if git -C "$dir" apply --check "$patch" >/dev/null 2>&1; then
        git -C "$dir" apply "$patch" || die "$provider patch $name failed to apply"
        log "applied $provider patch $name"
    elif git -C "$dir" apply --reverse --check "$patch" >/dev/null 2>&1; then
        log "$provider patch $name already applied"
    else
        die "$provider patch $name conflicts with $dir"
    fi
}

# apply_provider_patches <provider> <dir> <declared patch names...>
# The declared list must name every tracked patch, in tracked order.
apply_provider_patches() {
    local provider=$1 dir=$2 name i
    shift 2
    local declared=("$@")
    local files=()
    for name in "$repo_root/patches/$provider/"*.patch; do
        [ -f "$name" ] && files+=("$name")
    done
    for name in "${declared[@]}"; do
        [ -f "$repo_root/patches/$provider/$name" ] \
            || die "missing $provider patch: $repo_root/patches/$provider/$name"
    done
    [ "${#files[@]}" -eq "${#declared[@]}" ] \
        || die "declared $provider patch list does not match tracked patches"
    for ((i=0; i<${#files[@]}; ++i)); do
        [ "${files[$i]##*/}" = "${declared[$i]}" ] \
            || die "declared $provider patch list does not match tracked order"
    done
    for name in "${declared[@]}"; do
        apply_provider_patch "$provider" "$dir" "$name"
    done
}

apply_picoquic_patch() { apply_provider_patch picoquic "$@"; }
apply_picoquic_patches() { apply_provider_patches picoquic "$1" "${PICOQUIC_PATCHES[@]}"; }
apply_picotls_patch() { apply_provider_patch picotls "$@"; }
apply_picotls_patches() { apply_provider_patches picotls "$1" "${PICOTLS_PATCHES[@]}"; }

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
    local close_patch=0006-own-application-close-reason.patch
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
    if (PICOTLS_PATCHES=("$scratch_patch"); apply_picotls_patches "$tmp/tree") > "$tmp/log" 2>&1; then
        die "self-test: omitted picotls patch was accepted"
    fi
    grep -qF 'declared picotls patch list does not match tracked patches' "$tmp/log"
    if (PICOTLS_PATCHES=("$member_patch" "$scratch_patch"); apply_picotls_patches "$tmp/tree") > "$tmp/log" 2>&1; then
        die "self-test: reordered picotls patch list was accepted"
    fi
    grep -qF 'declared picotls patch list does not match tracked order' "$tmp/log"
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
    if (PICOQUIC_PATCHES=(0002-two.patch 0001-one.patch); \
        apply_picoquic_patches "$tmp/tree") > "$tmp/log" 2>&1; then
        die "self-test: reversed patch list was accepted"
    fi
    grep -qF 'does not match tracked order' "$tmp/log"
    if (PICOQUIC_PATCHES=(0001-one.patch); \
        apply_picoquic_patches "$tmp/tree") > "$tmp/log" 2>&1; then
        die "self-test: omitted patch was accepted"
    fi
    grep -qF 'does not match tracked patches' "$tmp/log"
    test "$(cat "$tmp/tree/one.txt")" = old
    printf 'conflicting source\n' > "$tmp/tree/two.txt"
    if (apply_picoquic_patches "$tmp/tree") > "$tmp/log" 2>&1; then
        die "self-test: conflicting second patch was accepted"
    fi
    grep -qF 'picoquic patch 0002-two.patch conflicts' "$tmp/log"
    test "$(cat "$tmp/tree/two.txt")" = 'conflicting source'
    printf 'patch list self-test: all members, repeat, missing, order, omission, conflict PASS\n'
)

if [ "${1:-}" = "--self-test" ]; then
    patch_selftest
    picotls_patch_selftest
    patch_list_selftest
    compiler_policy_selftest
    exit $?
fi

# Only the two KEY=VALUE result lines may reach real stdout (so callers
# can `eval "$(...)"`). Route everything else - including git/cmake and
# git-submodule chatter, which print to stdout - to stderr via fd 3.
exec 3>&1 1>&2

command -v git   >/dev/null 2>&1 || die "git not found"
command -v cmake >/dev/null 2>&1 || die "cmake not found"

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
    {
        printf 'MOQ_PICOQUIC_SOURCE_DIR=%s\n' "$picoquic_dir"
        printf 'MOQ_PICOTLS_PREFIX=%s\n'      "$picotls_build"
    } >> "$GITHUB_ENV"
fi

log "done. env written to $env_file"
