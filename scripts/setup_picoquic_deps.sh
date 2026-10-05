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
#
# The pinned picoquic includes the capsule-parser cursor reset upstream.
# --self-test checks the compiler policy with offline fixtures.
#
# Requires: git, cmake, a C compiler, and OpenSSL dev headers
# (Ubuntu: apt-get install -y libssl-dev cmake).

set -euo pipefail

# -- Pinned, known-good dependency commits -----------------------------
PICOQUIC_REPO="${PICOQUIC_REPO:-https://github.com/private-octopus/picoquic}"
# picoquic tag v2026.10.04.b4075dd, pinned by its immutable commit.
PICOQUIC_REF="${PICOQUIC_REF:-b4075dd184f4ae3276e4347308ffc7b0cfac737f}"
PICOTLS_REPO="${PICOTLS_REPO:-https://github.com/h2o/picotls.git}"
# picotls publishes no tags; pin master as of 2026-10-05 (commit 2026-09-30).
PICOTLS_REF="${PICOTLS_REF:-f06553b877ada93c860dd1db9002eabf7365292d}"

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
    local cmake_args=(-S "$source_dir" -B "$build_dir" -DCMAKE_BUILD_TYPE=Release
        "-DCMAKE_PROJECT_picotls_INCLUDE=$repo_root/cmake/MoqPicotlsBuildPolicy.cmake")
    if [ -n "${OPENSSL_ROOT_DIR:-}" ]; then
        cmake_args+=(-DOPENSSL_ROOT_DIR="$OPENSSL_ROOT_DIR")
    fi
    cmake "${cmake_args[@]}" >/dev/null || return $?
    # Only the library targets used by FindPTLS; not dependency CLI/test tools.
    cmake --build "$build_dir" \
        --target picotls-core picotls-openssl picotls-minicrypto \
        -j"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)" >/dev/null
}

compiler_policy_selftest() (
    set -eu
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
    printf '\nstatic int moq_setup_unused_function(void) { return 0; }\n' >> "$tmp/source/probe.c"
    if CFLAGS='-DMOQ_ENV_FLAG=23' build_picotls "$tmp/source" "$tmp/warning-build" > "$tmp/warning.log" 2>&1; then
        die "self-test: compiler warning was accepted"
    fi
    grep -q 'error:.*moq_setup_unused_function' "$tmp/warning.log" || {
        cat "$tmp/warning.log" >&2; die "self-test: wrong warning refusal"
    }
    printf 'picotls compiler policy self-test: assertions, caller flags, three archives, warning refusal PASS\n'
)

if [ "${1:-}" = "--self-test" ]; then
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
fetch_at "$PICOTLS_REPO"  "$PICOTLS_REF"  "$picotls_dir"

# picotls carries minicrypto backends as submodules pinned by the parent
# commit - deterministic with the pinned ref.
log "picotls submodules"
git -C "$picotls_dir" submodule update --init --recursive --depth 1

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
