#!/usr/bin/env bash
# Build the relay's pinned shared MsQuic provider into a private prefix.
# Usage: bash scripts/setup_msquic_deps.sh [extra CMake configure arguments]
# Requires git, CMake, a C/C++ compiler, Perl, make, and platform development
# headers (Linux: libnuma-dev/libnuma-devel). No system install or sudo.
# MSQUIC_DEPS_DIR overrides .deps/msquic-ci; MSQUIC_REPO overrides the remote.
# The exact upstream release and the two reviewed corrections are a single input.
set -euo pipefail
if [ "${1:-}" = "--help" ]; then
    sed -n '2,7s/^# \{0,1\}//p' "$0"
    exit 0
fi
script_dir=$(cd "$(dirname "$0")" && pwd)
deps_dir=${MSQUIC_DEPS_DIR:-"$script_dir/../.deps/msquic-ci"}
mkdir -p "$deps_dir"
deps_dir=$(cd "$deps_dir" && pwd)
source_dir="$deps_dir/msquic"
prefix="$deps_dir/prefix"
ref=87b53085d76bd7920d490a6f226c9999b6614d14
patch="$script_dir/../cmake/patches/msquic-2.5.9-verifier-c11.patch"
exec 3>&1 1>&2
if [ ! -d "$source_dir/.git" ]; then
    git init -q "$source_dir"
    git -C "$source_dir" remote add origin "${MSQUIC_REPO:-https://github.com/microsoft/msquic.git}"
fi
if [ -n "$(git -C "$source_dir" status --porcelain --untracked-files=all)" ]; then
    echo "setup_msquic_deps: source checkout is dirty: $source_dir" >&2
    exit 1
fi
if ! git -C "$source_dir" cat-file -e "$ref^{commit}" 2>/dev/null; then
    git -C "$source_dir" fetch --depth 1 origin "$ref"
fi
git -C "$source_dir" checkout -q --detach "$ref"
[ "$(git -C "$source_dir" rev-parse HEAD)" = "$ref" ]
# Export the base, leaving its cached checkout pristine. Only the two needed
# submodules are initialized in this owned build source; their gitlinks pin them.
run_dir=$(mktemp -d "$deps_dir/build.XXXXXXXX")
git clone -q --no-hardlinks --no-checkout "$source_dir" "$run_dir/source"
git -C "$run_dir/source" checkout -q --detach "$ref"
git -C "$run_dir/source" remote set-url origin "${MSQUIC_REPO:-https://github.com/microsoft/msquic.git}"
git -C "$run_dir/source" submodule update --init --depth 1 submodules/quictls submodules/clog
git -C "$run_dir/source" apply --check "$patch"
git -C "$run_dir/source" apply "$patch"
cmake -S "$run_dir/source" -B "$run_dir/build" \
    -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="$prefix" \
    -DQUIC_BUILD_TOOLS=OFF -DQUIC_BUILD_TEST=OFF -DQUIC_BUILD_PERF=OFF \
    -DQUIC_ENABLE_LOGGING=OFF -DQUIC_TLS_LIB=quictls "$@" \
    >"$run_dir/configure.log" 2>&1 || {
    cat "$run_dir/configure.log" >&2; exit 1
}
cmake --build "$run_dir/build" --parallel "${CMAKE_BUILD_PARALLEL_LEVEL:-2}" \
    >"$run_dir/build.log" 2>&1 || {
    cat "$run_dir/build.log" >&2; exit 1
}
cmake --install "$run_dir/build" >"$run_dir/install.log" 2>&1 || {
    cat "$run_dir/install.log" >&2; exit 1
}
[ -f "$prefix/share/msquic/msquic-config.cmake" ] || {
    echo 'setup_msquic_deps: installed msquic-config.cmake is missing' >&2
    exit 1
}
{
    printf 'msquic_DIR=%q\n' "$prefix/share/msquic"
    printf 'CMAKE_PREFIX_PATH=%q\n' "$prefix${CMAKE_PREFIX_PATH:+;$CMAKE_PREFIX_PATH}"
} | tee "$deps_dir/msquic_deps.env" >&3
if [ -n "${GITHUB_ENV:-}" ]; then
    printf 'msquic_DIR=%s\n' "$prefix/share/msquic" >> "$GITHUB_ENV"
fi
printf 'MsQuic %s plus %s; build retained at %s\n' "$ref" "$patch" "$run_dir" >&2
