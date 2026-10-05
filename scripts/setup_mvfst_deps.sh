#!/usr/bin/env bash
# Build corrected May25 mvfst from a caller-supplied immutable archive, offline.
# Required: MVFST_ARCHIVE, folly_DIR, Fizz_DIR, fmt_DIR, OPENSSL_INCLUDE_DIR,
# OPENSSL_SSL_LIBRARY, OPENSSL_CRYPTO_LIBRARY (all absolute existing paths).
# MVFST_DEPS_DIR selects the private output parent (default .deps/mvfst-ci).
# Extra arguments are normal CMake configure options for the caller's closure
# and toolchain. Each invocation retains a fresh source/build/prefix and logs.
set -euo pipefail
if [ "${1:-}" = --help ]; then
    sed -n '2,7s/^# \{0,1\}//p' "$0"
    exit 0
fi
script_dir=$(cd "$(dirname "$0")" && pwd)
fail() { printf 'setup_mvfst_deps: %s\n' "$*" >&2; exit 1; }
digest() { shasum -a 256 "$1" | awk '{print $1}'; }
for name in MVFST_ARCHIVE folly_DIR Fizz_DIR fmt_DIR OPENSSL_INCLUDE_DIR OPENSSL_SSL_LIBRARY OPENSSL_CRYPTO_LIBRARY; do
    value=${!name:-}
    case "$value" in /*) ;; *) fail "$name must be an explicit absolute path" ;; esac
    [ -e "$value" ] || fail "$name does not exist: $value"
done
[ -f "$folly_DIR/folly-config.cmake" ] || fail 'folly_DIR must contain folly-config.cmake'
[ -f "$Fizz_DIR/fizz-config.cmake" ] || fail 'Fizz_DIR must contain fizz-config.cmake'
[ -f "$fmt_DIR/fmt-config.cmake" ] || fail 'fmt_DIR must contain fmt-config.cmake'
[ -f "$OPENSSL_INCLUDE_DIR/openssl/opensslv.h" ] || fail 'OPENSSL_INCLUDE_DIR must contain openssl/opensslv.h'
[ -f "$OPENSSL_SSL_LIBRARY" ] && [ -f "$OPENSSL_CRYPTO_LIBRARY" ] || fail 'OpenSSL library inputs must be files'
ref=d48af283a4d14583382288bd57b7093ffb80dbf0
archive_sha=ec88bcabc7f8689348ad7b3a5ee4313f0dd196df16959e16ea62ab6cbd46d337
patch_dir="$script_dir/../cmake/patches"
annotations="$patch_dir/mvfst-2026.05.25-nullability.patch"
hostid="$patch_dir/mvfst-2026.05.25-hostid-guard.patch"
[ "$(digest "$MVFST_ARCHIVE")" = "$archive_sha" ] || fail 'source archive SHA256 mismatch (no CMake invoked)'
[ "$(digest "$annotations")" = 90082465ef41158ee20391f0d88234b0390f10a7c3f30795f1e9912a00015d31 ] || fail 'annotation patch SHA256 mismatch (no CMake invoked)'
[ "$(digest "$hostid")" = 95c18e1adeaca4f1f11d55bd2942ca1559a7311c1fa9ad3b9c82ecfd92437a47 ] || fail 'HostId patch SHA256 mismatch (no CMake invoked)'
# Inputs controlling ownership, correction identity, tests and warning policy
# belong to this recipe, not extra arguments. Dependency/toolchain flags remain
# caller-owned and are recorded verbatim below.
for arg in "$@"; do
    case "$arg" in
        -S*|-B*|--install*|-D|-Dfolly_DIR*|-DFizz_DIR*|-Dfmt_DIR*|-DOPENSSL_INCLUDE_DIR*|-DOPENSSL_SSL_LIBRARY*|-DOPENSSL_CRYPTO_LIBRARY*|-DCMAKE_INSTALL_*|-DINCLUDE_INSTALL_DIR*|-DCMAKE_BUILD_TYPE*|-DBUILD_TESTS*|-DCMAKE_COMPILE_WARNING_AS_ERROR*)
            fail "reserved configure argument: $arg" ;;
    esac
done
deps_dir=${MVFST_DEPS_DIR:-"$script_dir/../.deps/mvfst-ci"}
mkdir -p "$deps_dir"
deps_dir=$(cd "$deps_dir" && pwd)
run_dir=$(mktemp -d "$deps_dir/build.XXXXXXXX")
printf 'setup_mvfst_deps: retained run %s\n' "$run_dir" >&2
mkdir "$run_dir/source"
tar -xzf "$MVFST_ARCHIVE" --strip-components=1 -C "$run_dir/source"
git -C "$run_dir/source" apply --check "$annotations" "$hostid"
git -C "$run_dir/source" apply "$annotations" "$hostid"
prefix="$run_dir/prefix"
{
    printf 'source_commit=%s\narchive_sha256=%s\n' "$ref" "$archive_sha"
    shasum -a 256 "$annotations" "$hostid" "$OPENSSL_SSL_LIBRARY" "$OPENSSL_CRYPTO_LIBRARY"
    for name in folly_DIR Fizz_DIR fmt_DIR OPENSSL_INCLUDE_DIR OPENSSL_SSL_LIBRARY OPENSSL_CRYPTO_LIBRARY; do
        printf '%s=%q\n' "$name" "${!name}"
    done
    printf 'extra_cmake_args='; printf '%q ' "$@"; printf '\n'
} > "$run_dir/inputs.txt"
run() {
    local phase=$1
    shift
    if ! "$@" > "$run_dir/$phase.log" 2>&1; then
        cat "$run_dir/$phase.log" >&2
        fail "$phase failed; retained $run_dir"
    fi
    if LC_ALL=C grep -Ei '(^|[^[:alpha:]])warning([[:space:]:]|$)|\[-Werror' "$run_dir/$phase.log" >/dev/null; then
        cat "$run_dir/$phase.log" >&2
        fail "$phase emitted diagnostics; retained $run_dir"
    fi
}
config=${MVFST_CONFIG:-Release}
run configure cmake -S "$run_dir/source" -B "$run_dir/build" \
    "-DCMAKE_BUILD_TYPE=$config" -DBUILD_SHARED_LIBS=ON \
    "-Dfolly_DIR=$folly_DIR" "-DFizz_DIR=$Fizz_DIR" "-Dfmt_DIR=$fmt_DIR" \
    "-DOPENSSL_INCLUDE_DIR=$OPENSSL_INCLUDE_DIR" \
    "-DOPENSSL_SSL_LIBRARY=$OPENSSL_SSL_LIBRARY" \
    "-DOPENSSL_CRYPTO_LIBRARY=$OPENSSL_CRYPTO_LIBRARY" "$@" \
    "-DCMAKE_INSTALL_PREFIX=$prefix" -DBUILD_TESTS=OFF -DCMAKE_COMPILE_WARNING_AS_ERROR=ON
run build cmake --build "$run_dir/build" --config "$config" \
    --parallel "${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
run install cmake --install "$run_dir/build" --config "$config"
configs=()
while IFS= read -r config; do configs+=("$config"); done < <(find "$prefix" -name mvfst-config.cmake -type f)
[ "${#configs[@]}" -eq 1 ] || fail "expected exactly one installed mvfst-config.cmake in $prefix"
{
    printf 'mvfst_DIR=%q\n' "$(dirname "${configs[0]}")"
    printf 'CMAKE_PREFIX_PATH=%q\n' "$prefix${CMAKE_PREFIX_PATH:+;$CMAKE_PREFIX_PATH}"
} | tee "$run_dir/mvfst_deps.env"
printf 'setup_mvfst_deps: %s plus two reviewed production patches; retained %s\n' "$ref" "$run_dir" >&2
