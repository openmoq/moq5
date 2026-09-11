#!/usr/bin/env bash
# Build the pinned mbedTLS release used by the PicoQUIC mbedTLS CI lane.

set -euo pipefail

MBEDTLS_VERSION=3.6.6
MBEDTLS_SHA256=8fb65fae8dcae5840f793c0a334860a411f884cc537ea290ce1c52bb64ca007a
MBEDTLS_URL="https://github.com/Mbed-TLS/mbedtls/releases/download/mbedtls-${MBEDTLS_VERSION}/mbedtls-${MBEDTLS_VERSION}.tar.bz2"

script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
deps_dir="${MOQ_MBEDTLS_DEPS_DIR:-$repo_root/.deps/mbedtls-ci}"
archive="$deps_dir/mbedtls-${MBEDTLS_VERSION}.tar.bz2"
source_dir="$deps_dir/mbedtls-${MBEDTLS_VERSION}"
prefix="$deps_dir/prefix"

die() { printf '[setup_mbedtls_deps] ERROR: %s\n' "$*" >&2; exit 1; }
command -v curl >/dev/null 2>&1 || die "curl not found"
command -v make >/dev/null 2>&1 || die "make not found"

mkdir -p "$deps_dir"
if [ ! -f "$archive" ]; then
    curl --fail --location --retry 3 --output "$archive" "$MBEDTLS_URL"
fi

if command -v sha256sum >/dev/null 2>&1; then
    actual=$(sha256sum "$archive" | awk '{print $1}')
else
    actual=$(shasum -a 256 "$archive" | awk '{print $1}')
fi
[ "$actual" = "$MBEDTLS_SHA256" ] || die "archive SHA-256 mismatch"

if [ ! -f "$source_dir/CMakeLists.txt" ]; then
    tar -xjf "$archive" -C "$deps_dir"
fi

make -C "$source_dir/library" clean
make -C "$source_dir" lib -j"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)"
mkdir -p "$prefix/include" "$prefix/lib"
cp -R "$source_dir/include/mbedtls" "$prefix/include/"
cp -R "$source_dir/include/psa" "$prefix/include/"
cp "$source_dir"/library/libmbedcrypto.a \
   "$source_dir"/library/libmbedx509.a \
   "$source_dir"/library/libmbedtls.a "$prefix/lib/"

test -f "$prefix/include/mbedtls/mbedtls_config.h" || \
    die "installed headers are incomplete"
test -f "$prefix/lib/libmbedtls.a" || die "libmbedtls.a was not installed"

printf 'MBEDTLS_PREFIX=%q\n' "$prefix"
printf 'MBEDTLS_INCLUDE_DIR=%q\n' "$prefix/include"
printf 'MBEDTLS_LIBRARY=%q\n' "$prefix/lib/libmbedtls.a"
printf 'MBEDTLS_X509=%q\n' "$prefix/lib/libmbedx509.a"
printf 'MBEDTLS_CRYPTO=%q\n' "$prefix/lib/libmbedcrypto.a"
if [ -n "${GITHUB_ENV:-}" ]; then
    {
        printf 'MBEDTLS_PREFIX=%s\n' "$prefix"
        printf 'MBEDTLS_INCLUDE_DIR=%s\n' "$prefix/include"
        printf 'MBEDTLS_LIBRARY=%s\n' "$prefix/lib/libmbedtls.a"
        printf 'MBEDTLS_X509=%s\n' "$prefix/lib/libmbedx509.a"
        printf 'MBEDTLS_CRYPTO=%s\n' "$prefix/lib/libmbedcrypto.a"
    } >> "$GITHUB_ENV"
fi
