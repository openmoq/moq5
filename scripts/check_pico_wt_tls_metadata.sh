#!/usr/bin/env bash
# Validate the generated PicoWT package metadata for one configured backend.

set -euo pipefail

build_dir=${1:?usage: check_pico_wt_tls_metadata.sh BUILD_DIR BACKEND}
backend=${2:?usage: check_pico_wt_tls_metadata.sh BUILD_DIR BACKEND}
pc="$build_dir/adapters/pico_wt/libmoq-pico-wt.pc"
targets="$build_dir/libmoqPicoWtAdapterTargets.cmake"
config="$build_dir/libmoqConfig.cmake"

for path in "$pc" "$targets" "$config"; do
    [ -f "$path" ] || { echo "FAIL: missing generated file: $path"; exit 1; }
done

case "$backend" in
    mbedtls)
        for token in -lmbedtls -lmbedx509 -lmbedcrypto; do
            grep -F -- "$token" "$pc" >/dev/null || {
                echo "FAIL: $pc omits $token"; exit 1;
            }
        done
        if grep -E 'OpenSSL::|picotls-openssl|-lssl|-lcrypto' \
            "$pc" "$targets" >/dev/null; then
            echo "FAIL: mbedTLS PicoWT metadata retains an OpenSSL dependency"
            exit 1
        fi
        grep -F 'set(_LIBMOQ_ADAPTER_PICOQUIC_NEEDS_OPENSSL OFF)' \
            "$config" >/dev/null || {
                echo "FAIL: package config does not record the mbedTLS backend"; exit 1;
            }
        ;;
    openssl)
        for token in -lpicotls-openssl -lssl -lcrypto; do
            grep -F -- "$token" "$pc" >/dev/null || {
                echo "FAIL: $pc omits $token"; exit 1;
            }
        done
        ;;
    *)
        echo "FAIL: unsupported backend: $backend"
        exit 1
        ;;
esac

echo "PASS: PicoWT $backend package metadata"
