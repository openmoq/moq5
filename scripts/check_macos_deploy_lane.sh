#!/usr/bin/env bash
#
# check_macos_deploy_lane.sh - deployment-target checks for the macOS
# installed-SDK Swift lane (all C inputs built for an explicit minimum
# macOS against a private static OpenSSL; see build_macos_openssl.sh).
#
#   resolution <target> <openssl-prefix> <picotls-build> <libmoq-build> <install-prefix>
#       Both CMake caches resolved OpenSSL headers and static libraries
#       from <openssl-prefix> and recorded <target>; the libmoq build took
#       picotls headers and libraries from <picotls-build> and its source
#       tree; the installed .pc files point the link at <openssl-prefix>
#       and at no other OpenSSL.
#
#   archives <target> <install-prefix> <picotls-build> <openssl-prefix>
#       Every member of every static archive the lane links (installed
#       LibMoQ and picoquic, picotls, OpenSSL) is a macOS object at or below
#       <target> (macho_deploy_audit.py).
#
#   consumer <target> <install-prefix> <outdir>
#       Builds service/tests/consumer/main.c with nothing but
#       `pkg-config --cflags --libs libmoq-service` from <install-prefix>,
#       at -mmacosx-version-min=<target>. Any compiler or linker warning
#       fails. The binary must declare <target>, link OpenSSL statically
#       (OpenSSL symbols present, no libssl/libcrypto dylib) and run.
#
#   consumer-cmake <target> <install-prefix> <openssl-prefix> <outdir> <abi-preload>
#       The same consumer through the installed CMake package instead:
#       find_package(libmoq CONFIG COMPONENTS service), linked against
#       moq::service, at CMAKE_OSX_DEPLOYMENT_TARGET=<target> with the
#       private OpenSSL. The installed picoquic export must name the
#       OpenSSL::SSL / OpenSSL::Crypto targets, not <openssl-prefix>'s
#       archives by path. The consumer is built twice, declaring
#       cmake_minimum_required 3.20 and 3.20...3.31, and each result is
#       reported. Any CMake, compiler or linker warning in either fails; the
#       binary must declare <target>, link OpenSSL statically and run.
#       abi-preload is the test-owned ConsumerBuildContext output from the
#       SDK configure, not an installed package file.
#
#   diagnostic-logs <log> ... / probe-logs <CMakeFiles-directory>
#       CI-owned diagnostic checks; probe logs permit expected feature-test
#       failures, but require at least one recognized log and no warnings.
#
# Exit status 0 only when every check passes.

set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)

log() { printf '[check_macos_deploy_lane] %s\n' "$*" >&2; }
fail() { printf '[check_macos_deploy_lane] FAIL: %s\n' "$*" >&2; exit 1; }

scan_logs() {
    local pattern=$1 status
    shift
    [ $# -gt 0 ] || { log "FAIL: no diagnostic logs supplied"; return 1; }
    if grep -Ein "$pattern" "$@" >&2; then
        log "FAIL: diagnostic match"
        return 1
    else
        status=$?
        [ "$status" -eq 1 ] || { log "FAIL: diagnostic scanner status $status"; return "$status"; }
    fi
}

check_probe_logs() {
    local dir=$1 probe
    local logs=()
    for probe in "$dir/CMakeConfigureLog.yaml" "$dir/CMakeOutput.log" "$dir/CMakeError.log"; do
        if [ -e "$probe" ] || [ -L "$probe" ]; then logs+=("$probe"); fi
    done
    [ ${#logs[@]} -gt 0 ] || { log "FAIL: no CMake probe logs in $dir"; return 1; }
    scan_logs '(^|[^[:alnum:]_])warning(:|[[:space:]])' "${logs[@]}" || return $?
}

run_logged() {
    local path=$1 status=0
    shift
    "$@" >"$path" 2>"$path.stderr" || status=$?
    printf '%s\n' "$status" >"$path.status" || return $?
    cat "$path" "$path.stderr" >&2 || return $?
    [ "$status" -eq 0 ] || return "$status"
    if grep -Ei '(^|[^[:alnum:]_])(warning(:|[[:space:]])|fatal error(:|[[:space:]])|error(:|[[:space:]]+[[:alpha:]]+[[:digit:]]+:))|(^|[[:space:]])CMake Error([[:space:]]|$)' \
            "$path" "$path.stderr" >&2; then
        log "FAIL: diagnostics in $path"
        return 1
    else
        status=$?
        [ "$status" -eq 1 ] || return "$status"
    fi
}

cache_get() { # <CMakeCache.txt> <VAR>
    sed -n "s/^$2:[A-Z]*=//p" "$1" | head -1
}

check_resolution() {
    local target=$1 ossl=$2 ptls=$3 build=$4 inst=$5
    local c
    for c in "$ptls/CMakeCache.txt" "$build/CMakeCache.txt"; do
        [ -f "$c" ] || fail "missing $c"
        [ "$(cache_get "$c" CMAKE_OSX_DEPLOYMENT_TARGET)" = "$target" ] \
            || fail "$c: CMAKE_OSX_DEPLOYMENT_TARGET is '$(cache_get "$c" CMAKE_OSX_DEPLOYMENT_TARGET)', want $target"
        [ "$(cache_get "$c" OPENSSL_INCLUDE_DIR)" = "$ossl/include" ] \
            || fail "$c: OPENSSL_INCLUDE_DIR is '$(cache_get "$c" OPENSSL_INCLUDE_DIR)'"
        [ "$(cache_get "$c" OPENSSL_CRYPTO_LIBRARY)" = "$ossl/lib/libcrypto.a" ] \
            || fail "$c: OPENSSL_CRYPTO_LIBRARY is '$(cache_get "$c" OPENSSL_CRYPTO_LIBRARY)'"
        local ssl
        ssl=$(cache_get "$c" OPENSSL_SSL_LIBRARY)
        [ -z "$ssl" ] || [ "$ssl" = "$ossl/lib/libssl.a" ] \
            || fail "$c: OPENSSL_SSL_LIBRARY is '$ssl'"
        log "ok: $c resolves OpenSSL from $ossl (static) at $target"
    done
    [ -n "$(cache_get "$build/CMakeCache.txt" OPENSSL_SSL_LIBRARY)" ] \
        || fail "$build/CMakeCache.txt: OPENSSL_SSL_LIBRARY not resolved"
    local v p ptls_src
    ptls_src=$(cd "$ptls/.." && pwd)
    # picotls headers: picoquic's FindPTLS also searches sibling checkouts,
    # so the resolved directory is checked, not assumed.
    for v in PTLS_INCLUDE_DIR MOQ_PTLS_OPENSSL_INCLUDE_DIR; do
        p=$(cache_get "$build/CMakeCache.txt" "$v")
        [ "$p" = "$ptls_src/include" ] \
            || fail "$build/CMakeCache.txt: $v is '$p', want $ptls_src/include"
        log "ok: $v=$p"
    done
    for v in PTLS_CORE_LIBRARY PTLS_OPENSSL_LIBRARY PTLS_MINICRYPTO_LIBRARY; do
        p=$(cache_get "$build/CMakeCache.txt" "$v")
        case "$p" in
        "$ptls"/*.a) log "ok: $v=$p" ;;
        *) fail "$build/CMakeCache.txt: $v is '$p', not under $ptls" ;;
        esac
    done
    local pc libs
    for pc in "$inst/lib/pkgconfig/libmoq.pc" "$inst/lib/pkgconfig/libmoq-service.pc"; do
        [ -f "$pc" ] || fail "missing $pc"
    done
    libs=$(sed -n 's/^Libs: //p' "$inst/lib/pkgconfig/libmoq.pc")
    case " $libs " in
    *" -L$ossl/lib "*) ;;
    *) fail "libmoq.pc Libs does not select $ossl/lib: $libs" ;;
    esac
    case "$libs" in
    *homebrew*|*Cellar*|*GStreamer*|*/usr/local/*) fail "libmoq.pc Libs names another OpenSSL: $libs" ;;
    esac
    log "ok: libmoq.pc Libs: $libs"
}

check_archives() {
    local target=$1 inst=$2 ptls=$3 ossl=$4
    local archives=() listing a
    listing=$(find "$inst/lib" "$ptls" "$ossl/lib" -maxdepth 1 -name '*.a' -type f) || return $?
    while IFS= read -r a; do
        [ -z "$a" ] || archives+=("$a")
    done <<< "$listing"
    [ ${#archives[@]} -gt 0 ] || fail "no archives found"
    python3 "$script_dir/macho_deploy_audit.py" archives --max-macos "$target" \
        "${archives[@]}"
}

check_consumer() {
    local target=$1 inst=$2 out=$3
    mkdir -p "$out"
    local pcdir="$inst/lib/pkgconfig" cflags libs exe="$out/service-consumer-c"
    # Only this prefix's .pc files: no inherited or default search path.
    run_logged "$out/pkg-config-cflags.txt" env PKG_CONFIG_PATH= PKG_CONFIG_LIBDIR="$pcdir" pkg-config --cflags libmoq-service || return $?
    run_logged "$out/pkg-config-libs.txt" env PKG_CONFIG_PATH= PKG_CONFIG_LIBDIR="$pcdir" pkg-config --libs libmoq-service || return $?
    cflags=$(cat "$out/pkg-config-cflags.txt")
    libs=$(cat "$out/pkg-config-libs.txt")
    printf 'cflags: %s\nlibs: %s\n' "$cflags" "$libs" > "$out/pkg-config.txt"
    # shellcheck disable=SC2086
    run_logged "$out/link.log" xcrun --sdk macosx clang -mmacosx-version-min="$target" $cflags \
        "$repo_root/service/tests/consumer/main.c" -o "$exe" $libs || return $?
    python3 "$script_dir/macho_deploy_audit.py" binaries "$exe=$target" \
        || fail "consumer binary does not declare macOS $target"
    run_logged "$out/otool-L.txt" otool -L "$exe" || return $?
    if grep -Eq 'libssl|libcrypto' "$out/otool-L.txt"; then
        cat "$out/otool-L.txt" >&2
        fail "consumer links an OpenSSL dylib"
    fi
    run_logged "$out/nm.txt" nm "$exe" || return $?
    grep -Eq ' T _OPENSSL_init_crypto$' "$out/nm.txt" \
        || fail "consumer does not contain the static OpenSSL it should link"
    run_logged "$out/run.log" "$exe" || return $?
    log "ok: plain-C consumer built warning-free at $target, OpenSSL static, ran"
}

# One installed CMake consumer build at a given policy level; returns
# nonzero (and says why) instead of exiting, so every variant is reported.
cmake_consumer_variant() {
    local target=$1 inst=$2 ossl=$3 out=$4 policy=$5 abi=$6
    [ ! -e "$out" ] || { log "FAIL: use a fresh consumer output: $out"; return 1; }
    mkdir -p "$out/src" || return $?
    cat > "$out/src/CMakeLists.txt" <<CMAKE
cmake_minimum_required(VERSION $policy)
project(moq_service_cmake_consumer C)
find_package(libmoq REQUIRED CONFIG COMPONENTS service)
add_executable(service-consumer-cmake "$repo_root/service/tests/consumer/main.c")
target_link_libraries(service-consumer-cmake PRIVATE moq::service)
CMAKE
    [ $? -eq 0 ] || return 1
    run_logged "$out/configure.log" cmake -C "$abi" -S "$out/src" -B "$out/build" -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_PREFIX_PATH="$inst" -DOPENSSL_ROOT_DIR="$ossl" \
            -DOPENSSL_USE_STATIC_LIBS=TRUE -DCMAKE_OSX_DEPLOYMENT_TARGET="$target" || return $?
    # Failed feature probes are expected; warnings in their retained logs are not.
    check_probe_logs "$out/build/CMakeFiles" || return $?
    run_logged "$out/build.log" cmake --build "$out/build" -- VERBOSE=1 || return $?
    local exe="$out/build/service-consumer-cmake"
    if ! python3 "$script_dir/macho_deploy_audit.py" binaries "$exe=$target"; then
        log "FAIL [policy $policy]: binary does not declare macOS $target"; return 1
    fi
    run_logged "$out/otool-L.txt" otool -L "$exe" || return $?
    if grep -Eq 'libssl|libcrypto' "$out/otool-L.txt"; then
        cat "$out/otool-L.txt" >&2
        log "FAIL [policy $policy]: links an OpenSSL dylib"; return 1
    fi
    run_logged "$out/nm.txt" nm "$exe" || return $?
    if ! grep -Eq ' T _OPENSSL_init_crypto$' "$out/nm.txt"; then
        log "FAIL [policy $policy]: static OpenSSL not linked"; return 1
    fi
    run_logged "$out/run.log" "$exe" || return $?
    log "ok [policy $policy]: built warning-free at $target, OpenSSL static, ran"
}

check_consumer_cmake() {
    local target=$1 inst=$2 ossl=$3 out=$4 abi=$5
    [ -f "$abi" ] && [ -f "$abi.bundle.cmake" ] || fail "missing producer ABI preload/bundle: $abi"
    local export="$inst/lib/cmake/picoquic/picoquic-targets.cmake"
    [ -f "$export" ] || fail "missing $export"
    if grep -Fq "$ossl/lib/" "$export"; then
        grep -Fn "$ossl/lib/" "$export" >&2
        fail "installed picoquic export links OpenSSL by path"
    fi
    grep -q 'OpenSSL::SSL' "$export" && grep -q 'OpenSSL::Crypto' "$export" \
        || fail "installed picoquic export does not name the OpenSSL targets"
    # Both policy levels are supported consumer modes and both are checked:
    # 3.20 alone (CMP0156 OLD: CMake repeats static archives for one-pass
    # linkers) and 3.20...3.31 (CMP0156 NEW: de-duplication per linker).
    local failed=0 policy
    for policy in 3.20 3.20...3.31; do
        cmake_consumer_variant "$target" "$inst" "$ossl" \
            "$out/policy-$policy" "$policy" "$abi" || failed=1
    done
    [ $failed -eq 0 ] || fail "installed CMake consumer failed (see the FAIL lines above)"
    log "ok: installed CMake consumer (moq::service), every policy level"
}

case "${1:-}" in
diagnostic-logs) shift
            # Error severities have a delimiter; prose such as Swift test
            # names containing "error domains" is not a diagnostic.
            scan_logs '(^|[^[:alnum:]_])(warning(:|[[:space:]])|fatal error(:|[[:space:]])|error(:|[[:space:]]+[[:alpha:]]+[[:digit:]]+:))|(^|[[:space:]])CMake Error([[:space:]]|$)' "$@" ;;
probe-logs) [ $# -eq 2 ] || fail "usage: probe-logs <CMakeFiles-directory>"
            check_probe_logs "$2" ;;
resolution) [ $# -eq 6 ] || fail "usage: resolution <target> <openssl-prefix> <picotls-build> <libmoq-build> <install-prefix>"
            check_resolution "$2" "$3" "$4" "$5" "$6" ;;
archives)   [ $# -eq 5 ] || fail "usage: archives <target> <install-prefix> <picotls-build> <openssl-prefix>"
            check_archives "$2" "$3" "$4" "$5" ;;
consumer)   [ $# -eq 4 ] || fail "usage: consumer <target> <install-prefix> <outdir>"
            check_consumer "$2" "$3" "$4" ;;
consumer-cmake) [ $# -eq 6 ] || fail "usage: consumer-cmake <target> <install-prefix> <openssl-prefix> <outdir> <abi-preload>"
            check_consumer_cmake "$2" "$3" "$4" "$5" "$6" ;;
*)          fail "usage: $0 resolution|archives|consumer|consumer-cmake|diagnostic-logs|probe-logs ..." ;;
esac
