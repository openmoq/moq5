#!/usr/bin/env bash
#
# Install-tree consumer test for raw picoquic managed (moq_pq_threaded) +
# the certificate-verifier helper (<moq/picoquic_verify.h>).
#
# Installs picoquic and libmoq to a temp prefix, then consumes the threaded
# facade + verifier helper against THAT prefix — not the build tree — two
# ways:
#   (A) CMake: find_package(libmoq COMPONENTS adapter-picoquic-threaded)
#              + link moq::adapter-picoquic-threaded
#   (B) pkg-config: pkg-config --cflags --libs libmoq
# The consumer source includes only <moq/picoquic_threaded.h> +
# <moq/picoquic_verify.h>, with no source-tree include paths.
#
# Args:
#   1  cmake executable
#   2  consumer source dir (CMake project: consumer-threaded-install)
#   3  consumer .c source  (same project's main.c, for the pkg-config path)
#   4  libmoq build dir (already built, MOQ_BUILD_PQ_THREADED=ON)
#   5  picoquic source dir
#   6  picotls prefix (…/picotls/build)
#   7  C compiler
#   8  C flags        (forwarded so ASAN parent → ASAN consumer)
#   9  exe linker flags
#  10  build type
#  11  openssl root   (may be empty)
#  12  work dir       (created fresh; removed on success unless retained below)
#  13  install libdir (CMAKE_INSTALL_LIBDIR: lib, lib64, lib/<arch>, …)
#  14  C++ flags (may be empty)
#  15  generated parent staging check
#  16  install include dir (the parent's CMAKE_INSTALL_INCLUDEDIR)
#  17  test-owned toolchain/rules preload (required)
# Test-only: MOQ_TEST_KEEP_WORK=1 retains successful work and prints its path.
# Unset or any other value preserves the default success cleanup.
set -euo pipefail

CMAKE="$1"; CONSUMER_SRC="$2"; CONSUMER_MAIN="$3"; LIBMOQ_BUILD="$4"
PICOQUIC_SRC="$5"; PICOTLS_PREFIX="$6"; CC="$7"; CFLAGS="$8"; LDFLAGS="$9"
BUILD_TYPE="${10}"; OPENSSL_ROOT="${11}"; WORK="${12}"; LIBDIR="${13}"
CXXFLAGS="${14}"
STAGING_CHECK="${15}"
INCLUDEDIR="${16}"
# Final argument: test-owned toolchain/rules preload, not installed metadata.
ABI_PRELOAD="${17}"

P="$WORK/prefix"
PQB="$WORK/pq-build"
CONB="$WORK/consumer-build"

if [ -z "$PICOTLS_PREFIX" ]; then
    PICOTLS_PREFIX="$(cd "$PICOQUIC_SRC/../picotls/build" && pwd)"
fi
# picotls as picoquic's child build looks for it: an installed prefix keeps
# headers and libraries in include/ and lib/; a checkout build directory
# holds the libraries itself, with the headers in ../include.
if [ -f "$PICOTLS_PREFIX/include/picotls.h" ] && [ -d "$PICOTLS_PREFIX/lib" ]; then
    PTLS_SRC="$PICOTLS_PREFIX"; PTLS_LIB="$PICOTLS_PREFIX/lib"
else
    PTLS_SRC="$(dirname "$PICOTLS_PREFIX")"; PTLS_LIB="$PICOTLS_PREFIX"
fi

rm -rf "$WORK"
mkdir -p "$WORK"

"$CMAKE" -Dphase=before -P "$STAGING_CHECK"

ossl_arg=()
if [ -n "$OPENSSL_ROOT" ]; then
    ossl_arg=(-DOPENSSL_ROOT_DIR="$OPENSSL_ROOT")
fi

echo "== 1. install picoquic -> $P =="
"$CMAKE" -C "$ABI_PRELOAD" -S "$PICOQUIC_SRC" -B "$PQB" \
    -DBUILD_HTTP=ON -DBUILD_DEMO=OFF -DBUILD_PQBENCH=OFF \
    -DBUILD_PICO_SIM=OFF -Dpicoquic_BUILD_TESTS=OFF -DBUILD_LOGLIB=ON \
    -DCMAKE_INSTALL_PREFIX="$P" \
    -DPTLS_PREFIX="$PTLS_SRC" \
    -DPTLS_INCLUDE_DIR="$PTLS_SRC/include" \
    -DCMAKE_LIBRARY_PATH="$PTLS_LIB" \
    "${ossl_arg[@]}" \
    -DCMAKE_C_COMPILER="$CC" \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
"$CMAKE" --build "$PQB"
"$CMAKE" --install "$PQB"

echo "== 2. install libmoq -> $P =="
"$CMAKE" --install "$LIBMOQ_BUILD" --prefix "$P"
"$CMAKE" -Dphase=after -Dprefix="$P" -P "$STAGING_CHECK"

# Installed-header manifest: both public headers ship.
for hdr in picoquic_threaded.h picoquic_verify.h; do
    if [ ! -f "$P/$INCLUDEDIR/moq/$hdr" ]; then
        echo "FAIL: moq/$hdr was not installed"; exit 1
    fi
done

echo "== 3A. CMake consumer (find_package adapter-picoquic-threaded) =="
# Deliberately NOT passing MOQ_PICOQUIC_SOURCE_DIR: forces resolution of
# the INSTALLED picoquic.
"$CMAKE" -C "$ABI_PRELOAD" -S "$CONSUMER_SRC" -B "$CONB" \
    -DCMAKE_PREFIX_PATH="$P" \
    "${ossl_arg[@]}" \
    -DCMAKE_C_COMPILER="$CC" \
    -DCMAKE_C_FLAGS="$CFLAGS" \
    -DCMAKE_CXX_FLAGS="$CXXFLAGS" \
    -DCMAKE_EXE_LINKER_FLAGS="$LDFLAGS" \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
"$CMAKE" --build "$CONB"
# The consumer owns the loader path for this temporary install. On Linux,
# an executable's RUNPATH does not cover transitive shared dependencies.
# Keep caller-provided dependency directories after the installed libraries.
LD_LIBRARY_PATH="$P/$LIBDIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" "$CONB/moq_adapter_threaded_install_consumer_test"
LD_LIBRARY_PATH="$P/$LIBDIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" "$CONB/moq_adapter_threaded_install_consumer_cxx_test"

echo "== 3B. pkg-config consumer (libmoq) =="
if ! command -v pkg-config >/dev/null 2>&1; then
    echo "SKIP: pkg-config not found (CMake consumer already passed)"
else
    export PKG_CONFIG_PATH="$P/$LIBDIR/pkgconfig"
    if ! pkg-config --exists libmoq; then
        echo "FAIL: pkg-config could not find libmoq in $PKG_CONFIG_PATH"
        exit 1
    fi
    PC_CFLAGS="$(pkg-config --cflags libmoq)"
    PC_LIBS="$(pkg-config --libs libmoq)"
    echo "   cflags: $PC_CFLAGS"
    echo "   libs:   $PC_LIBS"
    # Guard: no source-tree include must leak through pkg-config cflags.
    # read decodes pkg-config's backslash escapes without evaluating shell code.
    # Compare complete paths: /pico must not also match /out/picoquic/include.
    pc_flags=()
    read -a pc_flags <<< "$PC_CFLAGS"
    for pc_flag in ${pc_flags[@]+"${pc_flags[@]}"}; do
        case "$pc_flag" in
            -I*) pc_path=${pc_flag#-I};;
            -isystem*) pc_path=${pc_flag#-isystem};;
            -iquote*) pc_path=${pc_flag#-iquote};;
            -idirafter*) pc_path=${pc_flag#-idirafter};;
            *) pc_path=$pc_flag;;
        esac
        for pc_root in "$PICOQUIC_SRC" "$PTLS_SRC"; do
            pc_root=${pc_root%/}
            case "$pc_path" in
                "$pc_root"|"$pc_root"/*)
                    echo "FAIL: pkg-config cflags leak a source-tree path"; exit 1;;
            esac
        done
    done
    "$CC" $CFLAGS $PC_CFLAGS -c "$CONSUMER_MAIN" -o "$WORK/pkgc.o"
    # pkg-config does not choose the application's runtime search policy.
    runtime_flags=()
    case "$(uname -s)" in
        Darwin|Linux) runtime_flags=("-Wl,-rpath,$P/$LIBDIR");;
    esac
    "$CC" $LDFLAGS "$WORK/pkgc.o" $PC_LIBS "${runtime_flags[@]}" -o "$WORK/pkgc-consumer"
    LD_LIBRARY_PATH="$P/$LIBDIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" "$WORK/pkgc-consumer"
fi

echo "== install-tree threaded consumer OK (CMake + pkg-config) =="
if [[ "${MOQ_TEST_KEEP_WORK:-0}" == 1 ]]; then
    printf 'MOQ_TEST_RETAINED_WORK=%s\n' "$WORK"
else
    rm -rf "$WORK"
fi
