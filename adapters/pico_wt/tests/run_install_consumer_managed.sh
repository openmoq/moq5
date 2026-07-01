#!/usr/bin/env bash
#
# Install-tree consumer test for moq-adapter-pico-wt-managed.
#
# Proves true `make install` consumption of the managed facade: install a
# BUILD_HTTP picoquic and libmoq to a temp prefix, then build the managed
# consumer project against THAT prefix — not the build tree — including
# only <moq/pico_wt_managed.h> and linking only
# moq::adapter-pico-wt-managed via find_package(libmoq COMPONENTS
# adapter-pico-wt-managed).
#
# Args (same shape as run_install_consumer.sh):
#   1  cmake executable
#   2  consumer source dir (the consumer-managed project)
#   3  libmoq build dir (already built, MOQ_BUILD_PICO_WT_MANAGED=ON)
#   4  picoquic source dir
#   5  picotls prefix (…/picotls/build)
#   6  C compiler
#   7  C flags        (forwarded so ASAN parent → ASAN consumer)
#   8  exe linker flags
#   9  build type
#  10  openssl root   (may be empty)
#  11  work dir       (created fresh; removed on success)
set -euo pipefail

CMAKE="$1"; CONSUMER_SRC="$2"; LIBMOQ_BUILD="$3"; PICOQUIC_SRC="$4"
PICOTLS_PREFIX="$5"; CC="$6"; CFLAGS="$7"; LDFLAGS="$8"; BUILD_TYPE="$9"
OPENSSL_ROOT="${10}"; WORK="${11}"

P="$WORK/prefix"
PQB="$WORK/pq-build"
CONB="$WORK/consumer-build"

if [ -z "$PICOTLS_PREFIX" ]; then
    PICOTLS_PREFIX="$(cd "$PICOQUIC_SRC/../picotls/build" && pwd)"
fi
PTLS_SRC="$(dirname "$PICOTLS_PREFIX")"

rm -rf "$WORK"
mkdir -p "$WORK"

ossl_arg=()
if [ -n "$OPENSSL_ROOT" ]; then
    ossl_arg=(-DOPENSSL_ROOT_DIR="$OPENSSL_ROOT")
fi

echo "== 1. install picoquic (BUILD_HTTP=ON) -> $P =="
"$CMAKE" -S "$PICOQUIC_SRC" -B "$PQB" \
    -DBUILD_HTTP=ON -DBUILD_DEMO=OFF -DBUILD_PQBENCH=OFF \
    -DBUILD_PICO_SIM=OFF -Dpicoquic_BUILD_TESTS=OFF -DBUILD_LOGLIB=ON \
    -DCMAKE_INSTALL_PREFIX="$P" \
    -DPTLS_PREFIX="$PTLS_SRC" \
    -DCMAKE_LIBRARY_PATH="$PICOTLS_PREFIX" \
    "${ossl_arg[@]}" \
    -DCMAKE_C_COMPILER="$CC" \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
"$CMAKE" --build "$PQB"
"$CMAKE" --install "$PQB"

echo "== 2. install libmoq -> $P =="
"$CMAKE" --install "$LIBMOQ_BUILD" --prefix "$P"

# Installed-header manifest: both public headers ship; internal headers
# must NOT.
for hdr in pico_wt.h pico_wt_managed.h; do
    if [ ! -f "$P/include/moq/$hdr" ]; then
        echo "FAIL: moq/$hdr was not installed"; exit 1
    fi
done
for hdr in pico_wt_adapter.h pico_wt_endpoint.h; do
    if [ -f "$P/include/moq/$hdr" ]; then
        echo "FAIL: $hdr must not be installed"; exit 1
    fi
done

echo "== 3. configure managed consumer against install prefix =="
# Deliberately NOT passing MOQ_PICOQUIC_SOURCE_DIR: forces the consumer
# to resolve the INSTALLED picoquic and its installed HTTP headers.
"$CMAKE" -S "$CONSUMER_SRC" -B "$CONB" \
    -DCMAKE_PREFIX_PATH="$P" \
    "${ossl_arg[@]}" \
    -DCMAKE_C_COMPILER="$CC" \
    -DCMAKE_C_FLAGS="$CFLAGS" \
    -DCMAKE_EXE_LINKER_FLAGS="$LDFLAGS" \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
"$CMAKE" --build "$CONB"

echo "== 4. run managed consumer =="
"$CONB/moq_pico_wt_managed_consumer_test"

echo "== install-tree managed consumer OK =="
rm -rf "$WORK"
