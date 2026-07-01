#!/usr/bin/env bash
#
# Install-tree consumer test for moq-adapter-pico-wt.
#
# Proves true `make install` consumption: install a BUILD_HTTP picoquic
# and libmoq to a temp prefix, then build the consumer project against
# THAT prefix — not the build tree — using the installed picoquic (no
# MOQ_PICOQUIC_SOURCE_DIR). Verifies the installed picoquic provides all
# transitive headers <pico_webtransport.h> / <moq/pico_wt.h> need,
# including picosplay.h.
#
# Args:
#   1  cmake executable
#   2  consumer source dir
#   3  libmoq build dir (already built)
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

# Fall back to the picotls build adjacent to picoquic (as FindPicoquic
# does) when no prefix was supplied.
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
# Build all picoquic targets before install (its picolog_t install rule
# references a binary that only exists after a full build).
"$CMAKE" --build "$PQB"
"$CMAKE" --install "$PQB"

echo "== 2. install libmoq -> $P =="
"$CMAKE" --install "$LIBMOQ_BUILD" --prefix "$P"

# Installed-header manifest: the public attach header ships; the internal
# headers must NOT. (pico_wt_managed.h is the managed component's concern
# — its presence depends on MOQ_BUILD_PICO_WT_MANAGED and is checked by
# the managed install-consumer test, not here.)
if [ ! -f "$P/include/moq/pico_wt.h" ]; then
    echo "FAIL: moq/pico_wt.h was not installed"; exit 1
fi
for hdr in pico_wt_adapter.h pico_wt_endpoint.h; do
    if [ -f "$P/include/moq/$hdr" ]; then
        echo "FAIL: $hdr must not be installed"; exit 1
    fi
done

echo "== 3. configure consumer against install prefix (installed picoquic) =="
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

echo "== 4. run consumer =="
"$CONB/moq_pico_wt_adapter_consumer_test"

echo "== install-tree consumer OK =="
rm -rf "$WORK"
