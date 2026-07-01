#!/usr/bin/env bash
#
# pkg-config consumer test for an installed pico WT component.
#
# Proves a plain external C program (no CMake) can compile, link, and run
# using ONLY `pkg-config --cflags --libs <component>` from the installed
# prefix — the Meson/autotools consumption path. Installs a BUILD_HTTP
# picoquic + libmoq to a temp prefix and builds the consumer against it
# with the installed picoquic (no MOQ_PICOQUIC_SOURCE_DIR, no source-tree
# includes).
#
# Args:
#   1  cmake executable
#   2  pkg-config package(s): a single package (e.g. libmoq-pico-wt,
#      libmoq-pico-wt-managed, or libmoq) OR a space-separated list passed
#      as one arg (e.g. "libmoq-pico-wt-managed libmoq" for the verifier
#      cross-link). pkg-config resolves multiple packages in one call.
#   3  consumer .c source (reuses the CMake consumer's main.c)
#   4  libmoq build dir (already built)
#   5  picoquic source dir
#   6  picotls prefix (…/picotls/build)
#   7  C compiler
#   8  C flags        (forwarded so ASAN parent → ASAN consumer)
#   9  exe linker flags
#  10  build type
#  11  openssl root   (may be empty)
#  12  work dir       (created fresh; removed on success)
#  13  install libdir (CMAKE_INSTALL_LIBDIR: lib, lib64, lib/<arch>, …)
set -euo pipefail

CMAKE="$1"; COMPONENT="$2"; CONSUMER_SRC="$3"; LIBMOQ_BUILD="$4"
PICOQUIC_SRC="$5"; PICOTLS_PREFIX="$6"; CC="$7"; CFLAGS="$8"; LDFLAGS="$9"
BUILD_TYPE="${10}"; OPENSSL_ROOT="${11}"; WORK="${12}"; LIBDIR="${13}"

P="$WORK/prefix"
PQB="$WORK/pq-build"

if [ -z "$PICOTLS_PREFIX" ]; then
    PICOTLS_PREFIX="$(cd "$PICOQUIC_SRC/../picotls/build" && pwd)"
fi
PTLS_SRC="$(dirname "$PICOTLS_PREFIX")"

if ! command -v pkg-config >/dev/null 2>&1; then
    echo "SKIP: pkg-config not found"; exit 0
fi

rm -rf "$WORK"; mkdir -p "$WORK"

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

echo "== 3. resolve $COMPONENT via pkg-config (prefix only) =="
export PKG_CONFIG_PATH="$P/$LIBDIR/pkgconfig"
if ! pkg-config --exists "$COMPONENT"; then
    echo "FAIL: pkg-config could not find $COMPONENT in $PKG_CONFIG_PATH"
    exit 1
fi
PC_CFLAGS="$(pkg-config --cflags "$COMPONENT")"
PC_LIBS="$(pkg-config --libs "$COMPONENT")"
echo "   cflags: $PC_CFLAGS"
echo "   libs:   $PC_LIBS"

# Guard: no source-tree include must leak through pkg-config cflags.
case "$PC_CFLAGS" in
    *"$PICOQUIC_SRC/picoquic"*|*"$PICOQUIC_SRC/picohttp"*)
        echo "FAIL: pkg-config cflags leak a picoquic source-tree path"; exit 1;;
esac

echo "== 4. compile + link the consumer with ONLY pkg-config output =="
OUT="$WORK/consumer"
# Intentionally no -I beyond pkg-config's, and no source dirs.
# shellcheck disable=SC2086
"$CC" $CFLAGS "$CONSUMER_SRC" $PC_CFLAGS $PC_LIBS $LDFLAGS -o "$OUT"

echo "== 5. run consumer =="
"$OUT"

echo "== pkg-config consumer ($COMPONENT) OK =="
rm -rf "$WORK"
