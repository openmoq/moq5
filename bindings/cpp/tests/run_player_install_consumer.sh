#!/usr/bin/env bash
#
# Install-tree consumer test for the C++ media player.
#
# Proves `make install` consumption of <moq/media_player.hpp>: install a
# BUILD_HTTP picoquic and libmoq to a temp prefix, then build the player
# consumer project against THAT prefix -- not the build tree -- linking only
# moq::cpp and moq::service via find_package(libmoq COMPONENTS service).
# The parent's library type (static or shared) and configuration are what
# get installed. Both children take the parent's full build context
# (compilers, flags including sanitizers, Apple architectures and sysroot)
# and its OpenSSL selection. The libmoq and picoquic packages are pinned to
# the prefix, and the files of the named libmoq and picoquic targets the
# consumer linked are verified to come from it, so another libmoq or
# picoquic SDK on the machine cannot produce a pass. This is a named target
# set, not a traversal of every transitive library or runtime image (the
# picotls, OpenSSL and system libraries are not checked here).
#
# Args:
#   1  cmake executable
#   2  consumer source dir (bindings/cpp/tests/player_consumer)
#   3  libmoq build dir (already built: C++ binding, service, playback)
#   4  picoquic source dir
#   5  configuration (CTest's $<CONFIG>; may be empty)
#   6  generator (the parent's CMAKE_GENERATOR)
#   7  parent build context preload (moq_write_consumer_context)
#   8  parent OpenSSL selection preload (moq_write_consumer_openssl_context)
#   9  generated parent staging check
#  10  install include dir (the parent's CMAKE_INSTALL_INCLUDEDIR)
#  11  install lib dir (the parent's CMAKE_INSTALL_LIBDIR)
#  12  the parent's resolved picotls include dir (PTLS_INCLUDE_DIR)
#  13  the parent's resolved picotls library dir (MOQ_PTLS_LIB_DIR)
#  14  the parent's MOQ_PICOTLS_PREFIX, as given
#  15  work dir (created fresh; removed on success unless retained below)
#  16  the parent's picotls selection preload (header and component
#      libraries), given to the child picoquic build and verified there
#  17  selected wtquic package dir, empty when service has no managed WT backend
# Test-only: MOQ_TEST_KEEP_WORK=1 retains successful work and prints its path.
set -euo pipefail

CMAKE="$1"; CONSUMER_SRC="$2"; LIBMOQ_BUILD="$3"; PICOQUIC_SRC="$4"
CONFIG="$5"; GENERATOR="$6"; CONTEXT="$7"; OPENSSL_PRELOAD="$8"
STAGING_CHECK="$9"; INCDIR="${10}"; LIBDIR="${11}"
PTLS_INCLUDE_DIR="${12}"; PTLS_LIB_DIR="${13}"; PICOTLS_PREFIX="${14}"
WORK="${15}"; PICOTLS_PRELOAD="${16}"
WTQUIC_DIR="${17:-}"

here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=player_consumer_lib.sh
. "$here/player_consumer_lib.sh"
PC_CMAKE="$CMAKE"

P="$WORK/prefix"
PQB="$WORK/pq-build"
CONB="$WORK/consumer-build"
multi=0
case "$GENERATOR" in *"Multi-Config"*|Xcode|"Visual Studio"*) multi=1 ;; esac
cfg_args=($(pc_configure_config_args "$CONFIG" "$multi"))
step_args=($(pc_step_config_args "$CONFIG"))
PTLS_TREE=$(pc_ptls_tree "$PTLS_INCLUDE_DIR")

rm -rf "$WORK"
mkdir -p "$WORK"

# One preload for each child: the parent's build context, then its OpenSSL
# selection, so the children compile and link as the parent's archives were.
PRELOAD="$WORK/preload.cmake"
printf 'include([==[%s]==])\ninclude([==[%s]==])\n' "$CONTEXT" "$OPENSSL_PRELOAD" \
    > "$PRELOAD"
# The child picoquic also takes the parent's picotls selection: its own
# discovery searches the sibling checkout's headers before giving up, so a
# derived PTLS_PREFIX alone does not make the selection authoritative.
PQ_PRELOAD="$WORK/pq-preload.cmake"
printf 'include([==[%s]==])\ninclude([==[%s]==])\n' "$PRELOAD" "$PICOTLS_PRELOAD" \
    > "$PQ_PRELOAD"

"$CMAKE" -Dphase=before -P "$STAGING_CHECK"

echo "== 1. install picoquic (BUILD_HTTP=ON) -> $P =="
"$CMAKE" -C "$PQ_PRELOAD" -G "$GENERATOR" -S "$PICOQUIC_SRC" -B "$PQB" \
    -DBUILD_HTTP=ON -DBUILD_DEMO=OFF -DBUILD_PQBENCH=OFF \
    -DBUILD_PICO_SIM=OFF -Dpicoquic_BUILD_TESTS=OFF -DBUILD_LOGLIB=ON \
    -DCMAKE_INSTALL_PREFIX="$P" -DCMAKE_INSTALL_LIBDIR="$LIBDIR" \
    -DPTLS_PREFIX="$PTLS_TREE" \
    -DCMAKE_LIBRARY_PATH="$PTLS_LIB_DIR" \
    ${cfg_args[@]+"${cfg_args[@]}"}
# The provider build selected exactly the parent's picotls.
for var in PTLS_INCLUDE_DIR PTLS_CORE_LIBRARY PTLS_MINICRYPTO_LIBRARY \
           PTLS_OPENSSL_LIBRARY PTLS_FUSION_LIBRARY; do
    want=$(pc_setting "$PICOTLS_PRELOAD" "$var")
    if ! pc_selected "$PQB/CMakeCache.txt" "$var" "$want"; then
        echo "FAIL: child picoquic selected $var=$(pc_setting "$PQB/CMakeCache.txt" "$var"), want ${want:-none}"
        exit 1
    fi
    echo "provider picotls: $var=${want:-none}"
done
"$CMAKE" --build "$PQB" ${step_args[@]+"${step_args[@]}"}
"$CMAKE" --install "$PQB" ${step_args[@]+"${step_args[@]}"}

echo "== 2. install libmoq -> $P =="
"$CMAKE" --install "$LIBMOQ_BUILD" --prefix "$P" ${step_args[@]+"${step_args[@]}"}
"$CMAKE" -Dphase=after -Dprefix="$P" -P "$STAGING_CHECK"

# Installed-header manifest under the configured include dir: the C++ player
# header and the C headers it wraps ship; service-internal headers must NOT.
for hdr in media_player.hpp media_player.h media_receiver.h; do
    if [ ! -f "$P/$INCDIR/moq/$hdr" ]; then
        echo "FAIL: $INCDIR/moq/$hdr was not installed"; exit 1
    fi
done
for hdr in endpoint_internal.h media_player_internal.h; do
    if [ -f "$P/$INCDIR/moq/$hdr" ] || [ -f "$P/$INCDIR/$hdr" ]; then
        echo "FAIL: $hdr must not be installed"; exit 1
    fi
done

echo "== 3. configure the C++ player consumer against the install prefix =="
# Deliberately NOT passing MOQ_PICOQUIC_SOURCE_DIR: the consumer resolves the
# INSTALLED picoquic and its exported picohttp-core. Both packages are pinned
# to the prefix; the package registry is not consulted.
transport_args=()
if [ -n "$WTQUIC_DIR" ]; then
    transport_args+=("-Dwtquic_DIR=$WTQUIC_DIR")
fi
"$CMAKE" -C "$PRELOAD" -G "$GENERATOR" -S "$CONSUMER_SRC" -B "$CONB" \
    -DCMAKE_PREFIX_PATH="$P" \
    -Dlibmoq_DIR="$P/$LIBDIR/cmake/libmoq" \
    -Dpicoquic_DIR="$P/$LIBDIR/cmake/picoquic" \
    -DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF \
    -DMOQ_PICOTLS_PREFIX="$PICOTLS_PREFIX" -DPTLS_INCLUDE_DIR="$PTLS_INCLUDE_DIR" \
    ${transport_args[@]+"${transport_args[@]}"} \
    ${cfg_args[@]+"${cfg_args[@]}"}
if [ -n "$WTQUIC_DIR" ] &&
   ! pc_selected "$CONB/CMakeCache.txt" wtquic_DIR "$WTQUIC_DIR"; then
    echo "FAIL: player consumer did not select the parent's wtquic package"
    exit 1
fi
"$CMAKE" --build "$CONB" ${step_args[@]+"${step_args[@]}"}

# Each named libmoq and picoquic target the consumer linked has its file in
# the prefix (the consumer records those targets' files per configuration;
# targets outside that list, and transitive dependencies, are not recorded).
linked="$CONB/linked-${CONFIG}.txt"
[ -f "$linked" ] || linked="$CONB/linked-.txt"
[ -f "$linked" ] || { echo "FAIL: no linked-target record in $CONB"; exit 1; }
while IFS= read -r lib; do
    [ -n "$lib" ] || continue
    if ! pc_under_prefix "$lib" "$P"; then
        echo "FAIL: linked $lib is not from the install prefix $P"; exit 1
    fi
done < "$linked"
echo "linked from the prefix: $(grep -c . "$linked") named target files"

echo "== 4. run the C++ player consumer =="
exe=$(pc_find_exe "$CONB" moq_player_consumer_test "$CONFIG")
[ -n "$exe" ] || { echo "FAIL: moq_player_consumer_test not built"; exit 1; }
"$exe"

echo "== install-tree C++ player consumer OK =="
if [[ "${MOQ_TEST_KEEP_WORK:-0}" == 1 ]]; then
    printf 'MOQ_TEST_RETAINED_WORK=%s\n' "$WORK"
else
    rm -rf "$WORK"
fi
