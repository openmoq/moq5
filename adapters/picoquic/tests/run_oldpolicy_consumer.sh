#!/bin/sh
# Build and run the CMP0156-unset consumer twice: against the generated
# build-tree package, and against the same build installed to a private
# prefix. Every step's output reaches CTest, whose warning expression judges it.
# usage: run_oldpolicy_consumer.sh CMAKE SRC WORK PARENT_BINARY_DIR
#            PICOQUIC_SRC PICOTLS_PREFIX CC EXPECT_THREADED EXPECT_PICO_WT_MANAGED
#            EXPECT_PICO_WT PARENT_PREFIX_PATH PARENT_PICOQUIC_DIR INSTALL_LIBDIR
#            [--sanitizer-c-flags=FLAGS] [--sanitizer-link-flags=FLAGS]
#
# The children resolve dependencies exactly as the parent did: the generated
# LibMoQ prefix comes first, then the parent's own CMAKE_PREFIX_PATH, and the
# parent's resolved picoquic_DIR when it found PicoQUIC as an installed package.
# A sanitized parent also passes its sanitizer options, so the children link the
# runtimes its instrumented archives need; an ordinary parent passes none.
set -eu
CMAKE=$1 SRC=$2 WORK=$3 PARENT=$4 PQSRC=$5 PTLS=$6 CC=$7 THREADED=$8 MANAGED=$9
shift 9
PICO_WT=$1 PARENT_PREFIX_PATH=$2 PARENT_PICOQUIC_DIR=$3 INSTALL_LIBDIR=$4
shift 4
SAN_C_FLAGS="" SAN_LINK_FLAGS=""
for opt in "$@"; do
    case $opt in
        --sanitizer-c-flags=*)    SAN_C_FLAGS=${opt#--sanitizer-c-flags=} ;;
        --sanitizer-link-flags=*) SAN_LINK_FLAGS=${opt#--sanitizer-link-flags=} ;;
        *) echo "FAIL: unknown option '$opt'"; exit 2 ;;
    esac
done

"$CMAKE" -E rm -rf "$WORK"
"$CMAKE" -E make_directory "$WORK"
"$CMAKE" --install "$PARENT" --prefix "$WORK/prefix"

# The WebTransport surfaces need source-tree picoquic's HTTP library, which
# the package builds only when the consumer asks for the pico-wt adapter.
http=OFF
case $MANAGED in 1|ON|TRUE|true|on) http=ON ;; esac
case $PICO_WT in 1|ON|TRUE|true|on) http=ON ;; esac

for mode in build-tree installed; do
    case $mode in
        build-tree) prefix=$PARENT ;;
        installed)  prefix=$WORK/prefix ;;
    esac
    bld="$WORK/$mode"
    echo "== oldpolicy consumer: $mode"
    search="$prefix"
    [ -n "$PARENT_PREFIX_PATH" ] && search="$prefix;$PARENT_PREFIX_PATH"
    set --
    [ -n "$PARENT_PICOQUIC_DIR" ] && set -- "-Dpicoquic_DIR=$PARENT_PICOQUIC_DIR"
    [ -n "$SAN_C_FLAGS" ] && set -- "$@" "-DCMAKE_C_FLAGS=$SAN_C_FLAGS"
    [ -n "$SAN_LINK_FLAGS" ] && set -- "$@" "-DCMAKE_EXE_LINKER_FLAGS=$SAN_LINK_FLAGS"
    "$CMAKE" -S "$SRC" -B "$bld" "$@" \
        -DCMAKE_PREFIX_PATH="$search" \
        -DMOQ_PICOQUIC_SOURCE_DIR="$PQSRC" \
        -DMOQ_PICOTLS_PREFIX="$PTLS" \
        -DCMAKE_C_COMPILER="$CC" \
        -DOLDPOLICY_EXPECT_THREADED="$THREADED" \
        -DOLDPOLICY_EXPECT_PICO_WT_MANAGED="$MANAGED" \
        -DOLDPOLICY_EXPECT_PICO_WT="$PICO_WT" \
        -DMOQ_BUILD_ADAPTER_PICO_WT="$http"
    "$CMAKE" --build "$bld"
    ran=0
    while IFS= read -r program; do
        [ -n "$program" ] || continue
        # The runner supplies the runtime search path a consumer of a private
        # prefix owns, and proves every LibMoQ/PicoQUIC image came from the
        # installed libdir. The expectation is per program: the direct
        # picoquic-log probe links the PicoQUIC this consumer builds from
        # MOQ_PICOQUIC_SOURCE_DIR, so it must load NO installed image; every
        # other probe links a LibMoQ surface and must load at least one.
        case $mode:$program in
            build-tree:*)
                "$bld/$program" ;;
            installed:oldpolicy_log)
                bash "$(dirname "$0")/run_installed_consumer.sh" --expect-no-image \
                    "$WORK/prefix/$INSTALL_LIBDIR" "$bld/$program" ;;
            installed:*)
                bash "$(dirname "$0")/run_installed_consumer.sh" \
                    "$WORK/prefix/$INSTALL_LIBDIR" "$bld/$program" ;;
        esac
        ran=$((ran + 1))
    done < "$bld/oldpolicy-programs.txt"
    [ "$ran" -ge 2 ] || { echo "FAIL: only $ran program(s) ran in $mode mode"; exit 1; }
done
