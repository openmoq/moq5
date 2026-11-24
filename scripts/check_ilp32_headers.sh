#!/usr/bin/env bash
#
# Compile the public headers for two 32-bit targets, syntax only, so the
# ILP32 size pins and alignment assumptions are exercised on every CI run
# even though no lane builds a 32-bit binary. The public headers depend only
# on the freestanding C headers, so no sysroot is needed. The two targets
# differ in the alignment uint64_t gets inside a struct (8 on ARM, 4 on
# i686), so the struct layout pins in tests/unit/test_abi_layout_d22.c,
# tests/unit/test_abi_layout_mpeg2ts.c and tests/unit/test_abi_layout_msf02.c
# are compiled for both as well, in
# normal and short-enum modes, with every tier whose public headers are
# freestanding. Each mode pins its own ABI; these
# checks do not imply that objects built with different enum modes can mix.
#
# The catalog's name hashing and matching (media/msf/src/msf_names.c) is
# also executed on a 32-bit target where the host can run i686 code (x86-64
# Linux): built freestanding and static, with undefined behaviour trapping.
# Elsewhere that step prints a SKIPPED line.
#
# Exit 0 if both targets accept the headers and the pins, and the execution
# step passes or is skipped; 1 otherwise.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CC="${CC_ILP32:-clang}"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

cat > "$TMP/probe.c" <<'EOF'
#include <moq/moq.h>
_Static_assert(sizeof(void *) == 4, "ILP32 probe must compile for a 32-bit target");
EOF

LAYOUT_INCLUDES=()
for dir in core/include sim/include media/loc/include media/object/include \
           media/msf/include media/cmaf/include media/mpeg2ts/include \
           service/include relay/include \
           adapters/mvfst/include adapters/proxygen/include; do
    if [ -d "$ROOT/$dir" ]; then
        LAYOUT_INCLUDES+=("-I$ROOT/$dir")
    fi
done

status=0
for target in armv8m.main-none-eabi i686-linux-gnu; do
    if "$CC" --target="$target" -std=c11 -ffreestanding -fsyntax-only \
        -Wall -Wextra -Werror -I"$ROOT/core/include" "$TMP/probe.c"; then
        echo "PASSED: public headers accepted for $target"
    else
        echo "FAILED: public headers rejected for $target"
        status=1
    fi
    for mode in normal short; do
        LAYOUT_FLAGS=(-std=c11 -ffreestanding -fsyntax-only
                      -Wall -Wextra -Wpedantic -Werror)
        if [ "$mode" = short ]; then
            LAYOUT_FLAGS+=(-fshort-enums)
        fi
        for probe in test_abi_layout_d22.c test_abi_layout_mpeg2ts.c \
                     test_abi_layout_msf02.c; do
            if "$CC" --target="$target" "${LAYOUT_FLAGS[@]}" \
                "${LAYOUT_INCLUDES[@]}" "$ROOT/tests/unit/$probe"; then
                echo "PASSED: $probe pins hold for $target ($mode enums)"
            else
                echo "FAILED: $probe pins broken for $target ($mode enums)"
                status=1
            fi
        done
    done
done

if [ "$(uname -s)" = Linux ] && [ "$(uname -m)" = x86_64 ]; then
    RESOURCE="$("$CC" -print-resource-dir)"
    if "$CC" --target=i686-linux-gnu -std=c11 -ffreestanding -nostdlib -nostdinc \
        -isystem "$RESOURCE/include" \
        -isystem "$ROOT/media/msf/tests/freestanding" \
        -I"$ROOT/core/include" -I"$ROOT/media/msf/include" \
        -static -fno-pic -fno-stack-protector -O1 \
        -fsanitize=undefined -fsanitize-trap=undefined \
        -Wall -Wextra -Werror \
        "$ROOT/media/msf/tests/test_msf_names_ilp32.c" -o "$TMP/msf_names_ilp32" &&
        "$TMP/msf_names_ilp32"; then
        echo "PASSED: msf_names executed for i686"
    else
        echo "FAILED: msf_names executed for i686"
        status=1
    fi
else
    echo "SKIPPED: msf_names i686 execution needs an x86-64 Linux host"
fi
exit $status
