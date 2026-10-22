#!/usr/bin/env bash
# Build the scripted service provider against a real libmoq SDK and run the
# deterministic rows.
#
#   run.sh <sdk-prefix> [cargo test args...]
#
# The provider is substituted for the SDK's service library through pkg-config
# alone: a generated libmoq-service.pc names the fake archive in Libs and the
# real libmoq in Requires, and PKG_CONFIG_PATH puts its directory ahead of the
# SDK's. moq5-sys and moq5 are built unchanged. The test binaries run with
# the loader path of the SDK's closure, which a shared SDK needs.
#
# Environment: CC (default cc), AR (default ar), CARGO (default cargo; a
# command line such as "rustup run 1.85 cargo" is split on spaces),
# MOQ5_CARGO_SUBCOMMAND (default test), MOQ5_FAKE_BUILD (the work directory,
# default <this dir>/build), MOQ5_FAKE_CFLAGS (extra flags for the provider's
# compile), MOQ5_FAKE_SANITIZE=address (the provider under AddressSanitizer:
# its compile is instrumented and the provider's pkg-config file names the
# compiler's own ASan runtime right after the provider archive, since rustc
# links with -nodefaultlibs; leak detection is off because the provider
# keeps its knob table for the process),
# LIBCLANG_PATH as bindgen needs it, PKG_CONFIG (default pkg-config).
set -euo pipefail
# A command substitution fails with the first command that fails in it
# (bash 4.4 and later; older shells keep the helpers' explicit returns).
shopt -s inherit_errexit 2>/dev/null || true
HERE=$(cd "$(dirname "$0")" && pwd)
PREFIX=${1:?sdk prefix}; shift
CC=${CC:-cc}; AR=${AR:-ar}; CARGO=${CARGO:-cargo}
SUBCOMMAND=${MOQ5_CARGO_SUBCOMMAND:-test}
WORK=${MOQ5_FAKE_BUILD:-$HERE/build}
# shellcheck source=../scripts/sdk_paths.sh
. "$HERE/../scripts/sdk_paths.sh"
mkdir -p "$WORK/pkgconfig"

SDK_PC=""
for d in lib/pkgconfig lib64/pkgconfig; do
    [ -f "$PREFIX/$d/libmoq.pc" ] && SDK_PC="$PREFIX/$d" && break
done
[ -n "$SDK_PC" ] || { echo "run.sh: no libmoq.pc under $PREFIX" >&2; exit 2; }

cflags=(${MOQ5_FAKE_CFLAGS:-})
provider_libs="-L$WORK -lmoq5-fake-service"
case ${MOQ5_FAKE_SANITIZE:-} in
    "") ;;
    address)
        cflags+=(-fsanitize=address -fno-omit-frame-pointer)
        # The runtime of the compiler that instruments the provider, never
        # another compiler's: clang's compiler-rt runtime from its resource
        # directory, gcc's own libasan. A missing runtime is a missing
        # prerequisite, named; clang does not fall back to gcc's libasan.
        case $("$CC" --version 2>/dev/null || true) in
            *clang*)
                resource=$("$CC" -print-resource-dir)
                case $(uname -s) in
                    Darwin) runtime=$resource/lib/darwin/libclang_rt.asan_osx_dynamic.dylib ;;
                    *)
                        # The per-target layout, else the per-OS one.
                        runtime=$("$CC" -print-runtime-dir 2>/dev/null || true)/libclang_rt.asan.so
                        [ -f "$runtime" ] || runtime=$resource/lib/linux/libclang_rt.asan-$(uname -m).so ;;
                esac
                missing="the compiler-rt AddressSanitizer runtime of $CC (Ubuntu: libclang-rt-dev, beside clang)" ;;
            *"Free Software Foundation"*)
                runtime=$("$CC" -print-file-name=libasan.so)
                missing="the libasan of $CC (installed with gcc)" ;;
            *)
                echo "run.sh: $CC is neither clang nor gcc; no AddressSanitizer runtime is chosen for it" >&2
                exit 2 ;;
        esac
        [ -f "$runtime" ] || { echo "run.sh: no $runtime: install $missing" >&2; exit 2; }
        # The runtime is the instrumented archive's dependency and is linked
        # as one, right after it and before libc, as a C compiler driver
        # places it. Linked after libc (where a trailing -C link-arg puts
        # it), GNU ld on aarch64 prints glibc's link warnings for the
        # runtime's tmpnam/tmpnam_r/tempnam interceptors.
        runtime_lib=${runtime##*/}
        runtime_lib=${runtime_lib#lib}
        runtime_lib=${runtime_lib%.so}
        runtime_lib=${runtime_lib%.dylib}
        provider_libs="$provider_libs -L$(dirname "$runtime") -l$runtime_lib"
        export RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-C link-arg=-Wl,-rpath,$(dirname "$runtime")"
        export ASAN_OPTIONS="detect_leaks=0${ASAN_OPTIONS:+:$ASAN_OPTIONS}"
        export DYLD_LIBRARY_PATH="$(dirname "$runtime")${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
        export LD_LIBRARY_PATH="$(dirname "$runtime")${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
        if [ "$(uname -s)" = Linux ]; then
            # The runtime must be the first library loaded, which link order
            # cannot make it, so the test binaries (and only they: cargo's
            # runner for the host target) preload it.
            host=$(rustc -vV | sed -n 's/^host: //p' | tr 'a-z-' 'A-Z_')
            export "CARGO_TARGET_${host}_RUNNER=env LD_PRELOAD=$runtime"
        fi ;;
    *) echo "run.sh: MOQ5_FAKE_SANITIZE must be empty or address" >&2; exit 2 ;;
esac

"$CC" -std=c11 -Wall -Wextra -Wpedantic -Werror -O1 -g -fPIC ${cflags[@]+"${cflags[@]}"} \
    "-I$PREFIX/include" -c "$HERE/fake_service.c" -o "$WORK/fake_service.o"
rm -f "$WORK/libmoq5-fake-service.a"
"$AR" rcs "$WORK/libmoq5-fake-service.a" "$WORK/fake_service.o"

cat > "$WORK/pkgconfig/libmoq-service.pc" <<EOF
# The scripted provider standing in for libmoq-service (moq5 fake tests).
Name: libmoq-service
Description: moq5 fake service provider
Version: 0.0.0
Requires: libmoq
Libs: $provider_libs
Cflags: -I$PREFIX/include
EOF

export PKG_CONFIG_PATH="$WORK/pkgconfig:$SDK_PC${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
# cargo does not watch a linked archive; the test crate's build script does
# (MOQ5_FAKE_ARCHIVE), so a rebuilt provider relinks the test binaries.
export MOQ5_FAKE_ARCHIVE="$WORK/libmoq5-fake-service.a"
unset MOQ_PREFIX
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$WORK/target}
# The SDK's own libraries (the core behind the provider) for the loader.
moq5_sdk_export_loader "$PREFIX"
cd "$HERE"
# shellcheck disable=SC2086  # CARGO may be a command line
exec $CARGO "$SUBCOMMAND" "$@"
