#!/usr/bin/env bash
# Build and run an out-of-tree consumer against an installed SDK, in a
# clean environment, and prove what it linked.
#
#   run.sh <static|shared> <sdk-prefix> <work-dir> [service|core]
#
# service (the default) builds moq5-consumer, over moq5; core builds
# moq5-core-consumer alone, whose graph asks only for the core tier.
#
# The build (cargo's structured diagnostics on stdout, its own on stderr)
# must have no diagnostic this build owns: every rustc and cargo diagnostic
# is classified by scripts/diagnostics.py against scripts/
# dependency_diagnostics.allow and inventoried in <work-dir>/
# diagnostics-<mode>.txt. The build must link from the SDK's own library
# directory and from no directory outside its pkg-config closure (exact
# directories, scripts/sdk_paths.sh), and the binary must contain no symbol
# of the fake provider. nm is required for the symbol proof. The binary runs
# with the loader path of the SDK's closure in both modes, every import
# bound at launch. With MOQ5_RUST_PLAYER=1 the service consumer is built
# with the player, whose entry point must be defined in a static binary and
# imported by a shared one, like the endpoint's.
#
# The core consumer is the link proof of the core tier: moq5-sys must probe
# libmoq-core alone and emit one link library, moq-core; the binary must
# define (static) or import (shared) the core's entry points and hold no
# symbol, defined or undefined, that the SDK's other libmoq libraries (the
# simulator, the media parsers, the service tier, the player, an adapter)
# define and the core does not; a shared binary must load libmoq-core and
# no other libmoq library.
#
# Environment: MOQ5_RUST_PLAYER (1 or 0, default 0), CARGO (default cargo),
# LIBCLANG_PATH as bindgen needs it,
# PKG_CONFIG (default pkg-config), PYTHON (default python3),
# MOQ5_SDK_PKG_CONFIG_PATH (the pkg-config directories of the SDK's
# dependency prefixes, carried in as the clean PKG_CONFIG_PATH), RUSTFMT (the
# formatter bindgen runs, carried in); CC/AR/PATH/HOME/CARGO_HOME/RUSTUP_HOME/
# RUSTUP_TOOLCHAIN/RUSTC/RUSTDOC/TMPDIR/CARGO_NET_OFFLINE are carried over;
# nothing else is.
set -euo pipefail
# A command substitution fails with the first command that fails in it
# (bash 4.4 and later; older shells keep the helpers' explicit returns).
shopt -s inherit_errexit 2>/dev/null || true
HERE=$(cd "$(dirname "$0")" && pwd)
MODE=${1:?static or shared}; PREFIX=${2:?sdk prefix}; WORK=${3:?work dir}; TIER=${4:-service}
case $MODE in static|shared) ;; *) echo "run.sh: static or shared" >&2; exit 2 ;; esac
CARGO=${CARGO:-cargo}
features=()
case $TIER in
    service)
        package=moq5-consumer
        symbols_proved=(moq_endpoint_connect)
        case ${MOQ5_RUST_PLAYER:-0} in
            0) ;;
            1) features=(--features player); symbols_proved+=(moq_media_player_attach) ;;
            *) echo "run.sh: MOQ5_RUST_PLAYER must be 1 or 0" >&2; exit 2 ;;
        esac ;;
    core)
        package=moq5-core-consumer
        symbols_proved=(moq_session_create moq_version_supported moq_rcbuf_create) ;;
    *) echo "run.sh: the consumer is service or core, not $TIER" >&2; exit 2 ;;
esac
label=$MODE${TIER/#service/}
label=${label/core/-core}
# shellcheck source=../scripts/sdk_paths.sh
. "$HERE/../scripts/sdk_paths.sh"
mkdir -p "$WORK"
JSON=$WORK/build-$label.json
LOG=$WORK/build-$label.log
command -v nm >/dev/null || { echo "run.sh: nm is required for the symbol proof" >&2; exit 2; }

closure=$(moq5_sdk_runtime_path "$PREFIX")

clean=(env -i "PATH=$PATH" "HOME=$HOME" "MOQ_PREFIX=$PREFIX" "CARGO_TARGET_DIR=$WORK/target-$label")
[ -n "${MOQ5_SDK_PKG_CONFIG_PATH:-}" ] && clean+=("PKG_CONFIG_PATH=$MOQ5_SDK_PKG_CONFIG_PATH")
for v in CARGO_HOME RUSTUP_HOME RUSTUP_TOOLCHAIN RUSTC RUSTDOC LIBCLANG_PATH PKG_CONFIG RUSTFMT CC AR \
         TMPDIR CARGO_NET_OFFLINE; do
    [ -n "${!v:-}" ] && clean+=("$v=${!v}")
done
for stem in $MOQ5_SDK_TIER_STEMS; do
    case $MODE in
        static) clean+=("${stem}_STATIC=1") ;;
        shared) clean+=("${stem}_DYNAMIC=1") ;;
    esac
done

cd "$HERE"
rm -rf "$WORK/target-$label"   # a cold build: the link lines must appear in the log
"${clean[@]}" "$CARGO" build -vv --locked --message-format=json -p "$package" \
    ${features[@]+"${features[@]}"} > "$JSON" 2> "$LOG" || {
    echo "run.sh: build failed (see $LOG)" >&2; tail -40 "$LOG" >&2; exit 1; }

bash "$HERE/../scripts/diagnostics.sh" "$JSON" "$LOG" "$WORK/diagnostics-$label.txt" \
    "$HERE/../scripts/dependency_diagnostics.allow" "consumer $label"
moq5_sdk_assert_link_search "$JSON" "$PREFIX"
if grep -qi 'fake' "$JSON" "$LOG"; then
    echo "run.sh: the build output mentions a fake provider" >&2; exit 1
fi
if [ "$TIER" = core ]; then
    # moq5-sys probed the core package alone and linked one library.
    grep -q 'moq5-sys: native source: .*packages for these tiers: libmoq-core$' "$JSON" ||
        { echo "run.sh: the core graph did not probe libmoq-core alone (see $JSON)" >&2; exit 1; }
    links=$(grep -o 'cargo:rustc-link-lib=[^ "]*' "$JSON" | sed 's/^cargo:rustc-link-lib=//' | sort -u | paste -sd' ' -)
    case $links in
        moq-core|static=moq-core|dylib=moq-core) ;;
        *) echo "run.sh: the core graph linked [$links], not moq-core alone" >&2; exit 1 ;;
    esac
fi

BIN=$WORK/target-$label/debug/$package
# The symbol table is read once: piping nm into grep -q would let grep
# close the pipe early and turn nm's SIGPIPE into a failed pipeline.
symbols=$(nm "$BIN")
if grep -q moq5_fake <<<"$symbols"; then
    echo "run.sh: the binary contains a fake provider symbol" >&2; exit 1
fi
if [ "$TIER" = core ]; then
    # No symbol of a library above the core, defined or imported: every
    # global symbol the SDK's other libmoq libraries define (the simulator,
    # the media parsers, the service, the player, any adapter), less the
    # names the core library itself defines, read from the libraries.
    libdir=$(moq5_sdk_libdir "$PREFIX")
    defined() {   # defined <library>: its defined global symbol names
        case $1 in
            *.so*) nm -D --defined-only "$1" ;;
            *) nm -g --defined-only "$1" ;;
        esac 2>/dev/null | awk 'NF >= 3 { print $3 }' | sed 's/^_//' | sort -u
    }
    core_defined=$(for lib in "$libdir"/libmoq-core.*; do
                       if [ -f "$lib" ]; then defined "$lib"; fi
                   done | sort -u)
    above=$(for lib in "$libdir"/libmoq-*; do
                case ${lib##*/} in libmoq-core.*) continue ;; esac
                if [ -f "$lib" ]; then defined "$lib"; fi
            done | { grep '^moq' || true; } | sort -u | comm -23 - <(printf '%s\n' "$core_defined"))
    binary=$(awk '{ print $NF }' <<<"$symbols" | sed 's/^_//' | sort -u)
    held=$(comm -12 <(printf '%s\n' "$above") <(printf '%s\n' "$binary"))
    if [ -n "$held" ]; then
        echo "run.sh: the core consumer holds symbols of the libraries above the core:" >&2
        head -20 <<<"$held" >&2; exit 1
    fi
    echo "run.sh: none of the $(grep -c . <<<"$above" || true) symbols the SDK's other libmoq libraries define is in the core consumer"
    if [ "$MODE" = shared ]; then
        case $(uname -s) in
            Darwin) loaded=$(otool -L "$BIN" | sed -n 's#^[[:space:]]*\(.*libmoq[^ ]*\) .*#\1#p') ;;
            *) loaded=$(readelf -d "$BIN" | sed -n 's#.*(NEEDED).*\[\(libmoq[^]]*\)\].*#\1#p') ;;
        esac
        names=$(for lib in $loaded; do basename "$lib" | sed 's/[.].*//'; done | sort -u | paste -sd' ' -)
        [ "$names" = libmoq-core ] ||
            { echo "run.sh: the shared core consumer loads [$names], not libmoq-core alone" >&2; exit 1; }
    fi
fi
for symbol in "${symbols_proved[@]}"; do
    case $MODE in
        static)
            grep -qE " T _?$symbol\$" <<<"$symbols" ||
                { echo "run.sh: $symbol is not defined in the static binary" >&2; exit 1; } ;;
        shared)
            grep -qE " U _?$symbol\$" <<<"$symbols" ||
                { echo "run.sh: $symbol is not an undefined import in the shared binary" >&2; exit 1; } ;;
    esac
done

# The closure's loader path in both modes: a static SDK's closure can still
# carry a shared dependency (the WTQuic managed SDK's msquic is a dylib).
# Every import is bound at launch, so an import the SDK does not define
# fails the run rather than the first call.
run=(env -i "PATH=$PATH" "LD_LIBRARY_PATH=$closure" "DYLD_LIBRARY_PATH=$closure"
    LD_BIND_NOW=1 DYLD_BIND_AT_LAUNCH=1)
"${run[@]}" "$BIN" | tee "$WORK/run-$label.log"
case $TIER in
    service) grep -q '^moq5 consumer: OK$' "$WORK/run-$label.log" ;;
    core) grep -q '^moq5-core consumer: OK$' "$WORK/run-$label.log" ;;
esac
if [ ${#features[@]} != 0 ]; then
    grep -q '^player linked$' "$WORK/run-$label.log" ||
        { echo "run.sh: the consumer ran without the player" >&2; exit 1; }
fi
echo "run.sh: $label consumer OK (diagnostics: $WORK/diagnostics-$label.txt)"
