#!/usr/bin/env bash
# Build and run the out-of-tree consumer against an installed SDK, in a
# clean environment, and prove what it linked.
#
#   run.sh <static|shared> <sdk-prefix> <work-dir>
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
# bound at launch. With MOQ5_RUST_PLAYER=1 the consumer is built with the
# player, whose entry point must be defined in a static binary and imported
# by a shared one, like the endpoint's.
#
# Environment: MOQ5_RUST_PLAYER (1 or 0, default 0), CARGO (default cargo),
# LIBCLANG_PATH as bindgen needs it,
# PKG_CONFIG (default pkg-config), PYTHON (default python3),
# MOQ5_SDK_PKG_CONFIG_PATH (the pkg-config directories of the SDK's
# dependency prefixes, carried in as the clean PKG_CONFIG_PATH), RUSTFMT (the
# formatter bindgen runs, carried in); CC/AR/PATH/HOME/CARGO_HOME/RUSTUP_HOME/
# RUSTUP_TOOLCHAIN/TMPDIR are carried over; nothing else is.
set -euo pipefail
# A command substitution fails with the first command that fails in it
# (bash 4.4 and later; older shells keep the helpers' explicit returns).
shopt -s inherit_errexit 2>/dev/null || true
HERE=$(cd "$(dirname "$0")" && pwd)
MODE=${1:?static or shared}; PREFIX=${2:?sdk prefix}; WORK=${3:?work dir}
case $MODE in static|shared) ;; *) echo "run.sh: static or shared" >&2; exit 2 ;; esac
CARGO=${CARGO:-cargo}
features=()
symbols_proved=(moq_endpoint_connect)
case ${MOQ5_RUST_PLAYER:-0} in
    0) ;;
    1) features=(--features player); symbols_proved+=(moq_media_player_attach) ;;
    *) echo "run.sh: MOQ5_RUST_PLAYER must be 1 or 0" >&2; exit 2 ;;
esac
# shellcheck source=../scripts/sdk_paths.sh
. "$HERE/../scripts/sdk_paths.sh"
mkdir -p "$WORK"
JSON=$WORK/build-$MODE.json
LOG=$WORK/build-$MODE.log
command -v nm >/dev/null || { echo "run.sh: nm is required for the symbol proof" >&2; exit 2; }

closure=$(moq5_sdk_runtime_path "$PREFIX")

clean=(env -i "PATH=$PATH" "HOME=$HOME" "MOQ_PREFIX=$PREFIX" "CARGO_TARGET_DIR=$WORK/target-$MODE")
[ -n "${MOQ5_SDK_PKG_CONFIG_PATH:-}" ] && clean+=("PKG_CONFIG_PATH=$MOQ5_SDK_PKG_CONFIG_PATH")
for v in CARGO_HOME RUSTUP_HOME RUSTUP_TOOLCHAIN LIBCLANG_PATH PKG_CONFIG RUSTFMT CC AR TMPDIR; do
    [ -n "${!v:-}" ] && clean+=("$v=${!v}")
done
case $MODE in
    static) clean+=(LIBMOQ_SERVICE_STATIC=1) ;;
    shared) clean+=(LIBMOQ_SERVICE_DYNAMIC=1) ;;
esac

cd "$HERE"
rm -rf "$WORK/target-$MODE"   # a cold build: the link lines must appear in the log
"${clean[@]}" "$CARGO" build -vv --locked --message-format=json ${features[@]+"${features[@]}"} \
    > "$JSON" 2> "$LOG" || {
    echo "run.sh: build failed (see $LOG)" >&2; tail -40 "$LOG" >&2; exit 1; }

bash "$HERE/../scripts/diagnostics.sh" "$JSON" "$LOG" "$WORK/diagnostics-$MODE.txt" \
    "$HERE/../scripts/dependency_diagnostics.allow" "consumer $MODE"
moq5_sdk_assert_link_search "$JSON" "$PREFIX"
if grep -qi 'fake' "$JSON" "$LOG"; then
    echo "run.sh: the build output mentions a fake provider" >&2; exit 1
fi

BIN=$WORK/target-$MODE/debug/moq5-consumer
# The symbol table is read once: piping nm into grep -q would let grep
# close the pipe early and turn nm's SIGPIPE into a failed pipeline.
symbols=$(nm "$BIN")
if grep -q moq5_fake <<<"$symbols"; then
    echo "run.sh: the binary contains a fake provider symbol" >&2; exit 1
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
"${run[@]}" "$BIN" | tee "$WORK/run-$MODE.log"
grep -q '^moq5 consumer: OK$' "$WORK/run-$MODE.log"
if [ ${#features[@]} != 0 ]; then
    grep -q '^player linked$' "$WORK/run-$MODE.log" ||
        { echo "run.sh: the consumer ran without the player" >&2; exit 1; }
fi
echo "run.sh: $MODE consumer OK (diagnostics: $WORK/diagnostics-$MODE.txt)"
