#!/usr/bin/env bash
# One qualification row of the Rust binding, as the ctest rows and the CI
# workflow run them.
#
#   ctest_row.sh <row> [row arguments]
#
# Rows: fmt, clippy, doc, tests, fake_tests, fake_asan, layout,
# installed_consumer_static, installed_consumer_shared, package, msrv,
# gate_controls, cross_refusal <target>, cross_build <target> <sysroot>
# [cmake args...], relay_rows <moq5-relay binary>.
#
# Environment: MOQ5_RUST_SDK (the installed SDK prefix the rows link;
# required), MOQ5_RUST_WORK (the work directory; required), MOQ5_RUST_PLAYER
# and MOQ5_RUST_MPEG2TS (required, 1 or 0: whether the rows qualify moq5's
# player feature, and whether the SDK carries MPEG-TS; each is checked
# against the SDK's installed headers), MOQ5_RUST_SOURCE
# (the libmoq source tree the cross_build row configures a target SDK
# from), CARGO (default cargo), RUSTUP (default rustup), MOQ5_MSRV_TOOLCHAIN
# (default 1.85) for the msrv row, MOQ5_RUST_TOOLCHAIN (default stable: the
# rustup toolchain whose cargo, rustc, rustdoc and rustfmt the doc and cross
# rows run, since rustup owns the foreign targets they add), MOQ5_RELAY_SDK
# (an SDK with the picoquic backend for the relay rows; default
# MOQ5_RUST_SDK), MOQ5_RELAY_PEER_CFLAGS (extra compile flags for the relay
# rows' native peer), MOQ5_RELAY_PEER_SANITIZE (thread: the peer under
# ThreadSanitizer), MOQ5_SDK_PKG_CONFIG_PATH (the pkg-config directories of
# the SDK's dependency prefixes, when its closure spans them), CC (default
# cc) for the fake provider and the native peer, PKG_CONFIG (default
# pkg-config), PYTHON (default python3), LIBCLANG_PATH as bindgen needs it.
# Every binary linked to an SDK runs with that SDK closure's loader path,
# and every build states the SDK's installed link mode to the pkg-config
# crate (scripts/sdk_paths.sh). Any failing command, a helper included,
# fails the row.
set -euo pipefail
set -o errtrace
# A command substitution fails with the first command that fails in it
# (bash 4.4 and later; older shells keep the helpers' explicit returns).
shopt -s inherit_errexit 2>/dev/null || true
trap 'echo "ctest_row.sh: command failed at line $LINENO (row ${ROW:-?})" >&2' ERR
HERE=$(cd "$(dirname "$0")/.." && pwd)
ROW=${1:?row}; shift
: "${MOQ5_RUST_SDK:?the installed SDK prefix}" "${MOQ5_RUST_WORK:?the work directory}"
CARGO=${CARGO:-cargo}; RUSTUP=${RUSTUP:-rustup}; MSRV=${MOQ5_MSRV_TOOLCHAIN:-1.85}
TOOLCHAIN=${MOQ5_RUST_TOOLCHAIN:-stable}
CC=${CC:-cc}
W=$MOQ5_RUST_WORK
mkdir -p "$W"
# shellcheck source=sdk_paths.sh
. "$HERE/scripts/sdk_paths.sh"

# -- helpers, before any use --------------------------------------------
product() {   # cargo <subcommand> on the product workspace
    local sub=$1; shift
    "$CARGO" "$sub" --manifest-path "$HERE/Cargo.toml" "$@"
}
fake() {      # the fake-provider workspace, through its own runner
    bash "$HERE/fake-tests/run.sh" "$MOQ_PREFIX" "$@"
}
sdk_pc() {    # pkg-config over one SDK: sdk_pc <prefix> <args...>
    # The SDK's own directories, or a failure: never an empty search path
    # that would let pkg-config answer from another SDK.
    local prefix=$1 path; shift
    path=$(moq5_sdk_pkgconfig "$prefix") || return 1
    PKG_CONFIG_PATH=$path "${PKG_CONFIG:-pkg-config}" "$@"
}
toolchain_tool() {   # the path of one tool of a rustup toolchain, empty when the component is absent
    "$RUSTUP" which --toolchain "$1" "$2" 2>/dev/null || true
}
toolchain_cargo() {   # cargo of one rustup toolchain, with its own rustc, rustdoc and rustfmt
    local toolchain=$1; shift
    local rustc rustdoc rustfmt tools
    rustc=$("$RUSTUP" which --toolchain "$toolchain" rustc)
    rustdoc=$("$RUSTUP" which --toolchain "$toolchain" rustdoc)
    tools=("RUSTC=$rustc" "RUSTDOC=$rustdoc")
    # A toolchain without the rustfmt component formats nothing; bindgen
    # then writes the bindings unformatted, which only the snapshot cares
    # about, and the snapshot is compared on the default toolchain.
    rustfmt=$(toolchain_tool "$toolchain" rustfmt)
    if [ -n "$rustfmt" ]; then tools+=("RUSTFMT=$rustfmt"); fi
    env "${tools[@]}" "$RUSTUP" run "$toolchain" cargo "$@"
}
# The docs.rs targets both manifests restrict documentation to: one set,
# which must agree.
fake_gated() {   # fake_gated <name> <fake args...>: a fake-provider row whose build diagnostics gate too
    local name=$1; shift
    local status=0
    fake "$@" 2> "$W/$name.stderr" || status=$?
    cat "$W/$name.stderr" >&2
    [ "$status" = 0 ] || return "$status"
    # Without -vv cargo shows no dependency lint, so any diagnostic here is
    # this build's own: a warning of the test crates, a linker message, a
    # build-script warning.
    : > "$W/$name.records.json"
    bash "$HERE/scripts/diagnostics.sh" "$W/$name.records.json" "$W/$name.stderr" \
        "$W/$name.diagnostics.txt" "$HERE/scripts/dependency_diagnostics.allow" "$name"
}
docs_targets() {
    local sys moq
    sys=$(sed -n 's/^targets = \[\(.*\)\]$/\1/p' "$HERE/moq5-sys/Cargo.toml" | tr -d '" ' | tr ',' ' ')
    moq=$(sed -n 's/^targets = \[\(.*\)\]$/\1/p' "$HERE/moq5/Cargo.toml" | tr -d '" ' | tr ',' ' ')
    [ -n "$sys" ] || { echo "ctest_row.sh: moq5-sys/Cargo.toml names no docs.rs targets" >&2; return 1; }
    [ "$sys" = "$moq" ] || { echo "ctest_row.sh: the manifests' docs.rs targets differ: [$sys] vs [$moq]" >&2; return 1; }
    echo "$sys"
}
# The features docs.rs documents both crates with, which must agree too
# (comma-separated, as cargo's --features takes them).
docs_features() {
    local sys moq
    sys=$(sed -n 's/^features = \[\(.*\)\]$/\1/p' "$HERE/moq5-sys/Cargo.toml" | tr -d '" ')
    moq=$(sed -n 's/^features = \[\(.*\)\]$/\1/p' "$HERE/moq5/Cargo.toml" | tr -d '" ')
    [ "$sys" = "$moq" ] || { echo "ctest_row.sh: the manifests' docs.rs features differ: [$sys] vs [$moq]" >&2; return 1; }
    echo "$sys"
}
stated() {   # stated <variable>: its value, which must be 1 or 0
    local value=${!1:-}
    case $value in
        1|0) echo "$value" ;;
        *) echo "ctest_row.sh: $1 must state 1 or 0, not '$value'" >&2; return 2 ;;
    esac
}
carries() {   # carries <prefix> <header>: 1 when the SDK installed moq/<header>, else 0
    local includedir
    includedir=$(sdk_pc "$1" --variable=includedir libmoq-service)
    if [ -n "$includedir" ] && [ -f "$includedir/moq/$2" ]; then echo 1; else echo 0; fi
}
check_statement() {   # check_statement <prefix>: the stated player and MPEG-TS against what the SDK installed
    local prefix=$1 player mpeg2ts
    player=$(carries "$prefix" media_player.h)
    mpeg2ts=$(carries "$prefix" mpeg2ts.h)
    if [ "$PLAYER" = 1 ] && [ "$player" = 0 ]; then
        echo "ctest_row.sh: MOQ5_RUST_PLAYER=1, but the SDK $prefix has no player (MOQ_BUILD_PLAYBACK=ON builds it; MOQ_RUST_PLAYER=OFF leaves the player's rows out)" >&2
        return 1
    fi
    if [ "$MPEG2TS" = 1 ] && [ "$mpeg2ts" = 0 ]; then
        echo "ctest_row.sh: MOQ5_RUST_MPEG2TS=1, but the SDK $prefix has no MPEG-TS (MOQ_BUILD_MPEG2TS=ON builds it)" >&2
        return 1
    fi
    if [ "$MPEG2TS" = 0 ] && [ "$mpeg2ts" = 1 ]; then
        echo "ctest_row.sh: MOQ5_RUST_MPEG2TS=0 states a refusal, but the SDK $prefix carries MPEG-TS" >&2
        return 1
    fi
    if [ "$PLAYER" = 0 ] && [ "$player" = 1 ]; then
        echo "ctest_row.sh: the SDK $prefix carries the player; its rows are left out (MOQ5_RUST_PLAYER=0)"
    fi
}

# -- the SDK every row links ------------------------------------------------
export MOQ_PREFIX="$MOQ5_RUST_SDK"
# The staged SDK only, plus the pkg-config directories of its dependency
# prefixes when its closure spans them (MOQ5_SDK_PKG_CONFIG_PATH); a fake
# .pc never leaks in.
if [ -n "${MOQ5_SDK_PKG_CONFIG_PATH:-}" ]; then
    export PKG_CONFIG_PATH="$MOQ5_SDK_PKG_CONFIG_PATH"
else
    unset PKG_CONFIG_PATH
fi
moq5_sdk_export_loader "$MOQ_PREFIX"
# The link mode the SDK was installed with, stated to the pkg-config crate:
# with the mode unset it reports only a .pc file's public Libs, and an SDK
# installed static whose .pc files keep dependencies in Libs.private (the
# WTQuic managed SDK: msquic) links only when the static closure is asked for.
moq5_sdk_export_link_mode "$MOQ_PREFIX"
echo "ctest_row.sh: SDK $MOQ_PREFIX, link mode $MOQ5_SDK_LINK_MODE" \
    "(LIBMOQ_SERVICE_STATIC=${LIBMOQ_SERVICE_STATIC:-unset}, LIBMOQ_SERVICE_DYNAMIC=${LIBMOQ_SERVICE_DYNAMIC:-unset})"
# What the SDK carries, as the configuration states it: a stated player or
# MPEG-TS the SDK lacks, or a refusal of MPEG-TS it carries, fails the row.
PLAYER=$(stated MOQ5_RUST_PLAYER)
MPEG2TS=$(stated MOQ5_RUST_MPEG2TS)
check_statement "$MOQ_PREFIX"
# A qualified player adds a build with the feature to every build without
# it (the feature only adds to the crates), and the rows that need an SDK
# build their crates with it.
player_features=()
if [ "$PLAYER" = 1 ]; then player_features=(--features player); fi
export MOQ5_RUST_PLAYER=$PLAYER
echo "ctest_row.sh: the player's rows $([ "$PLAYER" = 1 ] && echo run || echo 'are left out')," \
    "MPEG-TS $([ "$MPEG2TS" = 1 ] && echo carried || echo refused)"

case $ROW in
fmt)
    for ws in . fake-tests consumer; do
        "$CARGO" fmt --manifest-path "$HERE/$ws/Cargo.toml" --all -- --check
    done ;;
clippy)
    CARGO_TARGET_DIR=$W/target product clippy --all-targets -- -D warnings
    if [ "$PLAYER" = 1 ]; then
        CARGO_TARGET_DIR=$W/target product clippy --all-targets --features player -- -D warnings
    fi
    CARGO_TARGET_DIR=$W/consumer-clippy "$CARGO" clippy \
        --manifest-path "$HERE/consumer/Cargo.toml" --all-targets -- -D warnings
    if [ "$PLAYER" = 1 ]; then
        CARGO_TARGET_DIR=$W/consumer-clippy "$CARGO" clippy \
            --manifest-path "$HERE/consumer/Cargo.toml" --all-targets --features player -- -D warnings
    fi
    CARGO_TARGET_DIR=$W/fake-target MOQ5_FAKE_BUILD=$W/fake-build MOQ5_CARGO_SUBCOMMAND=clippy \
        fake --all-targets ${player_features[@]+"${player_features[@]}"} -- -D warnings ;;
doc)
    RUSTDOCFLAGS="-D warnings" CARGO_TARGET_DIR=$W/target product doc --no-deps
    if [ "$PLAYER" = 1 ]; then
        RUSTDOCFLAGS="-D warnings" CARGO_TARGET_DIR=$W/target product doc --no-deps --features player
    fi
    # The docs.rs path: no SDK, the committed snapshots, on every target the
    # manifests restrict docs.rs to and with the features they document
    # (read first: a lookup that fails fails the row rather than
    # documenting no target).
    targets=$(docs_targets)
    features=$(docs_features)
    for target in $targets; do
        "$RUSTUP" target add --toolchain "$TOOLCHAIN" "$target" >/dev/null
        (
            unset MOQ_PREFIX
            DOCS_RS=1 RUSTDOCFLAGS="-D warnings" CARGO_TARGET_DIR=$W/docsrs \
                toolchain_cargo "$TOOLCHAIN" doc --manifest-path "$HERE/Cargo.toml" --no-deps \
                --target "$target" -p moq5-sys -p moq5 ${features:+--features "$features"}
        )
    done ;;
tests)
    # The product tests: moq5-sys (the snapshots and preambles equal a fresh
    # generation, the header comments reach the documentation), moq5's unit
    # tests and the compile-fail cases (which build the current crate
    # through cargo in an isolated target). The live rows are #[ignore]d
    # here and run by relay_rows.
    CARGO_TARGET_DIR=$W/target product test --locked -p moq5-sys -p moq5
    if [ "$PLAYER" = 1 ]; then
        CARGO_TARGET_DIR=$W/target product test --locked -p moq5-sys -p moq5 --features player
    fi ;;
fake_tests)
    CARGO_TARGET_DIR=$W/fake-target MOQ5_FAKE_BUILD=$W/fake-build fake_gated fake-tests --no-fail-fast \
        ${player_features[@]+"${player_features[@]}"} ;;
fake_asan)
    # The same rows with the provider under AddressSanitizer (its memory
    # errors, such as a logger reading past a line buffer, are its own),
    # linked with the runtime of the compiler that instrumented it.
    MOQ5_FAKE_SANITIZE=address CARGO_TARGET_DIR=$W/fake-asan-target MOQ5_FAKE_BUILD=$W/fake-asan-build \
        fake_gated fake-asan --no-fail-fast ${player_features[@]+"${player_features[@]}"} ;;
layout)
    CARGO_TARGET_DIR=$W/fake-target MOQ5_FAKE_BUILD=$W/fake-build fake_gated layout --test layout \
        ${player_features[@]+"${player_features[@]}"} ;;
installed_consumer_static|installed_consumer_shared)
    bash "$HERE/consumer/run.sh" "${ROW#installed_consumer_}" "$MOQ_PREFIX" "$W/consumer" ;;
package)
    bash "$HERE/scripts/package_check.sh" "$MOQ_PREFIX" "$W/package" ;;
msrv)
    toolchain_cargo "$MSRV" --version
    "$RUSTUP" which --toolchain "$MSRV" rustc
    for ws in . consumer; do
        CARGO_TARGET_DIR=$W/msrv-target toolchain_cargo "$MSRV" check \
            --manifest-path "$HERE/$ws/Cargo.toml" --locked --all-targets
        if [ "$PLAYER" = 1 ]; then
            CARGO_TARGET_DIR=$W/msrv-target toolchain_cargo "$MSRV" check \
                --manifest-path "$HERE/$ws/Cargo.toml" --locked --all-targets --features player
        fi
    done
    msrv_rustc=$("$RUSTUP" which --toolchain "$MSRV" rustc)
    RUSTC=$msrv_rustc CARGO="$RUSTUP run $MSRV cargo" MOQ5_CARGO_SUBCOMMAND=check \
        CARGO_TARGET_DIR=$W/msrv-fake-target MOQ5_FAKE_BUILD=$W/msrv-fake-build \
        fake --locked --all-targets ${player_features[@]+"${player_features[@]}"} ;;
gate_controls)
    # The gates' own controls: the diagnostic classifier's negatives and
    # positives, the link-mode selection, the exact link-search assertion,
    # and a failing helper failing the row.
    bash "$HERE/scripts/gate_controls.sh" "$MOQ_PREFIX" "$W/gate-controls" ;;
cross_refusal)
    # A cross build is resolved before anything is probed: with no target
    # SDK, a host search path or TARGET override only, a sysroot token
    # inside an unrelated option, disagreeing sysroots, or no sysroot, it is
    # refused with the reason, whatever PKG_CONFIG_ALLOW_CROSS says. A target
    # search configuration that holds no SDK is refused by the probe without
    # falling back to the host.
    TARGET=${1:?the target triple}
    "$RUSTUP" target add --toolchain "$TOOLCHAIN" "$TARGET" >/dev/null
    toolchain_rustc=$("$RUSTUP" which --toolchain "$TOOLCHAIN" rustc)
    refused() {   # refused <log> <phrase> [VAR=value...]
        local log=$1 phrase=$2; shift 2
        if env "$@" "RUSTC=$toolchain_rustc" CARGO_TARGET_DIR=$W/cross-refusal \
                "$RUSTUP" run "$TOOLCHAIN" cargo check \
                --manifest-path "$HERE/Cargo.toml" -p moq5-sys --target "$TARGET" > "$log" 2>&1; then
            echo "ctest_row.sh: the cross build was accepted; expected the refusal: $phrase" >&2; exit 1
        fi
        grep -q "$phrase" "$log" ||
            { echo "ctest_row.sh: refused for another reason (see $log):" >&2; tail -20 "$log" >&2; exit 1; }
    }
    host_pc=$(moq5_sdk_pkgconfig "$MOQ_PREFIX")
    empty=$W/cross-empty-search
    rm -rf "$empty"; mkdir -p "$empty/lib/pkgconfig"
    # PKG_CONFIG_ALLOW_CROSS and the host SDK (MOQ_PREFIX) alone.
    refused "$W/cross-refusal-host-sdk.log" "no target SDK is named" PKG_CONFIG_ALLOW_CROSS=1
    # The host search path and a target sysroot: still no target SDK.
    refused "$W/cross-refusal-host-path.log" "no target SDK is named" \
        PKG_CONFIG_ALLOW_CROSS=1 "PKG_CONFIG_PATH=$host_pc" "MOQ_SYSROOT_$TARGET=/"
    # The generic TARGET_ and HOST_ spellings name no particular target: an
    # inherited one is not a target SDK.
    refused "$W/cross-refusal-generic-path.log" "no target SDK is named" \
        "TARGET_PKG_CONFIG_PATH=$host_pc" "HOST_PKG_CONFIG_PATH=$host_pc" \
        "TARGET_PKG_CONFIG_LIBDIR=$host_pc" "MOQ_SYSROOT_$TARGET=/"
    # A pkg-config sysroot alone names no search directory.
    refused "$W/cross-refusal-sysroot-only.log" "no target SDK is named" "PKG_CONFIG_SYSROOT_DIR=/"
    # A target SDK without its sysroot.
    refused "$W/cross-refusal-no-sysroot.log" "no target sysroot is named" "MOQ_PREFIX_$TARGET=$MOQ_PREFIX"
    # A sysroot token inside an unrelated option is not a sysroot.
    refused "$W/cross-refusal-unrelated-option.log" "no target sysroot is named" \
        "MOQ_PREFIX_$TARGET=$MOQ_PREFIX" "BINDGEN_EXTRA_CLANG_ARGS_$TARGET=-DWHY=--sysroot"
    # A spelling bindgen does not read carries no sysroot to libclang.
    refused "$W/cross-refusal-unread-clang-args.log" "no target sysroot is named" \
        "MOQ_PREFIX_$TARGET=$MOQ_PREFIX" "TARGET_BINDGEN_EXTRA_CLANG_ARGS=--sysroot=/"
    # Mixed root options naming two roots, in either order: clang takes
    # headers from -isysroot over --sysroot, so every operand must agree.
    refused "$W/cross-refusal-mixed-isysroot-first.log" "the sysroots named disagree" \
        "MOQ_PREFIX_$TARGET=$MOQ_PREFIX" "MOQ_SYSROOT_$TARGET=/" "BINDGEN_EXTRA_CLANG_ARGS_$TARGET=-isysroot $W --sysroot=/"
    refused "$W/cross-refusal-mixed-sysroot-first.log" "the sysroots named disagree" \
        "MOQ_PREFIX_$TARGET=$MOQ_PREFIX" "MOQ_SYSROOT_$TARGET=/" "BINDGEN_EXTRA_CLANG_ARGS_$TARGET=--sysroot=/ -isysroot $W"
    # Disagreeing sysroots.
    refused "$W/cross-refusal-conflicting-sysroots.log" "the sysroots named disagree" \
        "MOQ_PREFIX_$TARGET=$MOQ_PREFIX" "MOQ_SYSROOT_$TARGET=/" "BINDGEN_EXTRA_CLANG_ARGS_$TARGET=--sysroot=$W"
    # A target search configuration holding no SDK, with the host SDK on
    # every inherited spelling pkg-config could read: no host fallback.
    refused "$W/cross-refusal-no-host-fallback.log" "has no libmoq-service.pc" \
        "PKG_CONFIG_LIBDIR_$TARGET=$empty/lib/pkgconfig" "MOQ_SYSROOT_$TARGET=/" \
        "TARGET_PKG_CONFIG_PATH=$host_pc" "PKG_CONFIG_PATH=$host_pc" "HOST_PKG_CONFIG_PATH=$host_pc" \
        "MOQ_PREFIX=$MOQ_PREFIX" PKG_CONFIG_ALLOW_CROSS=1
    echo "refusals recorded under $W/cross-refusal-*.log" ;;
cross_build)
    # A target SDK configured from the source tree with the caller's cross
    # arguments and the player and MPEG-TS as stated, its own statement
    # checked; then the crates checked for that target against it, without
    # the player and, where it is stated, with it. Each pass names the host
    # SDK on every spelling and proves it unused. The player pass must show
    # the target's generated player bindings and both crates compiled with
    # the feature, which the base pass's log must not. This is cargo check
    # (bindings generated, metadata compiled): no target executable is
    # linked or run.
    TARGET=${1:?the target triple}; SYSROOT=${2:?the target sysroot}; shift 2
    : "${MOQ5_RUST_SOURCE:?the libmoq source tree}"
    "$RUSTUP" target add --toolchain "$TOOLCHAIN" "$TARGET" >/dev/null
    toolchain_rustc=$("$RUSTUP" which --toolchain "$TOOLCHAIN" rustc)
    onoff() { if [ "$1" = 1 ]; then echo ON; else echo OFF; fi; }
    stated_options=("MOQ_BUILD_PLAYBACK=$(onoff "$PLAYER")" "MOQ_BUILD_MPEG2TS=$(onoff "$MPEG2TS")")
    # Configured fresh: a value cached by an earlier run can never stand in
    # for an option this run must state.
    rm -rf "$W/cross-build"
    cmake -S "$MOQ5_RUST_SOURCE" -B "$W/cross-build" "-DCMAKE_INSTALL_PREFIX=$W/cross-sdk" \
        -DCMAKE_BUILD_TYPE=Release -DMOQ_BUILD_TESTS=OFF -DMOQ_BUILD_SIM=OFF -DMOQ_BUILD_EXAMPLES=OFF \
        -DMOQ_BUILD_MSF=ON -DMOQ_BUILD_MEDIA_OBJECT=ON -DMOQ_BUILD_SERVICE=ON \
        "${stated_options[@]/#/-D}" "$@" \
        > "$W/cross-configure.log" 2>&1 || { tail -30 "$W/cross-configure.log" >&2; exit 1; }
    # The target tree builds what is stated.
    for option in "${stated_options[@]}"; do
        grep -q -x "${option%%=*}:BOOL=${option#*=}" "$W/cross-build/CMakeCache.txt" ||
            { echo "ctest_row.sh: the target tree's cache does not hold ${option%%=*}:BOOL=${option#*=}" >&2; exit 1; }
    done
    cmake --build "$W/cross-build" > "$W/cross-build.log" 2>&1 || { tail -30 "$W/cross-build.log" >&2; exit 1; }
    rm -rf "$W/cross-sdk"
    cmake --install "$W/cross-build" > "$W/cross-install.log" 2>&1
    # The target SDK carries what is stated, as the host SDK does.
    check_statement "$W/cross-sdk"
    target_libdir=$(moq5_sdk_libdir "$W/cross-sdk")
    host_libdir=$(moq5_sdk_libdir "$MOQ_PREFIX")
    host_pc=$(moq5_sdk_pkgconfig "$MOQ_PREFIX")
    # The target SDK's pkg-config directories, as the build script lists
    # them: the search configuration the probe must see, and nothing else.
    expected_libdir=$(for d in lib/pkgconfig lib64/pkgconfig share/pkgconfig; do
        if [ -d "$W/cross-sdk/$d" ]; then printf '%s\n' "$W/cross-sdk/$d"; fi; done | paste -sd: -)
    real_pkg_config=$(command -v "${PKG_CONFIG:-pkg-config}")
    cross_pass() {   # cross_pass <pass> [cargo args...]: the crates checked for the target, every target-only assertion
        local pass=$1; shift
        local log=$W/cross-check-$pass.log probe_env=$W/cross-probe-env-$pass.log
        local recorder=$W/cross-pkg-config-$pass emitted probes libdirs exact
        # pkg-config itself, run through a recorder of the environment it
        # was given: what the probe searched is read, not inferred.
        : > "$probe_env"
        cat > "$recorder" <<EOF
#!/bin/sh
{ printf 'probe:'; printf ' %s' "\$@"; printf '\n'; env | grep '^PKG_CONFIG' | sort; } >> "$probe_env"
exec "$real_pkg_config" "\$@"
EOF
        chmod +x "$recorder"
        env "MOQ_PREFIX_$TARGET=$W/cross-sdk" "MOQ_SYSROOT_$TARGET=$SYSROOT" CARGO_TARGET_DIR=$W/cross-target \
            "MOQ_PREFIX=$MOQ_PREFIX" "TARGET_PKG_CONFIG_PATH=$host_pc" "PKG_CONFIG_PATH=$host_pc" \
            "HOST_PKG_CONFIG_PATH=$host_pc" "TARGET_PKG_CONFIG_LIBDIR=$host_pc" "PKG_CONFIG=$recorder" \
            "RUSTC=$toolchain_rustc" \
            "$RUSTUP" run "$TOOLCHAIN" cargo check --manifest-path "$HERE/Cargo.toml" --target "$TARGET" \
            --all-targets -vv "$@" \
            > "$log" 2>&1 || { tail -30 "$log" >&2; return 1; }
        emitted=$(grep -o 'rustc-link-search=native=[^ ]*' "$log" | sed 's/^rustc-link-search=native=//' |
            sort -u | while read -r dir; do (cd "$dir" && pwd); done | sort -u)
        [ "$emitted" = "$target_libdir" ] ||
            { echo "ctest_row.sh: the $pass cross check's link-search directories are not exactly the target SDK's: $emitted" >&2; return 1; }
        if grep -q "$host_libdir" "$log"; then
            echo "ctest_row.sh: the $pass cross check mentions the host SDK $host_libdir" >&2; return 1
        fi
        # Every probe ran with exactly the target's directories as its
        # library list (pkg-config's default search path replaced) and no
        # search path.
        probes=$(grep -c '^probe:' "$probe_env" || true)
        libdirs=$(grep -c '^PKG_CONFIG_LIBDIR=' "$probe_env" || true)
        exact=$(grep -c -x -F "PKG_CONFIG_LIBDIR=$expected_libdir" "$probe_env" || true)
        [ "$probes" -gt 0 ] ||
            { echo "ctest_row.sh: the $pass cross check never ran pkg-config through the recorder" >&2; return 1; }
        if [ "$libdirs" != "$probes" ] || [ "$exact" != "$probes" ]; then
            echo "ctest_row.sh: of $probes $pass probes, $exact searched exactly $expected_libdir (see $probe_env)" >&2; return 1
        fi
        if grep -q '^PKG_CONFIG_PATH=' "$probe_env" || grep -q -F "$host_libdir" "$probe_env"; then
            echo "ctest_row.sh: a $pass probe was given a search path or the host SDK (see $probe_env)" >&2; return 1
        fi
        # The same root named through MOQ_SYSROOT and both clang root
        # options agrees, in the order clang would apply them.
        env "MOQ_PREFIX_$TARGET=$W/cross-sdk" "MOQ_SYSROOT_$TARGET=$SYSROOT" CARGO_TARGET_DIR=$W/cross-target \
            "BINDGEN_EXTRA_CLANG_ARGS_$TARGET=-isysroot $SYSROOT --sysroot=$SYSROOT" \
            "RUSTC=$toolchain_rustc" \
            "$RUSTUP" run "$TOOLCHAIN" cargo check --manifest-path "$HERE/Cargo.toml" --target "$TARGET" \
            -p moq5-sys -vv "$@" > "$W/cross-check-same-root-$pass.log" 2>&1 ||
            { tail -30 "$W/cross-check-same-root-$pass.log" >&2; return 1; }
        echo "cross check ($pass) for $TARGET selected the target SDK $target_libdir alone, every probe searching $expected_libdir only (see $log, $probe_env)"
    }
    crate_built_with_player() {   # crate_built_with_player <log> <crate>: its target compilation carries the enabled feature
        # Counted, not grep -q: an early exit would fail the pipeline on
        # the SIGPIPE of the greps before it.
        local lines
        lines=$(grep -F -- "--crate-name $2 " "$1" | grep -F -- "--target $TARGET" |
            grep -c -F -- "--cfg 'feature=\"player\"'" || true)
        [ "${lines:-0}" -gt 0 ]
    }
    player_built() {   # player_built <log>: the target's player bindings generated, both crates compiled with the feature
        local log=$1 dir generated=""
        while read -r dir; do
            if [ -n "$dir" ] && grep -q 'pub fn moq_media_player_attach' "$dir/bindings_player.rs" 2>/dev/null; then
                generated=$dir/bindings_player.rs
            fi
        done < <(grep -o "OUT_DIR=[^ ]*/$TARGET/debug/build/moq5-sys-[^/ ]*/out" "$log" | sed 's/^OUT_DIR=//' | sort -u)
        [ -n "$generated" ] || return 1
        crate_built_with_player "$log" moq5_sys || return 1
        crate_built_with_player "$log" moq5 || return 1
        echo "$generated"
    }
    rm -rf "$W/cross-target"
    cross_pass base
    if [ "$PLAYER" = 1 ]; then
        cross_pass player --features player
        if ! generated=$(player_built "$W/cross-check-player.log"); then
            echo "ctest_row.sh: the player cross check shows no generated player bindings for $TARGET, or a crate compiled without the feature (see $W/cross-check-player.log)" >&2
            exit 1
        fi
        # The control: the base pass's log must not pass for the player's.
        if player_built "$W/cross-check-base.log" >/dev/null; then
            echo "ctest_row.sh: the base cross check's log passes for the player pass; the check cannot tell them apart" >&2
            exit 1
        fi
        echo "the player pass generated $generated and compiled moq5_sys and moq5 for $TARGET with feature=\"player\"; the base pass's log shows neither"
    fi ;;
relay_rows)
    RELAY=${1:?the moq5-relay binary}
    SDK=${MOQ5_RELAY_SDK:-$MOQ_PREFIX}
    moq5_sdk_export_loader "$SDK"
    moq5_sdk_export_link_mode "$SDK"
    # The live rows link this SDK: the statement holds for it too.
    check_statement "$SDK"
    # The native peer that completes a subscription with a chosen status,
    # built against the relay SDK's own closure. MOQ5_RELAY_PEER_CFLAGS adds
    # what the installed picoquic_threaded.h needs beyond the closure's Cflags
    # (picoquic's own headers, which that SDK does not install);
    # MOQ5_RELAY_PEER_SANITIZE=thread builds it under ThreadSanitizer.
    peer_flags=()
    case ${MOQ5_RELAY_PEER_SANITIZE:-} in
        "") ;;
        thread)
            peer_flags=(-fsanitize=thread -fno-omit-frame-pointer)
            # A report makes the peer exit 66, which the done-status row
            # checks; stated, not left to the defaults (on macOS a report
            # otherwise aborts the process).
            export TSAN_OPTIONS="${TSAN_OPTIONS:+$TSAN_OPTIONS:}abort_on_error=0:exitcode=66" ;;
        *) echo "ctest_row.sh: MOQ5_RELAY_PEER_SANITIZE must be empty or thread" >&2; exit 2 ;;
    esac
    # The closure's flags are read first, so a failing lookup fails the row.
    peer_cflags=$(sdk_pc "$SDK" --cflags libmoq-service)
    peer_libs=$(sdk_pc "$SDK" --libs libmoq-service)
    # shellcheck disable=SC2086  # pkg-config output and the flags are word-split on purpose
    "$CC" -std=c11 -Wall -Wextra -Werror -O1 -g ${peer_flags[@]+"${peer_flags[@]}"} \
        $peer_cflags ${MOQ5_RELAY_PEER_CFLAGS:-} \
        "$HERE/moq5/tests/support/done_peer.c" -o "$W/done-peer" \
        $peer_libs ${peer_flags[@]+"${peer_flags[@]}"}
    # Every row on both drafts, built with the player where it is
    # qualified; the MPEG-TS rows require the outcome the SDK is stated to
    # give (the stream, or the refusal of the track).
    rows=(live_publish_receive live_end_of_track live_done_status live_interrupt live_teardown
        live_mpeg2ts live_d22)
    if [ "$PLAYER" = 1 ]; then rows+=(live_player); fi
    for draft in 22 18; do
        for row in "${rows[@]}"; do
            echo "== $row (draft $draft)"
            MOQ_PREFIX=$SDK MOQ5_RELAY=$RELAY MOQ5_DONE_PEER=$W/done-peer MOQ5_RELAY_DRAFT=$draft \
                MOQ5_EXPECT_MPEG2TS=$MPEG2TS MOQ5_RELAY_WORK=$W/live-work CARGO_TARGET_DIR=$W/live-target \
                product test --locked -p moq5 ${player_features[@]+"${player_features[@]}"} --test "$row" \
                -- --ignored
        done
    done
    # The peer's verdict reaches the row: the same done-status rows, the
    # peer exiting 3 once its done is sent (after the progress the row
    # observes), must fail on that exit and on nothing else.
    for draft in 22 18; do
        echo "== live_done_status (draft $draft), the peer exiting 3 after the done: must fail"
        if MOQ_PREFIX=$SDK MOQ5_RELAY=$RELAY MOQ5_DONE_PEER=$W/done-peer MOQ5_RELAY_DRAFT=$draft \
            MOQ5_DONE_PEER_EXIT_STATUS=3 MOQ5_RELAY_WORK=$W/live-work-verdict CARGO_TARGET_DIR=$W/live-target \
            product test --locked -p moq5 ${player_features[@]+"${player_features[@]}"} --test live_done_status \
            -- --ignored \
            > "$W/peer-verdict-$draft.log" 2>&1; then
            echo "ctest_row.sh: the done-status row passed although its peer exited 3" >&2; exit 1
        fi
        grep -q 'the done peer exited with exit status: 3' "$W/peer-verdict-$draft.log" ||
            { echo "ctest_row.sh: the row failed for another reason:" >&2; tail -30 "$W/peer-verdict-$draft.log" >&2; exit 1; }
        echo "refused on the peer's exit status 3 (see $W/peer-verdict-$draft.log)"
    done ;;
*)
    echo "ctest_row.sh: unknown row $ROW" >&2; exit 2 ;;
esac
echo "ctest_row.sh: $ROW OK"
