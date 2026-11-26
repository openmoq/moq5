#!/usr/bin/env bash
# The package recipe, local only: package the four crates (moq5-sys,
# moq5-core, moq5-media, moq5), extract the .crate files into a fresh
# directory, build the extracted moq5-sys (the verification cargo would do)
# and a consumer of the extracted moq5-core alone (the core-only graph), and
# build a clean consumer against the extracted moq5, every lower crate
# resolved to its extracted package through [patch.crates-io] (never the
# repository checkout). Nothing is uploaded.
#
#   package_check.sh <sdk-prefix> <work-dir>
#
# Every cargo step runs in the same clean environment (the toolchain
# variables and the SDK, nothing inherited: neither PKG_CONFIG_PATH nor a
# target-specific override reaches it). Each step's diagnostics gate as its
# exit status does: `cargo package --list` and the package creation through
# scripts/cargo_step.sh (their stderr: cargo's own warnings), the builds by
# their structured diagnostics; all are classified by scripts/diagnostics.py
# and inventoried under <work-dir>;
# the consumer's verbose log must name the extracted moq5-sys, link from the
# SDK's own library directory and from no directory outside its closure.
# The packaged consumer runs with the SDK's loader path. With
# MOQ5_RUST_PLAYER=1 the extracted moq5-sys and the consumer are built with
# the player.
#
# Environment: MOQ5_RUST_PLAYER (1 or 0, default 0), CARGO (default cargo),
# LIBCLANG_PATH as bindgen needs it,
# PKG_CONFIG (default pkg-config), PYTHON (default python3), RUSTFMT,
# MOQ5_SDK_PKG_CONFIG_PATH (the pkg-config directories of the SDK's
# dependency prefixes, the clean build's PKG_CONFIG_PATH),
# LIBMOQ_<TIER>_STATIC / _DYNAMIC (the SDK's link mode for each tier
# package, carried into the clean builds).
set -euo pipefail
# A command substitution fails with the first command that fails in it
# (bash 4.4 and later; older shells keep the helpers' explicit returns).
shopt -s inherit_errexit 2>/dev/null || true
HERE=$(cd "$(dirname "$0")/.." && pwd)
PREFIX=${1:?sdk prefix}; WORK=${2:?work dir}
CARGO=${CARGO:-cargo}
features=()
case ${MOQ5_RUST_PLAYER:-0} in
    0) ;;
    1) features=(--features player) ;;
    *) echo "package_check.sh: MOQ5_RUST_PLAYER must be 1 or 0" >&2; exit 2 ;;
esac
# shellcheck source=sdk_paths.sh
. "$HERE/scripts/sdk_paths.sh"
# shellcheck source=cargo_step.sh
. "$HERE/scripts/cargo_step.sh"
rm -rf "$WORK"
mkdir -p "$WORK"
closure=$(moq5_sdk_runtime_path "$PREFIX")
ALLOW=$HERE/scripts/dependency_diagnostics.allow

clean=(env -i "PATH=$PATH" "HOME=$HOME" "MOQ_PREFIX=$PREFIX")
[ -n "${MOQ5_SDK_PKG_CONFIG_PATH:-}" ] && clean+=("PKG_CONFIG_PATH=$MOQ5_SDK_PKG_CONFIG_PATH")
for v in CARGO_HOME RUSTUP_HOME RUSTUP_TOOLCHAIN RUSTC RUSTDOC LIBCLANG_PATH PKG_CONFIG RUSTFMT CC AR \
         TMPDIR CARGO_NET_OFFLINE; do
    [ -n "${!v:-}" ] && clean+=("$v=${!v}")
done
for stem in $MOQ5_SDK_TIER_STEMS; do
    for v in "${stem}_STATIC" "${stem}_DYNAMIC"; do
        [ -n "${!v:-}" ] && clean+=("$v=${!v}")
    done
done
CRATES="moq5-sys moq5-core moq5-media moq5"
classify() {   # classify <json> <stderr> <inventory> <label>
    bash "$HERE/scripts/diagnostics.sh" "$1" "$2" "$3" "$ALLOW" "$4"
}

cd "$HERE"
for crate in $CRATES; do
    moq5_cargo_step "$WORK" "$crate.list" "$WORK/$crate.files" \
        "${clean[@]}" "CARGO_TARGET_DIR=$WORK/target" "$CARGO" package -p "$crate" --allow-dirty --list
done
# All packaged without cargo's own verification build: each copy above
# moq5-sys depends on registry versions of the lower crates that are not
# published, which cargo cannot resolve for a verification build or a
# packaged lock file, so they are packaged without either; the verification
# is below, from the extracted packages, with structured diagnostics.
for crate in $CRATES; do
    moq5_cargo_step "$WORK" "$crate.package" "$WORK/$crate.package.out" \
        "${clean[@]}" "CARGO_TARGET_DIR=$WORK/target" "$CARGO" package -p "$crate" --allow-dirty \
        --no-verify --exclude-lockfile
done
version=$(grep -m1 '^version' "$HERE/moq5/Cargo.toml" | sed 's/.*"\(.*\)"/\1/')
for crate in $CRATES; do
    test -f "$WORK/target/package/$crate-$version.crate"
done
# What must and must not be in a package: every tier's wrapper, snapshot
# and preambles among them.
grep -q '^build.rs$' "$WORK/moq5-sys.files"
grep -q '^build_sysroot.rs$' "$WORK/moq5-sys.files"
grep -q '^build_tiers.rs$' "$WORK/moq5-sys.files"
for tier in core sim media locmaf service player; do
    grep -q "^wrapper_$tier.h\$" "$WORK/moq5-sys.files"
    grep -q "^docs/bindings_$tier.rs\$" "$WORK/moq5-sys.files"
    grep -q "^docs/preambles_$tier.md\$" "$WORK/moq5-sys.files"
done
grep -q '^src/namespace.rs$' "$WORK/moq5-core.files"
grep -q '^src/cmaf.rs$' "$WORK/moq5-media.files"
grep -q '^src/locmaf.rs$' "$WORK/moq5-media.files"
grep -q '^tests/locmaf_compile_fail/locmaf_moves_between_threads.rs$' "$WORK/moq5-media.files"
grep -q '^src/lib.rs$' "$WORK/moq5.files"
grep -q '^src/player.rs$' "$WORK/moq5.files"
grep -q '^tests/compile_fail/buffer_cloned.rs$' "$WORK/moq5.files"
grep -q '^tests/compile_fail/player_output_sent_to_thread.rs$' "$WORK/moq5.files"
for crate in $CRATES; do
    if grep -qE 'fake|consumer/' "$WORK/$crate.files"; then
        echo "package_check.sh: the $crate package lists a test workspace file" >&2; exit 1
    fi
done

mkdir -p "$WORK/extracted"
for crate in $CRATES; do
    tar -xzf "$WORK/target/package/$crate-$version.crate" -C "$WORK/extracted"
done
# The extracted moq5-sys builds on its own against the SDK (what cargo's
# verification would have done), with its diagnostics classified.
"${clean[@]}" "CARGO_TARGET_DIR=$WORK/sys-target" "$CARGO" build -vv --message-format=json \
    --manifest-path "$WORK/extracted/moq5-sys-$version/Cargo.toml" ${features[@]+"${features[@]}"} \
    > "$WORK/moq5-sys.build.json" 2> "$WORK/moq5-sys.build.log" || {
    echo "package_check.sh: the extracted moq5-sys did not build (see $WORK/moq5-sys.build.log)" >&2
    tail -40 "$WORK/moq5-sys.build.log" >&2; exit 1; }
classify "$WORK/moq5-sys.build.json" "$WORK/moq5-sys.build.log" "$WORK/moq5-sys.diagnostics.txt" "extracted moq5-sys"

# The patches the consumers of the extracted packages use: each registry
# dependency on a lower crate resolves to its extracted package (a graph
# patches only the crates it holds; an unused patch is a cargo warning).
core_patches="moq5-sys = { path = \"$WORK/extracted/moq5-sys-$version\" }
moq5-core = { path = \"$WORK/extracted/moq5-core-$version\" }"
patches="$core_patches
moq5-media = { path = \"$WORK/extracted/moq5-media-$version\" }"

# The extracted moq5-core alone: the core-only graph of the packages, its
# moq5-sys the extracted one with its default features.
mkdir -p "$WORK/core-consumer/src"
cat > "$WORK/core-consumer/Cargo.toml" <<EOF
[package]
name = "moq5-package-core-consumer"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
moq5-core = { path = "$WORK/extracted/moq5-core-$version" }

[patch.crates-io]
$core_patches

[workspace]
EOF
cp "$HERE/consumer/core/src/main.rs" "$WORK/core-consumer/src/main.rs"
(
    cd "$WORK/core-consumer"
    "${clean[@]}" "CARGO_TARGET_DIR=$WORK/core-consumer-target" "$CARGO" build -vv --message-format=json \
        > "$WORK/core-consumer.build.json" 2> "$WORK/core-consumer.build.log"
) || {
    echo "package_check.sh: the core consumer build failed (see $WORK/core-consumer.build.log)" >&2
    tail -40 "$WORK/core-consumer.build.log" >&2; exit 1; }
classify "$WORK/core-consumer.build.json" "$WORK/core-consumer.build.log" "$WORK/core-consumer.diagnostics.txt" \
    "package core consumer"
grep -q 'moq5-sys: native source: .*packages for these tiers: libmoq-core$' "$WORK/core-consumer.build.json" ||
    { echo "package_check.sh: the packaged core graph did not probe libmoq-core alone" >&2; exit 1; }
env -i "PATH=$PATH" "LD_LIBRARY_PATH=$closure" "DYLD_LIBRARY_PATH=$closure" \
    "$WORK/core-consumer-target/debug/moq5-package-core-consumer" | tee "$WORK/core-consumer.run.log"
grep -q '^moq5-core consumer: OK$' "$WORK/core-consumer.run.log"

# A clean consumer: moq5 by path to the extracted package, whose moq5-sys
# dependency is a registry version that [patch.crates-io] points at the
# extracted moq5-sys. The registry has no such crate; the patch supplies it.
mkdir -p "$WORK/consumer/src"
cat > "$WORK/consumer/Cargo.toml" <<EOF
[package]
name = "moq5-package-consumer"
version = "0.0.0"
edition = "2021"
publish = false

[features]
player = ["moq5/player"]

[dependencies]
moq5 = { path = "$WORK/extracted/moq5-$version" }

[patch.crates-io]
$patches

[workspace]
EOF
cp "$HERE/consumer/src/main.rs" "$WORK/consumer/src/main.rs"
cd "$WORK/consumer"
"${clean[@]}" "CARGO_TARGET_DIR=$WORK/consumer-target" "$CARGO" build -vv --message-format=json \
    ${features[@]+"${features[@]}"} > "$WORK/consumer.build.json" 2> "$WORK/consumer.build.log" || {
    echo "package_check.sh: the consumer build failed (see $WORK/consumer.build.log)" >&2
    tail -40 "$WORK/consumer.build.log" >&2; exit 1; }
classify "$WORK/consumer.build.json" "$WORK/consumer.build.log" "$WORK/consumer.diagnostics.txt" "package consumer"
for crate in moq5-sys moq5-core moq5-media; do
    grep -q "extracted/$crate-$version" "$WORK/consumer.build.json" ||
        { echo "package_check.sh: the consumer did not build the extracted $crate" >&2; exit 1; }
    if grep -q "$HERE/$crate" "$WORK/consumer.build.json" "$WORK/consumer.build.log" \
        "$WORK/core-consumer.build.json" "$WORK/core-consumer.build.log"; then
        echo "package_check.sh: a consumer reached the repository checkout's $crate" >&2; exit 1
    fi
done
moq5_sdk_assert_link_search "$WORK/consumer.build.json" "$PREFIX"
if grep -qi 'fake' "$WORK/consumer.build.json" "$WORK/consumer.build.log"; then
    echo "package_check.sh: the consumer build output mentions a fake provider" >&2; exit 1
fi
env -i "PATH=$PATH" "LD_LIBRARY_PATH=$closure" "DYLD_LIBRARY_PATH=$closure" \
    "$WORK/consumer-target/debug/moq5-package-consumer" | tee "$WORK/consumer.run.log"
grep -q '^moq5 consumer: OK$' "$WORK/consumer.run.log"
if [ ${#features[@]} != 0 ]; then
    grep -q '^player linked$' "$WORK/consumer.run.log" ||
        { echo "package_check.sh: the packaged consumer ran without the player" >&2; exit 1; }
fi
echo "package_check.sh: OK ($WORK)"
