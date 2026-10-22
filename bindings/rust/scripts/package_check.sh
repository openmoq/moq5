#!/usr/bin/env bash
# The package recipe, local only: package both crates, extract the .crate
# files into a fresh directory, build the extracted moq5-sys (the
# verification cargo would do), and build a clean consumer against the
# extracted moq5 with the extracted moq5-sys resolved through
# [patch.crates-io] (never the repository checkout). Nothing is uploaded.
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
# The packaged consumer runs with the SDK's loader path.
#
# Environment: CARGO (default cargo), LIBCLANG_PATH as bindgen needs it,
# PKG_CONFIG (default pkg-config), PYTHON (default python3), RUSTFMT,
# MOQ5_SDK_PKG_CONFIG_PATH (the pkg-config directories of the SDK's
# dependency prefixes, the clean build's PKG_CONFIG_PATH),
# LIBMOQ_SERVICE_STATIC / LIBMOQ_SERVICE_DYNAMIC (the SDK's link mode,
# carried into the clean builds).
set -euo pipefail
# A command substitution fails with the first command that fails in it
# (bash 4.4 and later; older shells keep the helpers' explicit returns).
shopt -s inherit_errexit 2>/dev/null || true
HERE=$(cd "$(dirname "$0")/.." && pwd)
PREFIX=${1:?sdk prefix}; WORK=${2:?work dir}
CARGO=${CARGO:-cargo}
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
for v in CARGO_HOME RUSTUP_HOME RUSTUP_TOOLCHAIN LIBCLANG_PATH PKG_CONFIG RUSTFMT CC AR TMPDIR \
         LIBMOQ_SERVICE_STATIC LIBMOQ_SERVICE_DYNAMIC; do
    [ -n "${!v:-}" ] && clean+=("$v=${!v}")
done
classify() {   # classify <json> <stderr> <inventory> <label>
    bash "$HERE/scripts/diagnostics.sh" "$1" "$2" "$3" "$ALLOW" "$4"
}

cd "$HERE"
for crate in moq5-sys moq5; do
    moq5_cargo_step "$WORK" "$crate.list" "$WORK/$crate.files" \
        "${clean[@]}" "CARGO_TARGET_DIR=$WORK/target" "$CARGO" package -p "$crate" --allow-dirty --list
done
# Both packaged without cargo's own verification build: moq5's copy depends
# on a registry moq5-sys that is not published, which cargo cannot resolve
# for a verification build or a packaged lock file, so moq5 is packaged
# without either; the verification of both is below, from the extracted
# packages, with structured diagnostics.
for crate in moq5-sys moq5; do
    moq5_cargo_step "$WORK" "$crate.package" "$WORK/$crate.package.out" \
        "${clean[@]}" "CARGO_TARGET_DIR=$WORK/target" "$CARGO" package -p "$crate" --allow-dirty \
        --no-verify --exclude-lockfile
done
version=$(grep -m1 '^version' "$HERE/moq5/Cargo.toml" | sed 's/.*"\(.*\)"/\1/')
for crate in moq5-sys moq5; do
    test -f "$WORK/target/package/$crate-$version.crate"
done
# What must and must not be in a package.
grep -q '^build.rs$' "$WORK/moq5-sys.files"
grep -q '^build_sysroot.rs$' "$WORK/moq5-sys.files"
grep -q '^wrapper.h$' "$WORK/moq5-sys.files"
grep -q '^docs/bindings.rs$' "$WORK/moq5-sys.files"
grep -q '^docs/preambles.md$' "$WORK/moq5-sys.files"
grep -q '^src/lib.rs$' "$WORK/moq5.files"
grep -q '^tests/compile_fail/buffer_cloned.rs$' "$WORK/moq5.files"
if grep -qE 'fake|consumer/' "$WORK/moq5-sys.files" "$WORK/moq5.files"; then
    echo "package_check.sh: a package lists a test workspace file" >&2; exit 1
fi

mkdir -p "$WORK/extracted"
for crate in moq5-sys moq5; do
    tar -xzf "$WORK/target/package/$crate-$version.crate" -C "$WORK/extracted"
done
# The extracted moq5-sys builds on its own against the SDK (what cargo's
# verification would have done), with its diagnostics classified.
"${clean[@]}" "CARGO_TARGET_DIR=$WORK/sys-target" "$CARGO" build -vv --message-format=json \
    --manifest-path "$WORK/extracted/moq5-sys-$version/Cargo.toml" \
    > "$WORK/moq5-sys.build.json" 2> "$WORK/moq5-sys.build.log" || {
    echo "package_check.sh: the extracted moq5-sys did not build (see $WORK/moq5-sys.build.log)" >&2
    tail -40 "$WORK/moq5-sys.build.log" >&2; exit 1; }
classify "$WORK/moq5-sys.build.json" "$WORK/moq5-sys.build.log" "$WORK/moq5-sys.diagnostics.txt" "extracted moq5-sys"

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

[dependencies]
moq5 = { path = "$WORK/extracted/moq5-$version" }

[patch.crates-io]
moq5-sys = { path = "$WORK/extracted/moq5-sys-$version" }

[workspace]
EOF
cp "$HERE/consumer/src/main.rs" "$WORK/consumer/src/main.rs"
cd "$WORK/consumer"
"${clean[@]}" "CARGO_TARGET_DIR=$WORK/consumer-target" "$CARGO" build -vv --message-format=json \
    > "$WORK/consumer.build.json" 2> "$WORK/consumer.build.log" || {
    echo "package_check.sh: the consumer build failed (see $WORK/consumer.build.log)" >&2
    tail -40 "$WORK/consumer.build.log" >&2; exit 1; }
classify "$WORK/consumer.build.json" "$WORK/consumer.build.log" "$WORK/consumer.diagnostics.txt" "package consumer"
grep -q "extracted/moq5-sys-$version" "$WORK/consumer.build.json" ||
    { echo "package_check.sh: the consumer did not build the extracted moq5-sys" >&2; exit 1; }
if grep -q "$HERE/moq5-sys" "$WORK/consumer.build.json" "$WORK/consumer.build.log"; then
    echo "package_check.sh: the consumer reached the repository checkout" >&2; exit 1
fi
moq5_sdk_assert_link_search "$WORK/consumer.build.json" "$PREFIX"
if grep -qi 'fake' "$WORK/consumer.build.json" "$WORK/consumer.build.log"; then
    echo "package_check.sh: the consumer build output mentions a fake provider" >&2; exit 1
fi
env -i "PATH=$PATH" "LD_LIBRARY_PATH=$closure" "DYLD_LIBRARY_PATH=$closure" \
    "$WORK/consumer-target/debug/moq5-package-consumer" | tee "$WORK/consumer.run.log"
grep -q '^moq5 consumer: OK$' "$WORK/consumer.run.log"
echo "package_check.sh: OK ($WORK)"
