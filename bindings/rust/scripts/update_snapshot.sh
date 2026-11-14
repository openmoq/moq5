#!/usr/bin/env bash
# Regenerate the docs.rs snapshots of moq5-sys, one pair per tier:
# docs/bindings_<tier>.rs (with its provenance header) and
# docs/preambles_<tier>.md, from a build against an installed SDK.
# tests/snapshot.rs then holds each equal to a fresh generation on every
# qualified target.
#
#   update_snapshot.sh <sdk-prefix> [tier...]
#
# The tiers are core, sim, media, service and player; with none named, all
# five (the SDK then needs the simulator and the player). Naming tiers
# updates only those, for an SDK without the others; the core is always
# built, and a tier's snapshot does not depend on which others are built.
#
# The generation is the one this script's own cargo invocation built or
# found fresh: cargo's machine-readable record of moq5-sys's build script in
# that invocation names its out_dir, and exactly one such record is
# required. Another feature set's build in the same target directory is
# never read, however recent.
#
# Environment: CARGO (default cargo), LIBCLANG_PATH as bindgen needs it,
# RUSTFMT (the formatter bindgen runs; the snapshot is held to its output),
# PYTHON (default python3), CARGO_TARGET_DIR (default target/ beside the
# workspace), MOQ5_SNAPSHOT_DIR (where the snapshots are written; default
# moq5-sys/docs, the committed ones).
set -euo pipefail
HERE=$(cd "$(dirname "$0")/.." && pwd)
PREFIX=${1:?sdk prefix}
shift
tiers=("$@")
[ ${#tiers[@]} -gt 0 ] || tiers=(core sim media service player)
features=()
for tier in "${tiers[@]}"; do
    case $tier in
        core) ;;
        sim|media|service|player) features+=("$tier") ;;
        *) echo "usage: update_snapshot.sh <sdk-prefix> [core|sim|media|service|player...]" >&2; exit 2 ;;
    esac
done
CARGO=${CARGO:-cargo}
export MOQ_PREFIX="$PREFIX"
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HERE/target}
DOCS=${MOQ5_SNAPSHOT_DIR:-$HERE/moq5-sys/docs}
mkdir -p "$DOCS"

feature_list=$(IFS=,; echo "${features[*]-}")
mkdir -p "$CARGO_TARGET_DIR"
records=$(mktemp "$CARGO_TARGET_DIR/snapshot-build.XXXXXX")
trap 'rm -f "$records"' EXIT
"$CARGO" build --manifest-path "$HERE/Cargo.toml" -p moq5-sys ${feature_list:+--features "$feature_list"} \
    --message-format=json > "$records"
# The out_dir of this invocation's moq5-sys build script, from cargo's
# own record of it (emitted for a fresh build script too).
out=$("${PYTHON:-python3}" -I -c '
import json, sys
dirs = []
for line in open(sys.argv[1], encoding="utf-8", errors="replace"):
    if not line.startswith("{"):
        continue
    try:
        record = json.loads(line)
    except ValueError:
        continue
    package = record.get("package_id", "")
    if record.get("reason") == "build-script-executed" and (
            "/moq5-sys#" in package or "#moq5-sys@" in package):
        dirs.append(record["out_dir"])
if len(dirs) != 1:
    sys.exit(f"update_snapshot.sh: {len(dirs)} build-script records of moq5-sys in this build, not one")
print(dirs[0])
' "$records")
echo "update_snapshot.sh: this build's moq5-sys generation is $out"
for tier in "${tiers[@]}"; do
    [ -f "$out/bindings_$tier.rs" ] ||
        { echo "update_snapshot.sh: the build wrote no bindings_$tier.rs into $out" >&2; exit 1; }
done

# The commit is recorded only when git's top level is this source tree: in a
# plain copy (an export without .git), git would otherwise answer for
# whatever repository encloses the copy.
root=$(cd "$HERE/../.." && pwd -P)
top=$(git -C "$root" rev-parse --show-toplevel 2>/dev/null || true)
if [ -n "$top" ] && [ "$(cd "$top" && pwd -P)" = "$root" ]; then
    commit=$(git -C "$root" rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
    if [ -n "$(git -C "$root" status --porcelain -- core/include sim/include \
            service/include media 2>/dev/null)" ]; then
        commit="$commit (native headers modified in the worktree)"
    fi
else
    commit="unknown (not a git checkout)"
fi
# The packages the generating build linked, with their Requires, as its
# provenance report names them.
packages=$(sed -n 's/^moq5-sys: native source: .*packages for these tiers: //p' "$out/provenance.txt")
[ -n "$packages" ] || { echo "update_snapshot.sh: no package line in $out/provenance.txt" >&2; exit 1; }
sdk=""
for package in ${packages//,/ }; do
    pc=""
    for d in lib/pkgconfig lib64/pkgconfig; do
        [ -f "$PREFIX/$d/$package.pc" ] && pc="$PREFIX/$d/$package.pc" && break
    done
    [ -n "$pc" ] || { echo "update_snapshot.sh: no $package.pc under $PREFIX" >&2; exit 2; }
    requires=$(sed -n 's/^Requires: *//p' "$pc")
    sdk="${sdk:+$sdk; }$package.pc Requires: ${requires:-(none)}"
done
target=$(rustc -vV | sed -n 's/^host: //p')
bindgen=$(awk '/^name = "bindgen"$/ { getline; sub(/^version = "/, ""); sub(/"$/, ""); print }' "$HERE/Cargo.lock")
# The libclang that generated them: where it was found, and its version,
# which decides whether macro constants carry documentation.
libclang="from ${LIBCLANG_PATH:-"(clang-sys search)"} ($(cat "$out/libclang.txt"))"

for tier in "${tiers[@]}"; do
    {
        echo "// A snapshot of the generated $tier-tier bindings, compiled only under"
        echo "// cfg(docsrs), where no SDK is installed to generate them from. Regenerated"
        echo "// by scripts/update_snapshot.sh; tests/snapshot.rs holds it equal to a fresh"
        echo "// generation on every qualified target."
        echo "//"
        echo "// libmoq commit: $commit"
        echo "// SDK: $sdk"
        echo "// generated on: $target with bindgen $bindgen, libclang $libclang"
        echo
        cat "$out/bindings_$tier.rs"
    } > "$DOCS/bindings_$tier.rs"
    cp "$out/preambles_$tier.md" "$DOCS/preambles_$tier.md"
    echo "update_snapshot.sh: wrote $DOCS/bindings_$tier.rs and preambles_$tier.md from $out"
done
