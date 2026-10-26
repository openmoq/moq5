#!/bin/sh
# A deterministic identity for the Android binding candidate.
#
# `git diff | shasum` says nothing about this work: the whole `bindings/kotlin/`
# tree is UNTRACKED, so that digest is identical whether or not any of it
# exists. This manifest is the honest identifier.
#
# The inventory is GIT'S, not an extension allowlist. An allowlist missed four
# real candidate files -- two Java sources, the checked-in wrapper jar and
# gradlew.bat -- while including machine-local `local.properties`, so changing a
# developer's SDK path moved the digest and changing a Java source did not
# (chats/libmoq/1238 F1). `--exclude-standard` keeps ignored and generated state
# out naturally: build outputs, `.native/`, Gradle caches and `local.properties`
# are excluded because the repository already classifies them that way.
#
# Every stage is checked, and the HASH PRODUCER'S OWN STATUS is what is checked.
# `_h=$($sha "$f" | cut -f1)` reports `cut`'s status, not the hasher's, so a
# hasher that printed a plausible digest and exited nonzero produced a manifest
# and exit 0 (chats/libmoq/1240). Each producer therefore runs into its own
# output file, its status is read before anything parses that file, and the
# parsed value must be exactly 64 hexadecimal characters -- so partial,
# multi-line or diagnostic output can never become an identity.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)  || { echo "$_self: cannot resolve bindings/kotlin" >&2; exit 2; }
repo=$(cd "$root/../.." && pwd) || { echo "$_self: cannot resolve the worktree root" >&2; exit 2; }

command -v git >/dev/null 2>&1 || { echo "$_self: git is required" >&2; exit 2; }
sha=""
if command -v shasum >/dev/null 2>&1; then
    sha="shasum -a 256"
elif command -v sha256sum >/dev/null 2>&1; then
    sha="sha256sum"
else
    echo "$_self: no shasum or sha256sum" >&2
    exit 2
fi

scratch=$(mktemp -d "${TMPDIR:-/tmp}/moq5-manifest-XXXXXXXX") || { echo "$_self: no scratch" >&2; exit 2; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() { rm -rf -- "$scratch"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

# hash_of <label> [PATH]
# With a PATH, hashes that file; without one, hashes the manifest on stdin.
# Runs the producer into its own files, checks ITS status before parsing, then
# requires a full 64-hex digest. Sets `_hash` only on success.
_hash=""
hash_of() {
    _ho_label=$1
    if [ $# -ge 2 ]; then
        $sha "$2" > "$scratch/hout" 2> "$scratch/herr"
    else
        $sha < "$scratch/lines" > "$scratch/hout" 2> "$scratch/herr"
    fi
    _ho_rc=$?
    if [ "$_ho_rc" -ne 0 ]; then
        echo "$_self: the hash producer failed (status $_ho_rc) for $_ho_label: $(sed -n '1p' "$scratch/herr")" >&2
        return 1
    fi
    _hash=$(cut -d' ' -f1 < "$scratch/hout") \
        || { echo "$_self: cannot parse the hash producer's output for $_ho_label" >&2; return 1; }
    case $_hash in
        "" ) echo "$_self: the hash producer emitted no digest for $_ho_label" >&2; return 1 ;;
        *[!0123456789abcdef]* )
            echo "$_self: the hash producer emitted a non-hexadecimal digest for $_ho_label" >&2
            return 1 ;;
    esac
    if [ "${#_hash}" -ne 64 ]; then
        echo "$_self: the hash producer emitted a ${#_hash}-character digest for $_ho_label, expected 64" >&2
        return 1
    fi
    return 0
}

# 1. inventory -- checked, and never piped into the next stage
( cd "$repo" && git ls-files --cached --others --exclude-standard -- bindings/kotlin ) \
    > "$scratch/raw" 2>"$scratch/err" \
    || { echo "$_self: git could not list the candidate inventory: $(sed -n '1p' "$scratch/err")" >&2; exit 2; }

# 2. stable relative paths and ordering
sed 's|^bindings/kotlin/||' "$scratch/raw" > "$scratch/rel" \
    || { echo "$_self: cannot normalize the inventory paths" >&2; exit 2; }
sort "$scratch/rel" > "$scratch/files" \
    || { echo "$_self: cannot sort the inventory" >&2; exit 2; }

_n=$(grep -c . "$scratch/files") || _n=0
[ "$_n" -gt 0 ] || { echo "$_self: the candidate inventory is EMPTY; refusing to hash nothing" >&2; exit 2; }

# 3. hash the worktree contents, failing closed on any unreadable file
: > "$scratch/lines" || { echo "$_self: cannot write the manifest" >&2; exit 2; }
while IFS= read -r f; do
    [ -n "$f" ] || continue
    if [ ! -f "$root/$f" ]; then
        echo "$_self: inventoried file is missing from the worktree: $f" >&2
        exit 2
    fi
    hash_of "$f" "$root/$f" || exit 2
    printf '%s  %s\n' "$_hash" "$f" >> "$scratch/lines" \
        || { echo "$_self: cannot append $f to the manifest" >&2; exit 2; }
done < "$scratch/files"

_lines=$(grep -c . "$scratch/lines") || _lines=0
[ "$_lines" -eq "$_n" ] \
    || { echo "$_self: hashed $_lines of $_n inventoried files" >&2; exit 2; }

hash_of "the manifest" || exit 2
_digest=$_hash

if [ "${1:-}" = "--list" ]; then
    cat "$scratch/lines"
fi
printf 'android source manifest: %s file(s), digest %s\n' "$_n" "$_digest"
