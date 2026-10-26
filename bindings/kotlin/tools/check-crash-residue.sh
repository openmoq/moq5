#!/bin/sh
# No JVM crash artefact may survive a run inside the shippable inventory.
#
# A forked test worker that dies -- which an adversarial mutant can cause
# deliberately -- writes hs_err_pid*.log and replay_pid*.log into the module
# directory it was started from. Those are diagnostics, never inputs. Left
# behind they are hundreds of KiB of untracked residue sitting exactly where the
# Android tree would be staged from (chats/libmoq/1222 F4).
#
# Ignoring them is not sufficient: an ignore rule keeps them out of a commit
# while leaving them on disk, and the claim this gate makes is that a run leaves
# the tree clean. So the gate FAILS if any survives, whatever git thinks.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)  || { echo "$_self: cannot resolve bindings/kotlin" >&2; exit 2; }

for t in find grep printf; do
    command -v "$t" >/dev/null 2>&1 \
        || { echo "$_self: required tool not found: $t" >&2; exit 2; }
done

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

# The shippable inventory is the source tree, excluding build outputs and the
# native dependency stage -- neither is shipped, and both are regenerated.
_found=$(find "$root" \
    \( -path "$root/*/build" -o -path "$root/.native" -o -path "$root/.gradle" \) -prune -o \
    \( -name 'hs_err_pid*.log' -o -name 'replay_pid*.log' -o -name 'core.[0-9]*' \) -print \
    2>/dev/null)

if [ -n "$_found" ]; then
    fail crash-residue.absent "JVM crash artefact(s) left in the shippable inventory:
$_found"
else
    pass crash-residue.absent "no JVM crash or replay artefact in the shippable inventory"
fi

# The ignore rule exists so that one surviving artefact cannot be committed by
# accident before this gate is next run. It is a second line, not the first.
if grep -q '^hs_err_pid\*\.log$' "$root/.gitignore" 2>/dev/null &&
    grep -q '^replay_pid\*\.log$' "$root/.gitignore" 2>/dev/null; then
    pass crash-residue.ignored "the ignore rule names both JVM artefact kinds"
else
    fail crash-residue.ignored "bindings/kotlin/.gitignore does not name hs_err_pid*.log and replay_pid*.log"
fi

printf '\n'
if [ "$fails" -eq 0 ]; then
    printf 'crash residue: 0 failure(s)\n'
    exit 0
fi
printf 'crash residue: %s failure(s)\n' "$fails"
exit 1
