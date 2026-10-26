#!/bin/sh
# Guards the boundary of the NATIVE DSO source digest.
#
# Two ways the digest quietly stops meaning what it says, and one rule each:
#
#  1. A GHOST ROOT -- a declared root naming a path that does not exist. It
#     reads as coverage in the inventory while contributing nothing, and it can
#     stay wrong indefinitely because a missing root is skipped rather than
#     failed. Every declared root must be a real directory.
#
#  2. A GRADLE MODULE'S SOURCE IN THE DSO DIGEST. A Kotlin library module cannot
#     change the packaged libmoq5_jni.so. Hashing it here would invalidate the
#     native stage on every Kotlin edit and would assert a dependency that does
#     not exist. The AAR needs its own artifact digest covering Kotlin and the
#     native payload together; that boundary is NOT this one.
#
#     Overlap is checked in BOTH directions: a root may sit AT or UNDER a
#     module, and it may also CONTAIN one.
#
# The classification lives in lib-modules.sh, which selftest-input-roots.sh also
# sources, so the self-test exercises this exact classifier rather than a copy
# of it that could drift.
#
# Fail-closed: an unreadable declaration, a root that cannot be inspected, or a
# classification that comes back UNDECIDABLE is a named failure, never an empty
# pass.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
# Path handling above uses parameter expansion and the `cd`/`pwd` builtins only,
# so no external command runs before the preflight below.
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)  || { echo "$_self: cannot resolve bindings/kotlin" >&2; exit 2; }
repo=$(cd "$root/../.." && pwd) || { echo "$_self: cannot resolve the worktree root" >&2; exit 2; }
. "$here/lib-inputs.sh"
. "$here/lib-modules.sh"

# The COMPLETE tool preflight, before scratch creation and before any external
# command runs -- cleanup included. A missing `rm` previously let this gate
# print three PASS rows, leak residue and exit 0 (chats/libmoq/1189 R2).
moq_modules_preflight "$_self" || exit 2

usage() { echo "usage: $_self [REPO-ROOT]" >&2; exit 2; }
[ $# -le 1 ] || usage
if [ $# -eq 1 ]; then
    case $1 in -*) usage ;; esac
    repo=$1
fi
[ -d "$repo" ] || { echo "$_self: not a directory: $repo" >&2; exit 2; }

scratch=$(mktemp -d "${TMPDIR:-/tmp}/moq5-inroots-XXXXXXXX") \
    || { echo "$_self: cannot create a task-owned scratch" >&2; exit 2; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() { rm -rf -- "$scratch"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

# -- rule 1: no ghost roots ----------------------------------------------------
_ghost=""
_undec=""
_n=0
for r in $MOQ_INPUT_ROOTS; do
    _n=$((_n + 1))
    if ! _v=$(moq_classify_root "$repo" "$r" "$scratch"); then
        _undec="$r"
        break
    fi
    [ "$_v" = "absent" ] && { _ghost="$r"; break; }
done
if [ -n "$_undec" ]; then
    fail input-roots.exist "declared input root '$_undec' could not be classified: a producer failed, so its status is UNDECIDABLE and the boundary cannot be confirmed"
elif [ -n "$_ghost" ]; then
    fail input-roots.exist "declared input root '$_ghost' is not a directory under $repo: a root that does not exist reads as coverage while hashing nothing"
elif [ "$_n" -eq 0 ]; then
    fail input-roots.exist "no input roots are declared at all"
else
    pass input-roots.exist "all $_n declared input root(s) exist"
fi

# -- rule 2: no Gradle module overlaps the native digest, either direction -----
_bad=""
for r in $MOQ_INPUT_ROOTS; do
    if ! _v=$(moq_classify_root "$repo" "$r" "$scratch"); then
        _bad="$r: classification is UNDECIDABLE because a producer failed, so overlap cannot be ruled out"
        break
    fi
    case $_v in
        under-module:*)
            _bad="$r lies at or under module '${_v#under-module:}'"; break ;;
        contains-module:*)
            _bad="$r contains module '${_v#contains-module:}'"; break ;;
    esac
done
if [ -n "$_bad" ]; then
    fail input-roots.native-only "a Gradle module's source would enter the native DSO digest: $_bad -- a Kotlin module cannot change libmoq5_jni.so, and the AAR provenance boundary is a separate digest that does not exist yet"
else
    pass input-roots.native-only "no declared root lies at or under, or contains, a Gradle module other than $MOQ_ROOT_PROJECT"
fi

# -- rule 3: the individually declared files are real, or declared optional ----
# MOQ_INPUT_REQUIRED_FILES is the subset that must exist; anything else in
# MOQ_INPUT_FILES is hashed IF present, so its appearance moves the digest.
_missing=""
for f in $MOQ_INPUT_REQUIRED_FILES; do
    [ -f "$repo/$f" ] || { _missing="$f"; break; }
done
if [ -n "$_missing" ]; then
    fail input-roots.required-files "required input file '$_missing' is absent"
else
    pass input-roots.required-files "every required input file exists"
fi

# -- rule 4: the tools directory is SPLIT, and nothing there is unclassified ---
# `bindings/kotlin/tools` is deliberately not an input root any more: only the
# files the topology producer consumes bind the DSO (ruling 1734). A tool that
# matches neither declaration is UNDECIDABLE, which is a failure here rather
# than a quiet absence from both manifests.
_tunc=""
_tn=0; _tt=0
if [ -d "$repo/$MOQ_TOOLS_ROOT" ]; then
    _tlist="$scratch/tools.all"
    if ! find "$repo/$MOQ_TOOLS_ROOT" -type f -print > "$_tlist"; then
        _tunc="the tools directory could not be inventoried"
    else
        while IFS= read -r _tf; do
            [ -n "$_tf" ] || continue
            _trel=${_tf#"$repo"/}
            if _tk=$(inputs_classify_tool "$_trel"); then
                case $_tk in
                    native)  _tn=$((_tn + 1)) ;;
                    tooling) _tt=$((_tt + 1)) ;;
                esac
            else
                _tunc="$_trel"; break
            fi
        done < "$_tlist"
    fi
fi
if [ -n "$_tunc" ]; then
    fail input-roots.tools-split "a path under $MOQ_TOOLS_ROOT is classified as neither native nor tooling: $_tunc -- an unclassified tool would leave both manifests, so the boundary cannot be confirmed"
elif [ "$_tn" -eq 0 ]; then
    fail input-roots.tools-split "no file under $MOQ_TOOLS_ROOT is declared native: the topology producer's own recipe must bind the DSO, and the required family would be vacuous"
else
    pass input-roots.tools-split "every file under $MOQ_TOOLS_ROOT is classified: $_tn native (build-affecting), $_tt tooling/evidence"
fi

# -- rule 5: each declared native tool exists ---------------------------------
_tmiss=""
for f in $MOQ_NATIVE_TOOL_FILES; do
    [ -f "$repo/$f" ] || { _tmiss="$f"; break; }
done
if [ -n "$_tmiss" ]; then
    fail input-roots.native-tools "declared native tool '$_tmiss' is absent: it would read as coverage while hashing nothing"
else
    pass input-roots.native-tools "every declared native tool file exists"
fi

if [ "$fails" -eq 0 ]; then
    printf '\nnative input-root boundary: 0 failure(s)\n'
    exit 0
fi
printf '\nnative input-root boundary: %s failure(s)\n' "$fails"
exit 1
