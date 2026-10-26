#!/bin/sh
# Permanent non-vacuity evidence for the native input-root boundary.
#
# It sources lib-modules.sh and calls moq_classify_root directly, so it
# exercises the SHIPPING classifier -- the one check-input-roots.sh uses -- not
# a second copy of the algorithm that could drift from it.
#
# Every fixture is built inside one task-owned scratch tree, so the real
# worktree is never written to and nothing here depends on the current
# declaration in lib-inputs.sh. The scratch is removed on success, on failure
# and on a signal.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
. "$here/lib-modules.sh"

# The classifier's own tools, then every command THIS FILE uses. An
# unpreflighted fixture tool is worse than a missing gate tool: if `ln` were
# absent, the producer-failure case would go non-zero for the wrong reason and
# record a KILL without ever running the failing `sort`.
moq_modules_preflight "$_self" || exit 2
for _t in mkdir chmod ln sh cat; do
    command -v "$_t" >/dev/null 2>&1 \
        || { echo "$_self: required tool not found: $_t" >&2; exit 2; }
done

# Read the first line of a file for diagnostics. `cat` was used here before,
# on the MISS paths only -- so a missing `cat` erased the diagnostic exactly
# when the oracle failed. This uses the shell's own read and never fails.
read_first() {
    _rf=""
    # `read` returns non-zero both for a real failure AND for a final line with
    # no trailing newline -- which is how the classifier prints its verdict --
    # so the DATA decides, with the status used only to name a genuine failure.
    IFS= read -r _rf < "$1" 2>/dev/null
    if [ -z "$_rf" ] && [ -s "$1" ]; then
        _rf="<unreadable>"
    fi
    printf '%s' "$_rf"
}

work=$(mktemp -d "${TMPDIR:-/tmp}/moq5-inroots-selftest-XXXXXXXX") \
    || { echo "$_self: cannot create a task-owned scratch" >&2; exit 2; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() { rm -rf -- "$work"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

# Three DISTINCT categories, never summed into one "kills" total: an ordinary
# expectation that holds is a contract case passing, not a mutant killed.
case_pass=0; case_fail=0          # ordinary classifier contract cases
ctl_ok=0;    ctl_miss=0           # deliberately perturbed failure-path controls
neu_ok=0;    neu_fail=0           # setup neuters (the fixture proving itself)

pass_() { case_pass=$((case_pass + 1)); printf 'CASE-PASS[%s]: %s\n' "$1" "$2"; }
fail_() { case_fail=$((case_fail + 1)); printf 'CASE-FAIL[%s]: %s\n' "$1" "$2"; }
ctl_()  { ctl_ok=$((ctl_ok + 1));       printf 'CONTROL[%s]: %s\n' "$1" "$2"; }
ctlx_() { ctl_miss=$((ctl_miss + 1));   printf 'CONTROL-MISS[%s]: %s\n' "$1" "$2"; }
neu_()  { neu_ok=$((neu_ok + 1));       printf 'NEUTER[%s]: %s\n' "$1" "$2"; }
neux_() { neu_fail=$((neu_fail + 1));   printf 'NEUTER-FAIL[%s]: %s\n' "$1" "$2"; }

# A miniature worktree with the SAME shape as the real one: a Gradle root
# project carrying its own build script, one nested module, and a tools
# directory holding a Java helper that must stay legal.
fx="$work/repo"
mkdir -p "$fx/$MOQ_ROOT_PROJECT/tools" \
         "$fx/$MOQ_ROOT_PROJECT/jni" \
         "$fx/$MOQ_ROOT_PROJECT/moq5-android/src/main/kotlin" \
         "$fx/core" \
    || { echo "$_self: cannot build the fixture" >&2; exit 2; }
: > "$fx/$MOQ_ROOT_PROJECT/$MOQ_BUILD_SCRIPT"            || exit 2
: > "$fx/$MOQ_ROOT_PROJECT/moq5-android/$MOQ_BUILD_SCRIPT" || exit 2
: > "$fx/$MOQ_ROOT_PROJECT/tools/PropsEcho.java"          || exit 2
: > "$fx/$MOQ_ROOT_PROJECT/moq5-android/src/main/kotlin/Value.kt" || exit 2
: > "$fx/$MOQ_ROOT_PROJECT/jni/moq5_jni.c"                || exit 2
: > "$fx/core/session.c"                                  || exit 2

# expect <case> <relative-root> <expected-verdict-prefix>
expect() {
    _c=$1; _r=$2; _want=$3
    _sc="$work/sc"
    rm -rf -- "$_sc" || { echo "$_self: cannot clear the classifier scratch" >&2; exit 2; }
    mkdir -p "$_sc" || { echo "$_self: cannot create the classifier scratch" >&2; exit 2; }
    if ! _got=$(moq_classify_root "$fx" "$_r" "$_sc"); then
        _got="UNDECIDABLE"
    fi
    case $_got in
        "$_want"*) pass_ "$_c" "$_r -> $_got" ;;
        *)         fail_ "$_c" "$_r -> $_got, expected $_want*" ;;
    esac
}

echo "== a root that contains a module is rejected, in both directions =="
expect root-project-contains-module "$MOQ_ROOT_PROJECT"                        contains-module:
expect module-itself               "$MOQ_ROOT_PROJECT/moq5-android"            under-module:
expect module-descendant           "$MOQ_ROOT_PROJECT/moq5-android/src"        under-module:
expect module-descendant-deep       "$MOQ_ROOT_PROJECT/moq5-android/src/main/kotlin" under-module:

echo "== legitimate native roots are accepted =="
expect tools-root-with-java        "$MOQ_ROOT_PROJECT/tools"                   ok
expect jni-root                    "$MOQ_ROOT_PROJECT/jni"                     ok
expect plain-c-root                "core"                                      ok

echo "== a ghost root is named, not skipped =="
expect ghost-root                  "$MOQ_ROOT_PROJECT/src"                     absent

echo "== failure-path controls =="

# A shim PATH built from RESOLVED tool locations, never hardcoded /usr/bin and
# /bin: those paths differ per platform, and a link that silently failed to be
# created would remove a tool the case never meant to remove.
SHIM_TOOLS="find grep sed printf sort mktemp rm mkdir chmod sh"

# resolve_tool <name> -- the FILESYSTEM path of a tool. `command -v` alone is
# not enough: for a shell builtin (printf is one in dash and bash) it prints the
# bare name, and linking that name produces a dangling link -- a shim missing a
# tool it believed it had.
resolve_tool() {
    _rp=$(command -v "$1" 2>/dev/null) || _rp=""
    case $_rp in
        /*) [ -x "$_rp" ] && { printf '%s' "$_rp"; return 0; } ;;
    esac
    _save_ifs=$IFS
    IFS=:
    for _d in $PATH; do
        [ -n "$_d" ] || _d=.
        if [ -f "$_d/$1" ] && [ -x "$_d/$1" ]; then
            IFS=$_save_ifs
            printf '%s' "$_d/$1"
            return 0
        fi
    done
    IFS=$_save_ifs
    return 1
}

# mk_shim <dir> -- populate <dir> with links to the real tools. Every resolution
# and every link is checked, and each link is required to resolve to something
# executable; a partial shim is a setup failure, never a quiet control result.
mk_shim() {
    _sd=$1
    mkdir -p "$_sd" || return 1
    for _t in $SHIM_TOOLS; do
        _tp=$(resolve_tool "$_t") || return 1
        [ -n "$_tp" ] || return 1
        ln -s "$_tp" "$_sd/$_t" || return 1
        [ -x "$_sd/$_t" ] || return 1
    done
    return 0
}

# The witness proves the shim under test was actually SELECTED. Without it, any
# other failure inside the classifier records the expected control result.
MOQ5_SORT_WITNESS="$work/sort-witness"
export MOQ5_SORT_WITNESS

fresh_scratch() {
    rm -rf -- "$work/sc" || return 1
    mkdir -p "$work/sc" || return 1
    rm -f "$MOQ5_SORT_WITNESS" || return 1
    return 0
}

# -- control 1: a producer that FAILS -----------------------------------------
# `sort` exits non-zero. Its output feeds the verdict, so a lost status turned
# an unclassifiable root into "ok" (chats/libmoq/1189 R2).
shim_fail="$work/shim-fail"
mk_shim "$shim_fail" || { echo "$_self: cannot build the failing-sort shim" >&2; exit 2; }
rm -f "$shim_fail/sort" || exit 2
cat > "$shim_fail/sort" <<'SORT_FAIL_EOF' || exit 2
#!/bin/sh
printf ran >> "$MOQ5_SORT_WITNESS"
exit 1
SORT_FAIL_EOF
chmod +x "$shim_fail/sort" || exit 2

fresh_scratch || exit 2
if ( PATH="$shim_fail"; export PATH; moq_classify_root "$fx" "core" "$work/sc" \
       > "$work/out" 2>/dev/null ); then
    ctlx_ producer-failure-undecidable \
        "a failing sort produced '$(read_first "$work/out")' instead of UNDECIDABLE"
elif [ ! -s "$MOQ5_SORT_WITNESS" ]; then
    ctlx_ producer-failure-undecidable \
        "classification went non-zero WITHOUT running the failing sort shim -- the control proves nothing"
else
    ctl_ producer-failure-undecidable \
        "a failing sort makes classification UNDECIDABLE (non-zero), never 'ok'"
fi

# -- control 2: a producer that SUCCEEDS but leaves an unconsumable result ----
# The sort stage exits 0 and the result is non-empty, but the read stage cannot
# consume it. Treating that read failure as "empty" is the same false pass one
# stage later (chats/libmoq/1191 F4). The shim makes the result a DIRECTORY, so
# the read fails for any user, root included.
shim_unread="$work/shim-unread"
mk_shim "$shim_unread" || { echo "$_self: cannot build the unreadable-result shim" >&2; exit 2; }
rm -f "$shim_unread/sort" || exit 2
cat > "$shim_unread/sort" <<'SORT_UNREAD_EOF' || exit 2
#!/bin/sh
out=
while [ $# -gt 0 ]; do
    case $1 in
        -o) out=$2; shift 2 ;;
        *)  shift ;;
    esac
done
[ -n "$out" ] || exit 1
rm -rf -- "$out" || exit 1
mkdir -p "$out" || exit 1
printf ran >> "$MOQ5_SORT_WITNESS"
exit 0
SORT_UNREAD_EOF
chmod +x "$shim_unread/sort" || exit 2

fresh_scratch || exit 2
if ( PATH="$shim_unread"; export PATH; moq_classify_root "$fx" "core" "$work/sc" \
       > "$work/out" 2>/dev/null ); then
    ctlx_ unreadable-result-undecidable \
        "an unconsumable non-empty result produced '$(read_first "$work/out")' instead of UNDECIDABLE"
elif [ ! -s "$MOQ5_SORT_WITNESS" ]; then
    ctlx_ unreadable-result-undecidable \
        "classification went non-zero WITHOUT running the unreadable-result shim"
elif [ ! -d "$work/sc/sorted" ]; then
    ctlx_ unreadable-result-undecidable \
        "the shim did not leave a non-empty unconsumable result; the read stage was not exercised"
else
    ctl_ unreadable-result-undecidable \
        "a non-empty result the read stage cannot consume is UNDECIDABLE, never 'ok'"
fi

# -- control 3: an unusable scratch -------------------------------------------
if moq_classify_root "$fx" "core" "$work/no-such-scratch" > "$work/out2" 2>/dev/null; then
    ctlx_ missing-scratch-undecidable \
        "an absent scratch produced '$(read_first "$work/out2")'"
else
    ctl_ missing-scratch-undecidable "an absent scratch makes classification UNDECIDABLE"
fi

# -- control 4: the missing-`rm` PREFLIGHT case -------------------------------
# The exact case chats/libmoq/1189 R2 required: with `rm` absent the gate once
# printed three PASS rows, leaked residue and exited 0. `missing-scratch` is a
# classifier case and is NOT a substitute for it. This drives the real gate
# script end to end and additionally requires that nothing was created.
shim_norm="$work/shim-norm"
mk_shim "$shim_norm" || { echo "$_self: cannot build the no-rm shim" >&2; exit 2; }
rm -f "$shim_norm/rm" || exit 2
[ ! -e "$shim_norm/rm" ] || { echo "$_self: the no-rm shim still holds rm" >&2; exit 2; }
if ( PATH="$shim_norm"; export PATH; command -v rm >/dev/null 2>&1 ); then
    echo "$_self: the no-rm shim still resolves rm" >&2
    exit 2
fi

probe_tmp="$work/probe-tmp"
mkdir -p "$probe_tmp" || exit 2
PATH="$shim_norm" TMPDIR="$probe_tmp" sh "$here/check-input-roots.sh" \
    > "$work/norm.out" 2>"$work/norm.err"
_nrc=$?
_residue=$(find "$probe_tmp" -mindepth 1)
if [ "$_nrc" -ne 2 ]; then
    ctlx_ missing-rm-preflight \
        "the gate exited $_nrc with rm absent; a complete preflight must refuse with 2"
elif ! grep -q 'required tool not found: rm' "$work/norm.err"; then
    ctlx_ missing-rm-preflight "the gate refused, but not by naming the missing rm"
elif grep -q '^PASS' "$work/norm.out"; then
    ctlx_ missing-rm-preflight "the gate printed PASS rows before refusing"
elif [ -n "$_residue" ]; then
    ctlx_ missing-rm-preflight \
        "the gate left scratch residue; the preflight must precede scratch creation"
else
    ctl_ missing-rm-preflight \
        "a missing rm is refused BY NAME before scratch creation, with no PASS row and no residue"
fi

echo "== setup neuters =="

# The producer controls above are only evidence if their shim was what made the
# classification fail. With an UNPERTURBED shim on the same PATH the very same
# call must SUCCEED and leave no witness -- so a control that passes without its
# shim would be caught here.
fresh_scratch || exit 2
if ( PATH="$shim_norm"; export PATH; moq_classify_root "$fx" "core" "$work/sc" \
       > "$work/out3" 2>/dev/null ); then
    if [ -s "$MOQ5_SORT_WITNESS" ]; then
        neux_ sort-shim-not-selected "an unperturbed shim still recorded a sort-shim witness"
    elif [ "$(read_first "$work/out3")" != "ok" ]; then
        neux_ sort-shim-not-selected \
            "an unperturbed shim classified 'core' as '$(read_first "$work/out3")', expected ok"
    else
        neu_ sort-shim-not-selected \
            "with no sort shim the same call succeeds as 'ok' and leaves no witness"
    fi
else
    neux_ sort-shim-not-selected \
        "an unperturbed shim made classification fail; the producer controls cannot be attributed"
fi

printf '\ninput-root classifier self-test:\n'
printf '  ordinary contract cases : %s passed, %s failed\n' "$case_pass" "$case_fail"
printf '  failure-path controls   : %s established, %s missed\n' "$ctl_ok" "$ctl_miss"
printf '  setup neuters           : %s held, %s failed\n' "$neu_ok" "$neu_fail"
[ "$case_fail" -eq 0 ] && [ "$ctl_miss" -eq 0 ] && [ "$neu_fail" -eq 0 ] || exit 1
exit 0
