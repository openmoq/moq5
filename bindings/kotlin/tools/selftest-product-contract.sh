#!/bin/sh
# Non-vacuity evidence for the PRODUCT-mode contracts of check-jni-topology.sh:
# the build-root rule (ruling 1171 B), the pinned-only/linked archive split
# (ruling 1171 C), and the nested-stage path guard (ruling 1171 blocker 1).
#
# Those branches are reachable only for mode=product, so a synthetic fixture
# cannot exercise them -- it is refused as the product topology by design.
#
# The mutants are therefore applied IN PLACE to the durable product stage, and
# they must be: the link map records ABSOLUTE paths, so a copied stage fails the
# path-anchored link oracle for a reason that has nothing to do with the
# contract under test. Only four small text files are ever touched
# (topology.provenance, topology.log, link.map, jni-compile.json, per ABI).
# Each is saved first,
# restored on EVERY exit path including a signal, and the restoration is
# verified with cmp. The stage is regenerable with build-jni-topology.sh
# --force if a restore ever fails, and the suite says so loudly.
#
# The writer probes run the real recipe, which clears the fixed build container
# beside the stage. That whole container is preserved first and put back by the
# probe itself while the recipe still holds the lock; see "the durable build
# container" below.
#
# A mutant that does not change the checker's verdict is a MISS and fails the
# suite, so a case can never pass by being inert.
#
# MOQ5_PRODUCT_CONTRACT_RESTORE_FAULT is a self-test control for this suite's
# own restore path and is never set in ordinary use. <mode>-<probe>, where probe
# is 5 (the succeeding writer probe) or 7 (the failing one):
#   fail-N  the probe's restore fails after the container was cleared
#   skip-N  the probe restores nothing but reports success (a deferred restore)
# Either must turn the suite red by name. A third mode, cleanup, damages the
# sentinel build member at exit and makes the exit path's restore fail; the run
# must then exit non-zero, keep its lock and its preserved copies.
set -u
LC_ALL=C; export LC_ALL

here=$(cd "$(dirname "$0")" && pwd) || { echo "cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)        || { echo "cannot resolve bindings/kotlin" >&2; exit 2; }
. "$here/lib-declaration.sh"

for t in awk sed grep cp mv rm mkdir cmp printf mktemp find; do
    command -v "$t" >/dev/null 2>&1 || { echo "selftest-product-contract: required tool not found: $t" >&2; exit 2; }
done

dep="$root/.native"
# The durable stage by default; a successor stage built beside it with
# build-jni-topology.sh <dir> is named explicitly. It must be a direct child of
# the dependency root, where the recipe writes and the shared lock guards.
[ $# -le 1 ] || { echo "usage: $(basename "$0") [STAGE-DIR]" >&2; exit 2; }
STAGE=${1:-"$dep/_topology"}
case $(cd "$(dirname "$STAGE")" 2>/dev/null && pwd -P) in
    "$(cd "$dep" && pwd -P)") ;;
    *) echo "selftest-product-contract: $STAGE is not a stage directly under $dep" >&2; exit 2 ;;
esac
CHECKER="$here/check-jni-topology.sh"
[ -x "$CHECKER" ] || [ -r "$CHECKER" ] || { echo "selftest-product-contract: no checker at $CHECKER" >&2; exit 2; }
if [ ! -d "$STAGE" ]; then
    echo "selftest-product-contract: no durable product stage at $STAGE -- run build-jni-topology.sh first" >&2
    exit 2
fi

work=$(mktemp -d "${TMPDIR:-/tmp}/moq5-product-contract-XXXXXXXX") \
    || { echo "selftest-product-contract: cannot create a task-owned scratch" >&2; exit 2; }

# ---- the mutation lock -------------------------------------------------------
# This suite breaks the durable stage on purpose and puts it back. Anything else
# reading the stage during that window sees a mutant and reports it as a product
# failure, so the window is declared rather than left to timing.
#
# mkdir is the atomic primitive: it succeeds for exactly one caller and fails for
# every other. There is deliberately NO waiting and NO retry -- a second run is
# refused, not queued.
#
# The lock is a SIBLING of the stage; the stage's path manifest is exact, so a
# lock inside it would itself be an unclassified path.
LOCKDIR="$dep/_topology.lock"
LOCK_HELD=0
case $LOCKDIR in
    "$STAGE"|"$STAGE"/*)
        echo "selftest-product-contract: refusing a lock path inside the audited stage: $LOCKDIR" >&2
        rm -rf -- "$work"; exit 2 ;;
esac
if ! mkdir "$LOCKDIR" 2>/dev/null; then
    echo "selftest-product-contract: the durable topology stage is already locked for mutation ($LOCKDIR$([ -f "$LOCKDIR/owner" ] && printf ', held by %s' "$(cat "$LOCKDIR/owner" 2>/dev/null)")); refusing to mutate it concurrently. Re-run when the holder has finished; if no such run is active, the lock is stale and may be removed." >&2
    rm -rf -- "$work"; exit 2
fi
LOCK_HELD=1
# The token is minted here and written nowhere else, so presenting it is proof of
# holding the lock rather than a flag any caller could pass.
LOCK_TOKEN=$(mktemp -u "moq5-lock-XXXXXXXXXXXX" | sed 's|.*/||')
printf '%s\n' "$LOCK_TOKEN" > "$LOCKDIR/token" \
    || { rmdir "$LOCKDIR" 2>/dev/null; echo "cannot write the lock token" >&2; rm -rf -- "$work"; exit 2; }
printf 'selftest-product-contract pid %s\n' "$$" > "$LOCKDIR/owner" 2>/dev/null
MOQ5_TOPOLOGY_LOCK_TOKEN=$LOCK_TOKEN
export MOQ5_TOPOLOGY_LOCK_TOKEN
# The guard probes below stand up a lock that must look FOREIGN to the checker
# and the recipe. It carries its own token, never exported, so that cleanup can
# still recognise it as this run's and a genuinely foreign lock never is.
PROBE_TOKEN=$(mktemp -u "moq5-probe-XXXXXXXXXXXX" | sed 's|.*/||')

# The exact set of stage files this suite may write, saved before anything runs
# and restored unconditionally afterwards.
GUARDED=""
for _a in $(find "$STAGE" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; | sort); do
    for _n in topology.provenance topology.log link.map jni-compile.json; do
        GUARDED="$GUARDED $STAGE/$_a/$_n"
    done
done
for _g in $GUARDED; do
    [ -f "$_g" ] || { echo "selftest-product-contract: the stage is missing $_g" >&2; exit 2; }
    cp "$_g" "$work/$(printf '%s' "${_g#"$STAGE"/}" | tr / _).saved" \
        || { echo "selftest-product-contract: cannot save $_g" >&2; exit 2; }
done
# lock_token_is <token>: the lock on disk carries exactly <token>
lock_token_is() { [ -n "$1" ] && [ "$(cat "$LOCKDIR/token" 2>/dev/null)" = "$1" ]; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() {
    # Runs once: the signal trap's own exit re-enters through the EXIT trap.
    [ "${CLEANUP_RAN:-0}" -eq 0 ] || return 0
    CLEANUP_RAN=1
    _rcbad=0
    # Ownership first. Nothing below writes to the stage or the build container
    # unless this run holds the mutation lock: its own, the guard probes' lock it
    # installed itself, or a fresh one it takes now. A lock held by anyone else
    # means neither tree is touched and the lock is left exactly where it is.
    _own=0
    if [ "${LOCK_HELD:-0}" -eq 1 ]; then
        lock_token_is "$LOCK_TOKEN" && _own=1
    elif lock_token_is "${PROBE_TOKEN:-}"; then
        _own=1
    elif mkdir "$LOCKDIR" 2>/dev/null; then
        _own=1
    fi
    if [ "$_own" -eq 1 ]; then
        LOCK_HELD=1
        printf '%s\n' "$LOCK_TOKEN" > "$LOCKDIR/token" 2>/dev/null || _rcbad=1
        printf 'selftest-product-contract pid %s\n' "$$" > "$LOCKDIR/owner" 2>/dev/null
    else
        LOCK_HELD=0
    fi
    _cfault=0
    if [ "$_own" -eq 1 ] && [ "${MOQ5_PRODUCT_CONTRACT_RESTORE_FAULT-}" = cleanup ] \
       && [ "${SENTINEL_OWNED:-0}" -eq 1 ] && [ ! -L "$BUILD_SENTINEL" ] && [ -f "$BUILD_SENTINEL" ]; then
        printf 'damaged by the cleanup restore-fault control\n' >> "$BUILD_SENTINEL" && _cfault=1
    fi
    for _g in $GUARDED; do
        _sv="$work/$(printf '%s' "${_g#"$STAGE"/}" | tr / _).saved"
        cmp -s "$_sv" "$_g" && continue
        if [ "$_own" -eq 1 ] && cp "$_sv" "$_g" 2>/dev/null && cmp -s "$_sv" "$_g"; then
            :
        else
            _rcbad=1
        fi
    done
    if [ -n "${BUILD_BACKUP:-}" ] && [ -d "$BUILD_BACKUP" ]; then
        if build_fingerprint "$work/bfp.exit" && cmp -s "$work/bfp.before" "$work/bfp.exit"; then
            :
        elif [ "$_own" -eq 1 ] && [ "$_cfault" -eq 0 ] && restore_whole_build; then
            echo "selftest-product-contract: the durable build container was restored on exit" >&2
        else
            _rcbad=1
        fi
    fi
    # The sentinel goes only from a container proven exact; a tree that may be
    # partial is left exactly as found.
    # Only the exact sentinel this run created: a regular file, not a link,
    # still carrying the text this run wrote.
    if [ "$_rcbad" -eq 0 ] && [ "${SENTINEL_OWNED:-0}" -eq 1 ]; then
        if [ "$_own" -eq 1 ] && [ ! -L "$BUILD_SENTINEL" ] && [ -f "$BUILD_SENTINEL" ] \
           && [ "$(cat "$BUILD_SENTINEL" 2>/dev/null)" = "$SENTINEL_TEXT" ]; then
            rm -f -- "$BUILD_SENTINEL" || _rcbad=1
        else
            _rcbad=1
        fi
    fi
    if [ "$_rcbad" -ne 0 ]; then
        # Every preserved copy stays: the saved stage files and the build backup.
        KEEP_WORK=1
        if [ "$_own" -eq 1 ]; then
            # A tree that may be partial is never unlocked for a reader: the lock
            # stays, and says how to recover.
            printf 'selftest-product-contract pid %s: RESTORE FAILED, the stage or build container may be partial. Saved stage files: %s/*.saved; preserved build container: %s. Restore from them or rebuild with build-jni-topology.sh --force, then remove this lock.\n' \
                "$$" "$work" "${BUILD_BACKUP:-<none taken>}" > "$LOCKDIR/owner" 2>/dev/null
            echo "selftest-product-contract: the mutation lock $LOCKDIR is KEPT so no reader audits a partial tree" >&2
        else
            echo "selftest-product-contract: the mutation lock ($LOCKDIR) is not this run's, so neither the stage nor the build container was touched and the lock was left in place" >&2
        fi
        echo "selftest-product-contract: FAIL[cleanup-restore]: NOT fully restored. The saved stage files are under $work (*.saved), the preserved build container is at ${BUILD_BACKUP:-<none taken>}" >&2
        printf 'product-contract self-test: NOT GREEN -- the exit path could not restore the stage and build container\n'
        exit 3
    fi
    # Release ONLY a lock this run holds, and only over trees proven exact.
    [ "$_own" -eq 1 ] && rm -rf -- "$LOCKDIR"
    [ "${KEEP_WORK:-0}" -eq 1 ] || rm -rf -- "$work"
    return 0
}
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

# ---- the durable build container ---------------------------------------------
# The checker reads evidence from the fixed build container beside the stage --
# the staged picotls prefix, the configure's selections, every product build
# root -- and the writer probes below run the real recipe, which clears and
# recreates that container. So its WHOLE content -- every file and symlink, with
# a sentinel member beside the build files -- is fingerprinted and preserved
# here, while this run holds the lock, and the preserved copy is kept until the
# suite has proven the container exact again.
BUILD_CONTAINER="$dep/_topology.build"
[ -d "$BUILD_CONTAINER" ] \
    || { echo "selftest-product-contract: no durable build container at $BUILD_CONTAINER -- run build-jni-topology.sh first" >&2; exit 2; }
# One implementation of fingerprint and restore, shared by this suite and by the
# probe scripts the recipe runs under its own lock.
#   build-tree.sh <lib> fingerprint <root> <out>
#   build-tree.sh <lib> restore <container> <backup> <expected> <out>
cat > "$work/build-tree.sh" <<'TREE' || { echo "selftest-product-contract: cannot prepare the build-tree helper" >&2; exit 2; }
#!/bin/sh
set -u
LC_ALL=C; export LC_ALL
. "$1" || exit 2
shift
fingerprint() {
    ( cd "$1" && find . \( -type f -o -type l \) -print ) > "$2.list" || return 1
    sort "$2.list" > "$2.sorted" || return 1
    : > "$2" || return 1
    while IFS= read -r _bf; do
        if [ -L "$1/$_bf" ]; then
            _bl=$(readlink "$1/$_bf") || return 1
            printf 'link %s %s\n' "$_bf" "$_bl" >> "$2" || return 1
        else
            _bh=$(decl_sha256 "$1/$_bf") || return 1
            printf 'file %s %s\n' "$_bf" "$_bh" >> "$2" || return 1
        fi
    done < "$2.sorted"
    rm -f -- "$2.list" "$2.sorted"
}
case $1 in
    fingerprint) fingerprint "$2" "$3" ;;
    restore)
        rm -rf -- "$2" || exit 1
        cp -Rp "$3" "$2" || exit 1
        fingerprint "$2" "$5" || exit 1
        cmp -s "$4" "$5" ;;
    *) exit 2 ;;
esac
TREE
# build_fingerprint <out> [root]: default root is the durable build container
build_fingerprint() {
    sh "$work/build-tree.sh" "$here/lib-declaration.sh" fingerprint "${2:-$BUILD_CONTAINER}" "$1"
}
# restore_whole_build: put the preserved container back and prove it exact
restore_whole_build() {
    sh "$work/build-tree.sh" "$here/lib-declaration.sh" restore \
        "$BUILD_CONTAINER" "$BUILD_BACKUP" "$work/bfp.before" "$work/bfp.restored"
}
BUILD_SENTINEL_ABI=$(find "$STAGE" -mindepth 1 -maxdepth 1 -type d | sed -n '1p')
BUILD_SENTINEL="$BUILD_CONTAINER/$(basename "$BUILD_SENTINEL_ABI")/product-contract.sentinel"
# The sentinel must be a NEW path this run creates: an existing file would be
# overwritten and a symlink followed, possibly out of the container. Any existing
# name -- a dangling symlink included, which -e alone does not see -- is refused
# before anything is opened, and creation itself refuses to clobber.
if [ -e "$BUILD_SENTINEL" ] || [ -L "$BUILD_SENTINEL" ]; then
    echo "selftest-product-contract: FAIL[sentinel-exists]: $BUILD_SENTINEL already exists; refusing to overwrite or follow it. Nothing was modified." >&2
    exit 2
fi
SENTINEL_TEXT="a durable build member outside the checker inputs ($LOCK_TOKEN)"
( set -C; printf '%s\n' "$SENTINEL_TEXT" > "$BUILD_SENTINEL" ) 2>/dev/null \
    || { echo "selftest-product-contract: FAIL[sentinel-exists]: cannot create $BUILD_SENTINEL as a new file; nothing was overwritten" >&2; exit 2; }
SENTINEL_OWNED=1
build_fingerprint "$work/bfp.before" \
    || { echo "selftest-product-contract: cannot fingerprint the durable build container" >&2; exit 2; }
grep -q '^file \./[^/]*/product-contract\.sentinel ' "$work/bfp.before" \
    || { echo "selftest-product-contract: the build fingerprint does not include the sentinel" >&2; exit 2; }
_bb="$work/build-backup"
cp -Rp "$BUILD_CONTAINER" "$_bb" \
    || { echo "selftest-product-contract: cannot preserve the durable build container" >&2; exit 2; }
build_fingerprint "$work/bfp.backup" "$_bb" && cmp -s "$work/bfp.before" "$work/bfp.backup" \
    || { echo "selftest-product-contract: the preserved build container is not an exact copy" >&2; exit 2; }
BUILD_BACKUP=$_bb
# probe-restore.sh <probe>: run by a writer probe INSIDE the recipe, so the
# container is back and proven exact while the recipe still holds its lock. Its
# status is the probe's verdict: a failed restore fails the probe, and the suite
# names it before it takes any lock back.
cat > "$work/probe-restore.sh" <<RESTORE || { echo "selftest-product-contract: cannot prepare the probe restore" >&2; exit 2; }
#!/bin/sh
_n=\$1
cat "$LOCKDIR/owner" > "$work/restore\$_n.owner" 2>/dev/null
case "\${MOQ5_PRODUCT_CONTRACT_RESTORE_FAULT-}" in
    "skip-\$_n") _rs=0 ;;
    "fail-\$_n") sh "$work/build-tree.sh" "$here/lib-declaration.sh" restore "$BUILD_CONTAINER" "$work/no-such-backup" "$work/bfp.before" "$work/bfp.probe\$_n"; _rs=\$? ;;
    *) sh "$work/build-tree.sh" "$here/lib-declaration.sh" restore "$BUILD_CONTAINER" "$BUILD_BACKUP" "$work/bfp.before" "$work/bfp.probe\$_n"; _rs=\$? ;;
esac
printf '%s' "\$_rs" > "$work/restore\$_n.rc"
exit "\$_rs"
RESTORE

kills=0; misses=0; cases=0
kill_() { cases=$((cases + 1)); kills=$((kills + 1)); echo "KILL[$1]: $2"; }
miss_() { cases=$((cases + 1)); misses=$((misses + 1)); echo "MISS[$1]: $2"; }

COPY="$STAGE"
ABI=$(find "$COPY" -mindepth 1 -maxdepth 1 -type d | sed -n '1p')
ABI=$(basename "$ABI")
[ -n "$ABI" ] || { echo "the copied stage holds no ABI directory" >&2; exit 2; }
PROV="$COPY/$ABI/topology.provenance"
RCPT="$COPY/$ABI/topology.log"
LMAP="$COPY/$ABI/link.map"

# run_checker -> echoes the checker's combined output; never aborts the suite
run_checker() {
    sh "$CHECKER" --dep-stage "$dep" "$COPY" > "$work/out" 2>&1
    printf '%s' "$?" > "$work/rc"
    cat "$work/out"
}

echo "== the mutation lock refuses concurrent readers and writers =="
# Proven with a FOREIGN lock -- one this run does not hold -- so the guard is
# exercised exactly as a genuine second process would meet it. The suite's own
# lock is released for the duration and re-acquired afterwards, and every step
# is checked so a failure here can never leave the guard down.
rm -rf -- "$LOCKDIR" || { echo "cannot release the lock for the guard probe" >&2; exit 2; }
LOCK_HELD=0
mkdir "$LOCKDIR" || { echo "cannot install the foreign lock probe" >&2; exit 2; }
printf '%s\n' "$PROBE_TOKEN" > "$LOCKDIR/token" || { echo "cannot write the probe token" >&2; exit 2; }
printf 'foreign probe\n' > "$LOCKDIR/owner" 2>/dev/null

# (a) the checker refuses, by name, BEFORE auditing the stage
if sh "$CHECKER" --dep-stage "$dep" "$COPY" > "$work/lock1" 2>&1; then
    miss_ lock-refuses-checker "the checker audited a stage that was locked for mutation"
elif grep -q 'locked for mutation' "$work/lock1"; then
    if grep -qE '^(PASS|FAIL)\[[a-z0-9-]+\.' "$work/lock1"; then
        miss_ lock-refuses-checker "the checker refused, but only after auditing oracles"
    else
        kill_ lock-refuses-checker "$(grep -m1 'locked for mutation' "$work/lock1" | cut -c1-140)"
    fi
else
    miss_ lock-refuses-checker "the checker failed, but not with the lock diagnosis: $(grep -m1 '^FAIL' "$work/lock1" | cut -c1-140)"
fi

# (b) a second instance of THIS suite refuses before mutating anything
_pv_before=$(decl_sha256 "$PROV") || { echo "cannot hash the provenance" >&2; exit 2; }
if sh "$0" > "$work/lock2" 2>&1; then
    miss_ lock-refuses-second-run "a second product-contract run proceeded while the stage was locked"
elif grep -q 'already locked for mutation' "$work/lock2"; then
    _pv_after=$(decl_sha256 "$PROV") || { echo "cannot hash the provenance" >&2; exit 2; }
    if [ "$_pv_before" = "$_pv_after" ]; then
        kill_ lock-refuses-second-run "$(grep -m1 'already locked for mutation' "$work/lock2" | cut -c1-140)"
    else
        miss_ lock-refuses-second-run "the refused run still altered the stage"
    fi
else
    miss_ lock-refuses-second-run "the second run failed, but not on the lock: $(sed -n '1p' "$work/lock2" | cut -c1-140)"
fi

# (c) the refused run must not have removed the lock it does not own
if [ -d "$LOCKDIR" ]; then
    kill_ lock-not-stolen "the refused run left the holder's lock in place"
else
    miss_ lock-not-stolen "the refused run removed a lock it did not acquire"
    mkdir "$LOCKDIR" || { echo "cannot restore the probe lock" >&2; exit 2; }
fi

# (d) the WRITER is refused too, and refuses without touching the stage.
#     Proven by fingerprinting every file in the stage across the refusal, so
#     "nothing was deleted or modified" is measured rather than asserted.
RECIPE="$here/build-jni-topology.sh"
[ -r "$RECIPE" ] || { echo "no recipe at $RECIPE" >&2; exit 2; }
stage_fingerprint() { find "$STAGE" -type f -exec shasum -a 256 {} + | sort; }
stage_fingerprint > "$work/fp.before" || { echo "cannot fingerprint the stage" >&2; exit 2; }
if sh "$RECIPE" --force > "$work/lock4" 2>&1; then
    miss_ lock-refuses-writer "the recipe rebuilt a stage that was locked for mutation"
elif grep -q 'locked for mutation' "$work/lock4"; then
    stage_fingerprint > "$work/fp.after" || { echo "cannot fingerprint the stage" >&2; exit 2; }
    if cmp -s "$work/fp.before" "$work/fp.after"; then
        kill_ lock-refuses-writer "$(grep -m1 'locked for mutation' "$work/lock4" | cut -c1-140)"
    else
        miss_ lock-refuses-writer "the refused recipe still changed the durable stage"
    fi
else
    miss_ lock-refuses-writer "the recipe failed, but not on the lock: $(sed -n '1p' "$work/lock4" | cut -c1-140)"
fi

# (e) the refused writer must not have taken the holder's lock away
if [ -d "$LOCKDIR" ]; then
    kill_ lock-not-stolen-by-writer "the refused recipe left the holder's lock in place"
else
    miss_ lock-not-stolen-by-writer "the refused recipe removed a lock it did not acquire"
    mkdir "$LOCKDIR" || { echo "cannot restore the probe lock" >&2; exit 2; }
fi

# (f) while the WRITER genuinely holds the lock, the checker refuses by name.
#     The recipe's probe seam makes this deterministic: no sleep, no race, and
#     the observation happens with a real recipe-held lock on disk.
#
#     Both probe cases build into a TEMPORARY stage path, so the durable stage is
#     never the thing being deleted. They do still clear the fixed build
#     container, which the recipe always does, so each probe puts the preserved
#     container back and proves it exact BEFORE it returns: the recipe releases
#     its lock only after the probe, so no reader or writer can ever meet the
#     container missing or partial.
PROBE_STAGE="$work/probe-stage"
rm -rf -- "$LOCKDIR" || { echo "cannot release the probe lock" >&2; exit 2; }
cat > "$work/probe.sh" <<PROBE
#!/bin/sh
# runs INSIDE build-jni-topology.sh, while it holds the mutation lock
sh "$CHECKER" --dep-stage "$dep" "$STAGE" > "$work/lock5" 2>&1
printf '%s' "\$?" > "$work/lock5.rc"
cat "$LOCKDIR/owner" > "$work/lock5.owner" 2>/dev/null
# the recipe still holds its lock: put the durable build container back first
sh "$work/probe-restore.sh" 5 || exit 4
exit 0
PROBE
chmod +x "$work/probe.sh" || { echo "cannot prepare the probe" >&2; exit 2; }
if MOQ5_TOPOLOGY_LOCK_PROBE="$work/probe.sh" sh "$RECIPE" --force "$PROBE_STAGE" > "$work/lock6" 2>&1; then
    _prc=$(cat "$work/lock5.rc" 2>/dev/null)
    if [ "${_prc:-0}" = "0" ]; then
        miss_ writer-lock-refuses-checker "the checker audited the stage while the recipe held the lock"
    elif grep -q 'locked for mutation' "$work/lock5" \
         && ! grep -qE '^(PASS|FAIL)\[[a-z0-9-]+\.' "$work/lock5" \
         && grep -q 'build-jni-topology' "$work/lock5.owner"; then
        kill_ writer-lock-refuses-checker "the checker refused before auditing, naming the recipe as holder"
    else
        miss_ writer-lock-refuses-checker "refused, but not by name/before auditing: $(grep -m1 '^FAIL' "$work/lock5" | cut -c1-120)"
    fi
else
    miss_ writer-lock-refuses-checker "the probe run of the recipe did not complete: $(sed -n '$p' "$work/lock6" | cut -c1-140)"
fi
[ ! -d "$LOCKDIR" ] || miss_ writer-lock-released "the recipe kept its lock after a successful probe run"
# after_probe <case> <n>: the probe restored the container while the recipe held
# its lock, and the container is exact once that lock is gone -- checked BEFORE
# this suite takes any lock back, so a restore that only happens later misses.
after_probe() {
    _cs=$1; _n=$2
    _rr=$(cat "$work/restore$_n.rc" 2>/dev/null)
    if [ "${_rr:-none}" != 0 ]; then
        miss_ "$_cs" "the probe did not restore the build container exactly (status ${_rr:-none})"
    elif ! grep -q 'build-jni-topology' "$work/restore$_n.owner" 2>/dev/null; then
        miss_ "$_cs" "the build container was not restored under the recipe's lock"
    elif ! build_fingerprint "$work/bfp.post$_n"; then
        miss_ "$_cs" "cannot fingerprint the build container after the probe"
    elif ! cmp -s "$work/bfp.before" "$work/bfp.post$_n"; then
        miss_ "$_cs" "the build container differs from its original once the recipe released its lock: $(diff "$work/bfp.before" "$work/bfp.post$_n" | sed -n '2p' | cut -c1-120)"
    else
        kill_ "$_cs" "restored under the recipe's lock; byte-identical after it released ($(wc -l < "$work/bfp.before" | tr -d ' ') members, sentinel included)"
        return 0
    fi
    # The container was unprotected once the recipe unlocked; nothing after this
    # point may be credited. The exit path restores it under a lock this run
    # takes, or keeps the backup and says where.
    printf '\nproduct-contract self-test: %s kill(s), %s miss(es), %s case(s) -- stopped: the build container was not restored before the writer unlocked\n' "$kills" "$misses" "$cases"
    exit 1
}
after_probe whole-build-restored-by-writer-probe 5

# (g) a FAILING writer path releases the lock it acquired and leaves no partial
#     stage behind.
cat > "$work/probefail.sh" <<PROBE || { echo "cannot prepare the failing probe" >&2; exit 2; }
#!/bin/sh
# runs INSIDE build-jni-topology.sh, while it holds the mutation lock
sh "$work/probe-restore.sh" 7 || exit 4
exit 3
PROBE
chmod +x "$work/probefail.sh" || { echo "cannot prepare the failing probe" >&2; exit 2; }
if MOQ5_TOPOLOGY_LOCK_PROBE="$work/probefail.sh" sh "$RECIPE" --force "$PROBE_STAGE" > "$work/lock7" 2>&1; then
    miss_ writer-failure-releases-lock "a failing recipe path reported success"
elif [ -d "$LOCKDIR" ]; then
    miss_ writer-failure-releases-lock "a failing recipe path left its lock behind"
elif [ -e "$PROBE_STAGE" ]; then
    miss_ writer-failure-releases-lock "a failing recipe path left a partial stage behind"
elif ! grep -q 'the lock probe reported failure (3)' "$work/lock7"; then
    miss_ writer-failure-releases-lock "the recipe failed, but not on the intended probe failure: $(sed -n '$p' "$work/lock7" | cut -c1-140)"
else
    kill_ writer-failure-releases-lock "lock released and the partial stage removed on the failure path"
fi
after_probe whole-build-restored-by-failing-writer-probe 7

# (h) a lock inside the audited stage is not tolerated: the exact path manifest
#     rejects it, so it can never become a quiet resident of the stage
rm -rf -- "$LOCKDIR" || { echo "cannot remove the probe lock" >&2; exit 2; }
mkdir "$COPY/$ABI/.topology.lock" || { echo "cannot install the in-stage probe" >&2; exit 2; }
if sh "$CHECKER" --dep-stage "$dep" "$COPY" > "$work/lock3" 2>&1; then
    rmdir "$COPY/$ABI/.topology.lock"
    miss_ lock-inside-stage-rejected "a lock directory inside the audited stage was tolerated"
else
    rmdir "$COPY/$ABI/.topology.lock" || { echo "cannot remove the in-stage probe" >&2; exit 2; }
    if grep -q 'unclassified path in the audit stage' "$work/lock3"; then
        kill_ lock-inside-stage-rejected "$(grep -m1 'unclassified path in the audit stage' "$work/lock3" | cut -c1-140)"
    else
        miss_ lock-inside-stage-rejected "rejected, but not as an unclassified stage path: $(grep -m1 '^FAIL' "$work/lock3" | cut -c1-140)"
    fi
fi

# the suite takes its own lock back before it mutates anything
mkdir "$LOCKDIR" || { echo "cannot re-acquire the mutation lock" >&2; exit 2; }
LOCK_HELD=1
printf '%s\n' "$LOCK_TOKEN" > "$LOCKDIR/token" || { echo "cannot rewrite the lock token" >&2; exit 2; }
printf 'selftest-product-contract pid %s\n' "$$" > "$LOCKDIR/owner" 2>/dev/null

# (i) the exit path never writes under a lock this run does not hold. With a
#     stage file and the build container both deliberately differing from their
#     saved originals, cleanup runs while a lock it cannot claim is on disk: it
#     must touch neither tree, leave that lock in place, and name the preserved
#     copies. The lock is handed over by rewriting its token, never released.
cp "$PROV" "$work/cf.prov" || { echo "cannot save the provenance" >&2; exit 2; }
printf 'cleanup-control=1\n' >> "$PROV" || { echo "cannot mutate the provenance" >&2; exit 2; }
CF_EXTRA="$(dirname "$BUILD_SENTINEL")/product-contract.cleanup-control"
{ [ ! -e "$CF_EXTRA" ] && [ ! -L "$CF_EXTRA" ]; } \
    || { echo "selftest-product-contract: $CF_EXTRA already exists; refusing to overwrite or follow it" >&2; exit 2; }
( set -C; printf 'a member the exit path must not remove\n' > "$CF_EXTRA" ) 2>/dev/null \
    || { echo "cannot place the control member" >&2; exit 2; }
stage_fingerprint > "$work/cf.stage.before" || { echo "cannot fingerprint the stage" >&2; exit 2; }
build_fingerprint "$work/cf.build.before" || { echo "cannot fingerprint the build container" >&2; exit 2; }
printf '%s\n' "$PROBE_TOKEN" > "$LOCKDIR/token" || { echo "cannot hand the lock over" >&2; exit 2; }
printf 'foreign cleanup control\n' > "$LOCKDIR/owner" 2>/dev/null
LOCK_HELD=0
( trap - EXIT HUP INT TERM; PROBE_TOKEN=; KEEP_WORK=1; cleanup ) > "$work/cf.log" 2>&1
_cfs=$?
stage_fingerprint > "$work/cf.stage.after" || { echo "cannot fingerprint the stage" >&2; exit 2; }
build_fingerprint "$work/cf.build.after" || { echo "cannot fingerprint the build container" >&2; exit 2; }
if [ "$_cfs" -eq 0 ]; then
    miss_ cleanup-refuses-foreign-lock "the exit path reported success though it could restore nothing"
elif ! cmp -s "$work/cf.stage.before" "$work/cf.stage.after"; then
    miss_ cleanup-refuses-foreign-lock "the exit path rewrote the stage under a lock it does not hold"
elif ! cmp -s "$work/cf.build.before" "$work/cf.build.after"; then
    miss_ cleanup-refuses-foreign-lock "the exit path changed the build container under a lock it does not hold"
elif ! lock_token_is "$PROBE_TOKEN"; then
    miss_ cleanup-refuses-foreign-lock "the exit path removed or replaced a lock it does not hold"
elif ! grep -q 'is not this run' "$work/cf.log" || ! grep -qF "$BUILD_BACKUP" "$work/cf.log"; then
    miss_ cleanup-refuses-foreign-lock "the exit path stood down without naming the lock and the preserved copies: $(sed -n '1p' "$work/cf.log" | cut -c1-120)"
else
    kill_ cleanup-refuses-foreign-lock "stage and build untouched, the foreign lock kept, the preserved copies named"
fi
printf '%s\n' "$LOCK_TOKEN" > "$LOCKDIR/token" || { echo "cannot take the lock back" >&2; exit 2; }
printf 'selftest-product-contract pid %s\n' "$$" > "$LOCKDIR/owner" 2>/dev/null
LOCK_HELD=1
cp "$work/cf.prov" "$PROV" || { echo "cannot restore the provenance" >&2; exit 2; }
cmp -s "$work/cf.prov" "$PROV" || { echo "the provenance was not restored byte-identically" >&2; exit 2; }
rm -f -- "$CF_EXTRA" || { echo "cannot remove the control member" >&2; exit 2; }
build_fingerprint "$work/cf.build.restored" && cmp -s "$work/bfp.before" "$work/cf.build.restored" \
    || { echo "the build container is not back to its original after the cleanup control" >&2; exit 2; }

# The pristine stage must be GREEN, or every mutant below would be attributed to
# the wrong cause. This also proves the holder is admitted through its own lock.
_out=$(run_checker)
_rc=$(cat "$work/rc")
if [ "$_rc" -ne 0 ]; then
    echo "selftest-product-contract: the unmutated product stage is not green, so no mutant can be attributed:" >&2
    printf '%s\n' "$_out" | grep '^FAIL' >&2
    exit 1
fi
echo "baseline: the unmutated product stage passes every oracle"

# expect_fail <case> <substring> -- run the checker and require a NAMED failure
expect_fail() {
    _cs=$1; _sub=$2
    _o=$(run_checker); _r=$(cat "$work/rc")
    if [ "$_r" -eq 0 ]; then
        miss_ "$_cs" "the checker still passed: the mutant changed nothing the contract observes"
        return
    fi
    if printf '%s\n' "$_o" | grep -q -- "$_sub"; then
        kill_ "$_cs" "$(printf '%s\n' "$_o" | grep -m1 -- "$_sub" | cut -c1-150)"
    else
        miss_ "$_cs" "the checker failed, but not for the expected reason: $(printf '%s\n' "$_o" | grep -m1 '^FAIL' | cut -c1-150)"
    fi
}

# --- provenance mutants -------------------------------------------------------
# Each saves the file, mutates, runs, restores, and proves the restore was exact.
prov_mutate() {
    _cs=$1; _sedexpr=$2; _sub=$3
    cp "$PROV" "$work/prov.orig" || { echo "cannot save the provenance" >&2; exit 2; }
    sed "$_sedexpr" "$work/prov.orig" > "$PROV" || { echo "cannot mutate the provenance" >&2; exit 2; }
    if cmp -s "$work/prov.orig" "$PROV"; then
        miss_ "$_cs" "the mutation did not change the provenance at all"
    else
        expect_fail "$_cs" "$_sub"
    fi
    cp "$work/prov.orig" "$PROV" || { echo "cannot restore the provenance" >&2; exit 2; }
    cmp -s "$work/prov.orig" "$PROV" || { echo "the provenance was not restored byte-identically" >&2; exit 2; }
}

echo "== the product build root is declared, bounded, and outside the stage =="
prov_mutate build-root-missing '/^build\.root=/d' \
    "missing provenance key: build.root"
prov_mutate build-root-outside-container "s|^build\\.root=.*|build.root=/tmp|" \
    "not inside the recipe-owned container"
prov_mutate build-root-inside-stage "s|^build\\.root=.*|build.root=$COPY/$ABI/audit-inputs|" \
    "not inside the recipe-owned container"
prov_mutate build-root-is-dep-stage "s|^build\\.root=.*|build.root=$dep|" \
    "not inside the recipe-owned container"

echo "== the linked/pinned archive split is mechanically visible =="
cp "$PROV" "$work/prov.orig4" || { echo "cannot save the provenance" >&2; exit 2; }
awk '{ print } /^dependency\.libssl\.a=/ { sub(/^dependency\./, "archive."); print }' \
    "$work/prov.orig4" > "$PROV" || { echo "cannot mutate the provenance" >&2; exit 2; }
expect_fail pinned-key-renamed-to-linked "unknown provenance key: archive.libssl.a"
cp "$work/prov.orig4" "$PROV" || { echo "cannot restore the provenance" >&2; exit 2; }
cmp -s "$work/prov.orig4" "$PROV" || { echo "the provenance was not restored byte-identically" >&2; exit 2; }
prov_mutate pinned-key-missing '/^dependency\.libssl\.a=/d' \
    "missing provenance key: dependency.libssl.a"
prov_mutate pinned-hash-wrong \
    's|^dependency\.libssl\.a=.*|dependency.libssl.a=0000000000000000000000000000000000000000000000000000000000000000|' \
    "libssl.a hashes"

echo "== a pinned-only archive must not contribute a member =="
# The link map is the contribution evidence, and its hash is declared, so the
# declaration is updated too -- otherwise the strip oracle would fire first and
# the failure would be attributed to the wrong contract.
cp "$LMAP" "$work/lmap.orig" || { echo "cannot save the link map" >&2; exit 2; }
cp "$PROV" "$work/prov.orig2" || { echo "cannot save the provenance" >&2; exit 2; }
{
    cat "$work/lmap.orig"
    printf '                %s(ssl_lib.o):(.text)\n' "$dep/$ABI/lib/libssl.a"
} > "$LMAP" || { echo "cannot mutate the link map" >&2; exit 2; }
_nm=$(decl_sha256 "$LMAP") || { echo "cannot hash the mutated link map" >&2; exit 2; }
sed "s|^map\.sha256=.*|map.sha256=$_nm|" "$work/prov.orig2" > "$PROV" \
    || { echo "cannot re-declare the map hash" >&2; exit 2; }
expect_fail pinned-archive-contributed "must not be linked"
cp "$work/lmap.orig" "$LMAP" || { echo "cannot restore the link map" >&2; exit 2; }
cp "$work/prov.orig2" "$PROV" || { echo "cannot restore the provenance" >&2; exit 2; }
cmp -s "$work/lmap.orig" "$LMAP" || { echo "the link map was not restored byte-identically" >&2; exit 2; }
cmp -s "$work/prov.orig2" "$PROV" || { echo "the provenance was not restored byte-identically" >&2; exit 2; }

echo "== the recorded final-link cwd stays inside the stage's audit-inputs =="
cp "$RCPT" "$work/rcpt.orig" || { echo "cannot save the receipt" >&2; exit 2; }
cp "$PROV" "$work/prov.orig3" || { echo "cannot save the provenance" >&2; exit 2; }
_bc=$(props_count "$PROV" build.root) || { echo "cannot count build.root" >&2; exit 2; }
[ "$_bc" = "1" ] || { echo "the stage does not declare exactly one build.root" >&2; exit 2; }
_broot=$(props_value "$PROV" build.root) || { echo "cannot read the declared build root" >&2; exit 2; }
sed "s|^moq-topology: cwd: .*|moq-topology: cwd: $_broot|" "$work/rcpt.orig" > "$RCPT" \
    || { echo "cannot mutate the receipt" >&2; exit 2; }
_nr=$(decl_sha256 "$RCPT") || { echo "cannot hash the mutated receipt" >&2; exit 2; }
sed "s|^receipt\.sha256=.*|receipt.sha256=$_nr|" "$work/prov.orig3" > "$PROV" \
    || { echo "cannot re-declare the receipt hash" >&2; exit 2; }
expect_fail link-cwd-outside-audit-inputs "not the audited stage's audit-inputs"
cp "$work/rcpt.orig" "$RCPT" || { echo "cannot restore the receipt" >&2; exit 2; }
cp "$work/prov.orig3" "$PROV" || { echo "cannot restore the provenance" >&2; exit 2; }
cmp -s "$work/rcpt.orig" "$RCPT" || { echo "the receipt was not restored byte-identically" >&2; exit 2; }
cmp -s "$work/prov.orig3" "$PROV" || { echo "the provenance was not restored byte-identically" >&2; exit 2; }

echo "== the JNI substrate's picotls headers come from the staged prefix =="
# The compiler's own record of the JNI compile is what the checker judges. Each
# mutant rewrites that record and re-declares its hash, so the picotls oracle --
# not the hash binding -- is the one that must answer; one case leaves the hash
# stale on purpose.
JREC="$COPY/$ABI/jni-compile.json"
_ppc=$(props_count "$PROV" ptls.prefix) || { echo "cannot count ptls.prefix" >&2; exit 2; }
[ "$_ppc" = "1" ] || { echo "the stage does not declare exactly one ptls.prefix" >&2; exit 2; }
_pp=$(props_value "$PROV" ptls.prefix) || { echo "cannot read the staged prefix" >&2; exit 2; }
_srcinc=$(cd "$dep/_src/picotls/include" && pwd -P) || { echo "cannot resolve the pinned picotls headers" >&2; exit 2; }
grep -q "\"-I\", \"$_pp/include\"" "$JREC" \
    || { echo "the JNI compile record does not carry the staged include as expected" >&2; exit 2; }
# jni_mutate <case> <expect: fail|pass> <substring|-> <restamp: yes|no> <sed-expr>
jni_mutate() {
    _cs=$1; _exp=$2; _sub=$3; _rs=$4; _sx=$5
    cp "$JREC" "$work/jrec.orig" || { echo "cannot save the JNI record" >&2; exit 2; }
    cp "$PROV" "$work/prov.orig5" || { echo "cannot save the provenance" >&2; exit 2; }
    sed "$_sx" "$work/jrec.orig" > "$JREC" || { echo "cannot mutate the JNI record" >&2; exit 2; }
    if cmp -s "$work/jrec.orig" "$JREC"; then
        miss_ "$_cs" "the mutation did not change the JNI record at all"
    else
        if [ "$_rs" = yes ]; then
            _nj=$(decl_sha256 "$JREC") || { echo "cannot hash the mutated JNI record" >&2; exit 2; }
            sed "s|^jni\.compile\.sha256=.*|jni.compile.sha256=$_nj|" "$work/prov.orig5" > "$PROV" \
                || { echo "cannot re-declare the JNI record hash" >&2; exit 2; }
        fi
        if [ "$_exp" = pass ]; then
            _o=$(run_checker); _r=$(cat "$work/rc")
            if [ "$_r" -eq 0 ]; then kill_ "$_cs" "a valid record passes: $(printf '%s\n' "$_o" | grep -m1 "PASS\[$ABI\.ptls\]" | cut -c1-100)"
            else miss_ "$_cs" "a valid record was rejected: $(printf '%s\n' "$_o" | grep -m1 '^FAIL' | cut -c1-150)"; fi
        else
            expect_fail "$_cs" "$_sub"
        fi
    fi
    cp "$work/jrec.orig" "$JREC" || { echo "cannot restore the JNI record" >&2; exit 2; }
    cp "$work/prov.orig5" "$PROV" || { echo "cannot restore the provenance" >&2; exit 2; }
    cmp -s "$work/jrec.orig" "$JREC" || { echo "the JNI record was not restored byte-identically" >&2; exit 2; }
    cmp -s "$work/prov.orig5" "$PROV" || { echo "the provenance was not restored byte-identically" >&2; exit 2; }
}
# The staged headers as -isystem written BEFORE a foreign -I: every -I is
# searched before any -isystem, so the foreign tree supplies <picotls.h>.
jni_mutate jni-isystem-before-foreign-I fail "finds picotls.h first in '$_srcinc'" yes \
    "s|\"-I\", \"$_pp/include\"|\"-isystem\", \"$_pp/include\", \"-I\", \"$_srcinc\"|"
# A path with a space that resolves to the staged headers is VALID.
ln -s "$_pp/include" "$work/staged headers" || { echo "cannot create the spaced alias" >&2; exit 2; }
jni_mutate jni-spaced-path pass - yes \
    "s|\"-I\", \"$_pp/include\"|\"-I\", \"$work/staged headers\"|"
# Identity: a coherent record for ANOTHER source whose path also ends in
# jni/moq5_jni.c, and one for the right source naming another object.
_jsrc=$(sed -n 's/.*"file": "\([^"]*\)".*/\1/p' "$JREC")
_jout=$(sed -n 's/.*"output": "\([^"]*\)".*/\1/p' "$JREC")
[ -n "$_jsrc" ] && [ -n "$_jout" ] || { echo "cannot read the JNI record's file and output" >&2; exit 2; }
mkdir -p "$work/elsewhere/jni" && cp "$_jsrc" "$work/elsewhere/jni/moq5_jni.c" \
    || { echo "cannot create the look-alike JNI source" >&2; exit 2; }
jni_mutate jni-record-other-source fail "not the JNI source" yes \
    "s|\"file\": \"$_jsrc\"|\"file\": \"$work/elsewhere/jni/moq5_jni.c\"|"
jni_mutate jni-record-other-output fail "not the object the final link consumed" yes \
    "s|\"output\": \"$_jout\"|\"output\": \"$(dirname "$_jout")/other.o\"|"
jni_mutate jni-record-malformed fail "not a JSON array" yes 's|^{|[|'
jni_mutate jni-record-unbound fail "the JNI compile record hashes" no \
    "s|\"-I\", \"$_pp/include\"|\"-I\", \"$_srcinc\"|"

echo "== a nested stage is named, not silently audited as an empty container =="
# The nested guard fires before any provenance is read, so a copy is adequate
# here and keeps the probe away from the durable stage.
NEST="$work/nested"
mkdir -p "$NEST/stage" || { echo "cannot build the nested probe" >&2; exit 2; }
cp -R "$COPY/$ABI" "$NEST/stage/$ABI" || { echo "cannot populate the nested probe" >&2; exit 2; }
if sh "$CHECKER" --dep-stage "$dep" "$NEST" > "$work/nest.out" 2>&1; then
    miss_ nested-stage "the checker passed a container whose real stage is nested below it"
elif grep -q 'NESTED stage' "$work/nest.out"; then
    kill_ nested-stage "$(grep -m1 'NESTED stage' "$work/nest.out" | cut -c1-150)"
else
    miss_ nested-stage "the checker failed, but not with the nested-stage diagnosis: $(grep -m1 '^FAIL' "$work/nest.out" | cut -c1-150)"
fi

# --- the restored copy must still be green ------------------------------------
_out=$(run_checker); _rc=$(cat "$work/rc")
if [ "$_rc" -ne 0 ]; then
    echo "selftest-product-contract: the stage is NOT green after restoration, so a mutant leaked:" >&2
    printf '%s\n' "$_out" | grep '^FAIL' >&2
    exit 1
fi
echo "restored: the durable stage is green again, so no mutant leaked"

printf '\nproduct-contract self-test: %s kill(s), %s miss(es), %s case(s)\n' "$kills" "$misses" "$cases"
[ "$misses" -eq 0 ] || exit 1
exit 0
