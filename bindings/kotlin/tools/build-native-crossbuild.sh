#!/bin/bash
# MoQ5 Android Phase 0.2 -- the native dependency cross-build recipe.
#
# Produces, per ABI, the eight static archives the picoquic adapter's
# dependency graph needs, plus the provenance, CMake caches and build log that
# tools/check-native-crossbuild.sh consumes. THE CHECKER IS THE SUCCESS GATE:
# this script does not re-implement its archive accounting, closed-link, PIC,
# source-cleanliness, hermeticity or component-identity logic in a weaker
# parallel form -- it builds, publishes, and then submits to it.
#
# Everything generated lives under one ignored, script-owned root (default
# bindings/kotlin/.native). Nothing outside that root is ever deleted.
#
# Interpreter: written for macOS /bin/bash 3.2 as well as current Bash. No
# associative arrays, no mapfile, no ${var,,}.
set -euo pipefail

# These three resolve before die() is defined, so they report and exit inline.
here=$(cd "$(dirname "$0")" && pwd) || { echo "moq-crossbuild: FATAL: cannot resolve the script directory" >&2; exit 1; }
# The ONE portable, fail-closed SHA-256 implementation (sha256sum or shasum).
[ -r "$here/lib-hash.sh" ] || { echo "moq-crossbuild: FATAL: missing $here/lib-hash.sh" >&2; exit 1; }
# shellcheck source=lib-hash.sh
. "$here/lib-hash.sh"
root=$(cd "$here/.." && pwd)         || { echo "moq-crossbuild: FATAL: cannot resolve bindings/kotlin from $here" >&2; exit 1; }
repo=$(cd "$root/../.." && pwd)      || { echo "moq-crossbuild: FATAL: cannot resolve the worktree root from $root" >&2; exit 1; }

die()  { printf 'moq-crossbuild: FATAL: %s\n' "$*" >&2; exit 1; }
say()  { printf 'moq-crossbuild: %s\n' "$*"; }

usage() {
    cat >&2 <<USAGE
usage: $(basename "$0") --android-api <n>

  --android-api  REQUIRED. The Android API level to build against. This is an
                 experimental BUILD INPUT, not a minSdk ruling and not a stored
                 authority: it is recorded in provenance and must be passed
                 explicitly on every run.

There is deliberately no --abi and no --out. Phase 0.2 builds EXACTLY
arm64-v8a and x86_64, and submits both to the success gate: a subset run could
otherwise leave one ABI from an older run in place and pass. The generated root
is fixed at <bindings/kotlin>/.native so cleanup cannot be pointed elsewhere.
USAGE
    exit 2
}

API=""
while [ $# -gt 0 ]; do
    case $1 in
        --android-api) [ $# -ge 2 ] || usage; API=$2; shift 2 ;;
        -h|--help)     usage ;;
        *)             printf 'unknown argument: %s\n' "$1" >&2; usage ;;
    esac
done
[ -n "$API" ] || { printf '%s\n' "--android-api is required" >&2; usage; }
printf '%s' "$API" | grep -Eq '^[1-9][0-9]*$' \
    || die "--android-api '$API' is not an all-decimal positive number"

# The ABI set is FIXED, not selectable (see usage above).
ABIS="arm64-v8a x86_64"

# ---------------------------------------------------------------------------
# 1. Pins and repository locations, from the EXISTING authorities only.
#    Values are never duplicated here, and an environment variable may not
#    silently replace one: the authority's own default is what is read.
# ---------------------------------------------------------------------------
# TOOLS FIRST. The authority readers below run sed/awk/grep, so their
# availability is asserted before anything relies on them -- an absent reader
# must fail by name, not as a mysterious empty count.
MAKE=""
for m in gmake make; do command -v "$m" >/dev/null 2>&1 && { MAKE=$m; break; }; done
[ -n "$MAKE" ] || die "no make found (looked for gmake, make)"
for t in git cmake perl pkg-config sed awk grep tr find dirname; do
    command -v "$t" >/dev/null 2>&1 || die "required host tool '$t' is not on PATH"
done
say "host tools: $MAKE, $(command -v cmake), $(command -v perl), $(command -v pkg-config)"

# SOURCE INVENTORY GATE, wired fail-closed. The inventory check is worth
# nothing if a human has to remember to run it, so it runs here: after the tool
# preflight it depends on, and BEFORE any authority read, source fetch, build
# or stage invalidation. An edit that changes the substitution inventory stops
# the recipe here rather than silently invalidating the reviewed classification.
"$here/check-recipe-inventory.sh" >/dev/null \
    || die "the recipe's command-substitution inventory no longer matches the reviewed classification; run tools/check-recipe-inventory.sh and update chats/libmoq/1148 together with it"

sh_one() {  # sh_one <file> <VAR> <default|literal> -- exactly one declaration
    case $3 in
        default) _pat="s/^$2=\"\${[A-Za-z_]*:-\\(.*\\)}\"\$/\\1/p" ;;
        *)       _pat="s/^$2=\"\\([^\"]*\\)\"\$/\\1/p" ;;
    esac
    # `grep -c` exits 1 when the count is ZERO, so the assignment itself would
    # fail and `set -e` would exit before the "declares 0 time(s)" diagnostic
    # below could ever be printed. awk turns a SUCCESSFUL zero-match read into
    # the value 0 -- it does NOT make the program infallible: awk (or sed) can
    # still fail on an unreadable file or an I/O error, and `-f` proves only
    # that a regular file exists, not that it can be read. That failure is
    # therefore named here and never attributed to an impossible 0 count.
    _n=$(sed -n "$_pat" "$1" | awk 'END { print NR }') \
        || die "cannot read the authority $(basename "$1") (the reader itself failed)"
    [ "$_n" -eq 1 ] || die "$(basename "$1") declares $2 $_n time(s); exactly one is required"
    sed -n "$_pat" "$1" || die "cannot read the value of $2 from $(basename "$1")"
}
props_one() {  # props_one <file> <key> -- exactly one canonical key=value
    _n=$(awk -v k="$2=" 'index($0, k) == 1 { c++ } END { print c + 0 }' "$1") \
        || die "cannot read the authority $(basename "$1") (the reader itself failed)"
    [ "$_n" -eq 1 ] || die "$(basename "$1") declares $2 $_n time(s); exactly one is required"
    sed -n "s/^$2=//p" "$1" || die "cannot read the value of $2 from $(basename "$1")"
}

deps_sh="$repo/scripts/setup_picoquic_deps.sh"
ossl_sh="$repo/scripts/build_ios_openssl.sh"
toolchain="$root/gradle/toolchain.properties"
for f in "$deps_sh" "$ossl_sh" "$toolchain"; do
    [ -f "$f" ] || die "missing pin authority $f"
done

PICOQUIC_REF=$(sh_one "$deps_sh" PICOQUIC_REF default)
PICOQUIC_URL=$(sh_one "$deps_sh" PICOQUIC_REPO default)
PICOTLS_REF=$(sh_one "$deps_sh" PICOTLS_REF default)
PICOTLS_URL=$(sh_one "$deps_sh" PICOTLS_REPO default)
OPENSSL_TAG=$(sh_one "$ossl_sh" OPENSSL_REF default)
OPENSSL_URL=$(sh_one "$ossl_sh" OPENSSL_REPO default)
OPENSSL_REF=$(sh_one "$ossl_sh" OPENSSL_PINNED_COMMIT literal)
# The reader's status is carried at the consumption site: zero declarations, a
# duplicate declaration, an unreadable authority and a reader failure are four
# different faults, and none of them should surface only as an empty value
# further down. The aggregate non-empty check below stays as a second guard.
NDK_PIN=$(props_one "$toolchain" sdk.ndk) \
    || die "$toolchain declares no single sdk.ndk"
for kv in "picoquic=$PICOQUIC_REF" "picotls=$PICOTLS_REF" "openssl=$OPENSSL_REF" "ndk=$NDK_PIN"; do
    [ -n "${kv#*=}" ] || die "could not read the ${kv%%=*} pin from its authority"
done
say "pins: picoquic $PICOQUIC_REF / picotls $PICOTLS_REF / OpenSSL $OPENSSL_TAG ($OPENSSL_REF) / NDK $NDK_PIN"

# ---------------------------------------------------------------------------
# 2. Host tools -- every one required BY NAME. A missing tool is a failure,
#    never a skipped component. These are host executables by definition and
#    are unrelated to target dependency discovery.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 3. SDK/NDK discovery. No host path is checked in: the SDK is discovered from
#    the standard sources and the NDK is bound to the Phase 0.1 pin.
# ---------------------------------------------------------------------------
SDK=""
for cand in "${ANDROID_SDK_ROOT:-}" "${ANDROID_HOME:-}"; do
    [ -n "$cand" ] && [ -d "$cand" ] && { SDK=$cand; break; }
done
if [ -z "$SDK" ] && [ -f "$root/local.properties" ]; then
    _n=$(awk 'index($0, "sdk.dir=") == 1 { c++ } END { print c + 0 }' "$root/local.properties") \
        || die "cannot read local.properties (the reader itself failed)"
    [ "$_n" -le 1 ] || die "local.properties declares sdk.dir $_n times; exactly one is required"
    if [ "$_n" -eq 1 ]; then
        SDK=$(sed -n 's/^sdk.dir=//p' "$root/local.properties" | sed 's|\\\\|/|g') \
            || die "cannot read sdk.dir from local.properties"
    fi
fi
[ -n "$SDK" ] || die "no Android SDK: set ANDROID_SDK_ROOT or ANDROID_HOME, or declare sdk.dir in local.properties"
NDK="$SDK/ndk/$NDK_PIN"
[ -d "$NDK" ] || die "no NDK $NDK_PIN under $SDK/ndk (Phase 0.1 pins that revision)"
[ -f "$NDK/source.properties" ] || die "$NDK has no source.properties, so its revision cannot be verified"
_rev=$(sed -n 's/^Pkg.Revision *= *//p' "$NDK/source.properties") \
    || die "cannot read $NDK/source.properties (the reader itself failed)"
[ "$_rev" = "$NDK_PIN" ] || die "$NDK reports revision '$_rev', the pin is '$NDK_PIN'"
TC="$NDK/build/cmake/android.toolchain.cmake"
[ -f "$TC" ] || die "missing NDK toolchain file $TC"
_host=""
for h in darwin-x86_64 linux-x86_64 darwin-arm64; do
    [ -d "$NDK/toolchains/llvm/prebuilt/$h/bin" ] && { _host=$h; break; }
done
[ -n "$_host" ] || die "no llvm prebuilt toolchain under $NDK/toolchains/llvm/prebuilt"
BIN="$NDK/toolchains/llvm/prebuilt/$_host/bin"
SYSROOT="$NDK/toolchains/llvm/prebuilt/$_host/sysroot"
say "ndk: $NDK (revision $_rev), toolchain $_host, api $API"

# ---------------------------------------------------------------------------
# 4. The generated root. Deletion is bounded to a directory this script owns:
#    it must sit under bindings/kotlin and carry our marker (or be created by
#    us now). Nothing else is ever removed.
# ---------------------------------------------------------------------------
MARKER_NAME=".moq-crossbuild-root"
MARKER_BODY="moq-crossbuild owned generated root v1"

# CANONICAL containment, not a lexical prefix test. `"$root"/*` accepts
# "$root/.native/../../../../tmp/elsewhere", and a symlinked component would
# put the marker check -- and every later deletion -- on the far side of the
# link. The generated root is therefore a FIXED, direct, non-symlink child of
# the canonical bindings/kotlin directory, and both are resolved with pwd -P
# before anything is created or removed.
root_real=$(cd "$root" && pwd -P) || die "cannot resolve $root"
OUT="$root_real/.native"
if [ -L "$OUT" ]; then
    die "$OUT is a symlink; the generated root must be a real directory inside $root_real"
fi
if [ -e "$OUT" ]; then
    [ -d "$OUT" ] || die "$OUT exists and is not a directory"
    out_real=$(cd "$OUT" && pwd -P) || die "cannot resolve $OUT"
    [ "$out_real" = "$root_real/.native" ] \
        || die "$OUT resolves to '$out_real', which is not the expected direct child $root_real/.native"
    # The marker's exact CONTENT is required, not merely a file of that name:
    # an empty or forged file must not authorise deletion.
    [ -L "$OUT/$MARKER_NAME" ] \
        && die "$OUT/$MARKER_NAME is a symlink; refusing to authorise cleanup through it"
    [ -f "$OUT/$MARKER_NAME" ] \
        || die "$OUT exists but carries no $MARKER_NAME; refusing to clean a directory this script did not create"
    # Exact BYTES, and NOTHING is written to do it. Staging the expected form
    # at ${TMPDIR}/moq-crossbuild-marker.$$ was an ordinary redirection to an
    # external, predictable path, and redirection follows an existing symlink --
    # so the containment check itself could write outside the owned root. The
    # comparison is a pipe instead: it preserves the trailing-newline
    # discriminator that a command-substitution comparison loses, and it
    # creates no file.
    printf '%s\n' "$MARKER_BODY" | cmp -s - "$OUT/$MARKER_NAME" \
        || die "$OUT/$MARKER_NAME does not carry the exact marker bytes for this recipe version; refusing to clean it"
else
    mkdir -p "$OUT" || die "cannot create $OUT"
    printf '%s\n' "$MARKER_BODY" > "$OUT/$MARKER_NAME" || die "cannot write $OUT/$MARKER_NAME"
    out_real=$(cd "$OUT" && pwd -P) || die "cannot resolve $OUT"
    [ "$out_real" = "$root_real/.native" ] || die "$OUT resolves outside the expected location"
fi
OUT=$out_real

# A non-symlink ROOT does not make its descendants safe: an existing
# `.native/_src` symlink would make mkdir -p succeed on the far side of the
# link, and a lexical prefix test cannot see that. The guards below resolve the
# nearest EXISTING ancestor with pwd -P and require it to be the canonical root
# or inside it.
#
# WHERE THEY RUN -- the trust transition, stated exactly:
#
#   * the RESERVED generated components -- _src, _build, _prefix, _partial,
#     .probe, both ABI stages, the lock and the marker -- are screened before
#     they are created, removed or written;
#   * each DEPENDENCY CACHE ENTRY is screened before it is probed or handed to
#     git, because both `[ -d "$d/.git" ]` and `git -C "$d"` follow a symlink;
#   * files created BELOW a directory that was validated and freshly created in
#     the same run -- build outputs, staged archives, logs, provenance -- inherit
#     that trust and are not re-validated one by one.
#
# So this is not a claim that every individual rm/mkdir/git/mv/truncation calls
# a guard; it is a claim about which paths establish trust and what inherits it.
# Real-parent containment, shared by every guard below.
owned_parent_ok() {
    _p=$1
    # Nearest existing ancestor, resolved physically.
    _anc=$(dirname "$_p") || die "cannot resolve the parent of $_p"
    while [ ! -e "$_anc" ] && [ "$_anc" != "/" ]; do
        _anc=$(dirname "$_anc") || die "cannot walk up from $_p to a real parent"
    done
    [ -d "$_anc" ] || die "refusing to touch '$_p': its nearest existing parent '$_anc' is not a directory"
    _ancreal=$(cd "$_anc" && pwd -P) || die "cannot resolve the parent of '$_p'"
    case $_ancreal in
        "$OUT") ;;
        "$OUT"/*) ;;
        *) die "refusing to touch '$_p': its real parent resolves to '$_ancreal', outside $OUT (a symlinked component would otherwise escape the owned root)" ;;
    esac
    return 0
}
owned_path_ok() {
    case $1 in
        "$OUT"/*) ;;
        *) die "refusing to touch '$1': outside the script-owned root $OUT" ;;
    esac
    # A symlink at the final component is never followed, whatever it targets.
    [ -L "$1" ] && die "refusing to touch '$1': it is a symlink, and this script does not follow or repair symlinked generated paths"
    owned_parent_ok "$1"
    return 0
}
# B4: a removal that FAILS must be reported, and the target must actually be
# gone before this returns. Callers rely on that, in particular the publish
# step -- a failed removal followed by mv would move the new stage INSIDE the
# stale one and leave the stale stage active.
owned_rm() {
    owned_path_ok "$1"
    [ -e "$1" ] || return 0
    rm -rf "$1" || die "could not remove $1"
    [ -e "$1" ] && die "removal of $1 reported success but it still exists"
    return 0
}
owned_rm_force() {   # like owned_rm, but a symlink AT the final component is
                     # UNLINKED (never followed) instead of being refused
    case $1 in
        "$OUT"/*) ;;
        *) die "refusing to touch '$1': outside the script-owned root $OUT" ;;
    esac
    if [ -L "$1" ]; then
        # A lexical prefix test is not proof of containment: validate the real
        # parent before unlinking, so a symlinked ancestor cannot place this
        # rm outside the owned root.
        owned_parent_ok "$1"
        rm -f "$1" || die "could not remove the symlink $1"
        return 0
    fi
    owned_rm "$1"
}
# Screen a path that may or may not exist, WITHOUT deleting a valid one:
# unlink a final-component symlink, otherwise just validate containment.
owned_screen() {
    case $1 in
        "$OUT"/*) ;;
        *) die "refusing to touch '$1': outside the script-owned root $OUT" ;;
    esac
    if [ -L "$1" ]; then
        owned_parent_ok "$1"
        rm -f "$1" || die "could not remove the symlink $1"
        return 0
    fi
    owned_parent_ok "$1"
    return 0
}
owned_mkdir() {
    owned_path_ok "$1"
    mkdir -p "$1" || die "could not create $1"
    [ -d "$1" ] || die "creation of $1 reported success but it is not a directory"
    [ -L "$1" ] && die "creation of $1 produced a symlink"
    return 0
}

# B7: one exclusive run at a time. mkdir is atomic on every filesystem this
# runs on. The lock is held through the final checker and released by trap on
# normal exit, failure and signals. A stale lock fails CLOSED with a recovery
# message rather than being silently broken.
LOCK="$OUT/.lock"
if ! mkdir "$LOCK" 2>/dev/null; then
    die "another run holds $LOCK (pid $(cat "$LOCK/pid" 2>/dev/null || echo unknown)). If no build is running, remove that directory to recover."
fi
printf '%s\n' "$$" > "$LOCK/pid" || die "cannot record the lock owner"
release_lock() { rm -rf "$LOCK" 2>/dev/null || true; }
trap 'release_lock' EXIT
trap 'release_lock; exit 130' INT TERM HUP

SRC="$OUT/_src"; BUILD="$OUT/_build"; PREFIX="$OUT/_prefix"; PART="$OUT/_partial"

# WHOLE-RUN INVALIDATION, before ANY source or build mutation. Doing it later
# left a window in which a rejected source cache was refetched -- a real
# mutation -- while the previous two-ABI stage was still green, so a fetch
# failure there kept a stale stage published. From here until the final publish
# the checker is RED.
#
# What is INVALIDATED and what is PRESERVED are different sets, and an earlier
# revision conflated them: it ran owned_rm_force over _src as well, which
# deleted every valid source cache on every run and made the reuse branch dead
# code. Source caches are expensive and are validated on their own terms
# (origin, HEAD, cleanliness) inside fetch_src.
#
#   invalidated: both ABI stages, .probe, _partial, _build, _prefix
#   preserved:   _src (screened for a symlink, never deleted merely to check it)
#
# Diagnostics are NOT suppressed here: a failing removal must be visible and
# named, so nothing is redirected to /dev/null.
for abi in arm64-v8a x86_64; do owned_rm_force "$OUT/$abi"; done
owned_rm_force "$OUT/.probe"
owned_rm_force "$PART"
owned_rm_force "$BUILD"
owned_rm_force "$PREFIX"
owned_screen "$SRC"          # unlink a symlinked _src; keep a real one
owned_mkdir "$SRC"

# ---------------------------------------------------------------------------
# 5. Source caches at the exact pinned commits. A cache is REUSED only when its
#    origin, HEAD and cleanliness all match; any mismatch is replaced
#    deliberately and reported, never normalised in place.
# ---------------------------------------------------------------------------
fetch_src() {  # fetch_src <name> <url> <ref> <verify-commit>
    _name=$1; _url=$2; _ref=$3; _want=$4
    _d="$SRC/$_name"
    # SCREEN THE ENTRY FIRST. `[ -d "$_d/.git" ]` and `git -C "$_d"` both
    # follow a symlinked cache entry, so a symlinked _src/<dep> would have been
    # probed -- and, on the reuse path, trusted -- before anything removed it.
    # owned_screen unlinks a final-component symlink after validating its real
    # parent, and never follows it.
    owned_screen "$_d"
    if [ -d "$_d/.git" ] || git -C "$_d" rev-parse --git-dir >/dev/null 2>&1; then
        # CLASSIFIERS. Each of the four probes must (a) never terminate the
        # script and (b) distinguish "probed successfully, result empty" from
        # "the probe itself failed".
        #
        # Under `set -euo pipefail` a failing command substitution in an
        # assignment exits the shell, so an unguarded probe kills the run with
        # no diagnostic -- after both stages have already been invalidated.
        # Two reachable shapes do that: an interrupted fetch leaves a
        # repository with an unborn HEAD, and a corrupt .git/index leaves
        # rev-parse/remote working while status and clean fail with 128.
        #
        # Appending `|| true` is NOT a fix for all four. Empty is a sufficient
        # mismatch only for origin and HEAD, whose expected values are
        # NONEMPTY, so an empty result can never look like a match. For status
        # and clean-residue, empty is exactly what a CLEAN tree looks like, so
        # collapsing a failed probe to empty would make a corrupt cache with
        # matching origin and HEAD appear reusable. Those two therefore carry
        # an explicit successful-probe bit before empty may mean clean.
        #
        # BOUNDED, DRAINING, STATUS-PRESERVING. `head -1` cannot be used: it
        # exits early, so on a multi-line dirty tree the producer dies of
        # SIGPIPE and pipefail reports a failure indistinguishable from a real
        # probe failure. Capturing the COMPLETE output instead and slicing it
        # afterwards is not bounded either -- shell memory would scale with the
        # whole untracked/ignored inventory, which an earlier revision claimed
        # was bounded when only the printed diagnostic was.
        #
        # `sed -n '1p'` (no `q`) DRAINS all input, so the producer is never
        # signalled, retains exactly ONE line, and leaves pipefail free to
        # surface git's own non-zero status.
        _bad=""
        _o=$(git -C "$_d" remote get-url origin 2>/dev/null) || _bad="the origin probe failed"
        _h=$(git -C "$_d" rev-parse HEAD 2>/dev/null)        || _bad="${_bad:-the HEAD probe failed}"
        _dirty=""; _res=""
        if [ -z "$_bad" ]; then
            if ! _dirty=$(git -C "$_d" status --porcelain --untracked-files=all 2>/dev/null | sed -n '1p'); then
                _bad="the status probe failed (the repository is unreadable, so 'clean' cannot be observed)"
            fi
        fi
        if [ -z "$_bad" ]; then
            if ! _res=$(git -C "$_d" clean -ndx 2>/dev/null | sed -n '1p'); then
                _bad="the ignored/untracked residue probe failed (the repository is unreadable, so 'clean' cannot be observed)"
            fi
        fi
        if [ -z "$_bad" ] && [ "$_o" = "$_url" ] && [ "$_h" = "$_want" ] \
           && [ -z "$_dirty" ] && [ -z "$_res" ]; then
            say "$_name: reusing cached source at $_want (origin, HEAD and clean state all match)"
            return 0
        fi
        if [ -n "$_bad" ]; then
            say "$_name: cache rejected -- $_bad; refetching"
        else
            say "$_name: cache rejected (origin='$_o' head='$_h' dirty='$(printf '%.60s' "${_dirty:-none}")' residue='$(printf '%.60s' "${_res:-none}")'); refetching"
        fi
        owned_rm_force "$_d"
    fi
    # A pre-existing NON-repository directory (or a symlink) at the cache path
    # is removed through the guarded path rather than being initialised in
    # place: `git init` into whatever happens to sit there would mutate an
    # arbitrary directory, and a symlinked one would mutate its target.
    if [ -e "$_d" ] || [ -L "$_d" ]; then
        owned_rm_force "$_d"
    fi
    say "$_name: fetching $_ref from $_url"
    owned_mkdir "$_d"
    git -C "$_d" init -q                                   || die "$_name: git init failed"
    git -C "$_d" remote add origin "$_url"                 || die "$_name: git remote add failed"
    git -C "$_d" fetch -q --depth 1 origin "$_ref"         || die "$_name: fetching $_ref failed (network or wrong ref)"
    git -C "$_d" checkout -q FETCH_HEAD                    || die "$_name: checkout failed"
    # Submodules are part of the pinned tree: picotls builds its test targets
    # from deps/picotest, and CMake's generate step fails without it. The
    # commits come from the parent tree, so this introduces no new pin.
    git -C "$_d" submodule update --init --recursive --depth 1 -q >/dev/null 2>&1 \
        || git -C "$_d" submodule update --init --recursive -q \
        || die "$_name: submodule checkout failed"
    # VERIFICATION of what was just fetched -- not classification of a cache.
    # A failure here is fatal and named: the fetch completed but its result
    # cannot be verified, which must never be normalised into "reject and try
    # again". The same drained, bounded pipeline is used so a multi-line dirty
    # tree cannot be mistaken for a probe failure.
    _h=$(git -C "$_d" rev-parse HEAD) \
        || die "$_name: freshly fetched tree does not resolve HEAD, so the pinned commit cannot be verified"
    [ "$_h" = "$_want" ] || die "$_name: fetched $_ref resolves to $_h, the authority pins $_want"
    _dirty=$(git -C "$_d" status --porcelain --untracked-files=all | sed -n '1p') \
        || die "$_name: freshly fetched tree cannot be inspected for cleanliness"
    [ -z "$_dirty" ] || die "$_name: freshly fetched tree is not clean"
    say "$_name: at $_h, clean"
}
fetch_src picoquic "$PICOQUIC_URL" "$PICOQUIC_REF" "$PICOQUIC_REF"
fetch_src picotls  "$PICOTLS_URL"  "$PICOTLS_REF"  "$PICOTLS_REF"
fetch_src openssl  "$OPENSSL_URL"  "$OPENSSL_TAG"  "$OPENSSL_REF"

# ---------------------------------------------------------------------------
# 5b. The PicoQUIC and picotls sources actually compiled: each pinned tree plus
#     the repository's tracked patches for it, applied in order, in a RUN-OWNED
#     copy under _build. The caches in _src are never patched, so their
#     origin/HEAD/clean authority above still holds on every reuse; the copies
#     are invalidated with _build on every run. Each ordered list is the one
#     dependency setup applies (PICOQUIC_PATCHES / PICOTLS_PATCHES in
#     scripts/setup_picoquic_deps.sh), and it must be exactly the patches
#     tracked under patches/<dependency>; each patch is read in place -- there
#     is no second copy of its bytes. A missing patch, a list that disagrees
#     with the tracked set, or a patch that does not apply cleanly stops the run
#     here, before anything is compiled; both ABI stages were already
#     invalidated above, so nothing stale stays green.
# ---------------------------------------------------------------------------
PATCH_LIST_SH="$repo/scripts/setup_picoquic_deps.sh"
[ -f "$PATCH_LIST_SH" ] || die "missing the patch-list authority $PATCH_LIST_SH"
# `git apply` run from a subdirectory of a repository resolves paths from that
# repository's top level and silently SKIPS the ones outside the current
# directory -- every step below would then succeed while changing nothing. The
# copies live inside this repository's worktree, so discovery is stopped at
# _build and each call treats the copy itself as the patch root.
src_apply() { _sa_src=$1; _sa_patch=$2; shift 2; ( cd "$_sa_src" && GIT_CEILING_DIRECTORIES="$BUILD" git apply "$@" "$_sa_patch" ); }
# stage_patched_src <dependency> <LIST> <label>: $BUILD/<dependency>-src becomes
# the pinned $SRC/<dependency> tree (with its submodules) plus the <LIST>
# patches, and $BUILD/<dependency>-patches.properties the ordered provenance
# block (<dependency>.patch.count, then index -> path and index -> sha256).
stage_patched_src() {
    _sp_dep=$1; _sp_list=$2; _sp_label=$3
    _sp_dir="patches/$_sp_dep"
    _sp_src="$BUILD/$_sp_dep-src"
    owned_mkdir "$_sp_src"
    # The declared list: the entries of the <LIST> array, one per line ...
    sed -n "/^$_sp_list=(/,/^)/p" "$PATCH_LIST_SH" | sed '1d;$d' \
        | sed 's/^[[:space:]]*//; s/[[:space:]]*$//' | grep -v '^$' > "$BUILD/$_sp_dep-patches.list" \
        || die "cannot read the $_sp_list list from $PATCH_LIST_SH"
    # ... and it must be exactly the tracked patches, in the same order.
    ( cd "$repo/$_sp_dir" && ls -1 -- *.patch ) > "$BUILD/$_sp_dep-patches.tracked" \
        || die "cannot list the tracked $_sp_label patches in $_sp_dir"
    cmp -s "$BUILD/$_sp_dep-patches.list" "$BUILD/$_sp_dep-patches.tracked" \
        || die "the declared $_sp_label patch list in $PATCH_LIST_SH is not exactly the patches tracked in $_sp_dir"
    git -C "$SRC/$_sp_dep" archive --format=tar HEAD | tar -x -C "$_sp_src" \
        || die "cannot export the pinned $_sp_label tree into $_sp_src"
    # Submodule content is part of the pinned tree (picotls builds from
    # deps/picotest, among others). One level is supported; a nested submodule
    # stops the run rather than being silently left out.
    : > "$BUILD/$_sp_dep-submodules.list" || die "cannot start the $_sp_label submodule list"
    if [ -f "$SRC/$_sp_dep/.gitmodules" ]; then
        # `git config --get-regexp` exits 1 when nothing matches (an empty
        # .gitmodules); any other failure is an error.
        if git -C "$SRC/$_sp_dep" config -f .gitmodules --get-regexp '^submodule\..*\.path$' \
               > "$BUILD/$_sp_dep-submodules.raw"; then
            :
        else
            [ $? -eq 1 ] || die "cannot list the $_sp_label submodules"
        fi
        awk '{ print $2 }' "$BUILD/$_sp_dep-submodules.raw" > "$BUILD/$_sp_dep-submodules.list" \
            || die "cannot list the $_sp_label submodules"
    fi
    while IFS= read -r _sp_sub; do
        [ -n "$_sp_sub" ] || continue
        [ ! -f "$SRC/$_sp_dep/$_sp_sub/.gitmodules" ] \
            || die "$_sp_label submodule $_sp_sub has submodules of its own; nested export is not supported"
        mkdir -p "$_sp_src/$_sp_sub" || die "cannot create $_sp_src/$_sp_sub"
        git -C "$SRC/$_sp_dep/$_sp_sub" archive --format=tar HEAD | tar -x -C "$_sp_src/$_sp_sub" \
            || die "cannot export the $_sp_label submodule $_sp_sub into $_sp_src"
    done < "$BUILD/$_sp_dep-submodules.list"
    # Each patch in order: its identity through the shared fail-closed hasher
    # into a run-owned file and read back (no command substitution, so the
    # recipe's reviewed substitution inventory is unchanged), then applied, then
    # proved present. The (path, sha256) pairs are kept in order.
    : > "$BUILD/$_sp_dep-patches.tsv" || die "cannot start the $_sp_label patch record"
    while IFS= read -r _sp_name; do
        _sp_rel="$_sp_dir/$_sp_name"
        _sp_patch="$repo/$_sp_rel"
        [ -f "$_sp_patch" ] || die "missing the tracked $_sp_label patch $_sp_rel; the pinned source is not built without it"
        # The helper prints the digest with no newline; the line is terminated
        # here so `read` sees a complete record.
        { moq_hash_file "$_sp_patch" "$BUILD" && printf '\n'; } > "$BUILD/$_sp_dep-patch.sha256" \
            || die "cannot hash the tracked $_sp_label patch $_sp_rel"
        _sp_sha=""
        read -r _sp_sha < "$BUILD/$_sp_dep-patch.sha256" \
            || die "cannot read the tracked $_sp_label patch hash for $_sp_rel"
        moq_is_sha256 "$_sp_sha" || die "the tracked $_sp_label patch hash for $_sp_rel is not a SHA-256: '$_sp_sha'"
        src_apply "$_sp_src" "$_sp_patch" --check \
            || die "the tracked $_sp_label patch $_sp_rel does not apply cleanly at its place in the list"
        src_apply "$_sp_src" "$_sp_patch" \
            || die "applying the tracked $_sp_label patch $_sp_rel failed"
        # Proof of EFFECT, not just of a zero exit: once applied, the patch no
        # longer applies forward and does apply in reverse.
        src_apply "$_sp_src" "$_sp_patch" --reverse --check \
            || die "the tracked $_sp_label patch $_sp_rel is not fully present after applying it"
        if src_apply "$_sp_src" "$_sp_patch" --check >/dev/null 2>&1; then
            die "the tracked $_sp_label patch $_sp_rel still applies forward after applying it: nothing was changed"
        fi
        printf '%s\t%s\n' "$_sp_rel" "$_sp_sha" >> "$BUILD/$_sp_dep-patches.tsv" \
            || die "cannot record the $_sp_label patch $_sp_rel"
        say "$_sp_dep: applied $_sp_rel (sha256 $_sp_sha)"
    done < "$BUILD/$_sp_dep-patches.list"
    awk -F '\t' -v dep="$_sp_dep" '
        { rel[NR - 1] = $1; sha[NR - 1] = $2 }
        END {
            if (NR == 0) exit 1
            printf "%s.patch.count=%d\n", dep, NR
            for (i = 0; i < NR; i++) {
                printf "%s.patch.%d=%s\n", dep, i, rel[i]
                printf "%s.patch.%d.sha256=%s\n", dep, i, sha[i]
            }
        }' "$BUILD/$_sp_dep-patches.tsv" > "$BUILD/$_sp_dep-patches.properties" \
        || die "cannot write the ordered $_sp_label patch provenance (is the patch list empty?)"
    say "$_sp_dep: compiling the pinned tree plus the ordered patch list from $_sp_src"
}
stage_patched_src picoquic PICOQUIC_PATCHES PicoQUIC
stage_patched_src picotls PICOTLS_PATCHES picotls
PQ_BUILD_SRC="$BUILD/picoquic-src"
PT_BUILD_SRC="$BUILD/picotls-src"
# Every consumer below compiles against the PINNED picotls headers
# ($SRC/picotls/include), which the accepted JNI topology contract also stages.
# A picotls patch that changed a header would make that untrue, so it stops the
# run instead.
if grep -q '^diff --git a/include/' "$repo"/patches/picotls/*.patch; then
    die "a tracked picotls patch changes include/; the consumers compile against the pinned headers, so that needs its own contract"
fi

# ---------------------------------------------------------------------------
# 6. Per-ABI build. Every component is built OUT OF SOURCE, from a clean build
#    tree and prefix, into a PARTIAL stage; the ABI is published only after all
#    of its components and its provenance are complete, so a failed run cannot
#    leave a stage a later checker invocation mistakes for a finished one.
# ---------------------------------------------------------------------------
abi_triple()  { case $1 in arm64-v8a) echo aarch64-linux-android;; x86_64) echo x86_64-linux-android;; esac; }
abi_machine() { case $1 in arm64-v8a) echo elf64-littleaarch64;;  x86_64) echo elf64-x86-64;; esac; }
abi_ossl()    { case $1 in arm64-v8a) echo android-arm64;;        x86_64) echo android-x86_64;; esac; }

# CONTROLLED TARGET ENVIRONMENT. pkg-config was never the only channel: the NDK
# compiler consumes CPATH directly and the resulting -I never appears in the
# logged command, so the build log cannot show it. Every inherited variable the
# pinned build systems consume for target selection, include/library search,
# flags or tool choice is therefore UNSET here. Only named host mechanics are
# preserved (PATH, HOME, TMPDIR, SHELL, LANG/LC_*, TERM, plus git/network state
# such as SSL_CERT_FILE and the *_proxy variables used to FETCH sources).
unset CPATH C_INCLUDE_PATH CPLUS_INCLUDE_PATH OBJC_INCLUDE_PATH \
      LIBRARY_PATH LD_LIBRARY_PATH DYLD_LIBRARY_PATH DYLD_FALLBACK_LIBRARY_PATH \
      COMPILER_PATH \
      CFLAGS CPPFLAGS CXXFLAGS LDFLAGS ASFLAGS ARFLAGS LDLIBS \
      CC CXX CPP AR RANLIB NM STRIP OBJCOPY OBJDUMP LD AS \
      CROSS_COMPILE SDKROOT MACOSX_DEPLOYMENT_TARGET \
      CMAKE_PREFIX_PATH CMAKE_INCLUDE_PATH CMAKE_LIBRARY_PATH \
      CMAKE_FRAMEWORK_PATH CMAKE_GENERATOR CMAKE_BUILD_TYPE CMAKE_TOOLCHAIN_FILE \
      MAKEFLAGS MFLAGS GNUMAKEFLAGS MAKELEVEL \
      OPENSSL_CONF OPENSSL_MODULES PERL5LIB PERL5OPT \
      CMAKE_C_COMPILER_LAUNCHER CMAKE_CXX_COMPILER_LAUNCHER \
      CMAKE_OBJC_COMPILER_LAUNCHER CMAKE_OBJCXX_COMPILER_LAUNCHER \
      CMAKE_GENERATOR_PLATFORM CMAKE_GENERATOR_TOOLSET CMAKE_GENERATOR_INSTANCE \
      PKG_CONFIG_SYSTEM_INCLUDE_PATH PKG_CONFIG_SYSTEM_LIBRARY_PATH \
      PKG_CONFIG_ALLOW_SYSTEM_CFLAGS PKG_CONFIG_ALLOW_SYSTEM_LIBS \
      PKG_CONFIG_TOP_BUILD_DIR PKG_CONFIG_DISABLE_UNINSTALLED \
      2>/dev/null || true

# CMake initialises PKG_CONFIG_EXECUTABLE and PKG_CONFIG_ARGN from $PKG_CONFIG,
# so an inherited value chooses the program that answers target dependency
# queries. Bind it to the host pkg-config already resolved above, so the
# executable is the one this run verified rather than whatever was inherited.
# Infallible here: the required-tool loop above already failed the run if
# pkg-config were absent, so this lookup cannot fail.
PKG_CONFIG=$(command -v pkg-config)
export PKG_CONFIG

# The one target-discovery channel that is SET rather than unset: empty means
# "search nothing", where unset would mean "search the host". This is how a
# host Brotli once got compiled into an Android picotls archive.
export PKG_CONFIG_LIBDIR="" PKG_CONFIG_PATH="" PKG_CONFIG_SYSROOT_DIR="$SYSROOT"
export ANDROID_NDK_ROOT="$NDK" ANDROID_NDK_HOME="$NDK"
PATH="$BIN:$PATH"; export PATH

# B2: WHOLE-RUN publication. Both published stages, the probe output and any
# stale unpublished storage were invalidated above, BEFORE the sources were
# touched, so the checker has been RED since then. Each ABI is built into
# unpublished storage and neither is exposed until BOTH are complete.

for abi in $ABIS; do
    triple=$(abi_triple "$abi"); machine=$(abi_machine "$abi"); osslt=$(abi_ossl "$abi")
    CC_WRAP="$BIN/${triple}${API}-clang"
    [ -x "$CC_WRAP" ] || die "$abi: no compiler wrapper $CC_WRAP for API $API in the pinned NDK"

    S="$PART/$abi"; B="$BUILD/$abi"; P="$PREFIX/$abi"
    owned_rm "$S"; owned_rm "$B"; owned_rm "$P"
    owned_mkdir "$S/lib"; owned_mkdir "$S/cmake"; owned_mkdir "$B"
    LOG="$S/build.log"; : > "$LOG"

    # -- OpenSSL -----------------------------------------------------------
    say "[$abi] openssl"
    echo "moq-crossbuild: openssl: begin" >> "$LOG"
    ob="$B/openssl"; owned_mkdir "$ob"
    # -D__ANDROID_API__ is the mechanism OpenSSL's own android config parses to
    # SELECT the ${triple}${api}-clang wrapper (Configurations/15-android.conf).
    # Without it OpenSSL picks the NDK maximum while provenance declares this
    # API. Setting CC in the environment does NOT work: the config overwrites it.
    ( cd "$ob" && "$SRC/openssl/Configure" "$osslt" \
        no-shared no-tests no-docs no-apps -D__ANDROID_API__="$API" -Werror \
        --prefix="$P" >>"$LOG" 2>&1 ) || die "$abi: OpenSSL Configure failed (see $LOG)"
    # The NDK wrapper already defines __ANDROID_API__ SYMBOLICALLY as
    # __ANDROID_MIN_SDK_VERSION__, so OpenSSL's own numeric copy is a genuine
    # -Wmacro-redefined diagnostic. Remove ONLY that exact redundant generated
    # token, under a cardinality guard -- the wrapper stays the API authority.
    # EXACT ARGUMENT TOKENS, not a substring replacement. Against this pinned
    # source the token appears on exactly FOUR lines, and only one of them is a
    # live compile flag:
    #
    #   OPTIONS=          record of the Configure command line
    #   CONFIGURE_ARGS=   record, quoted ("-D__ANDROID_API__=21")
    #   CPPFLAGS=         THE LIVE FLAG -- reaches DSO/BIN/LIB_CPPFLAGS and the
    #                     compile rules; this is the one that is removed
    #   CPPFLAGS_Q=       the quoted reporting copy, a string value
    #
    # An unbounded s|-D__ANDROID_API__=21||g would also rewrite OpenSSL's own
    # records, and would corrupt an unrelated token such as
    # -D__ANDROID_API__=210, which merely CONTAINS the current one.
    _mk="$ob/Makefile"
    # EXACT per-variable inventory. Counting LINES that CONTAIN the substring
    # is not the claimed four-record preflight: a same-line duplicate does not
    # change the line count, and a containing token such as ...=210 satisfies
    # it. Each of the four measured variables is therefore required exactly
    # once, and each must carry exactly ONE standalone argument equal to the
    # declared token. CONFIGURE_ARGS records it quoted, so its field is the
    # quoted form.
    for _rec in OPTIONS CPPFLAGS CPPFLAGS_Q CONFIGURE_ARGS; do
        # NOT `grep -c ... || true`: under pipefail that absorbs EVERY producer
        # failure, not just the no-match status, so a failed inspection reads as
        # a valid count -- and an empty result reaches `[ -eq ]` and emits a raw
        # "integer expression expected" diagnostic before the named failure. awk
        # turns a SUCCESSFUL zero match into 0 and leaves a genuine read failure
        # non-zero, which is guarded by name here.
        _n=$(awk -v r="$_rec=" 'index($0, r) == 1 { c++ } END { print c + 0 }' "$_mk") \
            || die "$abi: cannot inspect the generated Makefile for the $_rec record"
        [ "$_n" -eq 1 ] || die "$abi: the generated Makefile declares $_rec $_n time(s); exactly one is required (the pinned source generates a different shape than this recipe was derived against)"
        case $_rec in
            CONFIGURE_ARGS) _want="\"-D__ANDROID_API__=$API\"," ;;
            *)              _want="-D__ANDROID_API__=$API" ;;
        esac
        _f=$(awk -v r="$_rec=" -v w="$_want" '
                index($0, r) == 1 {
                    v = substr($0, length(r) + 1)
                    m = split(v, a, /[ \t]+/)
                    for (i = 1; i <= m; i++) if (a[i] == w) c++
                }
                END { print c + 0 }' "$_mk") \
            || die "$abi: cannot inspect the $_rec record for its $_want arguments"
        [ "$_f" -eq 1 ] || die "$abi: $_rec carries $_f standalone '$_want' argument(s); exactly one is required before the edit"
    done
    # Rewrite ONLY the CPPFLAGS value, field by field, and report how many exact
    # fields were removed. Any -D__ANDROID_API__ field that is NOT the declared
    # API is left in place so the assertion below stops the build.
    awk -v want="-D__ANDROID_API__=$API" '
        BEGIN { removed = 0 }
        /^CPPFLAGS=/ {
            val = substr($0, 10)
            n = split(val, f, /[ \t]+/)
            out = ""
            for (i = 1; i <= n; i++) {
                if (f[i] == want) { removed++; continue }
                if (f[i] == "") continue
                out = (out == "" ? f[i] : out " " f[i])
            }
            print "CPPFLAGS=" out
            next
        }
        { print }
        END { print removed > "/dev/stderr" }
    ' "$_mk" > "$_mk.moqnew" 2>"$_mk.moqcount" || die "$abi: could not rewrite the generated Makefile"
    # A file a prior command produced is a DATA precondition, not proof that
    # this read cannot fail.
    _removed=$(tr -d ' \n' < "$_mk.moqcount") \
        || die "$abi: cannot read the recorded removal count"
    [ "$_removed" -eq 1 ] || die "$abi: removed $_removed exact '-D__ANDROID_API__=$API' field(s) from CPPFLAGS; exactly one is required"
    mv "$_mk.moqnew" "$_mk" || die "$abi: could not install the rewritten Makefile"
    rm -f "$_mk.moqcount"
    # No API-definition field of ANY value may remain in the live flag variable.
    # Zero is the ACCEPTED value here, so a fail-open is worst-case: a failed
    # inspection would read as "no definitions remain". index($0,"CPPFLAGS=")==1
    # does not match CPPFLAGS_Q=, whose ninth character is '_' not '='.
    _left=$(awk '
                index($0, "CPPFLAGS=") == 1 {
                    v = substr($0, 10)
                    m = split(v, a, /[ \t]+/)
                    for (i = 1; i <= m; i++) if (a[i] ~ /^-D__ANDROID_API__=/) c++
                }
                END { print c + 0 }' "$_mk") \
        || die "$abi: cannot inspect CPPFLAGS after the edit, so the no-API-definition-remains claim is unproven"
    [ "$_left" -eq 0 ] || die "$abi: $_left __ANDROID_API__ definition(s) remain in CPPFLAGS after the edit"
    # ARFLAGS=rsc: llvm-ar announces 'creating <archive>' when invoked without
    # the c modifier, which is a real diagnostic in the log.
    ( cd "$ob" && $MAKE ARFLAGS="rsc" -j8 build_libs >>"$LOG" 2>&1 ) \
        || die "$abi: OpenSSL build_libs failed (see $LOG)"
    ( cd "$ob" && $MAKE ARFLAGS="rsc" install_dev >>"$LOG" 2>&1 ) \
        || die "$abi: OpenSSL install_dev failed (see $LOG)"
    OSSL_LIB="$P/lib"; [ -d "$P/lib64" ] && OSSL_LIB="$P/lib64"
    for a in libcrypto.a libssl.a; do
        [ -f "$OSSL_LIB/$a" ] || die "$abi: OpenSSL produced no $a"
        cp "$OSSL_LIB/$a" "$S/lib/" || die "$abi: cannot stage $a"
    done
    [ -f "$P/include/openssl/opensslv.h" ] || die "$abi: OpenSSL's generated public headers were not installed to $P"
    echo "moq-crossbuild: openssl: done" >> "$LOG"

    # -- picotls -----------------------------------------------------------
    say "[$abi] picotls"
    echo "moq-crossbuild: picotls: begin" >> "$LOG"
    pb="$B/picotls"
    # Explicit Release for both components (ruling 1134/1135). Release defines
    # NDEBUG, which makes eight assert-only variables in the PINNED picotls
    # unused -- picotls.c:2192 and openssl.c:1259..1555. picotls therefore
    # keeps its assertions: -UNDEBUG is appended AFTER the NDK's own -DNDEBUG
    # in the Release flags, so the asserted variables stay used. That is a
    # dependency-local assertion policy forced by the pinned code, not a
    # warning suppression: -Werror stays on and no warning is filtered. The
    # next pin/update tranche should revisit it. picoquic keeps ordinary
    # Release semantics and takes no override.
    cmake -S "$PT_BUILD_SRC" -B "$pb" \
        -DCMAKE_TOOLCHAIN_FILE="$TC" -DANDROID_ABI="$abi" -DANDROID_PLATFORM="android-$API" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_C_FLAGS_RELEASE="-O3 -DNDEBUG -UNDEBUG" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DBUILD_SHARED_LIBS=OFF \
        -DCMAKE_C_FLAGS="-Werror" \
        -DOPENSSL_ROOT_DIR="$P" -DOPENSSL_USE_STATIC_LIBS=ON \
        -DOPENSSL_INCLUDE_DIR="$P/include" \
        -DOPENSSL_CRYPTO_LIBRARY="$OSSL_LIB/libcrypto.a" \
        -DOPENSSL_SSL_LIBRARY="$OSSL_LIB/libssl.a" \
        -DWITH_FUSION=OFF >>"$LOG" 2>&1 || die "$abi: picotls configure failed (see $LOG)"
    cmake --build "$pb" --target picotls-core picotls-openssl picotls-minicrypto -j8 >>"$LOG" 2>&1 \
        || die "$abi: picotls build failed (see $LOG)"
    for a in libpicotls-core.a libpicotls-openssl.a libpicotls-minicrypto.a; do
        [ -f "$pb/$a" ] || die "$abi: picotls produced no $a"
        cp "$pb/$a" "$S/lib/" || die "$abi: cannot stage $a"
    done
    cp "$pb/CMakeCache.txt" "$S/cmake/picotls.cache" || die "$abi: cannot stage the picotls cache"
    echo "moq-crossbuild: picotls: done" >> "$LOG"

    # -- picoquic ----------------------------------------------------------
    say "[$abi] picoquic"
    echo "moq-crossbuild: picoquic: begin" >> "$LOG"
    qb="$B/picoquic"
    cmake -S "$PQ_BUILD_SRC" -B "$qb" \
        -DCMAKE_TOOLCHAIN_FILE="$TC" -DANDROID_ABI="$abi" -DANDROID_PLATFORM="android-$API" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DBUILD_SHARED_LIBS=OFF \
        -DCMAKE_C_FLAGS="-Werror" \
        -DPTLS_INCLUDE_DIR="$SRC/picotls/include" \
        -DPTLS_CORE_LIBRARY="$pb/libpicotls-core.a" \
        -DPTLS_OPENSSL_LIBRARY="$pb/libpicotls-openssl.a" \
        -DPTLS_MINICRYPTO_LIBRARY="$pb/libpicotls-minicrypto.a" \
        -DOPENSSL_ROOT_DIR="$P" -DOPENSSL_USE_STATIC_LIBS=ON \
        -DOPENSSL_INCLUDE_DIR="$P/include" \
        -DOPENSSL_CRYPTO_LIBRARY="$OSSL_LIB/libcrypto.a" \
        -DOPENSSL_SSL_LIBRARY="$OSSL_LIB/libssl.a" \
        -DBUILD_TESTING=OFF >>"$LOG" 2>&1 || die "$abi: picoquic configure failed (see $LOG)"
    cmake --build "$qb" --target picoquic-core picoquic-log picohttp-core -j8 >>"$LOG" 2>&1 \
        || die "$abi: picoquic build failed (see $LOG)"
    for a in libpicoquic-core.a libpicoquic-log.a libpicohttp-core.a; do
        [ -f "$qb/$a" ] || die "$abi: picoquic produced no $a"
        cp "$qb/$a" "$S/lib/" || die "$abi: cannot stage $a"
    done
    cp "$qb/CMakeCache.txt" "$S/cmake/picoquic.cache" || die "$abi: cannot stage the picoquic cache"
    echo "moq-crossbuild: picoquic: done" >> "$LOG"

    # -- provenance --------------------------------------------------------
    # Resolved and checked BEFORE the heredoc: a failing substitution inside a
    # heredoc does not fail `cat`, so an unverifiable commit would otherwise be
    # written as an EMPTY provenance value.
    _pq_c=$(git -C "$SRC/picoquic" rev-parse HEAD) || die "$abi: cannot resolve the staged picoquic commit for provenance"
    _pt_c=$(git -C "$SRC/picotls"  rev-parse HEAD) || die "$abi: cannot resolve the staged picotls commit for provenance"
    _os_c=$(git -C "$SRC/openssl"  rev-parse HEAD) || die "$abi: cannot resolve the staged openssl commit for provenance"
    for _c in "$_pq_c" "$_pt_c" "$_os_c"; do
        [ -n "$_c" ] || die "$abi: a staged commit resolved to an empty value; refusing to write provenance"
    done
    cat > "$S/provenance.properties" <<PROV
abi=$abi
ndk.version=$NDK_PIN
ndk.cc=$CC_WRAP
ndk.target=$triple
android.api=$API
picoquic.commit=$_pq_c
picoquic.src=$SRC/picoquic
picoquic.build.src=$PQ_BUILD_SRC
picotls.commit=$_pt_c
picotls.src=$SRC/picotls
picotls.build.src=$PT_BUILD_SRC
openssl.tag=$OPENSSL_TAG
openssl.commit=$_os_c
openssl.src=$SRC/openssl
openssl.prefix=$P
elf.machine=$machine
PROV
    # The ordered PicoQUIC and picotls patch lists (count, then index -> path
    # and sha256), written once in step 5b and identical for both ABIs.
    cat "$BUILD/picoquic-patches.properties" "$BUILD/picotls-patches.properties" >> "$S/provenance.properties" \
        || die "$abi: cannot record the ordered patch lists in provenance"

    # Explicit guarded assignment: inside `say` a failing producer could not
    # fail the outer command, and `grep -c` returns 1 for a zero count, so an
    # inventory failure would have been printed as a plausible number. awk
    # makes a successful empty inventory the value 0; pipefail keeps a `find`
    # failure distinct from it. The checker remains the sole cardinality and
    # content gate -- this is only an inventory probe.
    _nbuilt=$(find "$S/lib" -name '*.a' -type f | awk 'END { print NR }') \
        || die "$abi: cannot inventory the built archives under $S/lib"
    say "[$abi] built $_nbuilt archives (unpublished)"
done

# Both ABIs are complete: publish them now, and only now.
for abi in $ABIS; do
    [ -f "$PART/$abi/provenance.properties" ] || die "$abi: no provenance in the unpublished stage; refusing to publish an incomplete run"
    owned_rm "$OUT/$abi"
    mv "$PART/$abi" "$OUT/$abi" || die "$abi: could not publish the stage"
    [ -d "$OUT/$abi/lib" ] || die "$abi: publication left no lib directory at $OUT/$abi"
    _npub=$(find "$OUT/$abi/lib" -name '*.a' -type f | awk 'END { print NR }') \
        || die "$abi: cannot inventory the published archives under $OUT/$abi/lib"
    say "[$abi] published $_npub archives"
done
owned_rm "$PART"

# ---------------------------------------------------------------------------
# 7. The signed checker is the success gate.
# ---------------------------------------------------------------------------
say "submitting the stage to tools/check-native-crossbuild.sh"
"$here/check-native-crossbuild.sh" "$OUT"
