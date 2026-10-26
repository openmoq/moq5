#!/bin/sh
# MoQ5 Android Phase 0.2 -- NDK cross-build contract for the native dependency
# graph the picoquic adapter requires.
#
# RED BY CONSTRUCTION until a build recipe produces the artifacts below. Every
# failure is named; nothing is ever skipped, so an absent artifact, an absent
# tool and an absent stage directory are all failures rather than silence.
#
# WHY THE SOURCE-TREE PIN IS MANDATORY, not upstream archives: the drain-capable
# adapter reads picoquic's PRIVATE picoquic_internal.h
# (adapters/picoquic/CMakeLists.txt:159-171 makes the installed CONFIG package a
# FATAL_ERROR; cmake/FindPicoquic.cmake:152-171 falls through to source mode
# regardless of picoquic_DIR). A contract satisfied by prebuilt upstream
# archives would therefore not exercise the thing that actually constrains us,
# so the link probe below compiles against that private header.
#
# Pins are NOT redeclared here. They are read from the repository authorities
# that already own them, so this gate cannot become a second source of truth:
#   * picoquic / picotls : scripts/setup_picoquic_deps.sh
#   * OpenSSL tag+commit : scripts/build_ios_openssl.sh
#   * NDK                : bindings/kotlin/gradle/toolchain.properties (Phase 0.1)
#
# Usage: check-native-crossbuild.sh [<stage-dir>]
#   stage-dir defaults to bindings/kotlin/.native (gitignored build output).

set -u

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/.." && pwd)                 # bindings/kotlin
repo=$(cd "$root/../.." && pwd)              # repository root
. "$here/lib-declaration.sh"                 # the SIGNED Phase 0.1 authority
# shellcheck source=lib-hash.sh
. "$here/lib-hash.sh"                        # the one fail-closed SHA-256

stage=${1:-$root/.native}

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { printf 'FAIL[%s]: %s\n' "$1" "$2"; fails=$((fails + 1)); }
finish() {
    printf '\nnative cross-build contract: %d failure(s)\n' "$fails"
    [ "$fails" -eq 0 ] || exit 1
    exit 0
}

# The two initial ABIs (1050 section 6c). Each is checked independently.
ABIS="arm64-v8a x86_64"

# Per-ABI expectations. The ELF machine is what `readelf -h`/`file` must report
# for EVERY member of EVERY archive -- this is the host-contamination check.
# The `file format` token llvm-objdump prints for a correctly cross-built
# member. Matching this exact string is what makes a wrong-ABI or host member
# detectable: a Mach-O member prints `mach-o ...` and an x86-64 member in an
# arm64 archive prints `elf64-x86-64`.
abi_machine() {
    case $1 in
        arm64-v8a) printf 'elf64-littleaarch64' ;;
        x86_64)    printf 'elf64-x86-64' ;;
        *)         printf '' ;;
    esac
}
abi_triple() {
    case $1 in
        arm64-v8a) printf 'aarch64-linux-android' ;;
        x86_64)    printf 'x86_64-linux-android' ;;
        *)         printf '' ;;
    esac
}

# The archive set the intended one-DSO topology must link, with its anchor:
#   libcrypto.a/libssl.a  -- OpenSSL, the picotls backend
#   libpicotls-core.a     -- cmake/FindPicoquic.cmake:110
#   libpicotls-openssl.a  -- adapters/picoquic/CMakeLists.txt needs <picotls/openssl.h>
#   libpicoquic-core.a    -- the adapter's link target
#   libpicoquic-log.a     -- PicoQUIC's loglib, staged and pinned with the
#                            rest. picoquic-core carries the qlog streaming
#                            functions (picoquic_set_qlog among them), so a
#                            closed link reaches nothing defined only here; the
#                            one-DSO topology pins it by hash and never links it
#   libpicohttp-core.a    -- HTTP/3 + WebTransport (cmake/FindPicoquic.cmake:5,20)
#   libpicotls-minicrypto.a -- required by top-level CMakeLists.txt:403,
#                            scripts/setup_picoquic_deps.sh:122-124,
#                            scripts/build_ios_xcframework.sh:190-197, and the
#                            pinned picoquic's cmake/FindPTLS.cmake:18-27,65-82
REQUIRED_ARCHIVES="libcrypto.a libssl.a libpicotls-core.a libpicotls-openssl.a
libpicotls-minicrypto.a libpicoquic-core.a libpicoquic-log.a libpicohttp-core.a"

# --- pin authorities, read not redeclared ---------------------------------
# Exactly ONE syntactically accepted declaration per pin. `head -1` would
# silently take the first if an authority ever acquired a second assignment,
# which is the same last-wins class the Phase 0.1 grammar work closed.
sh_one() {  # sh_one <file> <VAR> <kind: default|literal>
    case $3 in
        default) _pat="s/^$2=\"\${[A-Za-z_]*:-\\(.*\\)}\"\$/\\1/p" ;;
        *)       _pat="s/^$2=\"\\([^\"]*\\)\"\$/\\1/p" ;;
    esac
    _hits=$(sed -n "$_pat" "$1" | grep -c .)
    if [ "$_hits" -ne 1 ]; then
        fail pin.authority "$(basename "$1") declares $2 $_hits time(s); exactly one is required"
        printf ''
        return 1
    fi
    sed -n "$_pat" "$1"
}
sh_default() { sh_one "$1" "$2" default; }
sh_literal() { sh_one "$1" "$2" literal; }

deps_sh="$repo/scripts/setup_picoquic_deps.sh"
ossl_sh="$repo/scripts/build_ios_openssl.sh"
toolchain="$root/gradle/toolchain.properties"
for f in "$deps_sh" "$ossl_sh" "$toolchain"; do
    [ -f "$f" ] || { fail pin.authority "missing pin authority $f"; finish; }
done

WANT_PICOQUIC=$(sh_default "$deps_sh" PICOQUIC_REF)
# The tracked patches dependency setup applies to each pinned tree, in order:
# the PICOQUIC_PATCHES / PICOTLS_PATCHES lists in the setup script, each of
# which must be exactly the patches tracked under patches/<dependency>. The
# build compiles each pin plus exactly these files, applied in this order
# (build-native-crossbuild.sh, step 5b).
# want_patches <LIST> <dir>: sets WANT_LIST to the declared list, one name per
# line, and records a named pin.authority failure when it is unreadable or
# differs from the tracked set. It runs in this shell -- not in a command
# substitution -- so the failure is counted.
want_patches() {
    _wp=$(sed -n "/^$1=(/,/^)/p" "$deps_sh" | sed '1d;$d' \
        | sed 's/^[[:space:]]*//; s/[[:space:]]*$//' | grep -v '^$')
    _wt=$(cd "$repo/$2" 2>/dev/null && ls -1 -- *.patch 2>/dev/null)
    if [ -z "$_wp" ]; then
        fail pin.authority "could not read the $1 list from $(basename "$deps_sh")"
    elif [ "$_wp" != "$_wt" ]; then
        fail pin.authority "the declared $1 list is not exactly the patches tracked in $2"
    fi
    WANT_LIST=$_wp
}
WANT_PQ_PATCH_DIR="patches/picoquic"
want_patches PICOQUIC_PATCHES "$WANT_PQ_PATCH_DIR"; WANT_PQ_PATCHES=$WANT_LIST
WANT_PQ_PATCH_COUNT=$(printf '%s\n' "$WANT_PQ_PATCHES" | grep -c .)
WANT_PT_PATCH_DIR="patches/picotls"
want_patches PICOTLS_PATCHES "$WANT_PT_PATCH_DIR"; WANT_PT_PATCHES=$WANT_LIST
WANT_PT_PATCH_COUNT=$(printf '%s\n' "$WANT_PT_PATCHES" | grep -c .)
# export_pinned <checkout> <tree>: the pinned tree and its (one-level)
# submodules, exactly as the recipe exports them.
export_pinned() {
    git -C "$1" archive --format=tar HEAD 2>/dev/null | tar -x -C "$2" 2>/dev/null || return 1
    [ -f "$1/.gitmodules" ] || return 0
    # `--get-regexp` exits 1 when nothing matches (an empty .gitmodules).
    _ep_subs=$(git -C "$1" config -f .gitmodules --get-regexp '^submodule\..*\.path$'); _ep_rc=$?
    [ "$_ep_rc" -eq 0 ] || [ "$_ep_rc" -eq 1 ] || return 1
    printf '%s\n' "$_ep_subs" | awk 'NF { print $2 }' \
        | while IFS= read -r _ep_sub; do
            mkdir -p "$2/$_ep_sub" || exit 1
            git -C "$1/$_ep_sub" archive --format=tar HEAD 2>/dev/null | tar -x -C "$2/$_ep_sub" 2>/dev/null || exit 1
        done
}
# patch_rederive <scratch> <tree> <dir> <names>: apply every tracked patch to
# <tree> in order, and require each to be fully present afterwards (it no
# longer applies forward). `git apply` discovery stops at <scratch>, so <tree>
# itself is the patch root.
patch_rederive() {
    printf '%s\n' "$4" | while IFS= read -r _rd_name; do
        _rd_patch="$repo/$3/$_rd_name"
        ( cd "$2" && GIT_CEILING_DIRECTORIES="$1" git apply "$_rd_patch" ) >/dev/null 2>&1 || exit 1
        if ( cd "$2" && GIT_CEILING_DIRECTORIES="$1" git apply --check "$_rd_patch" ) >/dev/null 2>&1; then
            exit 1
        fi
    done
}
# check_patched_src <dep> <label> <names> <dir> <count>: the <dep>.patch.* list in
# this ABI's provenance, then the recorded <dep>.build.src re-derived from the
# pinned checkout plus that list, then the staged build's configure source.
check_patched_src() {
    # The identity is the ordered
    #     list (count, index -> path, index -> sha256) against the tracked
    #     files' bytes: a wrong count, a reordered or renamed entry, a hash that
    #     belongs to another entry, a patch whose bytes changed since this stage
    #     was built, or a leftover single-patch key invalidates the stage rather
    #     than leaving it green. Only the FIXED tracked paths are ever read; the
    #     receipt's paths are compared, never followed, so a poisoned receipt
    #     cannot point this check at another file.
    _cd=$1; _cl=$2; _cwant=$3; _cdir=$4; _ccount=$5
    pq_build_src=$(props_value "$prov" "$_cd.build.src")
    pq_pinned_src=$(props_value "$prov" "$_cd.src")
    pq_count=$(props_value "$prov" "$_cd.patch.count")
    pq_patch_ok=0
    # A directory this run creates, for the hasher's scratch files and the
    # re-derived source; removed at the end of this ABI's checks.
    _pq_tmp=$(mktemp -d "${TMPDIR:-/tmp}/moq-crossbuild-check.XXXXXX") \
        || { fail "$abi.src.$_cd.patch" "cannot create a private directory to check the patches"; return 0; }
    # Every patch key in the receipt, so a stray index, a duplicated key or the
    # old single-patch form cannot hide beside a valid-looking list.
    grep -E "^$_cd\\.patch(\\.|=)" "$prov" | sed 's/=.*//' > "$_pq_tmp/keys"
    _pq_bad=""
    if [ "$(props_count "$prov" "$_cd.patch")" -ne 0 ] || [ "$(props_count "$prov" "$_cd.patch.sha256")" -ne 0 ]; then
        _pq_bad="provenance carries the single-patch form ($_cd.patch / $_cd.patch.sha256), which names one patch while the build applies an ordered list; the authority is ambiguous"
    elif ! printf '%s' "$pq_count" | grep -Eq '^(0|[1-9][0-9]*)$'; then
        _pq_bad="$_cd.patch.count '$pq_count' is not a decimal count"
    elif [ "$pq_count" -ne "$_ccount" ]; then
        _pq_bad="provenance records $pq_count patch(es), the tracked ordered list has $_ccount"
    elif [ -n "$(sort "$_pq_tmp/keys" | uniq -d)" ]; then
        _pq_bad="provenance repeats a patch key: $(sort "$_pq_tmp/keys" | uniq -d | tr '\n' ' ')"
    else
        _pq_i=0
        printf '%s\n' "$_cwant" > "$_pq_tmp/want"
        while IFS= read -r _pq_name; do
            _pq_rel="$_cdir/$_pq_name"
            _pq_got=$(props_value "$prov" "$_cd.patch.$_pq_i")
            _pq_sha=$(props_value "$prov" "$_cd.patch.$_pq_i.sha256")
            _pq_have=""
            if [ "$_pq_got" != "$_pq_rel" ]; then
                _pq_bad="$_cd.patch.$_pq_i is '$_pq_got', the tracked list has '$_pq_rel' at that position"
            elif [ ! -f "$repo/$_pq_rel" ]; then
                _pq_bad="the tracked patch $_pq_rel is missing, so the compiled source cannot be reproduced"
            elif ! _pq_have=$(moq_hash_file "$repo/$_pq_rel" "$_pq_tmp" 2> "$_pq_tmp/hash.err"); then
                _pq_bad="the tracked patch $_pq_rel cannot be hashed: $(sed -n '1p' "$_pq_tmp/hash.err" | cut -c1-90)"
            elif [ "$_pq_have" != "$_pq_sha" ]; then
                _pq_bad="$_cd.patch.$_pq_i.sha256 is '$_pq_sha', but the tracked patch $_pq_rel hashes to $_pq_have: this stage was built from other patch bytes, or the hash belongs to another entry"
            fi
            [ -z "$_pq_bad" ] || break
            printf '%s.patch.%s\n%s.patch.%s.sha256\n' "$_cd" "$_pq_i" "$_cd" "$_pq_i" >> "$_pq_tmp/expected-keys"
            _pq_i=$((_pq_i + 1))
        done < "$_pq_tmp/want"
        if [ -z "$_pq_bad" ]; then
            printf '%s.patch.count\n' "$_cd" >> "$_pq_tmp/expected-keys"
            sort "$_pq_tmp/expected-keys" > "$_pq_tmp/ek"
            sort "$_pq_tmp/keys" > "$_pq_tmp/gk"
            if ! cmp -s "$_pq_tmp/ek" "$_pq_tmp/gk"; then
                _pq_bad="provenance carries patch keys outside the ordered list: $(comm -13 "$_pq_tmp/ek" "$_pq_tmp/gk" | tr '\n' ' ')"
            fi
        fi
    fi
    if [ -n "$_pq_bad" ]; then
        fail "$abi.src.$_cd.patch" "$_pq_bad"
    else
        pass "$abi.src.$_cd.patch" "$pq_count ordered patch(es): $(printf '%s' "$_cwant" | tr '\n' ' ')"
        pq_patch_ok=1
    fi
    if [ "$pq_patch_ok" -ne 1 ]; then
        # No second conclusion from an untrusted identity: the source checks
        # below depend on it, so they are not evaluated.
        printf 'SKIP[%s]: %s\n' "$abi.src.$_cd.build" "not evaluated: the patch identity failed"
        printf 'SKIP[%s]: %s\n' "$abi.src.$_cd.compiled" "not evaluated: the patch identity failed"
    else
        # The recorded build source must be EXACTLY the pinned tree with the
        # tracked patch applied: re-derive it, in a directory this run creates,
        # from the verified pinned checkout, and compare by exit status. Only
        # the first difference is printed; the full list stays in a file.
        mkdir "$_pq_tmp/expected"
        if [ ! -d "$pq_build_src" ] || [ -L "$pq_build_src" ]; then
            fail "$abi.src.$_cd.build" "the recorded build source '$pq_build_src' is not a real directory"
        elif ! export_pinned "$pq_pinned_src" "$_pq_tmp/expected"; then
            fail "$abi.src.$_cd.build" "cannot export the pinned tree from '$pq_pinned_src' to re-derive the build source"
        elif ! patch_rederive "$_pq_tmp" "$_pq_tmp/expected" "$_cdir" "$_cwant"; then
            fail "$abi.src.$_cd.build" "the tracked patch list does not apply in order to the pinned tree, so the build source cannot be re-derived"
        elif ! diff -rq "$_pq_tmp/expected" "$pq_build_src" > "$_pq_tmp/diff" 2>&1; then
            fail "$abi.src.$_cd.build" "the build source is not the pinned tree plus the tracked patches: $(head -1 "$_pq_tmp/diff" | cut -c1-90)"
        else
            pass "$abi.src.$_cd.build" "$pq_build_src is the pinned tree plus the $_ccount tracked patch(es), applied in order"
        fi
        # ... and that source is what the staged build was configured from.
        _pq_home=$(sed -n 's/^CMAKE_HOME_DIRECTORY:INTERNAL=//p' "$adir/cmake/$_cd.cache" 2>/dev/null)
        if [ "$_pq_home" = "$pq_build_src" ]; then
            pass "$abi.src.$_cd.compiled" "the staged $_cl build was configured from $pq_build_src"
        else
            fail "$abi.src.$_cd.compiled" "the staged $_cl build was configured from '$_pq_home', not the recorded build source '$pq_build_src'"
        fi
    fi
    rm -rf "$_pq_tmp"
}
WANT_PICOTLS=$(sh_default "$deps_sh" PICOTLS_REF)
WANT_OPENSSL_TAG=$(sh_default "$ossl_sh" OPENSSL_REF)
WANT_OPENSSL_COMMIT=$(sh_literal "$ossl_sh" OPENSSL_PINNED_COMMIT)
WANT_NDK=$(props_value "$toolchain" sdk.ndk)
for pair in "picoquic=$WANT_PICOQUIC" "picotls=$WANT_PICOTLS" \
            "openssl.tag=$WANT_OPENSSL_TAG" "openssl.commit=$WANT_OPENSSL_COMMIT" \
            "ndk=$WANT_NDK"; do
    [ -n "${pair#*=}" ] || { fail pin.authority "could not read the ${pair%%=*} pin from its authority"; }
done
[ "$fails" -eq 0 ] || finish
pass pin.authority "picoquic $WANT_PICOQUIC + $WANT_PQ_PATCH_COUNT ordered patch(es) / picotls $WANT_PICOTLS + $WANT_PT_PATCH_COUNT ordered patch(es) / OpenSSL $WANT_OPENSSL_TAG ($WANT_OPENSSL_COMMIT) / NDK $WANT_NDK"

# --- tools this contract needs; absent is a FAILURE, never a skip ---------
# The archives are SysV archives of Android ELF objects. A host BSD `ar` cannot
# read them, and host `file` on an .a reports only "current ar archive" -- so
# the inspection uses the NDK's OWN llvm-objdump, derived per ABI from the
# compiler each provenance file records. That also means the artifacts are read
# with the toolchain that produced them.
# No host `file` dependency: it reports only "current ar archive" for an .a and
# cannot read Android ELF reliably. Everything is classified with the NDK's own
# tools, derived per ABI from the compiler each provenance file records.
command -v git >/dev/null 2>&1 || \
    fail tooling "'git' is required to verify each dependency checkout's HEAD against its authority"
[ "$fails" -eq 0 ] || finish

# --- the staged artifacts -------------------------------------------------
if [ ! -d "$stage" ]; then
    fail stage "no cross-build stage at $stage: Phase 0.2 has no NDK build recipe yet, so nothing has produced the per-ABI dependency graph"
    finish
fi

# Walk up to the NDK root rather than counting directory levels: the prebuilt
# layout is <root>/toolchains/llvm/prebuilt/<host>/bin, and an off-by-one
# silently produced an empty revision instead of a mismatch. Both the toolchain
# rows and the per-component identity rows need this, so it is one function.
ndk_root_of() {
    _r=$(dirname "$1")
    _n=0
    while [ "$_n" -lt 8 ]; do
        _r=$(cd "$_r/.." 2>/dev/null && pwd) || return 0
        if [ -f "$_r/source.properties" ]; then
            printf '%s' "$_r"
            return 0
        fi
        _n=$((_n + 1))
    done
}

for abi in $ABIS; do
    adir="$stage/$abi"
    machine=$(abi_machine "$abi")
    triple=$(abi_triple "$abi")

    if [ ! -d "$adir" ]; then
        fail "$abi.stage" "no artifacts for ABI '$abi' at $adir"
        continue
    fi

    # 1. provenance, in the canonical properties subset the signed Phase 0.1
    #    authority already validates -- so the 0.3 topology audit can consume it
    #    deterministically instead of guessing.
    prov="$adir/provenance.properties"
    if [ ! -f "$prov" ]; then
        fail "$abi.provenance" "missing $prov (deterministic build metadata for the 0.3 audit)"
        continue
    fi
    if ! decl_validate_grammar "$prov"; then
        fail "$abi.provenance" "provenance is not in the canonical properties subset"
        continue
    fi
    _pv_missing=0
    for k in abi ndk.version ndk.cc ndk.target android.api \
             picoquic.commit picotls.commit openssl.tag openssl.commit \
             picoquic.src picotls.src openssl.src openssl.prefix elf.machine \
             picoquic.build.src picoquic.patch.count picotls.build.src picotls.patch.count; do
        if [ "$(props_count "$prov" "$k")" -ne 1 ] || [ -z "$(props_value "$prov" "$k")" ]; then
            fail "$abi.provenance" "provenance key '$k' is missing, empty or duplicated"
            _pv_missing=1
        fi
    done
    [ "$_pv_missing" -eq 0 ] || continue
    pass "$abi.provenance" "$prov"

    # 2. Source identity, verified against the ACTUAL checkout -- not the
    #    receipt. A provenance file claiming the right commit while the tree it
    #    names is at another revision is exactly the hazard here: the outer
    #    workspace carries LibMoQ/picoquic at 33afc776, which is NOT the pin.
    #    Provenance may carry paths: it is ignored build output, not a pin.
    for trip in "picoquic:$WANT_PICOQUIC" "picotls:$WANT_PICOTLS" "openssl:$WANT_OPENSSL_COMMIT"; do
        dep=${trip%%:*}; want=${trip#*:}
        got=$(props_value "$prov" "$dep.commit")
        src=$(props_value "$prov" "$dep.src")
        if [ -z "$src" ]; then
            fail "$abi.src.$dep" "provenance records no '$dep.src' checkout location, so its claimed commit cannot be verified"
            continue
        fi
        # Ask GIT whether this is a repository -- a `.git` DIRECTORY test
        # rejects a perfectly valid worktree, where `.git` is a file.
        if ! git -C "$src" rev-parse --git-dir >/dev/null 2>&1; then
            fail "$abi.src.$dep" "'$src' is not a git repository"
            continue
        fi
        head=$(git -C "$src" rev-parse HEAD 2>/dev/null)
        # A matching HEAD is not an exact source identity: tracked files can be
        # modified, and untracked files can be compiled in, without HEAD moving.
        # These builds are out-of-source, so the authority is a CLEAN checkout
        # at the pinned commit.
        #
        # Two queries, because one does not cover the other. `status --porcelain
        # --untracked-files=all` reports modified tracked files, ordinary
        # untracked files and dirty submodules (Git's recursive status
        # semantics), but it deliberately EXCLUDES ignored files -- and an
        # ignored file is just as compilable as any other. `git clean -ndx` is
        # a DRY RUN that lists ignored and untracked residue and removes
        # nothing; this gate never runs a destructive git clean.
        _dirty=$(git -C "$src" status --porcelain --untracked-files=all 2>/dev/null | head -3)
        if [ -n "$_dirty" ]; then
            fail "$abi.src.$dep" "checkout '$src' is at the pin but is NOT clean, so its content is not the pinned source: $(printf '%s' "$_dirty" | tr '\n' ';' | cut -c1-70)"
            continue
        fi
        _residue=$(git -C "$src" clean -ndx 2>/dev/null | head -3)
        if [ -n "$_residue" ]; then
            fail "$abi.src.$dep" "checkout '$src' is at the pin but carries ignored/untracked residue, which builds just as well as tracked source: $(printf '%s' "$_residue" | tr '\n' ';' | cut -c1-70)"
            continue
        fi
        if [ "$got" != "$want" ]; then
            fail "$abi.pin.$dep" "provenance claims '$got', the repository authority pins '$want'"
        elif [ "$head" != "$want" ]; then
            fail "$abi.src.$dep" "checkout '$src' is at HEAD $head, but provenance claims $want -- the receipt does not match the tree"
        else
            pass "$abi.src.$dep" "$src at $head"
        fi
    done
    # 2b. The PicoQUIC and picotls sources actually COMPILED: each pinned tree
    #     plus the repository's tracked patches for it, in order.
    check_patched_src picoquic PicoQUIC "$WANT_PQ_PATCHES" "$WANT_PQ_PATCH_DIR" "$WANT_PQ_PATCH_COUNT"
    check_patched_src picotls picotls "$WANT_PT_PATCHES" "$WANT_PT_PATCH_DIR" "$WANT_PT_PATCH_COUNT"

    got_tag=$(props_value "$prov" openssl.tag)
    if [ "$got_tag" = "$WANT_OPENSSL_TAG" ]; then
        pass "$abi.pin.openssl.tag" "$got_tag"
    else
        fail "$abi.pin.openssl.tag" "provenance claims '$got_tag', the authority pins '$WANT_OPENSSL_TAG'"
    fi

    # 3. the NDK toolchain actually used, and the API level as a recorded
    #    EXPERIMENTAL BUILD INPUT -- this contract makes no minSdk ruling.
    got_ndk=$(props_value "$prov" ndk.version)
    if [ "$got_ndk" = "$WANT_NDK" ]; then
        pass "$abi.ndk.version" "$got_ndk"
    else
        fail "$abi.ndk.version" "built with NDK '$got_ndk', Phase 0.1 pins '$WANT_NDK'"
    fi
    # Executable is not identity. The compiler must (a) live under an NDK whose
    # source.properties reports the pinned revision, (b) be the expected
    # ${triple}${api}-clang wrapper for THIS ABI, and (c) actually report that
    # target when asked -- a query it has to execute, not a filename claim.
    cc=$(props_value "$prov" ndk.cc)
    # GRAMMAR FIRST. The API is used to construct a wrapper name and then to
    # EXECUTE it, so a malformed value must be rejected on its own row before
    # it can be interpolated -- otherwise `21garbage` is intercepted by the
    # ndk.cc row and the grammar gate is unreachable.
    api_early=$(props_value "$prov" android.api)
    if ! printf '%s' "$api_early" | grep -Eq '^[1-9][0-9]*$'; then
        fail "$abi.android.api" "android.api '$api_early' is not an all-decimal positive number"
        continue
    fi
    if [ ! -x "$cc" ]; then
        fail "$abi.ndk.cc" "recorded compiler '$cc' is not executable"
        continue
    fi
    ndk_root=$(ndk_root_of "$cc")
    ndk_rev=""
    [ -n "$ndk_root" ] && ndk_rev=$(sed -n 's/^Pkg.Revision *= *//p' "$ndk_root/source.properties")
    if [ "$ndk_rev" != "$WANT_NDK" ]; then
        fail "$abi.ndk.cc" "compiler '$cc' resolves to NDK '$ndk_rev' (root $ndk_root), the pin is '$WANT_NDK'"
        continue
    fi
    want_wrapper="${triple}${api_early}-clang"
    if [ "$(basename "$cc")" != "$want_wrapper" ]; then
        fail "$abi.ndk.cc" "compiler is '$(basename "$cc")', expected the '$want_wrapper' wrapper for $abi at API $api_early"
        continue
    fi
    # The NDK wrappers report <arch>-unknown-linux-android<API>; normalise the
    # vendor component and the trailing API before comparing, and check the API
    # it reports against the one provenance declares -- so the wrapper name and
    # the compiler's own answer must agree.
    cc_target=$("$cc" -dumpmachine 2>/dev/null)
    cc_norm=$(printf '%s' "$cc_target" | sed 's/-unknown-/-/; s/[0-9]*$//')
    cc_api=$(printf '%s' "$cc_target" | sed 's/.*[^0-9]\([0-9][0-9]*\)$/\1/')
    if [ "$cc_norm" != "$triple" ]; then
        fail "$abi.ndk.cc" "compiler reports target '$cc_target' (normalised '$cc_norm'), expected '$triple' for $abi"
        continue
    fi
    if [ "$cc_api" != "$api_early" ]; then
        fail "$abi.ndk.cc" "compiler targets API '$cc_api' but provenance declares android.api '$api_early'"
        continue
    fi
    pass "$abi.ndk.cc" "$want_wrapper (NDK $ndk_rev), -dumpmachine $cc_target"
    objdump="$(dirname "$cc")/llvm-objdump"
    ar="$(dirname "$cc")/llvm-ar"
    if [ ! -x "$ar" ]; then
        fail "$abi.ndk.ar" "no llvm-ar beside the recorded compiler ($ar); the member inventory cannot be read with the toolchain that built it"
        continue
    fi
    if [ ! -x "$objdump" ]; then
        fail "$abi.ndk.objdump" "no llvm-objdump beside the recorded compiler ($objdump); archive members cannot be classified with the toolchain that built them"
        continue
    fi

    got_triple=$(props_value "$prov" ndk.target)
    if [ "$got_triple" = "$triple" ]; then
        pass "$abi.ndk.target" "$got_triple"
    else
        fail "$abi.ndk.target" "recorded target '$got_triple', expected '$triple' for $abi"
    fi
    got_abi=$(props_value "$prov" abi)
    if [ "$got_abi" = "$abi" ]; then
        pass "$abi.abi" "$got_abi"
    else
        fail "$abi.abi" "provenance says abi='$got_abi' but sits in the '$abi' directory"
    fi
    got_machine=$(props_value "$prov" elf.machine)
    if [ "$got_machine" = "$machine" ]; then
        pass "$abi.elf.machine" "$got_machine"
    else
        fail "$abi.elf.machine" "provenance says elf.machine='$got_machine', expected '$machine' for $abi"
    fi
    # Grammar was validated above, before the value was interpolated into a
    # wrapper name; this records it as the experimental build input it is.
    api=$(props_value "$prov" android.api)
    pass "$abi.android.api" "$api (experimental build input; NOT a minSdk ruling)"

    # 4/5. every required archive, every MEMBER target-ELF for this ABI, PIC.
    for a in $REQUIRED_ARCHIVES; do
        p="$adir/lib/$a"
        if [ ! -f "$p" ]; then
            fail "$abi.archive.$a" "missing $p"
            continue
        fi
        # EVERY MEMBER, ACCOUNTED FOR EXACTLY. Filtering objdump output to the
        # lines it recognised would let a garbage or non-object member simply
        # disappear from the observation, so the member INVENTORY from llvm-ar
        # is compared with the number of recognised formats: they must be equal.
        _listed=$("$ar" t "$p" 2>/dev/null | grep -c . || true)
        _fmt=$("$objdump" -f "$p" 2>&1 | sed -n 's/.*file format //p')
        _seen=$(printf '%s\n' "$_fmt" | grep -c . || true)
        if [ "${_listed:-0}" -eq 0 ]; then
            fail "$abi.archive.$a" "$p lists no members"
            continue
        fi
        if [ "${_seen:-0}" -ne "${_listed:-0}" ]; then
            fail "$abi.archive.$a" "$_listed member(s) listed but only $_seen classified: a member is not a readable object"
            continue
        fi
        _wrong=$(printf '%s\n' "$_fmt" | grep -v "^$machine$" | head -1)
        if [ -n "$_wrong" ]; then
            fail "$abi.archive.$a" "a member reports file format '$_wrong', expected '$machine' for $abi"
            continue
        fi
        _n=$_listed
        pass "$abi.archive.$a" "$_n member(s), all ELF $machine"
        # Two members with one name are indistinguishable to every tool that
        # addresses a member by name (extraction, debug maps), so each name
        # must be unique.
        _dup=$("$ar" t "$p" 2>/dev/null | sort | uniq -d | tr '\n' ' ')
        if [ -n "$_dup" ]; then
            fail "$abi.archive.$a.members" "member name(s) appear more than once: $_dup"
        else
            pass "$abi.archive.$a.members" "every member name is unique"
        fi
    done
done

# 6/7. THE PROBE IS EXECUTED HERE, not attested to.
#
# An earlier revision accepted a pre-staged `<abi>.linked` file and asked only
# whether its format matched -- so the target `libcrypto.a` could impersonate an
# executed probe, and the "not executed" mutant proved only missing-file
# detection. The checker now runs the compile and the link itself, from a clean
# output path, with the compiler it just bound to the pinned NDK.
probe_src="$here/probe/moq_pq_android_probe.c"
if [ ! -f "$probe_src" ]; then
    fail probe.source "missing $probe_src"
    finish
fi

for abi in $ABIS; do
    adir="$stage/$abi"
    prov="$adir/provenance.properties"
    [ -f "$prov" ] || continue          # already failed above, by name

    machine=$(abi_machine "$abi")
    triple=$(abi_triple "$abi")         # per ABI, never inherited from the
                                        # previous loop's last iteration
    cc=$(props_value "$prov" ndk.cc)
    [ -x "$cc" ] || continue            # already failed above, by name
    objdump="$(dirname "$cc")/llvm-objdump"
    readelf="$(dirname "$cc")/llvm-readelf"
    pq_src=$(props_value "$prov" picoquic.src)
    ptls_src=$(props_value "$prov" picotls.src)
    ossl_src=$(props_value "$prov" openssl.src)
    # OpenSSL's public headers are only complete in an INSTALLED prefix: the
    # generated opensslv.h/configuration.h are not in the source tree. The
    # provenance may name that prefix; the source include dir is added too, so
    # a source-only layout still resolves what it can.
    ossl_prefix=$(props_value "$prov" openssl.prefix)
    ossl_inc_src="-I$ossl_src/include"
    if [ ! -f "$ossl_prefix/include/openssl/opensslv.h" ]; then
        fail "$abi.openssl.prefix" "openssl.prefix '$ossl_prefix' has no include/openssl/opensslv.h: OpenSSL's generated public headers exist only in an INSTALLED prefix, not in the source tree"
        continue
    fi
    ossl_inc_pfx="-I$ossl_prefix/include"

    out="$stage/.probe/$abi"
    rm -rf "$out"                        # a stale artifact must never pass
    mkdir -p "$out"
    log="$out/build.log"

    # -- compile: our OWNED private-header consumer, warnings as errors -----
    if ! "$cc" -c -fPIC -Wall -Wextra -Werror \
            -I"$repo/adapters/common" \
            -I"$pq_src/picoquic" -I"$pq_src/loglib" -I"$pq_src/picohttp" \
            -I"$ptls_src/include" "$ossl_inc_pfx" "$ossl_inc_src" \
            -o "$out/probe.o" "$probe_src" >>"$log" 2>&1; then
        fail "$abi.probe.compile" "the probe did not compile against the staged headers: $(grep -m1 -i 'error' "$log" | cut -c1-90)"
        continue
    fi
    if [ ! -f "$out/probe.o" ]; then
        fail "$abi.probe.compile" "the compile command produced no object"
        continue
    fi
    pass "$abi.probe.compile" "our private-header consumer compiled for $abi"

    # -- TWO named link gates, because one command cannot honestly prove both.
    #
    # A --whole-archive link pulls in EVERY member, including members of
    # optional features the final DSO never calls, so it can only be a PIC
    # proof. A closed link proves the graph resolves, but deliberately links
    # only what is reached. They are therefore separate gates with separate
    # claims, and neither is described as the other.

    # -- gate A: all-member PIC -------------------------------------------
    # Proves ONLY that every member of every required archive can be linked
    # into a shared object, i.e. all members are position-independent. It
    # makes NO dependency-closure claim: unresolved references are permitted
    # here by construction, which is exactly why gate B exists.
    if ! "$cc" -shared -o "$out/libmoqpic.so" "$out/probe.o" \
            -Wl,--whole-archive \
            "$adir/lib/libpicoquic-core.a" "$adir/lib/libpicoquic-log.a" \
            "$adir/lib/libpicohttp-core.a" \
            "$adir/lib/libpicotls-core.a" "$adir/lib/libpicotls-openssl.a" \
            "$adir/lib/libpicotls-minicrypto.a" \
            "$adir/lib/libssl.a" "$adir/lib/libcrypto.a" \
            -Wl,--no-whole-archive >>"$log" 2>&1; then
        fail "$abi.archive.pic" "a member of the required graph could not be linked into a shared object (non-PIC member): $(grep -m1 -iE 'error|relocation' "$log" | cut -c1-90)"
        continue
    fi
    pass "$abi.archive.pic" "every member of all $(printf '%s' "$REQUIRED_ARCHIVES" | wc -w | tr -d ' ') archives links into a shared object (PIC; no closure claim)"

    # -- gate B: CLOSED dependency link ------------------------------------
    # Android permits unresolved references in a shared library by default, so
    # a plain -shared link proves only that lld emitted an ET_DYN file. This
    # gate adds -Wl,-z,defs (and --no-undefined) so EVERY reference must
    # resolve inside the staged graph plus the NDK sysroot. A link carrying
    # unresolved non-platform dependencies is never green here.
    #
    # No --whole-archive: this is the final-shaped link, so it pulls only the
    # members the probe's anchors reach. It proves the graph RESOLVES, not that
    # every archive contributes: libpicoquic-log.a stays on the line, but its
    # qlog anchor (picoquic_set_qlog) resolves from picoquic-core, so nothing is
    # selected from it here. That archive's members are proved PIC by gate A.
    if ! "$cc" -shared -o "$out/libmoqprobe.so" "$out/probe.o" \
            -Wl,-z,defs -Wl,--no-undefined \
            "$adir/lib/libpicoquic-log.a" "$adir/lib/libpicoquic-core.a" \
            "$adir/lib/libpicohttp-core.a" \
            "$adir/lib/libpicotls-openssl.a" "$adir/lib/libpicotls-minicrypto.a" \
            "$adir/lib/libpicotls-core.a" \
            "$adir/lib/libssl.a" "$adir/lib/libcrypto.a" >>"$log" 2>&1; then
        fail "$abi.probe.link.closed" "the graph does not resolve under -z defs: $(grep -m1 -iE 'undefined|error' "$log" | sed 's/^.*: //' | cut -c1-90)"
        continue
    fi

    # -- the output must be a FRESH shared object of the right type/machine -
    if [ ! -f "$out/libmoqprobe.so" ]; then
        fail "$abi.probe.link.closed" "the link command produced no output"
        continue
    fi
    _fmt=$("$objdump" -f "$out/libmoqprobe.so" 2>&1 | sed -n 's/.*file format //p' | head -1)
    if [ "$_fmt" != "$machine" ]; then
        fail "$abi.probe.link.closed" "probe output reports '$_fmt', expected '$machine'"
        continue
    fi
    # The command is -shared, so DYN is the only correct answer. EXEC is not an
    # acceptable alternate result for this contract, and REL is an object file.
    _type=$("$readelf" -h "$out/libmoqprobe.so" 2>/dev/null | sed -n 's/^ *Type: *\([A-Z]*\).*/\1/p' | head -1)
    if [ "$_type" != "DYN" ]; then
        fail "$abi.probe.link.closed" "probe output is ELF type '$_type'; -shared must produce exactly DYN, never REL, EXEC or an archive"
        continue
    fi
    # And the linker really did pull our symbol in.
    if ! "$objdump" -t "$out/libmoqprobe.so" 2>/dev/null | grep -q moq_pq_android_probe; then
        fail "$abi.probe.link.closed" "the probe symbol is absent from the linked output"
        continue
    fi
    # Belt and braces: read the ELF the checker just produced rather than
    # trusting the linker's exit status. STRONG undefined references only --
    # a WEAK undefined symbol is a deliberate runtime-optional reference (here
    # getentropy and memfd_create, which exist only above API 21 and are
    # guarded at runtime), and -z defs permits exactly those. Rejecting them
    # would overrule the gate, not strengthen it.
    _und=$("$readelf" --dyn-syms "$out/libmoqprobe.so" 2>/dev/null \
           | awk '$7=="UND" && $5!="WEAK" && $8!="" {print $8}' | grep -v '@LIBC' | sort -u | head -3)
    if [ -n "$_und" ]; then
        fail "$abi.probe.link.closed" "linked output still carries non-platform undefined symbols: $(printf '%s' "$_und" | tr '\n' ' ' | cut -c1-70)"
        continue
    fi
    pass "$abi.probe.link.closed" "closed link (-z defs), ELF DYN $machine, over all eight required archives (resolution only; no per-archive selection is measured)"

    # -- the build log: real evidence, not an empty receipt ----------------
    # An empty `.warnings` file could be created without running anything, so
    # the contract now requires a NON-EMPTY log carrying named completion
    # evidence for each component, and fails on any diagnostic in it.
    stage_log="$adir/build.log"
    if [ ! -s "$stage_log" ]; then
        fail "$abi.buildlog" "missing or empty $stage_log: an empty receipt is self-attestation, not evidence that OpenSSL, picotls and picoquic were built"
        continue
    fi
    _missing=""
    for comp in openssl picotls picoquic; do
        grep -qi "moq-crossbuild: $comp: done" "$stage_log" || _missing="${_missing:+$_missing }$comp"
    done
    if [ -n "$_missing" ]; then
        fail "$abi.buildlog" "no completion evidence for: $_missing"
        continue
    fi
    # ERRORS, not only warnings. Completion markers are lines the recipe
    # writes, so a log carrying a real failure followed by hand-added markers
    # would otherwise read as evidence of a clean build.
    _err=$(grep -inE 'error:|fatal error|CMake Error|make(\[[0-9]+\])?: \*\*\*|Error [0-9]+$' "$stage_log" | head -1)
    if [ -n "$_err" ]; then
        fail "$abi.buildlog" "the cross-build log records a failure, so its completion markers are not evidence: $(printf '%s' "$_err" | cut -c1-80)"
        continue
    fi
    _warn=$(grep -inE 'warning:|warning \[|CMake Warning' "$stage_log" | head -1)
    if [ -n "$_warn" ]; then
        fail "$abi.warnings" "the cross-build emitted a diagnostic: $(printf '%s' "$_warn" | cut -c1-80)"
        continue
    fi
    # COMPONENT IDENTITY -- three SEPARATE evidence classes, because the three
    # components leave different evidence and one cannot stand in for another.
    #
    # OpenSSL builds through make and prints every compile command, so its API
    # is read from the compiler tokens it actually invoked. The two CMake
    # components are not verbose and contribute NO compiler token at all, so a
    # token sweep over the whole log proves OpenSSL and merely INFERS the other
    # two -- which is how picotls configured at android-35 previously passed.
    # They are therefore validated from their own generated CMake caches.
    ndk_root_here=$(ndk_root_of "$cc")
    if [ -z "$ndk_root_here" ]; then
        fail "$abi.component.openssl" "cannot resolve the NDK root from '$cc', so component identity cannot be checked"
        continue
    fi

    # -- OpenSSL: COMMANDS, inside OpenSSL's OWN block ----------------------
    # An earlier revision searched every `androidNN` substring in the whole
    # combined log. That was neither command-shaped nor scoped: rewriting every
    # real invocation to androidXX-clang and appending one line of prose
    # containing "android21" left ZERO real API-21 invocations and still
    # passed. Both properties are fixed here.
    #
    # Boundary grammar first. The recipe emits `moq-crossbuild: <comp>: begin`
    # and `: done`; the block is what lies strictly between OpenSSL's pair.
    # Missing, duplicated, reversed or nested boundaries fail closed rather
    # than letting the reader pick the first convenient block.
    _b_begin=$(grep -c '^moq-crossbuild: openssl: begin$' "$stage_log" || true)
    _b_done=$(grep -c '^moq-crossbuild: openssl: done$' "$stage_log" || true)
    if [ "${_b_begin:-0}" -ne 1 ] || [ "${_b_done:-0}" -ne 1 ]; then
        fail "$abi.component.openssl" "the log must carry exactly one 'moq-crossbuild: openssl: begin' and one ': done' boundary; found $_b_begin begin / $_b_done done"
        continue
    fi
    _ln_begin=$(grep -n '^moq-crossbuild: openssl: begin$' "$stage_log" | cut -d: -f1)
    _ln_done=$(grep -n '^moq-crossbuild: openssl: done$' "$stage_log" | cut -d: -f1)
    if [ "$_ln_begin" -ge "$_ln_done" ]; then
        fail "$abi.component.openssl" "the openssl boundary is reversed: begin at line $_ln_begin, done at line $_ln_done"
        continue
    fi
    # No other component may open inside OpenSSL's block.
    _nested=$(sed -n "$((_ln_begin + 1)),$((_ln_done - 1))p" "$stage_log" \
              | grep -E '^moq-crossbuild: (picotls|picoquic): (begin|done)$' | head -1)
    if [ -n "$_nested" ]; then
        fail "$abi.component.openssl" "another component boundary is nested inside the openssl block ('$_nested'), so the block is ambiguous"
        continue
    fi

    # COMPILE COMMANDS, classified by position and arguments -- not by the
    # presence of a wrapper token. The previous regex accepted a wrapper name
    # preceded by any whitespace anywhere on a line, so one sentence of prose
    # inside a valid block ("note: selected aarch64-linux-android21-clang for
    # the OpenSSL build record") satisfied the whole row. That was still a
    # token classifier.
    #
    # This recipe's OpenSSL block contains exactly TWO measured forms that
    # mention a wrapper, and they are encoded deliberately:
    #
    #   (a) COMPILE -- the wrapper is the first command word and the argument
    #       list carries a separate -c and -o. 1133 lines (arm64-v8a) and
    #       1142 (x86_64). This is the evidence.
    #   (b) `CC="<wrapper>" perl .../asm/<x>.pl ...` -- the assembly GENERATOR,
    #       23 lines (arm64-v8a) and 40 (x86_64). The wrapper appears in a
    #       shell assignment, the command word is `CC=...`, and no compilation
    #       happens on that line, so it is not evidence and is not counted.
    #
    # The wrapper FAMILY is matched broadly (any arch, any API) and the
    # ABI/API comparison happens afterwards, so a wrong-architecture or
    # wrong-API compile stays an OBSERVED bad command instead of vanishing
    # from the inventory.
    _ossl_cmds=$(sed -n "$((_ln_begin + 1)),$((_ln_done - 1))p" "$stage_log" \
        | awk '
            {
                line = $0
                sub(/^[[:space:]]+/, "", line)
                n = split(line, w, /[[:space:]]+/)
                if (n == 0) next
                cmd = w[1]
                sub(/^.*\//, "", cmd)          # an absolute path is fine; compare the basename
                if (cmd !~ /^[A-Za-z0-9_]+-linux-android[0-9]+-clang(\+\+)?$/) next
                hasc = 0; haso = 0
                for (i = 2; i <= n; i++) {
                    if (w[i] == "-c") hasc = 1
                    if (w[i] == "-o") haso = 1
                }
                if (hasc && haso) print cmd
            }')
    _ossl_n=$(printf '%s\n' "$_ossl_cmds" | grep -c . || true)
    if [ "${_ossl_n:-0}" -eq 0 ]; then
        fail "$abi.component.openssl" "the openssl block (lines $_ln_begin-$_ln_done) contains NO compile command -- a line whose FIRST word is an NDK wrapper and whose arguments carry a separate -c and -o. A wrapper named in prose, in a CC= assignment, or outside the block is not evidence"
        continue
    fi
    # EVERY observed command must match this ABI's wrapper exactly. One good
    # command does not rescue a mixed-API or mixed-architecture build.
    _want_cc="${triple}${api}-clang"
    _bad_cmd=$(printf '%s\n' "$_ossl_cmds" | grep -v "^${_want_cc}\(++\)\{0,1\}$" | sort -u | head -1)
    if [ -n "$_bad_cmd" ]; then
        fail "$abi.component.openssl" "OpenSSL invoked '$_bad_cmd' but this ABI declares '$_want_cc'; a mixed-API or mixed-architecture build is a failure, not a detail"
        continue
    fi
    _ossl_u=$(printf '%s\n' "$_ossl_cmds" | sort -u | grep -c . || true)
    pass "$abi.component.openssl" "$_ossl_n compile command(s) in its own block, all $_want_cc ($_ossl_u distinct executable(s))"

    # -- picotls / picoquic: their OWN generated CMake cache -----------------
    _compfail=0
    for comp in picotls picoquic; do
        _cache="$adir/cmake/$comp.cache"
        if [ ! -f "$_cache" ]; then
            fail "$abi.component.$comp" "no staged CMake cache at $_cache, so the ABI/API/toolchain it was configured with is unproven"
            _compfail=1; continue
        fi
        _bad=""
        # Release, warnings-as-errors, and -- for picotls ONLY -- the assertion
        # override the pinned source forces: -UNDEBUG must come AFTER the NDK's
        # own -DNDEBUG, because the last definition on the command line wins.
        # picoquic must carry no such override.
        _rel=$(grep -m1 -E '^CMAKE_C_FLAGS_RELEASE:[A-Z]*=' "$_cache" | sed 's/^[^=]*=//' || true)
        case $comp in
            picotls)
                case $_rel in
                    *-DNDEBUG*-UNDEBUG*) ;;
                    *) _bad="CMAKE_C_FLAGS_RELEASE is '$_rel'; picotls requires -UNDEBUG AFTER the NDK's -DNDEBUG so the pinned source's assert-only variables stay used" ;;
                esac ;;
            picoquic)
                case $_rel in
                    *-UNDEBUG*) _bad="CMAKE_C_FLAGS_RELEASE is '$_rel'; picoquic takes ordinary Release semantics and must carry no assertion override" ;;
                    *) ;;
                esac ;;
        esac
        for kv in "ANDROID_ABI:$abi" "ANDROID_PLATFORM:android-$api" \
                  "CMAKE_BUILD_TYPE:Release" "CMAKE_C_FLAGS:-Werror" \
                  "CMAKE_TOOLCHAIN_FILE:$ndk_root_here/build/cmake/android.toolchain.cmake"; do
            [ -z "$_bad" ] || break
            _k=${kv%%:*}; _want=${kv#*:}
            _n=$(grep -cE "^$_k:[A-Z]*=" "$_cache" || true)
            if [ "${_n:-0}" -ne 1 ]; then
                _bad="$_k declared $_n time(s), expected exactly once"; break
            fi
            _got=$(grep -m1 -E "^$_k:[A-Z]*=" "$_cache" | sed 's/^[^=]*=//')
            if [ "$_got" != "$_want" ]; then
                _bad="$_k is '$_got', expected '$_want'"; break
            fi
        done
        if [ -n "$_bad" ]; then
            fail "$abi.component.$comp" "configured against the wrong target: $_bad"
            _compfail=1; continue
        fi
        pass "$abi.component.$comp" "configured ABI $abi, android-$api, pinned NDK toolchain file"
    done
    [ "$_compfail" -eq 0 ] || continue
    # CROSS-HERMETIC. Host pkg-config/headers/libraries must never satisfy
    # target dependency discovery: picotls found Homebrew brotli and compiled
    # certificate_compression.c into an ANDROID archive with no Android brotli
    # anywhere, which is precisely the unresolved-symbol class gate B rejects.
    _hostpat='/opt/homebrew|/usr/local/(include|lib|Cellar)|/opt/local/|/Library/Developer/CommandLineTools|/Applications/Xcode'
    _hostlog=$(grep -nE -- "-I($_hostpat)|-L($_hostpat)" "$stage_log" | head -1)
    if [ -n "$_hostlog" ]; then
        fail "$abi.hermetic" "the cross-build consumed a HOST include/library path: $(printf '%s' "$_hostlog" | grep -oE "($_hostpat)[^ \"';]*" | head -1)"
        continue
    fi
    _ncache=$(find "$adir/cmake" -name '*.cache' -type f 2>/dev/null | grep -c . || true)
    if [ "${_ncache:-0}" -eq 0 ]; then
        fail "$abi.hermetic" "no staged CMake caches at $adir/cmake: dependency discovery cannot be audited, so cross-hermeticity is unproven"
        continue
    fi
    # Only DEPENDENCY DISCOVERY counts as contamination. The host cmake, gmake
    # and pkg-config BINARIES are legitimate -- they run on the host by
    # definition -- and so are unused install destinations. What must never be
    # host-backed is a resolved include dir, library, or flag for the TARGET,
    # so the key family is named explicitly rather than matching any host path.
    _depkey='_(INCLUDE_DIR|INCLUDE_DIRS|INCLUDEDIR|LIBRARY|LIBRARIES|LIBDIR|LIBRARY_DIRS|CFLAGS|CFLAGS_I|LDFLAGS|LIBS|LIBS_L|LIBS_PATHS|ROOT_DIR)'
    _hostdep=$(find "$adir/cmake" -name '*.cache' -type f -exec \
                 grep -hE "^[A-Za-z0-9_]*$_depkey:[A-Z]*=.*($_hostpat)" {} + 2>/dev/null | head -1)
    if [ -n "$_hostdep" ]; then
        fail "$abi.hermetic" "a staged CMake cache resolved a TARGET dependency to a HOST path: $(printf '%s' "$_hostdep" | cut -c1-90)"
        continue
    fi
    pass "$abi.hermetic" "no host include/library path in the build log or the $_ncache staged CMake cache(s)"
    _probewarn=$(grep -inE 'warning:|warning \[' "$log" | head -1)
    if [ -n "$_probewarn" ]; then
        fail "$abi.warnings" "the probe emitted a diagnostic: $(printf '%s' "$_probewarn" | cut -c1-80)"
        continue
    fi
    pass "$abi.buildlog" "named completion for openssl, picotls, picoquic; no diagnostics"
done

finish
