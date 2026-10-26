#!/bin/sh
# Oracle self-test for tools/check-jni-topology.sh.
#
# Builds the disposable synthetic stage (tools/mk-topology-fixture.sh), proves
# the checker is GREEN on it for both ABIs, then perturbs one fact at a time and
# requires the INTENDED NAMED oracle to fire. A mutant caught by a different
# oracle is a MISS, not a kill: an aggregate failure count is not evidence.
#
# Every case is named in the EXPECTED ledger below. Setup, mutation, invocation
# and cleanup are each guarded: a failed setup is a NAMED failure, never a
# silently skipped shell chain (chats/libmoq/1153 R7). The run is green only if
# the executed set equals the ledger exactly, with no duplicate and no miss.
#
# Everything lives inside ONE unpredictable task-owned directory; only that
# exact path is removed. No accepted artifact is mutated, even temporarily.
set -u
LC_ALL=C; export LC_ALL

here=$(cd "$(dirname "$0")" && pwd) || { echo "cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)        || { echo "cannot resolve bindings/kotlin" >&2; exit 2; }
chk="$here/check-jni-topology.sh"
mkf="$here/mk-topology-fixture.sh"

for t in awk sed grep sort uniq tr cp mv rm mkdir mkfifo ln chmod sh printf \
         mktemp find cmp diff wc sleep kill readlink od dd cat date ps \
         dirname basename; do
    command -v "$t" >/dev/null 2>&1 || { echo "selftest: required tool not found: $t" >&2; exit 2; }
done
[ -x "$chk" ] || { echo "selftest: missing or non-executable $chk" >&2; exit 2; }
[ -x "$mkf" ] || { echo "selftest: missing or non-executable $mkf" >&2; exit 2; }
# shellcheck source=lib-declaration.sh
. "$here/lib-declaration.sh"
command -v sha256sum >/dev/null 2>&1 || command -v shasum >/dev/null 2>&1 \
    || { echo "selftest: no SHA-256 hasher is available" >&2; exit 2; }
repo_root=$(cd "$root/../.." && pwd) || { echo "selftest: cannot resolve the worktree root" >&2; exit 2; }

# A safety net over the PINNED toolchain: this suite installs tool shims, and a
# shim written through a symlink would destroy a real NDK binary. A CONTENT and
# ALIAS identity is recorded here and re-checked in the exit guard.
_ndkbin=$(dirname "$(sed -n 's/^ndk.cc=//p' "$root/.native/arm64-v8a/provenance.properties")")

# ndk_fingerprint <bin-root>
#
# A CONTENT identity for the pinned toolchain, taken over a GIVEN root so the
# mutants can exercise the real function on a disposable miniature copy rather
# than a look-alike (chats/libmoq/1159 F3).
#
# Each declared entry records: its type, the link spelling when it is a symlink,
# the FULLY RESOLVED final target, and that target's SHA-256. Recording only the
# link spelling misses a change to the target's bytes; recording only the bytes
# misses an alias re-pointed at an identical file. Two entries resolving to the
# same target each record it, so a duplicate target is not collapsed.
# fp_hash <file> -- FAILS CLOSED. A pipeline's status is its last command's, so
# `sha256sum f | awk` returned 0 with an empty hash when the file was unreadable
# (chats/libmoq/1161 F3). The producer is run on its own, and the result must be
# a well-formed SHA-256.
fp_hash() {
    _fh=$1
    [ -f "$_fh" ] && [ -r "$_fh" ] || return 1
    if command -v sha256sum >/dev/null 2>&1; then
        _raw=$(sha256sum "$_fh") || return 1
    else
        _raw=$(shasum -a 256 "$_fh") || return 1
    fi
    _hv=${_raw%% *}
    printf '%s' "$_hv" | grep -Eq '^[0-9a-f]{64}$' || return 1
    printf '%s' "$_hv"
}

# fp_resolve <path> -- the FINAL canonical target of a symlink chain, resolved
# with physical directory semantics. Concatenating only the first link's
# spelling onto a resolved parent stops at an intermediate, so a retargeted
# intermediate was invisible (1161 F3).
fp_resolve() {
    _fp=$1; _fn=0
    while [ -L "$_fp" ]; do
        _fn=$((_fn + 1)); [ "$_fn" -le 64 ] || return 1
        _ft=$(readlink "$_fp") || return 1
        case $_ft in
            /*) _fp=$_ft ;;
            *)  _fd=$(cd -P "$(dirname "$_fp")" 2>/dev/null && pwd -P) || return 1
                _fp="$_fd/$_ft" ;;
        esac
    done
    [ -e "$_fp" ] || return 1
    _fd=$(cd -P "$(dirname "$_fp")" 2>/dev/null && pwd -P) || return 1
    printf '%s/%s' "$_fd" "$(basename "$_fp")"
}

# ndk_fingerprint <bin-root> -- a CONTENT and ALIAS identity, or FAILURE.
#
# Each declared entry records its type, the link spelling when it is a symlink,
# the fully resolved FINAL target, and that target's SHA-256. Two entries
# resolving to the same target each record it. An empty inventory, an
# unresolved target, or a hash failure makes the whole operation fail -- an
# empty identity must never be able to compare equal.
ndk_fingerprint() {
    _fb=$1
    [ -d "$_fb" ] || return 1
    _fcount=0
    for _t in "$_fb"/llvm-* "$_fb"/clang "$_fb"/clang++ "$_fb"/ld.lld; do
        [ -e "$_t" ] || [ -L "$_t" ] || continue
        _fcount=$((_fcount + 1))
        _nm=${_t##*/}
        _tgt=$(fp_resolve "$_t") || return 1
        _h=$(fp_hash "$_tgt") || return 1
        if [ -L "$_t" ]; then
            printf '%s link %s final %s %s\n' "$_nm" "$(readlink "$_t")" "$_tgt" "$_h"
        else
            printf '%s file final %s %s\n' "$_nm" "$_tgt" "$_h"
        fi
    done
    [ "$_fcount" -gt 0 ] || return 1
    return 0
}

NDK_BEFORE=$(ndk_fingerprint "$_ndkbin") \
    || { echo "selftest: cannot fingerprint the pinned toolchain at $_ndkbin" >&2; exit 2; }

work=$(mktemp -d "${TMPDIR:-/tmp}/moq5-topology-selftest-XXXXXXXX") \
    || { echo "selftest: cannot create a task-owned temporary directory" >&2; exit 2; }
# ONE exit guard: it verifies the pinned toolchain is unchanged and then
# cleans up, PRESERVING the status it was entered with. An interrupt or an
# early failure after a mutation therefore still reaches the NDK check.
# shellcheck disable=SC2329  # invoked from the traps below
on_exit() {
    _st=$?
    if ! _after=$(ndk_fingerprint "$_ndkbin"); then
        printf 'FATAL: the pinned NDK toolchain could not be fingerprinted at exit\n' >&2
        [ "$_st" -eq 0 ] && _st=1
        _after="<unfingerprintable>"
    fi
    if [ "$NDK_BEFORE" != "$_after" ]; then
        printf 'FATAL: the pinned NDK toolchain changed during this run\n' >&2
        printf '%s\n' "$NDK_BEFORE" > "${work:-/tmp}/ndk.before" 2>/dev/null
        printf '%s\n' "$_after"    > "${work:-/tmp}/ndk.after"  2>/dev/null
        diff "${work:-/tmp}/ndk.before" "${work:-/tmp}/ndk.after" 2>/dev/null | sed -n '1,10p' >&2
        [ "$_st" -eq 0 ] && _st=1
    fi
    [ -n "${work:-}" ] && rm -rf -- "$work"
    exit "$_st"
}
trap 'on_exit' EXIT
trap 'exit 130' HUP INT TERM

# ---- the exact case ledger --------------------------------------------------
EXPECTED="base-green real-tree-green
extra-export dup-jni sole-versioned-export qlog-standin non-pic extra-needed
digest-content digest-restore product-digest-red product-digest-pass
extra-abi-dir top-level-file top-level-symlink special-entry unreferenced-license
elf-parser-failure extra-dso versioned-dso hidden-elf wrong-machine
fake-strip substitute-stripped dso-hash-mismatch map-hash-mismatch receipt-hash-mismatch
wrong-member omitted-witness foreign-moq-archive duplicate-basename empty-moq-archive
wrong-machine-member archive-tool-failure zero-member-archive impostor-runtime
runtime-hash-mismatch omitted-needed wrong-provenance wrong-archive-hash
codec-member-omitted codec-identity-missing codec-archive-hash outsider-archive
wrong-dependency-hash wrong-log-dependency-hash
qlog-wrong-member qlog-log-origin rsp-pinned-log rsp-pinned-ssl
prov-malformed-key prov-extra-key
missing-json-notice arbitrary-source extra-field duplicate-row license-escape license-payload
log-incomplete log-warning log-no-map relative-escape response-escape map-malformed
unreadable-stage fixture-refuses-existing
audit-archive-substitution provenance-trailing-space provenance-non-ascii
license-hardlink-outside outside-version-script outside-response-file
unknown-path-option receipt-scratch-root
receipt-arbitrary-cwd receipt-cwd-outside-buildroot buildroot-outside-stage
symlink-dotdot-escape leaf-symlink-escape nested-unknown-option
unmanifested-source-input excluded-local-properties
rsp-empty-substitution rsp-same-length-substitution rsp-undeclared-nested
rsp-declared-unreferenced rsp-read-failure rsp-declaration-hash
watchdog-terminates watchdog-transparent watchdog-no-child-leak
chk-invocation-coverage watchdog-covers-special-shapes
fp-chain-intermediate fp-hash-failure fp-empty-root
rsp-snapshot-race rsp-hasher-portability restamp-hash-failure
fp-same-size-target fp-alias-retarget fp-target-behind-alias fp-exit-guard
preflight-readlink product-needed-authority
input-file-cmakelists input-file-build-gradle input-file-settings-gradle
input-file-gradle-properties input-file-gradlew input-file-gradlew-bat

input-family-missing input-family-core input-family-service input-family-media
input-family-adapters input-family-cmake input-family-tools input-family-gradle
producer-readelf producer-nm producer-objdump producer-ar missing-tool-preflight
runtime-query-failure-real runtime-query-malformed impostor-runtime-real
product-rejects-repo product-rejects-inputs-root
setup-mkstage-fails setup-copystage-fails copystage-hash-failure
ptls-header-changed ptls-archive-changed ptls-entry-missing ptls-entry-extra
ptls-wrong-abi ptls-manifest-unbound ptls-cmake-selection ptls-isystem-order
ptls-space-path ptls-openssl-only ptls-cc-malformed ptls-cc-ambiguous
ptls-cc-unmodelled
cli-missing-value cli-duplicate-option cli-unknown-option cli-two-roots
ledger-self-check"

ran=""; kills=0; misses=0
record() { ran="$ran $1"; }
ok()  { kills=$((kills + 1));  record "$1"; printf 'KILL[%s]: %s\n' "$1" "$2"; }
bad() { misses=$((misses + 1)); record "$1"; printf 'MISS[%s]: %s\n' "$1" "$2"; }

# ---- a real watchdog around every checker invocation -------------------------
# 1158 claimed this existed; it did not (chats/libmoq/1159 F2). Without it a
# reintroduced loop wedges the suite instead of failing it. No host `timeout`
# is used: that executable is not portable.
RUN_DEADLINE=${RUN_DEADLINE:-120}
GUARD_TIMEOUT=0

# kill_tree <pid> -- SIGKILL a process and every descendant. Killing only the
# shell we spawned leaves a checker blocked in an external producer running
# (chats/libmoq/1161 F2). `ps -Ao pid,ppid` is the portable inventory here;
# where it is unavailable the direct kill is all that is possible, and the
# child-leak ledger case is what would surface that.
kill_tree() {
    # ITERATIVE on purpose. POSIX sh has no function locals, so a recursive
    # version overwrites its own variable: after descending into a child the
    # outer frame kills the child again and never the intended target, and the
    # watchdog then waits forever on a process it believed it had killed.
    #
    # The scan-and-kill is REPEATED until a scan finds no descendant. A single
    # pass races against a target that spawns after the scan, which leaves a
    # live grandchild behind (chats/libmoq/1163 F1).
    _kroot=$1
    _kround=0
    while [ "$_kround" -lt 8 ]; do
        _kround=$((_kround + 1))
        _klist=$_kroot
        _kfront=$_kroot
        _kdepth=0
        while [ -n "$_kfront" ] && [ "$_kdepth" -lt 32 ]; do
            _kdepth=$((_kdepth + 1))
            _knext=$(ps -Ao pid,ppid 2>/dev/null | awk -v ps="$_kfront" '
                BEGIN { n = split(ps, a, " "); for (i = 1; i <= n; i++) parent[a[i]] = 1 }
                parent[$2] { print $1 }' | tr '\n' ' ')
            [ -n "$_knext" ] || break
            _klist="$_klist $_knext"
            _kfront=$_knext
        done
        # descendants FIRST, so a dying parent cannot spawn a replacement
        _krev=""
        for _kp in $_klist; do _krev="$_kp $_krev"; done
        for _kp in $_krev; do kill -9 "$_kp" 2>/dev/null; done
        [ "$_kdepth" -le 1 ] && [ -z "${_knext:-}" ] && break
    done
    return 0
}

# pid_gone <pid> -- true when the pid no longer names a LIVE process. A killed
# child whose parent was killed first stays visible as a zombie until it is
# reparented and reaped, and `kill -0` succeeds on a zombie -- which made the
# child-leak assertion fail intermittently under load (chats/libmoq/1163 F1).
pid_gone() {
    kill -0 "$1" 2>/dev/null || return 0
    _st=$(ps -o state= -p "$1" 2>/dev/null | tr -d ' ')
    case $_st in
        ""|Z*) return 0 ;;
        *)     return 1 ;;
    esac
}

# pids_gone_within <seconds> <pid...> -- poll until every pid is gone.
pids_gone_within() {
    _lim=$1; shift
    _n=0
    while [ "$_n" -le "$_lim" ]; do
        _all=1
        for _q in "$@"; do pid_gone "$_q" || { _all=0; break; }; done
        [ "$_all" -eq 1 ] && return 0
        sleep 1
        _n=$((_n + 1))
    done
    return 1
}

# guarded_run <out-file> <path-override|-> <cmd...>
#
# The PATH override applies to the CHILD ONLY. Exporting it around the whole
# call starved the watchdog's own `sleep` under a thin PATH, so the guard broke
# exactly where it was most needed.
guarded_run() {
    _gout=$1; _gpath=$2; shift 2
    GUARD_TIMEOUT=0
    _flag="$work/.timedout.$$"
    rm -f "$_flag"
    (
        # shellcheck disable=SC2030  # subshell-local by design: the override
        # must not reach the watchdog or the rest of the suite
        if [ "$_gpath" != "-" ]; then PATH=$_gpath; export PATH; fi
        exec "$@"
    ) > "$_gout" 2>&1 &
    _cpid=$!
    (
        _n=0
        while [ "$_n" -lt "$RUN_DEADLINE" ]; do
            kill -0 "$_cpid" 2>/dev/null || exit 0
            sleep 1
            _n=$((_n + 1))
        done
        kill -0 "$_cpid" 2>/dev/null || exit 0
        : > "$_flag"
        kill_tree "$_cpid"
    ) &
    _wpid=$!
    wait "$_cpid" 2>/dev/null
    _rc=$?
    kill "$_wpid" 2>/dev/null
    wait "$_wpid" 2>/dev/null
    if [ -f "$_flag" ]; then
        rm -f "$_flag"
        GUARD_TIMEOUT=1
        return 124
    fi
    return "$_rc"
}

# run_chk <out-file> <path-override|-> <checker args...>
#
# THE ONLY PLACE THE CHECKER IS EXECUTED. Every invocation -- including the ones
# with a temporary PATH and the malformed-CLI ones -- goes through here, so all
# of them inherit the watchdog. `chk-invocation-coverage` fails the run if any
# other executable invocation appears in this file (1161 F2).
run_chk() {
    _ro=$1; _rp=$2; shift 2
    guarded_run "$_ro" "$_rp" "$chk" "$@"
}

run() { _s=$1; shift; run_chk "$work/out" - --self-test "$@" "$_s"; }

# expect_fail <case> <stage> <tag> [extra checker args...]
# The named oracle must fire FIRST for its ABI; a mutant masked by an earlier
# oracle proves nothing about the one under test.
expect_fail() {
    _n=$1; _s=$2; _tag=$3; shift 3
    if run "$_s" "$@"; then bad "$_n" "the checker PASSED a stage that must be rejected"; return; fi
    [ "$GUARD_TIMEOUT" -eq 0 ] || { bad "$_n" "the checker TIMED OUT after ${RUN_DEADLINE}s instead of failing at [$_tag]"; return; }
    _first=$(grep -E '^FAIL\[' "$work/out" | sed -n '1p')
    case $_first in
        "FAIL[$_tag"*) ok "$_n" "${_first%%:*} fired first" ;;
        *)             bad "$_n" "expected the first failure at [$_tag...]; got: ${_first:-<none>}" ;;
    esac
}

# expect_fail_named <case> <stage> <tag> <reason>
# As expect_fail, and the first failure must also NAME its reason: the row
# alone cannot tell a missing codec archive from any other link failure.
expect_fail_named() {
    _n=$1; _s=$2; _tag=$3; _why=$4
    if run "$_s"; then bad "$_n" "the checker PASSED a stage that must be rejected"; return; fi
    [ "$GUARD_TIMEOUT" -eq 0 ] || { bad "$_n" "the checker TIMED OUT after ${RUN_DEADLINE}s instead of failing at [$_tag]"; return; }
    _first=$(grep -E '^FAIL\[' "$work/out" | sed -n '1p')
    case $_first in
        "FAIL[$_tag"*"$_why"*) ok "$_n" "${_first%%:*} fired first, naming $_why" ;;
        *) bad "$_n" "expected the first failure at [$_tag...] naming '$_why'; got: ${_first:-<none>}" ;;
    esac
}

# expect_fail_both <case> <stage> <abi-suffix-tag>
# For an ABI-sensitive property: the named oracle must fire for BOTH ABIs, so a
# two-ABI claim can never rest on one ABI reaching the oracle (1153 R6).
expect_fail_both() {
    _n=$1; _s=$2; _sfx=$3
    if run "$_s"; then bad "$_n" "the checker PASSED a stage that must be rejected"; return; fi
    [ "$GUARD_TIMEOUT" -eq 0 ] || { bad "$_n" "the checker TIMED OUT after ${RUN_DEADLINE}s instead of failing at [$_sfx]"; return; }
    _a=$(grep -cE "^FAIL\[arm64-v8a\.$_sfx\]" "$work/out")
    _x=$(grep -cE "^FAIL\[x86_64\.$_sfx\]" "$work/out")
    if [ "$_a" -ge 1 ] && [ "$_x" -ge 1 ]; then
        ok "$_n" "[$_sfx] fired independently for both ABIs"
    else
        bad "$_n" "expected [$_sfx] on both ABIs; arm64=$_a x86_64=$_x"
    fi
}

setup_fail() { bad "$1" "SETUP FAILED: $2"; }

# mkstage <case> <dirname> [variant] -> sets STAGE_OUT, returns non-zero on
# failure. NOT command substitution: setup_fail must mutate the parent's ledger,
# and a subshell would discard it (chats/libmoq/1155 R9).
STAGE_OUT=""
mkstage() {
    _c=$1; _d=$2; _v=${3:-base}
    STAGE_OUT=""
    if ! "$mkf" "$work/$_d" "$_v" > "$work/mkout" 2>"$work/mkerr"; then
        setup_fail "$_c" "fixture build failed: $(sed -n '1p' "$work/mkerr")"
        return 1
    fi
    STAGE_OUT=$(sed -n '$p' "$work/mkout")
    [ -n "$STAGE_OUT" ] || { setup_fail "$_c" "the fixture builder printed no stage path"; return 1; }
    return 0
}


# copystage <case> <dirname> -> echoes a disposable copy of the base CONTAINER's
# stage, or "" on failure.
#
# The link map and provenance carry ABSOLUTE self-references, so a raw copy is
# no longer internally consistent and every downstream oracle would fail on the
# copy rather than on the mutant. The copy is therefore re-pointed at its own
# container and the map/receipt hashes are restamped, so the copy starts GREEN
# and each case's own mutation is the only difference.
copystage() {
    _c=$1; _d=$2
    STAGE_OUT=""
    if ! cp -R "$work/base" "$work/$_d" 2>/dev/null; then
        setup_fail "$_c" "cannot copy the base fixture"; return 1
    fi
    _st="$work/$_d/stage"
    # Some self-references are recorded in their physical form (pwd -P), which
    # differs from $work when TMPDIR is a symlink or ends in a slash, so both
    # spellings of the base are re-pointed.
    _wbp=$(cd "$work/base" && pwd -P) && _wdp=$(cd "$work/$_d" && pwd -P) \
        || { setup_fail "$_c" "cannot resolve the physical fixture paths"; return 1; }
    for _abi in arm64-v8a x86_64; do
        for _f in "$_st/$_abi/link.map" "$_st/$_abi/topology.provenance" \
                  "$_st/$_abi/topology.log" "$_st/$_abi/audit-inputs/link.rsp" \
                  "$_st/$_abi/responses.manifest" \
                  "$_st/$_abi/audit-inputs/CMakeCache.txt" \
                  "$_st/$_abi/audit-inputs/compile_commands.json"; do
            [ -f "$_f" ] || { setup_fail "$_c" "the copied stage lacks $_f"; return 1; }
            if ! sed -e "s|$work/base|$work/$_d|g" -e "s|$_wbp|$_wdp|g" "$_f" > "$_f.n"; then
                setup_fail "$_c" "cannot re-point $_f"; return 1
            fi
            if ! mv "$_f.n" "$_f"; then
                setup_fail "$_c" "cannot replace $_f"; return 1
            fi
        done
        # The response bytes were re-pointed, so each declared row's hash must be
        # recomputed before the declaration itself is re-stamped.
        #
        # Every hash is CAPTURED, its status CHECKED, and the digest VALIDATED
        # before it is written. `printf ... "$(hash f)"` observes printf's
        # status, not the hasher's, so a producer failure used to write an empty
        # field and let copystage return success -- the setup fault then
        # surfaced at an artifact oracle (chats/libmoq/1165).
        if ! awk -F"$(printf '\t')" '{ print $1 }' "$_st/$_abi/responses.manifest" > "$_st/$_abi/rm.paths"; then
            setup_fail "$_c" "cannot list the declared response paths for $_abi"; return 1
        fi
        if ! : > "$_st/$_abi/rm.new"; then
            setup_fail "$_c" "cannot start the rewritten response declaration for $_abi"; return 1
        fi
        while IFS= read -r _rp; do
            [ -n "$_rp" ] || continue
            if ! _rh2=$(decl_sha256 "$_rp"); then
                setup_fail "$_c" "cannot hash the declared response file $_rp"; return 1
            fi
            if ! decl_is_sha256 "$_rh2"; then
                setup_fail "$_c" "the recomputed hash of $_rp is not a SHA-256"; return 1
            fi
            if ! printf '%s\t%s\n' "$_rp" "$_rh2" >> "$_st/$_abi/rm.new"; then
                setup_fail "$_c" "cannot write the response declaration row for $_rp"; return 1
            fi
        done < "$_st/$_abi/rm.paths"
        if ! mv "$_st/$_abi/rm.new" "$_st/$_abi/responses.manifest"; then
            setup_fail "$_c" "cannot replace the response declaration for $_abi"; return 1
        fi
        rm -f "$_st/$_abi/rm.paths"
        for _pair in "map:$_st/$_abi/link.map" \
                     "receipt:$_st/$_abi/topology.log" \
                     "responses:$_st/$_abi/responses.manifest"; do
            _key=${_pair%%:*}; _file=${_pair#*:}
            if ! _hv=$(decl_sha256 "$_file"); then
                setup_fail "$_c" "cannot hash $_file for the $_key.sha256 restamp"; return 1
            fi
            if ! decl_is_sha256 "$_hv"; then
                setup_fail "$_c" "the recomputed $_key.sha256 for $_abi is not a SHA-256"; return 1
            fi
            case $_key in
                map)       _mh=$_hv ;;
                receipt)   _rh=$_hv ;;
                responses) _sh=$_hv ;;
            esac
        done
        if ! sed -e "s|^map.sha256=.*|map.sha256=$_mh|" -e "s|^receipt.sha256=.*|receipt.sha256=$_rh|" \
               -e "s|^responses.sha256=.*|responses.sha256=$_sh|" \
               "$_st/$_abi/topology.provenance" > "$_st/$_abi/tp.n"; then
            setup_fail "$_c" "cannot restamp the copied provenance"; return 1
        fi
        if ! mv "$_st/$_abi/tp.n" "$_st/$_abi/topology.provenance"; then
            setup_fail "$_c" "cannot replace the copied provenance"; return 1
        fi
    done
    STAGE_OUT=$_st
    return 0
}

# shellcheck disable=SC2329  # invoked indirectly through `guard`
# shellcheck disable=SC2329  # invoked indirectly through `guard`
mapedit() { sed "$3" "$1/$2/link.map" > "$1/$2/link.map.n" && mv "$1/$2/link.map.n" "$1/$2/link.map"; }
# shellcheck disable=SC2329  # invoked indirectly through `guard`
provedit() { sed "$3" "$1/$2/topology.provenance" > "$1/$2/tp.n" && mv "$1/$2/tp.n" "$1/$2/topology.provenance"; }
# shellcheck disable=SC2329  # invoked indirectly through `guard`
manedit() { sed "$3" "$1/$2/licenses.manifest" > "$1/$2/lm.n" && mv "$1/$2/lm.n" "$1/$2/licenses.manifest"; }
# shellcheck disable=SC2329  # invoked indirectly through `guard`
logedit() { sed "$3" "$1/$2/topology.log" > "$1/$2/tl.n" && mv "$1/$2/tl.n" "$1/$2/topology.log"; }

# shellcheck disable=SC2329  # invoked indirectly through `guard`
# restamp <stage> <abi> -- after editing the receipt or provenance
restamp() {
    _rh=$(decl_sha256 "$1/$2/topology.log") || return 1
    decl_is_sha256 "$_rh" || return 1
    sed "s|^receipt.sha256=.*|receipt.sha256=$_rh|" "$1/$2/topology.provenance" > "$1/$2/tp.n" \
        && mv "$1/$2/tp.n" "$1/$2/topology.provenance"
}

guard() { # guard <case> <description> <command...>
    _c=$1; _desc=$2; shift 2
    if "$@"; then return 0; fi
    setup_fail "$_c" "$_desc"
    return 1
}

echo "== baseline =="
mkstage base-green base || { echo "the base fixture is required" >&2; exit 1; }
base=$STAGE_OUT
if run "$base"; then
    _want="provenance onedso identity exports needed link qlog pic closure strip licenses hermetic alignment ptls"
    _wn=$(printf '%s\n' "$_want" | tr ' ' '\n' | awk 'NF { n++ } END { print n + 0 }')
    _bad=""
    for abi in arm64-v8a x86_64; do
        _n=$(grep -cE "^PASS\[$abi\." "$work/out")
        [ "$_n" -eq "$_wn" ] || _bad="${_bad:+$_bad }$abi has $_n PASS rows, expected exactly $_wn"
        for o in $_want; do
            _c=$(grep -cE "^PASS\[$abi\.$o\]" "$work/out")
            [ "$_c" -eq 1 ] || _bad="${_bad:+$_bad }$abi.$o appears $_c time(s), expected exactly once"
        done
    done
    _tot=$(grep -cE '^PASS\[' "$work/out")
    [ "$_tot" -eq $((_wn * 2)) ] || _bad="${_bad:+$_bad }$_tot PASS rows in total, expected exactly $((_wn * 2))"
    if [ -z "$_bad" ]; then
        ok base-green "exactly $_wn PASS rows per ABI, each exactly once, $_tot in total, 0 FAIL"
    else
        bad base-green "$_bad"
    fi
else
    sed -n '1,30p' "$work/out"
    bad base-green "the synthetic stage is not green"
fi

echo "== the real tree carries a durable product stage, and it is GREEN =="
# Phase 0.3 RED asserted the opposite: the stage was absent, so the real tree
# failed for exactly that reason. GREEN-A produced it at the canonical path, so
# the assertion is now that the product topology audits clean end to end.
# MOQ5_REAL_TOPOLOGY_STAGE names a successor stage built beside the durable
# one; unset, the checker audits its own default.
if run_chk "$work/red" - ${MOQ5_REAL_TOPOLOGY_STAGE:+"$MOQ5_REAL_TOPOLOGY_STAGE"}; then
    ok real-tree-green "the durable product stage passes every oracle on both ABIs"
else
    _f=$(grep -cE '^FAIL\[' "$work/red")
    sed -n '1,12p' "$work/red"
    bad real-tree-green "the durable product stage is not green; $_f failure(s)"
fi

echo "== relink mutants: the artifact itself is wrong =="
for v in extra-export dup-jni qlog-standin non-pic extra-needed; do
    mkstage "$v" "v-$v" "$v" || continue
    s=$STAGE_OUT
    case $v in
        extra-export|dup-jni) expect_fail_both "$v" "$s" exports ;;
        qlog-standin)         expect_fail_both "$v" "$s" qlog ;;
        non-pic)              expect_fail_both "$v" "$s" pic ;;
        extra-needed)         expect_fail_both "$v" "$s" needed ;;
    esac
done

# A SOLE versioned JNI_OnLoad: one row, right type/bind/visibility, wrong
# spelling. A parser that strips @VERSION before comparing accepts this (R5).
copystage sole-versioned-export m-solever && s=$STAGE_OUT && {
    ok_setup=1
    for abi in arm64-v8a x86_64; do
        guard sole-versioned-export "cannot stage the versioned-only export for $abi" \
            cp "$work/v-dup-jni/stage/$abi/package/libmoq5_jni.so" "$s/$abi/package/libmoq5_jni.so" || { ok_setup=0; break; }
    done
    [ "$ok_setup" -eq 1 ] && expect_fail_both sole-versioned-export "$s" exports
}

echo "== the source-content digest =="
# Flip one byte in an EXISTING declared input, changing no path at all.
copystage digest-content m-digest && s=$STAGE_OUT && {
    _in="$work/m-digest/.inputs/core/fixture.c"
    if guard digest-content "the fixture input is absent" test -f "$_in"; then
        # the copied stage still points at the ORIGINAL input root, so retarget it
        for abi in arm64-v8a x86_64; do
            sed "s|^libmoq.inputs.root=.*|libmoq.inputs.root=$work/m-digest/.inputs|" \
                "$s/$abi/topology.provenance" > "$s/$abi/tp.n" && mv "$s/$abi/tp.n" "$s/$abi/topology.provenance"
        done
        printf 'int moq_fixture_input(void){return 2;}\n' > "$_in"
        expect_fail_both digest-content "$s" provenance
        # control: restore the exact bytes and the same stage must pass again
        printf 'int moq_fixture_input(void){return 1;}\n' > "$_in"
        if run "$s"; then ok digest-restore "restoring the byte restores the green"
        else bad digest-restore "the restored stage did not pass: $(grep -E '^FAIL' "$work/out" | sed -n '1p')"; fi
    fi
}

# The PRODUCT code path, exercised over a task-owned copy of the input root.
mkstage product-digest-red pm product-mode && s=$STAGE_OUT && {
    _pin="$work/pm/.inputs"
    printf 'int moq_fixture_input(void){return 3;}\n' > "$_pin/core/fixture.c"
    expect_fail product-digest-red "$s" arm64-v8a.provenance --repo "$_pin"
    printf 'int moq_fixture_input(void){return 1;}\n' > "$_pin/core/fixture.c"
    # With the bytes restored the PRODUCT digest branch must accept the stage.
    # The case is about that branch, so it asserts the provenance oracle's own
    # verdict rather than wherever the run happens to stop afterwards -- a later
    # failure belongs to a different contract and must not stand in for this one.
    run "$s" --repo "$_pin"
    if grep -qE '^PASS\[arm64-v8a\.provenance\]' "$work/out"; then
        ok product-digest-pass "the product digest branch accepts the restored bytes"
    else
        bad product-digest-pass "the product digest branch rejected restored bytes: $(grep -m1 -E '^FAIL\[arm64-v8a\.provenance\]' "$work/out")"
    fi
}

echo "== stage totality from the topology root down =="
copystage extra-abi-dir m-abidir && s=$STAGE_OUT \
  && guard extra-abi-dir "cannot create the extra ABI directory" mkdir -p "$s/armeabi-v7a" \
  && expect_fail extra-abi-dir "$s" stage

copystage top-level-file m-topfile && s=$STAGE_OUT \
  && guard top-level-file "cannot create the top-level file" cp "$s/arm64-v8a/link.map" "$s/README" \
  && expect_fail top-level-file "$s" stage

copystage top-level-symlink m-topsym && s=$STAGE_OUT \
  && guard top-level-symlink "cannot create the top-level symlink" ln -s arm64-v8a "$s/alias" \
  && expect_fail top-level-symlink "$s" stage

copystage special-entry m-fifo && s=$STAGE_OUT \
  && guard special-entry "cannot create the FIFO" mkfifo "$s/arm64-v8a/licenses/pipe" \
  && expect_fail special-entry "$s" stage

copystage unreferenced-license m-unref && s=$STAGE_OUT \
  && guard unreferenced-license "cannot create the stray payload" cp "$s/arm64-v8a/licenses/openssl.LICENSE" "$s/arm64-v8a/licenses/EXTRA.LICENSE" \
  && expect_fail unreferenced-license "$s" arm64-v8a.stage

# ELF magic with no readable header: the producer must be guarded separately
# from the parser, so a failed readelf cannot turn an ELF into a non-ELF.
copystage elf-parser-failure m-elfparse && s=$STAGE_OUT && {
    printf '\177ELF' > "$s/arm64-v8a/diagnostic/libmoq5_jni.so"
    expect_fail elf-parser-failure "$s" arm64-v8a.onedso
}

copystage extra-dso m-extradso && s=$STAGE_OUT \
  && guard extra-dso "cannot add the extra DSO" cp "$s/arm64-v8a/package/libmoq5_jni.so" "$s/arm64-v8a/package/libextra.so" \
  && expect_fail extra-dso "$s" arm64-v8a.stage

copystage versioned-dso m-verdso && s=$STAGE_OUT \
  && guard versioned-dso "cannot add the versioned DSO" cp "$s/arm64-v8a/package/libmoq5_jni.so" "$s/arm64-v8a/package/libmoq5_jni.so.1" \
  && expect_fail versioned-dso "$s" arm64-v8a.stage

# An ELF hidden under a path the manifest allows: only content classification
# can reject it.
copystage hidden-elf m-hidden && s=$STAGE_OUT \
  && guard hidden-elf "cannot hide the ELF" cp "$s/arm64-v8a/package/libmoq5_jni.so" "$s/arm64-v8a/licenses/openssl.LICENSE" \
  && expect_fail hidden-elf "$s" arm64-v8a.onedso

echo "== identity, strip and the recorded artifact hashes =="
copystage wrong-machine m-wrongmach && s=$STAGE_OUT \
  && guard wrong-machine "cannot swap the machine" cp "$s/x86_64/package/libmoq5_jni.so" "$s/arm64-v8a/package/libmoq5_jni.so" \
  && expect_fail wrong-machine "$s" arm64-v8a.identity

copystage fake-strip m-fakestrip && s=$STAGE_OUT \
  && guard fake-strip "cannot publish the diagnostic as the package" cp "$s/arm64-v8a/diagnostic/libmoq5_jni.so" "$s/arm64-v8a/package/libmoq5_jni.so" \
  && expect_fail fake-strip "$s" arm64-v8a.strip

copystage substitute-stripped m-substrip && s=$STAGE_OUT \
  && guard substitute-stripped "cannot substitute the unrelated library" cp "$work/v-extra-export/stage/arm64-v8a/package/libmoq5_jni.so" "$s/arm64-v8a/package/libmoq5_jni.so" \
  && expect_fail substitute-stripped "$s" arm64-v8a.exports

for pair in dso-hash-mismatch:dso map-hash-mismatch:map receipt-hash-mismatch:receipt; do
    c=${pair%%:*}; k=${pair#*:}
    copystage "$c" "m-$c" || continue
    s=$STAGE_OUT
    if guard "$c" "cannot rewrite $k.sha256" sh -c \
        "sed 's|^$k.sha256=.*|$k.sha256=0000000000000000000000000000000000000000000000000000000000000000|' '$s/arm64-v8a/topology.provenance' > '$s/arm64-v8a/tp.n' && mv '$s/arm64-v8a/tp.n' '$s/arm64-v8a/topology.provenance'"; then
        expect_fail "$c" "$s" arm64-v8a.strip
    fi
done

echo "== archives: exact paths, real inspection =="

copystage wrong-member m-wrongmem && s=$STAGE_OUT \
  && guard wrong-member "cannot rewrite the map" mapedit "$s" arm64-v8a 's|libmoq-loc\.a|libmoq-playback.a|g' \
  && expect_fail wrong-member "$s" arm64-v8a.link

# The codec archive is a REQUIRED member of the closure: dropping its
# selection, its declared identity, or its hash each fails by name.
copystage codec-member-omitted m-codecmem && s=$STAGE_OUT \
  && guard codec-member-omitted "cannot drop the codec selection" sh -c "grep -v 'libmoq-codec-signaling\.a(' '$s/arm64-v8a/link.map' > '$s/arm64-v8a/link.map.n' && mv '$s/arm64-v8a/link.map.n' '$s/arm64-v8a/link.map'" \
  && expect_fail_named codec-member-omitted "$s" arm64-v8a.link "libmoq-codec-signaling.a: 0 selected path(s)"

copystage codec-identity-missing m-codecid && s=$STAGE_OUT \
  && guard codec-identity-missing "cannot drop the codec identity" provedit "$s" arm64-v8a '/^archive\.libmoq-codec-signaling\.a=/d' \
  && expect_fail_named codec-identity-missing "$s" arm64-v8a.provenance "missing provenance key: archive.libmoq-codec-signaling.a"

copystage codec-archive-hash m-codechash && s=$STAGE_OUT \
  && guard codec-archive-hash "cannot rewrite the codec hash" provedit "$s" arm64-v8a 's|^archive.libmoq-codec-signaling.a=.*|archive.libmoq-codec-signaling.a=0000000000000000000000000000000000000000000000000000000000000000|' \
  && expect_fail_named codec-archive-hash "$s" arm64-v8a.provenance "libmoq-codec-signaling.a hashes"

# An UNALLOWED archive contributing alongside the complete closure: every
# required archive is still selected once, so only the outsider rule can fire.
copystage outsider-archive m-outsider && s=$STAGE_OUT \
  && guard outsider-archive "cannot add the outsider" mapedit "$s" arm64-v8a '/libmoq-loc\.a(/{p;s|libmoq-loc\.a|libmoq-playback.a|;}' \
  && expect_fail_named outsider-archive "$s" arm64-v8a.link "contributed members: libmoq-playback.a"

copystage omitted-witness m-omitwit && s=$STAGE_OUT \
  && guard omitted-witness "cannot drop the witness" sh -c "grep -v 'libmoq-cmaf\.a(' '$s/arm64-v8a/link.map' > '$s/arm64-v8a/link.map.n' && mv '$s/arm64-v8a/link.map.n' '$s/arm64-v8a/link.map'" \
  && expect_fail omitted-witness "$s" arm64-v8a.link

# A FOREIGN archive carrying a required basename: the basename is right, the
# path is not. Only the exact-path pin rejects it.
copystage foreign-moq-archive m-foreign && s=$STAGE_OUT && {
    _fd="$work/m-foreign/foreign"
    if guard foreign-moq-archive "cannot stage the foreign archive" sh -c \
        "mkdir -p '$_fd' && cp '$s/arm64-v8a/audit-inputs/libmoq-core.a' '$_fd/libmoq-core.a'"; then
        guard foreign-moq-archive "cannot repoint the map" mapedit "$s" arm64-v8a "s|$s/arm64-v8a/audit-inputs/libmoq-core\.a|$_fd/libmoq-core.a|g" \
          && expect_fail foreign-moq-archive "$s" arm64-v8a.link
    fi
}

# The SAME required basename selected from two different paths: a sort -u on
# basenames collapses this into a pass.
copystage duplicate-basename m-dupbase && s=$STAGE_OUT && {
    _dd="$work/m-dupbase/dup"
    if guard duplicate-basename "cannot stage the duplicate" sh -c \
        "mkdir -p '$_dd' && cp '$s/arm64-v8a/audit-inputs/libmoq-loc.a' '$_dd/libmoq-loc.a'"; then
        _rec=$(grep -m1 -o "$s/arm64-v8a/audit-inputs/libmoq-loc\.a([^)]*):([^ ]*)" "$s/arm64-v8a/link.map")
        _new=$(printf '%s' "$_rec" | sed "s|$s/arm64-v8a/audit-inputs|$_dd|")
        if guard duplicate-basename "cannot append the duplicate record" sh -c \
            "printf '     0     0     8 %s\n' '$_new' >> '$s/arm64-v8a/link.map'"; then
            expect_fail duplicate-basename "$s" arm64-v8a.link
        fi
    fi
}

# redeclare <stage> <abi> <archive> -- keep the provenance hash consistent, so a
# mutation is judged by the ARCHIVE oracle rather than by the hash check that
# legitimately runs first.
# shellcheck disable=SC2329  # invoked indirectly through `guard`
redeclare() {
    _h=$(decl_sha256 "$1/$2/audit-inputs/$3") || return 1
    decl_is_sha256 "$_h" || return 1
    sed "s|^archive.$3=.*|archive.$3=$_h|" "$1/$2/topology.provenance" > "$1/$2/tp.n" \
        && mv "$1/$2/tp.n" "$1/$2/topology.provenance"
}

copystage empty-moq-archive m-emptyar && s=$STAGE_OUT \
  && guard empty-moq-archive "cannot empty the archive" sh -c ": > '$s/arm64-v8a/audit-inputs/libmoq-msf.a'" \
  && guard empty-moq-archive "cannot re-declare the hash" redeclare "$s" arm64-v8a libmoq-msf.a \
  && expect_fail empty-moq-archive "$s" arm64-v8a.provenance

# Non-empty but carrying NO members: passes the size check, so only the archive
# member inventory at the link stage can reject it.
copystage zero-member-archive m-zeromem && s=$STAGE_OUT \
  && guard zero-member-archive "cannot write the memberless archive" sh -c "printf '!<arch>\n' > '$s/arm64-v8a/audit-inputs/libmoq-loc.a'" \
  && guard zero-member-archive "cannot re-declare the hash" redeclare "$s" arm64-v8a libmoq-loc.a \
  && expect_fail zero-member-archive "$s" arm64-v8a.link

copystage wrong-machine-member m-wrongmem2 && s=$STAGE_OUT \
  && guard wrong-machine-member "cannot cross the ABIs" cp "$s/x86_64/audit-inputs/libmoq-cmaf.a" "$s/arm64-v8a/audit-inputs/libmoq-cmaf.a" \
  && guard wrong-machine-member "cannot re-declare the hash" redeclare "$s" arm64-v8a libmoq-cmaf.a \
  && expect_fail wrong-machine-member "$s" arm64-v8a.link

# A readable but corrupt archive: the archive TOOL fails, and that must be a
# named failure rather than an empty member inventory. Permission mode 000 is
# deliberately NOT the discriminator -- it is not unreadable to root (R8).
copystage archive-tool-failure m-corruptar && s=$STAGE_OUT \
  && guard archive-tool-failure "cannot corrupt the archive" sh -c "printf 'not an archive at all' > '$s/arm64-v8a/audit-inputs/libmoq-core.a'" \
  && guard archive-tool-failure "cannot re-declare the hash" redeclare "$s" arm64-v8a libmoq-core.a \
  && expect_fail archive-tool-failure "$s" arm64-v8a.link

echo "== the compiler runtime is ONE exact archive =="
# An impostor sitting under the pinned NDK root: accepted by a containment
# rule, rejected by the exact query (R3).
copystage impostor-runtime m-impostor && s=$STAGE_OUT && {
    _rt=$(sed -n 's/^compiler.runtime.path=//p' "$s/arm64-v8a/topology.provenance")
    _imp=$(dirname "$_rt")/libclang_rt.impostor-aarch64-android.a
    if guard impostor-runtime "cannot rewrite the map to the impostor" mapedit "$s" arm64-v8a "s|$_rt|$_imp|g"; then
        expect_fail impostor-runtime "$s" arm64-v8a.link
    fi
}

copystage runtime-hash-mismatch m-rthash && s=$STAGE_OUT \
  && guard runtime-hash-mismatch "cannot rewrite the runtime hash" sh -c \
      "sed 's|^compiler.runtime.sha256=.*|compiler.runtime.sha256=1111111111111111111111111111111111111111111111111111111111111111|' '$s/arm64-v8a/topology.provenance' > '$s/arm64-v8a/tp.n' && mv '$s/arm64-v8a/tp.n' '$s/arm64-v8a/topology.provenance'" \
  && expect_fail runtime-hash-mismatch "$s" arm64-v8a.link

# ---- a task-owned WRAPPED NDK: real sibling llvm-* tools, one shimmed -------
# This is what makes the compiler-runtime and producer negatives real: the
# toolchain is otherwise valid, so the named oracle is the only thing that can
# fail. The accepted Phase 0.2 stage is copied, never mutated.
DEPCOPY=""
make_depcopy() {
    _c=$1
    [ -z "$DEPCOPY" ] || return 0
    _ds="$work/depcopy"
    for abi in arm64-v8a x86_64; do
        mkdir -p "$_ds/$abi/lib" || { setup_fail "$_c" "cannot create the dependency copy"; return 1; }
        if ! cp "$root/.native/$abi"/lib/*.a "$_ds/$abi/lib/" 2>/dev/null; then
            setup_fail "$_c" "cannot copy the dependency archives"; return 1
        fi
        if ! cp "$root/.native/$abi/provenance.properties" "$_ds/$abi/"; then
            setup_fail "$_c" "cannot copy the Phase 0.2 provenance"; return 1
        fi
    done
    for rel in _src/picoquic/LICENSE _src/picotls/README.md _src/openssl/LICENSE.txt \
               _src/picotls/deps/cifra/COPYING _src/picotls/deps/micro-ecc/LICENSE.txt; do
        if ! mkdir -p "$_ds/$(dirname "$rel")"; then setup_fail "$_c" "cannot create $rel"; return 1; fi
        if ! cp "$root/.native/$rel" "$_ds/$rel"; then setup_fail "$_c" "cannot copy $rel"; return 1; fi
    done
    DEPCOPY=$_ds
    return 0
}

# wrap_ndk <case> <name> <shim-body-file> -> sets WRAP_BIN / WRAP_CC
WRAP_BIN=""; WRAP_CC=""
wrap_ndk() {
    _c=$1; _n=$2; _body=$3
    _realcc=$(sed -n 's/^ndk.cc=//p' "$root/.native/arm64-v8a/provenance.properties")
    _realbin=$(dirname "$_realcc")
    _wb="$work/wrap-$_n/toolchains/llvm/prebuilt/host/bin"
    mkdir -p "$_wb" || { setup_fail "$_c" "cannot create the wrapped bin"; return 1; }
    for t in "$_realbin"/llvm-*; do
        [ -e "$t" ] || continue
        ln -sf "$t" "$_wb/$(basename "$t")" || { setup_fail "$_c" "cannot link $(basename "$t")"; return 1; }
    done
    if ! ln -sf "$(dirname "$_realbin")/sysroot" "$(dirname "$_wb")/sysroot"; then
        setup_fail "$_c" "cannot link the sysroot"; return 1
    fi
    WRAP_CC="$_wb/$(basename "$_realcc")"
    rm -f "$WRAP_CC"
    cp "$_body" "$WRAP_CC" || { setup_fail "$_c" "cannot install the shim"; return 1; }
    chmod +x "$WRAP_CC" || { setup_fail "$_c" "cannot make the shim executable"; return 1; }
    WRAP_BIN=$_wb
    return 0
}

# shim_tool <case> <bin> <tool> <body>
# A wrapped bin holds SYMLINKS to the real NDK tools. Redirecting output onto a
# symlink writes through it and destroys the real binary, so the link is removed
# first and the target is re-checked before anything is written.
shim_tool() {
    _c=$1; _b=$2; _t=$3; _body=$4
    rm -f "$_b/$_t" || { setup_fail "$_c" "cannot remove the link for $_t"; return 1; }
    [ -L "$_b/$_t" ] && { setup_fail "$_c" "$_t is still a symlink after removal"; return 1; }
    printf '%s' "$_body" > "$_b/$_t" || { setup_fail "$_c" "cannot install the shim for $_t"; return 1; }
    [ -L "$_b/$_t" ] && { setup_fail "$_c" "$_t became a symlink"; return 1; }
    chmod +x "$_b/$_t" || { setup_fail "$_c" "cannot make the $_t shim executable"; return 1; }
    return 0
}

# use_wrapped <case> <stage> <shim-name> -> re-points both provenances at the shim
use_wrapped() {
    _c=$1; _st=$2
    for abi in arm64-v8a x86_64; do
        if ! sed "s|^ndk.cc=.*|ndk.cc=$WRAP_CC|" "$DEPCOPY/$abi/provenance.properties" > "$DEPCOPY/$abi/p.n"; then
            setup_fail "$_c" "cannot re-point the copied Phase 0.2 provenance"; return 1
        fi
        if ! mv "$DEPCOPY/$abi/p.n" "$DEPCOPY/$abi/provenance.properties"; then
            setup_fail "$_c" "cannot replace the copied Phase 0.2 provenance"; return 1
        fi
        if ! sed "s|^ndk.cc=.*|ndk.cc=$WRAP_CC|" "$_st/$abi/topology.provenance" > "$_st/$abi/tp.n"; then
            setup_fail "$_c" "cannot re-point the topology provenance"; return 1
        fi
        if ! mv "$_st/$abi/tp.n" "$_st/$abi/topology.provenance"; then
            setup_fail "$_c" "cannot replace the topology provenance"; return 1
        fi
    done
    return 0
}

_realcc0=$(sed -n 's/^ndk.cc=//p' "$root/.native/arm64-v8a/provenance.properties")

# (a) the runtime query itself fails, in an otherwise valid toolchain
printf '#!/bin/sh\ncase " $* " in *" --print-libgcc-file-name "*) exit 7 ;; esac\nexec "%s" "$@"\n' "$_realcc0" > "$work/shim-qfail"
# (b) the runtime query returns malformed output
printf '#!/bin/sh\ncase " $* " in *" --print-libgcc-file-name "*) printf "not a path\\n"; exit 0 ;; esac\nexec "%s" "$@"\n' "$_realcc0" > "$work/shim-qbad"

copystage runtime-query-failure-real m-qfail && s=$STAGE_OUT \
  && make_depcopy runtime-query-failure-real \
  && wrap_ndk runtime-query-failure-real qfail "$work/shim-qfail" \
  && use_wrapped runtime-query-failure-real "$s" \
  && expect_fail runtime-query-failure-real "$s" arm64-v8a.link --dep-stage "$DEPCOPY"

copystage runtime-query-malformed m-qbad && s=$STAGE_OUT \
  && make_depcopy runtime-query-malformed \
  && wrap_ndk runtime-query-malformed qbad "$work/shim-qbad" \
  && use_wrapped runtime-query-malformed "$s" \
  && expect_fail runtime-query-malformed "$s" arm64-v8a.link --dep-stage "$DEPCOPY"

# A REAL impostor archive inside an otherwise valid toolchain: the x86_64
# runtime, which genuinely exists under the pinned NDK, offered for arm64.
# A containment rule accepts it; only the exact query rejects it.
copystage impostor-runtime-real m-impreal && s=$STAGE_OUT && {
    _rt=$(sed -n 's/^compiler.runtime.path=//p' "$s/arm64-v8a/topology.provenance")
    _other=$(printf '%s' "$_rt" | sed 's|aarch64-android|x86_64-android|')
    if guard impostor-runtime-real "the impostor archive does not exist" test -f "$_other"; then
        guard impostor-runtime-real "cannot repoint the map" mapedit "$s" arm64-v8a "s|$_rt|$_other|g" \
          && expect_fail impostor-runtime-real "$s" arm64-v8a.link
    fi
}

echo "== producer failures are named, never empty results =="
for pf in readelf nm objdump ar; do
    if ! copystage "producer-$pf" "m-prod-$pf"; then continue; fi
    s=$STAGE_OUT
    make_depcopy "producer-$pf" || continue
    if wrap_ndk "producer-$pf" "prod-$pf" "$work/shim-qfail"; then
        # restore a working compiler, then break exactly one llvm tool
        rm -f "$WRAP_CC"; cp "$_realcc0" "$WRAP_CC" && chmod +x "$WRAP_CC"
        shim_tool "producer-$pf" "$WRAP_BIN" "llvm-$pf" '#!/bin/sh
exit 9
' || continue
        if use_wrapped "producer-$pf" "$s"; then
            case $pf in
                readelf|nm|objdump|ar) expect_fail "producer-$pf" "$s" arm64-v8a --dep-stage "$DEPCOPY" ;;
            esac
        fi
    fi
done

echo "== a missing tool is a named preflight failure =="
_pd="$work/thinpath"
if guard missing-tool-preflight "cannot build the thin PATH" mkdir -p "$_pd"; then
    for t in awk sed grep sort tr cut head od find dirname basename cmp mktemp rm cp mkdir printf shasum sha256sum; do
        _p=$(command -v "$t" 2>/dev/null) && ln -sf "$_p" "$_pd/$t"
    done
    # deliberately NOT linked: uniq
    if run_chk "$work/pf" "$_pd" --self-test "$base"; then
        bad missing-tool-preflight "the checker ran with a required tool missing"
    elif grep -q 'required tool not found: uniq' "$work/pf"; then
        ok missing-tool-preflight "the missing tool is named: $(grep -m1 'required tool not found' "$work/pf")"
    else
        bad missing-tool-preflight "rejected, but not by a named preflight: $(sed -n '1p' "$work/pf")"
    fi
fi

echo "== a real product stage cannot be redirected =="
copystage product-rejects-repo m-prodrepo && s=$STAGE_OUT && {
    for abi in arm64-v8a x86_64; do
        sed -e 's|^mode=.*|mode=product|' -e '/^needed.selftest=/d' -e '/^libmoq.inputs.root=/d' \
            "$s/$abi/topology.provenance" > "$s/$abi/tp.n" && mv "$s/$abi/tp.n" "$s/$abi/topology.provenance"
    done
    expect_fail product-rejects-repo "$s" arm64-v8a.provenance --repo "$work/base/.inputs"
}
copystage product-rejects-inputs-root m-prodroot && s=$STAGE_OUT && {
    for abi in arm64-v8a x86_64; do
        sed -e 's|^mode=.*|mode=product|' -e '/^needed.selftest=/d' \
            "$s/$abi/topology.provenance" > "$s/$abi/tp.n" && mv "$s/$abi/tp.n" "$s/$abi/topology.provenance"
    done
    expect_fail product-rejects-inputs-root "$s" arm64-v8a.provenance
}

echo "== stage-helper setup failures are recorded by case name =="
# expect_setup_failure <case> <helper> <args...>
# The helper must report through setup_fail IN THIS SHELL. The probe runs under
# a throwaway name so the real ledger records exactly one entry, here.
expect_setup_failure() {
    _case=$1; shift
    _m0=$misses; _r0=$ran
    "$@" > /dev/null 2>&1
    _rc=$?
    _seen=$misses
    misses=$_m0; ran=$_r0
    if [ "$_seen" -gt "$_m0" ] && [ "$_rc" -ne 0 ]; then
        ok "$_case" "the helper reported its setup failure in the parent shell and returned non-zero"
    else
        bad "$_case" "a failing helper did not reach the parent ledger (misses $_m0 -> $_seen, rc=$_rc)"
    fi
}
expect_setup_failure setup-mkstage-fails   mkstage probe-mkstage base
expect_setup_failure setup-copystage-fails copystage probe-copystage "does/not/exist/deeply"

echo "== the five false passes reported in 1155, inverted =="
copystage audit-archive-substitution m-audsub && s=$STAGE_OUT \
  && guard audit-archive-substitution "cannot substitute the audit archive" \
      cp "$s/arm64-v8a/audit-inputs/libmoq-service.a" "$s/arm64-v8a/audit-inputs/libmoq-core.a" \
  && expect_fail audit-archive-substitution "$s" arm64-v8a.provenance

copystage provenance-trailing-space m-trail && s=$STAGE_OUT \
  && guard provenance-trailing-space "cannot add the trailing space" sh -c \
      "sed 's|^needed.selftest=libc.so libdl.so\$|needed.selftest=libc.so libdl.so |' '$s/arm64-v8a/topology.provenance' > '$s/arm64-v8a/tp.n' && mv '$s/arm64-v8a/tp.n' '$s/arm64-v8a/topology.provenance'" \
  && expect_fail provenance-trailing-space "$s" arm64-v8a.provenance

copystage provenance-non-ascii m-nonascii && s=$STAGE_OUT \
  && guard provenance-non-ascii "cannot add the non-ASCII byte" sh -c \
      "printf 'ndk.target=aarch64\303\251\n' >> '$s/arm64-v8a/topology.provenance'" \
  && expect_fail provenance-non-ascii "$s" arm64-v8a.provenance

copystage license-hardlink-outside m-hlink && s=$STAGE_OUT && {
    _o="$work/m-hlink-outside.LICENSE"
    if guard license-hardlink-outside "cannot stage the outside file" sh -c \
        "cp '$s/arm64-v8a/licenses/openssl.LICENSE' '$_o' && rm '$s/arm64-v8a/licenses/openssl.LICENSE' && ln '$_o' '$s/arm64-v8a/licenses/openssl.LICENSE'"; then
        expect_fail license-hardlink-outside "$s" stage
    fi
}

copystage outside-version-script m-vscript && s=$STAGE_OUT \
  && guard outside-version-script "cannot add the version-script token" sh -c \
      "printf 'moq-topology: link: cc -Wl,--version-script=/private/etc/passwd -o x.so\n' >> '$s/arm64-v8a/topology.log'" \
  && guard outside-version-script "cannot re-hash the receipt" restamp "$s" arm64-v8a \
  && expect_fail outside-version-script "$s" arm64-v8a.hermetic

copystage outside-response-file m-rspout && s=$STAGE_OUT && {
    _r="$work/m-rspout-outside.rsp"
    if guard outside-response-file "cannot write the outside response file" sh -c "printf -- '-DFOO\n' > '$_r'"; then
        guard outside-response-file "cannot reference it" sh -c \
            "printf 'moq-topology: link: cc @%s -o x.so\n' '$_r' >> '$s/arm64-v8a/topology.log'" \
          && guard outside-response-file "cannot re-hash the receipt" restamp "$s" arm64-v8a \
          && expect_fail outside-response-file "$s" arm64-v8a.hermetic
    fi
}

copystage unknown-path-option m-unkopt && s=$STAGE_OUT \
  && guard unknown-path-option "cannot add the unknown option" sh -c \
      "printf 'moq-topology: link: cc --some-future-option=/private/etc/passwd -o x.so\n' >> '$s/arm64-v8a/topology.log'" \
  && guard unknown-path-option "cannot re-hash the receipt" restamp "$s" arm64-v8a \
  && expect_fail unknown-path-option "$s" arm64-v8a.hermetic

copystage receipt-scratch-root m-scratchroot && s=$STAGE_OUT \
  && guard receipt-scratch-root "cannot add the scratch input" sh -c \
      "printf 'moq-topology: compile: cc -c %s/audit-scratch-probe.c -o p.o\n' \"\${TMPDIR:-/tmp}\" >> '$s/arm64-v8a/topology.log'" \
  && guard receipt-scratch-root "cannot re-hash the receipt" restamp "$s" arm64-v8a \
  && expect_fail receipt-scratch-root "$s" arm64-v8a.hermetic

echo "== the four false passes reported in 1157, inverted =="
# The receipt may not nominate its own trusted root.
copystage receipt-arbitrary-cwd m-cwdroot && s=$STAGE_OUT \
  && guard receipt-arbitrary-cwd "cannot rewrite the cwd" logedit "$s" arm64-v8a 's|^moq-topology: cwd: .*|moq-topology: cwd: /private|' \
  && guard receipt-arbitrary-cwd "cannot add the outside input" sh -c \
      "printf 'moq-topology: compile: cc -c /private/etc/passwd -o x.o\n' >> '$s/arm64-v8a/topology.log'" \
  && guard receipt-arbitrary-cwd "cannot re-hash the receipt" restamp "$s" arm64-v8a \
  && expect_fail receipt-arbitrary-cwd "$s" arm64-v8a.hermetic

# A cwd that is real and readable but outside the DECLARED build root.
copystage receipt-cwd-outside-buildroot m-cwdout && s=$STAGE_OUT \
  && guard receipt-cwd-outside-buildroot "cannot rewrite the cwd" logedit "$s" arm64-v8a "s|^moq-topology: cwd: .*|moq-topology: cwd: $work|" \
  && guard receipt-cwd-outside-buildroot "cannot re-hash the receipt" restamp "$s" arm64-v8a \
  && expect_fail receipt-cwd-outside-buildroot "$s" arm64-v8a.hermetic

# A declared build root outside the audited stage.
copystage buildroot-outside-stage m-brout && s=$STAGE_OUT \
  && guard buildroot-outside-stage "cannot rewrite the build root" provedit "$s" arm64-v8a "s|^build.root=.*|build.root=$work|" \
  && expect_fail buildroot-outside-stage "$s" arm64-v8a.hermetic

# A symlinked component followed by "..": lexical collapse hides the escape.
copystage symlink-dotdot-escape m-symdd && s=$STAGE_OUT && {
    _lnk="$work/m-symdd/escape-dir"
    if guard symlink-dotdot-escape "cannot create the escaping symlink" ln -sfn /private "$_lnk"; then
        guard symlink-dotdot-escape "cannot add the escaping input" sh -c \
            "printf 'moq-topology: compile: cc -c %s/../etc/passwd -o y.o\n' '$_lnk' >> '$s/arm64-v8a/topology.log'" \
          && guard symlink-dotdot-escape "cannot re-hash the receipt" restamp "$s" arm64-v8a \
          && expect_fail symlink-dotdot-escape "$s" arm64-v8a.hermetic
    fi
}

# The FINAL component is a symlink to an outside regular file.
copystage leaf-symlink-escape m-leaf && s=$STAGE_OUT && {
    _lf="$work/m-leaf/leaf.c"
    if guard leaf-symlink-escape "cannot create the leaf symlink" ln -sfn /private/etc/passwd "$_lf"; then
        guard leaf-symlink-escape "cannot add the leaf input" sh -c \
            "printf 'moq-topology: compile: cc -c %s -o l.o\n' '$_lf' >> '$s/arm64-v8a/topology.log'" \
          && guard leaf-symlink-escape "cannot re-hash the receipt" restamp "$s" arm64-v8a \
          && expect_fail leaf-symlink-escape "$s" arm64-v8a.hermetic
    fi
}

# An unknown path-bearing option NESTED in a response file: it must be refused
# at that depth and must TERMINATE, not spin. The response file lives in the
# task-owned dependency copy, an allowed root that is exempt from the source
# manifest, so nothing earlier can mask the intended oracle.
copystage nested-unknown-option m-nested && s=$STAGE_OUT \
  && guard nested-unknown-option "cannot inject the nested unknown option" sh -c \
      "printf -- '--future-input=/private/etc/passwd\n' >> '$s/arm64-v8a/audit-inputs/link.rsp'" \
  && expect_fail nested-unknown-option "$s" arm64-v8a.hermetic

# A real source-tree file that the digest does not cover.
copystage unmanifested-source-input m-unman && s=$STAGE_OUT \
  && guard unmanifested-source-input "cannot add the unmanifested input" sh -c \
      "printf 'moq-topology: compile: cc -c %s/.git -o z.o\n' '$repo_root' >> '$s/arm64-v8a/topology.log'" \
  && guard unmanifested-source-input "cannot re-hash the receipt" restamp "$s" arm64-v8a \
  && expect_fail unmanifested-source-input "$s" arm64-v8a.hermetic

# local.properties is deliberately excluded from the digest, so it must be
# unable to influence the DSO.
copystage excluded-local-properties m-localprop && s=$STAGE_OUT \
  && guard excluded-local-properties "cannot add local.properties as an input" sh -c \
      "printf 'moq-topology: compile: cc -c %s/bindings/kotlin/local.properties -o q.o\n' '$repo_root' >> '$s/arm64-v8a/topology.log'" \
  && guard excluded-local-properties "cannot re-hash the receipt" restamp "$s" arm64-v8a \
  && expect_fail excluded-local-properties "$s" arm64-v8a.hermetic

echo "== response-file bytes are content-bound =="
# shellcheck disable=SC2329  # invoked indirectly through `guard`
rspedit() { sed "$3" "$1/$2/responses.manifest" > "$1/$2/rm.n" && mv "$1/$2/rm.n" "$1/$2/responses.manifest"; }
# rehash_responses <stage> <abi> -- recompute every declared row's hash from the
# CURRENT bytes, through the accepted hasher rather than a hard-coded one.
# shellcheck disable=SC2329  # invoked indirectly through `guard`
rehash_responses() {
    awk -F"$(printf '\t')" '{ print $1 }' "$1/$2/responses.manifest" > "$1/$2/rm.paths" || return 1
    : > "$1/$2/rm.new" || return 1
    while IFS= read -r _p; do
        [ -n "$_p" ] || continue
        _h=$(decl_sha256 "$_p") || return 1
        decl_is_sha256 "$_h" || return 1
        printf '%s\t%s\n' "$_p" "$_h" >> "$1/$2/rm.new" || return 1
    done < "$1/$2/rm.paths"
    mv "$1/$2/rm.new" "$1/$2/responses.manifest" || return 1
    rm -f "$1/$2/rm.paths"
    restamp_rsp "$1" "$2"
}

# declare_response <stage> <abi> <path> -- append a declaration row.
# shellcheck disable=SC2329  # invoked indirectly through `guard`
declare_response() {
    _h=$(decl_sha256 "$3") || return 1
    decl_is_sha256 "$_h" || return 1
    printf '%s\t%s\n' "$3" "$_h" >> "$1/$2/responses.manifest" || return 1
    restamp_rsp "$1" "$2"
}

# shellcheck disable=SC2329  # invoked indirectly through `guard`
restamp_rsp() {
    _h=$(decl_sha256 "$1/$2/responses.manifest") || return 1
    decl_is_sha256 "$_h" || return 1
    sed "s|^responses.sha256=.*|responses.sha256=$_h|" "$1/$2/topology.provenance" > "$1/$2/tp.n" \
        && mv "$1/$2/tp.n" "$1/$2/topology.provenance"
}

# The exact 1159 F1 false pass: empty the response file after the link.
copystage rsp-empty-substitution m-rspempty && s=$STAGE_OUT \
  && guard rsp-empty-substitution "cannot empty the response file" sh -c ": > '$s/arm64-v8a/audit-inputs/link.rsp'" \
  && expect_fail rsp-empty-substitution "$s" arm64-v8a.hermetic

# Substituted bytes of the SAME LENGTH: emptiness is not the discriminator.
copystage rsp-same-length-substitution m-rsplen && s=$STAGE_OUT && {
    _f="$s/arm64-v8a/audit-inputs/link.rsp"
    _len=$(wc -c < "$_f" | tr -d ' ')
    if guard rsp-same-length-substitution "cannot rewrite the response bytes" sh -c \
        "tr 'a-z' 'A-Z' < '$_f' > '$_f.n' && mv '$_f.n' '$_f'"; then
        _len2=$(wc -c < "$_f" | tr -d ' ')
        if [ "$_len" = "$_len2" ]; then
            expect_fail rsp-same-length-substitution "$s" arm64-v8a.hermetic
        else
            setup_fail rsp-same-length-substitution "the substitution changed the length ($_len -> $_len2)"
        fi
    fi
}

# A NESTED response file that is referenced but never declared.
copystage rsp-undeclared-nested m-rspnest && s=$STAGE_OUT && {
    _n="$s/arm64-v8a/audit-inputs/nested.rsp"
    if guard rsp-undeclared-nested "cannot write the nested response file" sh -c "printf -- '-DNESTED\n' > '$_n'"; then
        guard rsp-undeclared-nested "cannot reference it" sh -c "printf '@%s\n' '$_n' >> '$s/arm64-v8a/audit-inputs/link.rsp'" \
          && guard rsp-undeclared-nested "cannot re-declare the outer response hash" rehash_responses "$s" arm64-v8a \
          && expect_fail rsp-undeclared-nested "$s" arm64-v8a.hermetic
    fi
}

# A response file that is DECLARED but never referenced by any command.
copystage rsp-declared-unreferenced m-rspunref && s=$STAGE_OUT && {
    _u="$s/arm64-v8a/audit-inputs/unused.rsp"
    if guard rsp-declared-unreferenced "cannot write the unused response file" sh -c "printf -- '-DUNUSED\n' > '$_u'"; then
        guard rsp-declared-unreferenced "cannot declare it" declare_response "$s" arm64-v8a "$_u" \
          && expect_fail rsp-declared-unreferenced "$s" arm64-v8a.hermetic
    fi
}

# A declared response file that cannot be read at all.
copystage rsp-read-failure m-rspread && s=$STAGE_OUT \
  && guard rsp-read-failure "cannot remove the response file" rm -f "$s/arm64-v8a/audit-inputs/link.rsp" \
  && expect_fail rsp-read-failure "$s" arm64-v8a.stage

# The declaration itself is bound: tampering with it without re-stamping fails.
copystage rsp-declaration-hash m-rspdecl && s=$STAGE_OUT \
  && guard rsp-declaration-hash "cannot tamper with the declaration" rspedit "$s" arm64-v8a 's|$|  |' \
  && expect_fail rsp-declaration-hash "$s" arm64-v8a.strip

echo "== the source-input inventory is complete and non-vacuous =="
for fam in core service media adapters cmake tools gradle; do
    case $fam in
        tools)  rel="bindings/kotlin/tools/build-jni-topology.sh" ;;
        gradle) rel="bindings/kotlin/gradle/sentinel.c" ;;
        *)      rel="$fam/sentinel.c" ;;
    esac
    if ! copystage "input-family-$fam" "m-fam-$fam"; then continue; fi
    s=$STAGE_OUT
    _ir="$work/m-fam-$fam/.inputs"
    if guard "input-family-$fam" "the sentinel for $fam is absent" test -f "$_ir/$rel"; then
        for abi in arm64-v8a x86_64; do
            sed "s|^libmoq.inputs.root=.*|libmoq.inputs.root=$_ir|" "$s/$abi/topology.provenance" > "$s/$abi/tp.n" && mv "$s/$abi/tp.n" "$s/$abi/topology.provenance"
        done
        printf '/* mutated */\n' > "$_ir/$rel"
        expect_fail "input-family-$fam" "$s" arm64-v8a.provenance
    fi
done

# Every individually listed build file needs its own coverage proof: a sentinel
# elsewhere under bindings/kotlin/gradle/ cannot stand in for gradlew.
for pair in "input-file-cmakelists:CMakeLists.txt" \
            "input-file-build-gradle:bindings/kotlin/build.gradle.kts" \
            "input-file-settings-gradle:bindings/kotlin/settings.gradle.kts" \
            "input-file-gradle-properties:bindings/kotlin/gradle.properties" \
            "input-file-gradlew:bindings/kotlin/gradlew" \
            "input-file-gradlew-bat:bindings/kotlin/gradlew.bat"; do
    _c=${pair%%:*}; _rel=${pair#*:}
    if ! copystage "$_c" "m-${_c}"; then continue; fi
    s=$STAGE_OUT
    _ir="$work/m-${_c}/.inputs"
    if guard "$_c" "the required input file is absent: $_rel" test -f "$_ir/$_rel"; then
        for abi in arm64-v8a x86_64; do
            sed "s|^libmoq.inputs.root=.*|libmoq.inputs.root=$_ir|" "$s/$abi/topology.provenance" > "$s/$abi/tp.n" && mv "$s/$abi/tp.n" "$s/$abi/topology.provenance"
        done
        rm -f "$_ir/$_rel"
        _nd=$( . "$here/lib-declaration.sh"; . "$here/lib-inputs.sh"; inputs_digest "$_ir" "$work/m-${_c}/.scratch" )
        if guard "$_c" "cannot re-declare the digest" test -n "$_nd"; then
            for abi in arm64-v8a x86_64; do
                sed "s|^libmoq.inputs.sha256=.*|libmoq.inputs.sha256=$_nd|" "$s/$abi/topology.provenance" > "$s/$abi/tp.n" && mv "$s/$abi/tp.n" "$s/$abi/topology.provenance"
                # The stage carries the manifest it is bound to, so a
                # re-declared digest must re-declare that manifest too;
                # otherwise this case is refused for inconsistency instead of
                # for the fact it exists to pin (ruling 1734).
                cp "$work/m-${_c}/.scratch/libmoq-inputs.manifest" "$s/$abi/libmoq-inputs.manifest"
            done
            expect_fail "$_c" "$s" arm64-v8a.provenance
        fi
    fi
done

copystage input-family-missing m-fammiss && s=$STAGE_OUT && {
    _ir="$work/m-fammiss/.inputs"
    if guard input-family-missing "cannot drop the family" rm -rf "$_ir/media"; then
        for abi in arm64-v8a x86_64; do
            sed "s|^libmoq.inputs.root=.*|libmoq.inputs.root=$_ir|" "$s/$abi/topology.provenance" > "$s/$abi/tp.n" && mv "$s/$abi/tp.n" "$s/$abi/topology.provenance"
        done
        _nd=$( . "$here/lib-declaration.sh"; . "$here/lib-inputs.sh"; inputs_digest "$_ir" "$work/m-fammiss/.scratch" )
        if guard input-family-missing "cannot re-declare the digest" test -n "$_nd"; then
            for abi in arm64-v8a x86_64; do
                sed "s|^libmoq.inputs.sha256=.*|libmoq.inputs.sha256=$_nd|" "$s/$abi/topology.provenance" > "$s/$abi/tp.n" && mv "$s/$abi/tp.n" "$s/$abi/topology.provenance"
                cp "$work/m-fammiss/.scratch/libmoq-inputs.manifest" "$s/$abi/libmoq-inputs.manifest"
            done
            expect_fail input-family-missing "$s" arm64-v8a.provenance
        fi
    fi
}

echo "== provenance grammar and declared facts =="

copystage omitted-needed m-omitneed && s=$STAGE_OUT \
  && guard omitted-needed "cannot rewrite the declaration" provedit "$s" arm64-v8a 's|^needed.selftest=.*|needed.selftest=libc.so libdl.so libm.so|' \
  && expect_fail omitted-needed "$s" arm64-v8a.needed

copystage wrong-provenance m-wrongprov && s=$STAGE_OUT \
  && guard wrong-provenance "cannot rewrite the API" provedit "$s" arm64-v8a 's|^android.api=.*|android.api=35|' \
  && expect_fail wrong-provenance "$s" arm64-v8a.provenance

copystage wrong-archive-hash m-archash && s=$STAGE_OUT \
  && guard wrong-archive-hash "cannot rewrite the archive hash" provedit "$s" arm64-v8a 's|^archive.libcrypto.a=.*|archive.libcrypto.a=0000000000000000000000000000000000000000000000000000000000000000|' \
  && expect_fail wrong-archive-hash "$s" arm64-v8a.provenance
# A PINNED-ONLY Phase 0.2 output is verified under its own key family, so a
# wrong hash there must fail exactly as a linked archive's does. Without this
# the split could quietly turn `dependency.` into an unchecked declaration.
copystage wrong-dependency-hash m-dephash && s=$STAGE_OUT \
  && guard wrong-dependency-hash "cannot rewrite the dependency hash" provedit "$s" arm64-v8a 's|^dependency.libssl.a=.*|dependency.libssl.a=0000000000000000000000000000000000000000000000000000000000000000|' \
  && expect_fail wrong-dependency-hash "$s" arm64-v8a.provenance

# The pinned-only log archive keeps its exact dependency hash binding.
copystage wrong-log-dependency-hash m-loghash && s=$STAGE_OUT \
  && guard wrong-log-dependency-hash "cannot rewrite the log dependency hash" provedit "$s" arm64-v8a 's|^dependency.libpicoquic-log.a=.*|dependency.libpicoquic-log.a=0000000000000000000000000000000000000000000000000000000000000000|' \
  && expect_fail_named wrong-log-dependency-hash "$s" arm64-v8a.provenance "libpicoquic-log.a hashes"

# picoquic_set_qlog attributed to a DIFFERENT member of the pinned core
# archive: every archive is still selected exactly as before, so the link row
# passes and only the qlog row's member identity can fire.
copystage qlog-wrong-member m-qlogmem && s=$STAGE_OUT \
  && guard qlog-wrong-member "cannot re-attribute the qlog record" mapedit "$s" arm64-v8a 's|libpicoquic-core\.a(qlog_fns\.c\.o):(\.text\.picoquic_set_qlog)|libpicoquic-core.a(qlog_frames.c.o):(.text.picoquic_set_qlog)|' \
  && expect_fail_named qlog-wrong-member "$s" arm64-v8a.qlog "but the member that defines it is qlog_fns.c.o"

# The qlog member selected from the pinned-only log archive instead of core:
# the pinned archive then contributes a member, which fails by name.
copystage qlog-log-origin m-qloglog && s=$STAGE_OUT \
  && guard qlog-log-origin "cannot re-attribute the qlog member" mapedit "$s" arm64-v8a 's|libpicoquic-core\.a(qlog_fns\.c\.o)|libpicoquic-log.a(qlog_fns.c.o)|g' \
  && expect_fail_named qlog-log-origin "$s" arm64-v8a.link "libpicoquic-log.a is a pinned-only Phase 0.2 output and must not be linked"

# A pinned-only archive NAMED in the link response while the map still selects
# nothing from it. The response bytes are re-declared and re-stamped, so the
# content binding holds and only response membership can fire.
for _pv in log:libpicoquic-log.a ssl:libssl.a; do
    _pc=rsp-pinned-${_pv%%:*}; _pa=${_pv#*:}
    copystage "$_pc" "m-$_pc" && s=$STAGE_OUT \
      && guard "$_pc" "cannot name the pinned archive in the response" sh -c \
           "sed -n 's|/libpicoquic-core\.a\$|/$_pa|p' '$s/arm64-v8a/audit-inputs/link.rsp' > '$s/arm64-v8a/pin.add' && [ -s '$s/arm64-v8a/pin.add' ] && cat '$s/arm64-v8a/pin.add' >> '$s/arm64-v8a/audit-inputs/link.rsp' && rm -f '$s/arm64-v8a/pin.add'" \
      && guard "$_pc" "cannot re-declare the response hash" rehash_responses "$s" arm64-v8a \
      && expect_fail_named "$_pc" "$s" arm64-v8a.hermetic "the pinned-only archive $_pa is named in the link response file"
done

copystage prov-malformed-key m-provbad && s=$STAGE_OUT \
  && guard prov-malformed-key "cannot append the malformed line" sh -c "printf 'this line has no equals sign\n' >> '$s/arm64-v8a/topology.provenance'" \
  && expect_fail prov-malformed-key "$s" arm64-v8a.provenance

copystage prov-extra-key m-provextra && s=$STAGE_OUT \
  && guard prov-extra-key "cannot append the extra key" sh -c "printf 'unexpected.key=1\n' >> '$s/arm64-v8a/topology.provenance'" \
  && expect_fail prov-extra-key "$s" arm64-v8a.provenance

echo "== the staged picotls prefix =="

# Each copied stage starts GREEN; one fact about the staged prefix is broken,
# and the named picotls oracle must be the first to fire.
_ptsrc="$root/.native/_src/picotls/include"
_pttls="$root/.native/_src/picoquic/picoquic/tls_api.c"
# ptls_manifest_restamp <stage> <abi>: re-declare the manifest's hash, so a
# case that edits the manifest on purpose is judged past the binding check.
ptls_manifest_restamp() {
    _pmh=$(decl_sha256 "$1/$2/ptls-prefix.manifest") || return 1
    decl_is_sha256 "$_pmh" || return 1
    sed "s|^ptls.prefix.manifest.sha256=.*|ptls.prefix.manifest.sha256=$_pmh|" "$1/$2/topology.provenance" > "$1/$2/tp.n" \
        && mv "$1/$2/tp.n" "$1/$2/topology.provenance"
}
# ptls_cc_write <stage> <abi> <command>: a compile database with one tls_api.c
# entry whose command is <command> (already JSON-escaped).
ptls_cc_write() {
    printf '[\n{ "directory": "%s", "command": "%s", "file": "%s" }\n]\n' \
        "$1/$2/audit-inputs" "$3" "$_pttls" > "$1/$2/audit-inputs/compile_commands.json"
}

copystage ptls-header-changed m-ptlshdr && s=$STAGE_OUT \
  && guard ptls-header-changed "cannot change a staged header" sh -c "printf '/* changed */\\n' >> '$s/arm64-v8a/audit-inputs/ptls-prefix/include/picotls.h'" \
  && expect_fail_named ptls-header-changed "$s" arm64-v8a.ptls.staged "the staged include/picotls.h hashes"

copystage ptls-archive-changed m-ptlsarc && s=$STAGE_OUT \
  && guard ptls-archive-changed "cannot change a staged archive" sh -c "printf 'x' >> '$s/arm64-v8a/audit-inputs/ptls-prefix/lib/libpicotls-core.a'" \
  && expect_fail_named ptls-archive-changed "$s" arm64-v8a.ptls.staged "the staged lib/libpicotls-core.a hashes"

copystage ptls-entry-missing m-ptlsmiss && s=$STAGE_OUT \
  && guard ptls-entry-missing "cannot remove a staged header" rm -f "$s/arm64-v8a/audit-inputs/ptls-prefix/include/picotls/minicrypto.h" \
  && expect_fail_named ptls-entry-missing "$s" arm64-v8a.ptls.staged "the staged include/picotls/minicrypto.h is missing"

copystage ptls-entry-extra m-ptlsextra && s=$STAGE_OUT \
  && guard ptls-entry-extra "cannot add a staged file" sh -c "printf '/* extra */\\n' > '$s/arm64-v8a/audit-inputs/ptls-prefix/include/extra.h'" \
  && expect_fail_named ptls-entry-extra "$s" arm64-v8a.ptls.staged "first difference: include/extra.h"

# The OTHER ABI's accepted archive, staged and declared consistently: the copy
# is faithful to its source, but not to this ABI's pin.
copystage ptls-wrong-abi m-ptlsabi && s=$STAGE_OUT \
  && guard ptls-wrong-abi "cannot stage the x86_64 archive for arm64" sh -c "cp '$root/.native/x86_64/lib/libpicotls-core.a' '$s/arm64-v8a/audit-inputs/ptls-prefix/lib/libpicotls-core.a' && _h=\$(shasum -a 256 '$root/.native/x86_64/lib/libpicotls-core.a' | cut -d' ' -f1) && awk -F'\t' -v OFS='\t' -v h=\"\$_h\" -v src='$root/.native/x86_64/lib/libpicotls-core.a' '\$1==\"lib/libpicotls-core.a\" { \$2=h; \$3=src } { print }' '$s/arm64-v8a/ptls-prefix.manifest' > '$s/arm64-v8a/pm.n' && mv '$s/arm64-v8a/pm.n' '$s/arm64-v8a/ptls-prefix.manifest'" \
  && guard ptls-wrong-abi "cannot restamp the manifest" ptls_manifest_restamp "$s" arm64-v8a \
  && expect_fail_named ptls-wrong-abi "$s" arm64-v8a.ptls.staged "lib/libpicotls-core.a is recorded as copied from $root/.native/x86_64/lib/libpicotls-core.a"

copystage ptls-manifest-unbound m-ptlsman && s=$STAGE_OUT \
  && guard ptls-manifest-unbound "cannot edit the manifest" sh -c "sed '1d' '$s/arm64-v8a/ptls-prefix.manifest' > '$s/arm64-v8a/pm.n' && mv '$s/arm64-v8a/pm.n' '$s/arm64-v8a/ptls-prefix.manifest'" \
  && expect_fail_named ptls-manifest-unbound "$s" arm64-v8a.ptls.manifest "not the declared"

copystage ptls-cmake-selection m-ptlscache && s=$STAGE_OUT \
  && guard ptls-cmake-selection "cannot redirect the cached headers" sh -c "sed 's|^PTLS_INCLUDE_DIR:PATH=.*|PTLS_INCLUDE_DIR:PATH=$_ptsrc|' '$s/arm64-v8a/audit-inputs/CMakeCache.txt' > '$s/arm64-v8a/audit-inputs/cc.n' && mv '$s/arm64-v8a/audit-inputs/cc.n' '$s/arm64-v8a/audit-inputs/CMakeCache.txt'" \
  && expect_fail_named ptls-cmake-selection "$s" arm64-v8a.ptls.cmake "the configure selected PTLS_INCLUDE_DIR="

# The staged headers as -isystem, a foreign valid tree as -I -- and -isystem
# written FIRST. The compiler searches every -I before any -isystem, so the
# foreign tree supplies picotls.h; option order must not decide it.
copystage ptls-isystem-order m-ptlsisys && s=$STAGE_OUT \
  && guard ptls-isystem-order "cannot rewrite the compile command" ptls_cc_write "$s" arm64-v8a "cc -isystem $s/arm64-v8a/audit-inputs/ptls-prefix/include -I $_ptsrc -c $_pttls" \
  && expect_fail_named ptls-isystem-order "$s" arm64-v8a.ptls.header "finds picotls.h first in '$(cd "$_ptsrc" && pwd -P)'"

# A quoted include path containing a space that resolves to the staged
# headers is VALID: whitespace splitting would break it, the tokenizer must not.
copystage ptls-space-path m-ptlsspace && s=$STAGE_OUT \
  && guard ptls-space-path "cannot create the spaced alias" ln -s "$s/arm64-v8a/audit-inputs/ptls-prefix/include" "$work/m-ptlsspace/staged headers" \
  && guard ptls-space-path "cannot rewrite the compile command" ptls_cc_write "$s" arm64-v8a "cc -I\\\"$work/m-ptlsspace/staged headers\\\" -c $_pttls" \
  && { if run "$s"; then ok ptls-space-path "a quoted include path with a space resolving to the staged prefix passes"
       else bad ptls-space-path "the checker rejected a valid quoted path with a space: $(grep -E '^FAIL\[' "$work/out" | sed -n '1p')"; fi; }

# A foreign directory holding ONLY picotls/openssl.h, ahead of the staged
# headers: tls_api.c still finds picotls.h in the prefix, but its
# picotls/openssl.h comes from elsewhere -- a mixed compile an oracle that
# judged only picotls.h would pass.
copystage ptls-openssl-only m-ptlsossl && s=$STAGE_OUT \
  && guard ptls-openssl-only "cannot create the openssl-only tree" sh -c "mkdir -p '$work/m-ptlsossl/openssl-only/picotls' && cp '$_ptsrc/picotls/openssl.h' '$work/m-ptlsossl/openssl-only/picotls/'" \
  && guard ptls-openssl-only "cannot rewrite the compile command" ptls_cc_write "$s" arm64-v8a "cc -I$work/m-ptlsossl/openssl-only -I$s/arm64-v8a/audit-inputs/ptls-prefix/include -c $_pttls" \
  && expect_fail_named ptls-openssl-only "$s" arm64-v8a.ptls.header "finds picotls/openssl.h first in '$(cd "$work" 2>/dev/null && pwd -P)/m-ptlsossl/openssl-only'"

copystage ptls-cc-malformed m-ptlsbad && s=$STAGE_OUT \
  && guard ptls-cc-malformed "cannot truncate the compile database" sh -c "printf '[ { \"file\": ' > '$s/arm64-v8a/audit-inputs/compile_commands.json'" \
  && expect_fail_named ptls-cc-malformed "$s" arm64-v8a.ptls.header "not a JSON array"

copystage ptls-cc-ambiguous m-ptlsamb && s=$STAGE_OUT \
  && guard ptls-cc-ambiguous "cannot write two disagreeing compiles" sh -c "printf '[\\n{ \"directory\": \"%s\", \"command\": \"cc -I%s -c %s\", \"file\": \"%s\" },\\n{ \"directory\": \"%s\", \"command\": \"cc -I%s -c %s\", \"file\": \"%s\" }\\n]\\n' '$s/arm64-v8a/audit-inputs' '$s/arm64-v8a/audit-inputs/ptls-prefix/include' '$_pttls' '$_pttls' '$s/arm64-v8a/audit-inputs' '$_ptsrc' '$_pttls' '$_pttls' > '$s/arm64-v8a/audit-inputs/compile_commands.json'" \
  && expect_fail_named ptls-cc-ambiguous "$s" arm64-v8a.ptls.header "disagree on which directories supply the picotls headers"

copystage ptls-cc-unmodelled m-ptlsrsp && s=$STAGE_OUT \
  && guard ptls-cc-unmodelled "cannot rewrite the compile command" ptls_cc_write "$s" arm64-v8a "cc @extra.rsp -I$s/arm64-v8a/audit-inputs/ptls-prefix/include -c $_pttls" \
  && expect_fail_named ptls-cc-unmodelled "$s" arm64-v8a.ptls.header "does not model"

echo "== licences =="
_tab=$(printf '\t')

copystage missing-json-notice m-nojson && s=$STAGE_OUT && {
    if guard missing-json-notice "cannot drop the row" sh -c "grep -v '^moq-msf-json$_tab' '$s/arm64-v8a/licenses.manifest' > '$s/arm64-v8a/lm.n' && mv '$s/arm64-v8a/lm.n' '$s/arm64-v8a/licenses.manifest'"; then
        guard missing-json-notice "cannot remove the payload" rm -f "$s/arm64-v8a/licenses/moq-msf-json.LICENSE" \
          && expect_fail missing-json-notice "$s" arm64-v8a.stage
    fi
}

copystage arbitrary-source m-arbsrc && s=$STAGE_OUT \
  && guard arbitrary-source "cannot rewrite the source field" manedit "$s" arm64-v8a "s|${_tab}[^${_tab}]*openssl/LICENSE.txt\$|${_tab}/tmp/whatever/LICENSE|" \
  && expect_fail arbitrary-source "$s" arm64-v8a.licenses

copystage extra-field m-xfield && s=$STAGE_OUT \
  && guard extra-field "cannot append the fifth field" manedit "$s" arm64-v8a "s|^cifra${_tab}\(.*\)\$|cifra${_tab}\1${_tab}extra|" \
  && expect_fail extra-field "$s" arm64-v8a.licenses

copystage duplicate-row m-duprow && s=$STAGE_OUT \
  && guard duplicate-row "cannot duplicate the row" sh -c "grep '^picoquic$_tab' '$s/arm64-v8a/licenses.manifest' >> '$s/arm64-v8a/licenses.manifest'" \
  && expect_fail duplicate-row "$s" arm64-v8a.licenses

copystage license-escape m-lescape && s=$STAGE_OUT \
  && guard license-escape "cannot point the destination outside" manedit "$s" arm64-v8a 's|licenses/openssl.LICENSE|licenses/../topology.log|' \
  && expect_fail license-escape "$s" arm64-v8a.licenses

copystage license-payload m-lpayload && s=$STAGE_OUT \
  && guard license-payload "cannot tamper with the payload" sh -c "printf 'tampered\n' >> '$s/arm64-v8a/licenses/picoquic.LICENSE'" \
  && expect_fail license-payload "$s" arm64-v8a.licenses

echo "== the build receipt =="

copystage log-incomplete m-logdone && s=$STAGE_OUT \
  && guard log-incomplete "cannot drop the completion marker" sh -c "grep -v 'moq-topology: link: done' '$s/arm64-v8a/topology.log' > '$s/arm64-v8a/tl.n' && mv '$s/arm64-v8a/tl.n' '$s/arm64-v8a/topology.log'" \
  && guard log-incomplete "cannot re-hash the receipt" restamp "$s" arm64-v8a \
  && expect_fail log-incomplete "$s" arm64-v8a.hermetic

copystage log-warning m-logwarn && s=$STAGE_OUT \
  && guard log-warning "cannot append the diagnostic" sh -c "printf 'ld: warning: something was promoted\n' >> '$s/arm64-v8a/topology.log'" \
  && guard log-warning "cannot re-hash the receipt" restamp "$s" arm64-v8a \
  && expect_fail log-warning "$s" arm64-v8a.hermetic

copystage log-no-map m-logmap && s=$STAGE_OUT \
  && guard log-no-map "cannot rewrite the map argument" logedit "$s" arm64-v8a 's|-Wl,-Map,[^ ]*|-Wl,-Map,/tmp/other.map|' \
  && guard log-no-map "cannot re-hash the receipt" restamp "$s" arm64-v8a \
  && expect_fail log-no-map "$s" arm64-v8a.hermetic

copystage relative-escape m-relesc && s=$STAGE_OUT \
  && guard relative-escape "cannot inject the relative escape" sh -c "printf 'moq-topology: compile: cc -c ../../../../../../etc/hosts.c -o x.o\n' >> '$s/arm64-v8a/topology.log'" \
  && guard relative-escape "cannot re-hash the receipt" restamp "$s" arm64-v8a \
  && expect_fail relative-escape "$s" arm64-v8a.hermetic

copystage response-escape m-rspesc && s=$STAGE_OUT && {
    _rsp="$work/m-rspesc/link.rsp"
    if guard response-escape "cannot write the response file" sh -c "printf -- '-I/private/etc\n/private/etc/hosts.c\n' > '$_rsp'"; then
        guard response-escape "cannot reference the response file" sh -c "printf 'moq-topology: link: cc @%s -o out.so\n' '$_rsp' >> '$s/arm64-v8a/topology.log'" \
          && guard response-escape "cannot re-hash the receipt" restamp "$s" arm64-v8a \
          && expect_fail response-escape "$s" arm64-v8a.hermetic
    fi
}

copystage map-malformed m-mapbad && s=$STAGE_OUT \
  && guard map-malformed "cannot malform the map" mapedit "$s" arm64-v8a 's|:(|:[|g' \
  && expect_fail map-malformed "$s" arm64-v8a.link

copystage unreadable-stage m-unread && s=$STAGE_OUT \
  && guard unreadable-stage "cannot remove the package file" rm -f "$s/arm64-v8a/package/libmoq5_jni.so" \
  && expect_fail unreadable-stage "$s" arm64-v8a.stage

echo "== the toolchain fingerprint, exercised on a disposable root =="
# The REAL ndk_fingerprint() is called throughout. Every mutation happens in a
# miniature copy; no file under the pinned NDK is touched.
fp_root_make() {
    _c=$1; _d=$2
    rm -rf "$_d"
    mkdir -p "$_d" || { setup_fail "$_c" "cannot create the miniature root"; return 1; }
    printf 'binary-one\n'   > "$_d/llvm-alpha" || { setup_fail "$_c" "cannot write llvm-alpha"; return 1; }
    printf 'clang-nineteen\n' > "$_d/clang-19" || { setup_fail "$_c" "cannot write clang-19"; return 1; }
    printf 'clang-nineteen\n' > "$_d/clang-19b" || { setup_fail "$_c" "cannot write clang-19b"; return 1; }
    ( cd "$_d" && ln -sfn clang-19 clang ) || { setup_fail "$_c" "cannot create the clang alias"; return 1; }
    return 0
}

# (a) a SAME-SIZE change to a plain tool's bytes
if fp_root_make fp-same-size-target "$work/fp-a"; then
    _b=$(ndk_fingerprint "$work/fp-a")
    _s1=$(wc -c < "$work/fp-a/llvm-alpha" | tr -d ' ')
    printf 'binary-two\n' > "$work/fp-a/llvm-alpha"
    _s2=$(wc -c < "$work/fp-a/llvm-alpha" | tr -d ' ')
    _a=$(ndk_fingerprint "$work/fp-a")
    if [ "$_s1" = "$_s2" ] && [ "$_b" != "$_a" ]; then
        ok fp-same-size-target "a same-size ($_s1 byte) content change moves the real fingerprint"
    else
        bad fp-same-size-target "size $_s1 -> $_s2, fingerprint moved: $( [ "$_b" != "$_a" ] && echo yes || echo NO )"
    fi
fi

# (b) an alias RE-POINTED at a byte-identical file
if fp_root_make fp-alias-retarget "$work/fp-b"; then
    _b=$(ndk_fingerprint "$work/fp-b")
    cmp -s "$work/fp-b/clang-19" "$work/fp-b/clang-19b" && _same=yes || _same=no
    ( cd "$work/fp-b" && ln -sfn clang-19b clang )
    _a=$(ndk_fingerprint "$work/fp-b")
    if [ "$_same" = yes ] && [ "$_b" != "$_a" ]; then
        ok fp-alias-retarget "an alias re-pointed at a byte-identical file still moves the fingerprint"
    else
        bad fp-alias-retarget "targets identical: $_same, fingerprint moved: $( [ "$_b" != "$_a" ] && echo yes || echo NO )"
    fi
fi

# (c) the TARGET's bytes change behind an unchanged alias -- the case a
# link-spelling-only fingerprint misses entirely
if fp_root_make fp-target-behind-alias "$work/fp-c"; then
    _b=$(ndk_fingerprint "$work/fp-c")
    _sp1=$(readlink "$work/fp-c/clang")
    printf 'clang-twenty!!\n' > "$work/fp-c/clang-19"
    _sp2=$(readlink "$work/fp-c/clang")
    _a=$(ndk_fingerprint "$work/fp-c")
    if [ "$_sp1" = "$_sp2" ] && [ "$_b" != "$_a" ]; then
        ok fp-target-behind-alias "a target change behind an unchanged alias ($_sp1) moves the fingerprint"
    else
        bad fp-target-behind-alias "spelling $_sp1 -> $_sp2, fingerprint moved: $( [ "$_b" != "$_a" ] && echo yes || echo NO )"
    fi
fi

# (d) an EARLY nonzero exit must still run the exit guard, report the mismatch,
# preserve the nonzero status and clean its own scratch
if fp_root_make fp-exit-guard "$work/fp-d"; then
    _probe="$work/fp-d-probe.sh"
    {
        printf '#!/bin/sh\nset -u\n'
        printf 'FPROOT=%s\n' "'$work/fp-d'"
        printf 'SCRATCH=%s\n' "'$work/fp-d-scratch'"
        # the REAL functions, lifted verbatim from this file
        sed -n '/^fp_hash() {/,/^}$/p;/^fp_resolve() {/,/^}$/p;/^ndk_fingerprint() {/,/^}$/p' "$0"
        cat <<'PROBE'
mkdir -p "$SCRATCH"
BEFORE=$(ndk_fingerprint "$FPROOT")
on_exit() {
    _st=$?
    A=$(ndk_fingerprint "$FPROOT")
    if [ "$BEFORE" != "$A" ]; then
        echo "GUARD: fingerprint mismatch"
        [ "$_st" -eq 0 ] && _st=1
    fi
    rm -rf -- "$SCRATCH"
    exit "$_st"
}
trap on_exit EXIT
printf 'mutated!!!!!!\n' > "$FPROOT/llvm-alpha"
exit 3
PROBE
    } > "$_probe"
    chmod +x "$_probe"
    _po=$(sh "$_probe" 2>&1); _prc=$?
    if [ "$_prc" -eq 3 ] && printf '%s' "$_po" | grep -q 'GUARD: fingerprint mismatch' \
       && [ ! -d "$work/fp-d-scratch" ]; then
        ok fp-exit-guard "an early exit 3 still ran the guard, reported the mismatch, kept status 3 and removed its scratch"
    else
        bad fp-exit-guard "status=$_prc scratch-left=$( [ -d "$work/fp-d-scratch" ] && echo yes || echo no ) output=$_po"
    fi
fi

echo "== the watchdog is real =="
# A deliberately nonterminating disposable command must produce a prompt,
# NAMED timeout rather than wedging the suite.
_hang="$work/hang.sh"
printf '#!/bin/sh\nwhile :; do sleep 1; done\n' > "$_hang"; chmod +x "$_hang"
_save=$RUN_DEADLINE; RUN_DEADLINE=3
_t0=$(date +%s)
guarded_run "$work/hang.out" - sh "$_hang"
_hrc=$?
_t1=$(date +%s)
RUN_DEADLINE=$_save
_el=$((_t1 - _t0))
# BOTH bounds: an immediate false timeout is as wrong as a failure to expire.
if [ "$_hrc" -eq 124 ] && [ "$GUARD_TIMEOUT" -eq 1 ] && [ "$_el" -ge 2 ] && [ "$_el" -le 20 ]; then
    ok watchdog-terminates "a nonterminating command timed out after ${_el}s (deadline 3s, bounded 2..20) with status 124"
else
    bad watchdog-terminates "rc=$_hrc timeout-flag=$GUARD_TIMEOUT elapsed=${_el}s (expected 124, flag 1, 2..20s)"
fi

# A timed-out command that spawned a child must leave NO descendant behind.
_hang2="$work/hang2.sh"
cat > "$_hang2" <<'HANG2'
#!/bin/sh
sleep 600 &
echo "$!" > "$1"
echo "$$" > "$2"
wait
HANG2
chmod +x "$_hang2"
_save=$RUN_DEADLINE; RUN_DEADLINE=3
guarded_run "$work/hang2.out" - sh "$_hang2" "$work/kid.pid" "$work/parent.pid"
_h2=$?
RUN_DEADLINE=$_save
_kid=$(cat "$work/kid.pid" 2>/dev/null); _par=$(cat "$work/parent.pid" 2>/dev/null)
if [ "$_h2" -eq 124 ] && [ -n "$_kid" ] && [ -n "$_par" ] \
   && pids_gone_within 15 "$_par" "$_kid"; then
    ok watchdog-no-child-leak "the timed-out shell (pid $_par) and its spawned child (pid $_kid) are both gone"
else
    bad watchdog-no-child-leak "rc=$_h2 parent=$_par gone=$( pid_gone "$_par" && echo yes || echo no ) child=$_kid gone=$( pid_gone "$_kid" && echo yes || echo no )"
fi

# The wrapper must be transparent for an ordinary command.
_ok="$work/okcmd.sh"
printf '#!/bin/sh\nprintf "hello-from-child\\n"\nexit 7\n' > "$_ok"; chmod +x "$_ok"
guarded_run "$work/ok.out" - sh "$_ok"
_orc=$?
if [ "$_orc" -eq 7 ] && [ "$GUARD_TIMEOUT" -eq 0 ] && grep -q 'hello-from-child' "$work/ok.out"; then
    ok watchdog-transparent "an ordinary command keeps its status (7) and its output through the wrapper"
else
    bad watchdog-transparent "rc=$_orc timeout-flag=$GUARD_TIMEOUT output=$(sed -n '1p' "$work/ok.out")"
fi

echo "== a missing readlink is a named preflight failure =="
_pd2="$work/thinpath2"
if guard preflight-readlink "cannot build the thin PATH" mkdir -p "$_pd2"; then
    for t in awk sed grep sort uniq comm tr cut head od find dirname basename cmp \
             mktemp rm cp mkdir printf shasum sha256sum; do
        _p=$(command -v "$t" 2>/dev/null) && ln -sf "$_p" "$_pd2/$t"
    done
    # deliberately NOT linked: readlink
    if run_chk "$work/pf2" "$_pd2" --self-test "$base"; then
        bad preflight-readlink "the checker ran without readlink"
    elif grep -q 'required tool not found: readlink' "$work/pf2"; then
        ok preflight-readlink "the missing tool is named: $(grep -m1 'required tool not found' "$work/pf2")"
    else
        bad preflight-readlink "rejected, but not by a named preflight: $(sed -n '1p' "$work/pf2")"
    fi
fi

echo "== the frozen product DT_NEEDED authority is enforced =="
# Ruling 1171 A froze the product authority as exactly [libc.so libdl.so]. The
# `extra-needed` variant links one more platform library, so converting it to a
# product stage must be refused BY THE AUTHORITY -- not merely by some later
# oracle. Its build root cannot satisfy the product containment rule from a
# temp directory, but `needed` runs first, so the attribution is unambiguous.
mkstage product-needed-authority m-prodneed extra-needed && s=$STAGE_OUT && {
    mkdir -p "$work/prodscratch" 2>/dev/null
    _rd=$( . "$here/lib-declaration.sh"; . "$here/lib-inputs.sh"; inputs_digest "$repo_root" "$work/prodscratch" ) \
        || bad product-needed-authority "cannot recompute the real input digest"
    for abi in arm64-v8a x86_64; do
        sed -e 's|^mode=.*|mode=product|' -e '/^needed.selftest=/d' -e '/^libmoq.inputs.root=/d' \
            -e "s|^libmoq.inputs.sha256=.*|libmoq.inputs.sha256=$_rd|" \
            "$s/$abi/topology.provenance" > "$s/$abi/tp.n" && mv "$s/$abi/tp.n" "$s/$abi/topology.provenance"
        # A product stage is bound to the WORKTREE manifest, so the stage's
        # saved copy moves with the digest it declares.
        cp "$work/prodscratch/libmoq-inputs.manifest" "$s/$abi/libmoq-inputs.manifest"
        # A product stage also carries the JNI compile record and declares its
        # hash; an empty record suffices, because `needed` answers first.
        printf '[]\n' > "$s/$abi/jni-compile.json" \
            && _jh=$(decl_sha256 "$s/$abi/jni-compile.json") \
            && printf 'jni.compile.sha256=%s\n' "$_jh" >> "$s/$abi/topology.provenance" \
            || bad product-needed-authority "cannot add the product JNI compile record for $abi"
        # A product stage holds no picotls prefix or configure evidence of its
        # own: the product recipe keeps them in the build root, so they move out
        # of the audited stage.
        mkdir -p "$work/m-prodneed-out-$abi" \
            && mv "$s/$abi/audit-inputs/ptls-prefix" "$s/$abi/audit-inputs/CMakeCache.txt" \
                  "$s/$abi/audit-inputs/compile_commands.json" "$work/m-prodneed-out-$abi/" \
            || bad product-needed-authority "cannot move the build-root evidence out of the $abi stage"
    done
    if run "$s"; then
        bad product-needed-authority "a product stage with an extra DT_NEEDED passed"
    elif [ "$GUARD_TIMEOUT" -ne 0 ]; then
        bad product-needed-authority "the checker timed out"
    elif grep -m1 -E '^FAIL' "$work/out" | grep -qE '^FAIL\[arm64-v8a\.needed\].*libc\.so libdl\.so'; then
        ok product-needed-authority "$(grep -m1 -E '^FAIL\[arm64-v8a\.needed\]' "$work/out" | cut -c1-140)"
    else
        bad product-needed-authority "the first failure is not the DT_NEEDED authority: $(grep -m1 -E '^FAIL' "$work/out" | cut -c1-140)"
    fi
}

# A restamp whose target cannot be hashed must be a NAMED SETUP FAILURE, not a
# malformed declaration that later fails at some unrelated oracle.
copystage restamp-hash-failure m-restampfail && s=$STAGE_OUT && {
    if guard restamp-hash-failure "cannot remove the declaration" rm -f "$s/arm64-v8a/responses.manifest"; then
        expect_setup_failure restamp-hash-failure \
            guard probe-restamp "a restamp whose target cannot be hashed" restamp_rsp "$s" arm64-v8a
    fi
}

echo "== every checker invocation goes through one guarded chokepoint =="
# A source-level coverage oracle: the only executable "$chk" in this file must
# be the one inside run_chk. Anything else escapes the watchdog.
# shellcheck disable=SC2016  # these are literal source patterns, not expansions
_bad_inv=$(grep -n '"\$chk"' "$0" \
           | grep -v 'guarded_run "\$_ro" "\$_rp" "\$chk"' \
           | grep -v '\[ -x "\$chk" \]' \
           | grep -v '^[0-9]*:#' \
           | sed -n '1p')
if [ -z "$_bad_inv" ]; then
    ok chk-invocation-coverage "the only executable checker invocation is the one inside run_chk"
else
    bad chk-invocation-coverage "a checker invocation exists outside the chokepoint: $_bad_inv"
fi

# The special shapes -- a temporary PATH and a malformed CLI -- must inherit the
# watchdog too. Proven with a deliberately nonterminating stand-in checker.
_hangchk="$work/hangchk.sh"
printf '#!/bin/sh\nwhile :; do sleep 1; done\n' > "$_hangchk"; chmod +x "$_hangchk"
_realchk=$chk; chk=$_hangchk
_save=$RUN_DEADLINE; RUN_DEADLINE=3
run_chk "$work/sp1" "$work/thinpath" --self-test "$base" >/dev/null 2>&1
_s1=$?; _f1=$GUARD_TIMEOUT
run_chk "$work/sp2" - --not-an-option >/dev/null 2>&1
_s2=$?; _f2=$GUARD_TIMEOUT
RUN_DEADLINE=$_save; chk=$_realchk
if [ "$_s1" -eq 124 ] && [ "$_f1" -eq 1 ] && [ "$_s2" -eq 124 ] && [ "$_f2" -eq 1 ]; then
    ok watchdog-covers-special-shapes "both the thin-PATH and malformed-CLI shapes timed out under the watchdog"
else
    bad watchdog-covers-special-shapes "thin-PATH rc=$_s1 flag=$_f1, CLI rc=$_s2 flag=$_f2"
fi

echo "== the fingerprint resolves the whole chain and fails closed =="
# An INTERMEDIATE retargeted to byte-identical content, outer spelling unchanged.
if fp_root_make fp-chain-intermediate "$work/fp-e"; then
    _b=$(ndk_fingerprint "$work/fp-e") || setup_fail fp-chain-intermediate "cannot fingerprint the miniature root"
    _sp1=$(readlink "$work/fp-e/clang")
    rm -f "$work/fp-e/clang-19"
    ( cd "$work/fp-e" && ln -sfn clang-19b clang-19 )
    _sp2=$(readlink "$work/fp-e/clang")
    _a=$(ndk_fingerprint "$work/fp-e") || setup_fail fp-chain-intermediate "cannot re-fingerprint"
    # The byte-identical premise is LOAD-BEARING: without it this would be a
    # content-change discriminator, not a pure intermediate-symlink one.
    if cmp -s "$work/fp-e/clang-19b" "$(fp_resolve "$work/fp-e/clang-19")"; then _ident=yes; else _ident=no; fi
    if [ "$_ident" = yes ] && [ "$_sp1" = "$_sp2" ] && [ "$_b" != "$_a" ]; then
        ok fp-chain-intermediate "a retargeted intermediate moves the fingerprint although the outer spelling ($_sp1) is unchanged and the final bytes are identical"
    else
        bad fp-chain-intermediate "final targets byte-identical: $_ident, spelling $_sp1 -> $_sp2, moved: $( [ "$_b" != "$_a" ] && echo yes || echo NO )"
    fi
fi

# A hash producer failure must make the whole operation fail, never yield an
# empty identity that can compare equal.
if fp_root_make fp-hash-failure "$work/fp-f"; then
    chmod 000 "$work/fp-f/llvm-alpha"
    if ndk_fingerprint "$work/fp-f" > "$work/fp-f.out" 2>/dev/null; then
        bad fp-hash-failure "an unreadable target still produced a fingerprint: $(sed -n '1p' "$work/fp-f.out")"
    else
        ok fp-hash-failure "an unreadable target makes the fingerprint fail rather than emit an empty hash"
    fi
    chmod 644 "$work/fp-f/llvm-alpha"
fi

# An empty or missing root must fail, not compare equal to another empty root.
mkdir -p "$work/fp-empty"
if ndk_fingerprint "$work/fp-empty" >/dev/null 2>&1; then
    bad fp-empty-root "an empty root produced a fingerprint"
elif ndk_fingerprint "$work/fp-absent-root" >/dev/null 2>&1; then
    bad fp-empty-root "a missing root produced a fingerprint"
else
    ok fp-empty-root "an empty inventory and a missing root both fail rather than yield an empty identity"
fi

echo "== the response snapshot is the hashed byte stream =="
# A cp that mutates its source at the copy boundary: hashing the live file and
# classifying a later copy accepted this.
copystage rsp-snapshot-race m-rsprace && s=$STAGE_OUT && {
    _sd="$work/m-rsprace-shim"
    if guard rsp-snapshot-race "cannot build the cp shim" mkdir -p "$_sd"; then
        _realcp=$(command -v cp)
        {
            printf '#!/bin/sh\n'
            # shellcheck disable=SC2016  # literal shim source, not an expansion
            printf 'case "$1" in *link.rsp) printf "\\n" >> "$1" ;; esac\n'
            printf 'exec %s "$@"\n' "$_realcp"
        } > "$_sd/cp"
        chmod +x "$_sd/cp"
        # shellcheck disable=SC2031  # this PATH is the suite's own, unmodified
        _mixed="$_sd:$PATH"
        if run_chk "$work/race.out" "$_mixed" --self-test "$s"; then
            bad rsp-snapshot-race "the checker accepted a response file mutated at the copy boundary"
        elif [ "$GUARD_TIMEOUT" -ne 0 ]; then
            bad rsp-snapshot-race "the checker timed out"
        else
            _fr=$(grep -E '^FAIL\[' "$work/race.out" | sed -n '1p')
            case $_fr in
                "FAIL[arm64-v8a.hermetic"*) ok rsp-snapshot-race "${_fr%%:*} fired first" ;;
                *) bad rsp-snapshot-race "expected the first failure at [arm64-v8a.hermetic]; got: ${_fr:-<none>}" ;;
            esac
        fi
    fi
}

# The response cases must not depend on `shasum`. Starving PATH of everything
# else is the wrong instrument -- it breaks the NDK clang wrapper, which is
# `#!/usr/bin/env bash`, not the hasher. `shasum` is SHADOWED by a failing stub
# instead: any path that hard-codes it fails loudly, while the accepted helper
# selects sha256sum and the response oracle must still fire.
if command -v sha256sum >/dev/null 2>&1; then
    _sd3="$work/noshasum"
    mkdir -p "$_sd3"
    printf '#!/bin/sh\nexit 97\n' > "$_sd3/shasum"
    chmod +x "$_sd3/shasum"
    copystage rsp-hasher-portability m-rspport && s=$STAGE_OUT \
      && guard rsp-hasher-portability "cannot empty the response file" sh -c ": > '$s/arm64-v8a/audit-inputs/link.rsp'" \
      && { # shellcheck disable=SC2031  # the suite's own PATH, unmodified here
           if run_chk "$work/port.out" "$_sd3:$PATH" --self-test "$s"; then
               bad rsp-hasher-portability "the checker passed with shasum shadowed"
           elif [ "$GUARD_TIMEOUT" -ne 0 ]; then
               bad rsp-hasher-portability "the checker timed out"
           else
               _pf=$(grep -E '^FAIL\[' "$work/port.out" | sed -n '1p')
               case $_pf in
                   "FAIL[arm64-v8a.hermetic"*) ok rsp-hasher-portability "with shasum shadowed and sha256sum selected, the response oracle still fires" ;;
                   *) bad rsp-hasher-portability "expected [arm64-v8a.hermetic]; got: ${_pf:-<none>}" ;;
               esac
           fi; }
else
    ok rsp-hasher-portability "this host provides shasum only; the hasher helper is the single point of choice"
fi

echo "== a copystage hash failure is a NAMED SETUP failure =="
# Most mutants route through copystage first, so a hash failure there used to
# be written into the copy as an empty field while copystage returned SUCCESS.
# The checker then rejected the stage at an ARTIFACT oracle
# (FAIL[arm64-v8a.strip]: the declared 'map.sha256' is not a SHA-256),
# attributing a harness fault to the fixture. Both hashers are shadowed with
# failing stubs so the accepted helper genuinely fails.
_nh="$work/nohash"
if guard copystage-hash-failure "cannot build the failing-hasher stubs" mkdir -p "$_nh"; then
    _hok=1
    for _t in sha256sum shasum; do
        printf '#!/bin/sh\nexit 91\n' > "$_nh/$_t" || _hok=0
        chmod +x "$_nh/$_t" || _hok=0
    done
    if [ "$_hok" -eq 0 ]; then
        setup_fail copystage-hash-failure "cannot install the failing-hasher stubs"
    else
        # shellcheck disable=SC2031  # the suite's own PATH, saved and restored here
        _oldp=$PATH
        # shellcheck disable=SC2031  # the suite's own PATH, saved and restored here
        PATH="$_nh:$PATH"; export PATH
        expect_setup_failure copystage-hash-failure copystage probe-copyhash m-copyhash
        PATH=$_oldp; export PATH
    fi
fi

echo "== the builder and the CLI are fail-closed =="
# The builder must never rm -rf a caller-supplied path.
if mkdir -p "$work/pre-existing" 2>/dev/null && : > "$work/pre-existing/keep-me"; then
    if "$mkf" "$work/pre-existing" >/dev/null 2>&1; then
        bad fixture-refuses-existing "the builder accepted a pre-existing destination"
    elif [ -f "$work/pre-existing/keep-me" ]; then
        ok fixture-refuses-existing "a pre-existing destination is refused and left untouched"
    else
        bad fixture-refuses-existing "the builder removed content it did not create"
    fi
else
    setup_fail fixture-refuses-existing "cannot stage the pre-existing destination"
fi

cli_reject() { # cli_reject <case> <args...>
    _c=$1; shift
    if run_chk "$work/cli" - "$@"; then
        bad "$_c" "the CLI accepted a malformed invocation"
    elif grep -q 'usage:' "$work/cli"; then
        ok "$_c" "named usage failure: $(sed -n '1p' "$work/cli")"
    else
        bad "$_c" "rejected, but with no named usage failure: $(sed -n '1p' "$work/cli")"
    fi
}
cli_reject cli-missing-value    --dep-stage
cli_reject cli-duplicate-option --self-test --self-test
cli_reject cli-unknown-option   --not-an-option
cli_reject cli-two-roots        "$work/one" "$work/two"

echo "== the ledger itself must fail a prevented setup =="
# Deliberately break one mutant's setup and prove the ledger reports it by NAME
# rather than silently running fewer cases.
_probe=$(
    bad() { printf 'MISS[%s]: %s\n' "$1" "$2"; }
    setup_fail() { bad "$1" "SETUP FAILED: $2"; }
    guard probe-case "a deliberately prevented setup" false
    printf 'guard-returned=%s\n' "$?"
) 2>&1
case $_probe in
    *"MISS[probe-case]: SETUP FAILED: a deliberately prevented setup"*"guard-returned=1"*)
        ok ledger-self-check "a prevented setup is reported by case name and returns non-zero" ;;
    *)  bad ledger-self-check "a prevented setup was not reported by name: $_probe" ;;
esac

echo
_exp=$(printf '%s\n' "$EXPECTED" | tr ' ' '\n' | awk 'NF' | sort)
_got=$(printf '%s\n' "$ran" | tr ' ' '\n' | awk 'NF' | sort)
_expn=$(printf '%s\n' "$_exp" | awk 'END { print NR }')
_gotn=$(printf '%s\n' "$_got" | awk 'END { print NR }')
_dup=$(printf '%s\n' "$_got" | uniq -d | sed -n '1p')
_missing=$(printf '%s\n' "$_exp" | grep -vxF -- "$_got" | tr '\n' ' ' | sed 's/ *$//')
_unexpected=$(printf '%s\n' "$_got" | grep -vxF -- "$_exp" | tr '\n' ' ' | sed 's/ *$//')
_ledger=0
[ -n "$_dup" ] && { printf 'LEDGER: case ran more than once: %s\n' "$_dup"; _ledger=1; }
[ -n "$_missing" ] && { printf 'LEDGER: case(s) in the ledger never ran: %s\n' "$_missing"; _ledger=1; }
[ -n "$_unexpected" ] && { printf 'LEDGER: case(s) ran but are not in the ledger: %s\n' "$_unexpected"; _ledger=1; }
[ "$_expn" -eq "$_gotn" ] || { printf 'LEDGER: %s case(s) ran, %s declared\n' "$_gotn" "$_expn"; _ledger=1; }
printf 'self-test: %s kill(s), %s miss(es), %s/%s ledger cases\n' "$kills" "$misses" "$_gotn" "$_expn"
{ [ "$misses" -eq 0 ] && [ "$_ledger" -eq 0 ]; } || exit 1
exit 0
