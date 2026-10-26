#!/bin/sh
# Phase 0.3 one-DSO topology contract.
#
# Audits a generated topology stage: one self-contained libmoq5_jni.so per ABI,
# LibMoQ plus the selected picoquic/picotls/OpenSSL closure linked in as PIC
# static archives, only JNI_OnLoad exported.
#
# What is ARTIFACT evidence and what is a RECIPE obligation is kept distinct and
# is stated in every row that touches the distinction:
#
#   * absence of DT_TEXTREL proves the FINAL IMAGE carries no text relocation.
#     It does NOT prove every selected member was compiled position-independent.
#   * `-z text` and `-z defs` cannot be recovered from an ELF at all. They are
#     GREEN-recipe obligations whose removal must fail a recipe mutant, not this
#     checker.
#
# Exit 0 only when every oracle passed for BOTH ABIs.
set -u
LC_ALL=C; export LC_ALL

here=$(cd "$(dirname "$0")" && pwd) || { echo "cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)        || { echo "cannot resolve bindings/kotlin" >&2; exit 2; }
repo=$(cd "$root/../.." && pwd)     || { echo "cannot resolve the worktree root" >&2; exit 2; }
. "$here/lib-declaration.sh"
. "$here/lib-inputs.sh"

usage() {
    echo "usage: $(basename "$0") [--self-test] [--dep-stage DIR] [--repo DIR] [STAGE]" >&2
    exit 2
}

# ---- fail-closed CLI: missing values, duplicates, unknown options, and more
# ---- than one positional root are NAMED usage failures, never `set -u` noise.
SELFTEST=0; stage=""; dep_stage=""; repo_override=""
_seen_selftest=0; _seen_dep=0; _seen_repo=0; _seen_pos=0
while [ $# -gt 0 ]; do
    case $1 in
        --self-test)
            [ "$_seen_selftest" -eq 0 ] || { echo "duplicate --self-test" >&2; usage; }
            _seen_selftest=1; SELFTEST=1; shift ;;
        --dep-stage)
            [ "$_seen_dep" -eq 0 ] || { echo "duplicate --dep-stage" >&2; usage; }
            [ $# -ge 2 ] || { echo "--dep-stage requires a directory" >&2; usage; }
            _seen_dep=1; dep_stage=$2; shift 2 ;;
        --repo)
            [ "$_seen_repo" -eq 0 ] || { echo "duplicate --repo" >&2; usage; }
            [ $# -ge 2 ] || { echo "--repo requires a directory" >&2; usage; }
            _seen_repo=1; repo_override=$2; shift 2 ;;
        --help|-h) usage ;;
        --*) echo "unknown option: $1" >&2; usage ;;
        *)
            [ "$_seen_pos" -eq 0 ] || { echo "more than one stage root given" >&2; usage; }
            _seen_pos=1; stage=$1; shift ;;
    esac
done
[ -n "$stage" ]     || stage="$root/.native/_topology"
[ -n "$dep_stage" ] || dep_stage="$root/.native"
if [ -n "$repo_override" ]; then
    [ "$SELFTEST" -eq 1 ] || { echo "--repo is accepted only with --self-test" >&2; usage; }
    repo=$(cd "$repo_override" 2>/dev/null && pwd) || { echo "--repo is not a readable directory" >&2; exit 2; }
fi

# props_one <file> <key> -- the value of EXACTLY ONE canonical definition.
# Built on the accepted declaration primitives; a key defined zero or twice is
# a failure, never a silently-taken first value.
props_one() {
    _pc=$(props_count "$1" "$2") || return 1
    [ "$_pc" = "1" ] || return 1
    props_value "$1" "$2"
}

_TAB=$(printf '\t')
fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
note() { printf 'NOTE[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }
finish() {
    printf '\none-DSO topology contract: %s failure(s)\n' "$fails"
    [ "$fails" -eq 0 ] || exit 1
    exit 0
}

# ---- complete tool preflight: every external command this script invokes ----
for t in awk sed grep sort uniq comm tr cut head od find dirname basename cmp readlink \
         mktemp rm cp mkdir printf; do
    command -v "$t" >/dev/null 2>&1 || { fail preflight "required tool not found: $t"; }
done
command -v sha256sum >/dev/null 2>&1 || command -v shasum >/dev/null 2>&1 \
    || fail preflight "no SHA-256 hasher (sha256sum or shasum) is available"
# The sourced helpers are preflighted too: a missing one must be a NAMED
# failure, not a raw "command not found" on the first use.
for f in decl_sha256 decl_is_sha256 decl_validate_grammar props_count props_value \
         props_one inputs_manifest inputs_digest inputs_families_covered; do
    command -v "$f" >/dev/null 2>&1 || fail preflight "required helper function not available: $f"
done
[ "$fails" -eq 0 ] || finish

# ---- the declared closure, re-derived from current source --------------------
# service PUBLIC-links core and msf (service/CMakeLists.txt:19); enabling the
# service requires media-object (CMakeLists.txt:256-263); msf links media-object
# and CMAF (media/msf/CMakeLists.txt:18-22); media-object links core, LOC and
# CMAF (media/object/CMakeLists.txt:13). pico_wt requires picohttp-core, and
# fails the configure outright without it (adapters/pico_wt/CMakeLists.txt:15-21).
#
# The two PicoQUIC facades are in this closure because the ANDROID CANDIDATE
# opts into both service facades -- RAW_QUIC and WebTransport -- not because the
# project builds them by default. Every one of these is default-OFF at the
# project level: MOQ_BUILD_ADAPTER_PICOQUIC (CMakeLists.txt:12),
# MOQ_BUILD_SERVICE (:25), MOQ_BUILD_PQ_THREADED (:28),
# MOQ_BUILD_ADAPTER_PICO_WT (:33) and MOQ_BUILD_PICO_WT_MANAGED (:42), with
# MOQ_BUILD_MSF (:15) likewise OFF. The Android configure must enable them
# explicitly, and this manifest is what that opt-in is required to produce.
# The Phase 0.2 dependency outputs split in two, and the split is part of the
# contract rather than a link-order accident. DEP_ARCHIVES is the LINK closure:
# every one of these must contribute a member. DEP_PINNED_ARCHIVES are Phase 0.2
# outputs that are still verified by path and hash but are NOT linked, and must
# never contribute or appear in a response file.
#
# Two outputs are pinned-only. libssl.a: picotls provides TLS 1.3 and OpenSSL is
# used solely as the crypto provider, so the OpenSSL record layer is
# unreferenced. libpicoquic-log.a: picoquic-core carries the qlog streaming
# functions, picoquic_set_qlog among them (see the qlog row), so nothing the
# image reaches is defined only in the log archive. Both are declared under the
# `dependency.` key family, never `archive.`, so "hashed Phase 0.2 output" and
# "linked archive" cannot be confused (1171 ruling C).
DEP_ARCHIVES="libcrypto.a libpicotls-core.a libpicotls-openssl.a \
libpicotls-minicrypto.a libpicoquic-core.a libpicohttp-core.a"
DEP_PINNED_ARCHIVES="libssl.a libpicoquic-log.a"
# The picotls archives a staged picotls prefix carries: exactly the ones the
# link pins. Declared here independently of the recipe's helper, which this
# checker does not source.
PTLS_PREFIX_ARCHIVES="libpicotls-core.a libpicotls-minicrypto.a libpicotls-openssl.a"

# ptls_suppliers_ok <verdict-file> <what>: every "supplier <h>=<dir>" line names
# the staged prefix's include directory ($_ppp/include), and there is at least
# one. Sets _psbad on failure.
ptls_suppliers_ok() {
    _psbad=""
    _pserr=$(sed -n 's/^error=//p' "$1")
    [ -z "$_pserr" ] || { _psbad=$_pserr; return 1; }
    _psn=0
    while IFS= read -r _psl; do
        [ -n "$_psl" ] || continue
        case $_psl in
            "supplier "*=*) ;;
            *) _psbad="the header parser wrote an unexpected verdict: $_psl"; return 1 ;;
        esac
        _psh=${_psl#supplier }; _psh=${_psh%%=*}; _psd=${_psl#*=}
        [ "$_psd" = "$_ppp/include" ] \
            || { _psbad="$2 finds $_psh first in '$_psd', not the staged $_ppp/include"; return 1; }
        _psn=$((_psn + 1))
    done < "$1"
    [ "$_psn" -ge 1 ] || { _psbad="the header parser gave no verdict for $2"; return 1; }
    return 0
}
MOQ_ARCHIVES="libmoq-core.a libmoq-service.a libmoq-msf.a libmoq-media-object.a \
libmoq-loc.a libmoq-cmaf.a libmoq-adapter-picoquic.a \
libmoq-adapter-picoquic-threaded.a libmoq-adapter-pico-wt.a \
libmoq-adapter-pico-wt-managed.a libmoq-codec-signaling.a"
# codec-signaling is REQUIRED: the JNI codec rows call moq_codec_init_data_build
# and moq_codec_string_format (and their cfg initializers), and the archive
# PUBLIC-links only core (media/codec_signaling/CMakeLists.txt), so it adds no
# dependency beyond the closure above.
#
# A real LibMoQ archive OUTSIDE the product closure, named so that a
# contribution from it is rejected by name as well as by the exact-path
# allowlist below: moq-playback (MOQ_BUILD_PLAYBACK, default OFF) is a media
# library the Android product does not ship.
FORBIDDEN_ARCHIVES="libmoq-playback.a"
WANT_EXPORT=JNI_OnLoad
# The product DT_NEEDED authority, frozen by ruling 1171 A for both ABIs. It was
# DERIVED from the first real product link, read with the pinned NDK
# llvm-readelf over the packaged image: every undefined dynamic symbol there
# resolves to bionic libc except dlopen/dlsym/dlclose/dlerror, which OpenSSL's
# provider machinery draws from libdl.
WANT_NEEDED_PRODUCT="libc.so libdl.so"
# The recipe-owned container, relative to the Phase 0.2 dependency root, that a
# product build root must live inside (ruling 1171 B). It is a SIBLING of the
# audited stage: an intermediate build tree inside the stage would enter the
# stage's exact path manifest and break the `stage` oracle.
PRODUCT_BUILD_CONTAINER="_topology.build"
# Android's 16 KiB page-size floor for native LOAD segments, as a plain integer
# so the comparison never depends on how a producer formats the field.
MIN_LOAD_ALIGN=16384
ABIS="arm64-v8a x86_64"

# The seven licence components, each mapped to ONE exact canonical source under
# an independently pinned root. A stage-authored `source` path is never trusted
# merely because the packaged bytes happen to match it (1153 R10).
#   picotls ships NO top-level LICENSE in the pinned tree; its MIT statement is
#   in README.md, and that exact fact is what is recorded -- the row is not
#   called a LICENSE file.
LICENSE_COMPONENTS="libmoq picoquic picotls openssl cifra micro-ecc moq-msf-json"
license_source() {
    case $1 in
        libmoq)       printf '%s' "$repo/LICENSE.txt" ;;
        picoquic)     printf '%s' "$dep_stage/_src/picoquic/LICENSE" ;;
        picotls)      printf '%s' "$dep_stage/_src/picotls/README.md" ;;
        openssl)      printf '%s' "$dep_stage/_src/openssl/LICENSE.txt" ;;
        cifra)        printf '%s' "$dep_stage/_src/picotls/deps/cifra/COPYING" ;;
        micro-ecc)    printf '%s' "$dep_stage/_src/picotls/deps/micro-ecc/LICENSE.txt" ;;
        moq-msf-json) printf '%s' "$repo/media/msf/THIRD_PARTY.md" ;;
        *) return 1 ;;
    esac
}

# ---- the durable stage must not be under mutation ----------------------------
# selftest-product-contract.sh mutates the durable stage in place and restores
# it under a trap. A run that overlapped that window would audit a deliberately
# broken stage and report an oracle failure that describes the mutant, not the
# product -- a false result that looks exactly like a real one. It is refused by
# name instead (chats/libmoq/1175).
#
# The lock is a SIBLING of the stage: the stage's path manifest is exact, so a
# lock inside it would itself be an unclassified path.
#
# The holder is admitted, because the holder is precisely the caller that means
# to audit the mutated stage. It proves it holds the lock by presenting the
# token the lock records; that token is minted at acquisition and never written
# anywhere else, so presenting it is not a flag a bystander can pass.
# There is deliberately NO waiting: this is a refusal guard, not a scheduler.
_lockdir="$dep_stage/_topology.lock"
if [ -d "$_lockdir" ]; then
    _lt=""
    [ -f "$_lockdir/token" ] && _lt=$(cat "$_lockdir/token" 2>/dev/null)
    if [ -z "${MOQ5_TOPOLOGY_LOCK_TOKEN-}" ] || [ -z "$_lt" ] \
       || [ "${MOQ5_TOPOLOGY_LOCK_TOKEN-}" != "$_lt" ]; then
        fail stage "the durable topology stage is locked for mutation ($_lockdir$([ -f "$_lockdir/owner" ] && printf ', held by %s' "$(cat "$_lockdir/owner" 2>/dev/null)")): auditing it now would describe a mutant rather than the product. Re-run when the holder has finished; if no such run is active, the lock is stale and may be removed."
        finish
    fi
fi

# ---- topology-root totality: the stage as a WHOLE, not per ABI ---------------
# A recipe that writes its audited stage one level below the path the checker
# audits would leave this checker inspecting an empty container while a complete
# stage sat unexamined underneath it. That is named, not merely missed, because
# "no stage here" and "the stage is nested" call for different corrections
# (chats/libmoq/1171 blocker 1).
if [ -d "$stage/stage" ] && [ -f "$stage/stage/$(printf '%s' "$ABIS" | cut -d' ' -f1)/topology.provenance" ]; then
    fail stage "the audited path $stage holds a NESTED stage at $stage/stage: the recipe and the checker disagree about which directory is the audited stage"
    finish
fi
if [ ! -d "$stage" ]; then
    fail stage "no topology stage at $stage: Phase 0.3 has no one-DSO build recipe yet, so nothing has produced a packaged JNI library"
    finish
fi

# Task-owned scratch for reproduced artifacts and manifests.
scratch=$(mktemp -d "${TMPDIR:-/tmp}/moq5-topology-audit-XXXXXXXX") \
    || { fail stage "cannot create a task-owned scratch directory"; finish; }
# shellcheck disable=SC2329  # invoked from the trap below
cleanup() { _rc=$1; [ -n "${scratch:-}" ] && rm -rf -- "$scratch"; [ "$_rc" -ge 0 ] && exit "$_rc"; }
trap 'cleanup 130' HUP INT TERM
trap 'rm -rf -- "$scratch"' EXIT

# The compile-database oracle for the staged picotls prefix: CMake's own JSON
# parser and quote-aware tokenizer, written into this run's scratch.
cmake=$(command -v cmake) || { fail preflight "required tool not found: cmake"; cmake=cmake; }
cat > "$scratch/ptls_supplier.cmake" <<'PTLS_SUPPLIER_EOF' \
    || fail preflight "cannot write the compile-database oracle"
# Which directory first supplies each picotls header to a compile.
#
#   -DCC_JSON=<compile database: a JSON array of entries> -DSOURCE_SUFFIX=<path suffix>
#   [-DEXPECT_FILE=<real path> -DEXPECT_OUTPUT=<real path>]  the database must
#       then hold exactly ONE entry, for exactly that source, writing exactly
#       that object (relative fields resolve against the entry's directory)
#   -DKIND=quote|angle     how the source includes the headers
#   -DHEADERS=<h1>|<h2>... headers; a header written "<h>!<MACRO>" is included
#                          only when the command does not define MACRO
#   -DOUT=<file>
#
# Writes "supplier <header>=<real directory>" per header, or one
# "error=<reason>". The search is the compiler's, by CLASS: for a quoted include
# the source's directory, then every -iquote, every -I, every -isystem, every
# -idirafter; for an angle include the same without the first two. Every
# matching compile must agree. Anything that changes the search in a way this
# does not model is refused rather than judged.
cmake_minimum_required(VERSION 3.20)
function(_emit text)
    file(WRITE "${OUT}" "${text}")
endfunction()
string(REPLACE "|" ";" _headers "${HEADERS}")

# _judge <file> <dir> <args-list-var> -> sets _verdict (lines) or _error
function(_judge file dir argsvar)
    set(_iquote "")
    set(_i "")
    set(_isystem "")
    set(_idirafter "")
    set(_defs "")
    set(_next "")
    foreach(_a IN LISTS ${argsvar})
        if(_next)
            list(APPEND ${_next} "${_a}")
            set(_next "")
            continue()
        endif()
        if(_a MATCHES "^@" OR _a STREQUAL "-I-" OR _a MATCHES "^-iprefix"
           OR _a MATCHES "^-iwithprefix" OR _a MATCHES "^--include-directory"
           OR _a MATCHES "^-nostdinc" OR _a MATCHES "^-include" OR _a MATCHES "^-imacros")
            set(_error "the compile of ${file} uses '${_a}', which changes the include search in a way this oracle does not model" PARENT_SCOPE)
            return()
        endif()
        if(_a MATCHES "^-D([A-Za-z_][A-Za-z0-9_]*)")
            list(APPEND _defs "${CMAKE_MATCH_1}")
            continue()
        endif()
        set(_hit FALSE)
        foreach(_cls iquote isystem idirafter)
            if(_a STREQUAL "-${_cls}")
                set(_next "_${_cls}")
                set(_hit TRUE)
            elseif(_a MATCHES "^-${_cls}(.+)$")
                list(APPEND _${_cls} "${CMAKE_MATCH_1}")
                set(_hit TRUE)
            endif()
        endforeach()
        if(_hit)
            continue()
        endif()
        if(_a STREQUAL "-I")
            set(_next _i)
        elseif(_a MATCHES "^-I(.+)$")
            list(APPEND _i "${CMAKE_MATCH_1}")
        endif()
    endforeach()
    if(_next)
        set(_error "the compile of ${file} ends with a dangling include option" PARENT_SCOPE)
        return()
    endif()
    get_filename_component(_srcdir "${file}" DIRECTORY)
    if(KIND STREQUAL "quote")
        set(_search "${_srcdir}" ${_iquote} ${_i} ${_isystem} ${_idirafter})
    elseif(KIND STREQUAL "angle")
        set(_search ${_i} ${_isystem} ${_idirafter})
    else()
        set(_error "unknown include kind '${KIND}'" PARENT_SCOPE)
        return()
    endif()
    set(_lines "")
    foreach(_hspec IN LISTS _headers)
        set(_h "${_hspec}")
        if(_hspec MATCHES "^(.+)!(.+)$")
            set(_h "${CMAKE_MATCH_1}")
            if("${CMAKE_MATCH_2}" IN_LIST _defs)
                continue()
            endif()
        endif()
        set(_first "")
        foreach(_d IN LISTS _search)
            if(NOT IS_ABSOLUTE "${_d}")
                set(_d "${dir}/${_d}")
            endif()
            if(EXISTS "${_d}/${_h}")
                file(REAL_PATH "${_d}" _first)
                break()
            endif()
        endforeach()
        if(NOT _first)
            set(_error "no include directory of the compile of ${file} supplies ${_h}" PARENT_SCOPE)
            return()
        endif()
        string(APPEND _lines "supplier ${_h}=${_first}\n")
    endforeach()
    set(_verdict "${_lines}" PARENT_SCOPE)
endfunction()

set(_verdicts "")
set(_matched 0)
if(DEFINED CC_JSON)
    if(NOT EXISTS "${CC_JSON}")
        _emit("error=no compile database at ${CC_JSON}\n")
        return()
    endif()
    file(READ "${CC_JSON}" _json)
    string(JSON _type ERROR_VARIABLE _jerr TYPE "${_json}")
    if(_jerr OR NOT _type STREQUAL "ARRAY")
        _emit("error=the compile database is not a JSON array\n")
        return()
    endif()
    string(JSON _n LENGTH "${_json}")
    if(DEFINED EXPECT_FILE AND NOT _n EQUAL 1)
        _emit("error=the compile record holds ${_n} entries, not exactly one\n")
        return()
    endif()
    if(_n GREATER 0)
        math(EXPR _last "${_n} - 1")
        foreach(_k RANGE ${_last})
            string(JSON _file ERROR_VARIABLE _e1 GET "${_json}" ${_k} file)
            if(_e1)
                _emit("error=compile database entry ${_k} has no 'file'\n")
                return()
            endif()
            string(JSON _dir ERROR_VARIABLE _e2 GET "${_json}" ${_k} directory)
            if(_e2)
                _emit("error=compile database entry ${_k} has no 'directory'\n")
                return()
            endif()
            if(NOT IS_ABSOLUTE "${_file}")
                set(_file "${_dir}/${_file}")
            endif()
            if(DEFINED EXPECT_FILE)
                if(NOT EXISTS "${_file}")
                    _emit("error=the compile record's source ${_file} does not exist\n")
                    return()
                endif()
                file(REAL_PATH "${_file}" _file_real)
                if(NOT _file_real STREQUAL EXPECT_FILE)
                    _emit("error=the compile record is for ${_file_real}, not the JNI source ${EXPECT_FILE}\n")
                    return()
                endif()
                string(JSON _out ERROR_VARIABLE _e5 GET "${_json}" ${_k} output)
                if(_e5)
                    _emit("error=the compile record names no 'output'\n")
                    return()
                endif()
                if(NOT IS_ABSOLUTE "${_out}")
                    set(_out "${_dir}/${_out}")
                endif()
                # The object itself is consumed by the link and not retained, so
                # its DIRECTORY is resolved physically and its name compared.
                get_filename_component(_odir "${_out}" DIRECTORY)
                get_filename_component(_oname "${_out}" NAME)
                if(NOT IS_DIRECTORY "${_odir}")
                    _emit("error=the compile record's output directory ${_odir} does not exist\n")
                    return()
                endif()
                file(REAL_PATH "${_odir}" _odir_real)
                if(NOT "${_odir_real}/${_oname}" STREQUAL EXPECT_OUTPUT)
                    _emit("error=the compile record writes ${_odir_real}/${_oname}, not the object the final link consumed (${EXPECT_OUTPUT})\n")
                    return()
                endif()
            endif()
            if(NOT _file MATCHES "/${SOURCE_SUFFIX}$")
                continue()
            endif()
            math(EXPR _matched "${_matched} + 1")
            string(JSON _cmd ERROR_VARIABLE _e3 GET "${_json}" ${_k} command)
            if(_e3)
                string(JSON _alen ERROR_VARIABLE _e4 LENGTH "${_json}" ${_k} arguments)
                if(_e4)
                    _emit("error=the ${SOURCE_SUFFIX} entry ${_k} has neither 'command' nor 'arguments'\n")
                    return()
                endif()
                set(_args "")
                math(EXPR _alast "${_alen} - 1")
                foreach(_ai RANGE ${_alast})
                    string(JSON _av GET "${_json}" ${_k} arguments ${_ai})
                    list(APPEND _args "${_av}")
                endforeach()
            else()
                separate_arguments(_args UNIX_COMMAND "${_cmd}")
            endif()
            set(_error "")
            set(_verdict "")
            _judge("${_file}" "${_dir}" _args)
            if(_error)
                _emit("error=${_error}\n")
                return()
            endif()
            list(APPEND _verdicts "${_verdict}")
        endforeach()
    endif()
else()
    _emit("error=no compile input was given\n")
    return()
endif()
if(_matched EQUAL 0)
    _emit("error=no compile of ${SOURCE_SUFFIX} was found\n")
    return()
endif()
list(REMOVE_DUPLICATES _verdicts)
list(LENGTH _verdicts _nv)
if(NOT _nv EQUAL 1)
    _emit("error=the ${_matched} compiles of ${SOURCE_SUFFIX} disagree on which directories supply the picotls headers\n")
    return()
endif()
_emit("${_verdicts}")
PTLS_SUPPLIER_EOF

# Every entry under the topology root is classified by relative path AND TYPE.
# Anything else -- an extra ABI directory, a top-level file, a symlink, a FIFO,
# a socket, a device -- is rejected. `find -type f` alone made all of these
# invisible (1153 R4).
# Producers are captured to files and their status checked BEFORE any parser
# runs: a failed producer must never become an empty parsed result (1155 R5).
if ! find "$stage" ! -type d ! -type f -print > "$scratch/special" 2>/dev/null; then
    fail stage "cannot inventory the topology stage for non-regular entries"; finish
fi
_special=$(sed -n '1p' "$scratch/special")
[ -z "$_special" ] || { fail stage "non-regular entry in the topology stage (symlink, FIFO, socket or device): $_special"; finish; }
# A HARDLINK is a regular file, so `-type f` accepts one whose bytes live
# outside the stage. Stage ownership means exactly one link (1155 R7).
if ! find "$stage" -type f -links +1 -print > "$scratch/hardlinks" 2>/dev/null; then
    fail stage "cannot inventory the topology stage for hardlinks"; finish
fi
_hl=$(sed -n '1p' "$scratch/hardlinks")
[ -z "$_hl" ] || { fail stage "a staged file has more than one hard link, so its bytes are not owned by this stage: $_hl"; finish; }
if ! ( cd "$stage" && find . -mindepth 1 -maxdepth 1 -print ) > "$scratch/toproot" 2>/dev/null; then
    fail stage "cannot inventory the topology root"; finish
fi
_toproot=$(sed 's|^\./||' "$scratch/toproot" | sort)
_topbad=""
for e in $_toproot; do
    case " $ABIS " in
        *" $e "*) [ -d "$stage/$e" ] || _topbad="'$e' is an ABI name but not a directory" ;;
        *)        _topbad="unclassified entry at the topology root: $e" ;;
    esac
    [ -z "$_topbad" ] || break
done
[ -z "$_topbad" ] || { fail stage "$_topbad"; finish; }
for a in $ABIS; do
    [ -d "$stage/$a" ] || { fail stage "missing ABI directory: $a"; finish; }
done
_topn=$(printf '%s\n' "$_toproot" | awk 'NF { n++ } END { print n + 0 }')
_abin=$(printf '%s\n' "$ABIS" | tr ' ' '\n' | awk 'NF { n++ } END { print n + 0 }')
[ "$_topn" -eq "$_abin" ] \
    || { fail stage "the topology root holds $_topn entr(ies); exactly $_abin ABI directories are expected"; finish; }

# ---- the exact provenance key schema (1153 R12) ------------------------------
PROV_KEYS_COMMON="mode abi android.api ndk.version ndk.cc ndk.target \
picoquic.commit picotls.commit openssl.commit libmoq.inputs.sha256 \
libmoq.tooling.sha256 \
compiler.runtime.path compiler.runtime.sha256 dso.sha256 diagnostic.sha256 \
map.sha256 receipt.sha256 \
responses.sha256 ptls.prefix ptls.prefix.manifest.sha256"
PROV_KEYS_SELFTEST="needed.selftest libmoq.inputs.root"
# Only the product recipe compiles the JNI substrate on its own, so only a
# product stage carries the compiler's record of that compile.
PROV_KEYS_PRODUCT="jni.compile.sha256"
# The synthetic probe that exercises the PRODUCT digest branch. It is accepted
# only under --self-test, always prints the synthetic NOTE, and is never spelled
# `product` -- a synthetic stage must not be able to declare itself the product
# topology (chats/libmoq/1155 R10).
PROV_KEYS_PROBE="libmoq.inputs.root"

prov_schema_ok() {
    # $1 stage provenance, $2 mode -> echoes a diagnostic on failure
    _pp=$1; _pm=$2
    _want="$PROV_KEYS_COMMON"
    for _a in $DEP_ARCHIVES $MOQ_ARCHIVES; do _want="$_want archive.$_a"; done
    for _a in $DEP_PINNED_ARCHIVES; do _want="$_want dependency.$_a"; done
    # Every stage declares its build root: it is what bounds the intermediate
    # build tree, and a stage that does not name one cannot be audited for it.
    _want="$_want build.root"
    [ "$_pm" = selftest ] && _want="$_want $PROV_KEYS_SELFTEST"
    [ "$_pm" = product ] && _want="$_want $PROV_KEYS_PRODUCT"
    [ "$_pm" = selftest-product-probe ] && _want="$_want $PROV_KEYS_PROBE"
    _lines=$(awk 'NF && $0 !~ /^[[:space:]]*#/ { n++ } END { print n + 0 }' "$_pp") || {
        printf 'cannot read the topology provenance'; return 1; }
    _keys=$(sed -n "s/^\([A-Za-z0-9._-]*\)=.*/\1/p" "$_pp" | sort)
    _kn=$(printf '%s\n' "$_keys" | awk 'NF { n++ } END { print n + 0 }')
    [ "$_kn" -eq "$_lines" ] || { printf 'the provenance holds %s non-comment line(s) but only %s parse as key=value' "$_lines" "$_kn"; return 1; }
    _dup=$(printf '%s\n' "$_keys" | uniq -d | sed -n '1p')
    [ -z "$_dup" ] || { printf 'duplicate provenance key: %s' "$_dup"; return 1; }
    for _k in $_want; do
        printf '%s\n' "$_keys" | grep -qx -- "$_k" || { printf 'missing provenance key: %s' "$_k"; return 1; }
    done
    _wantsorted=$(printf '%s\n' "$_want" | tr ' ' '\n' | awk 'NF' | sort)
    _extra=$(printf '%s\n' "$_keys" | grep -vxF -- "$_wantsorted" | sed -n '1p')
    [ -z "$_extra" ] || { printf 'unknown provenance key: %s' "$_extra"; return 1; }
    return 0
}

for abi in $ABIS; do
    # Per-ABI failure baseline: an ARM failure must never suppress an x86_64
    # producer, parser or assertion (1153 R6).
    abi_fails0=$fails
    abi_bad() { [ "$fails" -gt "$abi_fails0" ]; }

    adir="$stage/$abi"
    tprov="$adir/topology.provenance"
    p02="$dep_stage/$abi/provenance.properties"
    dep="$dep_stage/$abi"
    case $abi in arm64-v8a) triple=aarch64-linux-android; machine=elf64-littleaarch64 ;;
                 x86_64)    triple=x86_64-linux-android;  machine=elf64-x86-64 ;; esac

    [ -f "$tprov" ] || { fail "$abi.provenance" "no topology.provenance: a stage that does not say how it was built is self-attestation"; continue; }
    [ -f "$p02" ]   || { fail "$abi.provenance" "no accepted Phase 0.2 provenance at $p02"; continue; }

    # The ACCEPTED property grammar runs before any value is read, so a value
    # carrying trailing whitespace, an escape or a non-ASCII byte is rejected
    # here rather than normalised away by later word splitting (1155 R2).
    _gout="$scratch/grammar.$abi"
    if ! decl_validate_grammar "$tprov" > "$_gout" 2>&1; then
        fail "$abi.provenance" "topology provenance fails the accepted property grammar: $(sed -n '1p' "$_gout" | cut -c1-140)"
        continue
    fi

    tmode=$(props_one "$tprov" mode) || { fail "$abi.provenance" "topology provenance declares no single 'mode'"; continue; }
    case $tmode in
        product) ;;
        selftest|selftest-product-probe)
            [ "$SELFTEST" -eq 1 ] || { fail "$abi.provenance" "this stage declares mode=$tmode; auditing it requires an explicit --self-test so a synthetic fixture can never be reported as the LibMoQ topology"; continue; }
            note "$abi.selftest" "SYNTHETIC STAGE ($tmode) -- checker non-vacuity evidence only, NOT the LibMoQ topology" ;;
        *) fail "$abi.provenance" "unknown provenance mode '$tmode'"; continue ;;
    esac

    _sch=$(prov_schema_ok "$tprov" "$tmode") || { fail "$abi.provenance" "$_sch"; continue; }

    # agreement with the ALREADY-ACCEPTED Phase 0.2 provenance
    _pmis=""
    for k in abi android.api ndk.version ndk.cc picoquic.commit picotls.commit openssl.commit; do
        _t=$(props_one "$tprov" "$k") || { _pmis="topology provenance has no single '$k'"; break; }
        _p=$(props_one "$p02"  "$k") || { _pmis="Phase 0.2 provenance has no single '$k'"; break; }
        [ "$_t" = "$_p" ] || { _pmis="'$k' is '$_t' here but '$_p' in the accepted Phase 0.2 stage"; break; }
    done
    [ -z "$_pmis" ] || { fail "$abi.provenance" "$_pmis"; continue; }
    # Taken in assignment form so a zero-or-duplicate 'abi' is reported as the
    # producer failure it is, rather than as a mismatch against an empty value.
    _pabi=$(props_one "$tprov" abi) \
        || { fail "$abi.provenance" "topology provenance declares no single 'abi'"; continue; }
    [ "$_pabi" = "$abi" ] || { fail "$abi.provenance" "provenance declares ABI '$_pabi', not '$abi'"; continue; }
    api=$(props_one "$p02" android.api) || { fail "$abi.provenance" "no android.api"; continue; }
    cc=$(props_one "$p02" ndk.cc)       || { fail "$abi.provenance" "no ndk.cc"; continue; }
    [ -x "$cc" ] || { fail "$abi.provenance" "the declared compiler is not executable: $cc"; continue; }
    bin=$(dirname "$cc") || { fail "$abi.provenance" "cannot resolve the toolchain bin directory"; continue; }
    ndkroot=$(cd "$bin/../../../.." 2>/dev/null && pwd) || { fail "$abi.provenance" "cannot resolve the pinned NDK root"; continue; }
    readelf="$bin/llvm-readelf"; nm="$bin/llvm-nm"; ar="$bin/llvm-ar"
    objdump="$bin/llvm-objdump"; strip="$bin/llvm-strip"
    _tmis=""
    for t in "$readelf" "$nm" "$ar" "$objdump" "$strip"; do
        [ -x "$t" ] || { _tmis="$t"; break; }
    done
    [ -z "$_tmis" ] || { fail "$abi.provenance" "pinned NDK tool missing or not executable: $_tmis"; continue; }

    # dependency archive hashes: RECOMPUTED, never trusted
    _amis=""
    for a in $DEP_ARCHIVES $MOQ_ARCHIVES; do
        case " $DEP_ARCHIVES " in
            *" $a "*) _p="$dep/lib/$a" ;;
            *)        _p="$adir/audit-inputs/$a" ;;
        esac
        [ -f "$_p" ] || { _amis="$a is not present in the dependency stage $dep/lib"; break; }
        [ -s "$_p" ] || { _amis="$a is empty"; break; }
        _h=$(decl_sha256 "$_p") || { _amis="cannot hash $a (no usable hasher, or the read failed)"; break; }
        _c=$(props_one "$tprov" "archive.$a") || { _amis="topology provenance has no single 'archive.$a' hash"; break; }
        decl_is_sha256 "$_c" || { _amis="the declared hash for $a is not a SHA-256"; break; }
        [ "$_h" = "$_c" ] || { _amis="$a hashes $_h but the topology provenance claims $_c"; break; }
    done
    for a in $DEP_PINNED_ARCHIVES; do
        _p="$dep/lib/$a"
        [ -f "$_p" ] || { _amis="$a is not present in the dependency stage $dep/lib"; break; }
        [ -s "$_p" ] || { _amis="$a is empty"; break; }
        _h=$(decl_sha256 "$_p") || { _amis="cannot hash $a (no usable hasher, or the read failed)"; break; }
        _c=$(props_one "$tprov" "dependency.$a") || { _amis="topology provenance has no single 'dependency.$a' hash"; break; }
        decl_is_sha256 "$_c" || { _amis="the declared hash for $a is not a SHA-256"; break; }
        [ "$_h" = "$_c" ] || { _amis="$a hashes $_h but the topology provenance claims $_c"; break; }
    done
    [ -z "$_amis" ] || { fail "$abi.provenance" "$_amis"; continue; }

    # LibMoQ input digest: SHA-256 of a manifest of path + CONTENT hash, always
    # recomputed. The earlier revision hashed path names, so an in-place edit
    # left it unchanged (1153 R1).
    _wantdig=$(props_one "$tprov" libmoq.inputs.sha256) \
        || { fail "$abi.provenance" "topology provenance has no single 'libmoq.inputs.sha256'"; continue; }
    decl_is_sha256 "$_wantdig" || { fail "$abi.provenance" "'libmoq.inputs.sha256' is not a SHA-256"; continue; }
    _digroot=$repo
    if [ "$tmode" = product ] && [ "$(props_count "$tprov" libmoq.inputs.root)" != "0" ]; then
        fail "$abi.provenance" "a product stage declares 'libmoq.inputs.root'; the product digest is always computed over the worktree"
        continue
    fi
    if [ "$tmode" = product ] && [ -n "$repo_override" ]; then
        fail "$abi.provenance" "--repo was supplied for a mode=product stage; the product digest may not be redirected"
        continue
    fi
    if [ "$tmode" = selftest ] || [ "$tmode" = selftest-product-probe ]; then
        _digroot=$(props_one "$tprov" libmoq.inputs.root) \
            || { fail "$abi.provenance" "the self-test stage declares no single 'libmoq.inputs.root'"; continue; }
        [ -d "$_digroot" ] || { fail "$abi.provenance" "the declared input root is not a directory: $_digroot"; continue; }
    fi
    if ! _dig=$(inputs_digest "$_digroot" "$scratch"); then
        fail "$abi.provenance" "cannot recompute the LibMoQ input digest over $_digroot (unreadable input, symlinked input, or no hasher) -- an unreadable input is never an empty pass"
        continue
    fi
    decl_is_sha256 "$_dig" || { fail "$abi.provenance" "the recomputed input digest is not a SHA-256"; continue; }
    [ "$_dig" = "$_wantdig" ] \
        || { fail "$abi.provenance" "the recomputed LibMoQ input digest $_dig does not match the declared $_wantdig: the audited DSO does not correspond to these sources"; continue; }
    _uncov=$(inputs_families_covered "$scratch/libmoq-inputs.manifest")
    [ -z "$_uncov" ] \
        || { fail "$abi.provenance" "the input manifest has no entry under the required family '$_uncov': an aggregate digest alone cannot detect a root dropped from the inventory"; continue; }

    # WHICH manifest bound this stage must be answerable from the stage itself.
    # The producer's scratch container is cleared by its own recipe and by the
    # self-test lane, so a stage that keeps its manifest only there cannot say
    # what it was bound to without reconstructing it afterwards (1733; 1734).
    _bnd="$adir/libmoq-inputs.manifest"
    [ -f "$_bnd" ] \
        || { fail "$abi.provenance" "the stage does not carry the input manifest it was bound to: a digest alone cannot name which inputs it covered"; continue; }
    _bndh=$(decl_sha256 "$_bnd") \
        || { fail "$abi.provenance" "cannot hash the stage's saved input manifest"; continue; }
    [ "$_bndh" = "$_wantdig" ] \
        || { fail "$abi.provenance" "the saved input manifest hashes $_bndh, not the declared $_wantdig: it is not the manifest this stage was bound to"; continue; }
    _samebnd=$(cmp -s "$_bnd" "$scratch/libmoq-inputs.manifest" && echo same || echo differs)

    # The tooling/evidence digest binds nothing -- a host-only tool cannot
    # change the DSO -- so it is checked for INTERNAL consistency only: the
    # stage's own manifest must be the one its provenance names. Drift between
    # that inventory and today's tools is reported, never a refusal.
    _wanttool=$(props_one "$tprov" libmoq.tooling.sha256) \
        || { fail "$abi.provenance" "topology provenance has no single 'libmoq.tooling.sha256'"; continue; }
    decl_is_sha256 "$_wanttool" || { fail "$abi.provenance" "'libmoq.tooling.sha256' is not a SHA-256"; continue; }
    _tbnd="$adir/libmoq-tooling.manifest"
    [ -f "$_tbnd" ] \
        || { fail "$abi.provenance" "the stage does not carry the tooling/evidence manifest its provenance declares"; continue; }
    _tbndh=$(decl_sha256 "$_tbnd") \
        || { fail "$abi.provenance" "cannot hash the stage's saved tooling manifest"; continue; }
    [ "$_tbndh" = "$_wanttool" ] \
        || { fail "$abi.provenance" "the saved tooling manifest hashes $_tbndh, not the declared $_wanttool"; continue; }
    if _tnow=$(tooling_digest "$_digroot" "$scratch"); then
        [ "$_tnow" = "$_wanttool" ] \
            || note "$abi.tooling" "the host tools have changed since this stage was built ($_tnow now, $_wanttool then); host-only tools do not bind the DSO, so this is evidence drift, not a rebind"
    else
        fail "$abi.provenance" "cannot recompute the tooling/evidence digest over $_digroot: an unclassifiable tool path is a hard failure, never a default"
        continue
    fi
    _nhashed=$(printf '%s %s %s' "$DEP_ARCHIVES" "$MOQ_ARCHIVES" "$DEP_PINNED_ARCHIVES" | tr ' ' '\n' | awk 'NF { n++ } END { print n + 0 }')
    pass "$abi.provenance" "provenance schema exact; agrees with Phase 0.2 (API $api, $(basename "$cc")); $_nhashed archive hashes and the family-covered source-content digest recomputed; bound to the manifest saved in the stage ($_samebnd from today's), tooling inventory $_wanttool"

    # -- stage totality per ABI: exact relative path AND type -----------------
    pkg="$adir/package/libmoq5_jni.so"
    diag="$adir/diagnostic/libmoq5_jni.so"
    audit="$adir/audit-inputs"
    # Licence destinations come from the exact component inventory, never a
    # licenses/* wildcard (1153 R4/R10).
    _wantfiles="package/libmoq5_jni.so diagnostic/libmoq5_jni.so link.map topology.log topology.provenance licenses.manifest"
    # The manifest this stage was BOUND to, and the evidence inventory beside
    # it, both live in the stage (ruling 1734).
    _wantfiles="$_wantfiles libmoq-inputs.manifest libmoq-tooling.manifest"
    for c in $LICENSE_COMPONENTS; do _wantfiles="$_wantfiles licenses/$c.LICENSE"; done
    for a in $MOQ_ARCHIVES; do _wantfiles="$_wantfiles audit-inputs/$a"; done
    _wantfiles="$_wantfiles audit-inputs/link.rsp responses.manifest ptls-prefix.manifest"
    _wantdirs="package diagnostic licenses audit-inputs"
    # A synthetic stage compiles inside the stage, so its build root -- and the
    # staged picotls prefix under it, with what its configure would have
    # selected -- lives in audit-inputs. The prefix's own file set is judged
    # exactly by the picotls-prefix oracle below, not by this path list.
    if [ "$tmode" != product ]; then
        _wantfiles="$_wantfiles audit-inputs/CMakeCache.txt audit-inputs/compile_commands.json"
    else
        _wantfiles="$_wantfiles jni-compile.json"
    fi

    if ! ( cd "$adir" && find . -mindepth 1 -type d ) > "$scratch/dirs.$abi" 2>/dev/null; then
        fail "$abi.stage" "cannot inventory the stage directories"; continue
    fi
    if ! ( cd "$adir" && find . -mindepth 1 -type f ) > "$scratch/files.$abi" 2>/dev/null; then
        fail "$abi.stage" "cannot inventory the stage files"; continue
    fi
    _gotdirs=$(sed 's|^\./||' "$scratch/dirs.$abi" | sort)
    _gotfiles=$(sed 's|^\./||' "$scratch/files.$abi" | sort)
    _expdirs=$(printf '%s\n' "$_wantdirs" | tr ' ' '\n' | awk 'NF' | sort)
    _expfiles=$(printf '%s\n' "$_wantfiles" | tr ' ' '\n' | awk 'NF' | sort)
    _sbad=""
    _x=$(printf '%s\n' "$_gotdirs" | grep -vxF -- "$_expdirs" \
         | { if [ "$tmode" != product ]; then grep -v '^audit-inputs/ptls-prefix\(/.*\)\{0,1\}$'; else cat; fi; } \
         | sed -n '1p')
    # Response files are build inputs whose trust comes from the response
    # DECLARATION and its hashes, not from this path list, so an additional
    # audit-inputs/*.rsp is classified here and judged there. Every file is
    # still classified by CONTENT below, so a disguised library cannot hide
    # behind the suffix.
    [ -z "$_x" ] && _x=$(printf '%s\n' "$_gotfiles" | grep -vxF -- "$_expfiles" \
                         | grep -v '^audit-inputs/[^/]*\.rsp$' \
                         | { if [ "$tmode" != product ]; then grep -v '^audit-inputs/ptls-prefix/'; else cat; fi; } \
                         | sed -n '1p')
    [ -z "$_x" ] || _sbad="unclassified path in the audit stage: $_x"
    if [ -z "$_sbad" ]; then
        _m=$(printf '%s\n' "$_expfiles" | grep -vxF -- "$_gotfiles" | sed -n '1p')
        [ -z "$_m" ] || _sbad="missing required stage file: $_m"
    fi
    if [ -z "$_sbad" ]; then
        _m=$(printf '%s\n' "$_expdirs" | grep -vxF -- "$_gotdirs" | sed -n '1p')
        [ -z "$_m" ] || _sbad="missing required stage directory: $_m"
    fi
    [ -z "$_sbad" ] || { fail "$abi.stage" "$_sbad"; continue; }
    # ABSENT, UNREADABLE and NOT-ELF are three different facts.
    _rbad=""
    for f in $_expfiles; do
        _fd=$(dirname "$adir/$f")
        if [ ! -r "$_fd" ] || [ ! -x "$_fd" ]; then _rbad="stage directory is not readable: $(dirname "$f")"; break; fi
        [ -r "$adir/$f" ] || { _rbad="required stage file is not readable: $f"; break; }
    done
    [ -z "$_rbad" ] || { fail "$abi.stage" "$_rbad"; continue; }

    # ELF DYN by CONTENT. The producer is guarded separately from the parser:
    # a failed readelf must never turn an ELF into a non-ELF (1153 R4).
    _elfdyn=""; _elfbad=""
    for f in $_gotfiles; do
        if ! _m4=$(od -An -N4 -tx1 "$adir/$f" 2>/dev/null); then _elfbad="cannot read $f to classify it by content"; break; fi
        [ "$(printf '%s' "$_m4" | tr -d ' \n')" = "7f454c46" ] || continue
        if ! _hdr=$("$readelf" -h "$adir/$f" 2>/dev/null); then _elfbad="readelf failed on the ELF file $f"; break; fi
        _t=$(printf '%s\n' "$_hdr" | sed -n 's/^ *Type: *\([A-Z]*\).*/\1/p' | sed -n '1p')
        [ -n "$_t" ] || { _elfbad="cannot parse the ELF type of $f"; break; }
        [ "$_t" = DYN ] && _elfdyn="${_elfdyn:+$_elfdyn }$f"
    done
    [ -z "$_elfbad" ] || { fail "$abi.onedso" "$_elfbad"; continue; }
    _elfn=$(printf '%s\n' "$_elfdyn" | tr ' ' '\n' | awk 'NF { n++ } END { print n + 0 }')
    [ "$_elfn" -eq 2 ] \
        || { fail "$abi.onedso" "$_elfn ELF DYN file(s) in the stage by CONTENT; exactly two are expected (package + diagnostic): $_elfdyn"; continue; }
    _extra=$(printf '%s\n' "$_elfdyn" | tr ' ' '\n' | grep -vx 'package/libmoq5_jni.so' | grep -vx 'diagnostic/libmoq5_jni.so' | sed -n '1p')
    [ -z "$_extra" ] || { fail "$abi.onedso" "an extra ELF DYN file (renamed or versioned) is packaged: $_extra"; continue; }
    pass "$abi.onedso" "exact path+type manifest from the topology root down; exactly two ELF DYN files by content"

    # -- identity ------------------------------------------------------------
    if ! "$objdump" -f "$pkg" > "$scratch/fmt.$abi" 2>/dev/null; then
        fail "$abi.identity" "the object-format producer failed on the packaged DSO"; continue
    fi
    _fmt=$(sed -n 's/.*file format //p' "$scratch/fmt.$abi" | sed -n '1p')
    [ -n "$_fmt" ] || { fail "$abi.identity" "the packaged DSO reports no object format"; continue; }
    [ "$_fmt" = "$machine" ] || { fail "$abi.identity" "packaged DSO reports '$_fmt', expected '$machine' for $abi"; continue; }
    if ! "$readelf" -h "$pkg" > "$scratch/ehdr.$abi" 2>/dev/null; then
        fail "$abi.identity" "the ELF header producer failed on the packaged DSO"; continue
    fi
    _cls=$(sed -n 's/^ *Class: *//p' "$scratch/ehdr.$abi" | sed -n '1p')
    [ "$_cls" = ELF64 ] || { fail "$abi.identity" "packaged DSO class is '$_cls', expected ELF64"; continue; }
    pass "$abi.identity" "packaged DYN $_fmt (ELF64), API $api, provenance-agreed"

    # -- exports: EXACT spelling, no version suffix accepted (1153 R5) --------
    _dynraw=$("$readelf" --wide --dyn-syms "$pkg" 2>/dev/null) \
        || { fail "$abi.exports" "cannot read the dynamic symbol table"; continue; }
    _rows=$(printf '%s\n' "$_dynraw" \
            | awk '$1 ~ /^[0-9]+:$/ && $7 != "UND" && $5 != "LOCAL" && $8 != "" { print $4 "|" $5 "|" $6 "|" $7 "|" $8 }')
    _rn=$(printf '%s\n' "$_rows" | awk 'NF { n++ } END { print n + 0 }')
    [ "$_rn" -eq 1 ] \
        || { fail "$abi.exports" "$_rn defined non-local dynamic row(s); exactly one is allowed: $(printf '%s' "$_rows" | tr '\n' ' ' | cut -c1-90)"; continue; }
    _rtype=$(printf '%s' "$_rows" | cut -d'|' -f1); _rbind=$(printf '%s' "$_rows" | cut -d'|' -f2)
    _rvis=$(printf '%s' "$_rows"  | cut -d'|' -f3); _rndx=$(printf '%s' "$_rows" | cut -d'|' -f4)
    _rname=$(printf '%s' "$_rows" | cut -d'|' -f5)
    case $_rname in
        *@*) fail "$abi.exports" "the sole dynamic definition is versioned ('$_rname'); the contract is exactly unversioned $WANT_EXPORT"; continue ;;
    esac
    [ "$_rname" = "$WANT_EXPORT" ] || { fail "$abi.exports" "the sole dynamic definition is '$_rname', expected '$WANT_EXPORT'"; continue; }
    [ "$_rtype" = FUNC ]   || { fail "$abi.exports" "$WANT_EXPORT is type '$_rtype', expected FUNC"; continue; }
    [ "$_rbind" = GLOBAL ] || { fail "$abi.exports" "$WANT_EXPORT is bind '$_rbind', expected GLOBAL"; continue; }
    [ "$_rvis" = DEFAULT ] || { fail "$abi.exports" "$WANT_EXPORT visibility is '$_rvis', expected DEFAULT"; continue; }
    case $_rndx in *[0-9]*) ;; *) fail "$abi.exports" "$WANT_EXPORT has no defined section index ('$_rndx')"; continue ;; esac
    pass "$abi.exports" "exactly one defined dynamic row: literal $WANT_EXPORT FUNC/GLOBAL/DEFAULT in section $_rndx"

    # -- DT_NEEDED: exact multiset against an authority OUTSIDE the artifact --
    _dyns=$("$readelf" --wide -d "$pkg" 2>/dev/null) \
        || { fail "$abi.needed" "cannot read the dynamic section"; continue; }
    _needed=$(printf '%s\n' "$_dyns" | sed -n 's/.*(NEEDED).*Shared library: \[\(.*\)\]/\1/p' | sort)
    if [ "$tmode" = selftest ]; then
        _wantn=$(props_one "$tprov" needed.selftest) \
            || { fail "$abi.needed" "the self-test stage declares no single 'needed.selftest'"; continue; }
    else
        # NOT GUESSED. An earlier revision froze "libc libdl liblog libm" as the
        # product authority; a source audit finds no Android logging reference
        # anywhere in LibMoQ or the pinned dependency sources, so liblog was a
        # guess (1153 R11). The set below was derived from the first real
        # product link and frozen by ruling 1171 A.
        _wantn="$WANT_NEEDED_PRODUCT"
    fi
    _got=$(printf '%s' "$_needed" | tr '\n' ' ' | sed 's/ *$//')
    _exp=$(printf '%s\n' "$_wantn" | tr ' ' '\n' | awk 'NF' | sort | tr '\n' ' ' | sed 's/ *$//')
    [ "$_got" = "$_exp" ] \
        || { fail "$abi.needed" "DT_NEEDED multiset is [$_got]; the frozen expectation is [$_exp] -- an omitted or extra dependency both fail here"; continue; }
    pass "$abi.needed" "DT_NEEDED multiset exactly [$_got]"

    # -- selected archive(member) records, by EXACT FULL PATH -----------------
    lmap="$adir/link.map"
    [ -s "$lmap" ] || { fail "$abi.link" "missing or empty link map: existing archive files are not evidence that the linker selected members from them"; continue; }
    if ! awk '{ for (i = 1; i <= NF; i++)
                  if ($i ~ /\.a\([^)]+\):\(/) { split($i, p, "("); print p[1] } }' "$lmap" \
              > "$scratch/sel.$abi" 2>/dev/null; then
        fail "$abi.link" "the link-map parser failed"; continue
    fi
    _selpath=$(sort -u "$scratch/sel.$abi")
    [ -n "$_selpath" ] || { fail "$abi.link" "the link map records no archive(member) selection at all"; continue; }

    # The exact path is pinned per archive: dependency archives from the audited
    # Phase 0.2 stage, LibMoQ archives from this stage's retained audit inputs.
    # Two archives sharing a required basename cannot collapse through sort -u,
    # because the comparison is on the full path (1153 R2).
    _lbad=""
    for a in $DEP_ARCHIVES; do
        _want="$dep/lib/$a"
        _n=$(printf '%s\n' "$_selpath" | awk -v w="$_want" '$0 == w { n++ } END { print n + 0 }')
        [ "$_n" -eq 1 ] || { _lbad="$a: $_n selected path(s) equal to $_want (exactly one required)"; break; }
    done
    if [ -z "$_lbad" ]; then
        for a in $MOQ_ARCHIVES; do
            _want="$audit/$a"
            _n=$(printf '%s\n' "$_selpath" | awk -v w="$_want" '$0 == w { n++ } END { print n + 0 }')
            [ "$_n" -eq 1 ] || { _lbad="$a: $_n selected path(s) equal to $_want (exactly one required)"; break; }
        done
    fi
    [ -z "$_lbad" ] || { fail "$abi.link" "$_lbad"; continue; }
    # A pinned-only Phase 0.2 output is hash-verified but must never be linked.
    _pbad=""
    for a in $DEP_PINNED_ARCHIVES; do
        _n=$(printf '%s\n' "$_selpath" | awk -v w="$dep/lib/$a" '$0 == w { n++ } END { print n + 0 }')
        [ "$_n" -eq 0 ] \
            || { _pbad="$a is a pinned-only Phase 0.2 output and must not be linked, but it contributed $_n member selection(s)"; break; }
    done
    [ -z "$_pbad" ] || { fail "$abi.link" "$_pbad"; continue; }
    _fbad=""
    for a in $FORBIDDEN_ARCHIVES; do
        printf '%s\n' "$_selpath" | awk -F/ -v b="$a" '$NF == b { found = 1 } END { exit !found }' \
            && _fbad="${_fbad:+$_fbad }$a"
    done
    [ -z "$_fbad" ] || { fail "$abi.link" "archive(s) outside the declared service closure contributed members: $_fbad"; continue; }

    # -- the compiler runtime: ONE exact archive, queried from the compiler ----
    # Not "anything under the NDK root" -- that turns a directory into an
    # allowlist (1153 R3).
    if ! _rt=$("$cc" --print-libgcc-file-name 2>/dev/null); then
        fail "$abi.link" "the pinned compiler could not report its runtime archive (--print-libgcc-file-name failed)"; continue
    fi
    case $_rt in
        /*.a) ;;
        *) fail "$abi.link" "the compiler reported a runtime archive of unexpected grammar: '$_rt'"; continue ;;
    esac
    # PHYSICAL containment: a lexical prefix test accepts a symlink that leaves
    # the pinned NDK entirely (1155 R6).
    _rtdir=$(cd "$(dirname "$_rt")" 2>/dev/null && pwd -P) \
        || { fail "$abi.link" "the reported runtime archive directory is not readable: $_rt"; continue; }
    _ndkp=$(cd "$ndkroot" 2>/dev/null && pwd -P) \
        || { fail "$abi.link" "the pinned NDK root is not readable: $ndkroot"; continue; }
    case $_rtdir in
        "$_ndkp"|"$_ndkp"/*) ;;
        *) fail "$abi.link" "the reported runtime archive resolves outside the pinned NDK $_ndkp: $_rtdir"; continue ;;
    esac
    _rtdecl=$(props_one "$tprov" compiler.runtime.path) || { fail "$abi.link" "provenance declares no single 'compiler.runtime.path'"; continue; }
    [ "$_rtdecl" = "$_rt" ] || { fail "$abi.link" "provenance declares runtime '$_rtdecl' but the pinned compiler reports '$_rt'"; continue; }
    [ -f "$_rt" ] && [ -s "$_rt" ] || { fail "$abi.link" "the compiler runtime archive is missing or empty: $_rt"; continue; }
    _rth=$(decl_sha256 "$_rt") || { fail "$abi.link" "cannot hash the compiler runtime archive"; continue; }
    _rtc=$(props_one "$tprov" compiler.runtime.sha256) || { fail "$abi.link" "provenance declares no single 'compiler.runtime.sha256'"; continue; }
    decl_is_sha256 "$_rtc" || { fail "$abi.link" "the declared compiler runtime hash is not a SHA-256"; continue; }
    [ "$_rth" = "$_rtc" ] || { fail "$abi.link" "the compiler runtime archive hashes $_rth but provenance claims $_rtc"; continue; }

    # every selected path must be a declared archive or that exact runtime
    # NEWLINE separated: the comparison loop below runs under IFS=newline, and
    # a space-separated list would never split there.
    _declpaths=$(printf '%s\n' "$_rt"
                 for a in $DEP_ARCHIVES; do printf '%s\n' "$dep/lib/$a"; done
                 for a in $MOQ_ARCHIVES; do printf '%s\n' "$audit/$a"; done)
    _unk=$(printf '%s\n' "$_selpath" | grep -vxF -- "$_declpaths" | sed -n '1p')
    [ -z "$_unk" ] || { fail "$abi.link" "an archive outside the declared closure and the exact compiler runtime contributed members: $_unk"; continue; }
    _seln=$(printf '%s\n' "$_selpath" | awk 'NF { n++ } END { print n + 0 }')
    _wantn2=$(printf '%s %s' "$DEP_ARCHIVES" "$MOQ_ARCHIVES" | tr ' ' '\n' | awk 'NF { n++ } END { print n + 0 }')
    [ "$_seln" -eq "$((_wantn2 + 1))" ] \
        || { fail "$abi.link" "$_seln distinct archive paths contributed; $_wantn2 declared plus exactly one compiler runtime"; continue; }

    # -- every declared linked archive plus the runtime: non-empty, ABI-correct
    _abad=""
    _oldifs=$IFS; IFS='
'
    for p in $_declpaths; do
        IFS=$_oldifs
        [ -n "$p" ] || continue
        _b=${p##*/}
        [ -f "$p" ] || { _abad="$_b is not present at $p"; break; }
        [ -s "$p" ] || { _abad="$_b is empty at $p"; break; }
        if ! _mem=$("$ar" t "$p" 2>/dev/null); then _abad="cannot list members of $_b (archive tool failure)"; break; fi
        _mn=$(printf '%s\n' "$_mem" | awk 'NF { n++ } END { print n + 0 }')
        [ "$_mn" -gt 0 ] || { _abad="$_b lists no members"; break; }
        if ! "$objdump" -f "$p" > "$scratch/afmt" 2>/dev/null; then _abad="the archive-format producer failed on $_b"; break; fi
        _fmts=$(sed -n 's/.*file format //p' "$scratch/afmt")
        _fn=$(printf '%s\n' "$_fmts" | awk 'NF { n++ } END { print n + 0 }')
        [ "$_fn" -eq "$_mn" ] || { _abad="$_b: $_mn member(s) listed but $_fn classified"; break; }
        _w=$(printf '%s\n' "$_fmts" | grep -vx "$machine" | sed -n '1p')
        [ -z "$_w" ] || { _abad="$_b carries a member of format '$_w', expected '$machine'"; break; }
        IFS='
'
    done
    IFS=$_oldifs
    [ -z "$_abad" ] || { fail "$abi.link" "$_abad"; continue; }
    pass "$abi.link" "all $_wantn2 declared archives selected by exact path and inspected, plus the compiler runtime $(basename "$_rt"); every member is $machine"

    # -- qlog ORIGIN: join the symbol row to the input record above it --------
    # picoquic_set_qlog belongs to picoquic-core's qlog streaming member. It
    # must be selected from the pinned core archive, from the ONE member that
    # defines it there; the pinned-only log archive must define it nowhere, and
    # the image exactly once. A stand-in definition, a different member, or a
    # log-archive definition each fails by name.
    _qarch=$(awk '{ for (i = 1; i <= NF; i++) if ($i ~ /:\(/) { cur = $i; next } }
                  $NF == "picoquic_set_qlog" && cur != "" { print cur; exit }' "$lmap") \
        || { fail "$abi.qlog" "cannot parse the link map for the qlog origin"; continue; }
    [ -n "$_qarch" ] || { fail "$abi.qlog" "no input record supplies picoquic_set_qlog"; continue; }
    case $_qarch in
        *.a\(*\):\(*) ;;
        *) fail "$abi.qlog" "picoquic_set_qlog was supplied by '${_qarch%%:*}', which is not an archive member -- a stand-in definition is not the pinned core archive"; continue ;;
    esac
    _qpath=${_qarch%%(*}
    _qmem=${_qarch#*(}; _qmem=${_qmem%%)*}
    [ "$_qpath" = "$dep/lib/libpicoquic-core.a" ] \
        || { fail "$abi.qlog" "picoquic_set_qlog was supplied by '$_qpath', not $dep/lib/libpicoquic-core.a"; continue; }
    if ! "$nm" --defined-only "$dep/lib/libpicoquic-core.a" > "$scratch/qsrc.$abi" 2>/dev/null; then
        fail "$abi.qlog" "the symbol producer failed on libpicoquic-core.a"; continue
    fi
    # llvm-nm heads each archive member's rows with "<member>:".
    _qdefs=$(awk '/^[^ ]+:$/ { m = substr($0, 1, length($0) - 1); next }
                  $NF == "picoquic_set_qlog" { print m }' "$scratch/qsrc.$abi")
    _qsrc=$(printf '%s\n' "$_qdefs" | awk 'NF { n++ } END { print n + 0 }')
    [ "$_qsrc" -eq 1 ] || { fail "$abi.qlog" "libpicoquic-core.a defines picoquic_set_qlog $_qsrc time(s); exactly one is required"; continue; }
    [ "$_qmem" = "$_qdefs" ] \
        || { fail "$abi.qlog" "the link map attributes picoquic_set_qlog to libpicoquic-core.a($_qmem), but the member that defines it is $_qdefs"; continue; }
    if ! "$nm" --defined-only "$dep/lib/libpicoquic-log.a" > "$scratch/qlog.$abi" 2>/dev/null; then
        fail "$abi.qlog" "the symbol producer failed on libpicoquic-log.a"; continue
    fi
    _qlogn=$(awk '$NF == "picoquic_set_qlog" { c++ } END { print c + 0 }' "$scratch/qlog.$abi")
    [ "$_qlogn" -eq 0 ] || { fail "$abi.qlog" "the pinned-only libpicoquic-log.a defines picoquic_set_qlog $_qlogn time(s); it must define none"; continue; }
    if ! "$nm" --defined-only "$diag" > "$scratch/qimg.$abi" 2>/dev/null; then
        fail "$abi.qlog" "the symbol producer failed on the diagnostic image"; continue
    fi
    _qimg=$(awk '$NF == "picoquic_set_qlog" { c++ } END { print c + 0 }' "$scratch/qimg.$abi")
    [ "$_qimg" -eq 1 ] || { fail "$abi.qlog" "picoquic_set_qlog is defined $_qimg time(s) in the image; exactly one is required"; continue; }
    pass "$abi.qlog" "picoquic_set_qlog selected from libpicoquic-core.a($_qmem), its only definition in the pinned core archive; none in the pinned libpicoquic-log.a; once in the image"

    # -- text relocations: an IMAGE property, stated as exactly that ----------
    _textrel=$(printf '%s\n' "$_dyns" | awk '$0 ~ /TEXTREL/ { n++ } END { print n + 0 }')
    [ "$_textrel" -eq 0 ] || { fail "$abi.pic" "the image carries DT_TEXTREL: at least one selected member was not position-independent"; continue; }
    pass "$abi.pic" "no DT_TEXTREL in the final image (image evidence; -z text remains a GREEN-recipe obligation, and this does not by itself prove every member was compiled PIC)"

    # -- closure: every strong UND provided by a DT_NEEDED platform library ---
    _sysroot=$(cd "$bin/../sysroot" 2>/dev/null && pwd) \
        || { fail "$abi.closure" "cannot resolve the NDK sysroot beside $bin"; continue; }
    _sysdir="$_sysroot/usr/lib/$triple/$api"
    [ -d "$_sysdir" ] || _sysdir="$_sysroot/usr/lib/$triple"
    [ -d "$_sysdir" ] || { fail "$abi.closure" "cannot resolve the sysroot library directory for $triple/$api"; continue; }
    _provided=""; _pbad=""
    for n in $_needed; do
        [ -f "$_sysdir/$n" ] || { _pbad="the sysroot provides no $n at $_sysdir: a missing provider is never silently skipped"; break; }
        if ! _d=$("$nm" --dynamic --defined-only "$_sysdir/$n" 2>/dev/null); then _pbad="cannot read the dynamic symbols of $n"; break; fi
        # Normalised EXACTLY as the undefined side is: comparing a stripped UND
        # against an unstripped name@@VERSION reports a closed graph as open.
        _provided="$_provided
$(printf '%s\n' "$_d" | awk '{ print $NF }' | sed 's/@.*//')"
    done
    [ -z "$_pbad" ] || { fail "$abi.closure" "$_pbad"; continue; }
    _und=$(printf '%s\n' "$_dynraw" | awk '$1 ~ /^[0-9]+:$/ && $7 == "UND" && $5 != "WEAK" && $8 != "" { print $8 }' | sed 's/@.*//' | sort -u)
    _unres=$(printf '%s\n' "$_und" | while IFS= read -r u; do
                 [ -n "$u" ] || continue
                 printf '%s\n' "$_provided" | grep -qx -- "$u" || printf '%s\n' "$u"
             done | sed -n '1,3p')
    [ -z "$_unres" ] || { fail "$abi.closure" "strong undefined symbol(s) not provided by any DT_NEEDED platform library: $(printf '%s' "$_unres" | tr '\n' ' ')"; continue; }
    pass "$abi.closure" "every strong undefined symbol is provided by a DT_NEEDED platform library (artifact evidence; -z defs remains a GREEN-recipe obligation)"

    # -- strip: REPRODUCED from the diagnostic twin, byte-identical ------------
    _sc="$scratch/$abi-strip.so"
    cp "$diag" "$_sc" 2>/dev/null || { fail "$abi.strip" "cannot copy the diagnostic twin into task-owned scratch"; continue; }
    "$strip" --strip-debug --strip-unneeded "$_sc" 2>/dev/null \
        || { fail "$abi.strip" "the pinned llvm-strip failed on the diagnostic twin"; continue; }
    cmp -s "$_sc" "$pkg" \
        || { fail "$abi.strip" "the package is NOT byte-identical to the reproduced strip of the diagnostic twin: it was not produced from that twin by the declared operation"; continue; }
    if ! "$readelf" --wide -d "$diag" > "$scratch/ddyn.$abi" 2>/dev/null; then
        fail "$abi.strip" "the dynamic-section producer failed on the diagnostic twin"; continue
    fi
    _dneed=$(sed -n 's/.*(NEEDED).*Shared library: \[\(.*\)\]/\1/p' "$scratch/ddyn.$abi" | sort | tr '\n' ' ' | sed 's/ *$//')
    [ "$_dneed" = "$_got" ] || { fail "$abi.strip" "the diagnostic twin's DT_NEEDED [$_dneed] differs from the package's [$_got]"; continue; }
    if ! "$objdump" -f "$diag" > "$scratch/dfmt.$abi" 2>/dev/null; then
        fail "$abi.strip" "the object-format producer failed on the diagnostic twin"; continue
    fi
    _dfmt=$(sed -n 's/.*file format //p' "$scratch/dfmt.$abi" | sed -n '1p')
    [ "$_dfmt" = "$machine" ] || { fail "$abi.strip" "the diagnostic twin reports format '$_dfmt', expected '$machine'"; continue; }
    if ! "$readelf" --wide --dyn-syms "$diag" > "$scratch/ddyns.$abi" 2>/dev/null; then
        fail "$abi.strip" "the dynamic-symbol producer failed on the diagnostic twin"; continue
    fi
    _dexp=$(awk '$1 ~ /^[0-9]+:$/ && $7 != "UND" && $5 != "LOCAL" && $8 != "" { print $8 }' "$scratch/ddyns.$abi")
    _dn=$(printf '%s\n' "$_dexp" | awk 'NF { n++ } END { print n + 0 }')
    { [ "$_dn" -eq 1 ] && [ "$_dexp" = "$WANT_EXPORT" ]; } \
        || { fail "$abi.strip" "the diagnostic twin exports [$(printf '%s' "$_dexp" | tr '\n' ' ')], expected exactly $WANT_EXPORT"; continue; }
    # the DSO and map hashes recorded in provenance describe THESE artifacts
    _hmis=""
    for pair in "dso.sha256:$pkg" "diagnostic.sha256:$diag" \
                "map.sha256:$lmap" "receipt.sha256:$adir/topology.log" \
                "responses.sha256:$adir/responses.manifest"; do
        _k=${pair%%:*}; _f=${pair#*:}
        _hv=$(decl_sha256 "$_f") || { _hmis="cannot hash $(basename "$_f")"; break; }
        _hc=$(props_one "$tprov" "$_k") || { _hmis="provenance declares no single '$_k'"; break; }
        decl_is_sha256 "$_hc" || { _hmis="the declared '$_k' is not a SHA-256"; break; }
        [ "$_hv" = "$_hc" ] || { _hmis="$(basename "$_f") hashes $_hv but provenance claims $_hc for $_k"; break; }
    done
    [ -z "$_hmis" ] || { fail "$abi.strip" "$_hmis"; continue; }
    pass "$abi.strip" "package byte-identical to the reproduced strip of the diagnostic twin; DSO, map and receipt hashes match provenance"

    # -- licences: exact component set, exact fields, canonical sources -------
    man="$adir/licenses.manifest"
    _lbad=""
    _rows2=$(awk 'NF { n++ } END { print n + 0 }' "$man") || { fail "$abi.licenses" "cannot count manifest rows"; continue; }
    _lwant=$(printf '%s\n' "$LICENSE_COMPONENTS" | tr ' ' '\n' | awk 'NF { n++ } END { print n + 0 }')
    [ "$_rows2" -eq "$_lwant" ] \
        || { fail "$abi.licenses" "the manifest has $_rows2 row(s); exactly $_lwant are required ($LICENSE_COMPONENTS)"; continue; }
    _names=$(awk -F'\t' 'NF { print $1 }' "$man" | sort)
    _dupn=$(printf '%s\n' "$_names" | uniq -d | sed -n '1p')
    [ -z "$_dupn" ] || { fail "$abi.licenses" "duplicate manifest row for component '$_dupn'"; continue; }
    _lexp=$(printf '%s\n' "$LICENSE_COMPONENTS" | tr ' ' '\n' | awk 'NF' | sort)
    _lx=$(printf '%s\n' "$_names" | grep -vxF -- "$_lexp" | sed -n '1p')
    [ -z "$_lx" ] || { fail "$abi.licenses" "manifest names an unexpected component: $_lx"; continue; }
    _lm=$(printf '%s\n' "$_lexp" | grep -vxF -- "$_names" | sed -n '1p')
    [ -z "$_lm" ] || { fail "$abi.licenses" "manifest omits the required component: $_lm"; continue; }

    _lreal=$(cd "$adir/licenses" 2>/dev/null && pwd -P) \
        || { fail "$abi.licenses" "the package licence directory is not readable"; continue; }
    for c in $LICENSE_COMPONENTS; do
        _line=$(awk -F'\t' -v c="$c" '$1 == c { print; exit }' "$man")
        [ -n "$_line" ] || { _lbad="no manifest row for '$c'"; break; }
        _nf=$(printf '%s' "$_line" | awk -F'\t' '{ print NF }')
        [ "$_nf" -eq 4 ] || { _lbad="'$c' has $_nf tab-separated field(s); exactly four are required"; break; }
        _sha=$(printf '%s' "$_line" | awk -F'\t' '{ print $2 }')
        _dst=$(printf '%s' "$_line" | awk -F'\t' '{ print $3 }')
        _src=$(printf '%s' "$_line" | awk -F'\t' '{ print $4 }')
        decl_is_sha256 "$_sha" || { _lbad="'$c' has a malformed sha256"; break; }
        [ "$_dst" = "licenses/$c.LICENSE" ] \
            || { _lbad="'$c' declares destination '$_dst'; the exact inventory requires licenses/$c.LICENSE"; break; }
        [ -f "$adir/$_dst" ] || { _lbad="'$c' names an absent destination: $_dst"; break; }
        [ -L "$adir/$_dst" ] && { _lbad="'$c' destination is a symlink"; break; }
        _dreal=$(cd "$(dirname "$adir/$_dst")" 2>/dev/null && pwd -P) \
            || { _lbad="'$c' destination directory is not readable"; break; }
        case $_dreal in
            "$_lreal") ;;
            "$_lreal"/*) ;;
            *) _lbad="'$c' destination resolves outside the package licence directory"; break ;;
        esac
        # The canonical source is the CHECKER's, not the stage's: an arbitrary
        # source path is never accepted merely because the bytes match (R10).
        _canon=$(license_source "$c") || { _lbad="no canonical source is defined for '$c'"; break; }
        [ "$_src" = "$_canon" ] \
            || { _lbad="'$c' declares source '$_src'; the pinned canonical source is '$_canon'"; break; }
        _gh=$(decl_sha256 "$adir/$_dst") || { _lbad="cannot hash the payload for '$c'"; break; }
        [ "$_gh" = "$_sha" ] || { _lbad="'$c' payload hash $_gh does not match the manifest"; break; }
        if [ "$tmode" = product ]; then
            [ -f "$_canon" ] || { _lbad="the canonical source for '$c' is absent: $_canon"; break; }
            cmp -s "$adir/$_dst" "$_canon" || { _lbad="'$c' payload differs from its pinned source $_canon"; break; }
        fi
    done
    [ -z "$_lbad" ] || { fail "$abi.licenses" "$_lbad"; continue; }
    pass "$abi.licenses" "$_rows2 notice(s), exact component and destination sets, each pinned to its canonical source: $LICENSE_COMPONENTS (picotls' MIT statement is in README.md; the pinned tree ships no top-level LICENSE)"

    # -- receipt: real command identity, diagnostics, allowed roots -----------
    log="$adir/topology.log"
    [ -s "$log" ] || { fail "$abi.hermetic" "missing or empty topology receipt: an empty file is not a build receipt"; continue; }
    for m in 'moq-topology: compile: ' 'moq-topology: link: ' 'moq-topology: link: done'; do
        grep -q -- "$m" "$log" || { fail "$abi.hermetic" "the receipt carries no '$m' record: a fabricated marker pair is not evidence"; continue 2; }
    done
    _diag=$(grep -inE '(^|[[:space:]])(warning|error):|fatal error|CMake (Error|Warning|Deprecation Warning)|ld: warning|make(\[[0-9]+\])?: \*\*\*' "$log" | sed -n '1p')
    [ -z "$_diag" ] || { fail "$abi.hermetic" "the receipt carries a diagnostic, and every owned warning is fatal: $(printf '%s' "$_diag" | cut -c1-90)"; continue; }
    # The command that produced the DSO must be the command that produced the map.
    grep -q -- "-Wl,-Map,$lmap" "$log" \
        || { fail "$abi.hermetic" "no recorded link command names this map ($lmap): the map is not evidence about this DSO"; continue; }
    grep -q -- "-o $pkg" "$log" || grep -q -- "-o $diag" "$log" \
        || { fail "$abi.hermetic" "no recorded link command names the audited DSO"; continue; }

    # ---- receipt path grammar -------------------------------------------
    # Every path-bearing option the recipe can use is enumerated. An unknown
    # option that carries a path is REFUSED rather than skipped, so a
    # linker-script, dynamic-list, sysroot, response or map input can no longer
    # escape by starting with "-" (1155 R8).
    # Allowed roots, PHYSICALLY resolved. The checker's own scratch is NOT a
    # root -- a real build receipt cannot depend on an audit-time directory --
    # and neither is the recorded working directory: a receipt that nominates
    # its own trusted root is its own authority (chats/libmoq/1157 F1).
    _roots=""
    for r in "$repo" "$dep_stage" "$ndkroot" "$stage"; do
        _rp=$(cd "$r" 2>/dev/null && pwd -P) || continue
        _roots="$_roots
$_rp"
    done

    # The BUILD ROOT is declared out of band -- a receipt that nominates its own
    # trusted root is its own authority -- and its containment rule differs by
    # mode, because the two recipes own different territory.
    #
    # A synthetic fixture compiles and links entirely inside the stage, so its
    # build root must be contained by the stage.
    #
    # The product recipe cannot: CMake's intermediate tree would land inside the
    # stage's exact path manifest and break the `stage` oracle. It therefore
    # declares a sibling container under the Phase 0.2 dependency root, bounded
    # here by the five conditions of ruling 1171 B. The container name is pinned
    # rather than inferred, so a build root cannot wander.
    _broot=$(props_one "$tprov" build.root) \
        || { fail "$abi.hermetic" "the stage declares no single 'build.root'"; continue; }
    _brootp=$(cd "$_broot" 2>/dev/null && pwd -P) \
        || { fail "$abi.hermetic" "the declared build root is not readable: $_broot"; continue; }
    _stagep=$(cd "$stage" 2>/dev/null && pwd -P) \
        || { fail "$abi.hermetic" "the topology stage is not readable"; continue; }
    if [ "$tmode" = product ]; then
        _depp=$(cd "$dep_stage" 2>/dev/null && pwd -P) \
            || { fail "$abi.hermetic" "the Phase 0.2 dependency stage is not readable"; continue; }
        _repop=$(cd "$repo" 2>/dev/null && pwd -P) \
            || { fail "$abi.hermetic" "the source repo is not readable"; continue; }
        _ndkp=$(cd "$ndkroot" 2>/dev/null && pwd -P) \
            || { fail "$abi.hermetic" "the pinned NDK root is not readable"; continue; }
        _bcont="$_depp/$PRODUCT_BUILD_CONTAINER"
        # (1) inside the recipe-owned container under the dependency root
        case $_brootp in
            "$_bcont"|"$_bcont"/*) ;;
            *) fail "$abi.hermetic" "the product build root is not inside the recipe-owned container $_bcont: $_brootp"; continue ;;
        esac
        # (2) never the audited stage, and never inside it
        case $_brootp in
            "$_stagep"|"$_stagep"/*)
                fail "$abi.hermetic" "the product build root is inside the audited stage, where it would enter the exact path manifest: $_brootp"; continue ;;
        esac
        # (3) never a root that is trusted for a different reason
        for _forb in "$_repop" "$_depp" "$_ndkp"; do
            [ "$_brootp" = "$_forb" ] \
                && { fail "$abi.hermetic" "the product build root is a root trusted for another reason and cannot also be the build root: $_brootp"; continue 2; }
        done
        # (4) the intermediate tree is bounded, so it becomes an allowed root
        _roots="$_roots
$_brootp"
        # (5) the FINAL LINK still runs inside the audited stage's retained
        #     inputs, so the linked bytes stay stage-local evidence
        _aip=$(cd "$adir/audit-inputs" 2>/dev/null && pwd -P) \
            || { fail "$abi.hermetic" "the stage's audit-inputs directory is not readable"; continue; }
    else
        case $_brootp in
            "$_stagep"|"$_stagep"/*) ;;
            *) fail "$abi.hermetic" "the declared build root is not contained by the audited stage: $_brootp"; continue ;;
        esac
        _aip=$_brootp
    fi

    _wd=$(grep -m1 '^moq-topology: cwd: ' "$log" | sed 's/^moq-topology: cwd: //')
    [ -n "$_wd" ] || { fail "$abi.hermetic" "the receipt records no working directory, so a relative input cannot be resolved"; continue; }
    _wdp=$(cd "$_wd" 2>/dev/null && pwd -P) \
        || { fail "$abi.hermetic" "the recorded working directory is not readable: $_wd"; continue; }
    case $_wdp in
        "$_aip"|"$_aip"/*) ;;
        *) if [ "$tmode" = product ]; then
               fail "$abi.hermetic" "the recorded working directory is not the audited stage's audit-inputs: $_wdp"
           else
               fail "$abi.hermetic" "the recorded working directory is not contained by the declared build root: $_wdp"
           fi
           continue ;;
    esac

    # rcpt_tokens <file> -> path tokens, one per line, prefixed KIND<TAB>PATH
    # KIND is `path` for something that must lie under an allowed root, or
    # `rsp` for a response file whose own path is checked and then recursed.
    rcpt_classify() {
        awk -v wd="$_wdp" '
            # Deliberately NO lexical ".." collapse. Rewriting a path before it
            # is physically resolved turns a symlinked component into a
            # different, innocent-looking path (1157 F2). The token is only
            # made absolute here; resolution happens in the shell, with real
            # symlink semantics.
            function emit(kind, v) {
                if (v == "") return
                if (substr(v, 1, 1) != "/") v = wd "/" v
                printf "%s\t%s\n", kind, v
            }
            /^moq-topology: cwd:/ { next }
            {
                line = $0
                sub(/^moq-topology: (compile|link): /, "", line)
                if (line == $0 && $0 ~ /^moq-topology:/) next
                n = split(line, tok, /[ \t]+/)
                for (i = 1; i <= n; i++) {
                    t = tok[i]
                    if (t == "") continue
                    if (substr(t, 1, 1) == "@") { emit("rsp", substr(t, 2)); continue }
                    if (substr(t, 1, 1) != "-") {
                        # a bare token is an input only if it looks like a path
                        if (t ~ /\//  || t ~ /\.(c|h|o|a|so|map|rsp|txt|cmake|ver)$/) emit("path", t)
                        continue
                    }
                    # ---- enumerated path-bearing options ----
                    # The prefix is STRIPPED, never counted: a hand-computed
                    # substring offset silently turns an absolute path into a
                    # relative one that then resolves inside the allowed cwd.
                    if (t ~ /^-Wl,-Map,/)                  { v = t; sub(/^-Wl,-Map,/, "", v);                  emit("path", v); continue }
                    if (t ~ /^-Wl,--version-script=/)      { v = t; sub(/^-Wl,--version-script=/, "", v);      emit("path", v); continue }
                    if (t ~ /^-Wl,--dynamic-list=/)        { v = t; sub(/^-Wl,--dynamic-list=/, "", v);        emit("path", v); continue }
                    if (t ~ /^-Wl,--retain-symbols-file=/) { v = t; sub(/^-Wl,--retain-symbols-file=/, "", v); emit("path", v); continue }
                    if (t ~ /^-Wl,-T/)                     { v = t; sub(/^-Wl,-T/, "", v);                     emit("path", v); continue }
                    if (t ~ /^--sysroot=/)                 { v = t; sub(/^--sysroot=/, "", v);                 emit("path", v); continue }
                    if (t ~ /^-I/ && length(t) > 2)        { v = t; sub(/^-I/, "", v);                         emit("path", v); continue }
                    if (t ~ /^-L/ && length(t) > 2)        { v = t; sub(/^-L/, "", v);                         emit("path", v); continue }
                    if (t ~ /^-isystem/ && length(t) > 8)  { v = t; sub(/^-isystem/, "", v);                   emit("path", v); continue }
                    if (t ~ /^-MJ/ && length(t) > 3)       { v = t; sub(/^-MJ/, "", v);                        emit("path", v); continue }
                    if (t == "-o" || t == "-I" || t == "-L" || t == "-isystem" || t == "--sysroot" || t == "-MJ") {
                        emit("path", tok[++i]); continue
                    }
                    # ---- options known to carry NO path ----
                    if (t ~ /^-(c|shared|f[A-Za-z0-9_-]*|W[a-z][A-Za-z0-9=_-]*|D[A-Za-z0-9_=]*|O[0-9s]?|g[0-9]?|std=.*|m[A-Za-z0-9_-]*|pthread)$/) continue
                    if (t ~ /^-Wl,(-z,[A-Za-z]+|--(start|end)-group|--no-as-needed|--as-needed|-l[A-Za-z0-9_.-]+)$/) continue
                    if (t ~ /^-l[A-Za-z0-9_.-]+$/) continue
                    # anything else that could carry a path is REFUSED
                    if (t ~ /\//) { printf "unknown\t%s\n", t; continue }
                }
            }' "$1"
    }

    # ---- response files are CONTENT-BOUND, transitively --------------------
    # The link map proves what an earlier link selected; it says nothing about
    # the bytes now sitting in a response file. Without binding them, an empty
    # file is accepted as the command that selected eighteen archives
    # (chats/libmoq/1159 F1). The declaration itself is covered by
    # responses.sha256, which is checked with the other artifact hashes above.
    _rman="$adir/responses.manifest"
    _rbad=""
    _rn=$(awk 'NF { n++ } END { print n + 0 }' "$_rman") \
        || { fail "$abi.hermetic" "cannot read the response-file declaration"; continue; }
    _rdup=$(awk -F"$_TAB" 'NF { print $1 }' "$_rman" | sort | uniq -d | sed -n '1p')
    [ -z "$_rdup" ] || { fail "$abi.hermetic" "the response-file declaration repeats a path: $_rdup"; continue; }
    _seenrsp="$scratch/rsp-seen.$abi"; : > "$_seenrsp"
    _snapdir="$scratch/rsp.$abi"; mkdir -p "$_snapdir" \
        || { fail "$abi.hermetic" "cannot create the response snapshot directory"; continue; }
    _i=0
    while IFS="$_TAB" read -r _rp _rh; do
        [ -n "${_rp:-}" ] || continue
        _i=$((_i + 1))
        decl_is_sha256 "${_rh:-}" || { _rbad="response row $_i has a malformed sha256"; break; }
        case $_rp in /*) ;; *) _rbad="response row $_i declares a non-absolute path: $_rp"; break ;; esac
        [ -f "$_rp" ] || { _rbad="a declared response file is absent: $_rp"; break; }
        [ -r "$_rp" ] || { _rbad="a declared response file is not readable: $_rp"; break; }
        # SNAPSHOT FIRST, then hash the SNAPSHOT. Hashing the live file and
        # copying afterwards trusts one byte stream and classifies another --
        # a source changed between the two operations is accepted
        # (chats/libmoq/1161 F1). The snapshot is the only thing hashed, and
        # the same snapshot is the only thing classified.
        cp "$_rp" "$_snapdir/$_i.rsp" || { _rbad="cannot snapshot the response file: $_rp"; break; }
        _gh=$(decl_sha256 "$_snapdir/$_i.rsp") \
            || { _rbad="cannot hash the snapshot of the response file: $_rp"; break; }
        [ "$_gh" = "$_rh" ] \
            || { _rbad="the snapshot of the response file $_rp hashes $_gh but the declaration claims $_rh: the bytes classified are not the bytes the recorded command consumed"; break; }
        printf '%s%s%s\n' "$_rp" "$_TAB" "$_snapdir/$_i.rsp" >> "$_seenrsp" \
            || { _rbad="cannot record the response snapshot"; break; }
    done < "$_rman"
    [ -z "$_rbad" ] || { fail "$abi.hermetic" "$_rbad"; continue; }

    # snap_of <path> -> the verified snapshot for a declared response file
    snap_of() {
        awk -F"$_TAB" -v p="$1" '$1 == p { print $2; exit }' "$_seenrsp"
    }
    _usedrsp="$scratch/rsp-used.$abi"; : > "$_usedrsp"

    if ! rcpt_classify "$log" > "$scratch/tok.$abi" 2>/dev/null; then
        fail "$abi.hermetic" "the receipt parser failed"; continue
    fi
    _unknown=$(awk -F'\t' '$1 == "unknown" { print $2; exit }' "$scratch/tok.$abi")
    [ -z "$_unknown" ] \
        || { fail "$abi.hermetic" "the receipt carries an unrecognised path-bearing option, which is refused rather than skipped: $_unknown"; continue; }

    # resolve_entry <existing-path> -> its PHYSICAL location, following a
    # final-component symlink as well as the directory prefix.
    # `cd -P` is deliberate throughout: plain `cd` resolves ".." LOGICALLY, so
    # `a/symlink/../b` becomes `a/b` and a genuine escape either fails to
    # resolve or resolves to the wrong place. -P navigates physically.
    resolve_entry() {
        _re=$1
        if [ -d "$_re" ]; then
            ( cd -P "$_re" 2>/dev/null && pwd -P ) || return 1
            return 0
        fi
        _rp=${_re%/*}; [ -n "$_rp" ] || _rp=/
        _rn=${_re##*/}
        _rg=0
        while [ -L "$_rp/$_rn" ]; do
            _rg=$((_rg + 1)); [ "$_rg" -le 64 ] || return 1
            _rt=$(readlink "$_rp/$_rn") || return 1
            case $_rt in
                /*) _rp=${_rt%/*}; [ -n "$_rp" ] || _rp=/; _rn=${_rt##*/} ;;
                *)  _rp="$_rp/${_rt%/*}"; [ "${_rt%/*}" = "$_rt" ] && _rp=${_rp%/*}
                    _rn=${_rt##*/} ;;
            esac
            if [ -d "$_rp/$_rn" ]; then break; fi
        done
        _rd=$( cd -P "$_rp" 2>/dev/null && pwd -P ) || return 1
        if [ -d "$_rd/$_rn" ]; then ( cd -P "$_rd/$_rn" && pwd -P ); return 0; fi
        printf '%s/%s' "$_rd" "$_rn"
    }

    # phys_path <abs-path> -> the physically resolved path.
    #
    # TOTAL by construction (1157 F3): a non-absolute argument is refused, each
    # iteration must strictly shorten the unresolved prefix, the loop is
    # bounded, and a ".." inside a NONEXISTENT suffix is refused rather than
    # collapsed -- collapsing it is exactly the F2 escape.
    phys_path() {
        _pp=$1
        case $_pp in /*) ;; *) return 1 ;; esac
        _sfx=""; _g=0
        while :; do
            _g=$((_g + 1)); [ "$_g" -le 256 ] || return 1
            if [ -e "$_pp" ] || [ -L "$_pp" ]; then
                _base=$(resolve_entry "$_pp") || return 1
                printf '%s%s' "$_base" "$_sfx"
                return 0
            fi
            case $_pp in
                */*) _new=${_pp%/*}; [ -n "$_new" ] || _new=/ ;;
                *)   return 1 ;;
            esac
            [ "${#_new}" -lt "${#_pp}" ] || return 1
            _sfx="/${_pp##*/}$_sfx"
            _pp=$_new
            case $_sfx in *"/../"*|*/..) return 1 ;; esac
        done
    }

    # in_roots <abs-path> -> 0 when PHYSICALLY contained under an allowed root
    in_roots() {
        _ip=$(phys_path "$1") || return 2
        [ -n "$_ip" ] || return 2
        printf '%s\n' "$_roots" | awk -v p="$_ip" 'NF { if (p == $0 || index(p, $0 "/") == 1) { ok = 1; exit } } END { exit !ok }'
    }

    # A receipt input physically under the SOURCE repo must be an input the
    # source-content digest actually covers. Containment in the repo is not
    # enough: an unmanifested file can change the DSO without moving the
    # digest (1157 F4). Generated inputs belong under the stage or the
    # accepted Phase 0.2 / NDK roots.
    _repop=$(cd "$repo" 2>/dev/null && pwd -P) || _repop=""
    _manifest="$scratch/libmoq-inputs.manifest"
    _depp=$(cd "$dep_stage" 2>/dev/null && pwd -P) || _depp=""
    _ndkp2=$(cd "$ndkroot" 2>/dev/null && pwd -P) || _ndkp2=""
    in_manifest() {
        _mp=$(phys_path "$1") || return 2
        # GENERATED and PINNED inputs are exempt: they live under the audited
        # stage, the accepted Phase 0.2 stage, or the pinned NDK -- all of which
        # are separately trusted and separately hashed. Only SOURCE inputs are
        # required to be in the source-content manifest.
        case $_mp in
            "$_stagep"|"$_stagep"/*) return 0 ;;
        esac
        [ -n "$_depp" ] && case $_mp in "$_depp"|"$_depp"/*) return 0 ;; esac
        [ -n "$_ndkp2" ] && case $_mp in "$_ndkp2"|"$_ndkp2"/*) return 0 ;; esac
        case $_mp in
            "$_repop"/*) ;;
            *) return 0 ;;
        esac
        [ -f "$_manifest" ] || return 2
        _rel=${_mp#"$_repop"/}
        if [ -d "$_mp" ]; then
            # A DIRECTORY token -- an include path, say -- is covered only when
            # EVERY file beneath it is hashed. A "some file under it is listed"
            # rule would pass a directory holding one hashed file beside an
            # unhashed header the compiler would happily read.
            find "$_mp" -type f -print > "$scratch/dircov.$$" 2>/dev/null || return 2
            awk -v r="$_rel/" 'index($2, r) == 1 { print $2 }' "$_manifest" \
                | sort > "$scratch/dircov.want.$$" || return 2
            sed "s|^$_repop/||" "$scratch/dircov.$$" | sort > "$scratch/dircov.got.$$" || return 2
            if cmp -s "$scratch/dircov.want.$$" "$scratch/dircov.got.$$"; then
                rm -f "$scratch/dircov.$$" "$scratch/dircov.want.$$" "$scratch/dircov.got.$$"
                return 0
            fi
            rm -f "$scratch/dircov.$$" "$scratch/dircov.want.$$" "$scratch/dircov.got.$$"
            return 1
        fi
        awk -v r="$_rel" '$2 == r { found = 1; exit } END { exit !found }' "$_manifest"
    }

    # Response files: the PATH itself is validated before it is opened, then
    # its contents are classified with the same grammar, recursively.
    _hbad=""
    _depth=0
    _pending="$scratch/tok.$abi"
    while [ -s "$_pending" ]; do
        _depth=$((_depth + 1))
        [ "$_depth" -le 4 ] || { _hbad="response files nest more than four deep"; break; }
        _next="$scratch/tok.$abi.$_depth"
        : > "$_next"
        # An `unknown` record is rejected at EVERY depth, before containment:
        # it is not a path and must never reach the resolver (1157 F3).
        _u=$(awk -F"$_TAB" '$1 == "unknown" { print $2; exit }' "$_pending")
        [ -z "$_u" ] || { _hbad="the receipt carries an unrecognised path-bearing option, which is refused rather than skipped: $_u"; break; }
        while IFS="$_TAB" read -r _kind _pth; do
            [ -n "${_pth:-}" ] || continue
            in_roots "$_pth"
            case $? in
                0) ;;
                2) _hbad="a receipt input cannot be physically resolved (non-absolute, unresolvable, or a '..' beyond an existing path): $_pth"; break ;;
                *) _hbad="a target input comes from outside the allowed roots (source, this stage, accepted Phase 0.2, pinned NDK): $_pth"; break ;;
            esac
            in_manifest "$_pth"
            case $? in
                0) ;;
                2) _hbad="cannot decide manifest membership for the receipt input: $_pth"; break ;;
                *) _hbad="a receipt input lies under the source tree but is not in the hashed input manifest, so it could change the DSO without moving the source digest: $_pth"; break ;;
            esac
            [ "$_kind" = rsp ] || continue
            _snap=$(snap_of "$_pth")
            [ -n "$_snap" ] \
                || { _hbad="a response file is referenced but not declared, so its bytes are unbound: $_pth"; break; }
            printf '%s\n' "$_pth" >> "$_usedrsp"
            rcpt_classify "$_snap" >> "$_next" 2>/dev/null \
                || { _hbad="cannot classify the verified snapshot of the response file $_pth"; break; }
        done < "$_pending"
        [ -z "$_hbad" ] || break
        _pending=$_next
    done
    [ -z "$_hbad" ] || { fail "$abi.hermetic" "$_hbad"; continue; }
    _unref=$(awk -F"$_TAB" 'NF { print $1 }' "$_rman" | sort -u > "$scratch/rd.$abi"
             sort -u "$_usedrsp" > "$scratch/ru.$abi"
             comm -23 "$scratch/rd.$abi" "$scratch/ru.$abi" | sed -n '1p')
    [ -z "$_unref" ] \
        || { fail "$abi.hermetic" "a response file is declared but never referenced by the recorded commands: $_unref"; continue; }
    # A pinned-only Phase 0.2 output must not be GIVEN to the linker either.
    # The link row judges what the linker selected; an unselected archive can
    # still be named by the command, so every referenced response file's
    # verified snapshot is scanned for it -- as a path, or as -l:NAME / -lSTEM.
    _pinrsp=""
    while IFS= read -r _up; do
        [ -n "$_up" ] || continue
        _snap=$(snap_of "$_up")
        [ -n "$_snap" ] || { _pinrsp="no verified snapshot for the referenced response file $_up"; break; }
        for a in $DEP_PINNED_ARCHIVES; do
            _hit=$(awk -v b="$a" '{
                       n = split($0, tok, /[ \t]+/)
                       for (i = 1; i <= n; i++) {
                           t = tok[i]; if (t == "") continue
                           if (t ~ /^-l:/)                 { t = substr(t, 4) }
                           else if (t ~ /^-l[^:]/)         { t = "lib" substr(t, 3) ".a" }
                           k = split(t, q, "/")
                           if (q[k] == b) { print tok[i]; exit }
                       }
                   }' "$_snap") \
                || { _pinrsp="cannot scan the verified snapshot of the response file $_up"; break 2; }
            [ -z "$_hit" ] \
                || { _pinrsp="the pinned-only archive $a is named in the link response file $_up ($_hit): it is hash-verified but must never be given to the linker"; break 2; }
        done
    done < "$scratch/ru.$abi"
    [ -z "$_pinrsp" ] || { fail "$abi.hermetic" "$_pinrsp"; continue; }

    pass "$abi.hermetic" "receipt records the real compile and link commands with no diagnostic; the linking command produced this map; every response file is content-bound and referenced and names no pinned-only archive; every resolved input lies under an allowed root"

    # -- the staged picotls prefix --------------------------------------------
    # LibMoQ's configure took picotls from ONE prefix the recipe staged under its
    # build root. Every file in it must be a byte-identical copy of an accepted
    # input -- the pinned picotls header tree, and THIS ABI's picotls archives as
    # this provenance pins them -- the prefix must hold nothing else, and the
    # configure must actually have selected it: the cached picotls entries and
    # the first picotls.h a picoquic source is compiled against.
    _ptb=""
    _pman="$adir/ptls-prefix.manifest"
    _ppfx=$(props_one "$tprov" ptls.prefix) || _ptb="the stage declares no single 'ptls.prefix'"
    [ -n "$_ptb" ] || _pmh=$(props_one "$tprov" ptls.prefix.manifest.sha256) \
        || _ptb="the stage declares no single 'ptls.prefix.manifest.sha256'"
    if [ -z "$_ptb" ]; then
        _pmg=$(decl_sha256 "$_pman") && decl_is_sha256 "$_pmg" || _ptb="cannot hash the picotls prefix manifest"
    fi
    [ -n "$_ptb" ] || [ "$_pmg" = "$_pmh" ] \
        || _ptb="the picotls prefix manifest hashes $_pmg, not the declared $_pmh"
    [ -z "$_ptb" ] || { fail "$abi.ptls.manifest" "$_ptb"; continue; }
    _ppp=$(cd "$_ppfx" 2>/dev/null && pwd -P) \
        || { fail "$abi.ptls.prefix" "the declared picotls prefix is not readable: $_ppfx"; continue; }
    [ "$_ppp" = "$_brootp/ptls-prefix" ] \
        || { fail "$abi.ptls.prefix" "the picotls prefix is $_ppp, not the build root's ptls-prefix ($_brootp/ptls-prefix)"; continue; }

    # The exact file set: the whole accepted header tree plus the picotls archives.
    _psrcinc="$dep_stage/_src/picotls/include"
    if ! ( cd "$_psrcinc" && find . -type f -print ) > "$scratch/pw.$abi" 2>/dev/null; then
        fail "$abi.ptls.inventory" "cannot inventory the accepted picotls headers at $_psrcinc"; continue
    fi
    { sed 's|^\./|include/|' "$scratch/pw.$abi"
      for _pa in $PTLS_PREFIX_ARCHIVES; do printf 'lib/%s\n' "$_pa"; done; } | sort > "$scratch/pwant.$abi"
    cut -f1 "$_pman" | sort > "$scratch/pman.$abi"
    if ! cmp -s "$scratch/pwant.$abi" "$scratch/pman.$abi"; then
        _pd=$(comm -3 "$scratch/pwant.$abi" "$scratch/pman.$abi" | sed -n '1p' | tr -d '\t')
        fail "$abi.ptls.inventory" "the picotls prefix manifest does not list exactly the accepted headers and this ABI's picotls archives (first difference: $_pd)"; continue
    fi

    # Each entry: the right accepted source, still equal to it, pinned for this
    # ABI where it is an archive, and staged byte-identically.
    _ptb=""
    while IFS="$_TAB" read -r _prel _phash _psrc; do
        case $_prel in
            include/*) _pwsrc="$_psrcinc/${_prel#include/}" ;;
            lib/*)     _pwsrc="$dep_stage/$abi/lib/${_prel#lib/}" ;;
            *) _ptb="unclassified picotls prefix entry: $_prel"; break ;;
        esac
        [ "$_psrc" = "$_pwsrc" ] || { _ptb="$_prel is recorded as copied from $_psrc, not the accepted $_pwsrc"; break; }
        _pcur=$(decl_sha256 "$_pwsrc") && decl_is_sha256 "$_pcur" \
            || { _ptb="cannot hash the accepted source $_pwsrc"; break; }
        [ "$_pcur" = "$_phash" ] || { _ptb="the accepted source $_pwsrc now hashes $_pcur, not the recorded $_phash"; break; }
        case $_prel in lib/*)
            _ppin=$(props_one "$tprov" "archive.${_prel#lib/}") \
                || { _ptb="no single archive.${_prel#lib/} pin for $abi"; break; }
            [ "$_ppin" = "$_phash" ] \
                || { _ptb="the staged ${_prel#lib/} hashes $_phash, not $abi's pinned archive $_ppin"; break; } ;;
        esac
        [ -f "$_ppp/$_prel" ] && [ ! -L "$_ppp/$_prel" ] \
            || { _ptb="the staged $_prel is missing or not a regular file"; break; }
        _pst=$(decl_sha256 "$_ppp/$_prel") && decl_is_sha256 "$_pst" \
            || { _ptb="cannot hash the staged $_prel"; break; }
        [ "$_pst" = "$_phash" ] || { _ptb="the staged $_prel hashes $_pst, not its recorded source $_phash"; break; }
    done < "$_pman"
    [ -z "$_ptb" ] || { fail "$abi.ptls.staged" "$_ptb"; continue; }
    if ! find "$_ppp" ! -type d ! -type f -print > "$scratch/pspec.$abi" 2>/dev/null; then
        fail "$abi.ptls.staged" "the non-regular inventory of the staged prefix failed"; continue
    fi
    _pspec=$(sed -n '1p' "$scratch/pspec.$abi")
    [ -z "$_pspec" ] || { fail "$abi.ptls.staged" "non-regular entry in the staged prefix: $_pspec"; continue; }
    if ! ( cd "$_ppp" && find . -type f -print ) > "$scratch/pgotraw.$abi" 2>/dev/null; then
        fail "$abi.ptls.staged" "the inventory of the staged prefix failed"; continue
    fi
    sed 's|^\./||' "$scratch/pgotraw.$abi" | sort > "$scratch/pgot.$abi"
    if ! cmp -s "$scratch/pwant.$abi" "$scratch/pgot.$abi"; then
        _pd=$(comm -3 "$scratch/pwant.$abi" "$scratch/pgot.$abi" | sed -n '1p' | tr -d '\t')
        fail "$abi.ptls.staged" "the staged prefix does not hold exactly the recorded files (first difference: $_pd)"; continue
    fi

    # What the configure selected: every picotls cache entry names the prefix.
    _pcache="$_brootp/CMakeCache.txt"
    [ -f "$_pcache" ] || { fail "$abi.ptls.cmake" "no CMake cache in the build root: $_pcache"; continue; }
    _ptb=""
    for _pent in "MOQ_PICOTLS_PREFIX:$_ppp" "PTLS_INCLUDE_DIR:$_ppp/include" \
                 "PTLS_CORE_LIBRARY:$_ppp/lib/libpicotls-core.a" \
                 "PTLS_MINICRYPTO_LIBRARY:$_ppp/lib/libpicotls-minicrypto.a" \
                 "PTLS_OPENSSL_LIBRARY:$_ppp/lib/libpicotls-openssl.a"; do
        _pvar=${_pent%%:*}; _pwant=${_pent#*:}
        _pval=$(sed -n "s/^$_pvar:[A-Z]*=//p" "$_pcache")
        [ "$(printf '%s\n' "$_pval" | awk 'NF { n++ } END { print n + 0 }')" -eq 1 ] \
            || { _ptb="the build root's CMake cache holds no single $_pvar"; break; }
        if [ -d "$_pval" ]; then _preal=$(cd "$_pval" && pwd -P)
        elif [ -f "$_pval" ]; then _preal="$(cd "$(dirname "$_pval")" && pwd -P)/$(basename "$_pval")"
        else _ptb="$_pvar=$_pval does not exist"; break; fi
        [ "$_preal" = "$_pwant" ] || { _ptb="the configure selected $_pvar=$_preal, not the staged $_pwant"; break; }
    done
    [ -z "$_ptb" ] || { fail "$abi.ptls.cmake" "$_ptb"; continue; }

    # ... and picoquic's tls_api.c finds picotls.h in the staged prefix first.
    # The compile database is parsed as JSON and each command tokenized with its
    # quoting (CMake's own string(JSON) and separate_arguments), and include
    # directories are walked in the compiler's search order for a quoted
    # include: the source's directory, then every -iquote, every -I, every
    # -isystem, every -idirafter -- by class, not by where each option appears.
    # EVERY tls_api.c entry must agree; a malformed, absent, ambiguous or
    # unmodelled entry (a response file, -I-, a prefix option) is refused.
    _pcc="$_brootp/compile_commands.json"
    [ -f "$_pcc" ] || { fail "$abi.ptls.header" "no compile commands in the build root: $_pcc"; continue; }
    rm -f "$scratch/psup.$abi"
    # tls_api.c includes "picotls.h", and "picotls/openssl.h" unless its compile
    # defines PTLS_WITHOUT_OPENSSL; each must come from the staged prefix.
    if ! "$cmake" -DCC_JSON="$_pcc" -DSOURCE_SUFFIX=picoquic/tls_api.c -DKIND=quote \
            "-DHEADERS=picotls.h|picotls/openssl.h!PTLS_WITHOUT_OPENSSL" \
            -DOUT="$scratch/psup.$abi" -P "$scratch/ptls_supplier.cmake" > "$scratch/psup.log.$abi" 2>&1; then
        fail "$abi.ptls.header" "the compile-database parser failed: $(sed -n '1p' "$scratch/psup.log.$abi")"; continue
    fi
    ptls_suppliers_ok "$scratch/psup.$abi" "picoquic's tls_api.c" || { fail "$abi.ptls.header" "$_psbad"; continue; }

    # The JNI substrate is compiled by the recipe itself, outside CMake: its
    # recorded compile must take picotls.h from the same staged prefix, so the
    # build has ONE picotls include authority.
    _pjmsg="picoquic compiles against its headers"
    if [ "$tmode" = product ]; then
        _pjmsg="picoquic compiles against its headers, and the compiler's own record of the JNI substrate compile -- exactly this worktree's moq5_jni.c, writing exactly the object the final link consumed -- takes them from the prefix too"
        # The COMPILER that ran the JNI substrate compile wrote its own
        # compilation-database entry (clang -MJ) into the stage, and the
        # provenance binds it. That record, not the receipt's space-joined
        # line, says which arguments the compile really had; moq5_jni.c's
        # <picotls.h> (an angle include) must resolve from the staged prefix.
        _pjrec="$adir/jni-compile.json"
        _pjh=$(props_one "$tprov" jni.compile.sha256) \
            || { fail "$abi.ptls.jni" "the stage declares no single 'jni.compile.sha256'"; continue; }
        _pjg=$(decl_sha256 "$_pjrec") && decl_is_sha256 "$_pjg" \
            || { fail "$abi.ptls.jni" "cannot hash the compiler's JNI compile record"; continue; }
        [ "$_pjg" = "$_pjh" ] \
            || { fail "$abi.ptls.jni" "the JNI compile record hashes $_pjg, not the declared $_pjh"; continue; }
        # -MJ writes one database ENTRY followed by a comma; the array around it
        # is added here, and nothing else about the record is rewritten.
        { printf '[\n'; sed '$ s/,[[:space:]]*$//' "$_pjrec"; printf ']\n'; } > "$scratch/pjrec.$abi" \
            || { fail "$abi.ptls.jni" "cannot read the JNI compile record"; continue; }
        # Its identity: exactly this worktree's JNI source, writing exactly the
        # object the receipt's final link command consumed.
        _pjsrc="$(cd "$repo/bindings/kotlin/jni" 2>/dev/null && pwd -P)/moq5_jni.c"
        _pjaip=$(cd "$adir/audit-inputs" 2>/dev/null && pwd -P) \
            || { fail "$abi.ptls.jni" "the stage's audit-inputs directory is not readable"; continue; }
        grep '^moq-topology: link: ' "$log" | grep -v '^moq-topology: link: done$' \
            | tr ' ' '\n' | grep '/moq5_jni\.o$' > "$scratch/pjobj.$abi" || true
        [ "$(awk 'NF { n++ } END { print n + 0 }' "$scratch/pjobj.$abi")" -eq 1 ] \
            || { fail "$abi.ptls.jni" "the receipt's final link does not name exactly one moq5_jni.o"; continue; }
        _pjlo=$(sed -n '1p' "$scratch/pjobj.$abi")
        _pjlod=$(cd "$(dirname "$_pjlo")" 2>/dev/null && pwd -P) \
            || { fail "$abi.ptls.jni" "the linked JNI object's directory is not readable: $_pjlo"; continue; }
        [ "$_pjlod/moq5_jni.o" = "$_pjaip/moq5_jni.o" ] \
            || { fail "$abi.ptls.jni" "the final link consumed $_pjlod/moq5_jni.o, not the stage's audit-inputs/moq5_jni.o"; continue; }
        if ! "$cmake" -DCC_JSON="$scratch/pjrec.$abi" -DSOURCE_SUFFIX=jni/moq5_jni.c -DKIND=angle \
                -DEXPECT_FILE="$_pjsrc" -DEXPECT_OUTPUT="$_pjaip/moq5_jni.o" \
                -DHEADERS=picotls.h -DOUT="$scratch/pjsup.$abi" -P "$scratch/ptls_supplier.cmake" \
                > "$scratch/pjsup.log.$abi" 2>&1; then
            fail "$abi.ptls.jni" "the compile-record parser failed: $(sed -n '1p' "$scratch/pjsup.log.$abi")"; continue
        fi
        ptls_suppliers_ok "$scratch/pjsup.$abi" "the JNI substrate" || { fail "$abi.ptls.jni" "$_psbad"; continue; }
    else
        note "$abi.ptls.jni" "a synthetic stage compiles no JNI substrate against picotls; the product receipt carries this proof"
    fi
    _pn=$(awk 'NF { n++ } END { print n + 0 }' "$_pman")
    pass "$abi.ptls" "the staged picotls prefix holds exactly $_pn byte-identical copies of the accepted headers and this ABI's pinned archives; the configure selected it; $_pjmsg"

    # -- 16 KiB load alignment: a GATE, not an observation --------------------
    # Android requires every LOAD segment of a native library to be aligned to
    # at least 2**14 so a 16 KiB page-size device can map it. This was a NOTE
    # read from the FIRST LOAD row while Phase 0.3 was in flight; it is now a
    # failure, it reads EVERY LOAD row, and it covers the diagnostic twin as
    # well as the package -- the twin is what the strip oracle reproduces the
    # package from, so an unaligned twin is an unaligned package in waiting.
    #
    # This is ARTIFACT evidence. The linker flags that produce it are a recipe
    # obligation, proven separately in the receipt.
    _albad=""
    for _img in "$pkg" "$diag"; do
        if ! "$readelf" --wide -l "$_img" > "$scratch/phdr.$abi" 2>/dev/null; then
            _albad="the program-header producer failed on $_img, so alignment is UNMEASURED -- a missing row is never an aligned row"
            break
        fi
        awk '$1 == "LOAD" { print $NF }' "$scratch/phdr.$abi" > "$scratch/al.$abi" 2>/dev/null \
            || { _albad="the program-header parser failed on $_img"; break; }
        _aln=$(awk 'NF { n++ } END { print n + 0 }' "$scratch/al.$abi")
        [ "$_aln" -gt 0 ] \
            || { _albad="$_img reports no LOAD segments at all; an image with nothing to map cannot be declared aligned"; break; }
        while IFS= read -r _av; do
            [ -n "$_av" ] || continue
            case $_av in
                0x*|0X*|[0-9]*) ;;
                *) _albad="an unparseable alignment field '$_av' in $_img"; break ;;
            esac
            [ "$(( _av ))" -ge "$MIN_LOAD_ALIGN" ] \
                || { _albad="$_img has a LOAD segment aligned to $_av, below the required $MIN_LOAD_ALIGN (2**14) -- Android cannot map it on a 16 KiB page-size device"; break; }
        done < "$scratch/al.$abi"
        [ -z "$_albad" ] || break
    done
    if [ -n "$_albad" ]; then
        fail "$abi.alignment" "$_albad"
    else
        pass "$abi.alignment" "every LOAD segment of the packaged and diagnostic images is aligned to at least 2**14"
    fi

    abi_bad && note "$abi" "one or more oracles failed for this ABI"
done

finish
