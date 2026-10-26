#!/usr/bin/env bash
#
# Install-tree consumer test for raw picoquic managed (moq_pq_threaded) +
# the certificate-verifier helper (<moq/picoquic_verify.h>).
#
# Installs picoquic and libmoq to a temp prefix, then consumes the threaded
# facade + verifier helper against THAT prefix — not the build tree — two
# ways:
#   (A) CMake: find_package(libmoq COMPONENTS adapter-picoquic-threaded)
#              + link moq::adapter-picoquic-threaded
#   (B) pkg-config: pkg-config --cflags --libs libmoq
# The consumer source includes only <moq/picoquic_threaded.h> +
# <moq/picoquic_verify.h>, with no source-tree include paths.
#
# Args:
#   1  cmake executable
#   2  consumer source dir (CMake project: consumer-threaded-install)
#   3  consumer .c source  (same project's main.c, for the pkg-config path)
#   4  libmoq build dir (already built, MOQ_BUILD_PQ_THREADED=ON)
#   5  picoquic source dir
#   6  picotls prefix (…/picotls/build)
#   7  C compiler
#   8  C flags        (forwarded so ASAN parent → ASAN consumer)
#   9  exe linker flags
#  10  build type
#  11  openssl root   (may be empty)
#  12  work dir       (created fresh; removed on success)
#  13  install libdir (CMAKE_INSTALL_LIBDIR: lib, lib64, lib/<arch>, …)
#  14  LibMoQ's selected picoquic verifier backend: openssl|mbedtls (mandatory)
#  15  the parent's actual picoquic WITH_OPENSSL value: ON|OFF (mandatory)
#  16  the parent's actual picoquic WITH_MBEDTLS value: ON|OFF (mandatory)
#      15/16 travel separately from 14: both upstream providers may be enabled
#      while only one verifier is selected, so the verifier is not a proxy.
#  17  the parent's CMAKE_PREFIX_PATH (may be empty)
#  18  the parent's resolved PTLS_CORE_LIBRARY (may be empty)
#  19  the parent's resolved PTLS_MINICRYPTO_LIBRARY (may be empty)
#      18/19 are forwarded verbatim and compared by identity against the
#      dependency cache, so a different existing archive cannot pass.
#  20  the parent's generated provenance manifest (mandatory). CMakeCache.txt
#      is not a total authority -- PTLS_* may be CMake-scope values that
#      never reach it -- so the parent writes what it actually selected and
#      resolved, and this script compares against that.
set -euo pipefail

# Sourcing with MOQ_CONSUMER_LIB_ONLY=1 makes this driver define its helper
# functions and return without executing anything, so the permanent self-test
# exercises THIS driver's own canonicalization rather than a copy of it.
# Placeholder positionals are supplied so the argument contract below is not
# bypassed by the seam -- it still runs, on values that touch nothing.
if [ "${MOQ_CONSUMER_LIB_ONLY:-0}" = "1" ]; then
    set -- lib-only lib-only lib-only lib-only lib-only lib-only lib-only \
           "" "" lib-only "" lib-only lib-only openssl ON OFF "" "" "" lib-only
fi

# Exact argument count FIRST: before any positional is dereferenced and before
# any filesystem mutation, so a short call produces this named diagnostic
# rather than an unbound-variable abort.
if [ "$#" -ne 20 ]; then
    echo "FAIL: expected 20 arguments, got $#" >&2
    exit 1
fi
ARGC="$#"

CMAKE="$1"; CONSUMER_SRC="$2"; CONSUMER_MAIN="$3"; LIBMOQ_BUILD="$4"
PICOQUIC_SRC="$5"; PICOTLS_PREFIX="$6"; CC="$7"; CFLAGS="$8"; LDFLAGS="$9"
BUILD_TYPE="${10}"; OPENSSL_ROOT="${11}"; WORK="${12}"; LIBDIR="${13}"
VERIFIER="${14-}"; PARENT_WITH_OPENSSL="${15-}"; PARENT_WITH_MBEDTLS="${16-}"
PARENT_PREFIX_PATH="${17-}"; PARENT_PTLS_CORE="${18-}"
PARENT_PTLS_MINICRYPTO="${19-}"; PARENT_MANIFEST="${20-}"

P="$WORK/prefix"
PQB="$WORK/pq-build"
CONB="$WORK/consumer-build"

if [ -z "$PICOTLS_PREFIX" ]; then
    PICOTLS_PREFIX="$(cd "$PICOQUIC_SRC/../picotls/build" && pwd)"
fi
PTLS_SRC="$(dirname "$PICOTLS_PREFIX")"

# -- provider provenance (mandatory) ---------------------------------------
#
# The dependency build must be the SAME provider the parent artifact was built
# with, and that must be PROVEN from the dependency's own cache rather than
# assumed from the flags. The selected LibMoQ verifier is not a proxy for
# picoquic's build matrix: both upstream providers can be enabled while one
# verifier is selected, so all three values travel separately.
for _req in VERIFIER:"$VERIFIER" WITH_OPENSSL:"$PARENT_WITH_OPENSSL" \
            WITH_MBEDTLS:"$PARENT_WITH_MBEDTLS"; do
    case "$_req" in
        *:) echo "FAIL: mandatory provenance ${_req%%:*} is empty" >&2; exit 1 ;;
    esac
done
case "$VERIFIER" in
    openssl|mbedtls) ;;
    *) echo "FAIL: unknown LibMoQ verifier backend '$VERIFIER'" >&2; exit 1 ;;
esac
case "$PARENT_WITH_OPENSSL" in ON|OFF) ;; *)
    echo "FAIL: WITH_OPENSSL must be ON or OFF, got '$PARENT_WITH_OPENSSL'" >&2
    exit 1 ;;
esac
case "$PARENT_WITH_MBEDTLS" in ON|OFF) ;; *)
    echo "FAIL: WITH_MBEDTLS must be ON or OFF, got '$PARENT_WITH_MBEDTLS'" >&2
    exit 1 ;;
esac

# split_pc_words <text>: the words of one line of pkg-config output, in the
# array split_out. pkg-config escapes a space in a path with a backslash, so its
# output is shell-quoted text: a plain `read -a` split would cut "-I/a\ b/inc"
# into two broken words. cmake/moq_split_command.cmake parses it with CMake's
# separate_arguments(UNIX_COMMAND) -- nothing is evaluated -- and input it would
# have to guess about (a quote character, a newline, a trailing backslash)
# fails by name here instead. Called directly, not through a command
# substitution, so a failure exits the driver.
MOQ_SPLIT_CMAKE="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/../../.." && pwd)/cmake/moq_split_command.cmake"

split_pc_words() {
    local text="$1" parsed line rc=0
    split_out=()
    case "$text" in
        *\'*|*\"*)
            echo "FAIL: pkg-config output contains a quote character, which is not parsed: $text" >&2
            exit 1 ;;
        *$'\n'*)
            echo "FAIL: pkg-config output spans more than one line: $text" >&2
            exit 1 ;;
        *\\)
            echo "FAIL: pkg-config output ends in a dangling backslash: $text" >&2
            exit 1 ;;
    esac
    if [ ! -f "$MOQ_SPLIT_CMAKE" ]; then
        echo "FAIL: the pkg-config word splitter $MOQ_SPLIT_CMAKE is missing" >&2
        exit 1
    fi
    parsed="$("$CMAKE" -DMOQ_SPLIT_IN="$text" -P "$MOQ_SPLIT_CMAKE" 2>&1)" || rc=$?
    if [ "$rc" != 0 ]; then
        echo "FAIL: could not split pkg-config output '$text': $parsed" >&2
        exit 1
    fi
    while IFS= read -r line; do
        [ -z "$line" ] && continue
        case "$line" in
            "MOQ-ARG "*) split_out+=("${line#MOQ-ARG }") ;;
            *) echo "FAIL: unexpected splitter output for '$text': $line" >&2
               exit 1 ;;
        esac
    done <<< "$parsed"
}

# Every command array this driver ever runs is built by one of these builders,
# so normal execution and preflight construct the SAME arrays. A builder that
# is deleted or broken therefore fails preflight instead of hiding until a
# real build runs.

build_prov_args() {
    # Bash 3.2 (the macOS default) treats "${arr[@]}" on an EMPTY array as an
    # unbound variable under `set -u`, so an empty array is never expanded.
    prov_args=(-DWITH_OPENSSL="$PARENT_WITH_OPENSSL"
               -DWITH_MBEDTLS="$PARENT_WITH_MBEDTLS")
    if [ -n "$PARENT_PREFIX_PATH" ]; then
        prov_args+=(-DCMAKE_PREFIX_PATH="$PARENT_PREFIX_PATH")
    fi
    if [ -n "$PARENT_PTLS_CORE" ]; then
        prov_args+=(-DPTLS_CORE_LIBRARY="$PARENT_PTLS_CORE")
    fi
    if [ -n "$PARENT_PTLS_MINICRYPTO" ]; then
        prov_args+=(-DPTLS_MINICRYPTO_LIBRARY="$PARENT_PTLS_MINICRYPTO")
    fi
    if [ -n "$OPENSSL_ROOT" ]; then
        prov_args+=(-DOPENSSL_ROOT_DIR="$OPENSSL_ROOT")
    fi
}

build_con_args() {  # $1 = install prefix
    con_args=(-DCMAKE_PREFIX_PATH="$1")
    if [ -n "$OPENSSL_ROOT" ]; then
        con_args+=(-DOPENSSL_ROOT_DIR="$OPENSSL_ROOT")
    fi
}

build_cc_arrays() {  # $1 = pkg-config cflags, $2 = pkg-config libs,
                     # $3 = runtime search directory (optional; shared installs)
    # pkg-config output is shell-quoted, so split_pc_words parses it into words
    # (a path with a space stays one word). The forwarded parent flags are
    # plain flag lists, split once and explicitly by `read -r -a` -- so the
    # intent is expressed in code rather than left to an unquoted expansion
    # (SC2086).
    split_pc_words "$1"
    pc_cflags_arr=()
    if [ ${#split_out[@]} -gt 0 ]; then pc_cflags_arr=("${split_out[@]}"); fi
    split_pc_words "$2"
    pc_libs_arr=()
    if [ ${#split_out[@]} -gt 0 ]; then pc_libs_arr=("${split_out[@]}"); fi
    read -r -a cflags_arr    <<< "$CFLAGS"
    read -r -a ldflags_arr   <<< "$LDFLAGS"

    cc_compile=("$CC")
    [ ${#cflags_arr[@]}    -gt 0 ] && cc_compile+=("${cflags_arr[@]}")
    [ ${#pc_cflags_arr[@]} -gt 0 ] && cc_compile+=("${pc_cflags_arr[@]}")
    cc_compile+=(-c "$CONSUMER_MAIN" -o "$WORK/pkgc.o")

    cc_link=("$CC")
    [ ${#ldflags_arr[@]} -gt 0 ] && cc_link+=("${ldflags_arr[@]}")
    cc_link+=("$WORK/pkgc.o")
    [ ${#pc_libs_arr[@]} -gt 0 ] && cc_link+=("${pc_libs_arr[@]}")
    if [ -n "${3-}" ]; then cc_link+=("-Wl,-rpath,$3"); fi
    cc_link+=(-o "$WORK/pkgc-consumer")
}

# MOQ_CONSUMER_PREFLIGHT=1 constructs EVERY final configure/compile/link array
# and stops. It builds nothing, deletes nothing and configures nothing, so the
# construction can be exercised for every optional-argument combination under
# any shell. Deterministic synthetic pkg-config output stands in for the real
# tool, which is not run. The emitted inventory -- one line per array element
# plus exact per-array counts -- is what a caller judges; a generic marker
# would not prove that each optional value entered its intended array.
preflight_finish() {
    [ "${MOQ_CONSUMER_PREFLIGHT:-0}" = "1" ] || return 0

    build_prov_args
    build_con_args "${MOQ_PREFLIGHT_PREFIX:-/preflight/prefix}"
    build_cc_arrays "${MOQ_PREFLIGHT_PC_CFLAGS-"-I/preflight/inc"}" \
        "${MOQ_PREFLIGHT_PC_LIBS-"-L/preflight/lib -lmoq"}" \
        "${MOQ_PREFLIGHT_RUNTIME_LIBDIR-}"

    local a
    for a in "${prov_args[@]}";  do printf 'PREFLIGHT-ITEM prov %s\n' "$a"; done
    for a in "${con_args[@]}";   do printf 'PREFLIGHT-ITEM con %s\n' "$a"; done
    for a in "${cc_compile[@]}"; do printf 'PREFLIGHT-ITEM compile %s\n' "$a"; done
    for a in "${cc_link[@]}";    do printf 'PREFLIGHT-ITEM link %s\n' "$a"; done
    printf 'PREFLIGHT-COUNTS prov=%d con=%d compile=%d link=%d\n' \
        "${#prov_args[@]}" "${#con_args[@]}" \
        "${#cc_compile[@]}" "${#cc_link[@]}"
    printf 'PREFLIGHT-OK %d args\n' "$ARGC"
    exit 0
}

# Every command array is now constructible; preflight stops here having
# mutated nothing.
preflight_finish

# Only now is anything on disk touched.
rm -rf "$WORK"
mkdir -p "$WORK"

cache_value() {  # $1 cache file, $2 name -> prints the value or nothing
    grep -m1 "^$2:" "$1" 2>/dev/null | cut -d= -f2- || true
}

# The PARENT's own cache is the authority, not the forwarded strings: comparing
# the dependency against what was forwarded would only prove the flags took
# effect, and a wrong forward would agree with itself. So the forwarded values
# are first checked AGAINST THE PARENT CACHE, and the dependency is then checked
# against the parent cache too. Artifact paths are compared by identity, so a
# different existing archive cannot pass.
manifest_value() {  # $1 key -> prints the value or nothing
    grep -m1 "^$1=" "$PARENT_MANIFEST" 2>/dev/null | cut -d= -f2- || true
}

# An artifact IDENTITY, not merely a non-empty string. A target name or a
# nonexistent path compares equal to itself and proves nothing, so every value
# claiming to name an artifact must be an absolute path to an existing file and
# is resolved to its fully canonical form before comparison.
#
# The resolution covers the FINAL component's symlink too, so two differently
# named aliases of the same archive compare equal. `pwd -P` on the parent plus
# `basename` would leave that link unresolved; `readlink -f` is not portable
# (BSD readlink has no -f). CMake's file(REAL_PATH) is, and the exact CMake
# executable is already an argument to this driver.
# ${BASH_SOURCE[0]} is this file whether executed or sourced; $0 is not.
MOQ_REAL_PATH_CMAKE="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/../../.." && pwd)/cmake/moq_real_path.cmake"
MOQ_RUN_INSTALLED="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)/run_installed_consumer.sh"

canonical_artifact() {  # <label> <value> -> prints the canonical path
    local label="$1" value="$2" resolved
    case "$value" in
        /*) ;;
        *) echo "FAIL: $label='$value' is not an absolute path, so it is a name rather than a resolved artifact" >&2
           exit 1 ;;
    esac
    if [ ! -f "$value" ]; then
        echo "FAIL: $label='$value' does not name an existing file" >&2
        exit 1
    fi
    if [ ! -f "$MOQ_REAL_PATH_CMAKE" ]; then
        echo "FAIL: the path resolver $MOQ_REAL_PATH_CMAKE is missing" >&2
        exit 1
    fi
    resolved="$("$CMAKE" -DMOQ_RP_IN="$value" -P "$MOQ_REAL_PATH_CMAKE" 2>&1)"
    if [ -z "$resolved" ] || [ ! -f "$resolved" ]; then
        echo "FAIL: $label='$value' did not resolve to an existing file (got '$resolved')" >&2
        exit 1
    fi
    printf '%s\n' "$resolved"
}

if [ "${MOQ_CONSUMER_LIB_ONLY:-0}" = "1" ]; then
    # Sourced: end the source here, with every helper above defined. Executed
    # with the seam set: `return` is invalid at top level, so exit instead.
    # The two cases are distinguished explicitly rather than by a conditional
    # `return` whose failure branch a linter cannot see.
    if [ "${BASH_SOURCE[0]}" != "$0" ]; then
        return 0
    fi
    exit 0
fi

# -- runtime search for a shared install ------------------------------------
#
# pkg-config supplies compile and link flags only. When the link resolves to a
# SHARED library in the installed libdir, finding it at run time is the
# consumer's own job, as for any pkg-config consumer: the consumer records that
# libdir as its runtime search path (one linker argument, built in the same
# array builder preflight uses). That entry proves where the link resolved; it
# does not reach the libraries' own dependencies on ELF, so the run goes
# through run_installed_consumer.sh, which supplies the consumer's runtime
# search path and proves every LibMoQ/PicoQUIC image came from that libdir --
# a build-tree or source-tree image, or none at all, fails by name. A static
# install records nothing and must load no LibMoQ/PicoQUIC image.

# resolve_runtime_libdir <packages> <pkg-config libs>: sets RUNTIME_LIBDIR to
# the canonical installed libdir when one of the named libraries is installed
# there in shared form, or to the empty string for a static install. Called
# directly (not through a command substitution) so a failure exits the driver.
resolve_runtime_libdir() {
    local pkgs="$1" libs="$2" pkg d canon tok suffix
    RUNTIME_LIBDIR=""
    local dir=""
    local -a pkg_arr lib_arr
    read -r -a pkg_arr <<< "$pkgs"
    split_pc_words "$libs"
    lib_arr=()
    if [ ${#split_out[@]} -gt 0 ]; then lib_arr=("${split_out[@]}"); fi
    if [ ${#pkg_arr[@]} -eq 0 ] || [ ${#lib_arr[@]} -eq 0 ]; then
        echo "FAIL: no package or no pkg-config libs to resolve a runtime libdir from" >&2
        exit 1
    fi
    for pkg in "${pkg_arr[@]}"; do
        # The variable is escaped like the flags ("/a\ b/lib"): exactly one word.
        split_pc_words "$(pkg-config --variable=libdir "$pkg")"
        if [ ${#split_out[@]} -ne 1 ]; then
            echo "FAIL: pkg-config libdir of $pkg is not exactly one path (${#split_out[@]} words)" >&2
            exit 1
        fi
        d="${split_out[0]}"
        if [ -z "$d" ] || [ ! -d "$d" ]; then
            echo "FAIL: pkg-config libdir of $pkg ('$d') is not a directory" >&2
            exit 1
        fi
        canon="$(cd "$d" && pwd -P)"
        if [ -z "$dir" ]; then
            dir="$canon"
        elif [ "$dir" != "$canon" ]; then
            echo "FAIL: packages '$pkgs' resolve to different libdirs: $dir and $canon" >&2
            exit 1
        fi
    done
    case "$(uname -s)" in Darwin) suffix=.dylib ;; *) suffix=.so ;; esac
    for tok in "${lib_arr[@]}"; do
        case "$tok" in
            -l*) if [ -e "$dir/lib${tok#-l}$suffix" ]; then RUNTIME_LIBDIR="$dir"; fi ;;
        esac
    done
    case "$RUNTIME_LIBDIR" in
        *,*) echo "FAIL: installed libdir '$RUNTIME_LIBDIR' contains a comma, which -Wl, would split" >&2
             exit 1 ;;
    esac
}

# run_consumer <exe>: runs the consumer. For a shared install it first requires
# the executable to record RUNTIME_LIBDIR as a runtime search path (where the
# link resolved), then runs it through run_installed_consumer.sh, which checks
# the loaded images' identity. A static install is run through the same runner
# against the installed libdir, which then requires no shared image at all.
run_consumer() {
    local exe="$1" recorded
    if [ -z "$RUNTIME_LIBDIR" ]; then
        bash "$MOQ_RUN_INSTALLED" "$P/$LIBDIR" "$exe"
        return 0
    fi
    case "$(uname -s)" in
        Darwin)
            # otool prints "path <dir> (offset N)"; <dir> may contain spaces.
            recorded="$(otool -l "$exe" | awk '/cmd LC_RPATH/{r=1} r && $1 == "path" {r=0; sub(/^ *path /, ""); sub(/ \(offset [0-9]+\)$/, ""); print}')"
            ;;
        *)
            recorded="$(readelf -d "$exe" | sed -n 's/.*(\(RUNPATH\|RPATH\)).*\[\(.*\)\]/\2/p' | tr ':' '\n')"
            ;;
    esac
    # grep reads the whole value: no pipe writer can take SIGPIPE under
    # pipefail and turn a match into a miss.
    if ! grep -xF -- "$RUNTIME_LIBDIR" <<<"$recorded" >/dev/null; then
        echo "FAIL: the consumer records no runtime search path to the installed libdir $RUNTIME_LIBDIR (recorded: $(printf '%s' "$recorded" | tr '\n' ' '))" >&2
        exit 1
    fi
    bash "$MOQ_RUN_INSTALLED" "$RUNTIME_LIBDIR" "$exe"
}

assert_forward_matches_parent() {
    if [ ! -f "$PARENT_MANIFEST" ]; then
        echo "FAIL: parent provenance manifest not found at $PARENT_MANIFEST" >&2
        exit 1
    fi
    local m_verifier m_ossl m_mbed m_core m_mini
    m_verifier="$(manifest_value VERIFIER)"
    m_ossl="$(manifest_value WITH_OPENSSL)"
    m_mbed="$(manifest_value WITH_MBEDTLS)"
    m_core="$(manifest_value PTLS_CORE_LIBRARY)"
    m_mini="$(manifest_value PTLS_MINICRYPTO_LIBRARY)"
    if [ "$VERIFIER" != "$m_verifier" ]; then
        echo "FAIL: forwarded verifier=$VERIFIER, parent manifest=$m_verifier" >&2
        exit 1
    fi
    if [ "$PARENT_WITH_OPENSSL" != "$m_ossl" ]; then
        echo "FAIL: forwarded WITH_OPENSSL=$PARENT_WITH_OPENSSL, parent manifest=$m_ossl" >&2
        exit 1
    fi
    if [ "$PARENT_WITH_MBEDTLS" != "$m_mbed" ]; then
        echo "FAIL: forwarded WITH_MBEDTLS=$PARENT_WITH_MBEDTLS, parent manifest=$m_mbed" >&2
        exit 1
    fi
    # In a lane claiming exact PTLS identity, an empty or unresolved identity
    # FAILS BY NAME rather than quietly disabling the comparison.
    if [ "$PARENT_WITH_MBEDTLS" = ON ] || [ -n "$m_core" ] || [ -n "$PARENT_PTLS_CORE" ]; then
        if [ -z "$m_core" ] || [ -z "$PARENT_PTLS_CORE" ]; then
            echo "FAIL: PTLS core identity unresolved (manifest='$m_core' forwarded='$PARENT_PTLS_CORE')" >&2
            exit 1
        fi
        local c_fwd c_man
        c_fwd="$(canonical_artifact "forwarded PTLS_CORE_LIBRARY" "$PARENT_PTLS_CORE")"
        c_man="$(canonical_artifact "manifest PTLS_CORE_LIBRARY" "$m_core")"
        if [ "$c_fwd" != "$c_man" ]; then
            echo "FAIL: forwarded PTLS_CORE_LIBRARY canonicalizes to $c_fwd, parent manifest to $c_man" >&2
            exit 1
        fi
    fi
    if [ "$PARENT_WITH_MBEDTLS" = ON ] || [ -n "$m_mini" ] || [ -n "$PARENT_PTLS_MINICRYPTO" ]; then
        if [ -z "$m_mini" ] || [ -z "$PARENT_PTLS_MINICRYPTO" ]; then
            echo "FAIL: PTLS minicrypto identity unresolved (manifest='$m_mini' forwarded='$PARENT_PTLS_MINICRYPTO')" >&2
            exit 1
        fi
        local n_fwd n_man
        n_fwd="$(canonical_artifact "forwarded PTLS_MINICRYPTO_LIBRARY" "$PARENT_PTLS_MINICRYPTO")"
        n_man="$(canonical_artifact "manifest PTLS_MINICRYPTO_LIBRARY" "$m_mini")"
        if [ "$n_fwd" != "$n_man" ]; then
            echo "FAIL: forwarded PTLS_MINICRYPTO_LIBRARY canonicalizes to $n_fwd, parent manifest to $n_man" >&2
            exit 1
        fi
    fi
    echo "== parent identity: verifier=$m_verifier WITH_OPENSSL=$m_ossl WITH_MBEDTLS=$m_mbed =="
    echo "== parent ptls: core=${m_core:-<unset>} minicrypto=${m_mini:-<unset>} =="
}

assert_provider_cache() {
    local cache="$1/CMakeCache.txt"
    local got_ossl got_mbed got_core got_mini p_core p_mini
    got_ossl="$(cache_value "$cache" WITH_OPENSSL)"
    got_mbed="$(cache_value "$cache" WITH_MBEDTLS)"
    p_core="$(manifest_value PTLS_CORE_LIBRARY)"
    p_mini="$(manifest_value PTLS_MINICRYPTO_LIBRARY)"
    if [ "$got_ossl" != "$PARENT_WITH_OPENSSL" ]; then
        echo "FAIL: dependency WITH_OPENSSL=$got_ossl, parent=$PARENT_WITH_OPENSSL" >&2
        exit 1
    fi
    if [ "$got_mbed" != "$PARENT_WITH_MBEDTLS" ]; then
        echo "FAIL: dependency WITH_MBEDTLS=$got_mbed, parent=$PARENT_WITH_MBEDTLS" >&2
        exit 1
    fi
    if [ -n "$p_core" ]; then
        got_core="$(cache_value "$cache" PTLS_CORE_LIBRARY)"
        local d_core a_core
        d_core="$(canonical_artifact "dependency PTLS_CORE_LIBRARY" "$got_core")"
        a_core="$(canonical_artifact "manifest PTLS_CORE_LIBRARY" "$p_core")"
        if [ "$d_core" != "$a_core" ]; then
            echo "FAIL: dependency PTLS_CORE_LIBRARY canonicalizes to $d_core, parent manifest to $a_core" >&2
            exit 1
        fi
    fi
    if [ -n "$p_mini" ]; then
        got_mini="$(cache_value "$cache" PTLS_MINICRYPTO_LIBRARY)"
        local d_mini a_mini
        d_mini="$(canonical_artifact "dependency PTLS_MINICRYPTO_LIBRARY" "$got_mini")"
        a_mini="$(canonical_artifact "manifest PTLS_MINICRYPTO_LIBRARY" "$p_mini")"
        if [ "$d_mini" != "$a_mini" ]; then
            echo "FAIL: dependency PTLS_MINICRYPTO_LIBRARY canonicalizes to $d_mini, parent manifest to $a_mini" >&2
            exit 1
        fi
    fi
    echo "== provider identity: verifier=$VERIFIER WITH_OPENSSL=$got_ossl WITH_MBEDTLS=$got_mbed =="
}

echo "== 1. install picoquic -> $P =="
build_prov_args
"$CMAKE" -S "$PICOQUIC_SRC" -B "$PQB" \
    -DBUILD_HTTP=ON -DBUILD_DEMO=OFF -DBUILD_PQBENCH=OFF \
    -DBUILD_PICO_SIM=OFF -Dpicoquic_BUILD_TESTS=OFF -DBUILD_LOGLIB=ON \
    -DCMAKE_INSTALL_PREFIX="$P" \
    -DPTLS_PREFIX="$PTLS_SRC" \
    -DCMAKE_LIBRARY_PATH="$PICOTLS_PREFIX" \
    "${prov_args[@]}" \
    -DCMAKE_C_COMPILER="$CC" \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
assert_forward_matches_parent
assert_provider_cache "$PQB"
"$CMAKE" --build "$PQB"
"$CMAKE" --install "$PQB"

echo "== 2. install libmoq -> $P =="
"$CMAKE" --install "$LIBMOQ_BUILD" --prefix "$P"

# Installed-header manifest: both public headers ship.
for hdr in picoquic_threaded.h picoquic_verify.h; do
    if [ ! -f "$P/include/moq/$hdr" ]; then
        echo "FAIL: moq/$hdr was not installed"; exit 1
    fi
done

echo "== 3A. CMake consumer (find_package adapter-picoquic-threaded) =="
# Deliberately NOT passing MOQ_PICOQUIC_SOURCE_DIR: forces resolution of
# the INSTALLED picoquic.
build_con_args "$P"
"$CMAKE" -S "$CONSUMER_SRC" -B "$CONB" \
    "${con_args[@]}" \
    -DCMAKE_C_COMPILER="$CC" \
    -DCMAKE_C_FLAGS="$CFLAGS" \
    -DCMAKE_EXE_LINKER_FLAGS="$LDFLAGS" \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
"$CMAKE" --build "$CONB"
bash "$MOQ_RUN_INSTALLED" "$P/$LIBDIR" "$CONB/moq_adapter_threaded_install_consumer_test"

echo "== 3B. pkg-config consumer (libmoq) =="
if ! command -v pkg-config >/dev/null 2>&1; then
    echo "SKIP: pkg-config not found (CMake consumer already passed)"
else
    export PKG_CONFIG_PATH="$P/$LIBDIR/pkgconfig"
    if ! pkg-config --exists libmoq; then
        echo "FAIL: pkg-config could not find libmoq in $PKG_CONFIG_PATH"
        exit 1
    fi
    PC_CFLAGS="$(pkg-config --cflags libmoq)"
    PC_LIBS="$(pkg-config --libs libmoq)"
    echo "   cflags: $PC_CFLAGS"
    echo "   libs:   $PC_LIBS"
    # Guard: no source-tree include must leak through pkg-config cflags.
    case "$PC_CFLAGS" in
        *"$PICOQUIC_SRC"*|*"$PTLS_SRC"*)
            echo "FAIL: pkg-config cflags leak a source-tree path"; exit 1;;
    esac
    # ONE construction site per final array: the same builder preflight uses,
    # so a preflight pass cannot diverge from the command that actually runs.
    resolve_runtime_libdir libmoq "$PC_LIBS"
    build_cc_arrays "$PC_CFLAGS" "$PC_LIBS" "$RUNTIME_LIBDIR"
    "${cc_compile[@]}"
    "${cc_link[@]}"
    run_consumer "$WORK/pkgc-consumer"
fi

echo "== install-tree threaded consumer OK (CMake + pkg-config) =="
rm -rf "$WORK"
