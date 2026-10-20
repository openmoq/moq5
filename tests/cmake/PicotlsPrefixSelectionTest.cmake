# Pins how FindPicoquic selects picotls from MOQ_PICOTLS_PREFIX, across cold
# configures and reconfigures of one tree. Hermetic: picotls trees are
# placeholder files (find_library only needs the names). Source-tree mode uses
# a stand-in picoquic whose lookups have the shape of picoquic's own FindPTLS
# and records what the provider ends up with; installed mode uses a stand-in
# picoquic CONFIG package, where FindPicoquic selects only the header and
# library directories libmoq itself reads.
#
# Source-tree cases:
#   cold        build-directory prefix beside a decoy sibling checkout
#   a_to_b      reconfigure onto prefix B while A stays on disk
#   stale_hdr   prefix B with a find-module PTLS_INCLUDE_DIR from A cached
#   stale_lib   prefix B with find-module library selections from A cached
#   conflict    an explicit header or library from A with prefix B
#   explicit    a matching explicit header, kept; a later prefix change
#               refused while it stays, accepted once it is dropped (-U)
#   installed   installed-layout prefix nested under an unrelated include
#   invalid     a prefix that is not a picotls tree
#   fetch_*     PICOQUIC_FETCH_PTLS with a stand-in picotls F
#               (picotls_prefix_selection/picotls): headers, library
#               directory, cache entries and the parsed pkg-config -L tokens
#               follow the picotls picoquic builds, for the configuration and
#               generator in use -- default, custom, config-specific, relative
#               and shared output locations; archives in different
#               directories, and multi-config generators, select no library
#               directory and refuse an explicit one; with or without a prefix
#               naming F; a prefix naming another picotls and explicit library
#               selections from another picotls are refused; the mbedTLS
#               configuration, which needs PTLS_INCLUDE_DIR while picoquic is
#               added, configures cold without OpenSSL, from a local source or
#               from a (local) clone; repeated find_package
#   fetch_policy  LibMoQ's picotls policy in a Release build: F compiles with
#               NDEBUG undefined and warnings as errors, other directories
#               keep the configuration's flags; caller hooks (a file and a
#               module) run first, in order and in picotls's scope; the hook
#               variables are left as they were; the isolation probes show
#               exactly their one intended warning
#   binding_*   the caller's bindings of both hook families and their
#               hand-over variables survive find_package by behaviour: unset,
#               empty, normal, cache-only (still follows set_property(CACHE))
#               and normal over cache (falls back to the cache entry)
#   nested_*    find_package beneath a directory holding the PTLS_*_LIBRARY
#               values the fetched FindPTLS sets: accepted; a conflicting one,
#               or a conflicting cache entry under a matching one, refused
#   swap_*      prebuilt A -> fetched F -> prebuilt A reconfigures leave
#               nothing of the other behind
#   unrelated_* a picotls populated through FetchContent but not linked by the
#               selected picoquic (installed package, or a pre-existing
#               prebuilt-linked target) does not displace prefix A
#   pre_*       a pre-existing picoquic that links F, directly or through the
#               picotls::core alias, selects F
# Every configure and build, refusals included, keeps its output and status
# under MOQ_WORK/logs and passes one diagnostic check: no warnings, except a
# probe's single intended one. An expected refusal is evidence only for its
# own error. control_* cases and checks show a stray warning is caught beside
# an expected refusal and beside a probe's intended warning.
# Installed-package cases (cfg_*): cold, A->B reconfigure, stale cached
# selections, a conflicting explicit library directory, and an installed
# prefix nested under an unrelated include.
# Adapter cases (adapter_*), run when MOQ_OPENSSL_ROOT is given and the
# generator is single-config: libmoq itself
# configured with the picoquic adapter and its OpenSSL verifier, reading the
# adapter's include directories from the File API codemodel.
#   adapter_incomplete   verifier headers cached from A, prefix B selected
#                        without <picotls/openssl.h>: refused
#   adapter_complete     verifier headers cached from A, complete prefix C:
#                        C's headers, never A's
#   adapter_system       installed picoquic, no prefix, and a picotls S on
#                        the default search path: S is selected and used
#   adapter_unselected   the same host, with picotls header discovery
#                        settled empty: no selection, so the explicit
#                        verifier header directory is used
#
# Inputs: MOQ_SOURCE (libmoq checkout), MOQ_WORK (recreated), optional
# MOQ_MODULE_DIR (FindPicoquic directory; defaults to MOQ_SOURCE/cmake),
# MOQ_GENERATOR, MOQ_CONTEXT (cache preload carrying the producer toolchain
# and its ABI probe hooks, as written by moq_write_consumer_context),
# MOQ_OPENSSL_ROOT (OpenSSL for the adapter cases).
cmake_minimum_required(VERSION 3.20)

foreach(var MOQ_SOURCE MOQ_WORK)
    if(NOT DEFINED ${var} OR "${${var}}" STREQUAL "")
        message(FATAL_ERROR "${var} is required")
    endif()
endforeach()
if(NOT DEFINED MOQ_MODULE_DIR OR MOQ_MODULE_DIR STREQUAL "")
    set(MOQ_MODULE_DIR "${MOQ_SOURCE}/cmake")
endif()

set(fixture "${MOQ_SOURCE}/tests/cmake/picotls_prefix_selection")
set(pt "${MOQ_WORK}/pt")
file(REMOVE_RECURSE "${MOQ_WORK}")

function(_pt_tree root libdir)
    file(WRITE "${root}/include/picotls.h" "")
    file(WRITE "${root}/include/picotls/minicrypto.h" "")
    foreach(lib IN LISTS ARGN)
        file(WRITE "${libdir}/libpicotls-${lib}.a" "")
    endforeach()
endfunction()
_pt_tree("${pt}/A" "${pt}/A/build" core minicrypto openssl fusion)
_pt_tree("${pt}/B" "${pt}/B/build" core minicrypto openssl)
_pt_tree("${pt}/nest/inst" "${pt}/nest/inst/lib" core minicrypto openssl)
_pt_tree("${pt}/C" "${pt}/C/build" core minicrypto openssl)
file(WRITE "${pt}/A/include/picotls/openssl.h" "")
file(WRITE "${pt}/C/include/picotls/openssl.h" "")
file(WRITE "${pt}/nest/include/picotls/minicrypto.h" "")
file(MAKE_DIRECTORY "${pt}/empty")

# The consumer's top-level source sits beside a decoy picotls checkout, which
# is where the provider's fallback header search looks.
file(COPY "${fixture}/consumer/" DESTINATION "${MOQ_WORK}/src/proj")
file(WRITE "${MOQ_WORK}/src/picotls/include/picotls/minicrypto.h" "")
file(COPY "${fixture}/fake_picoquic/" DESTINATION "${MOQ_WORK}/fakepq")
file(WRITE "${MOQ_WORK}/pqpkg/picoquic-config.cmake"
    "if(NOT TARGET picoquic::picoquic-core)\n"
    "    add_library(picoquic::picoquic-core INTERFACE IMPORTED)\n"
    "endif()\n")
set(SRC "-DMOQ_PICOQUIC_SOURCE_DIR=${MOQ_WORK}/fakepq" -DMOQ_BUILD_PQ_THREADED=ON)
set(CFG "-Dpicoquic_DIR=${MOQ_WORK}/pqpkg")

set(failures "")
set(generator_args "")
if(DEFINED MOQ_GENERATOR AND NOT MOQ_GENERATOR STREQUAL "")
    set(generator_args -G "${MOQ_GENERATOR}")
endif()
set(context_args "")
if(DEFINED MOQ_CONTEXT AND NOT MOQ_CONTEXT STREQUAL "")
    set(context_args -C "${MOQ_CONTEXT}")
endif()

# Every configure and build keeps its output and status under logs/.
file(MAKE_DIRECTORY "${MOQ_WORK}/logs")
function(_pt_keep name kind rc out)
    file(WRITE "${MOQ_WORK}/logs/${name}.${kind}.log" "${out}")
    file(WRITE "${MOQ_WORK}/logs/${name}.${kind}.status" "${rc}\n")
endfunction()

# Sets <out_var> to the diagnostic lines in <text> that are warnings: CMake
# warning banners and compiler "warning:" lines.
function(_pt_warnings out_var text)
    string(REPLACE ";" "<semicolon>" text "${text}")
    string(REPLACE "\n" ";" lines "${text}")
    set(found "")
    foreach(line IN LISTS lines)
        if(line MATCHES "CMake (Deprecation )?Warning|(^|[^A-Za-z0-9_])[Ww]arning:")
            string(STRIP "${line}" line)
            list(APPEND found "${line}")
        endif()
    endforeach()
    set(${out_var} "${found}" PARENT_SCOPE)
endfunction()

# The one diagnostic check: <text> may contain no warnings except, with
# EXACT <regex>, exactly one line matching it. Sets <out_var> to what is
# wrong, or "".
function(_pt_diagnostics out_var text)
    cmake_parse_arguments(_d "" "EXACT" "" ${ARGN})
    _pt_warnings(lines "${text}")
    set(problems "")
    if(DEFINED _d_EXACT)
        set(matched 0)
        set(rest "")
        foreach(line IN LISTS lines)
            if(line MATCHES "${_d_EXACT}")
                math(EXPR matched "${matched} + 1")
            else()
                list(APPEND rest "${line}")
            endif()
        endforeach()
        if(NOT matched EQUAL 1)
            list(APPEND problems "expected exactly one warning matching '${_d_EXACT}', saw ${matched}")
        endif()
        set(lines "${rest}")
    endif()
    if(lines)
        list(JOIN lines " | " joined)
        list(APPEND problems "unexpected warnings: ${joined}")
    endif()
    set(${out_var} "${problems}" PARENT_SCOPE)
endfunction()

# A gate result for case <name>: a failure normally; with PT_CONTROL set the
# case is a control whose stray warning the gate must catch.
macro(_pt_gate name problems)
    if(PT_CONTROL)
        if("${problems}" STREQUAL "")
            list(APPEND failures "${name}: control's stray warning was not caught")
        endif()
    elseif(NOT "${problems}" STREQUAL "")
        list(APPEND failures "${name}: ${problems}")
    endif()
endmacro()

# _pt_configure(<case> <build-dir> OK|<expected error text> <args>...)
# Leaves the provider's record in pt_<case>_<KEY> for the checks below. A
# refusal is evidence only for its own error: other warnings still fail.
function(_pt_configure name build expect)
    set(result "${MOQ_WORK}/result-${name}.txt")
    file(REMOVE "${result}")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" ${generator_args} ${context_args}
            -S "${MOQ_WORK}/src/proj" -B "${MOQ_WORK}/${build}"
            "-DMOQ_TEST_MODULE_DIR=${MOQ_MODULE_DIR}"
            "-DMOQ_TEST_RESULT=${result}"
            "-DMOQ_TEST_FIXTURE=${fixture}"
            ${ARGN}
        RESULT_VARIABLE rc
        OUTPUT_VARIABLE out
        ERROR_VARIABLE out)
    _pt_keep(${name} configure "${rc}" "${out}")
    string(REGEX REPLACE "[ \t\r\n]+" " " flat "${out}")
    _pt_diagnostics(problems "${out}")
    _pt_gate(${name} "${problems}")
    if(expect STREQUAL "OK")
        if(NOT rc EQUAL 0)
            list(APPEND failures "${name}: configure failed (see logs/${name}.configure.log)")
            set(failures "${failures}" PARENT_SCOPE)
            return()
        endif()
        file(STRINGS "${result}" lines)
        foreach(line IN LISTS lines)
            if(line MATCHES "^([A-Z]+)=(.*)$")
                set(pt_${name}_${CMAKE_MATCH_1} "${CMAKE_MATCH_2}" PARENT_SCOPE)
            endif()
        endforeach()
    else()
        string(FIND "${flat}" "${expect}" at)
        if(rc EQUAL 0)
            list(APPEND failures "${name}: configure succeeded; expected a refusal containing '${expect}'")
        elseif(at EQUAL -1)
            list(APPEND failures "${name}: refusal does not contain '${expect}' (see logs/${name}.configure.log)")
        endif()
    endif()
    set(failures "${failures}" PARENT_SCOPE)
endfunction()

# Controls for the check itself.
_pt_diagnostics(problems "CMake Error at x:\n  refused\nCMake Warning:\n  stray\n")
if(problems STREQUAL "")
    list(APPEND failures "control: a warning beside an error was not caught")
endif()
_pt_diagnostics(problems "a.c:1:2: warning: moq-isolation-probe\na.c:2:2: warning: unrelated\n1 warning generated.\n"
    EXACT "moq-isolation-probe")
if(problems STREQUAL "")
    list(APPEND failures "control: an extra warning beside the intended one was not caught")
endif()
_pt_diagnostics(problems "a.c:1:2: warning: moq-isolation-probe [-W#warnings]\n1 warning generated.\n"
    EXACT "moq-isolation-probe")
if(NOT problems STREQUAL "")
    list(APPEND failures "control: the intended warning alone was rejected: ${problems}")
endif()

# _pt_expect(<case> <KEY> <expected path or NONE>)
function(_pt_expect name key expected)
    set(actual "${pt_${name}_${key}}")
    if(expected STREQUAL "NONE")
        if(NOT actual STREQUAL "" AND NOT actual MATCHES "-NOTFOUND$")
            set(failures ${failures} "${name}: ${key} is ${actual}, expected none" PARENT_SCOPE)
        endif()
        return()
    endif()
    get_filename_component(a "${actual}" REALPATH)
    get_filename_component(e "${expected}" REALPATH)
    if(actual STREQUAL "" OR NOT a STREQUAL e)
        set(failures ${failures} "${name}: ${key} is '${actual}', expected ${expected}" PARENT_SCOPE)
    endif()
endfunction()

macro(_pt_expect_tree name inc libdir)
    _pt_expect(${name} INC "${inc}")
    _pt_expect(${name} CORE "${libdir}/libpicotls-core.a")
    _pt_expect(${name} MINICRYPTO "${libdir}/libpicotls-minicrypto.a")
    _pt_expect(${name} OPENSSL "${libdir}/libpicotls-openssl.a")
    _pt_expect(${name} LIBDIR "${libdir}")
endmacro()

set(A "-DMOQ_PICOTLS_PREFIX=${pt}/A/build")
set(B "-DMOQ_PICOTLS_PREFIX=${pt}/B/build")

_pt_configure(cold b-reconf OK "${A}" ${SRC})
_pt_expect_tree(cold "${pt}/A/include" "${pt}/A/build")
_pt_expect(cold FUSION "${pt}/A/build/libpicotls-fusion.a")

_pt_configure(a_to_b b-reconf OK "${B}" ${SRC})
_pt_expect_tree(a_to_b "${pt}/B/include" "${pt}/B/build")
_pt_expect(a_to_b FUSION NONE)

file(WRITE "${MOQ_WORK}/stale-hdr.cmake"
    "set(PTLS_INCLUDE_DIR [==[${pt}/A/include]==] CACHE PATH \"Path to a file.\")\n")
_pt_configure(stale_hdr b-stale-hdr OK -C "${MOQ_WORK}/stale-hdr.cmake" "${B}" ${SRC})
_pt_expect_tree(stale_hdr "${pt}/B/include" "${pt}/B/build")

file(WRITE "${MOQ_WORK}/stale-lib.cmake"
    "set(PTLS_CORE_LIBRARY [==[${pt}/A/build/libpicotls-core.a]==] CACHE FILEPATH \"Path to a library.\")\n"
    "set(PTLS_FUSION_LIBRARY [==[${pt}/A/build/libpicotls-fusion.a]==] CACHE FILEPATH \"Path to a library.\")\n"
    "set(MOQ_PTLS_LIB_DIR [==[${pt}/A/build]==] CACHE PATH \"picotls library directory (libpicotls-*.a)\")\n")
_pt_configure(stale_lib b-stale-lib OK -C "${MOQ_WORK}/stale-lib.cmake" "${B}" ${SRC})
_pt_expect_tree(stale_lib "${pt}/B/include" "${pt}/B/build")
_pt_expect(stale_lib FUSION NONE)

_pt_configure(conflict_hdr b-conflict-hdr
    "PTLS_INCLUDE_DIR=${pt}/A/include conflicts with MOQ_PICOTLS_PREFIX"
    "-DPTLS_INCLUDE_DIR=${pt}/A/include" "${B}" ${SRC})
_pt_configure(conflict_lib b-conflict-lib
    "PTLS_CORE_LIBRARY=${pt}/A/build/libpicotls-core.a conflicts with MOQ_PICOTLS_PREFIX"
    "-DPTLS_CORE_LIBRARY=${pt}/A/build/libpicotls-core.a" "${B}" ${SRC})

_pt_configure(explicit b-explicit OK "-DPTLS_INCLUDE_DIR=${pt}/B/include" "${B}" ${SRC})
_pt_expect_tree(explicit "${pt}/B/include" "${pt}/B/build")
_pt_configure(explicit_kept b-explicit
    "PTLS_INCLUDE_DIR=${pt}/B/include conflicts with MOQ_PICOTLS_PREFIX" "${A}" ${SRC})
_pt_configure(explicit_dropped b-explicit OK -UPTLS_INCLUDE_DIR "${A}" ${SRC})
_pt_expect_tree(explicit_dropped "${pt}/A/include" "${pt}/A/build")

_pt_configure(installed b-installed OK "-DMOQ_PICOTLS_PREFIX=${pt}/nest/inst" ${SRC})
_pt_expect_tree(installed "${pt}/nest/inst/include" "${pt}/nest/inst/lib")

_pt_configure(invalid b-invalid "is not a picotls tree" "-DMOQ_PICOTLS_PREFIX=${pt}/empty" ${SRC})

# PICOQUIC_FETCH_PTLS: picoquic builds the stand-in picotls F, from
# FETCHCONTENT_SOURCE_DIR_PICOTLS or cloned from a local repository.
file(COPY "${fixture}/picotls/" DESTINATION "${pt}/F")
file(WRITE "${pt}/F/include/picotls.h" "")
file(WRITE "${pt}/F/include/picotls/minicrypto.h" "")
file(WRITE "${pt}/F/include/picotls/openssl.h" "")
file(COPY "${pt}/F/" DESTINATION "${pt}/F-repo")
foreach(step "init -q" "add -A"
        "-c user.name=fixture -c user.email=fixture@example.invalid commit -q -m fixture")
    separate_arguments(step_args UNIX_COMMAND "${step}")
    execute_process(COMMAND git -C "${pt}/F-repo" ${step_args} RESULT_VARIABLE rc)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "fixture picotls repository: git ${step} failed")
    endif()
endforeach()
execute_process(COMMAND git -C "${pt}/F-repo" rev-parse HEAD
    OUTPUT_VARIABLE F_TAG OUTPUT_STRIP_TRAILING_WHITESPACE)
set(FETCH -DPICOQUIC_FETCH_PTLS=ON "-DFETCHCONTENT_SOURCE_DIR_PICOTLS=${pt}/F")
set(CLONE -DPICOQUIC_FETCH_PTLS=ON "-DMOQ_TEST_PICOTLS_REPO=${pt}/F-repo"
    "-DMOQ_TEST_PICOTLS_TAG=${F_TAG}")
set(multi_config FALSE)
if(DEFINED MOQ_GENERATOR AND MOQ_GENERATOR MATCHES "Multi-Config|^Xcode$|^Visual Studio")
    set(multi_config TRUE)
endif()
# A Release build: a configure-time choice for single-config generators; a
# multi-config tree builds it with --config Release.
set(RELEASE -DCMAKE_BUILD_TYPE=Release)
if(multi_config)
    set(RELEASE "")
endif()

# _pt_expect_fetched(<case> <build-dir> [DIR <dir under the picotls build>]
#                    [UNRESOLVED] [INCLUDE <dir>]): every selection is F's;
# DIR is where its archives go, UNRESOLVED means no directory is known (so
# none is selected or exported). Multi-config generators are UNRESOLVED.
function(_pt_expect_fetched name build)
    cmake_parse_arguments(_e "UNRESOLVED" "DIR;INCLUDE" "" ${ARGN})
    set(fail "")
    if(NOT DEFINED _e_INCLUDE)
        set(_e_INCLUDE "${pt}/F/include")
    endif()
    get_filename_component(inc "${_e_INCLUDE}" REALPATH)
    get_filename_component(sel "${pt_${name}_SELINC}" REALPATH)
    if(NOT sel STREQUAL inc)
        list(APPEND fail "${name}: PTLS_INCLUDE_DIR is '${pt_${name}_SELINC}', expected ${inc}")
    endif()
    foreach(key CCORE CMINICRYPTO COPENSSL)
        if(NOT "${pt_${name}_${key}}" STREQUAL "")
            list(APPEND fail "${name}: cached ${key} is '${pt_${name}_${key}}', expected none")
        endif()
    endforeach()
    separate_arguments(tokens UNIX_COMMAND "${pt_${name}_PCLIBS}")
    set(search "")
    foreach(token IN LISTS tokens)
        if(token MATCHES "^-L(.*)$")
            get_filename_component(dir "${CMAKE_MATCH_1}" REALPATH)
            list(APPEND search "${dir}")
        endif()
    endforeach()
    if(_e_UNRESOLVED OR multi_config)
        if(NOT "${pt_${name}_LIBDIR}" STREQUAL "")
            list(APPEND fail "${name}: MOQ_PTLS_LIB_DIR is '${pt_${name}_LIBDIR}', expected none")
        endif()
        if("${pt_${name}_UNRESOLVED}" STREQUAL "")
            list(APPEND fail "${name}: no MOQ_PTLS_LIB_DIR_UNRESOLVED reason recorded")
        endif()
        if(search)
            list(APPEND fail "${name}: pkg-config closure has search directories (${search}), expected none")
        endif()
    else()
        set(want "${MOQ_WORK}/${build}/_deps/picotls-build")
        if(DEFINED _e_DIR)
            set(want "${want}/${_e_DIR}")
        endif()
        get_filename_component(want "${want}" REALPATH)
        get_filename_component(got "${pt_${name}_LIBDIR}" REALPATH)
        if("${pt_${name}_LIBDIR}" STREQUAL "" OR NOT got STREQUAL want)
            list(APPEND fail "${name}: MOQ_PTLS_LIB_DIR is '${pt_${name}_LIBDIR}', expected ${want}")
        endif()
        if(NOT "${pt_${name}_UNRESOLVED}" STREQUAL "")
            list(APPEND fail "${name}: unexpected unresolved library directory: ${pt_${name}_UNRESOLVED}")
        endif()
        if(NOT search STREQUAL want)
            list(APPEND fail "${name}: pkg-config search directories are '${search}', expected exactly ${want}")
        endif()
    endif()
    set(failures ${failures} ${fail} PARENT_SCOPE)
endfunction()

_pt_configure(fetch_source b-fetch-source OK "-DMOQ_PICOTLS_PREFIX=${pt}/F" ${FETCH} ${SRC})
_pt_expect_fetched(fetch_source b-fetch-source)
if(NOT pt_fetch_source_HOOKBEFORE STREQUAL pt_fetch_source_HOOKAFTER)
    list(APPEND failures "fetch_source: hook state '${pt_fetch_source_HOOKBEFORE}' became '${pt_fetch_source_HOOKAFTER}'")
endif()
_pt_configure(fetch_noprefix b-fetch-noprefix OK ${FETCH} ${SRC})
_pt_expect_fetched(fetch_noprefix b-fetch-noprefix)
_pt_configure(fetch_twice b-fetch-twice OK -DMOQ_TEST_FIND_TWICE=ON ${FETCH} ${SRC})
_pt_expect_fetched(fetch_twice b-fetch-twice)
foreach(layout custom config relative shared)
    set(dir_custom custom)
    set(dir_config cfg)
    set(dir_relative rel)
    set(dir_shared shlib)
    _pt_configure(fetch_${layout} b-fetch-${layout} OK ${RELEASE}
        -DMOQ_TEST_PTLS_LAYOUT=${layout} ${FETCH} ${SRC})
    _pt_expect_fetched(fetch_${layout} b-fetch-${layout} DIR ${dir_${layout}})
endforeach()
_pt_configure(fetch_split b-fetch-split OK -DMOQ_TEST_PTLS_LAYOUT=split ${FETCH} ${SRC})
_pt_expect_fetched(fetch_split b-fetch-split UNRESOLVED)
_pt_configure(fetch_split_explicit b-fetch-split-explicit
    "cannot be checked against the picotls picoquic builds"
    -DMOQ_TEST_PTLS_LAYOUT=split "-DMOQ_PTLS_LIB_DIR=${pt}/A/build" ${FETCH} ${SRC})
_pt_configure(fetch_other b-fetch-other
    "names a different picotls than the one picoquic builds"
    "-DMOQ_PICOTLS_PREFIX=${pt}/B/build" ${FETCH} ${SRC})
# Control: an unrelated warning cannot ride along with an expected refusal.
set(PT_CONTROL TRUE)
_pt_configure(control_refusal b-control-refusal
    "names a different picotls than the one picoquic builds"
    "-DCMAKE_PROJECT_INCLUDE=${fixture}/hooks/stray_warning.cmake"
    "-DMOQ_PICOTLS_PREFIX=${pt}/B/build" ${FETCH} ${SRC})
set(PT_CONTROL FALSE)
if(multi_config)
    set(libdir_refusal "MOQ_PTLS_LIB_DIR=${pt}/A/build cannot be checked against the picotls picoquic builds")
else()
    set(libdir_refusal "MOQ_PTLS_LIB_DIR=${pt}/A/build conflicts with the picotls picoquic builds")
endif()
_pt_configure(fetch_explicit_libdir b-fetch-explicit-libdir "${libdir_refusal}"
    "-DMOQ_PTLS_LIB_DIR=${pt}/A/build" ${FETCH} ${SRC})
_pt_configure(fetch_explicit_core b-fetch-explicit-core
    "PTLS_CORE_LIBRARY=${pt}/A/build/libpicotls-core.a is set, but picoquic links the picotls targets"
    "-DPTLS_CORE_LIBRARY=${pt}/A/build/libpicotls-core.a" ${FETCH} ${SRC})
# Cold, no OpenSSL: picoquic's fetched mbedTLS configuration needs
# PTLS_INCLUDE_DIR while it is being added, from a local source and from a
# clone whose source exists only once FetchContent has populated it.
_pt_configure(fetch_mbedtls b-fetch-mbedtls OK -DWITH_MBEDTLS=ON -DWITH_OPENSSL=OFF ${FETCH} ${SRC})
_pt_expect(fetch_mbedtls SELINC "${pt}/F/include")
_pt_expect(fetch_mbedtls INC "${pt}/F/include")
_pt_configure(fetch_clone_mbedtls b-fetch-clone-mbedtls OK -DWITH_MBEDTLS=ON -DWITH_OPENSSL=OFF
    ${CLONE} ${SRC})
_pt_expect_fetched(fetch_clone_mbedtls b-fetch-clone-mbedtls
    INCLUDE "${MOQ_WORK}/b-fetch-clone-mbedtls/_deps/picotls-src/include")
_pt_expect(fetch_clone_mbedtls INC "${MOQ_WORK}/b-fetch-clone-mbedtls/_deps/picotls-src/include")

# LibMoQ's picotls policy, observed in compiles of a Release build: picotls
# keeps assertions and treats warnings as errors; the consumer's and
# picoquic's directories keep the configuration's flags. Caller hooks (a file
# and a module name) run first, in picotls's scope; the cached hook list comes
# back as it was.
set(hooks "${fixture}/hooks")
file(WRITE "${MOQ_WORK}/caller-hooks.cmake"
    "set(CMAKE_PROJECT_picotls_INCLUDE [==[${hooks}/caller_hook_a.cmake;moq_test_hook_b]==] CACHE STRING \"caller hooks\")\n")
_pt_configure(fetch_policy b-fetch-policy OK ${RELEASE}
    "-DMOQ_TEST_HOOK_DIR=${hooks}" -C "${MOQ_WORK}/caller-hooks.cmake"
    ${FETCH} ${SRC})
if(NOT pt_fetch_policy_HOOKORDER STREQUAL "A[];B[]")
    list(APPEND failures "fetch_policy: caller hooks ran as '${pt_fetch_policy_HOOKORDER}', expected A then B before LibMoQ's policy")
endif()
if(NOT pt_fetch_policy_PTLSSCOPE STREQUAL "set-by-hook-a")
    list(APPEND failures "fetch_policy: a caller hook's variable did not reach picotls's scope ('${pt_fetch_policy_PTLSSCOPE}')")
endif()
if(NOT pt_fetch_policy_HOOKBEFORE STREQUAL pt_fetch_policy_HOOKAFTER)
    list(APPEND failures "fetch_policy: hook state '${pt_fetch_policy_HOOKBEFORE}' became '${pt_fetch_policy_HOOKAFTER}'")
endif()
# _pt_build(<case> <target> OK|<expected error text> [EXACT <warning regex>])
function(_pt_build name target expect)
    execute_process(
        COMMAND "${CMAKE_COMMAND}" --build "${MOQ_WORK}/b-fetch-policy" --config Release
            --target ${target}
        RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE out)
    _pt_keep(${name}-${target} build "${rc}" "${out}")
    _pt_diagnostics(problems "${out}" ${ARGN})
    _pt_gate(${name}-${target} "${problems}")
    if(expect STREQUAL "OK" AND NOT rc EQUAL 0)
        list(APPEND failures "${name}: building ${target} failed (see logs/${name}-${target}.build.log)")
    elseif(NOT expect STREQUAL "OK")
        string(FIND "${out}" "${expect}" at)
        if(rc EQUAL 0 OR at EQUAL -1)
            list(APPEND failures "${name}: building ${target} was expected to fail with '${expect}' (rc ${rc})")
        endif()
    endif()
    set(failures "${failures}" PARENT_SCOPE)
endfunction()
_pt_build(fetch_policy picotls-ndebug-probe OK)
_pt_build(fetch_policy picotls-werror-probe "moq-werror-probe")
_pt_build(fetch_policy consumer-isolation-probe OK EXACT "moq-isolation-probe")
_pt_build(fetch_policy picoquic-isolation-probe OK EXACT "moq-isolation-probe")
# Controls: an unrelated warning beside the intended one is caught.
set(PT_CONTROL TRUE)
_pt_build(control_probe consumer-isolation-extra-probe OK EXACT "moq-isolation-probe")
set(PT_CONTROL FALSE)
# The caller's bindings of both hook families after find_package, by
# behaviour: unset, empty, normal, cache-only (must still follow its cache
# entry) and normal over cache (must fall back to the cache entry).
file(WRITE "${MOQ_WORK}/binding-cache.cmake" "")
foreach(v CMAKE_PROJECT_picoquic_INCLUDE MOQ_PICOQUIC_CALLER_PROJECT_INCLUDE
        CMAKE_PROJECT_picotls_INCLUDE MOQ_PICOTLS_CALLER_PROJECT_INCLUDE)
    file(APPEND "${MOQ_WORK}/binding-cache.cmake"
        "set(${v} [==[${hooks}/noop_cache.cmake]==] CACHE STRING \"caller binding\")\n")
endforeach()
foreach(state unset empty set cache over)
    set(preload "")
    if(state MATCHES "^(cache|over)$")
        set(preload -C "${MOQ_WORK}/binding-cache.cmake")
    endif()
    _pt_configure(binding_${state} b-binding-${state} OK ${preload}
        "-DMOQ_TEST_HOOK_DIR=${hooks}" -DMOQ_TEST_BINDING=${state} ${FETCH} ${SRC})
    if(NOT pt_binding_${state}_BINDING STREQUAL "ok")
        list(APPEND failures "binding_${state}: ${pt_binding_${state}_BINDING}")
    endif()
endforeach()

# Beneath a directory holding the fetched FindPTLS's PTLS_*_LIBRARY values.
_pt_configure(nested_ok b-nested-ok OK -DMOQ_TEST_NESTED=ON ${FETCH} ${SRC})
_pt_expect_fetched(nested_ok b-nested-ok)
_pt_configure(nested_conflict b-nested-conflict "visible from an enclosing directory"
    -DMOQ_TEST_NESTED=ON "-DMOQ_TEST_NESTED_CORE=${pt}/A/build/libpicotls-core.a"
    ${FETCH} ${SRC})
_pt_configure(nested_hidden b-nested-hidden
    "PTLS_CORE_LIBRARY=${pt}/A/build/libpicotls-core.a is set, but picoquic links the picotls targets"
    -DMOQ_TEST_NESTED=ON "-DPTLS_CORE_LIBRARY=${pt}/A/build/libpicotls-core.a" ${FETCH} ${SRC})

# Prebuilt A -> fetched F -> prebuilt A, in one tree.
_pt_configure(swap_a b-swap OK "${A}" ${SRC})
_pt_expect_tree(swap_a "${pt}/A/include" "${pt}/A/build")
_pt_configure(swap_f b-swap OK "-DMOQ_PICOTLS_PREFIX=${pt}/F" ${FETCH} ${SRC})
_pt_expect_fetched(swap_f b-swap)
_pt_configure(swap_back b-swap OK "${A}" -DPICOQUIC_FETCH_PTLS=OFF
    -UFETCHCONTENT_SOURCE_DIR_PICOTLS ${SRC})
_pt_expect_tree(swap_back "${pt}/A/include" "${pt}/A/build")
_pt_expect(swap_back CCORE "${pt}/A/build/libpicotls-core.a")
separate_arguments(swap_tokens UNIX_COMMAND "${pt_swap_back_PCLIBS}")
set(swap_search "")
foreach(token IN LISTS swap_tokens)
    if(token MATCHES "^-L(.*)$")
        get_filename_component(dir "${CMAKE_MATCH_1}" REALPATH)
        list(APPEND swap_search "${dir}")
    endif()
endforeach()
get_filename_component(swap_want "${pt}/A/build" REALPATH)
if(NOT swap_search STREQUAL swap_want)
    list(APPEND failures "swap_back: pkg-config search directories are '${swap_search}', expected exactly ${swap_want}")
endif()

# A populated picotls that the selected picoquic does not link.
_pt_configure(unrelated_cfg b-unrelated-cfg OK -DMOQ_TEST_UNRELATED_FETCH=ON
    -DMOQ_TEST_FIND_TWICE=ON "${A}" "-DFETCHCONTENT_SOURCE_DIR_PICOTLS=${pt}/F" ${CFG})
_pt_expect(unrelated_cfg SELINC "${pt}/A/include")
_pt_expect(unrelated_cfg LIBDIR "${pt}/A/build")
_pt_configure(unrelated_pre b-unrelated-pre OK -DMOQ_TEST_UNRELATED_FETCH=ON
    -DMOQ_TEST_PRE_ADD=ON -DMOQ_TEST_FIND_TWICE=ON "${A}"
    "-DFETCHCONTENT_SOURCE_DIR_PICOTLS=${pt}/F"
    "-DMOQ_PICOQUIC_SOURCE_DIR=${MOQ_WORK}/fakepq")
_pt_expect(unrelated_pre SELINC "${pt}/A/include")
_pt_expect(unrelated_pre LIBDIR "${pt}/A/build")
# A pre-existing picoquic that does link the picotls it built.
_pt_configure(pre_fetch b-pre-fetch OK -DMOQ_TEST_PRE_ADD=ON -DMOQ_TEST_FIND_TWICE=ON
    ${FETCH} "-DMOQ_PICOQUIC_SOURCE_DIR=${MOQ_WORK}/fakepq")
_pt_expect(pre_fetch SELINC "${pt}/F/include")
_pt_configure(pre_alias b-pre-alias OK -DMOQ_TEST_PRE_ADD=ON -DMOQ_TEST_PQ_LINK_ALIAS=ON
    ${FETCH} "-DMOQ_PICOQUIC_SOURCE_DIR=${MOQ_WORK}/fakepq")
_pt_expect(pre_alias SELINC "${pt}/F/include")

# Installed picoquic CONFIG package: only the directories libmoq reads.
macro(_pt_expect_dirs name inc libdir)
    _pt_expect(${name} SELINC "${inc}")
    _pt_expect(${name} LIBDIR "${libdir}")
endmacro()

_pt_configure(cfg_cold b-cfg-reconf OK "${A}" ${CFG})
_pt_expect_dirs(cfg_cold "${pt}/A/include" "${pt}/A/build")

_pt_configure(cfg_a_to_b b-cfg-reconf OK "${B}" ${CFG})
_pt_expect_dirs(cfg_a_to_b "${pt}/B/include" "${pt}/B/build")

file(WRITE "${MOQ_WORK}/cfg-stale.cmake"
    "set(PTLS_INCLUDE_DIR [==[${pt}/A/include]==] CACHE PATH \"picotls include directory (picotls.h)\")\n"
    "set(MOQ_PTLS_LIB_DIR [==[${pt}/A/build]==] CACHE PATH \"picotls library directory (libpicotls-*.a)\")\n")
_pt_configure(cfg_stale b-cfg-stale OK -C "${MOQ_WORK}/cfg-stale.cmake" "${B}" ${CFG})
_pt_expect_dirs(cfg_stale "${pt}/B/include" "${pt}/B/build")

_pt_configure(cfg_conflict b-cfg-conflict
    "MOQ_PTLS_LIB_DIR=${pt}/A/build conflicts with MOQ_PICOTLS_PREFIX"
    "-DMOQ_PTLS_LIB_DIR=${pt}/A/build" "${B}" ${CFG})

_pt_configure(cfg_installed b-cfg-installed OK "-DMOQ_PICOTLS_PREFIX=${pt}/nest/inst" ${CFG})
_pt_expect_dirs(cfg_installed "${pt}/nest/inst/include" "${pt}/nest/inst/lib")

# Adapter: the verifier's picotls headers follow the selection.
# The adapter's build-tree targets file is generated once for every
# configuration, so libmoq with the picoquic adapter configures only under a
# single-config generator.
if(multi_config)
    message(STATUS "picotls prefix selection: adapter cases need a single-config generator; not run")
elseif(DEFINED MOQ_OPENSSL_ROOT AND NOT MOQ_OPENSSL_ROOT STREQUAL "")
    # _pt_adapter(<case> <build-dir> OK|<expected error text> <args>...)
    # Leaves the adapter's include directories under the picotls trees in
    # pt_<case>_ADAPTER_INC.
    function(_pt_adapter name build expect)
        set(dir "${MOQ_WORK}/${build}")
        file(REMOVE_RECURSE "${dir}")
        file(MAKE_DIRECTORY "${dir}/.cmake/api/v1/query")
        file(WRITE "${dir}/.cmake/api/v1/query/codemodel-v2" "")
        # The stand-in picoquic's inputs, for the source-tree cases only.
        set(provider_args "")
        if("${ARGN}" MATCHES "-DMOQ_PICOQUIC_SOURCE_DIR=")
            set(provider_args "-DMOQ_TEST_RESULT=${MOQ_WORK}/result-${name}.txt"
                "-DMOQ_TEST_FIXTURE=${fixture}")
        endif()
        execute_process(
            COMMAND "${CMAKE_COMMAND}" ${generator_args} ${context_args}
                -S "${MOQ_SOURCE}" -B "${dir}"
                -DMOQ_BUILD_TESTS=OFF -DMOQ_BUILD_SIM=OFF
                -DMOQ_BUILD_EXAMPLES=OFF
                -DMOQ_BUILD_ADAPTER_PICOQUIC=ON
                -DMOQ_PICOQUIC_VERIFY_BACKEND=openssl
                "-DOPENSSL_ROOT_DIR=${MOQ_OPENSSL_ROOT}"
                ${provider_args}
                ${ARGN}
            RESULT_VARIABLE rc
            OUTPUT_VARIABLE out
            ERROR_VARIABLE out)
        _pt_keep(${name} configure "${rc}" "${out}")
        _pt_diagnostics(problems "${out}")
        _pt_gate(${name} "${problems}")
        set(failures "${failures}" PARENT_SCOPE)
        string(REGEX REPLACE "[ \t\r\n]+" " " flat "${out}")
        if(NOT expect STREQUAL "OK")
            string(FIND "${flat}" "${expect}" at)
            if(rc EQUAL 0)
                set(failures ${failures} "${name}: configure succeeded; expected a refusal containing '${expect}'" PARENT_SCOPE)
            elseif(at EQUAL -1)
                set(failures ${failures} "${name}: refusal does not contain '${expect}': ${out}" PARENT_SCOPE)
            endif()
            return()
        endif()
        if(NOT rc EQUAL 0)
            set(failures ${failures} "${name}: configure failed: ${out}" PARENT_SCOPE)
            return()
        endif()
        set(reply "${dir}/.cmake/api/v1/reply")
        file(GLOB candidates "${reply}/target-moq-adapter-picoquic-*.json")
        set(target "")
        foreach(candidate IN LISTS candidates)
            file(READ "${candidate}" json)
            string(JSON target_name GET "${json}" name)
            if(target_name STREQUAL "moq-adapter-picoquic")
                set(target "${json}")
            endif()
        endforeach()
        if(target STREQUAL "")
            set(failures ${failures} "${name}: no moq-adapter-picoquic codemodel" PARENT_SCOPE)
            return()
        endif()
        set(found "")
        string(JSON groups LENGTH "${target}" compileGroups)
        math(EXPR last_group "${groups} - 1")
        foreach(g RANGE ${last_group})
            string(JSON n ERROR_VARIABLE err LENGTH "${target}" compileGroups ${g} includes)
            if(err OR n EQUAL 0)
                continue()
            endif()
            math(EXPR last "${n} - 1")
            foreach(i RANGE ${last})
                string(JSON path GET "${target}" compileGroups ${g} includes ${i} path)
                string(FIND "${path}" "${pt}/" in_trees)
                if(in_trees EQUAL 0)
                    list(APPEND found "${path}")
                endif()
            endforeach()
        endforeach()
        list(REMOVE_DUPLICATES found)
        set(pt_${name}_ADAPTER_INC "${found}" PARENT_SCOPE)
    endfunction()

    file(WRITE "${MOQ_WORK}/verifier-a.cmake"
        "set(MOQ_PTLS_OPENSSL_INCLUDE_DIR [==[${pt}/A/include]==] CACHE PATH \"Directory containing <picotls/openssl.h> for the cert verifier\")\n")

    _pt_adapter(adapter_incomplete b-adapter-incomplete
        "have no <picotls/openssl.h>"
        -C "${MOQ_WORK}/verifier-a.cmake" "${B}" ${SRC})

    _pt_adapter(adapter_complete b-adapter-complete OK
        -C "${MOQ_WORK}/verifier-a.cmake"
        "-DMOQ_PICOTLS_PREFIX=${pt}/C/build" ${SRC})
    if(NOT pt_adapter_complete_ADAPTER_INC STREQUAL "${pt}/C/include")
        list(APPEND failures
            "adapter_complete: adapter picotls includes are '${pt_adapter_complete_ADAPTER_INC}', expected ${pt}/C/include")
    endif()

    # A picotls on the default search path (CMAKE_PREFIX_PATH), as an
    # installed system picotls would be. With no prefix it is the selection.
    file(WRITE "${pt}/sys/include/picotls.h" "")
    file(WRITE "${pt}/sys/include/picotls/minicrypto.h" "")
    file(WRITE "${pt}/sys/include/picotls/openssl.h" "")
    _pt_adapter(adapter_system b-adapter-system OK
        "-DCMAKE_PREFIX_PATH=${pt}/sys"
        "-DMOQ_PTLS_OPENSSL_INCLUDE_DIR=${pt}/A/include" ${CFG})
    if(NOT pt_adapter_system_ADAPTER_INC STREQUAL "${pt}/sys/include")
        list(APPEND failures
            "adapter_system: adapter picotls includes are '${pt_adapter_system_ADAPTER_INC}', expected ${pt}/sys/include")
    endif()

    # No picotls selection on the same host. A defined, empty
    # PTLS_INCLUDE_DIR is a settled find result, so FindPicoquic's lookup
    # does not search and no header directory is selected; compiler and
    # build-tool discovery are untouched.
    _pt_adapter(adapter_unselected b-adapter-unselected OK
        "-DCMAKE_PREFIX_PATH=${pt}/sys"
        "-DPTLS_INCLUDE_DIR:PATH="
        "-DMOQ_PTLS_OPENSSL_INCLUDE_DIR=${pt}/A/include" ${CFG})
    if(NOT pt_adapter_unselected_ADAPTER_INC STREQUAL "${pt}/A/include")
        list(APPEND failures
            "adapter_unselected: adapter picotls includes are '${pt_adapter_unselected_ADAPTER_INC}', expected ${pt}/A/include")
    endif()
else()
    message(STATUS "picotls prefix selection: adapter cases need MOQ_OPENSSL_ROOT; not run")
endif()

if(failures)
    list(JOIN failures "\n  " text)
    message(FATAL_ERROR "picotls prefix selection:\n  ${text}")
endif()
message(STATUS "picotls prefix selection: all cases hold")
