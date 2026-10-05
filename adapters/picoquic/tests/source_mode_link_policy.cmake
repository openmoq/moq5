# Source-mode project-policy contract for FindPicoquic.
#
# When FindPicoquic builds picoquic from source it injects
# cmake/MoqPicoquicProjectPolicy.cmake into picoquic's project() call. This
# script configures small driver projects that consume FindPicoquic the way
# this repository does and checks, from structured build information rather
# than generator-specific files, that
#   * the policies reach picoquic's scope and the link outcome matches the
#     linker's capability: a de-duplicating linker gets link lines without
#     repeated archives (and a warning-free actual link), a traditional one
#     keeps whatever repetitions it needs and still links a complete closure;
#   * a caller's CMAKE_PROJECT_picoquic_INCLUDE keeps its documented
#     semantics -- a file, a module name, or a list of them, run in order
#     with their native policy and variable effects -- and its own state
#     (unset / empty / set, cache value) is exactly as the caller left it
#     once the integration returns; the hand-over variable is restored too;
#   * nothing leaks to a sibling project configured afterwards;
#   * every nested configure is warning-free (a zero exit with a CMake
#     warning fails this gate), with the raw logs kept.
#
# Link information comes from the CMake File API (codemodel-v2
# link.commandFragments, shell-tokenized into path tokens) and the
# executable to run from the same codemodel's artifact; the nested trees use
# the single-configuration generator this test was registered from
# (GENERATOR). Every nested build and run must be free of diagnostics on any
# linker family; only archive multiplicity is judged per linker capability.
#
# Args (-D): SRC (repo root), WORK (scratch dir), PQ_SRC (picoquic source),
# PTLS_PREFIX (picotls build); optional C_COMPILER, CXX_COMPILER, GENERATOR.

foreach(_v SRC WORK PQ_SRC PTLS_PREFIX)
    if(NOT DEFINED ${_v})
        message(FATAL_ERROR "pass -D${_v}=<path>")
    endif()
endforeach()
file(MAKE_DIRECTORY "${WORK}")

set(_common "-DMOQ_PICOQUIC_SOURCE_DIR=${PQ_SRC}" "-DMOQ_PICOTLS_PREFIX=${PTLS_PREFIX}")
if(DEFINED C_COMPILER AND NOT C_COMPILER STREQUAL "")
    list(APPEND _common "-DCMAKE_C_COMPILER=${C_COMPILER}")
endif()
if(DEFINED CXX_COMPILER AND NOT CXX_COMPILER STREQUAL "")
    list(APPEND _common "-DCMAKE_CXX_COMPILER=${CXX_COMPILER}")
endif()
set(_gen "")
if(DEFINED GENERATOR AND NOT GENERATOR STREQUAL "")
    if(GENERATOR MATCHES "^(Xcode|Visual Studio|Ninja Multi-Config)")
        message(FATAL_ERROR "this gate supports single-configuration generators only; got '${GENERATOR}'")
    endif()
    set(_gen -G "${GENERATOR}")
endif()

# -- diagnostics --------------------------------------------------------------
# A nested configure/build is accepted only when it exits 0 AND its output
# carries no diagnostic line: CMake's own "CMake Warning"/"CMake Deprecation
# Warning" blocks, compiler "warning:" diagnostics and linker "ld: warning"
# lines. Matching is per line, anchored on the diagnostic prefix, never on a
# word that may appear inside a path.
function(_scan_for_warnings out text)
    set(_hits "")
    string(REPLACE ";" "\\;" _text "${text}")
    string(REGEX REPLACE "\r?\n" ";" _lines "${_text}")
    foreach(_l IN LISTS _lines)
        if(_l MATCHES "^CMake (Deprecation )?Warning" OR
           _l MATCHES "^(.*/)?ld: warning" OR
           _l MATCHES ":[0-9]+:[0-9]+: warning:" OR
           _l MATCHES "^(.*/)?(clang|clang\\+\\+|gcc|g\\+\\+|cc|c\\+\\+|cc1|cc1plus|ld|lld|collect2)(-[0-9.]+)?: warning:")
            list(APPEND _hits "${_l}")
        endif()
    endforeach()
    set(${out} "${_hits}" PARENT_SCOPE)
endfunction()

# The acceptance rule for a nested build or run, independent of linker
# capability: exit 0 AND no diagnostic line. Returns "ok" or the reason.
function(_judge_step out rc hits)
    if(NOT rc EQUAL 0)
        set(${out} "exited ${rc}" PARENT_SCOPE)
    elseif(hits)
        set(${out} "exit 0 but diagnostics present: ${hits}" PARENT_SCOPE)
    else()
        set(${out} "ok" PARENT_SCOPE)
    endif()
endfunction()

# Run a nested cmake invocation, keep its raw log, require exit 0 and no
# diagnostic line (unless ALLOW_FAIL: then return the rc and the hits).
set(_run_n 0)
function(_cmake_step name)
    cmake_parse_arguments(_s "ALLOW_FAIL" "" "COMMAND" ${ARGN})
    execute_process(COMMAND ${_s_COMMAND} RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
    file(WRITE "${WORK}/log-${name}.txt" "${_out}")
    _scan_for_warnings(_hits "${_out}")
    set(${name}_RC "${_rc}" PARENT_SCOPE)
    set(${name}_HITS "${_hits}" PARENT_SCOPE)
    if(_s_ALLOW_FAIL)
        return()
    endif()
    _judge_step(_verdict "${_rc}" "${_hits}")
    if(NOT _verdict STREQUAL "ok")
        message(FATAL_ERROR "${name}: ${_verdict}; log ${WORK}/log-${name}.txt\n${_out}")
    endif()
endfunction()

# -- File API: library fragments of a target ----------------------------------
function(_fileapi_query tree)
    file(MAKE_DIRECTORY "${tree}/.cmake/api/v1/query")
    file(WRITE "${tree}/.cmake/api/v1/query/codemodel-v2" "")
endfunction()
# Library fragments are shell fragments (quoted/escaped as the generator
# wrote them); decode them with shell tokenization into complete path tokens.
function(_decode_library_tokens out fragments)
    set(_toks "")
    foreach(_f IN LISTS fragments)
        separate_arguments(_parts UNIX_COMMAND "${_f}")
        list(APPEND _toks ${_parts})
    endforeach()
    set(${out} "${_toks}" PARENT_SCOPE)
endfunction()

# This gate works on single-configuration generators (the trees it is
# registered from use one); it refuses a multi-configuration codemodel instead
# of silently picking a configuration, and takes the target's artifact path
# from the same codemodel it reads the link fragments from.
function(_fileapi_target out_libs out_artifact tree target)
    file(GLOB _idx "${tree}/.cmake/api/v1/reply/index-*.json")
    list(SORT _idx)
    list(GET _idx -1 _idx)
    if(NOT _idx)
        message(FATAL_ERROR "${target}: no File API reply in ${tree}")
    endif()
    file(READ "${_idx}" _j)
    string(JSON _n LENGTH "${_j}" objects)
    math(EXPR _n "${_n} - 1")
    set(_cm "")
    foreach(_i RANGE ${_n})
        string(JSON _kind GET "${_j}" objects ${_i} kind)
        if(_kind STREQUAL "codemodel")
            string(JSON _cm GET "${_j}" objects ${_i} jsonFile)
        endif()
    endforeach()
    file(READ "${tree}/.cmake/api/v1/reply/${_cm}" _c)
    string(JSON _nc LENGTH "${_c}" configurations)
    if(NOT _nc EQUAL 1)
        message(FATAL_ERROR "${target}: codemodel has ${_nc} configurations; this gate supports single-configuration generators only")
    endif()
    string(JSON _nt LENGTH "${_c}" configurations 0 targets)
    math(EXPR _nt "${_nt} - 1")
    set(_tfile "")
    foreach(_ti RANGE ${_nt})
        string(JSON _name GET "${_c}" configurations 0 targets ${_ti} name)
        if(_name STREQUAL "${target}")
            string(JSON _tfile GET "${_c}" configurations 0 targets ${_ti} jsonFile)
        endif()
    endforeach()
    if(NOT _tfile)
        message(FATAL_ERROR "${target}: not in the codemodel of ${tree}")
    endif()
    file(READ "${tree}/.cmake/api/v1/reply/${_tfile}" _t)
    set(_frags "")
    string(JSON _nf ERROR_VARIABLE _e LENGTH "${_t}" link commandFragments)
    if(_e)
        message(FATAL_ERROR "${target}: no link fragments: ${_e}")
    endif()
    math(EXPR _nf "${_nf} - 1")
    foreach(_fi RANGE ${_nf})
        string(JSON _role GET "${_t}" link commandFragments ${_fi} role)
        if(_role STREQUAL "libraries")
            string(JSON _frag GET "${_t}" link commandFragments ${_fi} fragment)
            list(APPEND _frags "${_frag}")
        endif()
    endforeach()
    string(JSON _art GET "${_t}" artifacts 0 path)
    set(${out_libs} "${_frags}" PARENT_SCOPE)
    set(${out_artifact} "${_art}" PARENT_SCOPE)
endfunction()

# The archive closure picoquic's HTTP test executable needs.
set(_closure "libpicoquic-core" "libpicoquic-log" "libpicohttp-core" "libpicotls-core")
function(_require_closure target libs)
    _decode_library_tokens(_toks "${libs}")
    foreach(_need IN LISTS _closure)
        set(_found FALSE)
        foreach(_l IN LISTS _toks)
            get_filename_component(_base "${_l}" NAME_WE)
            if(_base STREQUAL "${_need}")
                set(_found TRUE)
            endif()
        endforeach()
        if(NOT _found)
            message(FATAL_ERROR "${target}: archive ${_need} missing from its link closure: ${libs}")
        endif()
    endforeach()
endfunction()
function(_count_repeated_archives out libs)
    _decode_library_tokens(_toks "${libs}")
    set(_seen "")
    set(_rep 0)
    foreach(_l IN LISTS _toks)
        if(_l MATCHES "\\.a$")
            if("${_l}" IN_LIST _seen)
                math(EXPR _rep "${_rep} + 1")
            endif()
            list(APPEND _seen "${_l}")
        endif()
    endforeach()
    set(${out} "${_rep}" PARENT_SCOPE)
endfunction()

# -- durable controls for the gate's own judgement helpers -------------------
# Synthetic inputs, run before any nested configure. They pin what the
# scanner and the archive accounting must do on both linker families; they
# are not an execution of a non-Darwin toolchain.
function(_control name got want)
    if(NOT "${got}" STREQUAL "${want}")
        message(FATAL_ERROR "control '${name}': got [${got}] want [${want}]")
    endif()
endfunction()
_scan_for_warnings(_c "/usr/bin/ld: warning: example\n")
_control("absolute-path ld warning is a diagnostic" "${_c}" "/usr/bin/ld: warning: example")
_scan_for_warnings(_c "ld: warning: ignoring duplicate libraries: 'x.a'\n")
_control("bare ld warning is a diagnostic" "${_c}" "ld: warning: ignoring duplicate libraries: 'x.a'")
_scan_for_warnings(_c "cc1: warning: example\n")
_control("compiler driver warning is a diagnostic" "${_c}" "cc1: warning: example")
_scan_for_warnings(_c "/tmp/a b/x.c:3:4: warning: unused variable\n")
_control("source diagnostic in a spaced path is a diagnostic" "${_c}" "/tmp/a b/x.c:3:4: warning: unused variable")
_scan_for_warnings(_c "CMake Warning:\n  Ignoring extra path\n")
_control("cmake warning block is a diagnostic" "${_c}" "CMake Warning:")
_scan_for_warnings(_c "-- Found thing: /work/warning-handling/libwarning.a\n-- Using /opt/ld warning tools/ld\n-- Configuring done\n")
_control("paths merely containing 'warning' or 'ld' are not diagnostics" "${_c}" "")
_judge_step(_v 0 "")
_control("clean zero-exit output is accepted" "${_v}" "ok")
_judge_step(_v 0 "/usr/bin/ld: warning: example")
_control("zero exit with a diagnostic is rejected regardless of linker family" "${_v}" "exit 0 but diagnostics present: /usr/bin/ld: warning: example")
_judge_step(_v 2 "")
_control("nonzero exit is rejected" "${_v}" "exited 2")
_count_repeated_archives(_r "\"/prefix with spaces/libpicoquic-core.a\";\"/prefix with spaces/libpicoquic-core.a\"")
_control("quoted duplicate archive in a spaced prefix counts once as repeated" "${_r}" "1")
_count_repeated_archives(_r "\"/prefix with spaces/libpicoquic-core.a\";\"/prefix with spaces/libpicoquic-log.a\";-framework;Foo")
_control("distinct quoted archives are not repeats" "${_r}" "0")
_require_closure(control "\"/prefix with spaces/libpicoquic-core.a\";\"/prefix with spaces/libpicoquic-log.a\";\"/prefix with spaces/libpicohttp-core.a\";\"/prefix with spaces/libpicotls-core.a\"")

# -- driver projects ----------------------------------------------------------
# A driver consumes FindPicoquic exactly as this repository does (module path,
# HTTP, loopback harness so picohttp_ct/picoquic_ct exist). PRE is CMake code
# run before find_package, POST after it; the sibling reports what it sees.
set(_hooks "${WORK}/hooks dir with spaces")
file(REMOVE_RECURSE "${_hooks}")
file(MAKE_DIRECTORY "${_hooks}")
# hook A: a file in a spaced path; sets a variable and a policy
file(WRITE "${_hooks}/hook a.cmake"
    "set(CALLER_HOOK_A_RAN 1)\n"
    "if(POLICY CMP0135)\n"
    "    cmake_policy(SET CMP0135 NEW)\n"
    "endif()\n")
# hook B: a MODULE name resolved through CMAKE_MODULE_PATH; reports, from
# inside picoquic's scope, the order and the native effects of hook A and
# the policy installed by the integration
file(WRITE "${_hooks}/MoqProbeHookB.cmake"
    "set(_p135 \"unknown\")\n"
    "if(POLICY CMP0135)\n"
    "    cmake_policy(GET CMP0135 _p135)\n"
    "endif()\n"
    "set(_p156 \"unknown\")\n"
    "if(POLICY CMP0156)\n"
    "    cmake_policy(GET CMP0156 _p156)\n"
    "endif()\n"
    "file(WRITE \"\${CMAKE_BINARY_DIR}/hookB.txt\" \"project=\${PROJECT_NAME} A_RAN=[\${CALLER_HOOK_A_RAN}] CMP0135=[\${_p135}] CMP0156=[\${_p156}]\")\n")
file(WRITE "${_hooks}/hook_status.cmake" "message(STATUS \"caller hook status message\")\n")
file(WRITE "${_hooks}/hook_warning.cmake" "message(WARNING \"caller hook warning\")\n")

function(_write_driver dir pre post)
    file(REMOVE_RECURSE "${dir}")
    file(MAKE_DIRECTORY "${dir}/sibling")
    file(WRITE "${dir}/CMakeLists.txt"
        "cmake_minimum_required(VERSION 3.20)\n"
        "project(driver C CXX)\n"
        "list(APPEND CMAKE_MODULE_PATH \"${SRC}/cmake\" \"${_hooks}\")\n"
        "set(MOQ_BUILD_ADAPTER_PICO_WT ON)\n"
        "set(MOQ_PICO_WT_BUILD_LOOPBACK ON)\n"
        "${pre}\n"
        "find_package(Picoquic REQUIRED)\n"
        "if(DEFINED CMAKE_PROJECT_picoquic_INCLUDE)\n"
        "    set(_d \"defined\")\n"
        "else()\n"
        "    set(_d \"undefined\")\n"
        "endif()\n"
        "if(DEFINED MOQ_PICOQUIC_CALLER_PROJECT_INCLUDE)\n"
        "    set(_h \"defined=[\${MOQ_PICOQUIC_CALLER_PROJECT_INCLUDE}]\")\n"
        "else()\n"
        "    set(_h \"undefined\")\n"
        "endif()\n"
        "set(_p156 \"unknown\")\n"
        "if(POLICY CMP0156)\n"
        "    cmake_policy(GET CMP0156 _p156)\n"
        "endif()\n"
        "file(WRITE \"\${CMAKE_BINARY_DIR}/after.txt\" \"include=\${_d}:[\${CMAKE_PROJECT_picoquic_INCLUDE}] handover=\${_h} CMP0156=[\${_p156}] processing=[\${CMAKE_C_LINK_LIBRARIES_PROCESSING}] version=\${CMAKE_VERSION}\")\n"
        "${post}\n"
        "add_subdirectory(sibling)\n")
    file(WRITE "${dir}/sibling/CMakeLists.txt"
        "cmake_minimum_required(VERSION 3.13)\n"
        "project(sibling C)\n"
        "if(DEFINED CMAKE_PROJECT_picoquic_INCLUDE)\n"
        "    set(_d \"defined\")\n"
        "else()\n"
        "    set(_d \"undefined\")\n"
        "endif()\n"
        "set(_p \"unknown\")\n"
        "if(POLICY CMP0156)\n"
        "    cmake_policy(GET CMP0156 _p)\n"
        "endif()\n"
        "file(WRITE \"\${CMAKE_BINARY_DIR}/sibling.txt\" \"include=\${_d} CMP0156=[\${_p}]\")\n")
endfunction()

function(_configure_driver name dir)
    cmake_parse_arguments(_c "ALLOW_FAIL" "" "" ${ARGN})
    set(_allow "")
    if(_c_ALLOW_FAIL)
        set(_allow ALLOW_FAIL)
    endif()
    set(_tree "${WORK}/${name}")
    file(REMOVE_RECURSE "${_tree}")
    _fileapi_query("${_tree}")
    _cmake_step(${name} ${_allow} COMMAND ${CMAKE_COMMAND} -S "${dir}" -B "${_tree}" ${_gen} ${_common} ${_c_UNPARSED_ARGUMENTS})
    set(${name}_RC "${${name}_RC}" PARENT_SCOPE)
    set(${name}_HITS "${${name}_HITS}" PARENT_SCOPE)
endfunction()
function(_read out tree file)
    file(READ "${tree}/${file}" _c)
    set(${out} "${_c}" PARENT_SCOPE)
endfunction()

# -- R0: default (no caller hook) ---------------------------------------------
_write_driver("${WORK}/drv-default" "" "")
_configure_driver(R0_default "${WORK}/drv-default")
_read(_after "${WORK}/R0_default" after.txt)
if(NOT _after MATCHES "^include=undefined:\\[\\] handover=undefined ")
    message(FATAL_ERROR "R0: state after the integration is not the caller's (unset) state: ${_after}")
endif()
_read(_sib "${WORK}/R0_default" sibling.txt)
if(NOT _sib MATCHES "^include=undefined CMP0156=\\[(unknown|OLD|)\\]$")
    message(FATAL_ERROR "R0: sibling project saw injected state: ${_sib}")
endif()
string(REGEX MATCH "processing=\\[([^]]*)\\]" _m "${_after}")
set(_processing "${CMAKE_MATCH_1}")
string(REGEX MATCH "CMP0156=\\[([^]]*)\\]" _m "${_after}")
set(_driver_p156 "${CMAKE_MATCH_1}")
if(_driver_p156 STREQUAL "unknown")
    set(_policy_known FALSE)
else()
    set(_policy_known TRUE)
endif()
if(_policy_known AND _processing MATCHES "DEDUPLICATION=ALL")
    set(_dedup TRUE)
else()
    set(_dedup FALSE)
endif()
message(STATUS "link policy capability: policies known=${_policy_known}, processing=[${_processing}], de-duplicating linker=${_dedup}")

# Link outcome from the File API, per linker capability.
foreach(_tgt picohttp_ct picoquic_ct)
    _fileapi_target(_libs _art "${WORK}/R0_default" ${_tgt})
    _require_closure(${_tgt} "${_libs}")
    _count_repeated_archives(_rep "${_libs}")
    if(_dedup AND NOT _rep EQUAL 0)
        message(FATAL_ERROR "${_tgt}: ${_rep} repeated archive(s) although the linker de-duplicates: ${_libs}")
    endif()
    message(STATUS "${_tgt}: ${_rep} repeated archive(s) in its library fragments")
endforeach()

# -- R1: caller list (spaced file + module name) through the cache -----------
_write_driver("${WORK}/drv-list" "" "")
set(_list_value "${_hooks}/hook a.cmake;MoqProbeHookB")
# A list value must reach the cache as one entry; an initial-cache script
# carries the semicolon intact where a -D argument would be split.
file(WRITE "${WORK}/drv-list/initial-cache.cmake"
    "set(CMAKE_PROJECT_picoquic_INCLUDE \"${_list_value}\" CACHE STRING \"caller hook list\")\n")
_configure_driver(R1_list "${WORK}/drv-list" -C "${WORK}/drv-list/initial-cache.cmake")
_read(_b "${WORK}/R1_list" hookB.txt)
if(NOT _b MATCHES "^project=picoquic A_RAN=\\[1\\] CMP0135=\\[(NEW|unknown)\\] CMP0156=\\[")
    message(FATAL_ERROR "R1: caller hooks did not run in order with native effects inside picoquic's scope: ${_b}")
endif()
if(_policy_known AND NOT _b MATCHES "CMP0156=\\[NEW\\]")
    message(FATAL_ERROR "R1: CMP0156 is not NEW inside picoquic's scope: ${_b}")
endif()
_read(_after "${WORK}/R1_list" after.txt)
if(NOT _after MATCHES "^include=defined:\\[${_list_value}\\] handover=undefined ")
    message(FATAL_ERROR "R1: caller's cached CMAKE_PROJECT_picoquic_INCLUDE not seen intact after the integration: ${_after}")
endif()
file(READ "${WORK}/R1_list/CMakeCache.txt" _cache_text)
string(FIND "${_cache_text}" "\nCMAKE_PROJECT_picoquic_INCLUDE:STRING=${_list_value}\n" _cache_pos)
if(_cache_pos EQUAL -1)
    message(FATAL_ERROR "R1: the caller's cache entry is not exactly the supplied list any more")
endif()

# -- R2: caller set the normal variable to EMPTY ------------------------------
_write_driver("${WORK}/drv-empty" "set(CMAKE_PROJECT_picoquic_INCLUDE \"\")" "")
_configure_driver(R2_empty "${WORK}/drv-empty")
_read(_after "${WORK}/R2_empty" after.txt)
if(NOT _after MATCHES "^include=defined:\\[\\] handover=undefined ")
    message(FATAL_ERROR "R2: an explicitly empty caller variable was not preserved as defined-empty: ${_after}")
endif()

# -- R3: caller set a nonempty normal variable + pre-existing hand-over state --
_write_driver("${WORK}/drv-set"
    "set(CMAKE_PROJECT_picoquic_INCLUDE \"${_hooks}/hook_status.cmake\")\nset(MOQ_PICOQUIC_CALLER_PROJECT_INCLUDE \"keep me\")"
    "")
_configure_driver(R3_set "${WORK}/drv-set")
_read(_after "${WORK}/R3_set" after.txt)
if(NOT _after MATCHES "^include=defined:\\[${_hooks}/hook_status\\.cmake\\] handover=defined=\\[keep me\\] ")
    message(FATAL_ERROR "R3: caller's set variable or unrelated hand-over state not preserved: ${_after}")
endif()
file(READ "${WORK}/log-R3_set.txt" _log3)
if(NOT _log3 MATCHES "caller hook status message")
    message(FATAL_ERROR "R3: the caller's status hook did not run")
endif()

# -- R4: a caller hook that only WARNS (exit 0) must be caught by this gate's
#       scanner; the clean status hook of R3 is the control. ------------------
_write_driver("${WORK}/drv-warn" "" "")
_configure_driver(R4_warning "${WORK}/drv-warn" ALLOW_FAIL
    "-DCMAKE_PROJECT_picoquic_INCLUDE=${_hooks}/hook_warning.cmake")
if(NOT R4_warning_RC EQUAL 0)
    message(FATAL_ERROR "R4: the warning-only hook made the configure fail (${R4_warning_RC}); the negative needs exit 0")
endif()
if(NOT R4_warning_HITS)
    message(FATAL_ERROR "R4: a CMake warning with exit 0 went undetected by the scanner")
endif()

# -- R5: actual link and execution of picoquic's HTTP test executable --------
# The build and the run must both be clean (exit 0, no diagnostic line) on
# every linker family; only the archive multiplicity above depends on the
# linker's capability. The executable is the artifact the codemodel names.
_cmake_step(R5_build COMMAND ${CMAKE_COMMAND} --build "${WORK}/R0_default" --target picohttp_ct ALLOW_FAIL)
_judge_step(_v "${R5_build_RC}" "${R5_build_HITS}")
if(NOT _v STREQUAL "ok")
    message(FATAL_ERROR "R5 build of picohttp_ct: ${_v}; log ${WORK}/log-R5_build.txt")
endif()
_fileapi_target(_libs _art "${WORK}/R0_default" picohttp_ct)
get_filename_component(_art_dir "${WORK}/R0_default/${_art}" DIRECTORY)
execute_process(
    COMMAND "${WORK}/R0_default/${_art}" -S "${PQ_SRC}" -n -r
    WORKING_DIRECTORY "${_art_dir}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
file(WRITE "${WORK}/log-R5_run.txt" "${_out}")
_scan_for_warnings(_hits "${_out}")
_judge_step(_v "${_rc}" "${_hits}")
if(NOT _v STREQUAL "ok")
    message(FATAL_ERROR "R5 run of picohttp_ct: ${_v}; log ${WORK}/log-R5_run.txt")
endif()

message(STATUS "picoquic_source_mode_link_policy: OK (de-duplicating linker=${_dedup})")
