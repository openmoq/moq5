# MOQ_PICOTLS_PREFIX is the authority for source-tree picoquic's PREBUILT
# picotls (PICOQUIC_FETCH_PTLS=OFF). The fetched-source mode has its own gate,
# test_ptls_fetch_local_source.cmake.
#
# Upstream's FindPTLS searches sibling paths relative to the top-level project
# and the build tree and caches the first picotls it finds there. So a project
# with a stray picotls beside it would compile picoquic against THAT tree,
# whatever MOQ_PICOTLS_PREFIX says. This drives a scratch source-tree consumer
# whose top-level project has exactly such a DECOY sibling, and judges what the
# generated build actually uses:
#
#   * headers: the compile command for picoquic's tls_api.c is split into its
#     arguments and its include directories walked in the compiler's search
#     order for a quoted include (the source's own directory, then -iquote,
#     -I, -isystem, -idirafter). The FIRST directory that supplies each picotls
#     header tls_api.c includes -- picotls.h, and picotls/openssl.h unless the
#     compile defines PTLS_WITHOUT_OPENSSL -- and the one upstream's search
#     names (picotls/minicrypto.h) must be the prefix's;
#   * archives: every picotls archive on the generated link line of an
#     executable that links picoquic-core must be one of the prefix's, and the
#     required ones must be there.
#
# Arms:
#   clean          the decoy is present and nothing is cached: the prefix wins;
#   fresh-foreign  a caller passes a typed foreign PTLS_INCLUDE_DIR: the
#                  configure fails by name, never quietly replacing it;
#   stale-cache    a reconfigure over that existing foreign entry fails by name,
#                  and succeeds on the prefix once the entry is removed;
#   coherent       a caller value that IS the prefix's headers is accepted;
#   prefix-change  LibMoQ's own recorded selection follows a changed prefix --
#                  here into a tree whose path contains a space;
#   mixed-fusion   an extra archive from another tree fails by name -- judged on
#                  a scratch prefix that has no fusion archive of its own, so
#                  the arm always runs whatever the declared prefix carries;
#   split-layout   a prefix that is a picotls SOURCE checkout, with the
#                  archives passed from elsewhere, is refused by name.
# Every configure's output, including the refused ones, is also scanned for
# warnings: a correct refusal does not excuse an unrelated warning.
#
# Three controls prove the oracle itself is live: a valid foreign include placed
# AHEAD of the prefix's must be caught by the ordered header walk (a plain
# "is the selected -I somewhere in the command" test would pass it); a foreign
# directory holding ONLY picotls/openssl.h ahead of the prefix must be caught
# (an oracle that asked only about picotls.h would pass it); and the
# captured output of a refused configure that also warns, judged in a child by
# the same _expect_refusal() the refusal arms use, must fail on the warning.
#
# Driven with -DCMAKE_EXE=<cmake> -DWORK=<dir> -DMODULES=<repo>/cmake
# -DPICOQUIC_SRC=<picoquic source> -DPICOTLS_PREFIX=<picotls build prefix>.
cmake_minimum_required(VERSION 3.20)
include("${CMAKE_CURRENT_LIST_DIR}/moq_subprocess_warnings.cmake")

function(_fail tag)
    string(CONCAT _msg ${ARGN})
    message(SEND_ERROR "FAIL[${tag}]: ${_msg}")
endfunction()

# CMake wraps long diagnostics across lines, so the text is compared with its
# whitespace collapsed. The refusal must name the variable, the rejected value
# and the prefix it conflicts with.
function(_refusal_named text var value out)
    string(REGEX REPLACE "[ \t\r\n]+" " " _flat "${text}")
    string(FIND "${_flat}" "${var}=${value} is not the picotls of MOQ_PICOTLS_PREFIX=" _a)
    string(FIND "${_flat}" "${var}=${value} was not selected from MOQ_PICOTLS_PREFIX=" _b)
    if(_a EQUAL -1 AND _b EQUAL -1)
        set(${out} FALSE PARENT_SCOPE)
    else()
        set(${out} TRUE PARENT_SCOPE)
    endif()
endfunction()

# An intended refusal: nonzero, named -- and, like every configure here, free
# of any warning.
function(_expect_refusal tag rc text var value)
    _moq_reject_warnings("${tag}" "${text}")
    _refusal_named("${text}" "${var}" "${value}" _named)
    if(rc EQUAL 0)
        _fail("${tag}" "the configure succeeded; it must refuse ${var}")
    elseif(NOT _named)
        _fail("${tag}" "the configure failed without naming ${var}=${value}:\n${text}")
    endif()
endfunction()

# Self-test entry: judge ONE captured configure through the same
# _expect_refusal() the refusal arms use (-DSELFTEST_TEXT=<file>
# -DSELFTEST_RC=<rc> -DSELFTEST_VAR=<var> -DSELFTEST_VALUE=<value>).
if(DEFINED SELFTEST_TEXT)
    file(READ "${SELFTEST_TEXT}" _t)
    _expect_refusal(selftest "${SELFTEST_RC}" "${_t}" "${SELFTEST_VAR}" "${SELFTEST_VALUE}")
    return()
endif()

foreach(_v CMAKE_EXE WORK MODULES PICOQUIC_SRC PICOTLS_PREFIX)
    if(NOT ${_v})
        message(FATAL_ERROR "test_ptls_prefix_authority: ${_v} is required")
    endif()
endforeach()

file(REAL_PATH "${PICOTLS_PREFIX}" _real_prefix)
file(REAL_PATH "${PICOTLS_PREFIX}/../include" _real_include)

# -- the scratch consumer and its decoy sibling ------------------------------
file(REMOVE_RECURSE "${WORK}")
set(_proj "${WORK}/proj")
set(_decoy "${WORK}/picotls")
file(MAKE_DIRECTORY "${_proj}" "${_decoy}/include/picotls" "${_decoy}/lib")
# The decoy has the real header and archive NAMES upstream searches for, but
# none of its content is usable.
file(WRITE "${_decoy}/include/picotls.h" "#error the decoy picotls was selected\n")
file(WRITE "${_decoy}/include/picotls/minicrypto.h" "#error the decoy picotls was selected\n")
foreach(_n core minicrypto openssl fusion)
    file(WRITE "${_decoy}/lib/libpicotls-${_n}.a" "decoy\n")
endforeach()
file(WRITE "${_proj}/CMakeLists.txt"
"cmake_minimum_required(VERSION 3.20)\n"
"project(moq_ptls_prefix_authority LANGUAGES C)\n"
"# Controls only: a foreign include ahead of everything, and a warning.\n"
"if(MOQ_TEST_FOREIGN_INCLUDE_FIRST)\n"
"    include_directories(BEFORE \"\${MOQ_TEST_FOREIGN_INCLUDE_FIRST}\")\n"
"endif()\n"
"if(MOQ_TEST_INJECT_WARNING)\n"
"    message(WARNING \"injected control warning\")\n"
"endif()\n"
"list(APPEND CMAKE_MODULE_PATH \"${MODULES}\")\n"
"find_package(Picoquic REQUIRED)\n"
"file(WRITE \"\${CMAKE_BINARY_DIR}/ptls_link_probe.c\" \"int main(void) { return 0; }\\n\")\n"
"add_executable(ptls_link_probe \"\${CMAKE_BINARY_DIR}/ptls_link_probe.c\")\n"
"target_link_libraries(ptls_link_probe PRIVATE picoquic-core)\n")

# -- one configure ------------------------------------------------------------
function(_configure dir out_rc out_text)
    execute_process(
        COMMAND "${CMAKE_EXE}" -S "${_proj}" -B "${dir}" -G "Unix Makefiles"
                -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
                "-DMOQ_PICOQUIC_SOURCE_DIR=${PICOQUIC_SRC}"
                ${ARGN}
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _o ERROR_VARIABLE _e)
    set(${out_rc} "${_rc}" PARENT_SCOPE)
    set(${out_text} "${_o}${_e}" PARENT_SCOPE)
endfunction()

# The directory that supplies <header> first for a quoted include in <source>
# compiled by <command> run in <cwd>; empty if none does.
function(_first_supplier command cwd source header out)
    separate_arguments(_args UNIX_COMMAND "${command}")
    set(_iquote "")
    set(_i "")
    set(_isystem "")
    set(_idirafter "")
    set(_next "")
    foreach(_a IN LISTS _args)
        if(_next)
            list(APPEND ${_next} "${_a}")
            set(_next "")
        elseif(_a MATCHES "^-iquote(.*)$")
            if(CMAKE_MATCH_1)
                list(APPEND _iquote "${CMAKE_MATCH_1}")
            else()
                set(_next _iquote)
            endif()
        elseif(_a MATCHES "^-isystem(.*)$")
            if(CMAKE_MATCH_1)
                list(APPEND _isystem "${CMAKE_MATCH_1}")
            else()
                set(_next _isystem)
            endif()
        elseif(_a MATCHES "^-idirafter(.*)$")
            if(CMAKE_MATCH_1)
                list(APPEND _idirafter "${CMAKE_MATCH_1}")
            else()
                set(_next _idirafter)
            endif()
        elseif(_a MATCHES "^-I(.*)$")
            if(CMAKE_MATCH_1)
                list(APPEND _i "${CMAKE_MATCH_1}")
            else()
                set(_next _i)
            endif()
        endif()
    endforeach()
    get_filename_component(_srcdir "${source}" DIRECTORY)
    set(${out} "" PARENT_SCOPE)
    foreach(_d "${_srcdir}" ${_iquote} ${_i} ${_isystem} ${_idirafter})
        if(NOT IS_ABSOLUTE "${_d}")
            set(_d "${cwd}/${_d}")
        endif()
        if(EXISTS "${_d}/${header}")
            file(REAL_PATH "${_d}" _real)
            set(${out} "${_real}" PARENT_SCOPE)
            return()
        endif()
    endforeach()
endfunction()

# What the generated build in <dir> uses, as a list of problems (empty = the
# prefix's headers first and only the prefix's archives on the link line).
function(_prefix_problems dir prefix_real include_real out)
    set(_problems "")
    set(_cc "${dir}/compile_commands.json")
    set(_cmd "")
    if(EXISTS "${_cc}")
        file(READ "${_cc}" _json)
        string(JSON _n LENGTH "${_json}")
        math(EXPR _last "${_n} - 1")
        foreach(_k RANGE ${_last})
            string(JSON _f GET "${_json}" ${_k} file)
            if(_f MATCHES "picoquic/tls_api\\.c$")
                string(JSON _cmd GET "${_json}" ${_k} command)
                string(JSON _cwd GET "${_json}" ${_k} directory)
                set(_src "${_f}")
                break()
            endif()
        endforeach()
    endif()
    if(NOT _cmd)
        list(APPEND _problems "no compile command for picoquic/tls_api.c")
    else()
        set(_hdrs picotls.h picotls/minicrypto.h)
        separate_arguments(_cargs UNIX_COMMAND "${_cmd}")
        if(NOT "-DPTLS_WITHOUT_OPENSSL" IN_LIST _cargs)
            list(APPEND _hdrs picotls/openssl.h)
        endif()
        foreach(_h IN LISTS _hdrs)
            _first_supplier("${_cmd}" "${_cwd}" "${_src}" "${_h}" _first)
            if(NOT _first STREQUAL include_real)
                list(APPEND _problems
                     "the first directory supplying ${_h} is '${_first}', not the prefix's ${include_real}")
            endif()
        endforeach()
    endif()
    set(_link "${dir}/CMakeFiles/ptls_link_probe.dir/link.txt")
    if(NOT EXISTS "${_link}")
        list(APPEND _problems "no generated link line for ptls_link_probe")
    else()
        file(READ "${_link}" _line)
        separate_arguments(_largs UNIX_COMMAND "${_line}")
        set(_seen "")
        foreach(_a IN LISTS _largs)
            if(_a MATCHES "libpicotls-([a-z]+)\\.a$")
                set(_name "${CMAKE_MATCH_1}")
                if(NOT IS_ABSOLUTE "${_a}")
                    set(_a "${dir}/${_a}")
                endif()
                file(REAL_PATH "${_a}" _real)
                string(FIND "${_real}" "${prefix_real}/" _in)
                if(NOT _in EQUAL 0)
                    list(APPEND _problems "linked archive outside the prefix: ${_real}")
                endif()
                list(APPEND _seen "${_name}")
            endif()
        endforeach()
        foreach(_need core minicrypto)
            if(NOT _need IN_LIST _seen)
                list(APPEND _problems "picotls-${_need} is not on the generated link line")
            endif()
        endforeach()
    endif()
    set(${out} "${_problems}" PARENT_SCOPE)
endfunction()

function(_expect_prefix tag dir prefix_real include_real)
    _prefix_problems("${dir}" "${prefix_real}" "${include_real}" _p)
    foreach(_x IN LISTS _p)
        _fail("${tag}" "${_x}")
    endforeach()
endfunction()

set(_prefix_arg "-DMOQ_PICOTLS_PREFIX=${PICOTLS_PREFIX}")

# A second, distinct picotls tree -- the same headers and archives copied, so
# the real paths differ -- under a directory whose name contains a space.
set(_alt "${WORK}/alt picotls")
file(COPY "${PICOTLS_PREFIX}/../include/" DESTINATION "${_alt}/include")
file(MAKE_DIRECTORY "${_alt}/build")
file(GLOB _archives "${PICOTLS_PREFIX}/libpicotls-*.a")
file(COPY ${_archives} DESTINATION "${_alt}/build")
file(REAL_PATH "${_alt}/build" _alt_prefix)
file(REAL_PATH "${_alt}/include" _alt_include)

# -- clean: the decoy sibling is not selected --------------------------------
_configure("${WORK}/clean" _rc _t "${_prefix_arg}")
_moq_reject_warnings(clean "${_t}")
if(NOT _rc EQUAL 0)
    _fail(clean "a clean configure failed:\n${_t}")
else()
    _expect_prefix(clean "${WORK}/clean" "${_real_prefix}" "${_real_include}")
endif()

# -- control: a valid foreign include AHEAD of the prefix's is caught --------
_configure("${WORK}/foreign-first" _rc _t "${_prefix_arg}"
           "-DMOQ_TEST_FOREIGN_INCLUDE_FIRST=${_alt}/include")
if(NOT _rc EQUAL 0)
    _fail(control-foreign-first "the control configure failed:\n${_t}")
else()
    _prefix_problems("${WORK}/foreign-first" "${_real_prefix}" "${_real_include}" _p)
    if(NOT _p MATCHES "first directory supplying picotls.h is '${_alt_include}'")
        _fail(control-foreign-first
              "the ordered header walk did not catch a foreign include placed "
              "ahead of the prefix's: ${_p}")
    endif()
endif()

# -- control: a foreign picotls/openssl.h alone, ahead of the prefix ----------
set(_osslonly "${WORK}/openssl-only/include")
file(MAKE_DIRECTORY "${_osslonly}/picotls")
file(COPY "${PICOTLS_PREFIX}/../include/picotls/openssl.h" DESTINATION "${_osslonly}/picotls")
file(REAL_PATH "${_osslonly}" _osslonly_real)
_configure("${WORK}/openssl-only-first" _rc _t "${_prefix_arg}"
           "-DMOQ_TEST_FOREIGN_INCLUDE_FIRST=${_osslonly}")
if(NOT _rc EQUAL 0)
    _fail(control-openssl-only "the control configure failed:\n${_t}")
else()
    _prefix_problems("${WORK}/openssl-only-first" "${_real_prefix}" "${_real_include}" _p)
    string(FIND "${_p}" "first directory supplying picotls/openssl.h is '${_osslonly_real}'" _caught)
    string(FIND "${_p}" "supplying picotls.h is" _picotls_h)
    if(_caught EQUAL -1 OR NOT _picotls_h EQUAL -1)
        _fail(control-openssl-only
              "a foreign picotls/openssl.h ahead of the prefix -- with picotls.h "
              "still the prefix's -- was not caught by name: ${_p}")
    endif()
endif()

# -- fresh-foreign: a typed caller value is never replaced -------------------
_configure("${WORK}/fresh" _rc _t "${_prefix_arg}"
           "-DPTLS_INCLUDE_DIR:PATH=${_decoy}/include")
_expect_refusal(fresh-foreign "${_rc}" "${_t}" PTLS_INCLUDE_DIR "${_decoy}/include")

# -- stale-cache: reconfigure over the existing foreign entry ----------------
_configure("${WORK}/fresh" _rc _t "${_prefix_arg}")
_expect_refusal(stale-cache "${_rc}" "${_t}" PTLS_INCLUDE_DIR "${_decoy}/include")
# The removal is part of the judged configure: its status and output are the
# arm's own.
_configure("${WORK}/fresh" _rc _t "-UPTLS_*" "${_prefix_arg}")
_moq_reject_warnings(stale-cache-cleared "${_t}")
if(NOT _rc EQUAL 0)
    _fail(stale-cache-cleared "the configure that removed PTLS_* failed:\n${_t}")
else()
    _expect_prefix(stale-cache-cleared "${WORK}/fresh" "${_real_prefix}" "${_real_include}")
endif()

# -- control: a warning beside an intended refusal is refused ---------------
# The captured output of a real refused configure that ALSO warns is judged by a
# child running the same _expect_refusal() the refusal arms use; it must fail,
# attributing the failure to the warning.
_configure("${WORK}/warned" _rc _t "${_prefix_arg}" -DMOQ_TEST_INJECT_WARNING=ON
           "-DPTLS_INCLUDE_DIR:PATH=${_decoy}/include")
file(WRITE "${WORK}/warned.out" "${_t}")
execute_process(
    COMMAND "${CMAKE_EXE}" "-DSELFTEST_TEXT=${WORK}/warned.out"
            "-DSELFTEST_RC=${_rc}" -DSELFTEST_VAR=PTLS_INCLUDE_DIR
            "-DSELFTEST_VALUE=${_decoy}/include"
            -P "${CMAKE_CURRENT_LIST_FILE}"
    RESULT_VARIABLE _crc OUTPUT_VARIABLE _co ERROR_VARIABLE _ce)
string(FIND "${_co}${_ce}" "FAIL[selftest.warning]" _attributed)
string(FIND "${_t}" "injected control warning" _injected)
if(_crc EQUAL 0 OR _attributed EQUAL -1 OR _injected EQUAL -1)
    _fail(control-warned-refusal
          "a warning beside an intended refusal was not refused by "
          "_expect_refusal (child rc ${_crc}):\n${_co}${_ce}")
endif()

# -- coherent: a caller value that is the prefix's headers -------------------
_configure("${WORK}/coherent" _rc _t "${_prefix_arg}"
           "-DPTLS_INCLUDE_DIR:PATH=${PICOTLS_PREFIX}/../include")
_moq_reject_warnings(coherent "${_t}")
if(NOT _rc EQUAL 0)
    _fail(coherent "a coherent caller value was refused:\n${_t}")
else()
    _expect_prefix(coherent "${WORK}/coherent" "${_real_prefix}" "${_real_include}")
endif()

# -- prefix-change: the recorded selection follows the prefix ---------------
_configure("${WORK}/clean" _rc _t "-DMOQ_PICOTLS_PREFIX=${_alt}/build")
_moq_reject_warnings(prefix-change "${_t}")
if(NOT _rc EQUAL 0)
    _fail(prefix-change "reconfiguring onto another prefix failed:\n${_t}")
else()
    _expect_prefix(prefix-change "${WORK}/clean" "${_alt_prefix}" "${_alt_include}")
endif()

# -- mixed-fusion: an archive from another tree ------------------------------
# On a scratch prefix with the accepted headers and exactly the required
# archives and NO fusion archive, so the arm runs whatever the declared prefix
# happens to carry.
set(_nofusion "${WORK}/nofusion-picotls")
file(COPY "${PICOTLS_PREFIX}/../include/" DESTINATION "${_nofusion}/include")
file(MAKE_DIRECTORY "${_nofusion}/build")
foreach(_n core minicrypto openssl)
    file(COPY "${PICOTLS_PREFIX}/libpicotls-${_n}.a" DESTINATION "${_nofusion}/build")
endforeach()
if(EXISTS "${_nofusion}/build/libpicotls-fusion.a")
    message(FATAL_ERROR "the no-fusion scratch prefix has a fusion archive")
endif()
_configure("${WORK}/fusion" _rc _t "-DMOQ_PICOTLS_PREFIX=${_nofusion}/build"
           "-DPTLS_FUSION_LIBRARY:FILEPATH=${_decoy}/lib/libpicotls-fusion.a")
_expect_refusal(mixed-fusion "${_rc}" "${_t}" PTLS_FUSION_LIBRARY
                "${_decoy}/lib/libpicotls-fusion.a")

# -- split-layout: a source checkout is not a prebuilt prefix ---------------
# The shape the Android JNI recipe used before it staged a coherent prefix:
# MOQ_PICOTLS_PREFIX names the picotls SOURCE checkout (headers, no archives)
# and the archives come from elsewhere.
_configure("${WORK}/split" _rc _t "-DMOQ_PICOTLS_PREFIX=${PICOTLS_PREFIX}/.."
           "-DPTLS_INCLUDE_DIR:PATH=${PICOTLS_PREFIX}/../include"
           "-DPTLS_CORE_LIBRARY:FILEPATH=${PICOTLS_PREFIX}/libpicotls-core.a"
           "-DPTLS_MINICRYPTO_LIBRARY:FILEPATH=${PICOTLS_PREFIX}/libpicotls-minicrypto.a"
           "-DPTLS_OPENSSL_LIBRARY:FILEPATH=${PICOTLS_PREFIX}/libpicotls-openssl.a")
_moq_reject_warnings(split-layout "${_t}")
string(REGEX REPLACE "[ \t\r\n]+" " " _flat "${_t}")
if(_rc EQUAL 0)
    _fail(split-layout "a source-checkout prefix with archives from elsewhere configured")
elseif(NOT _flat MATCHES "has no picotls-core archive")
    _fail(split-layout "the split layout failed without naming the missing archive:\n${_t}")
endif()
