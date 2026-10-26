# PICOQUIC_FETCH_PTLS=ON builds picotls from a source checkout, and in the
# supported local-source shape (MOQ_PICOTLS_PREFIX and
# FETCHCONTENT_SOURCE_DIR_PICOTLS naming the same checkout, as the iOS
# xcframework build does) that checkout is the authority:
#
#   local-source  both name the checkout: the configure succeeds, picoquic's
#                 FetchContent uses the checkout, and picoquic's tls_api.c is
#                 compiled with the checkout's headers first;
#   mismatch      they name different checkouts: the configure fails by name
#                 BEFORE picoquic -- and so before FetchContent -- runs.
#
# Whether picoquic was entered is recorded structurally, not read from its
# configure prose: CMAKE_PROJECT_picoquic_INCLUDE_BEFORE runs a marker script
# when picoquic's project() executes. The mismatch arm requires the marker
# ABSENT; the local-source arm requires it PRESENT, so an inert marker cannot
# pass as an early refusal.
#
# FETCHCONTENT_FULLY_DISCONNECTED=ON makes any attempt to download a failure,
# so neither arm can reach the network. Remote-fetch configurations are not
# covered here.
#
# Driven with -DCMAKE_EXE=<cmake> -DWORK=<dir> -DMODULES=<repo>/cmake
# -DPICOQUIC_SRC=<picoquic source> -DPICOTLS_SOURCE=<picotls checkout>.
cmake_minimum_required(VERSION 3.20)
include("${CMAKE_CURRENT_LIST_DIR}/moq_subprocess_warnings.cmake")

foreach(_v CMAKE_EXE WORK MODULES PICOQUIC_SRC PICOTLS_SOURCE)
    if(NOT ${_v})
        message(FATAL_ERROR "test_ptls_fetch_local_source: ${_v} is required")
    endif()
endforeach()

function(_fail tag)
    string(CONCAT _msg ${ARGN})
    message(SEND_ERROR "FAIL[${tag}]: ${_msg}")
endfunction()

file(REAL_PATH "${PICOTLS_SOURCE}" _source)
if(NOT EXISTS "${_source}/CMakeLists.txt" OR NOT EXISTS "${_source}/include/picotls.h")
    message(FATAL_ERROR "PICOTLS_SOURCE=${PICOTLS_SOURCE} is not a picotls checkout")
endif()

file(REMOVE_RECURSE "${WORK}")
set(_proj "${WORK}/proj")
file(MAKE_DIRECTORY "${_proj}")
file(WRITE "${_proj}/CMakeLists.txt"
"cmake_minimum_required(VERSION 3.20)\n"
"project(moq_ptls_fetch_local_source LANGUAGES C)\n"
"list(APPEND CMAKE_MODULE_PATH \"${MODULES}\")\n"
"find_package(Picoquic REQUIRED)\n")

set(_marker_script "${WORK}/picoquic-entered.cmake")
file(WRITE "${_marker_script}"
     "file(WRITE \"\${CMAKE_BINARY_DIR}/picoquic-entered\" \"entered\\n\")\n")

function(_configure dir out_rc out_text)
    execute_process(
        COMMAND "${CMAKE_EXE}" -S "${_proj}" -B "${dir}" -G "Unix Makefiles"
                -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
                -DFETCHCONTENT_FULLY_DISCONNECTED=ON
                -DPICOQUIC_FETCH_PTLS=ON -DWITH_FUSION=OFF
                "-DCMAKE_PROJECT_picoquic_INCLUDE_BEFORE=${_marker_script}"
                "-DMOQ_PICOQUIC_SOURCE_DIR=${PICOQUIC_SRC}"
                ${ARGN}
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _o ERROR_VARIABLE _e)
    set(${out_rc} "${_rc}" PARENT_SCOPE)
    set(${out_text} "${_o}${_e}" PARENT_SCOPE)
endfunction()

# -- local-source ---------------------------------------------------------------
_configure("${WORK}/local" _rc _t
           "-DMOQ_PICOTLS_PREFIX=${PICOTLS_SOURCE}"
           "-DFETCHCONTENT_SOURCE_DIR_PICOTLS=${PICOTLS_SOURCE}")
_moq_reject_warnings(local-source "${_t}")
if(NOT _rc EQUAL 0)
    _fail(local-source "the local-source configure failed:\n${_t}")
elseif(NOT EXISTS "${WORK}/local/picoquic-entered")
    _fail(local-source "the picoquic entry marker never fired, so it cannot "
          "witness an early refusal")
else()
    set(_cmd "")
    file(READ "${WORK}/local/compile_commands.json" _json)
    string(JSON _n LENGTH "${_json}")
    math(EXPR _last "${_n} - 1")
    foreach(_k RANGE ${_last})
        string(JSON _f GET "${_json}" ${_k} file)
        if(_f MATCHES "picoquic/tls_api\\.c$")
            string(JSON _cmd GET "${_json}" ${_k} command)
            break()
        endif()
    endforeach()
    # The first -I that supplies picotls.h must be the checkout's include.
    separate_arguments(_args UNIX_COMMAND "${_cmd}")
    set(_first "")
    set(_take FALSE)
    foreach(_a IN LISTS _args)
        set(_d "")
        if(_take)
            set(_d "${_a}")
            set(_take FALSE)
        elseif(_a MATCHES "^-(I|isystem|iquote)(.*)$")
            if(CMAKE_MATCH_2)
                set(_d "${CMAKE_MATCH_2}")
            else()
                set(_take TRUE)
            endif()
        endif()
        if(_d AND EXISTS "${_d}/picotls.h")
            file(REAL_PATH "${_d}" _first)
            break()
        endif()
    endforeach()
    if(NOT _first STREQUAL "${_source}/include")
        _fail(local-source "tls_api.c's first picotls.h comes from '${_first}', "
              "not the checkout ${_source}/include")
    endif()
endif()

# -- mismatch ---------------------------------------------------------------------
set(_other "${WORK}/other-picotls")
file(COPY "${PICOTLS_SOURCE}/include" DESTINATION "${_other}")
file(WRITE "${_other}/CMakeLists.txt" "# a second checkout, never configured\n")
_configure("${WORK}/mismatch" _rc _t
           "-DMOQ_PICOTLS_PREFIX=${_other}"
           "-DFETCHCONTENT_SOURCE_DIR_PICOTLS=${PICOTLS_SOURCE}")
_moq_reject_warnings(mismatch "${_t}")
string(REGEX REPLACE "[ \t\r\n]+" " " _flat "${_t}")
if(_rc EQUAL 0)
    _fail(mismatch "the configure succeeded with two different checkouts")
elseif(NOT _flat MATCHES "name different picotls checkouts")
    _fail(mismatch "the configure failed without naming the mismatch:\n${_t}")
elseif(EXISTS "${WORK}/mismatch/picoquic-entered")
    _fail(mismatch "picoquic's project() ran before the mismatch was refused")
endif()
