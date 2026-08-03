# Install the configured libmoq build to a scratch prefix, then build
# and run a standalone consumer that does
#     find_package(libmoq REQUIRED COMPONENTS adapter-msquic-managed)
# against it. Pins that the installed package re-resolves MsQuic
# through the shipped FindMsQuic module and normal package search.
#
# Args (all -D): BUILD (libmoq build dir), SRC (consumer source dir),
# WORK (scratch dir); optional MSQUIC_DIR_HINT / MSQUIC_ROOT_HINT
# (forwarded discovery hints from the parent build — no hardcoded
# paths) and C_COMPILER/C_FLAGS/LINK_FLAGS.

foreach(_v BUILD SRC WORK)
    if(NOT DEFINED ${_v})
        message(FATAL_ERROR "pass -D${_v}=<path>")
    endif()
endforeach()

set(_prefix "${WORK}/prefix")
set(_cbuild "${WORK}/consumer-build")
file(REMOVE_RECURSE "${_prefix}" "${_cbuild}")

execute_process(
    COMMAND ${CMAKE_COMMAND} --install "${BUILD}" --prefix "${_prefix}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "libmoq install failed:\n${_out}")
endif()

set(_fwd "")
if(DEFINED MSQUIC_DIR_HINT AND NOT MSQUIC_DIR_HINT STREQUAL "" AND
   NOT MSQUIC_DIR_HINT MATCHES "NOTFOUND")
    list(APPEND _fwd "-Dmsquic_DIR=${MSQUIC_DIR_HINT}")
endif()
if(DEFINED MSQUIC_ROOT_HINT AND NOT MSQUIC_ROOT_HINT STREQUAL "")
    list(APPEND _fwd "-DMOQ_MSQUIC_ROOT=${MSQUIC_ROOT_HINT}")
endif()
if(DEFINED C_COMPILER AND NOT C_COMPILER STREQUAL "")
    list(APPEND _fwd "-DCMAKE_C_COMPILER=${C_COMPILER}")
endif()
if(DEFINED C_FLAGS AND NOT C_FLAGS STREQUAL "")
    list(APPEND _fwd "-DCMAKE_C_FLAGS=${C_FLAGS}")
endif()
if(DEFINED LINK_FLAGS AND NOT LINK_FLAGS STREQUAL "")
    list(APPEND _fwd "-DCMAKE_EXE_LINKER_FLAGS=${LINK_FLAGS}")
endif()

execute_process(
    COMMAND ${CMAKE_COMMAND} -S "${SRC}" -B "${_cbuild}"
        "-DCMAKE_PREFIX_PATH=${_prefix}" ${_fwd}
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "consumer configure failed:\n${_out}")
endif()

execute_process(
    COMMAND ${CMAKE_COMMAND} --build "${_cbuild}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "consumer build failed:\n${_out}")
endif()

execute_process(
    COMMAND "${_cbuild}/moq_msquic_consumer_test"
    RESULT_VARIABLE _rc)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "consumer run failed: ${_rc}")
endif()

message(STATUS "msquic_install_consumer: OK")
