# Build and run the standalone consumer against the libmoq BUILD tree:
#     find_package(libmoq REQUIRED COMPONENTS adapter-msquic-managed)
# resolved via -Dlibmoq_DIR=<build dir> (the generated build-tree
# config bakes the MsQuic discovery this build resolved).
#
# Args (all -D): BUILD (libmoq build dir), SRC (consumer source dir),
# WORK (scratch dir), optional C_COMPILER/C_FLAGS/LINK_FLAGS.

foreach(_v BUILD SRC WORK)
    if(NOT DEFINED ${_v})
        message(FATAL_ERROR "pass -D${_v}=<path>")
    endif()
endforeach()

set(_cbuild "${WORK}/consumer-build")
file(REMOVE_RECURSE "${_cbuild}")

include("${CMAKE_CURRENT_LIST_DIR}/../../../tests/cmake/RejectConsumerWarnings.cmake")
include("${CMAKE_CURRENT_LIST_DIR}/consumer_environment.cmake")

set(_fwd "")
if(DEFINED C_COMPILER AND NOT C_COMPILER STREQUAL "")
    list(APPEND _fwd "-DCMAKE_C_COMPILER=${C_COMPILER}")
endif()
if(DEFINED C_FLAGS AND NOT C_FLAGS STREQUAL "")
    list(APPEND _fwd "-DCMAKE_C_FLAGS=${C_FLAGS}")
endif()
if(DEFINED LINK_FLAGS AND NOT LINK_FLAGS STREQUAL "")
    list(APPEND _fwd "-DCMAKE_EXE_LINKER_FLAGS=${LINK_FLAGS}")
endif()

moq_consumer_execute(
    COMMAND ${CMAKE_COMMAND} -S "${SRC}" -B "${_cbuild}"
        "-Dlibmoq_DIR=${BUILD}" ${_fwd}
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "consumer configure failed:\n${_out}")
endif()

moq_consumer_execute(
    COMMAND ${CMAKE_COMMAND} --build "${_cbuild}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "consumer build failed:\n${_out}")
endif()

msquic_consumer_environment(_env)
moq_consumer_execute(
    COMMAND ${_env} "${_cbuild}/moq_msquic_consumer_test"
    RESULT_VARIABLE _rc)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "consumer run failed: ${_rc}")
endif()

message(STATUS "msquic_adapter_consumer: OK")
