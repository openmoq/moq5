# Build and run the standalone consumer against the libmoq BUILD tree:
#     find_package(libmoq REQUIRED COMPONENTS adapter-wtquic)
# resolved via -Dlibmoq_DIR=<build dir> (the generated build-tree config
# bakes the wtquic package location this build resolved).
#
# Args (all -D): BUILD (libmoq build dir), SRC (consumer source dir),
# WORK (scratch dir), optional C_COMPILER/C_FLAGS/LINK_FLAGS/OPENSSL_ROOT.

foreach(_v BUILD SRC WORK)
    if(NOT DEFINED ${_v})
        message(FATAL_ERROR "pass -D${_v}=<path>")
    endif()
endforeach()

set(_cbuild "${WORK}/consumer-build")
file(REMOVE_RECURSE "${_cbuild}")

function(assert_child_openssl_root dir label)
    if(DEFINED OPENSSL_ROOT AND NOT OPENSSL_ROOT STREQUAL "")
        unset(_child_OPENSSL_ROOT_DIR)
        load_cache("${dir}" READ_WITH_PREFIX _child_ OPENSSL_ROOT_DIR)
        if(NOT DEFINED _child_OPENSSL_ROOT_DIR OR
           NOT _child_OPENSSL_ROOT_DIR STREQUAL OPENSSL_ROOT)
            message(FATAL_ERROR
                "${label} did not retain the selected OpenSSL root: got "
                "'${_child_OPENSSL_ROOT_DIR}', expected '${OPENSSL_ROOT}'")
        endif()
    endif()
endfunction()

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
if(DEFINED OPENSSL_ROOT AND NOT OPENSSL_ROOT STREQUAL "")
    list(APPEND _fwd "-DOPENSSL_ROOT_DIR=${OPENSSL_ROOT}")
endif()

execute_process(
    COMMAND ${CMAKE_COMMAND} -S "${SRC}" -B "${_cbuild}"
        "-Dlibmoq_DIR=${BUILD}" ${_fwd}
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "consumer configure failed:\n${_out}")
endif()
assert_child_openssl_root("${_cbuild}" "build-tree consumer")

execute_process(
    COMMAND ${CMAKE_COMMAND} --build "${_cbuild}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "consumer build failed:\n${_out}")
endif()

execute_process(
    COMMAND "${_cbuild}/moq_wtquic_consumer_test"
    RESULT_VARIABLE _rc)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "consumer run failed: ${_rc}")
endif()

message(STATUS "wtquic_adapter_consumer: OK")
