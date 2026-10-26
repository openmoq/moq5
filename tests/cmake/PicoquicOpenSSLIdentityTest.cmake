# FindPicoquic gives a source-tree picoquic one OpenSSL link identity with
# the adapter: picoquic-core's and picohttp-core's raw OpenSSL paths become
# the OpenSSL::SSL / OpenSSL::Crypto targets the adapter links, so a static
# OpenSSL reaches a link line once. Against a stand-in provider whose
# targets carry raw paths, with placeholder OpenSSL and picotls trees:
#   build      libmoq's own build with the adapter and its OpenSSL
#              verifier: both targets respelled;
#   installed  a consumer of the installed package, whose config recorded
#              that the adapter links OpenSSL (no build options): both
#              respelled;
#   neither    no adapter, no record: left raw;
#   off        the record, but picoquic configured WITH_OPENSSL=OFF: left raw;
#   preexist   the record, but picoquic's targets existed before
#              find_package: left raw.
# Inputs: MOQ_SOURCE (libmoq source tree), MOQ_WORK (recreated), optional
# MOQ_GENERATOR.
cmake_minimum_required(VERSION 3.20)
foreach(var MOQ_SOURCE MOQ_WORK)
    if(NOT DEFINED ${var} OR "${${var}}" STREQUAL "")
        message(FATAL_ERROR "${var} is required")
    endif()
endforeach()
file(REMOVE_RECURSE "${MOQ_WORK}")
set(fixture "${MOQ_SOURCE}/tests/cmake/picoquic_openssl_identity")
set(ssl "${MOQ_WORK}/openssl")
file(WRITE "${ssl}/include/openssl/ssl.h" "")
file(WRITE "${ssl}/include/openssl/opensslv.h"
"# define OPENSSL_VERSION_MAJOR 3
# define OPENSSL_VERSION_MINOR 0
# define OPENSSL_VERSION_PATCH 1
# define OPENSSL_VERSION_STR \"3.0.1\"
# define OPENSSL_VERSION_NUMBER 0x30000010L
")
file(WRITE "${ssl}/lib/libssl.a" "")
file(WRITE "${ssl}/lib/libcrypto.a" "")
set(ptls "${MOQ_WORK}/picotls")
file(WRITE "${ptls}/include/picotls.h" "")
file(WRITE "${ptls}/include/picotls/minicrypto.h" "")
file(WRITE "${ptls}/include/picotls/openssl.h" "")
foreach(lib IN ITEMS core minicrypto openssl)
    file(WRITE "${ptls}/build/libpicotls-${lib}.a" "")
endforeach()

set(generator_args "")
if(DEFINED MOQ_GENERATOR AND NOT MOQ_GENERATOR STREQUAL "")
    set(generator_args -G "${MOQ_GENERATOR}")
endif()

set(failures "")
function(_case name expect)
    set(build "${MOQ_WORK}/${name}")
    set(result "${MOQ_WORK}/${name}.txt")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" ${generator_args}
            -S "${fixture}/consumer" -B "${build}"
            "-DMOQ_TEST_MODULE_DIR=${MOQ_SOURCE}/cmake"
            "-DMOQ_PICOQUIC_SOURCE_DIR=${fixture}/fake_picoquic"
            "-DMOQ_PICOTLS_PREFIX=${ptls}/build"
            "-DOPENSSL_ROOT_DIR=${ssl}"
            "-DOPENSSL_USE_STATIC_LIBS=TRUE"
            "-DMOQ_TEST_RESULT=${result}"
            ${ARGN}
        RESULT_VARIABLE rc
        OUTPUT_VARIABLE out
        ERROR_VARIABLE out)
    if(NOT rc EQUAL 0)
        set(failures "${failures}${name}: configure failed (${rc}):\n${out}\n" PARENT_SCOPE)
        return()
    endif()
    file(READ "${result}" got)
    set(want "picoquic-core=${expect}\npicohttp-core=${expect}\n")
    if(NOT got STREQUAL want)
        set(failures "${failures}${name}: expected\n${want}got\n${got}\n" PARENT_SCOPE)
    endif()
endfunction()

_case(build targets -DMOQ_BUILD_ADAPTER_PICOQUIC=ON -DMOQ_PICOQUIC_VERIFY_BACKEND=openssl)
_case(installed targets -D_LIBMOQ_ADAPTER_PICOQUIC_NEEDS_OPENSSL=TRUE)
_case(neither raw)
_case(off raw -D_LIBMOQ_ADAPTER_PICOQUIC_NEEDS_OPENSSL=TRUE -DWITH_OPENSSL=OFF)
_case(preexist raw -D_LIBMOQ_ADAPTER_PICOQUIC_NEEDS_OPENSSL=TRUE -DMOQ_TEST_PRE_ADD=ON)

if(failures)
    message(FATAL_ERROR "OpenSSL link identity:\n${failures}")
endif()
message(STATUS "OpenSSL link identity: build, installed, neither, off and preexist cases hold")
