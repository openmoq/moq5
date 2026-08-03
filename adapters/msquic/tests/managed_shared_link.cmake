# Shared-library link regression for the managed MsQuic facade.
#
# The doorbell facade calls an adapter-internal helper
# (moq_msq_conn_service_wake) that is inseparable from the hidden conn
# machinery in msquic_adapter.c. With BUILD_SHARED_LIBS=ON the attach and
# managed translation units must therefore share one dylib: a split would
# put the caller and a hidden callee in different shared objects and fail to
# link. A static build hides this (archives resolve every member), so this
# lane configures a genuinely shared tree and exercises the managed
# component two ways that a static build cannot cover:
#   1. build tree: build + RUN a consumer that resolves managed symbols
#      through the shared base dylib;
#   2. installed contract: install the shared tree and run the install
#      consumer, which does find_package(... COMPONENTS adapter-msquic
#      adapter-msquic-managed) against the relocated shared package and
#      links BOTH components — proving the INTERFACE managed target resolves
#      to the one installed dylib with no duplicate/ambiguous ownership.
#
# Args (-D): SRC (libmoq source root), WORK (scratch dir); optional
# MSQUIC_DIR_HINT / MSQUIC_ROOT_HINT / C_COMPILER forwarded discovery hints.

foreach(_v SRC WORK)
    if(NOT DEFINED ${_v})
        message(FATAL_ERROR "pass -D${_v}=<path>")
    endif()
endforeach()

set(_tree "${WORK}/shared-tree")
file(REMOVE_RECURSE "${_tree}")

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

# A shared tree trimmed to core + the MsQuic adapter/managed component, so
# the install below is self-consistent and the lane stays bounded.
execute_process(
    COMMAND ${CMAKE_COMMAND} -S "${SRC}" -B "${_tree}"
        -DBUILD_SHARED_LIBS=ON
        -DMOQ_BUILD_TESTS=ON
        -DMOQ_BUILD_SIM=OFF
        -DMOQ_BUILD_LOC=OFF
        -DMOQ_BUILD_CMAF=OFF
        -DMOQ_BUILD_ADAPTER_MSQUIC=ON
        -DMOQ_BUILD_MSQUIC_MANAGED=ON
        ${_fwd}
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "shared managed configure failed:\n${_out}")
endif()

# Build the shared tree. Pre-fix this fails to link the managed shared
# object on the hidden helper; the fix makes the two units share the base
# dylib so it resolves.
execute_process(
    COMMAND ${CMAKE_COMMAND} --build "${_tree}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "shared managed build/link failed:\n${_out}")
endif()

# 1. Build-tree consumer: prove the shared object loads and both public
#    surfaces resolved at runtime.
execute_process(
    COMMAND "${_tree}/adapters/msquic/test_msquic_public_compile"
    RESULT_VARIABLE _rc)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "shared managed build-tree consumer run failed: ${_rc}")
endif()

# 2. Installed contract: run the install consumer against the shared tree.
#    It installs the shared package to a scratch prefix, then builds and
#    runs the both-component consumer against it.
execute_process(
    COMMAND ${CMAKE_CTEST_COMMAND} --test-dir "${_tree}"
        -R "^msquic_install_consumer$" --output-on-failure
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "shared install consumer failed:\n${_out}")
endif()

message(STATUS "msquic_managed_shared_link: OK")
