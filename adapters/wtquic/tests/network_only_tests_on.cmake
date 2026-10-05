# Network-only tests-on generation lane: clone the wtquic prefix
# WITHOUT its msquic component, then prove a tests-on libmoq tree
# (adapter + managed component) configures and builds against it —
# every MsQuic-server test must gate itself out, never break the
# configure. Args (-D): SRC (libmoq source root), WORK (scratch),
# WTQUIC_PREFIX (full wtquic install prefix).

foreach(_v SRC WORK WTQUIC_PREFIX)
    if(NOT DEFINED ${_v})
        message(FATAL_ERROR "pass -D${_v}=<path>")
    endif()
endforeach()

set(_prefix "${WORK}/network-only-prefix")
set(_tree "${WORK}/network-only-tree")
set(_context)
if(CONSUMER_CONTEXT)
    list(APPEND _context -C "${CONSUMER_CONTEXT}")
endif()
file(REMOVE_RECURSE "${_prefix}" "${_tree}")

# clone the prefix, then strip the msquic component out of it
file(COPY "${WTQUIC_PREFIX}/include" "${WTQUIC_PREFIX}/lib"
     DESTINATION "${_prefix}")
file(GLOB _msq
    "${_prefix}/lib/cmake/wtquic/wtquic-msquic*"
    "${_prefix}/lib/cmake/wtquic/FindMsQuic*"
    "${_prefix}/lib/libwtquic-msquic*"
    "${_prefix}/lib/pkgconfig/wtquic-msquic*"
    "${_prefix}/include/wtquic/wtquic_msquic.h")
if(_msq STREQUAL "")
    message(FATAL_ERROR "prefix clone has no msquic files to strip")
endif()
file(REMOVE ${_msq})

execute_process(
    COMMAND ${CMAKE_COMMAND} ${_context} -S "${SRC}" -B "${_tree}"
        -DMOQ_BUILD_TESTS=ON
        -DMOQ_BUILD_ADAPTER_WTQUIC=ON
        -DMOQ_BUILD_WTQUIC_NETWORK_MANAGED=ON
        -DMOQ_BUILD_WTQUIC_MSQUIC_MANAGED=OFF
        -DMOQ_BUILD_SERVICE=ON -DMOQ_BUILD_MSF=ON
        -DMOQ_WARNINGS_AS_ERRORS=ON
        "-DCMAKE_PREFIX_PATH=${_prefix}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0 OR _out MATCHES "[Ww]arning([ :]|$)")
    message(FATAL_ERROR "network-only tests-on configure failed:\n${_out}")
endif()
if(_out MATCHES "wtq::msquic")
    message(FATAL_ERROR "network-only configure still mentions msquic:\n${_out}")
endif()

execute_process(
    COMMAND ${CMAKE_COMMAND} --build "${_tree}"
        --target moq-adapter-wtquic-network-managed test_wtquic_keys
        test_wtquic_public_compile test_wtquic_network_managed
        test_endpoint_wtquic_network_smoke
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0 OR _out MATCHES "[Ww]arning([ :]|$)")
    message(FATAL_ERROR "network-only tests-on build failed:\n${_out}")
endif()

execute_process(
    COMMAND ${CMAKE_CTEST_COMMAND} --test-dir "${_tree}" -V
        -R "^(wtquic_network_managed|endpoint_wtquic_network_smoke)$"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
file(WRITE "${WORK}/network-only-policy-tests.log" "${_out}")
if(NOT _rc EQUAL 0 OR _out MATCHES "[Ww]arning([ :]|$)")
    message(FATAL_ERROR "network-only public policy tests failed:\n${_out}")
endif()

message(STATUS "network_only_tests_on: OK")
