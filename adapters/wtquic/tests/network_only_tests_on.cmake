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

# Run one nested step. Its exit status and complete output are printed
# whether or not it succeeds, and kept in WORK/<name>.log, so the test log
# carries every configure, compiler and linker diagnostic of a passing run
# too. The caller gets <name>_rc and <name>_out to judge the step by.
function(_nested_step name)
    execute_process(COMMAND ${ARGN}
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
    file(WRITE "${WORK}/${name}.log" "${_out}")
    message(STATUS "${name}: exit ${_rc}; complete output follows "
                   "(also ${WORK}/${name}.log)")
    if(NOT _out STREQUAL "")
        message(NOTICE "${_out}")
    endif()
    message(STATUS "${name}: end of output")
    set(${name}_rc "${_rc}" PARENT_SCOPE)
    set(${name}_out "${_out}" PARENT_SCOPE)
endfunction()

_nested_step(network_only_tests_on_configure
    ${CMAKE_COMMAND} ${_context} -S "${SRC}" -B "${_tree}"
        -DMOQ_BUILD_TESTS=ON
        -DMOQ_BUILD_ADAPTER_WTQUIC=ON
        -DMOQ_BUILD_WTQUIC_NETWORK_MANAGED=ON
        -DMOQ_BUILD_WTQUIC_MSQUIC_MANAGED=OFF
        -DMOQ_BUILD_SERVICE=ON -DMOQ_BUILD_MSF=ON
        -DMOQ_WARNINGS_AS_ERRORS=ON
        "-DCMAKE_PREFIX_PATH=${_prefix}")
if(NOT network_only_tests_on_configure_rc EQUAL 0 OR
   network_only_tests_on_configure_out MATCHES "[Ww]arning([ :]|$)")
    message(FATAL_ERROR "network-only tests-on configure failed")
endif()
if(network_only_tests_on_configure_out MATCHES "wtq::msquic")
    message(FATAL_ERROR "network-only configure still mentions msquic")
endif()

# One job, whatever the nested generator or environment would choose.
_nested_step(network_only_tests_on_build
    ${CMAKE_COMMAND} --build "${_tree}" --parallel 1
        --target moq-adapter-wtquic-network-managed test_wtquic_keys
        test_wtquic_public_compile test_wtquic_network_managed
        test_endpoint_wtquic_network_smoke)
if(NOT network_only_tests_on_build_rc EQUAL 0 OR
   network_only_tests_on_build_out MATCHES "[Ww]arning([ :]|$)")
    message(FATAL_ERROR "network-only tests-on build failed")
endif()

_nested_step(network_only_policy_tests
    ${CMAKE_CTEST_COMMAND} --test-dir "${_tree}" -V
        -R "^(wtquic_network_managed|endpoint_wtquic_network_smoke)$")
if(NOT network_only_policy_tests_rc EQUAL 0 OR
   network_only_policy_tests_out MATCHES "[Ww]arning([ :]|$)")
    message(FATAL_ERROR "network-only public policy tests failed")
endif()

message(STATUS "network_only_tests_on: OK")
