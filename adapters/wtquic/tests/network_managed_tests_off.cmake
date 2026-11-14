# Tests-off generation lane for the managed Network.framework
# component: a fresh tree with MOQ_BUILD_TESTS=OFF must CONFIGURE
# (Threads and every other dependency found outside the tests block)
# and BUILD the component target. Args (-D): SRC (libmoq source root),
# WORK (scratch), WTQUIC_PREFIX (wtquic install prefix).

foreach(_v SRC WORK WTQUIC_PREFIX)
    if(NOT DEFINED ${_v})
        message(FATAL_ERROR "pass -D${_v}=<path>")
    endif()
endforeach()

set(_tree "${WORK}/tests-off-tree")
set(_context)
if(CONSUMER_CONTEXT)
    list(APPEND _context -C "${CONSUMER_CONTEXT}")
endif()
file(REMOVE_RECURSE "${_tree}")

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

_nested_step(network_managed_tests_off_configure
    ${CMAKE_COMMAND} ${_context} -S "${SRC}" -B "${_tree}"
        -DMOQ_BUILD_TESTS=OFF
        -DMOQ_BUILD_ADAPTER_WTQUIC=ON
        -DMOQ_BUILD_WTQUIC_NETWORK_MANAGED=ON
        "-DCMAKE_PREFIX_PATH=${WTQUIC_PREFIX}")
if(NOT network_managed_tests_off_configure_rc EQUAL 0)
    message(FATAL_ERROR "tests-off configure failed")
endif()

# One job, whatever the nested generator or environment would choose.
_nested_step(network_managed_tests_off_build
    ${CMAKE_COMMAND} --build "${_tree}" --parallel 1
        --target moq-adapter-wtquic-network-managed)
if(NOT network_managed_tests_off_build_rc EQUAL 0)
    message(FATAL_ERROR "tests-off component build failed")
endif()

message(STATUS "network_managed_tests_off: OK")
