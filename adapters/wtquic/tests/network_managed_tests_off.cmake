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
file(REMOVE_RECURSE "${_tree}")

execute_process(
    COMMAND ${CMAKE_COMMAND} -S "${SRC}" -B "${_tree}"
        -DMOQ_BUILD_TESTS=OFF
        -DMOQ_BUILD_ADAPTER_WTQUIC=ON
        -DMOQ_BUILD_WTQUIC_NETWORK_MANAGED=ON
        "-DCMAKE_PREFIX_PATH=${WTQUIC_PREFIX}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "tests-off configure failed:\n${_out}")
endif()

execute_process(
    COMMAND ${CMAKE_COMMAND} --build "${_tree}"
        --target moq-adapter-wtquic-network-managed
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "tests-off component build failed:\n${_out}")
endif()

message(STATUS "network_managed_tests_off: OK")
