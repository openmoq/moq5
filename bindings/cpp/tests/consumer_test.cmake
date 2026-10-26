foreach(_v SRC WORK BUILD CONSUMER_CONTEXT)
    if(NOT DEFINED ${_v} OR "${${_v}}" STREQUAL "")
        message(FATAL_ERROR "pass -D${_v}=<path>")
    endif()
endforeach()
include("${CMAKE_CURRENT_LIST_DIR}/../../../tests/cmake/RejectConsumerWarnings.cmake")
file(REMOVE_RECURSE "${WORK}")

# Provider hints are preloaded with the shared context rather than passed
# as -D: a consumer whose dependencies do not read them (a core-only one)
# then has nothing unused on its command line.
set(_context "${WORK}.context.cmake")
file(WRITE "${_context}" "include([==[${CONSUMER_CONTEXT}]==])\n")
foreach(_pair "PICOQUIC_SOURCE_DIR;MOQ_PICOQUIC_SOURCE_DIR" "PICOTLS_PREFIX;MOQ_PICOTLS_PREFIX")
    list(GET _pair 0 _input)
    list(GET _pair 1 _cache)
    if(DEFINED ${_input} AND NOT "${${_input}}" STREQUAL "")
        file(APPEND "${_context}"
            "set(${_cache} [==[${${_input}}]==] CACHE PATH \"Provider hint\")\n")
    endif()
endforeach()
moq_consumer_execute(COMMAND "${CMAKE_COMMAND}" -C "${_context}"
    -S "${SRC}" -B "${WORK}" "-DCMAKE_PREFIX_PATH=${BUILD}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc STREQUAL "0")
    message(FATAL_ERROR "C++ consumer configure failed: ${_rc}\n${_out}")
endif()
moq_consumer_execute(COMMAND "${CMAKE_COMMAND}" --build "${WORK}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc STREQUAL "0")
    message(FATAL_ERROR "C++ consumer build failed: ${_rc}\n${_out}")
endif()
moq_consumer_execute(COMMAND "${WORK}/moq_consumer_test"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc STREQUAL "0")
    message(FATAL_ERROR "C++ consumer run failed: ${_rc}\n${_out}")
endif()
message(STATUS "cpp_consumer: OK")
