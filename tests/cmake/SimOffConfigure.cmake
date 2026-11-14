# Configures this checkout with tests on and moq-sim off, plus the optional
# tiers whose tests drive SimPair sessions, and requires the configure and
# generate steps to succeed: every SimPair-driven test must sit behind
# if(TARGET moq::sim) rather than link a target that is not built.
#
# Inputs: MOQ_SOURCE (libmoq checkout), MOQ_WORK (recreated), MOQ_CONTEXT
# (cache preload with the producer toolchain), optional MOQ_GENERATOR.
cmake_minimum_required(VERSION 3.20)

foreach(var MOQ_SOURCE MOQ_WORK MOQ_CONTEXT)
    if(NOT DEFINED ${var} OR "${${var}}" STREQUAL "")
        message(FATAL_ERROR "${var} is required")
    endif()
endforeach()

file(REMOVE_RECURSE "${MOQ_WORK}")
set(generator_args "")
if(DEFINED MOQ_GENERATOR AND NOT MOQ_GENERATOR STREQUAL "")
    set(generator_args -G "${MOQ_GENERATOR}")
endif()
execute_process(
    COMMAND "${CMAKE_COMMAND}" ${generator_args} -C "${MOQ_CONTEXT}"
        -S "${MOQ_SOURCE}" -B "${MOQ_WORK}"
        -DMOQ_BUILD_TESTS=ON
        -DMOQ_BUILD_SIM=OFF
        -DMOQ_BUILD_MSF=ON
        -DMOQ_BUILD_MEDIA_OBJECT=ON
        -DMOQ_BUILD_MPEG2TS=ON
        -DMOQ_BUILD_SERVICE=ON
        -DMOQ_BUILD_PLAYBACK=ON
        -DMOQ_BUILD_RELAY_CORE=ON
        -DMOQ_BUILD_RELAY_RUNTIME=ON
        -DMOQ_BUILD_RELAY_TOOL=ON
    RESULT_VARIABLE rc
    OUTPUT_VARIABLE out
    ERROR_VARIABLE out)
# The nested configure's status and whole output are kept and shown even on
# success, so its diagnostics can be judged after the work tree is gone.
file(WRITE "${MOQ_WORK}/logs/sim_off.configure.log" "${out}")
file(WRITE "${MOQ_WORK}/logs/sim_off.configure.status" "${rc}\n")
message(STATUS "MOQ_BUILD_SIM=OFF configure exit ${rc}:\n${out}")
if(NOT rc EQUAL 0)
    message(FATAL_ERROR "configure with MOQ_BUILD_SIM=OFF failed (${rc}):\n${out}")
endif()
message(STATUS "MOQ_BUILD_SIM=OFF configure and generate succeed")
