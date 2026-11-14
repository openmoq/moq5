# Controls for the tier consumer test's registration and install layout.
#
# This checkout is configured, not built, into scratch builds with the
# producer's own context. For each, ctest reports the command it registered
# for tier_pkgconfig_consumers, and that exact command is run:
#   - the default layout reaches the point of installing (run with
#     MOQ_PREFLIGHT_ONLY, since nothing was built to install), and nothing
#     short of that, an unsupported verdict included, passes. GNUInstallDirs
#     defaults directories this project never installs into, such as
#     OLDINCLUDEDIR, to absolute paths; they must not stop the test;
#   - an include directory, or a documentation directory, that climbs out of
#     the prefix is refused before anything is installed: the run reports
#     itself unsupported and leaves no stage behind.
#
# Inputs: MOQ_SOURCE (this checkout), MOQ_WORK (recreated), MOQ_CONTEXT
# (from moq_write_consumer_context), MOQ_GENERATOR, MOQ_PKG_CONFIG,
# CTEST_COMMAND. Optional: MOQ_MAKE_PROGRAM, MOQ_CONFIG.
cmake_minimum_required(VERSION 3.20)

foreach(var MOQ_SOURCE MOQ_WORK MOQ_CONTEXT MOQ_GENERATOR MOQ_PKG_CONFIG
            CTEST_COMMAND)
    if(NOT DEFINED ${var} OR "${${var}}" STREQUAL "")
        message(FATAL_ERROR "${var} is required")
    endif()
endforeach()
include("${CMAKE_CURRENT_LIST_DIR}/TierPkgConfigChecks.cmake")

file(REMOVE_RECURSE "${MOQ_WORK}")
file(MAKE_DIRECTORY "${MOQ_WORK}")
set(WORK "${MOQ_WORK}/logs")
tier_scrub_environment(scrub)
set(clean_env "${CMAKE_COMMAND}" -E env ${scrub})

set(generator_args -G "${MOQ_GENERATOR}")
if(DEFINED MOQ_MAKE_PROGRAM AND NOT MOQ_MAKE_PROGRAM STREQUAL "")
    list(APPEND generator_args "-DCMAKE_MAKE_PROGRAM=${MOQ_MAKE_PROGRAM}")
endif()
set(config_args "")
if(DEFINED MOQ_CONFIG AND NOT MOQ_CONFIG STREQUAL "")
    set(config_args -C "${MOQ_CONFIG}")
endif()

# Configure this checkout with tests on, so the consumer test is registered
# exactly as in a real build.
function(registration_configure build)
    tier_run("configure ${build}" ${clean_env} "${CMAKE_COMMAND}"
        ${generator_args} --no-warn-unused-cli -C "${MOQ_CONTEXT}"
        -S "${MOQ_SOURCE}" -B "${build}"
        -DMOQ_BUILD_TESTS=ON "-DMOQ_TIER_PKG_CONFIG=${MOQ_PKG_CONFIG}" ${ARGN})
endfunction()

# The command ctest registered for tier_pkgconfig_consumers in `build`, and
# the work directory it names. The listing is metadata: its stdout is
# validated as the one expected JSON document, and its stderr must be free
# of diagnostics (tier_run_metadata, tier_registration_listing).
function(registration_command build out_command out_work)
    tier_run_metadata("list the tests of ${build}" ${clean_env} "${CTEST_COMMAND}"
        --test-dir "${build}" --show-only=json-v1
        -R "^tier_pkgconfig_consumers$" ${config_args})
    tier_registration_listing("${tier_stdout}" tier_pkgconfig_consumers
        "${MOQ_SOURCE}/tests/cmake/TierPkgConfigConsumer.cmake" command work)
    set(${out_command} "${command}" PARENT_SCOPE)
    set(${out_work} "${work}" PARENT_SCOPE)
endfunction()

# == The default layout reaches the point of installing =======================
set(build "${MOQ_WORK}/default")
registration_configure("${build}")
registration_command("${build}" command work)
list(INSERT command 1 "-DMOQ_PREFLIGHT_ONLY=ON")
tier_run("default layout preflight" ${clean_env} ${command})
set(text "${tier_stdout}\n${tier_stderr}")
string(FIND "${text}" "${TIER_PREFLIGHT_MARKER} install destinations inside the prefix:" at)
if(at EQUAL -1)
    message(FATAL_ERROR
        "control default layout: the destinations were not accepted:\n${text}")
endif()
# Only the positive counts. An unsupported verdict, a producer context the
# consumer test cannot represent included, is evidence of its own and fails
# this control: it does not show the default registration reaching install.
string(FIND "${text}" "${TIER_UNSUPPORTED_MARKER}" unsupported)
if(NOT unsupported EQUAL -1)
    message(FATAL_ERROR
        "control default layout: the registered command reported itself "
        "unsupported before installing; that is separate evidence, not this "
        "control's positive:\n${text}")
endif()
string(FIND "${text}" "${TIER_PREFLIGHT_MARKER} install would proceed" proceed)
if(proceed EQUAL -1)
    message(FATAL_ERROR
        "control default layout: the registered command did not reach the "
        "point of installing:\n${text}")
endif()
message(STATUS "control default layout: expected verdict, install would proceed")

# == A destination that climbs out of the prefix is refused before installing =
function(registration_refused name expected_dest)
    set(build "${MOQ_WORK}/${name}")
    registration_configure("${build}" ${ARGN})
    registration_command("${build}" command work)
    tier_run("${name} run" ${clean_env} ${command})
    set(text "${tier_stdout}\n${tier_stderr}")
    string(FIND "${text}"
        "${TIER_UNSUPPORTED_MARKER} install destinations outside the prefix, so nothing was installed: "
        at)
    string(FIND "${text}" "${expected_dest}" named)
    if(at EQUAL -1 OR named EQUAL -1)
        message(FATAL_ERROR
            "control ${name}: not refused for ${expected_dest}:\n${text}")
    endif()
    foreach(left IN ITEMS stage sdk "relocated sdk")
        if(EXISTS "${work}/${left}")
            message(FATAL_ERROR
                "control ${name}: refused only after installing: ${work}/${left}")
        endif()
    endforeach()
    message(STATUS "control ${name}: refused before installing")
endfunction()

registration_refused(escape-include "\${CMAKE_INSTALL_PREFIX}/../headers"
    "-DCMAKE_INSTALL_INCLUDEDIR=../headers")
registration_refused(escape-doc "\${CMAKE_INSTALL_PREFIX}/../../outside-doc"
    "-DCMAKE_INSTALL_DOCDIR=../../outside-doc")

message(STATUS "tier pkg-config registration controls: every verdict as expected")
