# FindMsQuic's third branch: the caller already defined msquic::msquic.
#
# That dependency lives only in the parent's own scope, so the nested-configure
# registration regression cannot reproduce it and is deliberately NOT registered
# in this mode. This control proves the omission is a clean skip rather than a
# failing test, and that nothing else in the MsQuic suite went missing with it.
#
# Fail-closed throughout: a failed configure, an unreadable cache, or an
# unparseable structured listing is an error, never an empty set that compares
# equal to an expectation.

cmake_minimum_required(VERSION 3.20)

foreach(_v SOURCE_DIR BUILD_DIR CMAKE_CMD CTEST_CMD MSQUIC_INCLUDE_DIR
           MSQUIC_LIBRARY SELF_TEST_NAME)
    if(NOT DEFINED ${_v} OR "${${_v}}" STREQUAL "")
        message(FATAL_ERROR "preexisting_msquic_target: ${_v} not provided")
    endif()
endforeach()

file(REMOVE_RECURSE "${BUILD_DIR}")
if(EXISTS "${BUILD_DIR}")
    message(FATAL_ERROR
        "preexisting_msquic_target: could not remove ${BUILD_DIR}")
endif()
file(MAKE_DIRECTORY "${BUILD_DIR}")

# Define a real imported msquic::msquic BEFORE the project's find_package(MsQuic)
# runs, which is exactly the situation FindMsQuic's first branch exists for.
set(inject "${BUILD_DIR}/preexisting-init.cmake")
file(WRITE "${inject}"
"add_library(msquic::msquic UNKNOWN IMPORTED)\n"
"set_target_properties(msquic::msquic PROPERTIES\n"
"    IMPORTED_LOCATION \"${MSQUIC_LIBRARY}\"\n"
"    INTERFACE_INCLUDE_DIRECTORIES \"${MSQUIC_INCLUDE_DIR}\")\n")

execute_process(
    COMMAND "${CMAKE_CMD}" -S "${SOURCE_DIR}" -B "${BUILD_DIR}"
            -DMOQ_BUILD_ADAPTER_MSQUIC=ON
            -DMOQ_BUILD_MSQUIC_MANAGED=ON
            "-DCMAKE_PROJECT_TOP_LEVEL_INCLUDES=${inject}"
    RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "preexisting_msquic_target: configure with a preexisting "
        "msquic::msquic target FAILED (rc=${rc}). This branch is supported and "
        "must configure cleanly.\n--- stdout ---\n${out}\n--- stderr ---\n${err}")
endif()

# The recorded mode must be the preexisting branch. It is a normal result
# variable, so it is read from the configure log the branch prints rather than
# from the cache -- see the status line emitted by adapters/msquic.
if(NOT out MATCHES "MsQuic discovery mode: preexisting")
    message(FATAL_ERROR
        "preexisting_msquic_target: the configure did not report the "
        "preexisting discovery branch.\n--- stdout ---\n${out}")
endif()

# The child must have left a cache behind: without one the configure did not
# really run, and every expectation below would be read from a listing that
# means nothing.
if(NOT EXISTS "${BUILD_DIR}/CMakeCache.txt")
    message(FATAL_ERROR
        "preexisting_msquic_target: the child configure left no cache at "
        "${BUILD_DIR}/CMakeCache.txt")
endif()
execute_process(
    COMMAND "${CTEST_CMD}" --test-dir "${BUILD_DIR}" --show-only=json-v1
    RESULT_VARIABLE rc OUTPUT_VARIABLE listing ERROR_VARIABLE err)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "preexisting_msquic_target: ctest --show-only=json-v1 failed "
        "(rc=${rc})\n--- stdout ---\n${listing}\n--- stderr ---\n${err}")
endif()
string(JSON tests_type ERROR_VARIABLE jerr TYPE "${listing}" tests)
if(jerr OR NOT tests_type STREQUAL "ARRAY")
    message(FATAL_ERROR
        "preexisting_msquic_target: listing has no `tests` ARRAY "
        "(error='${jerr}')\n--- ctest output ---\n${listing}")
endif()
string(JSON n_tests ERROR_VARIABLE jerr LENGTH "${listing}" tests)
if(jerr OR n_tests EQUAL 0)
    message(FATAL_ERROR
        "preexisting_msquic_target: no tests registered at all; an empty set "
        "would make every absence check below vacuous (error='${jerr}')")
endif()
set(names "")
math(EXPR last "${n_tests} - 1")
foreach(i RANGE ${last})
    string(JSON nm ERROR_VARIABLE jerr GET "${listing}" tests ${i} name)
    if(jerr OR nm STREQUAL "")
        message(FATAL_ERROR
            "preexisting_msquic_target: tests[${i}] has no usable name "
            "(error='${jerr}')")
    endif()
    list(APPEND names "${nm}")
endforeach()

# 1. the inapplicable nested-configure test is ABSENT, not failing
if("${SELF_TEST_NAME}" IN_LIST names)
    message(FATAL_ERROR
        "preexisting_msquic_target: ${SELF_TEST_NAME} is registered in "
        "preexisting mode, where its child cannot be given the parent's "
        "target. It must be omitted, not registered to fail.")
endif()

# 2. the tests that need no certificate are still there, in BOTH capability
#    states -- omitting the nested-configure test must not take the rest of the
#    MsQuic suite with it
foreach(want msquic_public_compile msquic_unit msquic_settings)
    if(NOT "${want}" IN_LIST names)
        message(FATAL_ERROR
            "preexisting_msquic_target: ${want} is missing. Omitting the "
            "nested-configure test must not take the rest of the MsQuic suite "
            "with it.\n  registered: ${names}")
    endif()
endforeach()

# 3. the certificate-consuming tests are unconditional: the loopback identity
#    is committed to the source tree, so this branch must register them like
#    any other. There is no capability state to follow any more.
set(cert_tests msquic_recv_loopback msquic_over_window_credit msquic_loopback)
foreach(want IN LISTS cert_tests)
    if(NOT "${want}" IN_LIST names)
        message(FATAL_ERROR
            "preexisting_msquic_target: ${want} must be registered.\n"
            "  registered: ${names}")
    endif()
endforeach()

# 4. an injected target outranks an explicit pin
# Target injection is priority zero: the caller has not asked the project to
# find MsQuic, they have handed it one. A pin is an instruction about how to
# FIND it, so it does not apply. Pinning this precedence keeps it from drifting
# now that a pin exists to compete with it.
set(pin_build "${BUILD_DIR}-with-pin")
file(REMOVE_RECURSE "${pin_build}")
file(MAKE_DIRECTORY "${pin_build}")
execute_process(
    COMMAND "${CMAKE_CMD}" -S "${SOURCE_DIR}" -B "${pin_build}"
            -DMOQ_BUILD_ADAPTER_MSQUIC=ON
            -DMOQ_BUILD_MSQUIC_MANAGED=ON
            -DMOQ_MSQUIC_REGISTRATION_CHILD=ON
            "-DCMAKE_PROJECT_TOP_LEVEL_INCLUDES=${inject}"
            "-DMOQ_MSQUIC_PIN_INCLUDE_DIR=${MSQUIC_INCLUDE_DIR}"
            "-DMOQ_MSQUIC_PIN_LIBRARY=${MSQUIC_LIBRARY}"
    RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "preexisting_msquic_target: a preexisting target combined with a pin "
        "must configure (rc=${rc}).\n--- stdout ---\n${out}\n"
        "--- stderr ---\n${err}")
endif()
if(NOT out MATCHES "MsQuic discovery mode: preexisting")
    message(FATAL_ERROR
        "preexisting_msquic_target: a pin displaced the caller's injected "
        "msquic::msquic target. Target injection is priority zero.\n"
        "--- stdout ---\n${out}")
endif()
file(REMOVE_RECURSE "${pin_build}")

file(REMOVE_RECURSE "${BUILD_DIR}")
message(STATUS
    "preexisting_msquic_target: ${n_tests} tests, nested-configure regression "
    "correctly omitted, and an injected target outranks a pin")
