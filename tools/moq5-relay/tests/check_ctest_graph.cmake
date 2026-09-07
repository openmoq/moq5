# Authority over the GENERATED CTest graph, not over source text.
#
# Matching `add_test(...)` text proves only that a line exists. It does not
# prove CMake evaluated it: wrapping the call in `if(FALSE)` removes the test
# from the graph entirely, and `DISABLED TRUE` leaves it registered but never
# executed. Either way a full `ctest` run succeeds with the authority absent.
#
# This reads CTest's own machine-readable model of what it will run, so the
# question asked is "does this test exist, is it enabled, and what exact command
# will be executed" rather than "does this text appear somewhere".
#
# Commands are compared element-for-element, including length and order. A
# containment test is not identity: an executable named `not-the-cmake-runner`
# contains `cmake`, and a path named `not-the-test_relay_snapshot_cap` contains
# the binary name, so a substring rule accepts a graph in which neither intended
# program ever runs. Every expected element is therefore supplied explicitly by
# CMake rather than inferred here from a basename pattern.
#
# This script also audits its own entry. That is necessary but not sufficient:
# a test cannot prove it was not removed from CTest. The trust root for that is
# the configure-time assertion at the end of tools/moq5-relay/CMakeLists.txt,
# which runs before CTest exists and is itself pinned by check_pump_overlap.sh.
#
# Parsed with CMake's own string(JSON), so the check needs no interpreter
# beyond the CMake already required to build.

foreach(v MOQR_BUILD_DIR MOQR_CTEST MOQR_CMAKE MOQR_PROD_BIN MOQR_ORACLE
          MOQR_SELFTEST_DRIVER MOQR_GRAPH_SCRIPT MOQR_PUMP_GUARD
          MOQR_PUMP_SUBJECT MOQR_CMAKELISTS)
    if(NOT DEFINED ${v})
        message(FATAL_ERROR "FAIL: ${v} was not provided")
    endif()
endforeach()
foreach(v MOQR_EXPECT_SELFTEST MOQR_BASH)
    if(NOT DEFINED ${v})
        message(FATAL_ERROR "FAIL: ${v} was not provided")
    endif()
endforeach()

execute_process(
    COMMAND "${MOQR_CTEST}" --test-dir "${MOQR_BUILD_DIR}" --show-only=json-v1
    OUTPUT_VARIABLE model
    ERROR_VARIABLE model_err
    RESULT_VARIABLE model_status)
if(NOT model_status EQUAL 0)
    message(FATAL_ERROR "FAIL: could not read the CTest model\n${model_err}")
endif()

set(problems "")

# Collect every entry for a name, so a duplicate registration is visible.
function(collect_test name out_count out_cmd out_disabled)
    string(JSON n ERROR_VARIABLE nerr LENGTH "${model}" tests)
    set(count 0)
    set(cmd "")
    set(disabled "FALSE")
    # An empty or absent test array is a legitimate answer -- it means the test
    # is not in the graph at all, which is exactly one of the states this check
    # exists to report. It must read as a clean failure, never a crash.
    if(NOT n GREATER 0)
        set(${out_count} 0 PARENT_SCOPE)
        set(${out_cmd} "" PARENT_SCOPE)
        set(${out_disabled} "FALSE" PARENT_SCOPE)
        return()
    endif()
    math(EXPR last "${n} - 1")
    foreach(i RANGE 0 ${last})
        string(JSON entry GET "${model}" tests ${i})
        string(JSON tname GET "${entry}" name)
        if(NOT tname STREQUAL name)
            continue()
        endif()
        math(EXPR count "${count} + 1")
        string(JSON cn ERROR_VARIABLE cerr LENGTH "${entry}" command)
        set(parts "")
        if(cn GREATER 0)
            math(EXPR clast "${cn} - 1")
            foreach(c RANGE 0 ${clast})
                string(JSON part GET "${entry}" command ${c})
                list(APPEND parts "${part}")
            endforeach()
        endif()
        set(cmd "${parts}")
        # DISABLED is a property; absent means enabled.
        string(JSON pn ERROR_VARIABLE perr LENGTH "${entry}" properties)
        if(pn GREATER 0)
            math(EXPR plast "${pn} - 1")
            foreach(pi RANGE 0 ${plast})
                string(JSON pname GET "${entry}" properties ${pi} name)
                if(pname STREQUAL "DISABLED")
                    string(JSON pval GET "${entry}" properties ${pi} value)
                    if(pval)
                        set(disabled "TRUE")
                    endif()
                endif()
            endforeach()
        endif()
    endforeach()
    set(${out_count} "${count}" PARENT_SCOPE)
    set(${out_cmd} "${cmd}" PARENT_SCOPE)
    set(${out_disabled} "${disabled}" PARENT_SCOPE)
endfunction()

# Exact array identity: same length, same elements, same order. Missing, extra,
# reordered, lookalike, prefixed and suffixed elements all fail here.
function(require_exact_command label actual expected)
    list(LENGTH actual alen)
    list(LENGTH expected elen)
    if(NOT alen EQUAL elen)
        string(APPEND problems
               "  ${label} command has ${alen} element(s), expected ${elen}\n    actual:   ${actual}\n    expected: ${expected}\n")
        set(problems "${problems}" PARENT_SCOPE)
        return()
    endif()
    if(alen EQUAL 0)
        set(problems "${problems}" PARENT_SCOPE)
        return()
    endif()
    math(EXPR last "${alen} - 1")
    foreach(i RANGE 0 ${last})
        list(GET actual ${i} a)
        list(GET expected ${i} e)
        if(NOT a STREQUAL e)
            string(APPEND problems
                   "  ${label} command element ${i} is not the intended one\n    actual:   ${a}\n    expected: ${e}\n")
        endif()
    endforeach()
    set(problems "${problems}" PARENT_SCOPE)
endfunction()

function(require_present label count disabled)
    if(NOT count EQUAL 1)
        string(APPEND problems
               "  ${label} appears ${count} time(s) in the configured graph, expected 1\n")
    endif()
    if(disabled)
        string(APPEND problems "  ${label} is DISABLED in the configured graph\n")
    endif()
    set(problems "${problems}" PARENT_SCOPE)
endfunction()

# -- the production receipt test --------------------------------------------
collect_test("relay_snapshot_cap" prod_count prod_cmd prod_disabled)
require_present("relay_snapshot_cap" "${prod_count}" "${prod_disabled}")
if(prod_count EQUAL 1)
    set(prod_expected
        "${MOQR_CMAKE}"
        "-DMOQR_BIN=${MOQR_PROD_BIN}"
        "-P"
        "${MOQR_ORACLE}")
    require_exact_command("relay_snapshot_cap" "${prod_cmd}" "${prod_expected}")
endif()

# -- the graph authority's own entry ----------------------------------------
# Necessary but not sufficient on its own; see the header note.
collect_test("relay_ctest_graph" graph_count graph_cmd graph_disabled)
require_present("relay_ctest_graph" "${graph_count}" "${graph_disabled}")
if(graph_count EQUAL 1)
    set(graph_expected
        "${MOQR_CMAKE}"
        "-DMOQR_BUILD_DIR=${MOQR_BUILD_DIR}"
        "-DMOQR_CTEST=${MOQR_CTEST}"
        "-DMOQR_CMAKE=${MOQR_CMAKE}"
        "-DMOQR_PROD_BIN=${MOQR_PROD_BIN}"
        "-DMOQR_ORACLE=${MOQR_ORACLE}"
        "-DMOQR_SELFTEST_DRIVER=${MOQR_SELFTEST_DRIVER}"
        "-DMOQR_GRAPH_SCRIPT=${MOQR_GRAPH_SCRIPT}"
        "-DMOQR_PUMP_GUARD=${MOQR_PUMP_GUARD}"
        "-DMOQR_PUMP_SUBJECT=${MOQR_PUMP_SUBJECT}"
        "-DMOQR_CMAKELISTS=${MOQR_CMAKELISTS}"
        "-DMOQR_BASH=${MOQR_BASH}"
        "-DMOQR_EXPECT_SELFTEST=${MOQR_EXPECT_SELFTEST}"
        "-P"
        "${MOQR_GRAPH_SCRIPT}")
    require_exact_command("relay_ctest_graph" "${graph_cmd}" "${graph_expected}")
endif()

# -- the oracle self-test ----------------------------------------------------
collect_test("relay_snapshot_cap_oracle" self_count self_cmd self_disabled)
if(MOQR_EXPECT_SELFTEST)
    require_present("relay_snapshot_cap_oracle" "${self_count}" "${self_disabled}")
    if(self_count EQUAL 1)
        set(self_expected
            "${MOQR_BASH}"
            "${MOQR_SELFTEST_DRIVER}"
            "${MOQR_ORACLE}"
            "${MOQR_CMAKE}")
        require_exact_command("relay_snapshot_cap_oracle" "${self_cmd}" "${self_expected}")
    endif()
else()
    # The stated platform boundary: without a shell the self-test is absent, but
    # the production check and this authority must still be present and enabled.
    if(NOT self_count EQUAL 0)
        string(APPEND problems
               "  relay_snapshot_cap_oracle is registered ${self_count} time(s) with no shell expected\n")
    endif()
endif()

# -- the source pin ----------------------------------------------------------
# Requiring this test by name authenticates only its name. Rewriting its command
# to a no-op leaves the pin nominally registered, enabled, and inert -- so the
# command is authenticated here exactly as the other three are. The pin's
# independent authority is the configure-time run in CMakeLists.txt; this check
# is what keeps the graph honest about how CTest invokes it.
collect_test("relay_pump_overlap" pump_count pump_cmd pump_disabled)
if(MOQR_EXPECT_SELFTEST)
    require_present("relay_pump_overlap" "${pump_count}" "${pump_disabled}")
    if(pump_count EQUAL 1)
        set(pump_expected
            "${MOQR_BASH}"
            "${MOQR_PUMP_GUARD}"
            "${MOQR_PUMP_SUBJECT}"
            "${MOQR_CMAKELISTS}")
        require_exact_command("relay_pump_overlap" "${pump_cmd}" "${pump_expected}")
    endif()
else()
    if(NOT pump_count EQUAL 0)
        string(APPEND problems
               "  relay_pump_overlap is registered ${pump_count} time(s) with no shell expected\n")
    endif()
endif()

if(NOT problems STREQUAL "")
    message(FATAL_ERROR "FAIL: configured CTest graph\n${problems}")
endif()
message("PASS: relay_snapshot_cap, relay_ctest_graph, the oracle self-test and the source pin are registered once, enabled, and run exactly the intended commands")
