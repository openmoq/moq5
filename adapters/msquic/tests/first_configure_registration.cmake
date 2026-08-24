# Configure-order regression: one fresh configure must register every test.
#
# A guard that reads a find_program() result BEFORE the find_program() runs is
# invisible to any build directory that has already been configured once: the
# first pass silently drops the test, and the cache makes every later pass look
# correct. So this fixture cannot inspect the build tree it is running in. It
# configures a CHILD build in a directory it deletes first, and compares the
# CTest name set after the child's FIRST configure against the set after an
# immediate identical SECOND configure.
#
# Comparison is exact sorted-list equality. A sorted list preserves
# MULTIPLICITY, so a duplicate registration that appears in one pass and not the
# other is a difference; the mismatch diagnostic prints both full lists rather
# than a set subtraction, because subtraction cannot express "the same name,
# twice".
#
# DISCOVERY MODE IS READ FROM THE MODULE THAT RESOLVED IT, and the child's
# resulting dependency IDENTITY is then validated from its own cache before any
# CTest query. A mode inferred from a cache HINT is not proof: msquic_DIR can be
# stale while package discovery is disabled and the checkout fallback is what
# actually ran, and a child sent off with that stale hint silently resolves
# whatever ambient package it can find -- passing while testing a different
# dependency than its parent.
#
# DISCOVERY MODE IS AN EXPLICIT CONTRACT, not a guess. The project finds MsQuic
# three documented ways (cmake/FindMsQuic.cmake): an explicit artifact pin, an
# installed msquic CONFIG package, or a checkout root. The pin is honoured
# first and the config path RETURNS before MOQ_MSQUIC_ROOT is ever set, so a
# child told only "use this root" cannot reproduce a parent that used either of
# the others. The parent therefore states which mode it used and the child is
# pinned to exactly that one -- never left to discover whichever dependency
# happens to win.
#
# `pinned` is what an MsQuic built OUTSIDE its checkout needs: there is no root
# whose build/bin holds the library, so the header and library are named
# directly. Forwarding them is not a convenience -- a child given only a root
# cannot configure at all against such a build.
#
# The name set comes from CTest's STRUCTURED output (--show-only=json-v1),
# parsed with string(JSON) -- both within the project's declared CMake 3.20
# floor. CTest's human listing is documented as unstable, so it is not a
# boundary worth hardening; a schema-checked document is.
#
# Every step is fail-closed. A failed configure, a failed CTest query, a
# document that is not valid JSON, a missing or wrong-typed `tests` array, a
# non-object entry, a missing/wrong-typed/empty `name`, or an empty set is a
# FAILURE -- never an empty or partial set that happens to compare equal to
# another.
#
# The child is configured with MOQ_MSQUIC_REGISTRATION_CHILD=ON so it does not
# register this fixture again. The fixture asserts that suppression actually
# took effect, so deleting or inverting it fails here rather than nesting.

cmake_minimum_required(VERSION 3.20)

foreach(_v SOURCE_DIR BUILD_DIR CMAKE_CMD CTEST_CMD MSQUIC_MODE
           EXPECT_RECEIVE_REGISTRATIONS SELF_TEST_NAME)
    if(NOT DEFINED ${_v})
        message(FATAL_ERROR "first_configure_registration: ${_v} not provided")
    endif()
endforeach()

# -- pin the child to the parent's ACTUAL MsQuic discovery mode --------------
if(MSQUIC_MODE STREQUAL "pinned")
    if(NOT DEFINED MSQUIC_INCLUDE_DIR OR MSQUIC_INCLUDE_DIR STREQUAL ""
       OR NOT DEFINED MSQUIC_LIBRARY OR MSQUIC_LIBRARY STREQUAL "")
        message(FATAL_ERROR
            "first_configure_registration: MSQUIC_MODE=pinned but the parent "
            "did not forward both artifacts.\n"
            "  MSQUIC_INCLUDE_DIR: '${MSQUIC_INCLUDE_DIR}'\n"
            "  MSQUIC_LIBRARY:     '${MSQUIC_LIBRARY}'")
    endif()
    # The pin variables are inputs, so forwarding them is all the child needs;
    # its own MOQ_MSQUIC_INCLUDE_DIR/LIBRARY are results it will derive. Package
    # discovery is disabled anyway, so a failure to honour the pin shows up here
    # as a wrong identity rather than as a package quietly standing in.
    set(MSQUIC_ARGS
        "-DCMAKE_DISABLE_FIND_PACKAGE_msquic=TRUE"
        "-DMOQ_MSQUIC_PIN_INCLUDE_DIR:PATH=${MSQUIC_INCLUDE_DIR}"
        "-DMOQ_MSQUIC_PIN_LIBRARY:FILEPATH=${MSQUIC_LIBRARY}")
elseif(MSQUIC_MODE STREQUAL "package")
    if(NOT DEFINED MSQUIC_DIR OR MSQUIC_DIR STREQUAL "")
        message(FATAL_ERROR
            "first_configure_registration: MSQUIC_MODE=package but no "
            "MSQUIC_DIR was forwarded")
    endif()
    # Pin the exact package directory; do NOT disable the package search, since
    # the package IS the parent's dependency.
    set(MSQUIC_ARGS "-Dmsquic_DIR=${MSQUIC_DIR}")
elseif(MSQUIC_MODE STREQUAL "root")
    if(NOT DEFINED MSQUIC_ROOT OR MSQUIC_ROOT STREQUAL "")
        message(FATAL_ERROR
            "first_configure_registration: MSQUIC_MODE=root but no "
            "MSQUIC_ROOT was forwarded")
    endif()
    # Pin the exact checkout and refuse an ambient package, so the child cannot
    # silently resolve a different dependency than the parent did.
    set(MSQUIC_ARGS
        "-DCMAKE_DISABLE_FIND_PACKAGE_msquic=TRUE"
        "-DMOQ_MSQUIC_ROOT=${MSQUIC_ROOT}")
else()
    message(FATAL_ERROR
        "first_configure_registration: MSQUIC_MODE is '${MSQUIC_MODE}', which "
        "is not a branch this fixture can reproduce. It must be the value "
        "FindMsQuic.cmake recorded for the branch that actually resolved "
        "MsQuic ('package' or 'root').")
endif()


# -- fresh child directory, proven fresh -------------------------------------
# Reusing a preconfigured directory is exactly what hides the defect, so the
# removal is verified rather than assumed.
file(REMOVE_RECURSE "${BUILD_DIR}")
if(EXISTS "${BUILD_DIR}")
    message(FATAL_ERROR
        "first_configure_registration: could not remove ${BUILD_DIR}; "
        "refusing to run against a directory that may already be configured")
endif()

function(ar_configure_child pass_label out_text)
    execute_process(
        COMMAND "${CMAKE_CMD}" -S "${SOURCE_DIR}" -B "${BUILD_DIR}"
                -DMOQ_BUILD_ADAPTER_MSQUIC=ON
                -DMOQ_BUILD_MSQUIC_MANAGED=ON
                ${MSQUIC_ARGS}
                -DMOQ_MSQUIC_REGISTRATION_CHILD=ON
        RESULT_VARIABLE rc
        OUTPUT_VARIABLE out
        ERROR_VARIABLE err)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR
            "first_configure_registration: ${pass_label} configure failed "
            "(rc=${rc})\n--- stdout ---\n${out}\n--- stderr ---\n${err}")
    endif()
    string(REGEX REPLACE "[ \t\r\n]+" " " flat "${out}\n${err}")
    set(${out_text} "${flat}" PARENT_SCOPE)
endfunction()

# Returns the sorted CTest name list, read from the structured listing. Every
# element is schema-checked, so a document that is valid JSON but not the shape
# we expect fails instead of yielding a short list that could compare equal to
# another short list.
function(ar_test_names pass_label out_var)
    execute_process(
        COMMAND "${CTEST_CMD}" --test-dir "${BUILD_DIR}" --show-only=json-v1
        RESULT_VARIABLE rc
        OUTPUT_VARIABLE out
        ERROR_VARIABLE err)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR
            "first_configure_registration: ${pass_label} ctest "
            "--show-only=json-v1 failed (rc=${rc})\n"
            "--- stdout ---\n${out}\n--- stderr ---\n${err}")
    endif()

    # Top level must be a JSON object.
    string(JSON doc_type ERROR_VARIABLE jerr TYPE "${out}")
    if(jerr OR NOT doc_type STREQUAL "OBJECT")
        message(FATAL_ERROR
            "first_configure_registration: ${pass_label} listing is not a JSON "
            "object (type='${doc_type}' error='${jerr}').\n"
            "--- ctest output ---\n${out}")
    endif()

    # `tests` must be present and an array.
    string(JSON tests_type ERROR_VARIABLE jerr TYPE "${out}" tests)
    if(jerr OR NOT tests_type STREQUAL "ARRAY")
        message(FATAL_ERROR
            "first_configure_registration: ${pass_label} listing has no "
            "`tests` ARRAY (type='${tests_type}' error='${jerr}').\n"
            "--- ctest output ---\n${out}")
    endif()

    string(JSON n_tests ERROR_VARIABLE jerr LENGTH "${out}" tests)
    if(jerr)
        message(FATAL_ERROR
            "first_configure_registration: ${pass_label} could not measure the "
            "`tests` array: ${jerr}\n--- ctest output ---\n${out}")
    endif()

    set(names "")
    if(n_tests GREATER 0)
        math(EXPR last "${n_tests} - 1")
        foreach(i RANGE ${last})
            string(JSON item_type ERROR_VARIABLE jerr TYPE "${out}" tests ${i})
            if(jerr OR NOT item_type STREQUAL "OBJECT")
                message(FATAL_ERROR
                    "first_configure_registration: ${pass_label} tests[${i}] is "
                    "not an OBJECT (type='${item_type}' error='${jerr}').\n"
                    "--- ctest output ---\n${out}")
            endif()
            string(JSON name_type ERROR_VARIABLE jerr TYPE "${out}"
                   tests ${i} name)
            if(jerr OR NOT name_type STREQUAL "STRING")
                message(FATAL_ERROR
                    "first_configure_registration: ${pass_label} tests[${i}] "
                    "has no STRING `name` (type='${name_type}' "
                    "error='${jerr}').\n--- ctest output ---\n${out}")
            endif()
            string(JSON nm ERROR_VARIABLE jerr GET "${out}" tests ${i} name)
            if(jerr OR nm STREQUAL "")
                message(FATAL_ERROR
                    "first_configure_registration: ${pass_label} tests[${i}] "
                    "has an empty name (error='${jerr}').\n"
                    "--- ctest output ---\n${out}")
            endif()
            list(APPEND names "${nm}")
        endforeach()
    endif()

    # Every array element yielded exactly one name.
    list(LENGTH names parsed)
    if(NOT parsed EQUAL n_tests)
        message(FATAL_ERROR
            "first_configure_registration: ${pass_label} read ${parsed} names "
            "from a `tests` array of ${n_tests}.\n"
            "--- ctest output ---\n${out}")
    endif()

    if(names STREQUAL "")
        message(FATAL_ERROR
            "first_configure_registration: ${pass_label} found NO tests. "
            "An empty set must never compare equal.\n"
            "--- ctest output ---\n${out}")
    endif()

    list(SORT names)
    set(${out_var} "${names}" PARENT_SCOPE)
endfunction()

# Read one cache entry from the child. Fails closed: a missing entry, or the
# same key appearing more than once, is an error rather than an empty string
# that would silently compare equal to another empty string.
function(ar_cache_get key out_var)
    set(cache_file "${BUILD_DIR}/CMakeCache.txt")
    if(NOT EXISTS "${cache_file}")
        message(FATAL_ERROR
            "first_configure_registration: no ${cache_file} to validate the "
            "child's dependency identity against")
    endif()
    file(STRINGS "${cache_file}" hits REGEX "^${key}:[^=]*=")
    list(LENGTH hits n)
    if(n EQUAL 0)
        file(READ "${cache_file}" cache_text)
        message(FATAL_ERROR
            "first_configure_registration: child cache has no '${key}'.\n"
            "--- child CMakeCache.txt ---\n${cache_text}")
    elseif(NOT n EQUAL 1)
        message(FATAL_ERROR
            "first_configure_registration: child cache has ${n} entries for "
            "'${key}', which is contradictory: ${hits}")
    endif()
    list(GET hits 0 line)
    string(REGEX REPLACE "^${key}:[^=]*=" "" val "${line}")
    set(${out_var} "${val}" PARENT_SCOPE)
endfunction()

# Compare two paths as paths, not as strings: resolve both before comparing so a
# symlinked or non-normalized spelling is not reported as a mismatch. An empty
# expectation is refused rather than matching everything.
function(ar_same_path label got want)
    if(want STREQUAL "")
        message(FATAL_ERROR
            "first_configure_registration: ${label}: nothing to compare "
            "against -- the parent forwarded an empty expected path")
    endif()
    get_filename_component(g "${got}" REALPATH)
    get_filename_component(w "${want}" REALPATH)
    if(NOT g STREQUAL w)
        message(FATAL_ERROR
            "first_configure_registration: ${label} identity mismatch.\n"
            "  child resolved: ${got}\n"
            "     (real path)  ${g}\n"
            "  parent used:    ${want}\n"
            "     (real path)  ${w}\n"
            "The child must exercise the parent's dependency, not whichever "
            "one its own search happens to find.")
    endif()
endfunction()

# Is `path` inside `root`? Both are resolved first.
function(ar_path_under label path root)
    get_filename_component(p "${path}" REALPATH)
    get_filename_component(r "${root}" REALPATH)
    string(FIND "${p}" "${r}/" pos)
    if(NOT pos EQUAL 0)
        message(FATAL_ERROR
            "first_configure_registration: ${label} does not belong to the "
            "parent's MsQuic root.\n  resolved: ${p}\n  root:     ${r}")
    endif()
endfunction()

# After a child configure, prove it resolved the SAME dependency the parent did.
# Read the child's machine-readable discovery record. Paths are never parsed
# out of the human status text: that stream is wrapped and space-separated, so
# any path containing a space would be silently truncated and the identity
# check would fail on a configure that was perfectly correct.
function(ar_discovery_record pass_label)
    set(record "${BUILD_DIR}/msquic-discovery.cmake")
    if(NOT EXISTS "${record}")
        message(FATAL_ERROR
            "first_configure_registration: ${pass_label} child left no "
            "discovery record at ${record}")
    endif()
    include("${record}")
    foreach(_v MOQ_MSQUIC_DISCOVERY_MODE MOQ_MSQUIC_OWNS_PAIR
               MOQ_MSQUIC_INCLUDE_DIR MOQ_MSQUIC_LIBRARY)
        set(${_v} "${${_v}}" PARENT_SCOPE)
    endforeach()
endfunction()

function(ar_check_identity pass_label text)
    if(MSQUIC_MODE STREQUAL "pinned")
        ar_cache_get("CMAKE_DISABLE_FIND_PACKAGE_msquic" child_disable)
        if(NOT child_disable)
            message(FATAL_ERROR
                "first_configure_registration: ${pass_label} pinned mode but "
                "the child left package discovery enabled "
                "(CMAKE_DISABLE_FIND_PACKAGE_msquic='${child_disable}'), so an "
                "ambient package could have shadowed the pin")
        endif()
        # The pair IS the identity here: there is no root to fall back on, so
        # both halves are compared against exactly what the parent used. They
        # are read from the child's configure log rather than its cache: a
        # pinned tree publishes its results in configure scope so that clearing
        # the pin cannot leave them behind.
        ar_discovery_record("${pass_label}")
        if(NOT MOQ_MSQUIC_DISCOVERY_MODE STREQUAL "pinned")
            message(FATAL_ERROR
                "first_configure_registration: ${pass_label} child recorded "
                "mode '${MOQ_MSQUIC_DISCOVERY_MODE}', not 'pinned'")
        endif()
        set(child_inc "${MOQ_MSQUIC_INCLUDE_DIR}")
        set(child_lib "${MOQ_MSQUIC_LIBRARY}")
        # Preseeding the RESULT variables still reaches the pin branch, by way
        # of the legacy migration. That is for other people's build scripts,
        # not for this project's own harness: forwarding the deprecated
        # spelling here would test the compatibility path instead of the
        # contract, and would go on passing after the compatibility path is
        # eventually removed.
        string(FIND "${text}" "switch to MOQ_MSQUIC_PIN_INCLUDE_DIR" _dep)
        if(NOT _dep EQUAL -1)
            message(FATAL_ERROR
                "first_configure_registration: ${pass_label} child reached the "
                "pin through the deprecated result-variable spelling. The "
                "fixture must forward MOQ_MSQUIC_PIN_* directly.\n"
                "--- child output ---\n${text}")
        endif()
        ar_same_path("${pass_label} include dir" "${child_inc}"
                     "${MSQUIC_INCLUDE_DIR}")
        ar_same_path("${pass_label} library" "${child_lib}" "${MSQUIC_LIBRARY}")
        # And that the child reached them through the pin branch rather than
        # arriving at the same paths by searching: the pin inputs it was given
        # must be the ones it published as results.
        ar_cache_get("MOQ_MSQUIC_PIN_INCLUDE_DIR" child_pin_inc)
        ar_cache_get("MOQ_MSQUIC_PIN_LIBRARY" child_pin_lib)
        ar_same_path("${pass_label} pin include dir" "${child_pin_inc}"
                     "${MSQUIC_INCLUDE_DIR}")
        ar_same_path("${pass_label} pin library" "${child_pin_lib}"
                     "${MSQUIC_LIBRARY}")
    elseif(MSQUIC_MODE STREQUAL "package")
        ar_cache_get("msquic_DIR" child_pkg)
        ar_same_path("${pass_label} msquic_DIR" "${child_pkg}" "${MSQUIC_DIR}")
    else()
        ar_cache_get("CMAKE_DISABLE_FIND_PACKAGE_msquic" child_disable)
        if(NOT child_disable)
            message(FATAL_ERROR
                "first_configure_registration: ${pass_label} root mode but the "
                "child left package discovery enabled "
                "(CMAKE_DISABLE_FIND_PACKAGE_msquic='${child_disable}'), so it "
                "may have resolved an ambient package instead of the root")
        endif()
        ar_cache_get("MOQ_MSQUIC_ROOT" child_root)
        ar_same_path("${pass_label} MOQ_MSQUIC_ROOT" "${child_root}"
                     "${MSQUIC_ROOT}")
        # The root can be right while the resolved artifacts came from
        # somewhere else, so both are checked against it.
        ar_cache_get("MOQ_MSQUIC_INCLUDE_DIR" child_inc)
        ar_cache_get("MOQ_MSQUIC_LIBRARY" child_lib)
        ar_path_under("${pass_label} MOQ_MSQUIC_INCLUDE_DIR" "${child_inc}"
                      "${MSQUIC_ROOT}")
        ar_path_under("${pass_label} MOQ_MSQUIC_LIBRARY" "${child_lib}"
                      "${MSQUIC_ROOT}")
        if(DEFINED MSQUIC_INCLUDE_DIR AND NOT MSQUIC_INCLUDE_DIR STREQUAL "")
            ar_same_path("${pass_label} include dir" "${child_inc}"
                         "${MSQUIC_INCLUDE_DIR}")
        endif()
        if(DEFINED MSQUIC_LIBRARY AND NOT MSQUIC_LIBRARY STREQUAL "")
            ar_same_path("${pass_label} library" "${child_lib}"
                         "${MSQUIC_LIBRARY}")
        endif()
    endif()

endfunction()

ar_configure_child("first" first_log)
ar_check_identity("first" "${first_log}")
ar_test_names("first" first_names)

ar_configure_child("second" second_log)
ar_check_identity("second" "${second_log}")
ar_test_names("second" second_names)

# -- 1. the two passes must agree exactly, multiplicity included -------------
if(NOT first_names STREQUAL second_names)
    list(LENGTH first_names n_first)
    list(LENGTH second_names n_second)
    # A set subtraction names the usual case directly, but it reports NOTHING
    # when the only difference is how many times a name appears. Both full
    # sorted lists are therefore printed too, and they are authoritative.
    set(second_only "${second_names}")
    list(REMOVE_ITEM second_only ${first_names})
    set(first_only "${first_names}")
    list(REMOVE_ITEM first_only ${second_names})
    message(FATAL_ERROR
        "first_configure_registration: the first configure did not register "
        "the same tests as an immediate second configure.\n"
        "  registered only by the SECOND pass: ${second_only}\n"
        "  registered only by the FIRST pass:  ${first_only}\n"
        "  (a duplicate-only difference shows in neither line above; the full "
        "sorted lists are authoritative)\n"
        "  FIRST pass  (${n_first}): ${first_names}\n"
        "  SECOND pass (${n_second}): ${second_names}\n"
        "A guard that reads a cache entry its own find_program() has not yet "
        "written produces exactly this.")
endif()

list(LENGTH first_names n_first)

# -- 2. the certificate-consuming registrations, present from pass one -------
# The loopback identity is COMMITTED, so nothing about these registrations is
# conditional on a tool being discovered: a single fresh configure must
# already carry both receive cells. A guard that read a cache entry its own
# discovery had not yet written is exactly what drops one of them here.
if(EXPECT_RECEIVE_REGISTRATIONS)
    foreach(_want msquic_recv_loopback msquic_over_window_credit)
        if(NOT "${_want}" IN_LIST first_names)
            message(FATAL_ERROR
                "first_configure_registration: a single fresh configure must "
                "already register ${_want}, but it is absent from the first "
                "pass.\n  first-pass names: ${first_names}")
        endif()
    endforeach()
endif()

# -- 3. the recursion suppression really suppressed --------------------------
if("${SELF_TEST_NAME}" IN_LIST first_names)
    message(FATAL_ERROR
        "first_configure_registration: the child registered this fixture "
        "(${SELF_TEST_NAME}) despite MOQ_MSQUIC_REGISTRATION_CHILD=ON, so the "
        "suppression is not load-bearing")
endif()

file(REMOVE_RECURSE "${BUILD_DIR}")
message(STATUS
    "first_configure_registration: ${n_first} tests (mode=${MSQUIC_MODE}), "
    "identical across both configures")
