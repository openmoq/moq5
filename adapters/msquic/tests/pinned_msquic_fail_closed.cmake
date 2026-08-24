# The pinned-MsQuic contract: what it must accept, and what it must refuse.
#
# A pin exists because an MsQuic built outside its checkout has no root to
# point at. Two things then have to hold, and each fails in a way that looks
# like success:
#
#   - A pin must WIN, on a fresh tree and on a reconfigure alike. A pin that
#     loses to whatever the tree already resolved configures happily against
#     the wrong dependency.
#   - A near-miss pin must be REFUSED. A missing half, or a path that is a
#     directory where a file belongs, otherwise resolves some other MsQuic --
#     or pairs a header from one build with a library from another.
#
# So the refusal cases require a NON-ZERO configure AND the specific message;
# a case that merely fails would pass against any broken configure at all. The
# acceptance cases require the pinned branch by name and the exact pair.
#
# Every child directory is deleted before use, except where a case is
# deliberately reconfiguring the previous one.

cmake_minimum_required(VERSION 3.20)

foreach(_v SOURCE_DIR BUILD_DIR CMAKE_CMD CTEST_CMD MSQUIC_INCLUDE_DIR
           MSQUIC_LIBRARY LIB_PREFIX LIB_SUFFIX LIB_SUFFIXES)
    if(NOT DEFINED ${_v} OR "${${_v}}" STREQUAL "")
        message(FATAL_ERROR "pinned_msquic_fail_closed: ${_v} not provided")
    endif()
endforeach()

set(GOOD_ARGS
    "-DMOQ_MSQUIC_PIN_INCLUDE_DIR:PATH=${MSQUIC_INCLUDE_DIR}"
    "-DMOQ_MSQUIC_PIN_LIBRARY:FILEPATH=${MSQUIC_LIBRARY}")

set(SCRATCH "${BUILD_DIR}-scratch")

function(ar_fresh_dir d)
    file(REMOVE_RECURSE "${d}")
    if(EXISTS "${d}")
        message(FATAL_ERROR
            "pinned_msquic_fail_closed: could not remove ${d}; refusing to run "
            "against a directory that may already be configured")
    endif()
endfunction()

# Configure BUILD_DIR. `fresh` decides whether the directory is wiped first --
# the reconfigure cases depend on it NOT being.
function(ar_configure fresh extra_args out_rc out_text)
    if(fresh)
        ar_fresh_dir("${BUILD_DIR}")
    endif()
    execute_process(
        COMMAND "${CMAKE_CMD}" -S "${SOURCE_DIR}" -B "${BUILD_DIR}"
                -DMOQ_BUILD_ADAPTER_MSQUIC=ON
                -DMOQ_BUILD_MSQUIC_MANAGED=ON
                -DMOQ_MSQUIC_REGISTRATION_CHILD=ON
                ${extra_args}
        RESULT_VARIABLE rc
        OUTPUT_VARIABLE out
        ERROR_VARIABLE err)
    set(${out_rc} "${rc}" PARENT_SCOPE)
    set(${out_text} "${out}\n${err}" PARENT_SCOPE)
endfunction()

# Read the child's machine-readable discovery record. A pinned tree publishes
# its results in configure scope rather than the cache, and the human status
# text cannot be parsed for paths -- it is wrapped and space-separated, so a
# path containing a space would come back truncated.
function(ar_discovery_record label)
    set(record "${BUILD_DIR}/msquic-discovery.cmake")
    if(NOT EXISTS "${record}")
        message(FATAL_ERROR
            "pinned_msquic_fail_closed: ${label} left no discovery record at "
            "${record}")
    endif()
    include("${record}")
    foreach(_v MOQ_MSQUIC_DISCOVERY_MODE MOQ_MSQUIC_OWNS_PAIR
               MOQ_MSQUIC_INCLUDE_DIR MOQ_MSQUIC_LIBRARY)
        set(${_v} "${${_v}}" PARENT_SCOPE)
    endforeach()
endfunction()

function(ar_expect_marker label text marker)
    string(REGEX REPLACE "[ \t\r\n]+" " " flat "${text}")
    string(FIND "${flat}" "${marker}" pos)
    if(pos EQUAL -1)
        message(FATAL_ERROR
            "pinned_msquic_fail_closed: ${label} configure never reported "
            "'${marker}'.\n--- child output ---\n${text}")
    endif()
endfunction()

# The configure took `want_mode` and resolved exactly `want_inc`/`want_lib`.
function(ar_expect_mode label text want_mode want_inc want_lib)
    ar_discovery_record("${label}")
    if(NOT MOQ_MSQUIC_DISCOVERY_MODE STREQUAL "${want_mode}")
        message(FATAL_ERROR
            "pinned_msquic_fail_closed: ${label} resolved through the "
            "'${MOQ_MSQUIC_DISCOVERY_MODE}' branch, not '${want_mode}'.\n"
            "--- child output ---\n${text}")
    endif()
    ar_same_path("${label} include dir" "${MOQ_MSQUIC_INCLUDE_DIR}" "${want_inc}")
    ar_same_path("${label} library" "${MOQ_MSQUIC_LIBRARY}" "${want_lib}")
endfunction()

# A mode that owns no pair must publish none: a package or an injected target
# configured over an earlier root search's cache would otherwise report that
# search's paths as its own.
function(ar_expect_no_pair label want_mode)
    ar_discovery_record("${label}")
    if(NOT MOQ_MSQUIC_DISCOVERY_MODE STREQUAL "${want_mode}")
        message(FATAL_ERROR
            "pinned_msquic_fail_closed: ${label} resolved through the "
            "'${MOQ_MSQUIC_DISCOVERY_MODE}' branch, not '${want_mode}'")
    endif()
    if(MOQ_MSQUIC_OWNS_PAIR OR NOT MOQ_MSQUIC_INCLUDE_DIR STREQUAL ""
       OR NOT MOQ_MSQUIC_LIBRARY STREQUAL "")
        message(FATAL_ERROR
            "pinned_msquic_fail_closed: ${label} is ${want_mode} mode but "
            "published a header/library pair it does not own:\n"
            "  include dir: ${MOQ_MSQUIC_INCLUDE_DIR}\n"
            "  library:     ${MOQ_MSQUIC_LIBRARY}")
    endif()
endfunction()

function(ar_cache_value key out_var)
    set(cache_file "${BUILD_DIR}/CMakeCache.txt")
    if(NOT EXISTS "${cache_file}")
        message(FATAL_ERROR
            "pinned_msquic_fail_closed: no ${cache_file} to read '${key}' from")
    endif()
    file(STRINGS "${cache_file}" hits REGEX "^${key}:[^=]*=")
    list(LENGTH hits n)
    if(NOT n EQUAL 1)
        message(FATAL_ERROR
            "pinned_msquic_fail_closed: child cache has ${n} entries for "
            "'${key}': ${hits}")
    endif()
    list(GET hits 0 line)
    string(REGEX REPLACE "^${key}:[^=]*=" "" val "${line}")
    set(${out_var} "${val}" PARENT_SCOPE)
endfunction()

function(ar_cache_type key out_var)
    set(cache_file "${BUILD_DIR}/CMakeCache.txt")
    file(STRINGS "${cache_file}" hits REGEX "^${key}:[^=]*=")
    list(GET hits 0 line)
    string(REGEX REPLACE "^${key}:([^=]*)=.*$" "\\1" t "${line}")
    set(${out_var} "${t}" PARENT_SCOPE)
endfunction()

# A result variable must NOT be in the cache after a pinned configure: a cached
# result outlives the pin that produced it, and the next configure without a
# pin finds it already populated and skips the search it was asked to do.
function(ar_cache_absent label key)
    set(cache_file "${BUILD_DIR}/CMakeCache.txt")
    file(STRINGS "${cache_file}" hits REGEX "^${key}:[^=]*=")
    list(LENGTH hits n)
    if(NOT n EQUAL 0)
        message(FATAL_ERROR
            "pinned_msquic_fail_closed: ${label} left '${key}' in the cache: "
            "${hits}\nA pinned result that persists is what makes a later "
            "unpinned configure skip its search and claim a root it never "
            "looked in.")
    endif()
endfunction()

function(ar_same_path label got want)
    get_filename_component(g "${got}" REALPATH)
    get_filename_component(w "${want}" REALPATH)
    if(NOT g STREQUAL w)
        message(FATAL_ERROR
            "pinned_msquic_fail_closed: ${label} identity mismatch.\n"
            "  resolved to: ${got}\n     (real path) ${g}\n"
            "  pin asked:   ${want}\n     (real path) ${w}")
    endif()
endfunction()

# The pin was honoured, published as the results, and normalized to real cache
# types. Every acceptance case ends here.
function(ar_expect_pinned label text)
    ar_discovery_record("${label}")
    if(NOT MOQ_MSQUIC_DISCOVERY_MODE STREQUAL "pinned")
        message(FATAL_ERROR
            "pinned_msquic_fail_closed: ${label} resolved through the "
            "'${MOQ_MSQUIC_DISCOVERY_MODE}' branch, not 'pinned'.\n"
            "--- child output ---\n${text}")
    endif()
    ar_same_path("${label} include dir" "${MOQ_MSQUIC_INCLUDE_DIR}"
                 "${MSQUIC_INCLUDE_DIR}")
    ar_same_path("${label} library" "${MOQ_MSQUIC_LIBRARY}" "${MSQUIC_LIBRARY}")
    # The pin INPUTS are cache entries and are normalized to real types; the
    # results deliberately are not cached at all, and a leftover entry for
    # either of them is the stale-result defect this contract exists to keep
    # out.
    ar_cache_type("MOQ_MSQUIC_PIN_INCLUDE_DIR" t)
    if(NOT t STREQUAL "PATH")
        message(FATAL_ERROR
            "pinned_msquic_fail_closed: ${label} left MOQ_MSQUIC_PIN_INCLUDE_DIR "
            "typed '${t}', not PATH")
    endif()
    ar_cache_type("MOQ_MSQUIC_PIN_LIBRARY" t)
    if(NOT t STREQUAL "FILEPATH")
        message(FATAL_ERROR
            "pinned_msquic_fail_closed: ${label} left MOQ_MSQUIC_PIN_LIBRARY "
            "typed '${t}', not FILEPATH")
    endif()
    foreach(_k MOQ_MSQUIC_INCLUDE_DIR MOQ_MSQUIC_LIBRARY)
        ar_cache_absent("${label}" "${_k}")
    endforeach()
endfunction()

function(ar_must_accept label fresh extra_args)
    ar_configure("${fresh}" "${extra_args}" rc text)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR
            "pinned_msquic_fail_closed: ${label} did not configure.\n"
            "--- child output ---\n${text}")
    endif()
    ar_expect_pinned("${label}" "${text}")
endfunction()

function(ar_must_refuse label extra_args want_text)
    ar_configure(TRUE "${extra_args}" rc text)
    if(rc EQUAL 0)
        message(FATAL_ERROR
            "pinned_msquic_fail_closed: ${label} CONFIGURED SUCCESSFULLY, but "
            "this input must be refused -- it can resolve an MsQuic other than "
            "the one named.\n--- child output ---\n${text}")
    endif()
    # CMake wraps message() text, so a phrase can arrive split across lines.
    # Both sides are flattened to single-spaced text before comparing.
    string(REGEX REPLACE "[ \t\r\n]+" " " flat "${text}")
    string(REGEX REPLACE "[ \t\r\n]+" " " flat_want "${want_text}")
    string(FIND "${flat}" "${flat_want}" pos)
    if(pos EQUAL -1)
        message(FATAL_ERROR
            "pinned_msquic_fail_closed: ${label} failed, but not for its own "
            "reason.\n  expected to contain: ${want_text}\n"
            "--- child output ---\n${text}")
    endif()
endfunction()

# -- the pin is honoured on a fresh tree -------------------------------------
# Package discovery is left ENABLED here and in every acceptance case below:
# where an installed msquic package exists it would win under the old search
# order, so this is also the proof that a package cannot shadow a pin.
ar_must_accept("fresh pin" TRUE "${GOOD_ARGS}")

# -- and the honest pin's consumer really builds and links -------------------
# Refusals prove what is rejected; this proves the accepted pin is usable.
execute_process(
    COMMAND "${CMAKE_CMD}" --build "${BUILD_DIR}" --target test_msquic_public_compile
    RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: a pinned tree does not build and link its "
        "MsQuic consumer (rc=${rc}).\n--- stdout ---\n${out}\n"
        "--- stderr ---\n${err}")
endif()

# -- the two competing dependencies this test builds for itself --------------
# Neither an installed msquic package nor a second MsQuic checkout may be
# required to be present: a case that only runs where Homebrew happens to have
# libmsquic is not a test of anything. Both are synthesized here from the
# honest pair, so every transition below is exercised on any machine.
#
# The synthetic root's library is named the way THIS platform names one, from
# CMake's own knowledge, forwarded by the caller. find_library(NAMES msquic)
# looks for <prefix>msquic<suffix>, so a name taken from the pinned file
# instead would be wrong wherever that file is versioned (libmsquic.so.2.6.0
# yields ".0") -- and a fixture that cannot be found does not fail, it quietly
# stops testing the root branch.
set(_lib_suffix "${LIB_SUFFIX}")
set(SYN_ROOT "${SCRATCH}/root")
set(SYN_ROOT_INC "${SYN_ROOT}/src/inc")
set(SYN_ROOT_LIB "${SYN_ROOT}/build/bin/Release/${LIB_PREFIX}msquic${_lib_suffix}")
set(SYN_PKG "${SCRATCH}/pkg")

function(ar_build_scratch_deps)
    ar_fresh_dir("${SCRATCH}")
    file(MAKE_DIRECTORY "${SYN_ROOT_INC}")
    file(MAKE_DIRECTORY "${SYN_ROOT}/build/bin/Release")
    file(CREATE_LINK "${MSQUIC_INCLUDE_DIR}/msquic.h"
         "${SYN_ROOT_INC}/msquic.h" SYMBOLIC)
    file(CREATE_LINK "${MSQUIC_LIBRARY}" "${SYN_ROOT_LIB}" SYMBOLIC)

    # A minimal but real config package: find_package(msquic CONFIG) with
    # msquic_DIR pointed here resolves it exactly as an installed one would.
    file(MAKE_DIRECTORY "${SYN_PKG}")
    file(WRITE "${SYN_PKG}/msquicConfig.cmake"
"add_library(msquic::msquic UNKNOWN IMPORTED)\n"
"set_target_properties(msquic::msquic PROPERTIES\n"
"    IMPORTED_LOCATION \"${SYN_ROOT_LIB}\"\n"
"    INTERFACE_INCLUDE_DIRECTORIES \"${SYN_ROOT_INC}\")\n"
"set(msquic_FOUND TRUE)\n"
"message(STATUS \"synthetic msquic package in use\")\n")
endfunction()

# -- a pin added to an already-resolved tree still wins -----------------------
# This is the case a persistent "how did this tree resolve MsQuic" latch gets
# wrong: the tree has already answered that question, and the pin arrives
# afterwards. It must win anyway, from either direction it can arrive from.
ar_build_scratch_deps()
ar_configure(TRUE "-Dmsquic_DIR=${SYN_PKG}" rc text)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: the synthesized package did not configure, "
        "so the package reconfigure case proves nothing.\n"
        "--- child output ---\n${text}")
endif()
# A package carries its artifacts on the imported target rather than in the
# module's result variables, so identity is proved by the package announcing
# itself: this is THAT package, not whichever one the machine has installed.
ar_expect_marker("package baseline" "${text}" "MsQuic discovery mode: package")
ar_expect_marker("package baseline" "${text}" "synthetic msquic package in use")
ar_must_accept("pin added to a package-resolved tree" FALSE
               "-Dmsquic_DIR=${SYN_PKG};${GOOD_ARGS}")

ar_configure(TRUE
    "-DCMAKE_DISABLE_FIND_PACKAGE_msquic=TRUE;-DMOQ_MSQUIC_ROOT=${SYN_ROOT}"
    rc text)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: the synthesized root did not configure, so "
        "the root reconfigure case proves nothing.\n"
        "--- child output ---\n${text}")
endif()
ar_expect_mode("root baseline" "${text}" "root"
               "${SYN_ROOT_INC}" "${SYN_ROOT_LIB}")

# Reconfiguring that tree unchanged must stay root mode. The search branches
# write their results into the same variables a legacy caller used to preseed,
# so a legacy check that ran on every configure would read this tree's own
# find_path/find_library output back as caller input and reclassify it.
ar_configure(FALSE "" rc text)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: reconfiguring the root tree failed.\n"
        "--- child output ---\n${text}")
endif()
ar_expect_mode("root tree reconfigured" "${text}" "root"
               "${SYN_ROOT_INC}" "${SYN_ROOT_LIB}")

ar_must_accept("pin added to a root-resolved tree" FALSE "${GOOD_ARGS}")

# The same root branch, reached through a DIFFERENT suffix this platform
# searches. The naming has to come from the platform's own list rather than one
# hardcoded spelling, and a fixture that names a library nobody looks for does
# not fail -- it quietly stops testing anything.
list(GET LIB_SUFFIXES -1 _alt_suffix)
set(ALT_ROOT "${SCRATCH}/altroot")
set(ALT_ROOT_INC "${ALT_ROOT}/src/inc")
set(ALT_ROOT_LIB "${ALT_ROOT}/build/bin/Release/${LIB_PREFIX}msquic${_alt_suffix}")
file(MAKE_DIRECTORY "${ALT_ROOT_INC}")
file(MAKE_DIRECTORY "${ALT_ROOT}/build/bin/Release")
file(CREATE_LINK "${MSQUIC_INCLUDE_DIR}/msquic.h" "${ALT_ROOT_INC}/msquic.h"
     SYMBOLIC)
file(CREATE_LINK "${MSQUIC_LIBRARY}" "${ALT_ROOT_LIB}" SYMBOLIC)
ar_configure(TRUE
    "-DCMAKE_DISABLE_FIND_PACKAGE_msquic=TRUE;-DMOQ_MSQUIC_ROOT=${ALT_ROOT}"
    rc text)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: a root whose library carries this "
        "platform's alternate suffix did not configure.\n--- child output ---\n${text}")
endif()
ar_expect_mode("root with an alternate library suffix" "${text}" "root"
               "${ALT_ROOT_INC}" "${ALT_ROOT_LIB}")

# -- and removing a pin gives the search back -------------------------------
# A pinned result written into the cache outlives the pin: the next configure
# finds it populated, skips the root it was asked about, and reports success
# for a root it never looked in. So the pin must leave nothing behind.
ar_must_accept("pin before removal" TRUE "${GOOD_ARGS}")
ar_configure(FALSE
    "-DMOQ_MSQUIC_PIN_INCLUDE_DIR=;-DMOQ_MSQUIC_PIN_LIBRARY=;-DCMAKE_DISABLE_FIND_PACKAGE_msquic=TRUE;-DMOQ_MSQUIC_ROOT=${SCRATCH}/no-such-root"
    rc text)
if(rc EQUAL 0)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: clearing the pin and pointing at a root "
        "that holds no MsQuic still CONFIGURED. The removed pin's results were "
        "reused as if they had been found under that root.\n"
        "--- child output ---\n${text}")
endif()

ar_must_accept("pin before handover to a real root" TRUE "${GOOD_ARGS}")
ar_configure(FALSE
    "-DMOQ_MSQUIC_PIN_INCLUDE_DIR=;-DMOQ_MSQUIC_PIN_LIBRARY=;-DCMAKE_DISABLE_FIND_PACKAGE_msquic=TRUE;-DMOQ_MSQUIC_ROOT=${SYN_ROOT}"
    rc text)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: clearing the pin in favour of a real root "
        "failed to configure.\n--- child output ---\n${text}")
endif()
ar_expect_mode("root after pin removal" "${text}" "root"
               "${SYN_ROOT_INC}" "${SYN_ROOT_LIB}")

# -- legacy result-variable inputs are migrated, never silently replaced ------
# Callers used to name an out-of-tree build by preseeding the RESULT variables.
# Those are results now, so such a caller must be told -- not quietly resolved
# to whatever else is installed.
ar_configure(TRUE
    "-DMOQ_MSQUIC_INCLUDE_DIR=${MSQUIC_INCLUDE_DIR};-DMOQ_MSQUIC_LIBRARY=${MSQUIC_LIBRARY}"
    rc text)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: a complete legacy pair must still "
        "configure.\n--- child output ---\n${text}")
endif()
ar_expect_mode("legacy pair" "${text}" "pinned"
               "${MSQUIC_INCLUDE_DIR}" "${MSQUIC_LIBRARY}")
string(REGEX REPLACE "[ \t\r\n]+" " " _flat "${text}")
string(FIND "${_flat}" "switch to MOQ_MSQUIC_PIN_INCLUDE_DIR" pos)
if(pos EQUAL -1)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: a legacy pair was honoured without telling "
        "the caller to migrate.\n--- child output ---\n${text}")
endif()

# The same, with a package installed that would otherwise win. This is the
# silent-substitution case: the caller named one MsQuic and would get another.
ar_configure(TRUE
    "-Dmsquic_DIR=${SYN_PKG};-DMOQ_MSQUIC_INCLUDE_DIR=${MSQUIC_INCLUDE_DIR};-DMOQ_MSQUIC_LIBRARY=${MSQUIC_LIBRARY}"
    rc text)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: a legacy pair alongside a package must "
        "configure.\n--- child output ---\n${text}")
endif()
ar_expect_mode("legacy pair beside a package" "${text}" "pinned"
               "${MSQUIC_INCLUDE_DIR}" "${MSQUIC_LIBRARY}")

# An explicit pin still outranks a legacy pair naming something else.
ar_configure(TRUE
    "-DMOQ_MSQUIC_INCLUDE_DIR=${SYN_ROOT_INC};-DMOQ_MSQUIC_LIBRARY=${SYN_ROOT_LIB};${GOOD_ARGS}"
    rc text)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: an explicit pin beside a legacy pair must "
        "configure.\n--- child output ---\n${text}")
endif()
ar_expect_mode("explicit pin outranks a legacy pair" "${text}" "pinned"
               "${MSQUIC_INCLUDE_DIR}" "${MSQUIC_LIBRARY}")

ar_must_refuse("half a legacy pair"
    "-DMOQ_MSQUIC_LIBRARY=${MSQUIC_LIBRARY}"
    "Use the pin inputs instead")
ar_must_refuse("half a legacy pair, include side"
    "-DMOQ_MSQUIC_INCLUDE_DIR=${MSQUIC_INCLUDE_DIR}"
    "Use the pin inputs instead")

# Half an EXPLICIT pin is refused even when a complete legacy pair sits beside
# it and could have finished the job. Completing it would resolve the MsQuic
# the operator did not name while discarding the path they did type -- the one
# outcome worse than refusing.
ar_must_refuse("explicit include only, beside a complete legacy pair"
    "-DMOQ_MSQUIC_PIN_INCLUDE_DIR=${MSQUIC_INCLUDE_DIR};-DMOQ_MSQUIC_INCLUDE_DIR=${SYN_ROOT_INC};-DMOQ_MSQUIC_LIBRARY=${SYN_ROOT_LIB}"
    "MOQ_MSQUIC_PIN_INCLUDE_DIR and MOQ_MSQUIC_PIN_LIBRARY pin one MsQuic build and must be supplied together")
ar_must_refuse("explicit library only, beside a complete legacy pair"
    "-DMOQ_MSQUIC_PIN_LIBRARY=${MSQUIC_LIBRARY};-DMOQ_MSQUIC_INCLUDE_DIR=${SYN_ROOT_INC};-DMOQ_MSQUIC_LIBRARY=${SYN_ROOT_LIB}"
    "MOQ_MSQUIC_PIN_INCLUDE_DIR and MOQ_MSQUIC_PIN_LIBRARY pin one MsQuic build and must be supplied together")

# ...but a COMPLETE explicit pin is authoritative, so a half-written legacy
# entry beside it is the pin's business, not a reason to refuse.
foreach(_half
        "-DMOQ_MSQUIC_LIBRARY=${SYN_ROOT_LIB}"
        "-DMOQ_MSQUIC_INCLUDE_DIR=${SYN_ROOT_INC}")
    ar_configure(TRUE "${_half};${GOOD_ARGS}" rc text)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR
            "pinned_msquic_fail_closed: a complete explicit pin must decide, "
            "even beside half a legacy entry (${_half}).\n"
            "--- child output ---\n${text}")
    endif()
    ar_expect_mode("explicit pin beside ${_half}" "${text}" "pinned"
                   "${MSQUIC_INCLUDE_DIR}" "${MSQUIC_LIBRARY}")
endforeach()

# -- a failed discovery must not retire the migration ------------------------
# Marking a tree "initialized" on the way out of a FAILED configure would
# retire the legacy migration before it ever ran: the caller's next attempt,
# now with a complete legacy pair, would skip migration and be resolved by
# whatever package happens to be installed.
ar_configure(TRUE
    "-DCMAKE_DISABLE_FIND_PACKAGE_msquic=TRUE;-DMOQ_MSQUIC_ROOT=${SCRATCH}/no-such-root"
    rc text)
if(rc EQUAL 0)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: a root holding no MsQuic must not "
        "configure.\n--- child output ---\n${text}")
endif()
ar_configure(FALSE
    "-Dmsquic_DIR=${SYN_PKG};-DMOQ_MSQUIC_INCLUDE_DIR=${MSQUIC_INCLUDE_DIR};-DMOQ_MSQUIC_LIBRARY=${MSQUIC_LIBRARY}"
    rc text)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: retrying a failed tree with a complete "
        "legacy pair must configure.\n--- child output ---\n${text}")
endif()
ar_expect_mode("legacy retry after a failed configure" "${text}" "pinned"
               "${MSQUIC_INCLUDE_DIR}" "${MSQUIC_LIBRARY}")
ar_expect_marker("legacy retry after a failed configure" "${text}"
                 "switch to MOQ_MSQUIC_PIN_INCLUDE_DIR")

# -- a mode that owns no pair publishes none ---------------------------------
# A root search leaves its results in the cache. Configure a package over that
# same tree and the paths are still sitting there, belonging to a dependency
# this configure is not using.
ar_configure(TRUE
    "-DCMAKE_DISABLE_FIND_PACKAGE_msquic=TRUE;-DMOQ_MSQUIC_ROOT=${SYN_ROOT}"
    rc text)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: the root baseline for the stale-cache case "
        "failed.\n--- child output ---\n${text}")
endif()
# The baseline disabled package discovery to reach the root branch, and that
# choice is cached; re-enable it explicitly rather than assume.
ar_configure(FALSE
    "-DCMAKE_DISABLE_FIND_PACKAGE_msquic=FALSE;-Dmsquic_DIR=${SYN_PKG}"
    rc text)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: reconfiguring a root tree onto a package "
        "failed.\n--- child output ---\n${text}")
endif()
ar_expect_no_pair("package over a root-resolved tree" "package")

# -- a pin whose paths contain spaces ----------------------------------------
# Nothing about a filesystem path forbids a space, and the product accepts one
# happily. An oracle that reads paths out of human status text does not: it
# truncates at the space and fails a configure that was correct.
set(SPACED "${SCRATCH}/moq pin")
file(MAKE_DIRECTORY "${SPACED}")
# The whole include directory, not just msquic.h: this case compiles against
# the pin, and msquic.h includes its siblings.
file(CREATE_LINK "${MSQUIC_INCLUDE_DIR}" "${SPACED}/inc" SYMBOLIC)
file(CREATE_LINK "${MSQUIC_LIBRARY}" "${SPACED}/${LIB_PREFIX}msquic${LIB_SUFFIX}"
     SYMBOLIC)
set(SPACED_ARGS
    "-DMOQ_MSQUIC_PIN_INCLUDE_DIR:PATH=${SPACED}/inc"
    "-DMOQ_MSQUIC_PIN_LIBRARY:FILEPATH=${SPACED}/${LIB_PREFIX}msquic${LIB_SUFFIX}")
ar_configure(TRUE "${SPACED_ARGS}" rc text)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: a pin whose paths contain a space did not "
        "configure.\n--- child output ---\n${text}")
endif()
ar_expect_mode("pin containing spaces" "${text}" "pinned"
               "${SPACED}/inc" "${SPACED}/${LIB_PREFIX}msquic${LIB_SUFFIX}")
execute_process(
    COMMAND "${CMAKE_CMD}" --build "${BUILD_DIR}" --target test_msquic_public_compile
    RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: a spaced pin configured but its consumer "
        "does not build and link (rc=${rc}).\n--- stdout ---\n${out}\n"
        "--- stderr ---\n${err}")
endif()

# And the nested registration authority, whose own oracle reads the same
# identity, must survive it across both of its configures.
ar_fresh_dir("${BUILD_DIR}")
execute_process(
    COMMAND "${CMAKE_CMD}" -S "${SOURCE_DIR}" -B "${BUILD_DIR}"
            -DMOQ_BUILD_ADAPTER_MSQUIC=ON
            -DMOQ_BUILD_MSQUIC_MANAGED=ON
            ${SPACED_ARGS}
    RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: a spaced pin must configure with the "
        "registration fixture enabled.\n--- stdout ---\n${out}\n"
        "--- stderr ---\n${err}")
endif()
execute_process(
    COMMAND "${CTEST_CMD}" --test-dir "${BUILD_DIR}"
            -R "^msquic_first_configure_registration$" --output-on-failure
    RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR
        "pinned_msquic_fail_closed: the first/second configure registration "
        "authority fails when the pin contains a space (rc=${rc}).\n"
        "--- stdout ---\n${out}\n--- stderr ---\n${err}")
endif()

# -- an untyped command line is ordinary CMake usage -------------------------
# -DVAR=value lands as UNINITIALIZED. That is not a broken pin; it is how
# everyone writes one. It must resolve the same artifacts and be normalized.
ar_must_accept("untyped pin" TRUE
    "-DMOQ_MSQUIC_PIN_INCLUDE_DIR=${MSQUIC_INCLUDE_DIR};-DMOQ_MSQUIC_PIN_LIBRARY=${MSQUIC_LIBRARY}")

# -- half a pin --------------------------------------------------------------
# The missing half would be filled in by a search, pairing a header and a
# library from two different builds.
ar_must_refuse("include dir without library"
    "-DMOQ_MSQUIC_PIN_INCLUDE_DIR:PATH=${MSQUIC_INCLUDE_DIR}"
    "must be supplied together")
ar_must_refuse("library without include dir"
    "-DMOQ_MSQUIC_PIN_LIBRARY:FILEPATH=${MSQUIC_LIBRARY}"
    "must be supplied together")

# -- a pin that does not name what it claims ---------------------------------
# EXISTS alone is not enough: a directory exists too, and a pin that points at
# one configures cleanly and fails much later, somewhere else.
ar_must_refuse("library that does not exist"
    "-DMOQ_MSQUIC_PIN_INCLUDE_DIR:PATH=${MSQUIC_INCLUDE_DIR};-DMOQ_MSQUIC_PIN_LIBRARY:FILEPATH=${MSQUIC_LIBRARY}.absent"
    "does not exist")
ar_must_refuse("library that is a directory"
    "-DMOQ_MSQUIC_PIN_INCLUDE_DIR:PATH=${MSQUIC_INCLUDE_DIR};-DMOQ_MSQUIC_PIN_LIBRARY:FILEPATH=${MSQUIC_INCLUDE_DIR}"
    "is a directory, not a library file")
ar_must_refuse("include dir that is not a directory"
    "-DMOQ_MSQUIC_PIN_INCLUDE_DIR:PATH=${MSQUIC_LIBRARY};-DMOQ_MSQUIC_PIN_LIBRARY:FILEPATH=${MSQUIC_LIBRARY}"
    "is not a directory")
ar_must_refuse("include dir with no msquic.h"
    "-DMOQ_MSQUIC_PIN_INCLUDE_DIR:PATH=${SOURCE_DIR};-DMOQ_MSQUIC_PIN_LIBRARY:FILEPATH=${MSQUIC_LIBRARY}"
    "has no msquic.h")

# An include root whose msquic.h is itself a directory: EXISTS says yes, and
# the compiler says something unrecognizable much later.
ar_fresh_dir("${SCRATCH}")
file(MAKE_DIRECTORY "${SCRATCH}/dirheader/msquic.h")
ar_must_refuse("msquic.h that is a directory"
    "-DMOQ_MSQUIC_PIN_INCLUDE_DIR:PATH=${SCRATCH}/dirheader;-DMOQ_MSQUIC_PIN_LIBRARY:FILEPATH=${MSQUIC_LIBRARY}"
    "msquic.h that is a directory")

# -- a symlinked pin is a real pin -------------------------------------------
# Canonicalization exists so the checks judge what a link reaches; it must not
# make a linked spelling invalid.
ar_fresh_dir("${SCRATCH}")
file(MAKE_DIRECTORY "${SCRATCH}/link")
file(CREATE_LINK "${MSQUIC_INCLUDE_DIR}" "${SCRATCH}/link/inc" SYMBOLIC)
file(CREATE_LINK "${MSQUIC_LIBRARY}" "${SCRATCH}/link/libmsquic.dylib" SYMBOLIC)
ar_must_accept("symlinked pin" TRUE
    "-DMOQ_MSQUIC_PIN_INCLUDE_DIR:PATH=${SCRATCH}/link/inc;-DMOQ_MSQUIC_PIN_LIBRARY:FILEPATH=${SCRATCH}/link/libmsquic.dylib")

file(REMOVE_RECURSE "${BUILD_DIR}")
file(REMOVE_RECURSE "${SCRATCH}")
message(STATUS
    "pinned_msquic_fail_closed: the pin wins on a fresh tree, on a "
    "package-resolved tree and on a root-resolved tree, typed or untyped, "
    "through symlinks; removing it gives the search back; a legacy pair is "
    "migrated rather than replaced; and eight near-miss inputs are each "
    "refused for their own reason")
