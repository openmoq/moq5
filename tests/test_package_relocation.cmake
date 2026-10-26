# Focused RED: an INSTALLED LibMoQ package must be relocatable.
#
# A package that records the producing machine's dependency prefixes appears to
# work only because those artifacts are still on the box that built it.
#
# What this test actually does, stated exactly: it installs LibMoQ to one
# prefix, MOVES that prefix so the original LibMoQ install location no longer
# exists, and requires the moved package to configure and BUILD a real
# consumer. External dependencies are supplied EXPLICITLY by the consumer (its
# own -D inputs and CMAKE_PREFIX_PATH environment) -- this test does NOT make
# every producer dependency tree unavailable, and it is not a proof that the
# package works on a machine without them. What it proves is that the PACKAGE
# does not depend on its own former location, backed by an exact scan over the
# complete former record so no individual field can regress silently.
#
# Driven with -DCMAKE_EXE=<cmake> -DINSTALL_TREE=<build dir to install from>
# -DWORK=<scratch> -DCONSUMER_SRC=<dir> -DPICOQUIC_SRC= -DPICOTLS_PREFIX=
# -DCC=<compiler> -DPRODUCER_PREFIXES=<comma-separated prefixes that must not
# appear in the installed config>.

# Script mode starts from CMake's oldest policy defaults; run with the
# project's floor so if(IN_LIST) and variable references behave as they do
# in the build itself.
cmake_minimum_required(VERSION 3.20)

include("${CMAKE_CURRENT_LIST_DIR}/moq_subprocess_warnings.cmake")

if(NOT PRODUCER_PREFIXES)
    message(FATAL_ERROR
        "PRODUCER_PREFIXES is required: without the exact prefixes to look for, "
        "the leakage scan could only check that SOME text was absent")
endif()

# --- the ONE leakage classifier --------------------------------------------
# Used for the synthetic controls and for the real installed config, so a
# control cannot pass against a different implementation than the one that
# judges the product. A prefix is looked for as an exact substring of the file:
# any occurrence at all is leakage, because a relocatable package has no reason
# to name the producing host's paths.
#
# Verdicts: CLEAN, LEAKED.
function(_classify_leakage content prefixes out_verdict out_hit)
    foreach(_p IN LISTS prefixes)
        if(NOT "${_p}" STREQUAL "")
            string(FIND "${content}" "${_p}" _i)
            if(NOT _i EQUAL -1)
                set(${out_verdict} LEAKED PARENT_SCOPE)
                set(${out_hit} "${_p}" PARENT_SCOPE)
                return()
            endif()
        endif()
    endforeach()
    set(${out_verdict} CLEAN PARENT_SCOPE)
    set(${out_hit} "" PARENT_SCOPE)
endfunction()

string(REPLACE "," ";" _producer_list "${PRODUCER_PREFIXES}")

# CONSUMER_DEFS arrives as a comma-separated list of -D arguments: the
# consumer's OWN dependency declarations, which the package must not need to
# have recorded.
set(_consumer_deps "")
if(CONSUMER_DEFS)
    string(REPLACE "," ";" _consumer_deps "${CONSUMER_DEFS}")
endif()
# A sanitized producer's static archives need the same sanitizer runtimes at the
# consumer's link. Both are empty for an ordinary producer, which passes nothing.
if(SANITIZER_C_FLAGS)
    list(APPEND _consumer_deps "-DCMAKE_C_FLAGS=${SANITIZER_C_FLAGS}")
endif()
if(SANITIZER_LINK_FLAGS)
    list(APPEND _consumer_deps "-DCMAKE_EXE_LINKER_FLAGS=${SANITIZER_LINK_FLAGS}")
endif()

# --- the former-record MANIFEST, validated before it is used ---------------
# The inventory is parsed as a real manifest: exactly these nine names, once
# each, no missing, duplicate, malformed or extra entry. Recording only the
# non-empty names -- as an earlier version did -- would let a dropped,
# misspelled or duplicated field disappear silently and the "complete former
# record" claim would be unfounded.
set(_required_fields
    MOQ_PICOTLS_PREFIX
    PTLS_INCLUDE_DIR
    MOQ_PARENT_PTLS_CORE
    MOQ_PARENT_PTLS_MINICRYPTO
    MBEDTLS_INCLUDE_DIR
    MBEDTLS_LIBRARY
    MBEDTLS_CRYPTO
    MBEDTLS_X509
    MOQ_PICOQUIC_MBEDTLS_INCLUDE_DIR)

set(_seen_fields "")
set(_former_fields "")
set(_inventory_ok TRUE)
# Accepted values are staged here; nothing reaches the leakage needles until
# the WHOLE manifest has been validated, so a later missing/duplicate/extra
# entry cannot leave a half-built needle set behind.
set(_staged_values "")
if(NOT FORMER_RECORD_FIELDS)
    message(SEND_ERROR
        "FAIL[relocation.inventory.absent]: no former-record manifest was "
        "supplied, so the leakage scan would check nothing")
    set(_inventory_ok FALSE)
endif()
string(REPLACE "," ";" _former_pairs "${FORMER_RECORD_FIELDS}")
foreach(_pair IN LISTS _former_pairs)
    if(NOT _pair MATCHES "^([A-Za-z0-9_]+)=(.*)$")
        message(SEND_ERROR
            "FAIL[relocation.inventory.malformed]: manifest entry '${_pair}' "
            "is not NAME=VALUE")
        set(_inventory_ok FALSE)
    else()
        set(_fname "${CMAKE_MATCH_1}")
        set(_fval "${CMAKE_MATCH_2}")
        if(NOT _fname IN_LIST _required_fields)
            message(SEND_ERROR
                "FAIL[relocation.inventory.extra]: '${_fname}' is not one of "
                "the nine former-record fields")
            set(_inventory_ok FALSE)
        elseif(_fname IN_LIST _seen_fields)
            message(SEND_ERROR
                "FAIL[relocation.inventory.duplicate]: '${_fname}' appears "
                "more than once in the manifest")
            set(_inventory_ok FALSE)
        else()
            list(APPEND _seen_fields "${_fname}")
            # An empty value stays a DECLARED entry: erasing its name would
            # hide a field that a later build might start populating.
            set(_former_value_${_fname} "${_fval}")
            if(NOT "${_fval}" STREQUAL "")
                list(APPEND _former_fields "${_fname}")
                list(APPEND _staged_values "${_fval}")
            endif()
        endif()
    endif()
endforeach()
foreach(_req IN LISTS _required_fields)
    if(NOT _req IN_LIST _seen_fields)
        message(SEND_ERROR
            "FAIL[relocation.inventory.missing]: the manifest declares no "
            "'${_req}' entry")
        set(_inventory_ok FALSE)
    endif()
endforeach()

# Provider-applicable fields must carry real values, or their leakage arms
# would be silently vacuous.
foreach(_req MOQ_PICOTLS_PREFIX PTLS_INCLUDE_DIR MOQ_PARENT_PTLS_CORE
             MOQ_PARENT_PTLS_MINICRYPTO)
    if(NOT _req IN_LIST _former_fields)
        message(SEND_ERROR
            "FAIL[relocation.inventory.empty]: ${_req} is empty in the "
            "supported source topology, so its leakage arm would prove nothing")
        set(_inventory_ok FALSE)
    endif()
endforeach()
# An unknown or missing selector would silently skip the provider-specific
# non-vacuity checks below, so it is validated rather than defaulted.
if(NOT "${SELECTED_PROVIDER}" STREQUAL "openssl"
   AND NOT "${SELECTED_PROVIDER}" STREQUAL "mbedtls")
    message(SEND_ERROR
        "FAIL[relocation.inventory.provider]: SELECTED_PROVIDER is "
        "'${SELECTED_PROVIDER}', expected exactly openssl or mbedtls")
    set(_inventory_ok FALSE)
endif()
if("${SELECTED_PROVIDER}" STREQUAL "mbedtls")
    foreach(_req MBEDTLS_INCLUDE_DIR MBEDTLS_LIBRARY MBEDTLS_CRYPTO
                 MBEDTLS_X509 MOQ_PICOQUIC_MBEDTLS_INCLUDE_DIR)
        if(NOT _req IN_LIST _former_fields)
            message(SEND_ERROR
                "FAIL[relocation.inventory.empty]: ${_req} is empty for an "
                "mbedTLS artifact, so its leakage arm would prove nothing")
            set(_inventory_ok FALSE)
        endif()
    endforeach()
endif()

function(_leak_control name content expect)
    _classify_leakage("${content}" "${_producer_list}" _v _hit)
    if(NOT "${_v}" STREQUAL "${expect}")
        message(SEND_ERROR
            "FAIL[leak_control.${name}]: the shared classifier said ${_v}, "
            "expected ${expect}")
    endif()
endfunction()

list(GET _producer_list 0 _first_prefix)
_leak_control(clean_text "set(_LIBMOQ_PICOQUIC_WITH_OPENSSL ON)\n" CLEAN)
_leak_control(leaked_text "set(X \"${_first_prefix}/lib/libfoo.a\")\n" LEAKED)
# a prefix mentioned in a COMMENT is still leakage: it still names the producer
_leak_control(leaked_comment "# built at ${_first_prefix}\n" LEAKED)

# --- the validity boundary --------------------------------------------------
# Every inventory diagnostic above has been emitted, so a broken manifest is
# reported completely; but nothing downstream runs on one. No needles are
# derived, and no install, move, configure or build happens.
if(NOT _inventory_ok)
    message(STATUS "MOQ_RELOC_INVENTORY_INVALID_NO_DOWNSTREAM")
    return()
endif()
list(APPEND _producer_list ${_staged_values})

# --- install, then MOVE the prefix ------------------------------------------
set(_prefix "${WORK}/prefix-original")
set(_moved "${WORK}/prefix-moved")
file(REMOVE_RECURSE "${_prefix}" "${_moved}" "${WORK}/consumer-build")
execute_process(
    COMMAND "${CMAKE_EXE}" --install "${INSTALL_TREE}" --prefix "${_prefix}"
    RESULT_VARIABLE _irc OUTPUT_VARIABLE _iout ERROR_VARIABLE _ierr)
set(_iall "${_iout}${_ierr}")
_moq_reject_warnings(install "${_iall}")
if(NOT _irc EQUAL 0)
    message(FATAL_ERROR "install failed:\n${_iall}")
endif()

# --- leakage scan on the INSTALLED config -----------------------------------
file(GLOB_RECURSE _cfgs "${_prefix}/*/cmake/libmoq/libmoqConfig.cmake")
list(LENGTH _cfgs _ncfg)
if(NOT _ncfg EQUAL 1)
    message(SEND_ERROR
        "FAIL[relocation.config_count]: expected exactly one installed "
        "libmoqConfig.cmake, found ${_ncfg}: ${_cfgs}. With several, "
        "_cfg_content would be whichever happened to be read last.")
endif()
foreach(_cfg IN LISTS _cfgs)
    file(READ "${_cfg}" _cfg_content)
    _classify_leakage("${_cfg_content}" "${_producer_list}" _v _hit)
    if(NOT "${_v}" STREQUAL "CLEAN")
        message(SEND_ERROR
            "FAIL[relocation.leaked]: the installed ${_cfg} names the "
            "producing machine's prefix '${_hit}'. A consumer's success would "
            "then depend on artifacts from the box that built the package.")
    endif()
endforeach()

# --- per-field independence -------------------------------------------------
# Reinstating the whole former record and watching the scan fail proves only
# that SOME covered field leaks. Each field is therefore injected on its OWN
# into a copy of the installed config and must be caught by itself.
foreach(_fname IN LISTS _former_fields)
    set(_probe "${_cfg_content}\nset(_LIBMOQ_${_fname} \"${_former_value_${_fname}}\")\n")
    _classify_leakage("${_probe}" "${_producer_list}" _pv _phit)
    if(NOT "${_pv}" STREQUAL "LEAKED")
        message(SEND_ERROR
            "FAIL[relocation.field_invisible.${_fname}]: reintroducing "
            "_LIBMOQ_${_fname}='${_former_value_${_fname}}' on its own is NOT "
            "detected by the leakage scan, so that field could regress "
            "silently.")
    endif()
endforeach()

# --- move it, then consume the MOVED prefix ---------------------------------
file(RENAME "${_prefix}" "${_moved}")

# The MOVED package is the explicit `-D` input. Anything else the consumer
# needs is supplied through its ENVIRONMENT, which is how a consumer normally
# receives its package-manager prefixes -- and it keeps the moved package from
# being displaced by a `;`-list that execute_process would split.
set(_reloc_prefix_path "${_moved}")
set(_reloc_env "")
if(EXTRA_PREFIX_PATH)
    string(REPLACE ";" ":" _reloc_env_path "${EXTRA_PREFIX_PATH}")
    set(_reloc_env "CMAKE_PREFIX_PATH=${_reloc_env_path}")
endif()

execute_process(
    COMMAND "${CMAKE_EXE}" -E env ${_reloc_env}
            "${CMAKE_EXE}" -S "${CONSUMER_SRC}" -B "${WORK}/consumer-build"
            "-DCMAKE_PREFIX_PATH=${_reloc_prefix_path}"
            "-DMOQ_PICOQUIC_SOURCE_DIR=${PICOQUIC_SRC}"
            "-DMOQ_PICOTLS_PREFIX=${PICOTLS_PREFIX}"
            "-DCMAKE_C_COMPILER=${CC}"
            # The consumer's OWN dependency declarations. Supplying them here
            # is the point: the package must not need to have recorded them.
            ${_consumer_deps}
    RESULT_VARIABLE _crc OUTPUT_VARIABLE _cout ERROR_VARIABLE _cerr)
set(_call "${_cout}${_cerr}")
_moq_reject_warnings(consumer_configure "${_call}")
if(NOT _crc EQUAL 0)
    message(SEND_ERROR
        "FAIL[relocation.configure]: the MOVED install prefix could not be "
        "consumed. The original prefix no longer exists, so anything the "
        "package recorded about its own location is now wrong:\n${_call}")
else()
    execute_process(
        COMMAND "${CMAKE_EXE}" --build "${WORK}/consumer-build"
        RESULT_VARIABLE _brc OUTPUT_VARIABLE _bout ERROR_VARIABLE _berr)
    set(_ball "${_bout}${_berr}")
    _moq_reject_warnings(consumer_build "${_ball}")
    if(NOT _brc EQUAL 0)
        message(SEND_ERROR
            "FAIL[relocation.build]: the consumer configured against the moved "
            "prefix but failed to compile/link:\n${_ball}")
    endif()
endif()
