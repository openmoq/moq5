# Configure-level gate for the normalized provider record's SCOPE.
#
# The record must exist for every topology in which the picoquic package
# component is built -- not only the threaded-facade test topology -- and the
# provider package tests must not be registered at all when there is no
# picoquic artifact whose provider they could describe.
#
# These are configuration oracles: they inspect the GENERATED CTest commands.
# They assert nothing about trust behaviour.
#
# Driven with -DCMAKE_EXE=<cmake> -DSRC=<repo> -DWORK=<scratch dir>
# -DPICOQUIC_SRC=<...> -DOPENSSL_ROOT=<...> (may be empty)
# -DWANT_VERIFIER=<openssl|mbedtls> -DWANT_WITH_OPENSSL=<ON|OFF>
# -DWANT_WITH_MBEDTLS=<ON|OFF> -DEXTRA=<;-separated extra configure args>

include("${CMAKE_CURRENT_LIST_DIR}/moq_subprocess_warnings.cmake")

set(_failures 0)
# Callers pass the message in several string arguments (a sentence, then the
# nested output that explains it); every one of them is printed, so a failing
# row always shows its decisive diagnostic.
function(_fail tag)
    string(CONCAT _msg ${ARGN})
    message(SEND_ERROR "FAIL[${tag}]: ${_msg}")
endfunction()

function(_configure tag dir)  # remaining args: configure arguments
    file(REMOVE_RECURSE "${dir}")
    execute_process(
        COMMAND "${CMAKE_EXE}" -S "${SRC}" -B "${dir}" ${ARGN}
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _o ERROR_VARIABLE _e)
    # Warnings are judged BEFORE the outcome oracle, on every path: a
    # successful configure and an intentionally failing one are both subject to
    # the policy.
    _moq_reject_warnings("${tag}" "${_o}${_e}")
    if(NOT _rc EQUAL 0)
        _fail("${tag}.configure" "configure failed:\n${_o}${_e}")
    endif()
endfunction()

# Reads the generated service CTest file and returns its text (or "" if absent).
function(_ctest_text dir out)
    set(_f "${dir}/service/CTestTestfile.cmake")
    if(EXISTS "${_f}")
        file(READ "${_f}" _t)
    else()
        set(_t "")
    endif()
    set(${out} "${_t}" PARENT_SCOPE)
endfunction()

# --- topology 1: picoquic adapter + service + tests, threaded facade OFF ----
# The record lives next to backend selection, so turning the threaded facade
# off must not empty it.
# The parent's own provider selection is forwarded, so the sub-configure lands
# on the SAME provider as the lane under test rather than reverting to
# picoquic's defaults -- otherwise an mbedTLS lane would silently gate an
# OpenSSL topology and its expectations could never match.
set(_args
    -DMOQ_BUILD_ADAPTER_PICOQUIC=ON
    -DMOQ_BUILD_PQ_THREADED=OFF
    -DMOQ_BUILD_SERVICE=ON
    -DMOQ_BUILD_MSF=ON
    -DMOQ_BUILD_MEDIA_OBJECT=ON
    "-DMOQ_PICOQUIC_SOURCE_DIR=${PICOQUIC_SRC}"
    "-DWITH_OPENSSL=${WANT_WITH_OPENSSL}"
    "-DWITH_MBEDTLS=${WANT_WITH_MBEDTLS}")
if(PICOTLS_PREFIX)
    list(APPEND _args "-DMOQ_PICOTLS_PREFIX=${PICOTLS_PREFIX}")
endif()
# The parent's already-resolved picotls artifacts, so the sub-configure does
# not have to rediscover them from a prefix whose layout differs per lane.
if(PTLS_CORE)
    list(APPEND _args "-DPTLS_CORE_LIBRARY=${PTLS_CORE}")
endif()
if(PTLS_MINICRYPTO)
    list(APPEND _args "-DPTLS_MINICRYPTO_LIBRARY=${PTLS_MINICRYPTO}")
endif()
if(OPENSSL_ROOT)
    list(APPEND _args "-DOPENSSL_ROOT_DIR=${OPENSSL_ROOT}")
endif()
if(EXTRA)
    list(APPEND _args ${EXTRA})
endif()

_configure(threaded_off "${WORK}/nothread" ${_args})
_ctest_text("${WORK}/nothread" _t1)

# The LITERAL expected matrix -- an empty value must not satisfy it.
foreach(_pair "WANT_VERIFIER=${WANT_VERIFIER}"
              "WANT_WITH_OPENSSL=${WANT_WITH_OPENSSL}"
              "WANT_WITH_MBEDTLS=${WANT_WITH_MBEDTLS}")
    if(NOT _t1 MATCHES "-D${_pair}[\"\n ]")
        _fail(threaded_off.record
              "the generated command does not carry -D${_pair}. With the "
              "threaded facade OFF the provider record must still be exact; "
              "an empty expectation would compare equal to nothing.")
    endif()
endforeach()
# and explicitly: neither boolean may be empty
if(_t1 MATCHES "-DWANT_WITH_OPENSSL=[\"\n ]" OR _t1 MATCHES "-DWANT_WITH_MBEDTLS=[\"\n ]")
    _fail(threaded_off.empty_boolean
          "a provider boolean is EMPTY in the generated command; the record "
          "does not exist for this topology.")
endif()

# --- topology 2: service + tests, no picoquic adapter ----------------------
# There is no picoquic artifact, so the provider package tests describe
# nothing and must not be registered.
_configure(no_adapter "${WORK}/noadapter"
    -DMOQ_BUILD_ADAPTER_PICOQUIC=OFF
    -DMOQ_BUILD_SERVICE=ON
    -DMOQ_BUILD_MSF=ON
    -DMOQ_BUILD_MEDIA_OBJECT=ON)
_ctest_text("${WORK}/noadapter" _t2)
foreach(_t package_provider_metadata incompatible_provider_target
           provider_record_topology)
    if(_t2 MATCHES "${_t}")
        _fail(no_adapter.registered
              "${_t} is registered without the picoquic adapter, where no "
              "provider record can exist; it would compare against empty "
              "expectations.")
    endif()
endforeach()

# --- topology 3: installed / already-defined picoquic --------------------
# Its build settings are NOT in caller scope, so the upstream provider facts
# are UNKNOWN. Publishing OFF/OFF there records a demonstrably false matrix --
# an installed package whose config runs find_dependency(OpenSSL) would be
# recorded as OpenSSL-incapable. Configure must fail closed instead.
#
# The package is a SELF-CONTAINED fixture generated under this test's own WORK
# directory. Depending on a developer's out-of-tree installation would not be a
# durable gate: a clean checkout has none, and the arm would silently report
# itself unavailable. The fixture deliberately omits BOTH provider variables --
# that omission is the condition under test -- and stamps a marker so the arms
# prove THIS package was loaded rather than a host installation or a
# source-tree fallback.
set(_fixp "${WORK}/fixture-prefix")
file(REMOVE_RECURSE "${_fixp}")
file(MAKE_DIRECTORY "${_fixp}/lib/cmake/picoquic")
file(WRITE "${_fixp}/lib/cmake/picoquic/picoquic-config.cmake"
"# Minimal picoquic CONFIG package fixture (test-only).\n"
"# Defines the exact imported target spelling FindPicoquic consumes, carries\n"
"# real include information, and DOES NOT define WITH_OPENSSL/WITH_MBEDTLS --\n"
"# exactly like a real installed package, whose build settings are not in the\n"
"# consumer's scope.\n"
"set(MOQ_TEST_FIXTURE_PICOQUIC_PACKAGE_LOADED ON CACHE INTERNAL\n"
"    \"the test-only picoquic CONFIG fixture was loaded\")\n"
"if(NOT TARGET picoquic::picoquic-core)\n"
"    add_library(picoquic::picoquic-core INTERFACE IMPORTED)\n"
"    set_target_properties(picoquic::picoquic-core PROPERTIES\n"
"        INTERFACE_INCLUDE_DIRECTORIES \"${PICOQUIC_SRC}/picoquic\")\n"
"endif()\n"
"if(NOT TARGET picoquic::picoquic-log)\n"
"    add_library(picoquic::picoquic-log INTERFACE IMPORTED)\n"
"    set_target_properties(picoquic::picoquic-log PROPERTIES\n"
"        INTERFACE_INCLUDE_DIRECTORIES \"${PICOQUIC_SRC}/loglib\")\n"
"endif()\n"
"set(picoquic_FOUND TRUE)\n")
file(WRITE "${_fixp}/lib/cmake/picoquic/picoquic-config-version.cmake"
"set(PACKAGE_VERSION 1.0.0)\n"
"set(PACKAGE_VERSION_COMPATIBLE TRUE)\n"
"set(PACKAGE_VERSION_EXACT TRUE)\n")

# Common inputs for the installed arms.
#
# CMAKE_PREFIX_PATH is NOT an exclusive search boundary: CMake's user and system
# package registries and the ordinary system prefixes all remain eligible. The
# fixture is therefore named EXACTLY via picoquic_DIR, and both registries are
# disabled. MOQ_PICOQUIC_SOURCE_DIR is deliberately absent so no source-tree
# fallback can satisfy the find either.
#
# MOQ_PICOTLS_PREFIX and the resolved picotls artifacts are supplied because the
# verifier needs <picotls/openssl.h>; that is a separate prerequisite and must
# not be confused with the provider record under test.
set(_inst_common
    -DMOQ_BUILD_ADAPTER_PICOQUIC=ON
    -DMOQ_BUILD_SERVICE=ON
    -DMOQ_BUILD_MSF=ON
    -DMOQ_BUILD_MEDIA_OBJECT=ON
    "-DCMAKE_PREFIX_PATH=${_fixp}"
    "-Dpicoquic_DIR=${_fixp}/lib/cmake/picoquic"
    -DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF
    -DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF)
if(PICOTLS_PREFIX)
    list(APPEND _inst_common "-DMOQ_PICOTLS_PREFIX=${PICOTLS_PREFIX}")
endif()
if(PTLS_CORE)
    list(APPEND _inst_common "-DPTLS_CORE_LIBRARY=${PTLS_CORE}")
endif()
if(PTLS_MINICRYPTO)
    list(APPEND _inst_common "-DPTLS_MINICRYPTO_LIBRARY=${PTLS_MINICRYPTO}")
endif()
if(OPENSSL_ROOT)
    list(APPEND _inst_common "-DOPENSSL_ROOT_DIR=${OPENSSL_ROOT}")
endif()

# --- the ONE provider-marker classifier ------------------------------------
# These diagnostics are LibMoQ-owned, so their grammar is defined rather than
# sniffed: the marker is emitted as `TOKEN:` -- a whitespace-delimited field
# ending in a colon. An unanchored substring test would let
# `XLIBMOQ_PICOQUIC_PROVIDER_FACTS_UNKNOWNX` satisfy the UNKNOWN oracle and
# would make repeated markers invisible.
#
# Requires EXACTLY ONE marker of the expected class and ZERO of the other.
# Verdicts: ABSENT, DUPLICATE, WRONG_CLASS, OK.
function(_classify_provider_markers text expect other out_verdict)
    string(REPLACE "\r" "" _t "${text}")
    string(REPLACE "\n" ";" _lines "${_t}")
    set(_n_expect 0)
    set(_n_other 0)
    foreach(_line IN LISTS _lines)
        string(REGEX REPLACE "[ \t]+" ";" _fields "${_line}")
        foreach(_f IN LISTS _fields)
            if(_f STREQUAL "${expect}:")
                math(EXPR _n_expect "${_n_expect} + 1")
            elseif(_f STREQUAL "${other}:")
                math(EXPR _n_other "${_n_other} + 1")
            endif()
        endforeach()
    endforeach()
    if(_n_other GREATER 0)
        set(${out_verdict} WRONG_CLASS PARENT_SCOPE)
    elseif(_n_expect EQUAL 0)
        set(${out_verdict} ABSENT PARENT_SCOPE)
    elseif(_n_expect GREATER 1)
        set(${out_verdict} DUPLICATE PARENT_SCOPE)
    else()
        set(${out_verdict} OK PARENT_SCOPE)
    endif()
endfunction()

# --- the ONE installed-arm observation path -------------------------------
# Every installed-package configure -- the seven real expected-failure arms AND
# the markerless discriminator -- is observed through this single function, so a
# discriminator cannot pass by re-implementing a weaker check of its own.
#
# It performs no assertions. It reports what it saw:
#   <out>_rc        configure exit status
#   <out>_all       combined output
#   <out>_cache     TRUE when a fresh cache exists
#   <out>_identity  FIXTURE | DECOY | NONE  (which package stamped the cache)
#   <out>_marker    the provider-marker verdict for the expected class
# The caller decides which combination is correct for its arm.
function(_inst_observe out dir expect other)
    file(REMOVE_RECURSE "${dir}")
    execute_process(
        COMMAND "${CMAKE_EXE}" -S "${SRC}" -B "${dir}" ${ARGN}
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _o ERROR_VARIABLE _e)
    set(_all "${_o}${_e}")
    set(${out}_rc "${_rc}" PARENT_SCOPE)
    set(${out}_all "${_all}" PARENT_SCOPE)

    if(EXISTS "${dir}/CMakeCache.txt")
        set(${out}_cache TRUE PARENT_SCOPE)
        file(READ "${dir}/CMakeCache.txt" _cache)
        if(_cache MATCHES "MOQ_TEST_FIXTURE_PICOQUIC_PACKAGE_LOADED:INTERNAL=ON")
            set(${out}_identity FIXTURE PARENT_SCOPE)
        elseif(_cache MATCHES "MOQ_TEST_FIXTURE_DECOY_MARKER:INTERNAL=ON")
            set(${out}_identity DECOY PARENT_SCOPE)
        else()
            set(${out}_identity NONE PARENT_SCOPE)
        endif()
        set(${out}_cachetext "${_cache}" PARENT_SCOPE)
    else()
        set(${out}_cache FALSE PARENT_SCOPE)
        set(${out}_identity NONE PARENT_SCOPE)
        set(${out}_cachetext "" PARENT_SCOPE)
    endif()

    _classify_provider_markers("${_all}" "${expect}" "${other}" _mv)
    set(${out}_marker "${_mv}" PARENT_SCOPE)
endfunction()

# The JUDGEMENT, factored out so it can be exercised directly. Returns the
# first thing wrong with an observed installed configure:
#   NO_CACHE | WRONG_IDENTITY | CONFIGURED | BAD_MARKER | OK
#
# The identity requirement lives HERE, in one place. The decoy control below
# calls this same function, so removing the requirement makes that control
# report OK where WRONG_IDENTITY is required -- which is how a one-change
# bypass is killed by name.
function(_inst_judge out rc cache identity marker)
    if(NOT cache)
        set(${out} NO_CACHE PARENT_SCOPE)
    elseif(NOT identity STREQUAL "FIXTURE")
        set(${out} WRONG_IDENTITY PARENT_SCOPE)
    elseif(rc EQUAL 0)
        set(${out} CONFIGURED PARENT_SCOPE)
    elseif(NOT marker STREQUAL "OK")
        set(${out} BAD_MARKER PARENT_SCOPE)
    else()
        set(${out} OK PARENT_SCOPE)
    endif()
endfunction()

# One real expected-failure arm: observed through the shared path, judged by
# the shared judgement, then reported.
function(_inst_expect_fail tag dir expect_token other_token)
    # The common inputs are forwarded here, so an arm supplies only its own
    # per-arm overrides.
    _inst_observe(_ob "${dir}" "${expect_token}" "${other_token}"
                  ${_inst_common} ${ARGN})
    _moq_reject_warnings("${tag}" "${_ob_all}")
    _inst_judge(_v "${_ob_rc}" "${_ob_cache}" "${_ob_identity}" "${_ob_marker}")
    if(_v STREQUAL "NO_CACHE")
        _fail("${tag}.no_cache"
              "the arm produced no cache, so the picoquic package it loaded "
              "cannot be identified.")
    elseif(_v STREQUAL "WRONG_IDENTITY")
        _fail("${tag}.fixture"
              "the package that stamped this cache was '${_ob_identity}', not "
              "the self-contained fixture, so this arm proves nothing about "
              "installed mode.")
    elseif(_v STREQUAL "CONFIGURED")
        _fail("${tag}.configured"
              "configure SUCCEEDED where ${expect_token} was required.")
    elseif(_v STREQUAL "BAD_MARKER")
        _fail("${tag}.token"
              "the provider-marker classifier said ${_ob_marker}; exactly one "
              "${expect_token} marker and zero ${other_token} markers are "
              "required:\n${_ob_all}")
    endif()
endfunction()

set(_UNKNOWN "LIBMOQ_PICOQUIC_PROVIDER_FACTS_UNKNOWN")
set(_MISMATCH "LIBMOQ_PICOQUIC_PROVIDER_CAPABILITY_MISMATCH")

# Permanent controls for that classifier, judged through the SAME function the
# seven real arms use.
function(_marker_control name text expect other want)
    _classify_provider_markers("${text}" "${expect}" "${other}" _v)
    if(NOT _v STREQUAL "${want}")
        _fail("marker_control.${name}"
              "the shared provider-marker classifier said ${_v}, expected ${want}")
    endif()
endfunction()

_marker_control(valid       "CMake Error: ${_UNKNOWN}: missing=WITH_MBEDTLS\n" "${_UNKNOWN}" "${_MISMATCH}" OK)
_marker_control(absent      "CMake Error: something else entirely\n"           "${_UNKNOWN}" "${_MISMATCH}" ABSENT)
_marker_control(prefixed    "CMake Error: X${_UNKNOWN}: missing=WITH_MBEDTLS\n" "${_UNKNOWN}" "${_MISMATCH}" ABSENT)
_marker_control(suffixed    "CMake Error: ${_UNKNOWN}X: missing=WITH_MBEDTLS\n" "${_UNKNOWN}" "${_MISMATCH}" ABSENT)
_marker_control(no_colon    "CMake Error: ${_UNKNOWN} missing=WITH_MBEDTLS\n"  "${_UNKNOWN}" "${_MISMATCH}" ABSENT)
_marker_control(dup_equal   "CMake Error: ${_UNKNOWN}: a\nCMake Error: ${_UNKNOWN}: b\n" "${_UNKNOWN}" "${_MISMATCH}" DUPLICATE)
_marker_control(dup_same_line "CMake Error: ${_UNKNOWN}: ${_UNKNOWN}: a\n"     "${_UNKNOWN}" "${_MISMATCH}" DUPLICATE)
_marker_control(wrong_class "CMake Error: ${_MISMATCH}: selected=openssl\n"    "${_UNKNOWN}" "${_MISMATCH}" WRONG_CLASS)
_marker_control(both_classes "CMake Error: ${_UNKNOWN}: a\nCMake Error: ${_MISMATCH}: b\n" "${_UNKNOWN}" "${_MISMATCH}" WRONG_CLASS)
# and symmetrically for the capability class
_marker_control(cap_valid   "CMake Error: ${_MISMATCH}: selected=openssl\n"    "${_MISMATCH}" "${_UNKNOWN}" OK)
_marker_control(cap_wrong   "CMake Error: ${_UNKNOWN}: missing=WITH_MBEDTLS\n" "${_MISMATCH}" "${_UNKNOWN}" WRONG_CLASS)

# 3a. no provider facts at all
_inst_expect_fail(installed_none "${WORK}/i-none" "${_UNKNOWN}" "${_MISMATCH}")
# an OFF/OFF record must not have been published
if(EXISTS "${WORK}/i-none/CMakeCache.txt")
    file(READ "${WORK}/i-none/CMakeCache.txt" _icache)
    if(_icache MATCHES "MOQ_PARENT_WITH_OPENSSL:INTERNAL=OFF" AND
       _icache MATCHES "MOQ_PARENT_WITH_MBEDTLS:INTERNAL=OFF")
        _fail(installed_none.record_written
              "an OFF/OFF provider record was published for a picoquic whose "
              "facts are unknown.")
    endif()
endif()

# 3b. only WITH_OPENSSL absent
_inst_expect_fail(installed_no_ossl "${WORK}/i-noossl" "${_UNKNOWN}" "${_MISMATCH}"
                  -DWITH_MBEDTLS=ON)
# 3c. only WITH_MBEDTLS absent, OpenSSL ON
_inst_expect_fail(installed_no_mbed_on "${WORK}/i-nombed-on" "${_UNKNOWN}" "${_MISMATCH}"
                  -DWITH_OPENSSL=ON)
# 3d. only WITH_MBEDTLS absent, OpenSSL OFF -- the reproduced discriminator.
#     AUTO must not consume the unavailable mbedTLS fact as false.
_inst_expect_fail(installed_no_mbed_off "${WORK}/i-nombed-off" "${_UNKNOWN}" "${_MISMATCH}"
                  -DWITH_OPENSSL=OFF)
# 3e. an unrecognized spelling is rejected, never guessed
_inst_expect_fail(installed_bogus "${WORK}/i-bogus" "${_UNKNOWN}" "${_MISMATCH}"
                  -DWITH_OPENSSL=perhaps -DWITH_MBEDTLS=OFF)
# 3f. facts KNOWN and neither provider available -> capability, not unknown
_inst_expect_fail(installed_cap_none "${WORK}/i-capnone" "${_MISMATCH}" "${_UNKNOWN}"
                  -DWITH_OPENSSL=OFF -DWITH_MBEDTLS=OFF)
# 3g. facts KNOWN, selected verifier not offered -> capability, not unknown
_inst_expect_fail(installed_cap_sel "${WORK}/i-capsel" "${_MISMATCH}" "${_UNKNOWN}"
                  -DMOQ_PICOQUIC_VERIFY_BACKEND=openssl
                  -DWITH_OPENSSL=OFF -DWITH_MBEDTLS=ON)

# 3h. with both facts stated it configures, publishing the stated matrix, its
#     provenance, and the proof that the FIXTURE package was the one loaded.
foreach(_spell "ON;OFF" "TRUE;N" "1;0" "yes;no")
    list(GET _spell 0 _sp_o)
    list(GET _spell 1 _sp_m)
    string(REPLACE ";" "_" _sp_tag "${_spell}")
    set(_dir "${WORK}/i-ok-${_sp_tag}")
    file(REMOVE_RECURSE "${_dir}")
    execute_process(
        COMMAND "${CMAKE_EXE}" -S "${SRC}" -B "${_dir}" ${_inst_common}
                "-DWITH_OPENSSL=${_sp_o}" "-DWITH_MBEDTLS=${_sp_m}"
        RESULT_VARIABLE _erc OUTPUT_VARIABLE _eo ERROR_VARIABLE _ee)
    _moq_reject_warnings("installed_explicit_${_sp_tag}" "${_eo}${_ee}")
    if(NOT _erc EQUAL 0)
        _fail("installed_explicit_${_sp_tag}"
              "stating both facts (${_sp_o}/${_sp_m}) did not let the fixture "
              "package configure:\n${_eo}${_ee}")
    else()
        file(READ "${_dir}/CMakeCache.txt" _ecache)
        if(NOT _ecache MATCHES "MOQ_TEST_FIXTURE_PICOQUIC_PACKAGE_LOADED:INTERNAL=ON")
            _fail("installed_explicit_${_sp_tag}.fixture"
                  "the self-contained fixture package was NOT the picoquic that "
                  "was loaded, so this arm proves nothing about installed mode.")
        endif()
        if(NOT _ecache MATCHES "MOQ_PARENT_WITH_OPENSSL:INTERNAL=ON")
            _fail("installed_explicit_${_sp_tag}.record"
                  "the stated true value was not published as ON")
        endif()
        if(NOT _ecache MATCHES "MOQ_PARENT_WITH_MBEDTLS:INTERNAL=OFF")
            _fail("installed_explicit_${_sp_tag}.record_mbed"
                  "the stated false value was not published as OFF")
        endif()
        if(NOT _ecache MATCHES "MOQ_PARENT_PROVIDER_FACTS_ORIGIN:INTERNAL=explicit")
            _fail("installed_explicit_${_sp_tag}.origin"
                  "the record's provenance is not marked explicit, so a stated "
                  "fact is indistinguishable from an inferred default")
        endif()
    endif()
endforeach()

# --- discriminators for the fixture authority itself -----------------------
# 1. A DECOY package that produces the expected provider text but is NOT the
#    fixture. It is built by copying the fixture and swapping ONLY its marker,
#    so identity is the sole difference. It runs through the SAME
#    _inst_observe() path the seven real arms use, and requires that path to
#    report a non-FIXTURE identity -- which is exactly what the real arms
#    reject. Re-implementing a weaker check here would let the real arms lose
#    their identity requirement unnoticed.
set(_decoy "${WORK}/decoy-prefix")
file(REMOVE_RECURSE "${_decoy}")
file(MAKE_DIRECTORY "${_decoy}/lib/cmake/picoquic")
file(READ "${_fixp}/lib/cmake/picoquic/picoquic-config.cmake" _fixtext)
string(REPLACE
    "MOQ_TEST_FIXTURE_PICOQUIC_PACKAGE_LOADED"
    "MOQ_TEST_FIXTURE_DECOY_MARKER"
    _decoytext "${_fixtext}")
file(WRITE "${_decoy}/lib/cmake/picoquic/picoquic-config.cmake" "${_decoytext}")
file(COPY "${_fixp}/lib/cmake/picoquic/picoquic-config-version.cmake"
     DESTINATION "${_decoy}/lib/cmake/picoquic")

set(_decoy_common "${_inst_common}")
list(TRANSFORM _decoy_common REPLACE "${_fixp}" "${_decoy}")
_inst_observe(_dc "${WORK}/i-decoy" "${_UNKNOWN}" "${_MISMATCH}" ${_decoy_common})
_moq_reject_warnings(decoy "${_dc_all}")

# Complete premises, all required before any conclusion is drawn.
if(_dc_rc EQUAL 0)
    _fail(decoy.premise_status
          "the decoy configure SUCCEEDED; the premise requires the same "
          "expected failure the real arms see.")
endif()
if(NOT _dc_cache)
    _fail(decoy.premise_cache
          "the decoy produced no cache, so its identity cannot be observed.")
endif()
if(NOT _dc_cachetext MATCHES "MOQ_TEST_FIXTURE_DECOY_MARKER:INTERNAL=ON")
    _fail(decoy.premise_alt_marker
          "the decoy's own marker is absent, so the package under observation "
          "is not the one this control built.")
endif()
if(_dc_cachetext MATCHES "MOQ_TEST_FIXTURE_PICOQUIC_PACKAGE_LOADED:INTERNAL=ON")
    _fail(decoy.premise_canonical_marker
          "the canonical fixture marker is present in the decoy's cache, so "
          "the two packages are not distinguishable.")
endif()
if(NOT _dc_marker STREQUAL "OK")
    _fail(decoy.premise_marker
          "the decoy did not emit the expected provider marker (${_dc_marker}); "
          "without it this control would be testing text, not identity.")
endif()
# THE conclusion: the SHARED JUDGEMENT -- the very function the seven real arms
# use to decide -- must reject this package on identity. Asserting the observed
# identity alone would not do it: that would test _inst_observe(), leaving the
# arms' identity REQUIREMENT free to be removed unnoticed.
if(NOT _dc_identity STREQUAL "DECOY")
    _fail(decoy.identity_observed
          "the shared observation reported '${_dc_identity}' identity, not "
          "DECOY, so this control is not observing the package it built.")
endif()
_inst_judge(_dcv "${_dc_rc}" "${_dc_cache}" "${_dc_identity}" "${_dc_marker}")
if(NOT _dcv STREQUAL "WRONG_IDENTITY")
    _fail(decoy.identity_accepted
          "the shared installed-arm judgement returned '${_dcv}' for a package "
          "that is NOT the fixture; WRONG_IDENTITY is required, or the seven "
          "real arms would accept a foreign package emitting the same provider "
          "text.")
endif()

# 2. With no package at the named location, nothing may stand in for the
#    fixture. Proving "configure failed" is not enough: a host package could be
#    discovered through the ordinary system prefixes and fail the same way. The
#    conclusion is made observable and exact.
set(_none "${WORK}/absent-prefix")
file(REMOVE_RECURSE "${_none}")
file(MAKE_DIRECTORY "${_none}")
set(_none_common "${_inst_common}")
list(TRANSFORM _none_common REPLACE "${_fixp}" "${_none}")
_inst_observe(_nf "${WORK}/i-nofixture" "${_UNKNOWN}" "${_MISMATCH}" ${_none_common})
_moq_reject_warnings(nofixture "${_nf_all}")

if(_nf_rc EQUAL 0)
    _fail(nofixture.configured
          "configure SUCCEEDED with no picoquic package at the named location.")
endif()
if(NOT _nf_cache)
    _fail(nofixture.cache
          "no cache was produced, so the fallback conclusion cannot be "
          "observed at all.")
else()
    # The decisive fact: the package was never found. Anything else means some
    # other picoquic satisfied the find and the failure came later.
    if(NOT _nf_cachetext MATCHES "picoquic_DIR:PATH=picoquic_DIR-NOTFOUND")
        _fail(nofixture.found_a_package
              "picoquic_DIR resolved to something; a host or fallback package "
              "stood in for the removed fixture.")
    endif()
endif()
if(NOT _nf_identity STREQUAL "NONE")
    _fail(nofixture.identity
          "a package marker (${_nf_identity}) was stamped although no package "
          "exists at the named location.")
endif()
# ... and the failure must be the expected FIND failure, not an arbitrary one.
if(NOT _nf_all MATCHES "picoquic not found")
    _fail(nofixture.wrong_failure
          "configure failed for some reason other than not finding picoquic, "
          "so the fallback conclusion is unfounded:\n${_nf_all}")
endif()
