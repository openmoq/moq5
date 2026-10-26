# Focused RED: LibMoQ's generated package metadata must record the provider the
# artifact was actually built with, so a nested dependency resolution cannot
# revert an mbedTLS artifact to picoquic's OpenSSL defaults.
#
# Driven by CTest with -DBUILD_CONFIG=<file> -DINSTALL_CONFIG=<file>
# -DWANT_VERIFIER=<openssl|mbedtls> -DWANT_WITH_OPENSSL=<ON|OFF>
# -DWANT_WITH_MBEDTLS=<ON|OFF>. Both are configs the build ACTUALLY generates,
# so implementing the metadata is all this test needs to become green.
#
# Three requirements, each independently distinguishable:
#   1. the recorded verifier backend is present and correct;
#   2. the recorded upstream WITH_OPENSSL is present and correct;
#   3. the recorded upstream WITH_MBEDTLS is present and correct.
# A pre-existing incompatible picoquic target is a fourth requirement that the
# metadata alone cannot satisfy; it is checked by the consumer tests.

# --- the ONE classifier -----------------------------------------------------
# Both the synthetic fixtures and the real generated configs are judged by this
# function and nothing else. A second copy could stay correct while the one
# that judges the real configs was broken, which is exactly what a self-check
# is supposed to prevent.
#
# Verdicts: ABSENT (no such record), WRONG (recorded, different value), OK.
function(_classify_metadata content key want out_verdict out_found)
    # ACTIVE, LINE-ANCHORED assignments only, in the CANONICAL syntax this test
    # owns and the package template emits:
    #
    #     set(_LIBMOQ_<KEY> <value>)          -- or a balanced-quoted value
    #
    # anchored through the end of the command, allowing only trailing
    # whitespace or a trailing line comment. Quoting must be BALANCED: two
    # independent optional quotes would accept `"ON` and `ON"`. An unanchored
    # first-match regex would additionally accept a commented-out record and
    # would stop at the first of several, so a correct record followed by a
    # conflicting duplicate would pass. Package metadata must carry exactly ONE
    # active assignment per key.
    #
    # This is deliberately NOT a general CMake parser: the generated record
    # syntax is narrow and owned here.
    #
    # Verdicts: ABSENT, WRONG, DUPLICATE, OK.
    string(REPLACE "\r" "" _c "${content}")
    string(REPLACE "\n" ";" _lines "${_c}")
    set(_values "")
    foreach(_line IN LISTS _lines)
        # Leading whitespace is allowed; a leading '#' is not. The tail after
        # the closing paren may only be whitespace or a line comment.
        if(_line MATCHES "^[ \t]*set\\(_LIBMOQ_${key}[ \t]+\"([A-Za-z0-9_]+)\"[ \t]*\\)[ \t]*(#.*)?$")
            list(APPEND _values "${CMAKE_MATCH_1}")
        elseif(_line MATCHES "^[ \t]*set\\(_LIBMOQ_${key}[ \t]+([A-Za-z0-9_]+)[ \t]*\\)[ \t]*(#.*)?$")
            list(APPEND _values "${CMAKE_MATCH_1}")
        endif()
    endforeach()
    list(LENGTH _values _n)
    if(_n EQUAL 0)
        set(${out_verdict} ABSENT PARENT_SCOPE)
        set(${out_found} "" PARENT_SCOPE)
        return()
    endif()
    list(GET _values 0 _first)
    set(${out_found} "${_first}" PARENT_SCOPE)
    if(_n GREATER 1)
        set(${out_verdict} DUPLICATE PARENT_SCOPE)
        return()
    endif()
    if(NOT "${_first}" STREQUAL "${want}")
        set(${out_verdict} WRONG PARENT_SCOPE)
    else()
        set(${out_verdict} OK PARENT_SCOPE)
    endif()
endfunction()

# --- self-check: exercise the SHARED classifier ----------------------------
# Mutating the classifier must be caught here, because the real-config checks
# below call the same function.
function(_selftest_expect name content key want expect_verdict)
    _classify_metadata("${content}" "${key}" "${want}" _v _found)
    if(NOT "${_v}" STREQUAL "${expect_verdict}")
        message(SEND_ERROR
            "FAIL[selftest.${name}]: the shared classifier said ${_v} "
            "(found '${_found}'), expected ${expect_verdict}")
    endif()
endfunction()

set(_good "set(_LIBMOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls)\nset(_LIBMOQ_PICOQUIC_WITH_OPENSSL OFF)\nset(_LIBMOQ_PICOQUIC_WITH_MBEDTLS ON)\n")
set(_missing "set(_LIBMOQ_PICOQUIC_WITH_OPENSSL OFF)\nset(_LIBMOQ_PICOQUIC_WITH_MBEDTLS ON)\n")
set(_inverted "set(_LIBMOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls)\nset(_LIBMOQ_PICOQUIC_WITH_OPENSSL ON)\nset(_LIBMOQ_PICOQUIC_WITH_MBEDTLS ON)\n")
# only a COMMENTED record for the verifier: suggestive text, not metadata
set(_commented "# set(_LIBMOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls)\nset(_LIBMOQ_PICOQUIC_WITH_OPENSSL OFF)\nset(_LIBMOQ_PICOQUIC_WITH_MBEDTLS ON)\n")
# two ACTIVE records for one key, equal and conflicting
set(_dup_equal "set(_LIBMOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls)\nset(_LIBMOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls)\nset(_LIBMOQ_PICOQUIC_WITH_OPENSSL OFF)\nset(_LIBMOQ_PICOQUIC_WITH_MBEDTLS ON)\n")
set(_dup_conflict "set(_LIBMOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls)\nset(_LIBMOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND openssl)\nset(_LIBMOQ_PICOQUIC_WITH_OPENSSL OFF)\nset(_LIBMOQ_PICOQUIC_WITH_MBEDTLS ON)\n")

# the complete fixture passes on every key
_selftest_expect(complete_verifier "${_good}" ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls OK)
_selftest_expect(complete_ossl     "${_good}" PICOQUIC_WITH_OPENSSL           OFF     OK)
_selftest_expect(complete_mbed     "${_good}" PICOQUIC_WITH_MBEDTLS           ON      OK)
# one key missing: that key is ABSENT, the unrelated keys still pass
_selftest_expect(missing_key       "${_missing}" ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls ABSENT)
_selftest_expect(missing_others_o  "${_missing}" PICOQUIC_WITH_OPENSSL           OFF     OK)
_selftest_expect(missing_others_m  "${_missing}" PICOQUIC_WITH_MBEDTLS           ON      OK)
# one boolean inverted: that key is WRONG, the unrelated keys still pass
_selftest_expect(inverted_bool     "${_inverted}" PICOQUIC_WITH_OPENSSL           OFF     WRONG)
_selftest_expect(inverted_others_v "${_inverted}" ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls OK)
_selftest_expect(inverted_others_m "${_inverted}" PICOQUIC_WITH_MBEDTLS           ON      OK)
# a COMMENTED record is not metadata, and the unrelated keys still pass
_selftest_expect(commented_only    "${_commented}" ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls ABSENT)
_selftest_expect(commented_others  "${_commented}" PICOQUIC_WITH_MBEDTLS           ON      OK)
# duplicates are rejected even when they agree, so "exactly one" is the rule
_selftest_expect(duplicate_equal   "${_dup_equal}" ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls DUPLICATE)
_selftest_expect(duplicate_others  "${_dup_equal}" PICOQUIC_WITH_MBEDTLS           ON      OK)
# ... and a correct first record cannot hide a conflicting second one
_selftest_expect(duplicate_conflict "${_dup_conflict}" ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls DUPLICATE)
# same-line trailing junk is not the canonical record
set(_trailing "set(_LIBMOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls) and more\nset(_LIBMOQ_PICOQUIC_WITH_MBEDTLS ON)\n")
_selftest_expect(trailing_junk    "${_trailing}" ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls ABSENT)
_selftest_expect(trailing_others  "${_trailing}" PICOQUIC_WITH_MBEDTLS           ON      OK)
# a trailing line COMMENT is still the canonical record
set(_trailcomment "set(_LIBMOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls) # provider\n")
_selftest_expect(trailing_comment "${_trailcomment}" ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls OK)
# a quoted value is canonical only when the quoting is BALANCED
set(_quoted "set(_LIBMOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND \"mbedtls\")\n")
_selftest_expect(quoted_balanced  "${_quoted}" ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls OK)
set(_badquote_open "set(_LIBMOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND \"mbedtls)\n")
_selftest_expect(quote_unclosed   "${_badquote_open}" ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls ABSENT)
set(_badquote_close "set(_LIBMOQ_ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls\")\n")
_selftest_expect(quote_unopened   "${_badquote_close}" ADAPTER_PICOQUIC_VERIFY_BACKEND mbedtls ABSENT)

# --- the real generated configs, through the SAME classifier ---------------
function(_expect_recorded file key want)
    if(NOT EXISTS "${file}")
        message(SEND_ERROR "package metadata absent: ${file}")
        return()
    endif()
    file(READ "${file}" _content)
    _classify_metadata("${_content}" "${key}" "${want}" _v _found)
    if("${_v}" STREQUAL "ABSENT")
        message(SEND_ERROR
            "FAIL[${key}]: ${file} records no _LIBMOQ_${key}. A consumer cannot "
            "know which provider this artifact was built with.")
    elseif("${_v}" STREQUAL "DUPLICATE")
        message(SEND_ERROR
            "FAIL[${key}]: ${file} carries MORE THAN ONE active _LIBMOQ_${key} "
            "assignment. A consumer's answer would depend on evaluation order, "
            "so exactly one record is required.")
    elseif("${_v}" STREQUAL "WRONG")
        message(SEND_ERROR
            "FAIL[${key}]: ${file} records '${_found}', built with '${want}'")
    endif()
endfunction()

foreach(_tree "${BUILD_CONFIG}" "${INSTALL_CONFIG}")
    _expect_recorded("${_tree}" ADAPTER_PICOQUIC_VERIFY_BACKEND "${WANT_VERIFIER}")
    _expect_recorded("${_tree}" PICOQUIC_WITH_OPENSSL "${WANT_WITH_OPENSSL}")
    _expect_recorded("${_tree}" PICOQUIC_WITH_MBEDTLS "${WANT_WITH_MBEDTLS}")
endforeach()
