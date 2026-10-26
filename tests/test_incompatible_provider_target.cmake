# Focused RED: a pre-existing, provider-INCOMPATIBLE picoquic target must be
# refused with an actionable diagnostic rather than accepted or misclassified.
#
# Driven with -DCMAKE_EXE=<cmake> -DSRC=<consumer dir> -DBUILD_DIR=<work>
# -DPREFIX_PATH=<libmoq build tree> -DMARKER=<the other provider's picotls lib>
# -DWANT_SELECTED=<provider> -DWANT_OBSERVED=<other provider>.

# Script mode starts from CMake's oldest policy defaults; run with the
# project's floor so if(IN_LIST) and variable references behave as they do
# in the build itself.
cmake_minimum_required(VERSION 3.20)

include("${CMAKE_CURRENT_LIST_DIR}/moq_subprocess_warnings.cmake")

if(NOT WANT_SELECTED OR NOT WANT_OBSERVED)
    message(FATAL_ERROR
        "the expected selected/observed provider names are required; without "
        "them the oracle could only check that SOME facts were present")
endif()

# --- the ONE guard-invocation classifier ------------------------------------
# `if(COMMAND _libmoq_guard_picoquic_target)` proves only that the generated
# config DEFINED the function: deleting the call site leaves it defined and
# such a check stays green. Execution is proven instead from a configure trace
# restricted to the generated package config, so the statement must actually
# have run. Works identically for the build-tree and installed configs, and
# adds no switch to the package.
#
# Verdicts: RAN, NOT_RUN.
function(_classify_guard_ran trace out_verdict)
    string(REPLACE "\r" "" _t "${trace}")
    string(REPLACE "\n" ";" _lines "${_t}")
    foreach(_line IN LISTS _lines)
        # A trace line is `<file>(<line>):  <statement>`; require the statement
        # to be the guard invocation and the file to be the package config.
        # `/` or start-of-line before the name: `notlibmoqConfig.cmake` is a
        # different file, not this package's config.
        if(_line MATCHES "(^|/)libmoqConfig\\.cmake\\([0-9]+\\):[ \t]+_libmoq_guard_picoquic_target\\(")
            set(${out_verdict} RAN PARENT_SCOPE)
            return()
        endif()
    endforeach()
    set(${out_verdict} NOT_RUN PARENT_SCOPE)
endfunction()

function(_guard_control name trace expect)
    _classify_guard_ran("${trace}" _v)
    if(NOT "${_v}" STREQUAL "${expect}")
        message(SEND_ERROR
            "FAIL[guard_control.${name}]: the shared guard classifier said "
            "${_v}, expected ${expect}")
    endif()
endfunction()

_guard_control(invoked
    "/x/libmoqConfig.cmake(316):  _libmoq_guard_picoquic_target()\n" RAN)
# merely DECLARING it is not running it
_guard_control(declared_only
    "/x/libmoqConfig.cmake(120):  function(_libmoq_guard_picoquic_target )\n"
    NOT_RUN)
# a call from some OTHER file is not this package's guard
_guard_control(other_file
    "/x/Other.cmake(7):  _libmoq_guard_picoquic_target()\n" NOT_RUN)
# a similarly-named statement is a different statement
_guard_control(prefixed
    "/x/libmoqConfig.cmake(316):  x_libmoq_guard_picoquic_target()\n" NOT_RUN)
# a file whose name merely ENDS with the config's name is a different file
_guard_control(prefixed_file
    "/x/notlibmoqConfig.cmake(316):  _libmoq_guard_picoquic_target()\n" NOT_RUN)

# --- the ONE classifier -----------------------------------------------------
# Used for the real configure output and for every synthetic control, so a
# control cannot pass against a different implementation than the one that
# judges the product.
#
# LibMoQ OWNS this diagnostic, so the record grammar is defined exactly rather
# than sniffed: on one physical line, the bare token
# `LIBMOQ_PICOQUIC_PROVIDER_MISMATCH` appears exactly once as a
# WHITESPACE-DELIMITED FIELD, accompanied by exactly one `selected=<value>`
# field and exactly one `observed=<value>` field. Splitting on whitespace makes
# the boundaries real: a `-`-prefixed or `-`-suffixed token is a different
# field, two tokens on one line are counted, and a duplicated `selected=` is
# visible instead of being satisfied by whichever came first.
#
# Verdicts: NO_TOKEN, MULTIPLE, BAD_SELECTED, BAD_OBSERVED, OK.
function(_classify_mismatch text selected observed out_verdict)
    _classify_record("${text}" LIBMOQ_PICOQUIC_PROVIDER_MISMATCH
                     "${selected}" "${observed}" _v)
    set(${out_verdict} "${_v}" PARENT_SCOPE)
endfunction()

# The single record implementation. `_classify_mismatch` above and the
# fail-closed classifier below are both thin wrappers over it, so there is one
# grammar and one place a mutation can be caught.
function(_classify_record text token selected observed out_verdict)
    string(REPLACE "\r" "" _t "${text}")
    string(REPLACE "\n" ";" _lines "${_t}")
    set(_records "")
    set(_rec_tokens "")
    set(_rec_sel "")
    set(_rec_obs "")
    foreach(_line IN LISTS _lines)
        # Split the line into whitespace-delimited fields.
        string(REGEX REPLACE "[ \t]+" ";" _fields "${_line}")
        set(_ntok 0)
        set(_sel "")
        set(_obs "")
        set(_nsel 0)
        set(_nobs 0)
        foreach(_f IN LISTS _fields)
            if(_f STREQUAL "${token}")
                math(EXPR _ntok "${_ntok} + 1")
            elseif(_f MATCHES "^selected=(.*)$")
                math(EXPR _nsel "${_nsel} + 1")
                set(_sel "${CMAKE_MATCH_1}")
            elseif(_f MATCHES "^observed=(.*)$")
                math(EXPR _nobs "${_nobs} + 1")
                set(_obs "${CMAKE_MATCH_1}")
            endif()
        endforeach()
        if(_ntok GREATER 0)
            list(APPEND _records "${_line}")
            list(APPEND _rec_tokens "${_ntok}")
            list(APPEND _rec_sel "${_nsel}:${_sel}")
            list(APPEND _rec_obs "${_nobs}:${_obs}")
        endif()
    endforeach()

    list(LENGTH _records _n)
    if(_n EQUAL 0)
        set(${out_verdict} NO_TOKEN PARENT_SCOPE)
        return()
    endif()
    if(_n GREATER 1)
        set(${out_verdict} MULTIPLE PARENT_SCOPE)
        return()
    endif()
    # Exactly one record, and the token must occur exactly once WITHIN it.
    list(GET _rec_tokens 0 _ntok)
    if(NOT _ntok EQUAL 1)
        set(${out_verdict} MULTIPLE PARENT_SCOPE)
        return()
    endif()

    list(GET _rec_sel 0 _selpair)
    string(FIND "${_selpair}" ":" _i)
    string(SUBSTRING "${_selpair}" 0 ${_i} _nsel)
    math(EXPR _i "${_i} + 1")
    string(SUBSTRING "${_selpair}" ${_i} -1 _got_sel)
    if(NOT _nsel EQUAL 1 OR NOT _got_sel STREQUAL selected)
        set(${out_verdict} BAD_SELECTED PARENT_SCOPE)
        return()
    endif()

    list(GET _rec_obs 0 _obspair)
    string(FIND "${_obspair}" ":" _j)
    string(SUBSTRING "${_obspair}" 0 ${_j} _nobs)
    math(EXPR _j "${_j} + 1")
    string(SUBSTRING "${_obspair}" ${_j} -1 _got_obs)
    if(NOT _nobs EQUAL 1 OR NOT _got_obs STREQUAL observed)
        set(${out_verdict} BAD_OBSERVED PARENT_SCOPE)
        return()
    endif()

    set(${out_verdict} OK PARENT_SCOPE)
endfunction()

# --- synthetic controls, through the SAME classifier -----------------------
function(_control name text expect)
    _classify_mismatch("${text}" "${WANT_SELECTED}" "${WANT_OBSERVED}" _v)
    if(NOT "${_v}" STREQUAL "${expect}")
        message(SEND_ERROR
            "FAIL[control.${name}]: the shared classifier said ${_v}, "
            "expected ${expect}")
    endif()
endfunction()

set(_valid "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCH selected=${WANT_SELECTED} observed=${WANT_OBSERVED} \n")
_control(valid_record        "${_valid}" OK)
_control(token_absent        "CMake Error: providers disagree selected=${WANT_SELECTED} observed=${WANT_OBSERVED} \n" NO_TOKEN)
_control(selected_empty      "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCH selected= observed=${WANT_OBSERVED} \n" BAD_SELECTED)
_control(selected_wrong      "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCH selected=nonsense observed=${WANT_OBSERVED} \n" BAD_SELECTED)
_control(observed_empty      "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCH selected=${WANT_SELECTED} observed= \n" BAD_OBSERVED)
_control(observed_wrong      "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCH selected=${WANT_SELECTED} observed=nonsense \n" BAD_OBSERVED)
# facts present but on a DIFFERENT line from the token: not one record
_control(facts_other_line    "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCH \n-- selected=${WANT_SELECTED} observed=${WANT_OBSERVED} \n" BAD_SELECTED)
_control(unrelated_failure   "CMake Error at CMakeLists.txt:3 (message):\n  unrelated configure failure\n" NO_TOKEN)

# --- exactness controls: a value SUFFIX must not satisfy the expectation ----
_control(selected_suffix     "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCH selected=${WANT_SELECTED}-extra observed=${WANT_OBSERVED} \n" BAD_SELECTED)
_control(observed_suffix     "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCH selected=${WANT_SELECTED} observed=${WANT_OBSERVED}-extra \n" BAD_OBSERVED)
# --- a PREFIXED field name is a different field ----------------------------
_control(selected_prefixed   "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCH notselected=${WANT_SELECTED} observed=${WANT_OBSERVED} \n" BAD_SELECTED)
_control(observed_prefixed   "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCH selected=${WANT_SELECTED} unobserved=${WANT_OBSERVED} \n" BAD_OBSERVED)
# --- a PREFIXED/SUFFIXED token is a different token ------------------------
_control(token_prefixed      "CMake Error: XLIBMOQ_PICOQUIC_PROVIDER_MISMATCH selected=${WANT_SELECTED} observed=${WANT_OBSERVED} \n" NO_TOKEN)
_control(token_suffixed      "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCHX selected=${WANT_SELECTED} observed=${WANT_OBSERVED} \n" NO_TOKEN)
# --- duplicates: exactly one record, so a valid one cannot hide a bad one --
_control(duplicate_equal     "${_valid}${_valid}" MULTIPLE)
_control(duplicate_conflict
    "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCH selected=${WANT_SELECTED} observed=${WANT_OBSERVED} \nCMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCH selected=nonsense observed=nonsense \n"
    MULTIPLE)
# --- hyphenated token spellings are DIFFERENT fields ------------------------
_control(token_hyphen_prefix "CMake Error: prefix-LIBMOQ_PICOQUIC_PROVIDER_MISMATCH selected=${WANT_SELECTED} observed=${WANT_OBSERVED} \n" NO_TOKEN)
_control(token_hyphen_suffix "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCH-suffix selected=${WANT_SELECTED} observed=${WANT_OBSERVED} \n" NO_TOKEN)
# --- two tokens on ONE physical line are two records ------------------------
_control(two_tokens_one_line "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCH LIBMOQ_PICOQUIC_PROVIDER_MISMATCH selected=${WANT_SELECTED} observed=${WANT_OBSERVED} \n" MULTIPLE)
# --- a duplicated field must not be satisfied by whichever came first -------
_control(selected_dup_equal    "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCH selected=${WANT_SELECTED} selected=${WANT_SELECTED} observed=${WANT_OBSERVED} \n" BAD_SELECTED)
_control(selected_dup_conflict "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCH selected=${WANT_SELECTED} selected=nonsense observed=${WANT_OBSERVED} \n" BAD_SELECTED)
_control(observed_dup_equal    "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCH selected=${WANT_SELECTED} observed=${WANT_OBSERVED} observed=${WANT_OBSERVED} \n" BAD_OBSERVED)
_control(observed_dup_conflict "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_MISMATCH selected=${WANT_SELECTED} observed=${WANT_OBSERVED} observed=nonsense \n" BAD_OBSERVED)

# --- the product RED --------------------------------------------------------
file(REMOVE_RECURSE "${BUILD_DIR}")
execute_process(
    COMMAND "${CMAKE_EXE}" -S "${SRC}" -B "${BUILD_DIR}"
            "-DCMAKE_PREFIX_PATH=${PREFIX_PATH}"
            "-DMOQ_INCOMPATIBLE_MARKER=${MARKER}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _err)
set(_all "${_out}${_err}")

# Warnings are judged BEFORE the outcome oracle, on every path.
_moq_reject_warnings(incompatible_target "${_all}")
if(_rc EQUAL 0)
    message(SEND_ERROR
        "FAIL[incompatible_target.accepted]: find_package(libmoq) configured "
        "successfully against a pre-existing picoquic target built for the "
        "other provider. Compatibility is not checked, so an mbedTLS artifact "
        "can be consumed against an OpenSSL-built picoquic (and vice versa).")
else()
    _classify_mismatch("${_all}" "${WANT_SELECTED}" "${WANT_OBSERVED}" _v)
    if(NOT "${_v}" STREQUAL "OK")
        message(SEND_ERROR
            "FAIL[incompatible_target.diagnostic]: it failed, but the "
            "classifier said ${_v}. One record must carry "
            "LIBMOQ_PICOQUIC_PROVIDER_MISMATCH with selected=${WANT_SELECTED} "
            "and observed=${WANT_OBSERVED}:\n${_all}")
    endif()
endif()

# --- negative control against the SAME classifier ---------------------------
# An unrelated configure failure must fail to configure and must not satisfy
# the oracle. Without this, an oracle keyed on any failure text would pass.
file(REMOVE_RECURSE "${BUILD_DIR}-negctl")
file(MAKE_DIRECTORY "${BUILD_DIR}-negctl-src")
file(WRITE "${BUILD_DIR}-negctl-src/CMakeLists.txt"
     "cmake_minimum_required(VERSION 3.20)\n"
     "project(moq_negctl C)\n"
     "message(FATAL_ERROR \"unrelated configure failure\")\n")
execute_process(
    COMMAND "${CMAKE_EXE}" -S "${BUILD_DIR}-negctl-src" -B "${BUILD_DIR}-negctl"
    RESULT_VARIABLE _nrc OUTPUT_VARIABLE _nout ERROR_VARIABLE _nerr)
set(_nall "${_nout}${_nerr}")
_moq_reject_warnings(negative_control "${_nall}")
if(_nrc EQUAL 0)
    message(SEND_ERROR
        "FAIL[negative_control.configured]: the unrelated-failure control "
        "configured successfully, so it proves nothing.")
else()
    _classify_mismatch("${_nall}" "${WANT_SELECTED}" "${WANT_OBSERVED}" _nv)
    if("${_nv}" STREQUAL "OK")
        message(SEND_ERROR
            "FAIL[negative_control.accepted]: an unrelated configure failure "
            "satisfied the mismatch oracle:\n${_nall}")
    endif()
endif()

# --- fail-closed classifier: the SAME record grammar, a different token ------
# Unknown or contradictory provider evidence is not a mismatch and must not be
# reported as one, so it carries its own token and its own verdicts through the
# one record implementation above.
function(_classify_failclosed text selected observed out_verdict)
    _classify_record("${text}" LIBMOQ_PICOQUIC_PROVIDER_EVIDENCE
                     "${selected}" "${observed}" _v)
    set(${out_verdict} "${_v}" PARENT_SCOPE)
endfunction()

_control(failclosed_is_not_mismatch
    "CMake Error: LIBMOQ_PICOQUIC_PROVIDER_EVIDENCE selected=${WANT_SELECTED} observed=unknown \n"
    NO_TOKEN)

# --- topology arms ----------------------------------------------------------
# Provider evidence must be STRUCTURAL: followed through aliases and transitive
# interface dependencies, and matched on exact atoms. Each arm below builds a
# pre-existing picoquic target with a different closure shape and declares the
# exact outcome, all judged by the classifiers above. A shallow implementation
# -- one assumed target spelling, a direct-link-only walk, or substring
# matching -- fails at least one arm.
#
# SELECTED_MARKER is the selected provider's atom; MARKER is the other one.
# Markers are comma-separated ATOM SETS: a provider is proven by a complete
# signature, so these are expanded into every atom the signature needs.
string(REPLACE "," ";" _marker_atoms "${MARKER}")
string(REPLACE "," ";" _selected_atoms "${SELECTED_MARKER}")
set(_decoy_atoms "")
foreach(_a IN LISTS _selected_atoms)
    list(APPEND _decoy_atoms "x${_a}" "${_a}-extra")
endforeach()

if(NOT SELECTED_MARKER)
    message(FATAL_ERROR
        "SELECTED_MARKER is required: without the selected provider's own atom "
        "the accept arms could not distinguish 'evidence found' from 'nothing "
        "checked'")
endif()

# `graph` is the CMake text that defines the pre-existing picoquic target.
# `expect` is REFUSE_MISMATCH, REFUSE_EVIDENCE, or ACCEPT.
function(_topology name graph expect)
    set(_src "${BUILD_DIR}-topo-${name}-src")
    set(_bld "${BUILD_DIR}-topo-${name}")
    file(REMOVE_RECURSE "${_src}" "${_bld}")
    file(MAKE_DIRECTORY "${_src}")
    file(WRITE "${_src}/CMakeLists.txt"
         "cmake_minimum_required(VERSION 3.20)\n"
         "project(moq_topo_${name} LANGUAGES C)\n"
         "${graph}"
         "find_package(libmoq REQUIRED COMPONENTS adapter-picoquic)\n"
         "message(STATUS \"MOQ_TOPOLOGY_ACCEPTED\")\n")
    execute_process(
        COMMAND "${CMAKE_EXE}" -S "${_src}" -B "${_bld}"
                "-DCMAKE_PREFIX_PATH=${PREFIX_PATH}"
        RESULT_VARIABLE _trc OUTPUT_VARIABLE _tout ERROR_VARIABLE _terr)
    set(_tall "${_tout}${_terr}")
    _moq_reject_warnings(topology_${name} "${_tall}")

    if("${expect}" STREQUAL "ACCEPT")
        if(NOT _trc EQUAL 0)
            message(SEND_ERROR
                "FAIL[topology.${name}.refused]: a closure that DOES carry the "
                "selected provider was refused:\n${_tall}")
        endif()
        # ... and it must not have emitted either refusal record
        _classify_mismatch("${_tall}" "${WANT_SELECTED}" "${WANT_OBSERVED}" _mv)
        if("${_mv}" STREQUAL "OK")
            message(SEND_ERROR
                "FAIL[topology.${name}.spurious_mismatch]: accepted, but a "
                "mismatch record was emitted anyway")
        endif()
    elseif("${expect}" STREQUAL "REFUSE_MISMATCH")
        if(_trc EQUAL 0)
            message(SEND_ERROR
                "FAIL[topology.${name}.accepted]: a picoquic target whose "
                "closure proves the OTHER provider was accepted:\n${_tall}")
        else()
            _classify_mismatch("${_tall}" "${WANT_SELECTED}" "${WANT_OBSERVED}" _mv)
            if(NOT "${_mv}" STREQUAL "OK")
                message(SEND_ERROR
                    "FAIL[topology.${name}.diagnostic]: refused, but the "
                    "classifier said ${_mv}; one "
                    "LIBMOQ_PICOQUIC_PROVIDER_MISMATCH record with "
                    "selected=${WANT_SELECTED} observed=${WANT_OBSERVED} is "
                    "required:\n${_tall}")
            endif()
        endif()
    else()   # REFUSE_EVIDENCE
        if(_trc EQUAL 0)
            message(SEND_ERROR
                "FAIL[topology.${name}.accepted]: a picoquic target whose "
                "closure proves NOTHING about the provider was accepted; "
                "unknown evidence must fail closed:\n${_tall}")
        else()
            _classify_failclosed("${_tall}" "${WANT_SELECTED}" unknown _fv)
            if(NOT "${_fv}" STREQUAL "OK")
                message(SEND_ERROR
                    "FAIL[topology.${name}.diagnostic]: refused, but the "
                    "fail-closed classifier said ${_fv}; one "
                    "LIBMOQ_PICOQUIC_PROVIDER_EVIDENCE record with "
                    "selected=${WANT_SELECTED} observed=unknown is required, "
                    "and it must NOT be reported as a mismatch:\n${_tall}")
            endif()
        endif()
    endif()
endfunction()

# 1. evidence behind an ALIAS and an INTERMEDIATE interface target. A walk that
#    stops at the alias, or looks only at direct link libraries, sees nothing.
_topology(alias_intermediate
    "add_library(moq_topo_atom INTERFACE)\ntarget_link_libraries(moq_topo_atom INTERFACE ${_marker_atoms})\nadd_library(moq_topo_mid INTERFACE)\ntarget_link_libraries(moq_topo_mid INTERFACE moq_topo_atom)\nadd_library(moq_topo_root INTERFACE)\ntarget_link_libraries(moq_topo_root INTERFACE moq_topo_mid)\nadd_library(picoquic::picoquic-core ALIAS moq_topo_root)\nadd_library(picoquic::picoquic-log ALIAS moq_topo_root)\n"
    REFUSE_MISMATCH)

# 2. a PREFIXED/SUFFIXED decoy of the selected atom must not count as evidence:
#    the real evidence here is the opposite provider only.
_topology(decoy_atoms
    "add_library(moq_topo_atom INTERFACE)\ntarget_link_libraries(moq_topo_atom INTERFACE ${_decoy_atoms} ${_marker_atoms})\nadd_library(picoquic::picoquic-core ALIAS moq_topo_atom)\nadd_library(picoquic::picoquic-log ALIAS moq_topo_atom)\n"
    REFUSE_MISMATCH)

# 3. a BOTH-provider closure is valid when the selected verifier is present.
_topology(both_providers
    "add_library(moq_topo_atom INTERFACE)\ntarget_link_libraries(moq_topo_atom INTERFACE ${_selected_atoms} ${_marker_atoms})\nadd_library(picoquic::picoquic-core ALIAS moq_topo_atom)\nadd_library(picoquic::picoquic-log ALIAS moq_topo_atom)\n"
    ACCEPT)

# 4. selected-only, behind the same alias/intermediate shape as arm 1, so the
#    accept is not an artifact of a shallower walk.
_topology(selected_only
    "add_library(moq_topo_atom INTERFACE)\ntarget_link_libraries(moq_topo_atom INTERFACE ${_selected_atoms})\nadd_library(moq_topo_mid INTERFACE)\ntarget_link_libraries(moq_topo_mid INTERFACE moq_topo_atom)\nadd_library(picoquic::picoquic-core ALIAS moq_topo_mid)\nadd_library(picoquic::picoquic-log ALIAS moq_topo_mid)\n"
    ACCEPT)

# 5. a closure that proves NOTHING must fail closed, and must be classified
#    separately from a mismatch.
_topology(unknown_evidence
    "add_library(moq_topo_atom INTERFACE)\nadd_library(picoquic::picoquic-core ALIAS moq_topo_atom)\nadd_library(picoquic::picoquic-log ALIAS moq_topo_atom)\n"
    REFUSE_EVIDENCE)

# --- an explicit CONFLICTING consumer provider fact -------------------------
# No pre-existing target here, so nested discovery runs. A consumer that has
# explicitly stated the OPPOSITE provider must not be silently overwritten with
# the recorded facts, and must not be reported as a target mismatch: it is a
# contradiction between two authorities, classified on its own.
#
# The compatible counterpart proves the arm is not simply "any consumer
# statement is refused".
function(_consumer_fact name defs expect)
    set(_preset "${ARGV3}")
    set(_cf_prefix "${ARGV4}")
    set(_cf_tree "${ARGV5}")
    if("${_cf_prefix}" STREQUAL "")
        set(_cf_prefix "${PREFIX_PATH}")
    endif()
    if("${_cf_tree}" STREQUAL "")
        set(_cf_tree "build")
    endif()
    set(name "${_cf_tree}.${name}")
    set(_src "${BUILD_DIR}-fact-${name}-src")
    set(_bld "${BUILD_DIR}-fact-${name}")
    file(REMOVE_RECURSE "${_src}" "${_bld}")
    file(MAKE_DIRECTORY "${_src}")
    # The consumer records its own values BEFORE the call, defines a
    # selected-provider picoquic target so the structural guard has something
    # to judge, and reports both that the guard ran and that its values came
    # back unchanged.
    file(WRITE "${_src}/CMakeLists.txt"
         "cmake_minimum_required(VERSION 3.20)\n"
         "project(moq_fact_${name} LANGUAGES C)\n"
         "${_preset}"
         "set(_b_o \"\${WITH_OPENSSL}\")\n"
         "set(_b_m \"\${WITH_MBEDTLS}\")\n"
         "${_selected_graph}"
         "find_package(libmoq REQUIRED COMPONENTS adapter-picoquic)\n"
         "if(NOT \"\${WITH_OPENSSL}\" STREQUAL \"\${_b_o}\" OR NOT \"\${WITH_MBEDTLS}\" STREQUAL \"\${_b_m}\")\n"
         "  message(FATAL_ERROR \"MOQ_FACT_REWRITTEN\")\n"
         "endif()\n"
         "message(STATUS \"MOQ_FACT_PRESERVED\")\n"
         "message(STATUS \"MOQ_FACT_ACCEPTED\")\n")
    # The same discovery inputs the real adapter consumers get: without them
    # the arm would "fail" because picoquic is simply not findable, which is
    # not the contradiction under test.
    # The trace is the guard-INVOCATION instrument, and it is used ONLY on the
    # arms that must prove the guard ran. A refusal arm is judged by its record
    # grammar, and the trace echoes the very message(FATAL_ERROR ...) statement
    # that carries the record -- the token would then appear twice and "exactly
    # one record" would be violated by the instrument rather than by the
    # product. CMake also turns developer warnings on whenever tracing is
    # enabled, so -Wno-dev keeps the traced run's diagnostic surface identical
    # to the untraced ones: real warnings still fail the gate below.
    set(_fact_trace "")
    if("${expect}" STREQUAL "ACCEPT")
        set(_fact_trace --trace-source=libmoqConfig.cmake --trace-expand -Wno-dev)
    endif()
    execute_process(
        COMMAND "${CMAKE_EXE}" -S "${_src}" -B "${_bld}" ${_fact_trace}
                "-DCMAKE_PREFIX_PATH=${_cf_prefix}"
                "-DMOQ_PICOQUIC_SOURCE_DIR=${PICOQUIC_SRC}"
                "-DMOQ_PICOTLS_PREFIX=${PICOTLS_PREFIX}" ${defs}
        RESULT_VARIABLE _frc OUTPUT_VARIABLE _fout ERROR_VARIABLE _ferr)
    set(_fall "${_fout}${_ferr}")
    _moq_reject_warnings(consumer_fact_${name} "${_fall}")
    if("${expect}" STREQUAL "ACCEPT")
        _classify_guard_ran("${_fall}" _gv)
        if(NOT _frc EQUAL 0)
            message(SEND_ERROR
                "FAIL[fact.${name}.refused]: a consumer statement that AGREES "
                "with the recorded provider was refused:\n${_fall}")
        elseif(NOT "${_gv}" STREQUAL "RAN")
            message(SEND_ERROR
                "FAIL[fact.${name}.guard_skipped]: it configured, but the "
                "structural provider guard was never INVOKED -- accepting "
                "because nothing was checked proves nothing.")
        elseif(NOT _fall MATCHES "MOQ_FACT_PRESERVED")
            message(SEND_ERROR
                "FAIL[fact.${name}.rewritten]: it configured, but the "
                "consumer's own explicit value did not survive the call "
                "unchanged:\n${_fall}")
        endif()
    elseif("${expect}" STREQUAL "UNPARSABLE")
        if(_frc EQUAL 0)
            message(SEND_ERROR
                "FAIL[fact.${name}.guessed]: a consumer set a value that is "
                "not a CMake boolean and it was accepted anyway:\n${_fall}")
        else()
            _classify_failclosed("${_fall}" "${WANT_SELECTED}" unparsable _uv)
            if(NOT "${_uv}" STREQUAL "OK")
                message(SEND_ERROR
                    "FAIL[fact.${name}.diagnostic]: refused, but the "
                    "fail-closed classifier said ${_uv}; one "
                    "LIBMOQ_PICOQUIC_PROVIDER_EVIDENCE record with "
                    "selected=${WANT_SELECTED} observed=unparsable is "
                    "required:\n${_fall}")
            endif()
        endif()
    else()
        if(_frc EQUAL 0)
            message(SEND_ERROR
                "FAIL[fact.${name}.overwritten]: a consumer that explicitly "
                "stated the opposite provider configured successfully, so its "
                "statement was silently overwritten:\n${_fall}")
        else()
            _classify_failclosed("${_fall}" "${WANT_SELECTED}" conflict _cv)
            if(NOT "${_cv}" STREQUAL "OK")
                message(SEND_ERROR
                    "FAIL[fact.${name}.diagnostic]: refused, but the "
                    "fail-closed classifier said ${_cv}; one "
                    "LIBMOQ_PICOQUIC_PROVIDER_EVIDENCE record with "
                    "selected=${WANT_SELECTED} observed=conflict is "
                    "required:\n${_fall}")
            endif()
        endif()
    endif()
endfunction()

# Consumers legitimately spell booleans several ways. EVERY compatible spelling
# must be accepted, must still reach the structural guard, and must come back
# EXACTLY as the consumer wrote it; a genuine opposite pair must be refused as a
# conflict; and an unrecognized token must be refused as unparsable rather than
# guessed at. These are the permanent forms, not a one-off measurement.
if("${WANT_SELECTED}" STREQUAL "mbedtls")
    set(_true_is  "MBEDTLS")
    set(_false_is "OPENSSL")
else()
    set(_true_is  "OPENSSL")
    set(_false_is "MBEDTLS")
endif()

# name;true-spelling;false-spelling
foreach(_spell "canonical;ON;OFF" "truefalse;TRUE;FALSE" "onezero;1;0"
               "mixedcase;YeS;nO")
    string(REPLACE ";" ";" _sp "${_spell}")
    list(GET _sp 0 _sp_name)
    list(GET _sp 1 _sp_true)
    list(GET _sp 2 _sp_false)
    _consumer_fact(compatible_${_sp_name}
        "-DWITH_${_true_is}=${_sp_true};-DWITH_${_false_is}=${_sp_false}"
        ACCEPT)
endforeach()

# a genuine opposite pair, in a non-canonical spelling so the refusal cannot be
# an artifact of lexical equality
_consumer_fact(conflicting
    "-DWITH_${_true_is}=FALSE;-DWITH_${_false_is}=TRUE" REFUSE)
# ... and an unknown token is classified on its own
_consumer_fact(unparsable "-DWITH_${_true_is}=MAYBE" UNPARSABLE)

# The same compatible statement made as an ORDINARY variable rather than a
# cache entry. This is the form that catches a resolver which overwrites the
# consumer's value: with a cache entry, dropping the overwrite would merely
# expose the identical cache value again and the damage would be invisible.
_consumer_fact(compatible_normal_var "" ACCEPT
    "set(WITH_${_true_is} TRUE)\nset(WITH_${_false_is} FALSE)\n")

# The same acceptance and guard-invocation proofs against the INSTALLED
# package, so a declaration-only guard cannot hide in either tree.
if(_route_install_prefix)
    _consumer_fact(compatible_canonical
        "-DWITH_${_true_is}=ON;-DWITH_${_false_is}=OFF" ACCEPT ""
        "${_route_install_prefix}" install)
    _consumer_fact(compatible_truefalse
        "-DWITH_${_true_is}=TRUE;-DWITH_${_false_is}=FALSE" ACCEPT ""
        "${_route_install_prefix}" install)
    _consumer_fact(conflicting
        "-DWITH_${_true_is}=FALSE;-DWITH_${_false_is}=TRUE" REFUSE ""
        "${_route_install_prefix}" install)
endif()

# --- F4 partial-evidence controls -------------------------------------------
# A provider is proven by a COMPLETE signature. A single generic crypto/TLS
# library, or an incomplete tuple, proves nothing and must classify as unknown
# rather than as the provider -- otherwise an incidental dependency would be
# read as provider identity.
#
# These arms only exist where the selected provider's own signature is a
# multi-atom tuple or the opposite provider's is; each individual atom of the
# generic mbedTLS tuple, and each incomplete pair, is exercised.
set(_generic_mbedtls "mbedtls;mbedx509;mbedcrypto")

foreach(_one IN LISTS _generic_mbedtls)
    _topology(partial_single_${_one}
        "add_library(moq_topo_atom INTERFACE)\ntarget_link_libraries(moq_topo_atom INTERFACE \"${_one}\")\nadd_library(picoquic::picoquic-core ALIAS moq_topo_atom)\nadd_library(picoquic::picoquic-log ALIAS moq_topo_atom)\n"
        REFUSE_EVIDENCE)
endforeach()

foreach(_pair "mbedtls+mbedx509" "mbedtls+mbedcrypto" "mbedx509+mbedcrypto")
    string(REPLACE "+" ";" _pair_atoms "${_pair}")
    string(REPLACE "+" "_" _pair_name "${_pair}")
    _topology(partial_pair_${_pair_name}
        "add_library(moq_topo_atom INTERFACE)\ntarget_link_libraries(moq_topo_atom INTERFACE ${_pair_atoms})\nadd_library(picoquic::picoquic-core ALIAS moq_topo_atom)\nadd_library(picoquic::picoquic-log ALIAS moq_topo_atom)\n"
        REFUSE_EVIDENCE)
endforeach()

# An OpenSSL closure with an INCIDENTAL lone mbedTLS-family library is still
# OpenSSL -- not a both-provider closure, and certainly not a mismatch.
if("${WANT_SELECTED}" STREQUAL "openssl")
    _topology(openssl_with_incidental_mbedtls
        "add_library(moq_topo_atom INTERFACE)\ntarget_link_libraries(moq_topo_atom INTERFACE \"picotls-openssl\" \"mbedcrypto\")\nadd_library(picoquic::picoquic-core ALIAS moq_topo_atom)\nadd_library(picoquic::picoquic-log ALIAS moq_topo_atom)\n"
        ACCEPT)
endif()

# --- F1: every component ROUTE is guarded, in BOTH trees --------------------
# Picoquic resolution must be guarded on every route, not only the one the
# adapter-picoquic component takes. Each route below is requested DIRECTLY,
# once with an opposite-provider pre-existing target (must refuse) and once
# with the selected provider (must accept, so a refuse-all guard cannot pass),
# against the build-tree package AND the installed one.
#
# ROUTES is the set of components this build actually registered, so a lane
# that does not build pico-WT does not pretend to test it.
function(_route name component prefix tree graph expect)
    set(_src "${BUILD_DIR}-route-${tree}-${name}-src")
    set(_bld "${BUILD_DIR}-route-${tree}-${name}")
    file(REMOVE_RECURSE "${_src}" "${_bld}")
    file(MAKE_DIRECTORY "${_src}")
    file(WRITE "${_src}/CMakeLists.txt"
         "cmake_minimum_required(VERSION 3.20)\n"
         "project(moq_route_${name} LANGUAGES C)\n"
         "${graph}"
         "find_package(libmoq REQUIRED COMPONENTS ${component})\n"
         "message(STATUS \"MOQ_ROUTE_ACCEPTED\")\n")
    # No picoquic discovery inputs: this consumer DEFINES the picoquic targets
    # itself, so passing them would go unused and CMake would warn -- and a
    # warning is a failure here.
    execute_process(
        COMMAND "${CMAKE_EXE}" -S "${_src}" -B "${_bld}"
                "-DCMAKE_PREFIX_PATH=${prefix}"
        RESULT_VARIABLE _rrc OUTPUT_VARIABLE _rout ERROR_VARIABLE _rerr)
    set(_rall "${_rout}${_rerr}")
    _moq_reject_warnings(route_${tree}_${name} "${_rall}")
    if("${expect}" STREQUAL "ACCEPT")
        if(NOT _rrc EQUAL 0)
            message(SEND_ERROR
                "FAIL[route.${tree}.${name}.refused]: component "
                "'${component}' refused a picoquic target that DOES carry the "
                "selected provider:\n${_rall}")
        endif()
    else()
        if(_rrc EQUAL 0)
            message(SEND_ERROR
                "FAIL[route.${tree}.${name}.accepted]: component "
                "'${component}' resolved Picoquic WITHOUT the provider guard: "
                "an opposite-provider target was accepted:\n${_rall}")
        else()
            _classify_mismatch("${_rall}" "${WANT_SELECTED}" "${WANT_OBSERVED}" _rv)
            if(NOT "${_rv}" STREQUAL "OK")
                message(SEND_ERROR
                    "FAIL[route.${tree}.${name}.diagnostic]: component "
                    "'${component}' refused, but the classifier said ${_rv}: "
                    "the refusal must be the provider guard, not an unrelated "
                    "failure:\n${_rall}")
            endif()
        endif()
    endif()
endfunction()

set(_opposite_graph
    "add_library(moq_route_atom INTERFACE)\ntarget_link_libraries(moq_route_atom INTERFACE ${_marker_atoms})\nadd_library(picoquic::picoquic-core ALIAS moq_route_atom)\nadd_library(picoquic::picoquic-log ALIAS moq_route_atom)\nadd_library(picoquic::picohttp-core ALIAS moq_route_atom)\n")
set(_selected_graph
    "add_library(moq_route_atom INTERFACE)\ntarget_link_libraries(moq_route_atom INTERFACE ${_selected_atoms})\nadd_library(picoquic::picoquic-core ALIAS moq_route_atom)\nadd_library(picoquic::picoquic-log ALIAS moq_route_atom)\nadd_library(picoquic::picohttp-core ALIAS moq_route_atom)\n")

string(REPLACE "," ";" ROUTES "${ROUTES}")
if(NOT ROUTES)
    message(FATAL_ERROR
        "ROUTES is required: without the component set this build registered, "
        "the route cases could silently test nothing")
endif()
# The install-tree routes need a REAL installed prefix, not the staged config
# directory: a consumer resolves targets files too, not just libmoqConfig.
set(_route_install_prefix "")
if(INSTALL_TREE)
    set(_route_install_prefix "${BUILD_DIR}-route-install-prefix")
    file(REMOVE_RECURSE "${_route_install_prefix}")
    execute_process(
        COMMAND "${CMAKE_EXE}" --install "${INSTALL_TREE}"
                --prefix "${_route_install_prefix}"
        RESULT_VARIABLE _rirc OUTPUT_VARIABLE _riout ERROR_VARIABLE _rierr)
    _moq_reject_warnings(route_install "${_riout}${_rierr}")
    if(NOT _rirc EQUAL 0)
        message(SEND_ERROR
            "FAIL[route.install.setup]: could not install the package to test "
            "its install-tree routes:\n${_riout}${_rierr}")
        set(_route_install_prefix "")
    endif()
endif()

foreach(_tree_pair "build;${PREFIX_PATH}" "install;${_route_install_prefix}")
    list(GET _tree_pair 0 _tree)
    list(GET _tree_pair 1 _prefix)
    if("${_prefix}" STREQUAL "")
        continue()
    endif()
    foreach(_component IN LISTS ROUTES)
        string(REPLACE "-" "_" _rname "${_component}")
        _route("${_rname}_opposite" "${_component}" "${_prefix}" "${_tree}"
               "${_opposite_graph}" REFUSE_MISMATCH)
        _route("${_rname}_selected" "${_component}" "${_prefix}" "${_tree}"
               "${_selected_graph}" ACCEPT)
    endforeach()
endforeach()

# --- F1: a NOT-FOUND dependency must not leave this project's state behind --
# find_dependency() returns from the enclosing package config when a dependency
# is missing. Anything LibMoQ set up for that resolution therefore has to be
# torn down BEFORE the failure propagates, or the consumer inherits our
# temporary provider facts and location hints.
#
# The probe records, for every variable LibMoQ may touch, its definedness and
# exact value BEFORE the call and compares afterwards -- so a restored-but-
# rewritten value is caught as well as a leaked one.
set(_libmoq_touched_vars
    "WITH_OPENSSL;WITH_MBEDTLS;MOQ_PICOTLS_PREFIX;PTLS_INCLUDE_DIR;\
PTLS_CORE_LIBRARY;PTLS_MINICRYPTO_LIBRARY;MBEDTLS_INCLUDE_DIR;MBEDTLS_LIBRARY;\
MBEDTLS_CRYPTO;MBEDTLS_X509;MOQ_PICOQUIC_MBEDTLS_INCLUDE_DIR")
string(REPLACE ";" ";" _libmoq_touched_vars "${_libmoq_touched_vars}")

# --- the ONE not-found-message classifier -----------------------------------
# The diagnostic is LibMoQ-owned, so its required content is stated exactly:
# it must name LibMoQ, name the dependency Picoquic, and offer BOTH actionable
# routes. An unrelated failure must not satisfy it.
#
# Verdicts: OK, NO_LIBMOQ, NO_DEP, NO_PREFIX_ROUTE, NO_SOURCE_ROUTE.
function(_classify_notfound_message text out_verdict)
    if(NOT text MATCHES "libmoq")
        set(${out_verdict} NO_LIBMOQ PARENT_SCOPE)
    elseif(NOT text MATCHES "Picoquic")
        set(${out_verdict} NO_DEP PARENT_SCOPE)
    elseif(NOT text MATCHES "CMAKE_PREFIX_PATH")
        set(${out_verdict} NO_PREFIX_ROUTE PARENT_SCOPE)
    elseif(NOT text MATCHES "MOQ_PICOQUIC_SOURCE_DIR")
        set(${out_verdict} NO_SOURCE_ROUTE PARENT_SCOPE)
    else()
        set(${out_verdict} OK PARENT_SCOPE)
    endif()
endfunction()

# Extract exactly one `MOQ_NF_MESSAGE:` record from a transcript. Classifying
# the WHOLE transcript would let unrelated output containing the four required
# words satisfy the oracle while libmoq_NOT_FOUND_MESSAGE was empty.
#
# Verdicts: OK (one record, payload returned), NONE, MULTIPLE.
function(_extract_nf_record text out_verdict out_payload)
    string(REPLACE "\r" "" _t "${text}")
    string(REPLACE "\n" ";" _lines "${_t}")
    # Counted explicitly rather than by list length: appending an EMPTY payload
    # to an empty list leaves LENGTH at 0, so a degraded (empty) record would
    # be indistinguishable from no record at all -- which is exactly the case
    # the decoy control exercises.
    set(_n 0)
    set(_last "")
    foreach(_line IN LISTS _lines)
        if(_line MATCHES "MOQ_NF_MESSAGE:[ \t]*(.*)$")
            math(EXPR _n "${_n} + 1")
            set(_last "${CMAKE_MATCH_1}")
        endif()
    endforeach()
    if(_n EQUAL 0)
        set(${out_verdict} NONE PARENT_SCOPE)
        set(${out_payload} "" PARENT_SCOPE)
    elseif(_n GREATER 1)
        set(${out_verdict} MULTIPLE PARENT_SCOPE)
        set(${out_payload} "" PARENT_SCOPE)
    else()
        set(${out_verdict} OK PARENT_SCOPE)
        set(${out_payload} "${_last}" PARENT_SCOPE)
    endif()
endfunction()

function(_nfmsg_control name text expect)
    _classify_notfound_message("${text}" _v)
    if(NOT "${_v}" STREQUAL "${expect}")
        message(SEND_ERROR
            "FAIL[nfmsg_control.${name}]: the shared message classifier said "
            "${_v}, expected ${expect}")
    endif()
endfunction()

set(_nf_good "libmoq could not find its required dependency Picoquic. Install picoquic to a prefix in CMAKE_PREFIX_PATH, or pass -DMOQ_PICOQUIC_SOURCE_DIR=/path.")
_nfmsg_control(complete      "${_nf_good}" OK)
_nfmsg_control(unrelated     "some other configure failure" NO_LIBMOQ)
_nfmsg_control(no_dep_named  "libmoq could not find a dependency. Use CMAKE_PREFIX_PATH or MOQ_PICOQUIC_SOURCE_DIR." NO_DEP)
_nfmsg_control(no_prefix     "libmoq could not find Picoquic. Pass MOQ_PICOQUIC_SOURCE_DIR." NO_PREFIX_ROUTE)
_nfmsg_control(no_source     "libmoq could not find Picoquic. Use CMAKE_PREFIX_PATH." NO_SOURCE_ROUTE)

function(_nfrec_control name text expect_verdict expect_msg_verdict)
    _extract_nf_record("${text}" _rv _payload)
    if(NOT "${_rv}" STREQUAL "${expect_verdict}")
        message(SEND_ERROR
            "FAIL[nfrec_control.${name}]: the extractor said ${_rv}, expected "
            "${expect_verdict}")
    elseif("${_rv}" STREQUAL "OK")
        _classify_notfound_message("${_payload}" _mv)
        if(NOT "${_mv}" STREQUAL "${expect_msg_verdict}")
            message(SEND_ERROR
                "FAIL[nfrec_control.${name}.message]: the payload classified "
                "as ${_mv}, expected ${expect_msg_verdict}")
        endif()
    endif()
endfunction()

_nfrec_control(one_good "-- MOQ_NF_MESSAGE: ${_nf_good}\n" OK OK)
_nfrec_control(none     "-- unrelated output\n" NONE OK)
_nfrec_control(two      "-- MOQ_NF_MESSAGE: ${_nf_good}\n-- MOQ_NF_MESSAGE: ${_nf_good}\n" MULTIPLE OK)
# THE decoy: every required word appears in the transcript, but the record
# itself is degraded. Classifying the transcript would say OK; classifying the
# record must not.
_nfrec_control(decoy
    "-- libmoq resolving Picoquic via CMAKE_PREFIX_PATH or MOQ_PICOQUIC_SOURCE_DIR\n-- MOQ_NF_MESSAGE: \n"
    OK NO_LIBMOQ)

function(_notfound name prefix tree preset)
    set(_src "${BUILD_DIR}-nf-${tree}-${name}-src")
    set(_bld "${BUILD_DIR}-nf-${tree}-${name}")
    file(REMOVE_RECURSE "${_src}" "${_bld}")
    file(MAKE_DIRECTORY "${_src}")
    set(_body "")
    string(APPEND _body
        "cmake_minimum_required(VERSION 3.20)\n"
        "project(moq_nf_${name} LANGUAGES C)\n"
        "${preset}"
        "foreach(v ${_libmoq_touched_vars})\n"
        "  if(DEFINED \${v})\n"
        "    set(_before_defined_\${v} 1)\n"
        "    set(_before_value_\${v} \"\${\${v}}\")\n"
        "  else()\n"
        "    set(_before_defined_\${v} 0)\n"
        "  endif()\n"
        "endforeach()\n"
        "set(CMAKE_DISABLE_FIND_PACKAGE_Picoquic TRUE)\n"
        "find_package(libmoq QUIET COMPONENTS adapter-picoquic)\n"
        "if(libmoq_FOUND)\n"
        "  message(FATAL_ERROR \"MOQ_NF_FOUND_ANYWAY\")\n"
        "endif()\n"
        "message(STATUS \"MOQ_NF_MESSAGE: \${libmoq_NOT_FOUND_MESSAGE}\")\n"
        "if(TARGET moq::adapter-picoquic)\n"
        "  message(FATAL_ERROR \"MOQ_NF_TARGET_INCLUDED\")\n"
        "endif()\n"
        "foreach(v ${_libmoq_touched_vars})\n"
        "  if(_before_defined_\${v})\n"
        "    if(NOT DEFINED \${v})\n"
        "      message(FATAL_ERROR \"MOQ_NF_UNDEFINED \${v}\")\n"
        "    elseif(NOT \"\${\${v}}\" STREQUAL \"\${_before_value_\${v}}\")\n"
        "      message(FATAL_ERROR \"MOQ_NF_REWRITTEN \${v} now='\${\${v}}' was='\${_before_value_\${v}}'\")\n"
        "    endif()\n"
        "  elseif(DEFINED \${v})\n"
        "    message(FATAL_ERROR \"MOQ_NF_LEAKED \${v}='\${\${v}}'\")\n"
        "  endif()\n"
        "endforeach()\n"
        "message(STATUS \"MOQ_NF_CLEAN\")\n")
    file(WRITE "${_src}/CMakeLists.txt" "${_body}")
    execute_process(
        COMMAND "${CMAKE_EXE}" -S "${_src}" -B "${_bld}"
                "-DCMAKE_PREFIX_PATH=${prefix}"
        RESULT_VARIABLE _nrc OUTPUT_VARIABLE _nout ERROR_VARIABLE _nerr)
    set(_nall "${_nout}${_nerr}")
    _moq_reject_warnings(notfound_${tree}_${name} "${_nall}")
    if(NOT _nrc EQUAL 0)
        message(SEND_ERROR
            "FAIL[notfound.${tree}.${name}]: a not-found Picoquic left this "
            "project's state behind, included a target, or reported found. "
            "The probe names the exact variable:\n${_nall}")
    else()
        # The probe echoes libmoq_NOT_FOUND_MESSAGE; a degraded or missing
        # diagnostic must fail by name rather than pass silently.
        _extract_nf_record("${_nall}" _rv _payload)
        if(NOT "${_rv}" STREQUAL "OK")
            message(SEND_ERROR
                "FAIL[notfound.${tree}.${name}.record]: expected exactly one "
                "MOQ_NF_MESSAGE record, the extractor said ${_rv}:\n${_nall}")
        else()
            _classify_notfound_message("${_payload}" _mv)
            if(NOT "${_mv}" STREQUAL "OK")
                message(SEND_ERROR
                    "FAIL[notfound.${tree}.${name}.message]: the RECORD "
                    "classified as ${_mv}. libmoq_NOT_FOUND_MESSAGE must name "
                    "libmoq, name the Picoquic dependency, and offer both the "
                    "CMAKE_PREFIX_PATH and MOQ_PICOQUIC_SOURCE_DIR routes. "
                    "Record was: '${_payload}'")
            endif()
        endif()
    endif()
endfunction()

# --- a REQUIRED consumer must still FAIL, with the same diagnostic ----------
function(_notfound_required name prefix tree)
    set(_src "${BUILD_DIR}-nfreq-${tree}-${name}-src")
    set(_bld "${BUILD_DIR}-nfreq-${tree}-${name}")
    file(REMOVE_RECURSE "${_src}" "${_bld}")
    file(MAKE_DIRECTORY "${_src}")
    file(WRITE "${_src}/CMakeLists.txt"
         "cmake_minimum_required(VERSION 3.20)\n"
         "project(moq_nfreq_${name} LANGUAGES C)\n"
         "set(CMAKE_DISABLE_FIND_PACKAGE_Picoquic TRUE)\n"
         "find_package(libmoq REQUIRED COMPONENTS adapter-picoquic)\n"
         "message(STATUS \"MOQ_NFREQ_CONFIGURED\")\n")
    execute_process(
        COMMAND "${CMAKE_EXE}" -S "${_src}" -B "${_bld}"
                "-DCMAKE_PREFIX_PATH=${prefix}"
        RESULT_VARIABLE _qrc OUTPUT_VARIABLE _qout ERROR_VARIABLE _qerr)
    set(_qall "${_qout}${_qerr}")
    _moq_reject_warnings(notfound_required_${tree}_${name} "${_qall}")
    if(_qrc EQUAL 0)
        message(SEND_ERROR
            "FAIL[notfound_required.${tree}.${name}.configured]: a REQUIRED "
            "consumer configured successfully although its dependency was "
            "unavailable:\n${_qall}")
    else()
        # Causal attribution: CMake prints NOT_FOUND_MESSAGE inside its own
        # "Could not find a package configuration file" / REQUIRED block, so
        # the LibMoQ-owned sentence must appear as ONE contiguous fragment
        # rather than being assembled from words scattered in the transcript.
        if(NOT _qall MATCHES "libmoq could not find its required dependency Picoquic")
            message(SEND_ERROR
                "FAIL[notfound_required.${tree}.${name}.attribution]: it "
                "failed, but not with the LibMoQ-owned dependency message; the "
                "required words must come from that message, not from "
                "unrelated output:\n${_qall}")
        else()
            _classify_notfound_message("${_qall}" _qmv)
            if(NOT "${_qmv}" STREQUAL "OK")
                message(SEND_ERROR
                    "FAIL[notfound_required.${tree}.${name}.message]: the "
                    "classifier said ${_qmv}; the actionable content must not "
                    "be weakened:\n${_qall}")
            endif()
        endif()
    endif()
endfunction()

# --- a SUCCESSFUL resolution must also leave nothing behind ----------------
# Restoration on the success path is a different code path from restoration on
# the not-found path, and it must not pass because the resolver skipped the
# guard: the same trace instrument proves the guard actually ran.
# --- the applied-values MANIFEST, validated before any probe uses it --------
# Keyed by the CONSUMER variable each value is applied to, so a surviving
# variable is compared only against the value LibMoQ would have put THERE. A
# flat value set cannot do that, and a bare `;`-list does not even survive the
# CTest command as one argument.
set(_applied_required
    MOQ_PICOTLS_PREFIX PTLS_INCLUDE_DIR PTLS_CORE_LIBRARY
    PTLS_MINICRYPTO_LIBRARY MBEDTLS_INCLUDE_DIR MBEDTLS_LIBRARY
    MBEDTLS_CRYPTO MBEDTLS_X509 MOQ_PICOQUIC_MBEDTLS_INCLUDE_DIR)
set(_applied_seen "")
set(_applied_ok TRUE)
if(NOT APPLIED_MANIFEST)
    message(SEND_ERROR
        "FAIL[applied.inventory.absent]: no applied-values manifest was "
        "supplied, so the success-path probe would compare against nothing")
    set(_applied_ok FALSE)
endif()
string(REPLACE "," ";" _applied_pairs "${APPLIED_MANIFEST}")
foreach(_pair IN LISTS _applied_pairs)
    if(NOT _pair MATCHES "^([A-Za-z0-9_]+)=(.*)$")
        message(SEND_ERROR
            "FAIL[applied.inventory.malformed]: '${_pair}' is not NAME=VALUE")
        set(_applied_ok FALSE)
    else()
        set(_an "${CMAKE_MATCH_1}")
        set(_av "${CMAKE_MATCH_2}")
        if(NOT _an IN_LIST _applied_required)
            message(SEND_ERROR
                "FAIL[applied.inventory.extra]: '${_an}' is not one of the "
                "nine applied location variables")
            set(_applied_ok FALSE)
        elseif(_an IN_LIST _applied_seen)
            message(SEND_ERROR
                "FAIL[applied.inventory.duplicate]: '${_an}' appears twice")
            set(_applied_ok FALSE)
        else()
            list(APPEND _applied_seen "${_an}")
            set(_applied_value_${_an} "${_av}")
        endif()
    endif()
endforeach()
foreach(_an IN LISTS _applied_required)
    if(NOT _an IN_LIST _applied_seen)
        message(SEND_ERROR
            "FAIL[applied.inventory.missing]: no '${_an}' entry")
        set(_applied_ok FALSE)
    endif()
endforeach()

function(_success_clean name prefix tree)
    if(NOT _applied_ok)
        message(SEND_ERROR
            "FAIL[success_clean.${tree}.${name}.skipped]: the applied-values "
            "manifest is invalid, so this arm was not run")
        return()
    endif()
    set(_src "${BUILD_DIR}-succ-${tree}-${name}-src")
    set(_bld "${BUILD_DIR}-succ-${tree}-${name}")
    file(REMOVE_RECURSE "${_src}" "${_bld}")
    file(MAKE_DIRECTORY "${_src}")
    # Per-variable comparisons, built from the validated manifest.
    set(_applied_checks "")
    foreach(_an IN LISTS _applied_required)
        if(NOT "${_applied_value_${_an}}" STREQUAL "")
            string(APPEND _applied_checks
                "if(DEFINED ${_an} AND \"\${${_an}}\" STREQUAL \"${_applied_value_${_an}}\")\n"
                "  message(FATAL_ERROR \"MOQ_SUCC_LEAKED ${_an}='\${${_an}}'\")\n"
                "endif()\n")
        endif()
    endforeach()
    # The undefined PRE-state is asserted, not assumed: if some earlier step
    # had already defined one of these, "still undefined afterwards" would
    # prove nothing about restoration.
    set(_pre_checks "")
    foreach(_v WITH_OPENSSL WITH_MBEDTLS)
        string(APPEND _pre_checks
            "if(DEFINED ${_v})\n"
            "  message(FATAL_ERROR \"MOQ_SUCC_PRESET ${_v}\")\n"
            "endif()\n")
    endforeach()
    foreach(_an IN LISTS _applied_required)
        string(APPEND _pre_checks
            "if(DEFINED ${_an})\n"
            "  message(FATAL_ERROR \"MOQ_SUCC_PRESET ${_an}\")\n"
            "endif()\n")
    endforeach()
    set(_body "")
    string(APPEND _body
        "cmake_minimum_required(VERSION 3.20)\n"
        "project(moq_succ_${name} LANGUAGES C)\n"
        "${_pre_checks}"
        "${_selected_graph}"
        "find_package(libmoq REQUIRED COMPONENTS adapter-picoquic)\n"
        # The provider booleans are ALWAYS applied by LibMoQ, so on a clean
        # success they must be undefined again.
        "foreach(v WITH_OPENSSL WITH_MBEDTLS)\n"
        "  if(DEFINED \${v})\n"
        "    message(FATAL_ERROR \"MOQ_SUCC_LEAKED \${v}='\${\${v}}'\")\n"
        "  endif()\n"
        "endforeach()\n"
        # The location hints are different: a nested dependency's own find
        # module legitimately defines names like PTLS_INCLUDE_DIR (as
        # -NOTFOUND, say). What must not survive is the value LIBMOQ applied to
        # THAT variable, compared per variable rather than against a pooled
        # value set.
        "${_applied_checks}"
        "message(STATUS \"MOQ_SUCC_CLEAN\")\n")
    file(WRITE "${_src}/CMakeLists.txt" "${_body}")
    execute_process(
        COMMAND "${CMAKE_EXE}" -S "${_src}" -B "${_bld}"
                --trace-source=libmoqConfig.cmake --trace-expand -Wno-dev
                "-DCMAKE_PREFIX_PATH=${prefix}"
        RESULT_VARIABLE _src_rc OUTPUT_VARIABLE _sout ERROR_VARIABLE _serr)
    set(_sall "${_sout}${_serr}")
    _moq_reject_warnings(success_clean_${tree}_${name} "${_sall}")
    if(NOT _src_rc EQUAL 0)
        message(SEND_ERROR
            "FAIL[success_clean.${tree}.${name}]: a SUCCESSFUL resolution left "
            "one of this project's temporary variables defined in the "
            "consumer, or failed outright:\n${_sall}")
    else()
        _classify_guard_ran("${_sall}" _sgv)
        if(NOT "${_sgv}" STREQUAL "RAN")
            message(SEND_ERROR
                "FAIL[success_clean.${tree}.${name}.guard_skipped]: nothing "
                "was left behind, but the structural guard was never invoked, "
                "so the clean state proves nothing.")
        endif()
    endif()
endfunction()

# Every var initially UNDEFINED must still be undefined afterwards.
_notfound(undefined "${PREFIX_PATH}" build "")
if(_route_install_prefix)
    _notfound(undefined "${_route_install_prefix}" install "")
endif()
# ... and a consumer's own COMPATIBLE spellings/locations survive byte-for-byte.
# EVERY touched variable is preset, each to a DISTINCT consumer-owned sentinel,
# with provider booleans that are compatible (so the call is not refused for an
# unrelated reason). Presetting only three of eleven would leave the other eight
# unproven -- which is exactly what the earlier version did.
if("${WANT_SELECTED}" STREQUAL "mbedtls")
    set(_nf_preset "set(WITH_OPENSSL FALSE)\nset(WITH_MBEDTLS TRUE)\n")
else()
    set(_nf_preset "set(WITH_OPENSSL TRUE)\nset(WITH_MBEDTLS FALSE)\n")
endif()
foreach(_v MOQ_PICOTLS_PREFIX PTLS_INCLUDE_DIR PTLS_CORE_LIBRARY
           PTLS_MINICRYPTO_LIBRARY MBEDTLS_INCLUDE_DIR MBEDTLS_LIBRARY
           MBEDTLS_CRYPTO MBEDTLS_X509 MOQ_PICOQUIC_MBEDTLS_INCLUDE_DIR)
    string(APPEND _nf_preset "set(${_v} \"/consumer/own/${_v}\")\n")
endforeach()
_notfound(preset "${PREFIX_PATH}" build "${_nf_preset}")
if(_route_install_prefix)
    _notfound(preset "${_route_install_prefix}" install "${_nf_preset}")
endif()

_notfound_required(disabled "${PREFIX_PATH}" build)
_success_clean(undefined "${PREFIX_PATH}" build)
if(_route_install_prefix)
    _notfound_required(disabled "${_route_install_prefix}" install)
    _success_clean(undefined "${_route_install_prefix}" install)
endif()
