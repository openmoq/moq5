# =====================================================================
# Negotiated-profile substrate: the FOUR-ROLE DEPENDENCY POLICY.
#
# The roles, and what each may depend on (0659/0661):
#
#   1 PURE ORACLE          np_oracle.c
#       libc only. NO moq_* symbol may appear, defined or undefined.
#   2 SCRIPTED WIRE BUILDER (staged)
#       may use the oracle and the corpus; may NOT call the product's LOC,
#       KVP or integer-codec APIs -- it must build bytes from the drafts, or
#       it would inherit the very bug it exists to detect.
#   3 PRODUCT ADAPTER      (staged)
#       the ONLY role allowed to call product LOC APIs. Its boundary is
#       enumerated rather than forbidden: see _np_adapter_allowed below.
#   4 CLOSURE RUNNER       (staged)
#       drives roles 2 and 3 against each other; may NOT call LOC/KVP itself.
#
# TWO scans, because either alone can be fooled (0659 F5):
#
#   SOURCE tokens  catch a call written in the file;
#   OBJECT symbols catch a call that arrives through a helper or an inline
#                  function, where no forbidden token appears in this file at
#                  all -- link transitivity hides exactly that.
#
# Roles 2-4 have no file yet. The policy FAILS CLOSED in both directions: a
# declared role file that appears is scanned, and if a role that requires an
# OBJECT audit has a source but no object was supplied, that is an error, not
# a skip. NP_STRICT=ON additionally requires every declared role file to
# exist. Nothing here is a hollow stub.
#
# Inputs:
#   NP_SRC_DIR      the tests/support/np directory
#   NP_ORACLE_OBJ   compiled np_oracle.c object
#   NP_BUILDER_OBJ  compiled np_wire_builder.c object (required once its
#                   source exists)
#   NP_RUNNER_OBJ   compiled np_closure_runner.c object (same rule)
#   NP_NM           the toolchain's symbol tool; CMake passes CMAKE_NM
#   NP_STRICT       require every declared role file to exist
# =====================================================================

set(_np_problems 0)

function(_np_fail msg)
    message(SEND_ERROR "np-roles: ${msg}")
    math(EXPR _np_problems "${_np_problems}+1")
    set(_np_problems ${_np_problems} PARENT_SCOPE)
endfunction()

# ---- role file manifest ---------------------------------------------
# role;file;forbidden-identifier-regex (empty = none)
# role@file@forbidden-identifier-regex  ('@' so ';' cannot split a row)
set(_np_roles
    [[oracle@np_oracle.c@moq_]]
    [[builder@np_wire_builder.c@moq_loc_|moq_kvp_|moq_vi64_|moq_quic_varint_]]
    [[adapter@np_product_adapter.c@]]
    [[runner@np_closure_runner.c@moq_loc_|moq_kvp_]]
)

# The roles whose OBJECT symbols must also be audited, and the variable each
# expects. A role listed here with a source but no object is a policy failure.
set(_np_obj_roles
    [[oracle@NP_ORACLE_OBJ@moq_]]
    [[builder@NP_BUILDER_OBJ@moq_loc_|moq_kvp_|moq_vi64_|moq_quic_varint_]]
    [[runner@NP_RUNNER_OBJ@moq_loc_|moq_kvp_]]
)

# The product-adapter boundary, DECLARED -- and it is a declaration, not an
# enforced allowlist. The signed plan sanctions that one translation unit as
# the only place product LOC APIs may be called, and requires the boundary to
# be STATED; nothing here scans the adapter's source or object, and this line
# must not be described as runtime or object enforcement. The load-bearing
# audits are the oracle, builder and runner scans below.
set(_np_adapter_allowed
    "moq_loc_ (product LOC encode/decode) -- DECLARED, not scanned")

set(_np_nm "${NP_NM}")
if(NOT _np_nm)
    # CMake always knows its own toolchain's nm; a literal "nm" would be the
    # host's, which is not necessarily the one that produced these objects.
    set(_np_nm "${CMAKE_NM}")
endif()
if(NOT _np_nm)
    set(_np_nm "nm")
    message(STATUS "np-roles: no CMAKE_NM/NP_NM given; falling back to nm")
endif()

foreach(_row IN LISTS _np_roles)
    string(REGEX MATCH "^([^@]+)@([^@]+)@(.*)$" _m "${_row}")
    if(NOT _m)
        message(FATAL_ERROR "np-roles: invalid source role record: ${_row}")
    endif()
    set(_role "${CMAKE_MATCH_1}")
    set(_file "${CMAKE_MATCH_2}")
    set(_forbid "${CMAKE_MATCH_3}")
    set(_path "${NP_SRC_DIR}/${_file}")

    if(NOT EXISTS "${_path}")
        if(NP_STRICT)
            _np_fail("role '${_role}': ${_file} is missing but strict mode is on")
        else()
            message(STATUS "np-roles: role '${_role}' staged (no ${_file})")
        endif()
        continue()
    endif()

    if(_forbid STREQUAL "")
        message(STATUS "np-roles: role '${_role}': no forbidden identifiers")
    else()
        file(STRINGS "${_path}" _hits REGEX "${_forbid}")
        # A comment naming a forbidden identifier is still a hit: the policy is
        # deliberately blunt so it cannot be talked around in prose.
        if(_hits)
            list(LENGTH _hits _n)
            _np_fail("role '${_role}': ${_n} forbidden identifier line(s) in ${_file} matching /${_forbid}/")
            foreach(_h IN LISTS _hits)
                message(SEND_ERROR "    ${_h}")
            endforeach()
        else()
            message(STATUS "np-roles: role '${_role}': clean")
        endif()
    endif()
endforeach()

message(STATUS "np-roles: product-adapter boundary (declared only): "
               "${_np_adapter_allowed}")

# ---- OBJECT audits: undefined as well as defined symbols -------------
foreach(_row IN LISTS _np_obj_roles)
    string(REGEX MATCH "^([^@]+)@([^@]+)@(.*)$" _m "${_row}")
    if(NOT _m)
        message(FATAL_ERROR "np-roles: invalid object role record: ${_row}")
    endif()
    set(_role "${CMAKE_MATCH_1}")
    set(_var "${CMAKE_MATCH_2}")
    set(_forbid "${CMAKE_MATCH_3}")
    set(_obj "${${_var}}")

    # find the role's source, to decide whether an object audit is REQUIRED
    set(_src "")
    foreach(_r2 IN LISTS _np_roles)
        string(REGEX MATCH "^([^@]+)@([^@]+)@" _m2 "${_r2}")
        if(CMAKE_MATCH_1 STREQUAL _role)
            set(_src "${NP_SRC_DIR}/${CMAKE_MATCH_2}")
        endif()
    endforeach()

    if(NOT EXISTS "${_src}")
        message(STATUS "np-roles: role '${_role}' object audit staged (no source)")
        continue()
    endif()
    if(NOT _obj)
        _np_fail("role '${_role}': its source exists but ${_var} was not supplied -- the object audit cannot be skipped")
        continue()
    endif()
    if(NOT EXISTS "${_obj}")
        _np_fail("role '${_role}': ${_var} does not exist: ${_obj}")
        continue()
    endif()

    # -a lists ALL symbols including undefined ones, which is the point: a
    # forbidden call that arrives through a helper shows up here as an
    # undefined reference even though the source carries no forbidden token.
    execute_process(COMMAND "${_np_nm}" -a "${_obj}"
                    OUTPUT_VARIABLE _syms ERROR_VARIABLE _e RESULT_VARIABLE _rc)
    if(NOT _rc EQUAL 0)
        _np_fail("role '${_role}': cannot read symbols from ${_obj}: ${_e}")
        continue()
    endif()
    string(REGEX MATCHALL "[^\n]*(${_forbid})[^\n]*" _hits "${_syms}")
    if(_hits)
        list(LENGTH _hits _n)
        _np_fail("role '${_role}': ${_n} forbidden symbol(s) in ${_obj} matching /${_forbid}/")
        foreach(_s IN LISTS _hits)
            message(SEND_ERROR "    ${_s}")
        endforeach()
    else()
        message(STATUS "np-roles: role '${_role}' object is clean")
    endif()
endforeach()

if(_np_problems GREATER 0)
    message(FATAL_ERROR "np-roles: ${_np_problems} policy violation(s)")
endif()
message(STATUS "np-roles: policy satisfied")
