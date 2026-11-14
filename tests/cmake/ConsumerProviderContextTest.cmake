# Pins the OpenSSL context test-owned children receive
# (ConsumerProviderContext.cmake), with placeholder OpenSSL trees:
#   SEL   the provider the parent selects (OPENSSL_ROOT_DIR, framework search
#         off);
#   HDEC  headers only, first on the child's CMAKE_PREFIX_PATH;
#   LDEC  libraries only, on the child's CMAKE_LIBRARY_PATH.
# Left to its own discovery the child would combine HDEC headers with LDEC
# libraries, a mixed provider. Cases:
#   selected   the child resolves exactly the parent's canonical include
#              directory and SSL/crypto libraries, and its framework setting;
#   override   a child's own -D still wins over the context;
#   unselected a parent that resolved no OpenSSL writes no entries, and the
#              child's normal discovery is untouched.
# Nested configures run with OPENSSL_ROOT_DIR, CMAKE_PREFIX_PATH and
# CMAKE_LIBRARY_PATH removed from their environment so only the fixture
# decides discovery.
#
# Inputs: MOQ_SOURCE (libmoq checkout), MOQ_WORK (recreated), optional
# MOQ_CONTEXT (toolchain preload), MOQ_GENERATOR.
cmake_minimum_required(VERSION 3.20)

foreach(var MOQ_SOURCE MOQ_WORK)
    if(NOT DEFINED ${var} OR "${${var}}" STREQUAL "")
        message(FATAL_ERROR "${var} is required")
    endif()
endforeach()

set(fixture "${MOQ_SOURCE}/tests/cmake/consumer_provider_context")
file(REMOVE_RECURSE "${MOQ_WORK}")

function(_fake_openssl root headers libs version)
    if(headers)
        file(WRITE "${root}/include/openssl/ssl.h" "")
        file(WRITE "${root}/include/openssl/opensslv.h"
"# define OPENSSL_VERSION_MAJOR 3
# define OPENSSL_VERSION_MINOR 0
# define OPENSSL_VERSION_PATCH ${version}
# define OPENSSL_VERSION_STR \"3.0.${version}\"
# define OPENSSL_VERSION_NUMBER 0x300000${version}0L
")
    endif()
    if(libs)
        file(WRITE "${root}/lib/libssl.a" "")
        file(WRITE "${root}/lib/libcrypto.a" "")
    endif()
endfunction()
_fake_openssl("${MOQ_WORK}/SEL" ON ON 1)
_fake_openssl("${MOQ_WORK}/HDEC" ON OFF 2)
_fake_openssl("${MOQ_WORK}/LDEC" OFF ON 3)

set(common_args "")
if(DEFINED MOQ_GENERATOR AND NOT MOQ_GENERATOR STREQUAL "")
    list(APPEND common_args -G "${MOQ_GENERATOR}")
endif()
if(DEFINED MOQ_CONTEXT AND NOT MOQ_CONTEXT STREQUAL "")
    list(APPEND common_args -C "${MOQ_CONTEXT}")
endif()
set(isolated "${CMAKE_COMMAND}" -E env --unset=OPENSSL_ROOT_DIR
    --unset=CMAKE_PREFIX_PATH --unset=CMAKE_LIBRARY_PATH "${CMAKE_COMMAND}")

set(failures "")

function(_configure what src build)
    execute_process(
        COMMAND ${isolated} ${common_args} -S "${src}" -B "${MOQ_WORK}/${build}" ${ARGN}
        RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE out)
    # Each nested configure's status and whole output are kept and shown even
    # on success, so its diagnostics can be judged after the work tree is gone.
    file(WRITE "${MOQ_WORK}/logs/${build}.configure.log" "${out}")
    file(WRITE "${MOQ_WORK}/logs/${build}.configure.status" "${rc}\n")
    message(STATUS "${what} configure exit ${rc}:\n${out}")
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "${what} configure failed (${rc}):\n${out}")
    endif()
endfunction()

function(_parent name)
    _configure("${name} parent" "${fixture}/parent" "${name}-parent"
        "-DMOQ_SOURCE=${MOQ_SOURCE}"
        "-DMOQ_CONTEXT_OUT=${MOQ_WORK}/${name}-context.cmake" ${ARGN})
endfunction()

# Reads <build>/CMakeCache.txt entries <vars> into <prefix>_<var>.
function(_cache_values prefix build)
    file(STRINGS "${MOQ_WORK}/${build}/CMakeCache.txt" lines)
    foreach(var IN LISTS ARGN)
        set(value "")
        foreach(line IN LISTS lines)
            if(line MATCHES "^${var}:[A-Z]+=(.*)$")
                set(value "${CMAKE_MATCH_1}")
            endif()
        endforeach()
        set(${prefix}_${var} "${value}" PARENT_SCOPE)
    endforeach()
endfunction()

function(_child name context)
    _configure("${name} child" "${fixture}/child" "${name}-child"
        -C "${context}"
        "-DMOQ_RESULT=${MOQ_WORK}/${name}-result.txt"
        "-DCMAKE_PREFIX_PATH=${MOQ_WORK}/HDEC"
        "-DCMAKE_LIBRARY_PATH=${MOQ_WORK}/LDEC/lib" ${ARGN})
    file(STRINGS "${MOQ_WORK}/${name}-result.txt" lines)
    foreach(line IN LISTS lines)
        if(line MATCHES "^([A-Z]+)=(.*)$")
            set(${name}_${CMAKE_MATCH_1} "${CMAKE_MATCH_2}" PARENT_SCOPE)
        endif()
    endforeach()
endfunction()

# Canonical identity comparison: symlinks and spellings do not matter, the
# resolved file or directory does.
function(_same name key actual expected)
    get_filename_component(a "${actual}" REALPATH)
    get_filename_component(e "${expected}" REALPATH)
    if(actual STREQUAL "" OR NOT a STREQUAL e)
        set(failures ${failures}
            "${name}: child ${key} is '${actual}', expected '${expected}'" PARENT_SCOPE)
    endif()
endfunction()

# selected
_parent(selected "-DFIND_OPENSSL=ON" "-DOPENSSL_ROOT_DIR=${MOQ_WORK}/SEL"
    -DCMAKE_FIND_FRAMEWORK=NEVER)
_cache_values(parent selected-parent
    OPENSSL_INCLUDE_DIR OPENSSL_SSL_LIBRARY OPENSSL_CRYPTO_LIBRARY)
_same(selected "parent include" "${parent_OPENSSL_INCLUDE_DIR}" "${MOQ_WORK}/SEL/include")
_child(selected "${MOQ_WORK}/selected-context.cmake")
_same(selected INCLUDE "${selected_INCLUDE}" "${parent_OPENSSL_INCLUDE_DIR}")
_same(selected SSL "${selected_SSL}" "${parent_OPENSSL_SSL_LIBRARY}")
_same(selected CRYPTO "${selected_CRYPTO}" "${parent_OPENSSL_CRYPTO_LIBRARY}")
if(NOT selected_FRAMEWORK STREQUAL "NEVER")
    list(APPEND failures "selected: child CMAKE_FIND_FRAMEWORK is '${selected_FRAMEWORK}', expected NEVER")
endif()

# override: a child's own setting stays authoritative.
_child(override "${MOQ_WORK}/selected-context.cmake"
    "-DOPENSSL_INCLUDE_DIR=${MOQ_WORK}/HDEC/include")
_same(override INCLUDE "${override_INCLUDE}" "${MOQ_WORK}/HDEC/include")
_same(override SSL "${override_SSL}" "${parent_OPENSSL_SSL_LIBRARY}")

# unselected: nothing forwarded, normal discovery (framework search off by the
# child's own choice, for a host-independent result).
_parent(unselected "-DFIND_OPENSSL=OFF")
file(READ "${MOQ_WORK}/unselected-context.cmake" unselected_text)
if(unselected_text MATCHES "set\\(")
    list(APPEND failures "unselected: the context pins values although the parent selected none:\n${unselected_text}")
endif()
_child(unselected "${MOQ_WORK}/unselected-context.cmake" -DCMAKE_FIND_FRAMEWORK=NEVER)
_same(unselected INCLUDE "${unselected_INCLUDE}" "${MOQ_WORK}/HDEC/include")
_same(unselected SSL "${unselected_SSL}" "${MOQ_WORK}/LDEC/lib/libssl.a")
_same(unselected CRYPTO "${unselected_CRYPTO}" "${MOQ_WORK}/LDEC/lib/libcrypto.a")

if(failures)
    list(JOIN failures "\n  " text)
    message(FATAL_ERROR "consumer OpenSSL context:\n  ${text}")
endif()
message(STATUS "consumer OpenSSL context: selected, override and unselected cases hold")
