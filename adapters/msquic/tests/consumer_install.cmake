# Installed and relocated SDK consumer; explicit provider identity, provider-only
# Linux runtime input, and complete diagnostic/image checks at each boundary.
# Inputs: BUILD, SRC, WORK, MSQUIC_RUNTIME_FILE; optional CONSUMER_CONTEXT
# preload and discovery/compiler hints (legacy compiler hints are the fallback).
foreach(_v BUILD SRC WORK)
    if(NOT DEFINED ${_v})
        message(FATAL_ERROR "pass -D${_v}=<path>")
    endif()
endforeach()
include("${CMAKE_CURRENT_LIST_DIR}/../../../tests/cmake/RejectConsumerWarnings.cmake")
include("${CMAKE_CURRENT_LIST_DIR}/../../wtquic/tests/loader_images.cmake")
include("${CMAKE_CURRENT_LIST_DIR}/consumer_environment.cmake")

set(_prefix "${WORK}/prefix")
set(_relocated "${WORK}/relocated prefix")
set(_cbuild "${WORK}/consumer-build")
set(_cbuild2 "${WORK}/consumer-build-relocated")
set(_logdir "${WORK}/log")
file(REMOVE_RECURSE "${_prefix}" "${_relocated}" "${_cbuild}" "${_cbuild2}" "${_logdir}")
file(MAKE_DIRECTORY "${_logdir}")

# Run one subprocess at a gate boundary: keep its complete output, report a
# nonzero status as that step's own failure, and refuse diagnostics even at
# status zero. `out_var` receives the output for callers that parse it.
function(_step label out_var)
    string(MAKE_C_IDENTIFIER "${label}" _id)
    # Preserve the command's literal argument vector, including empty values.
    set(_call "moq_consumer_execute(COMMAND")
    math(EXPR _last "${ARGC} - 1")
    foreach(_i RANGE 2 ${_last})
        moq_consumer_quote_argument("${ARGV${_i}}" _arg)
        string(APPEND _call " ${_arg}")
    endforeach()
    string(APPEND _call " RESULT_VARIABLE _r OUTPUT_VARIABLE _o ERROR_VARIABLE _o)")
    cmake_language(EVAL CODE "${_call}")
    file(WRITE "${_logdir}/${_id}.txt" "${_o}")
    if(NOT _r EQUAL 0)
        message(FATAL_ERROR "${label}: failed with status ${_r}; output kept at ${_logdir}/${_id}.txt\n${_o}")
    endif()
    set(${out_var} "${_o}" PARENT_SCOPE)
endfunction()

_step("libmoq install" _out ${CMAKE_COMMAND} --install "${BUILD}" --prefix "${_prefix}")

set(_fwd "")
if(DEFINED MSQUIC_DIR_HINT AND NOT MSQUIC_DIR_HINT STREQUAL "" AND
   NOT MSQUIC_DIR_HINT MATCHES "NOTFOUND")
    list(APPEND _fwd "-Dmsquic_DIR=${MSQUIC_DIR_HINT}")
endif()
if(DEFINED MSQUIC_ROOT_HINT AND NOT MSQUIC_ROOT_HINT STREQUAL "")
    list(APPEND _fwd "-DMOQ_MSQUIC_ROOT=${MSQUIC_ROOT_HINT}")
endif()
if(DEFINED CONSUMER_CONTEXT AND NOT CONSUMER_CONTEXT STREQUAL "")
    list(APPEND _fwd -C "${CONSUMER_CONTEXT}")
else()
    if(DEFINED C_COMPILER AND NOT C_COMPILER STREQUAL "")
        list(APPEND _fwd "-DCMAKE_C_COMPILER=${C_COMPILER}")
    endif()
    if(DEFINED C_FLAGS AND NOT C_FLAGS STREQUAL "")
        list(APPEND _fwd "-DCMAKE_C_FLAGS=${C_FLAGS}")
    endif()
    if(DEFINED LINK_FLAGS AND NOT LINK_FLAGS STREQUAL "")
        list(APPEND _fwd "-DCMAKE_EXE_LINKER_FLAGS=${LINK_FLAGS}")
    endif()
endif()

set(_linux OFF)
if(CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux")
    set(_linux ON)
endif()
# The parent's provider artifact: required on Linux, where it becomes the one
# external runtime directory, and validated on every host where it is given.
set(_have_runtime_file OFF)
if(DEFINED MSQUIC_RUNTIME_FILE AND NOT MSQUIC_RUNTIME_FILE STREQUAL "" AND
   NOT MSQUIC_RUNTIME_FILE MATCHES "NOTFOUND")
    if(NOT EXISTS "${MSQUIC_RUNTIME_FILE}" OR IS_DIRECTORY "${MSQUIC_RUNTIME_FILE}")
        message(FATAL_ERROR "MSQUIC_RUNTIME_FILE is not a file: ${MSQUIC_RUNTIME_FILE}")
    endif()
    file(REAL_PATH "${MSQUIC_RUNTIME_FILE}" _runtime_real)
    file(SHA256 "${_runtime_real}" _runtime_sha)
    set(_have_runtime_file ON)
elseif(_linux)
    message(FATAL_ERROR "MSQUIC_RUNTIME_FILE is required on Linux: the provider's "
                        "resolved artifact was not forwarded")
endif()

# Read the record a consumer configure generated, and check every field is a
# real file / a known type. Sets CONSUMER_* in the caller's scope. Each field
# starts from an invalid local value before the record is included, so a
# field the record omits is refused rather than completed by a value left in
# the caller's scope (or the cache) by an earlier record.
function(_read_record cbuild label)
    set(_rec "${cbuild}/consumer_targets.cmake")
    if(NOT EXISTS "${_rec}")
        message(FATAL_ERROR "${label}: the consumer did not generate ${_rec}")
    endif()
    set(_fields CONSUMER_PROVIDER_FILE CONSUMER_ADAPTER_FILE CONSUMER_ADAPTER_TYPE
                CONSUMER_CORE_FILE CONSUMER_CORE_TYPE)
    foreach(_f IN LISTS _fields)
        set(${_f} "")
    endforeach()
    include("${_rec}")
    foreach(_f CONSUMER_PROVIDER_FILE CONSUMER_ADAPTER_FILE CONSUMER_CORE_FILE)
        if("${${_f}}" STREQUAL "" OR NOT EXISTS "${${_f}}" OR IS_DIRECTORY "${${_f}}")
            message(FATAL_ERROR "${label}: record field ${_f} is not a file: '${${_f}}'")
        endif()
    endforeach()
    foreach(_t CONSUMER_ADAPTER_TYPE CONSUMER_CORE_TYPE)
        if(NOT "${${_t}}" MATCHES "^(SHARED_LIBRARY|STATIC_LIBRARY)$")
            message(FATAL_ERROR "${label}: record field ${_t} is not a library type: '${${_t}}'")
        endif()
    endforeach()
    foreach(_f IN LISTS _fields)
        set(${_f} "${${_f}}" PARENT_SCOPE)
    endforeach()
endfunction()

# Both SDK files the consumer resolved must live under the SDK prefix under
# test: canonical path-component containment, no library-directory guess.
# The record says what resolved; this says it was the installed SDK.
function(_check_sdk_membership sdk_prefix label)
    file(REAL_PATH "${sdk_prefix}" _sdk_real)
    foreach(_pair "adapter;${CONSUMER_ADAPTER_FILE}" "core;${CONSUMER_CORE_FILE}")
        list(GET _pair 0 _what)
        list(GET _pair 1 _file)
        file(REAL_PATH "${_file}" _real)
        string(FIND "${_real}" "${_sdk_real}/" _in)
        if(NOT _in EQUAL 0)
            message(FATAL_ERROR "${label}: the consumer resolved ${_what} ${_real}, "
                                "not a file under the SDK prefix ${_sdk_real}")
        endif()
    endforeach()
    message(STATUS "${label}: adapter and core resolved under ${_sdk_real}")
endfunction()

# The consumer's resolved provider artifact must be the parent's: same
# canonical file (symlink aliases allowed) AND the same bytes.
function(_check_provider_binding label)
    if(NOT _have_runtime_file)
        return()
    endif()
    file(REAL_PATH "${CONSUMER_PROVIDER_FILE}" _c_real)
    file(SHA256 "${_c_real}" _c_sha)
    if(NOT _c_real STREQUAL _runtime_real)
        message(FATAL_ERROR "${label}: provider artifact mismatch: the consumer resolved "
            "${CONSUMER_PROVIDER_FILE} (${_c_real}) but the parent linked "
            "${MSQUIC_RUNTIME_FILE} (${_runtime_real})")
    endif()
    if(NOT _c_sha STREQUAL _runtime_sha)
        message(FATAL_ERROR "${label}: provider artifact bytes differ: ${_c_real}")
    endif()
    message(STATUS "${label}: provider artifact bound: ${_c_real} sha256=${_c_sha}")
endfunction()

# The scrubbed environment every consumer runs under. On Linux the provider
# directory -- the directory of the artifact the consumer resolved -- is the
# only entry; the SDK's own layout has to carry the rest.
function(_scrubbed_env out_var)
    msquic_consumer_environment(_env)
    if(_linux)
        cmake_path(GET _runtime_real PARENT_PATH _provider_dir)
        list(APPEND _env "LD_LIBRARY_PATH=${_provider_dir}")
    endif()
    set(${out_var} "${_env}" PARENT_SCOPE)
endfunction()

function(_run_consumer exe label)
    _scrubbed_env(_env)
    if(_linux)
        cmake_path(GET _runtime_real PARENT_PATH _provider_dir)
        message(STATUS "${label}: provider-assisted run on Linux; external provider "
                       "directory ${_provider_dir} (directory of ${_runtime_real}); "
                       "SDK directories are NOT added")
    endif()
    _step("${label} run" _out ${_env} "${exe}")
endfunction()

# Read actual consumer flags, not a producer build-directory label. Follow the
# existing loader policy: an explicit later address/all disable wins.
function(_consumer_asan_lane cbuild out)
    foreach(_key CMAKE_C_FLAGS CMAKE_EXE_LINKER_FLAGS CMAKE_BUILD_TYPE)
        set(_actual_${_key} "")
    endforeach()
    load_cache("${cbuild}" READ_WITH_PREFIX _actual_
        CMAKE_C_FLAGS CMAKE_EXE_LINKER_FLAGS CMAKE_BUILD_TYPE)
    string(TOUPPER "${_actual_CMAKE_BUILD_TYPE}" _config)
    set(_actual_CMAKE_C_FLAGS_${_config} "")
    set(_actual_CMAKE_EXE_LINKER_FLAGS_${_config} "")
    if(NOT _config STREQUAL "")
        load_cache("${cbuild}" READ_WITH_PREFIX _actual_
            CMAKE_C_FLAGS_${_config} CMAKE_EXE_LINKER_FLAGS_${_config})
    endif()
    set(_asan_lane FALSE)
    foreach(_flags IN ITEMS "${_actual_CMAKE_C_FLAGS}"
            "${_actual_CMAKE_C_FLAGS_${_config}}"
            "${_actual_CMAKE_EXE_LINKER_FLAGS}"
            "${_actual_CMAKE_EXE_LINKER_FLAGS_${_config}}")
        separate_arguments(_args NATIVE_COMMAND "${_flags}")
        foreach(_flag IN LISTS _args)
            if(_flag MATCHES "^-fsanitize=(.*)$")
                if(",${CMAKE_MATCH_1}," MATCHES ",address,")
                    set(_asan_lane TRUE)
                endif()
            elseif(_flag MATCHES "^-fno-sanitize=(.*)$")
                if(",${CMAKE_MATCH_1}," MATCHES ",(address|all),")
                    set(_asan_lane FALSE)
                endif()
            endif()
        endforeach()
    endforeach()
    set(${out} "${_asan_lane}" PARENT_SCOPE)
endfunction()

# Reuse the loader oracle without introducing another trace parser.
function(_check_loaded exe sdk_prefix label)
    _scrubbed_env(_env)
    loader_trace_env(_trace_env)
    if(NOT _trace_env)
        message(FATAL_ERROR "${label}: unsupported loader trace host")
    endif()
    _step("${label} traced run" _trace ${_env} "${_trace_env}" "${exe}")
    get_filename_component(_consumer_build "${exe}" DIRECTORY)
    _consumer_asan_lane("${_consumer_build}" _asan_lane)
    loader_only_image_records("${_trace}" "${CMAKE_HOST_SYSTEM_NAME}"
        _why "${_asan_lane}")
    if(_why)
        message(FATAL_ERROR "${label}: ${_why}")
    endif()
    loader_parse_images("${_trace}" "${CMAKE_HOST_SYSTEM_NAME}" _images)
    set(_expected "${CONSUMER_PROVIDER_FILE}")
    foreach(_kind ADAPTER CORE)
        if(CONSUMER_${_kind}_TYPE STREQUAL "SHARED_LIBRARY")
            list(APPEND _expected "${CONSUMER_${_kind}_FILE}")
        endif()
    endforeach()
    # Darwin includes the executable itself in its image records.
    list(REMOVE_ITEM _images "${exe}")
    loader_require_images("${_images}" "${_expected}"
        "${sdk_prefix};${BUILD};${SRC}" "${label}" _why)
    if(_why)
        message(FATAL_ERROR "${_why}")
    endif()
    _check_provider_binding("${label} mapped")
    message(STATUS "${label}: mapped exactly ${_expected}")
endfunction()

# The package directory the install actually produced: the one installed
# libmoqConfig.cmake under the SDK root. The consumer is pointed at it with
# libmoq_DIR so the layout (lib, lib64, an architecture triplet) is whatever
# the install wrote, not a search rule of the host platform -- Debian's CMake,
# for one, does not search lib64 under a prefix.
function(_sdk_package_dir root label out_var)
    file(GLOB_RECURSE _cfgs "${root}/*/libmoqConfig.cmake")
    list(LENGTH _cfgs _n)
    if(NOT _n EQUAL 1)
        message(FATAL_ERROR "${label}: expected exactly one installed libmoqConfig.cmake under ${root}, found ${_n}: ${_cfgs}")
    endif()
    cmake_path(GET _cfgs PARENT_PATH _dir)
    message(STATUS "${label}: SDK package directory ${_dir}")
    set(${out_var} "${_dir}" PARENT_SCOPE)
endfunction()

# -- 1. consumer against the freshly installed prefix ----------------------
_sdk_package_dir("${_prefix}" "installed prefix" _pkgdir)
_step("installed prefix configure" _out ${CMAKE_COMMAND} -S "${SRC}" -B "${_cbuild}"
      "-DCMAKE_PREFIX_PATH=${_prefix}" "-Dlibmoq_DIR=${_pkgdir}" ${_fwd})
_read_record("${_cbuild}" "installed prefix")
_check_sdk_membership("${_prefix}" "installed prefix")
_check_provider_binding("installed prefix")
_step("installed prefix build" _out ${CMAKE_COMMAND} --build "${_cbuild}")
_run_consumer("${_cbuild}/moq_msquic_consumer_test" "installed prefix")
_check_loaded("${_cbuild}/moq_msquic_consumer_test" "${_prefix}" "installed prefix")

# -- 2. the same SDK, relocated: original path gone, new path has a space ---
file(RENAME "${_prefix}" "${_relocated}")
if(EXISTS "${_prefix}")
    message(FATAL_ERROR "the original install path still exists after relocation")
endif()
_sdk_package_dir("${_relocated}" "relocated prefix" _pkgdir2)
_step("relocated prefix configure" _out ${CMAKE_COMMAND} -S "${SRC}" -B "${_cbuild2}"
      "-DCMAKE_PREFIX_PATH=${_relocated}" "-Dlibmoq_DIR=${_pkgdir2}" ${_fwd})
_read_record("${_cbuild2}" "relocated prefix")
# the relocated consumer must have resolved the RELOCATED SDK, not a remnant
_check_sdk_membership("${_relocated}" "relocated prefix")
_check_provider_binding("relocated prefix")
_step("relocated prefix build" _out ${CMAKE_COMMAND} --build "${_cbuild2}")
_run_consumer("${_cbuild2}/moq_msquic_consumer_test" "relocated prefix")
_check_loaded("${_cbuild2}/moq_msquic_consumer_test" "${_relocated}" "relocated prefix")

message(STATUS "msquic_install_consumer: OK")
