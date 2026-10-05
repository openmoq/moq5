# API availability only, not backend qualification or a warning-clean configure.
# try_compile exposes build output here; its configure diagnostics go directly
# to the caller. Qualification drivers must inspect the complete CMake output.
function(moq_check_wtquic_api result reason)
    set(${result} FALSE PARENT_SCOPE)
    set(_why "wtquic public API compile/link contract requires wtq_session_receive_contract and wtq_session_service_stream_admission (wtquic_DIR='${wtquic_DIR}').")
    if(NOT TARGET wtq::wtquic)
        set(${reason} "${_why} Imported target wtq::wtquic is unavailable." PARENT_SCOPE)
        return()
    endif()
    get_property(_languages GLOBAL PROPERTY ENABLED_LANGUAGES)
    if("C" IN_LIST _languages)
        set(_extension c)
    elseif("CXX" IN_LIST _languages)
        set(_extension cpp)
    else()
        set(${reason} "${_why} An enabled C or C++ language and executable-link-capable toolchain are required." PARENT_SCOPE)
        return()
    endif()
    set(_dir "${CMAKE_BINARY_DIR}/CMakeFiles/moq-wtquic-api")
    file(MAKE_DIRECTORY "${_dir}")
    file(WRITE "${_dir}/probe.${_extension}" [=[
#include <wtquic/session.h>
static wtq_result_t (*volatile receive_contract)(const wtq_session_t *,
    size_t *, wtq_receive_pause_mode_t *) = wtq_session_receive_contract;
static wtq_result_t (*volatile service_admission)(wtq_session_t *) =
    wtq_session_service_stream_admission;
int main(void) { return receive_contract == 0 || service_admission == 0; }
]=])
    # Function scope restores caller state, including a STATIC_LIBRARY override.
    # Always relink: cached success must not survive replacement of the SDK.
    set(CMAKE_TRY_COMPILE_TARGET_TYPE EXECUTABLE)
    unset(_moq_wt_api_links CACHE)
    unset(_moq_wt_api_links)
    try_compile(_moq_wt_api_links "${_dir}/build" "${_dir}/probe.${_extension}"
        LINK_LIBRARIES wtq::wtquic OUTPUT_VARIABLE _output)
    file(WRITE "${_dir}/output.log"
        "Build output only; configure diagnostics are in the calling CMake process output.\nwtquic_DIR=${wtquic_DIR}\n${_output}\n")
    set(_ok "${_moq_wt_api_links}")
    unset(_moq_wt_api_links CACHE)
    if(NOT _ok OR _output MATCHES "[Ww][Aa][Rr][Nn][Ii][Nn][Gg]:|CMake (Deprecation )?Warning|\\[-Werror")
        set(${reason} "${_why} Probe failed or emitted diagnostics; this may be an SDK or toolchain/link-closure incompatibility. See ${_dir}/output.log." PARENT_SCOPE)
        return()
    endif()
    set(${result} TRUE PARENT_SCOPE)
    set(${reason} "" PARENT_SCOPE)
endfunction()

# Package component preflight. No REQUIRED lookup here: the containing package
# owns REQUIRED/OPTIONAL_COMPONENTS semantics and must remain usable when a
# caller only optionally requests an incompatible WT component.
macro(moq_wtquic_component_preflight component)
    set(_libmoq_wt_api_ok TRUE)
    set(_libmoq_wt_needed FALSE)
    set(_libmoq_wt_components)
    if("${component}" MATCHES "^adapter-wtquic")
        set(_libmoq_wt_needed TRUE)
        if("${component}" STREQUAL "adapter-wtquic-network-managed")
            list(APPEND _libmoq_wt_components network)
        elseif("${component}" STREQUAL "adapter-wtquic-msquic-managed")
            list(APPEND _libmoq_wt_components msquic)
        endif()
    elseif("${component}" STREQUAL "service")
        if(EXISTS "${CMAKE_CURRENT_LIST_DIR}/libmoqWtquicNetworkManagedTargets.cmake" OR
           EXISTS "${CMAKE_CURRENT_LIST_DIR}/libmoqWtquicMsquicManagedTargets.cmake")
            set(_libmoq_wt_needed TRUE)
        endif()
        if(EXISTS "${CMAKE_CURRENT_LIST_DIR}/libmoqWtquicNetworkManagedTargets.cmake")
            list(APPEND _libmoq_wt_components network)
        endif()
        if(EXISTS "${CMAKE_CURRENT_LIST_DIR}/libmoqWtquicMsquicManagedTargets.cmake")
            list(APPEND _libmoq_wt_components msquic)
        endif()
    endif()
    if(_libmoq_wt_needed)
        find_package(wtquic CONFIG QUIET COMPONENTS ${_libmoq_wt_components})
        moq_check_wtquic_api(_libmoq_wt_api_ok _libmoq_wt_api_reason)
        if(NOT wtquic_FOUND)
            set(_libmoq_wt_api_ok FALSE)
            set(_libmoq_wt_api_reason "wtquic public API compile/link contract requires wtq_session_receive_contract and wtq_session_service_stream_admission; package lookup failed (wtquic_DIR='${wtquic_DIR}'). ${wtquic_NOT_FOUND_MESSAGE}")
        endif()
        if(NOT _libmoq_wt_api_ok)
            set(${CMAKE_FIND_PACKAGE_NAME}_${component}_FOUND FALSE)
            set(${CMAKE_FIND_PACKAGE_NAME}_NOT_FOUND_MESSAGE "${_libmoq_wt_api_reason}")
        endif()
    endif()
endmacro()
