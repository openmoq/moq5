# API availability only, not backend qualification or a warning-clean configure.
# try_compile exposes build output here; its configure diagnostics go directly
# to the caller. Qualification drivers must inspect the complete CMake output.
function(moq_check_wtquic_api result reason)
    set(${result} FALSE PARENT_SCOPE)
    set(_why "wtquic public API compile/link contract requires wtq_session_receive_contract, wtq_session_service_stream_admission, wtq_session_stream_budget, wtq_session_local_flush_mode and wtq_session_local_flush (wtquic_DIR='${wtquic_DIR}').")
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
static wtq_result_t (*volatile stream_budget)(const wtq_session_t *,
    wtq_stream_budget_t *) = wtq_session_stream_budget;
static wtq_local_flush_mode_t (*volatile local_flush_mode)(const wtq_session_t *) =
    wtq_session_local_flush_mode;
static wtq_result_t (*volatile local_flush)(const wtq_session_t *) =
    wtq_session_local_flush;
int main(void) {
    wtq_stream_budget_t budget = WTQ_STREAM_BUDGET_INIT;
    return budget.struct_size < WTQ_STREAM_BUDGET_V1_SIZE ||
           budget.api_capacity != 0 || stream_budget == 0 ||
           receive_contract == 0 || service_admission == 0 ||
           local_flush_mode == 0 || local_flush == 0;
}
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

# The MsQuic managed facade's stream-resource contract, checked only when that
# component is required. The probe compiles against the installed MsQuic
# header (the appended client/listener max_local_streams fields, at their
# declared width, and the complete current initializers) AND links through
# wtq::msquic the provider entry points the facade calls plus the core budget
# query. It refuses an older header, a backend missing one of those entry
# points and a core without the budget query. It cannot tell an older backend
# library that exports the same entry points from a current one (the stream
# plan adds no backend export): such a library links, and each WebTransport
# session it creates is refused when the attach adapter's budget query returns
# UNSUPPORTED, after establishment, not at managed create().
function(moq_check_wtquic_msquic_api result reason)
    set(${result} FALSE PARENT_SCOPE)
    set(_why "wtquic public API compile/link contract requires the MsQuic component's wtq_msquic_client_cfg_t/wtq_msquic_listener_cfg_t max_local_streams with wtq_msquic_client_cfg_init_ex, wtq_msquic_listener_cfg_init_ex, wtq_msquic_client_connect, wtq_msquic_listener_start and wtq_session_stream_budget (wtquic_DIR='${wtquic_DIR}').")
    if(NOT TARGET wtq::msquic)
        set(${reason} "${_why} Imported target wtq::msquic is unavailable." PARENT_SCOPE)
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
    set(_dir "${CMAKE_BINARY_DIR}/CMakeFiles/moq-wtquic-msquic-api")
    file(MAKE_DIRECTORY "${_dir}")
    file(WRITE "${_dir}/probe.${_extension}" [=[
#include <stdint.h>
#include <wtquic/session.h>
#include <wtquic/wtquic_msquic.h>
static void (*volatile client_init)(wtq_msquic_client_cfg_t *, size_t) =
    wtq_msquic_client_cfg_init_ex;
static void (*volatile listener_init)(wtq_msquic_listener_cfg_t *, size_t) =
    wtq_msquic_listener_cfg_init_ex;
static wtq_result_t (*volatile client_connect)(wtq_msquic_env_t *,
    const wtq_msquic_client_cfg_t *, wtq_session_t **) =
    wtq_msquic_client_connect;
static wtq_result_t (*volatile listener_start)(wtq_msquic_env_t *,
    const wtq_msquic_listener_cfg_t *, wtq_msquic_listener_t **) =
    wtq_msquic_listener_start;
static wtq_result_t (*volatile stream_budget)(const wtq_session_t *,
    wtq_stream_budget_t *) = wtq_session_stream_budget;
int main(void) {
    wtq_msquic_client_cfg_t client = WTQ_MSQUIC_CLIENT_CFG_INIT;
    wtq_msquic_listener_cfg_t listener = WTQ_MSQUIC_LISTENER_CFG_INIT;
    uint32_t *client_local = &client.max_local_streams;
    uint32_t *listener_local = &listener.max_local_streams;
    return *client_local != 0 || *listener_local != 0 ||
           client_init == 0 || listener_init == 0 || client_connect == 0 ||
           listener_start == 0 || stream_budget == 0;
}
]=])
    set(CMAKE_TRY_COMPILE_TARGET_TYPE EXECUTABLE)
    unset(_moq_wt_msquic_api_links CACHE)
    unset(_moq_wt_msquic_api_links)
    try_compile(_moq_wt_msquic_api_links "${_dir}/build" "${_dir}/probe.${_extension}"
        LINK_LIBRARIES wtq::msquic OUTPUT_VARIABLE _output)
    file(WRITE "${_dir}/output.log"
        "Build output only; configure diagnostics are in the calling CMake process output.\nwtquic_DIR=${wtquic_DIR}\n${_output}\n")
    set(_ok "${_moq_wt_msquic_api_links}")
    unset(_moq_wt_msquic_api_links CACHE)
    if(NOT _ok OR _output MATCHES "[Ww][Aa][Rr][Nn][Ii][Nn][Gg]:|CMake (Deprecation )?Warning|\\[-Werror")
        set(${reason} "${_why} Probe failed or emitted diagnostics; this may be an older MsQuic header, a backend or core missing a probed entry point, or a toolchain/link-closure incompatibility. See ${_dir}/output.log." PARENT_SCOPE)
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
            set(_libmoq_wt_api_reason "wtquic public API compile/link contract requires wtq_session_receive_contract, wtq_session_service_stream_admission, wtq_session_stream_budget, wtq_session_local_flush_mode and wtq_session_local_flush; package lookup failed (wtquic_DIR='${wtquic_DIR}'). ${wtquic_NOT_FOUND_MESSAGE}")
        endif()
        # Only a consumer that actually needs the MsQuic managed facade (the
        # component itself, or a service built with it) pays this check;
        # base attach and Network-only discovery are unchanged.
        if(_libmoq_wt_api_ok AND "msquic" IN_LIST _libmoq_wt_components)
            moq_check_wtquic_msquic_api(_libmoq_wt_api_ok _libmoq_wt_api_reason)
        endif()
        if(NOT _libmoq_wt_api_ok)
            set(${CMAKE_FIND_PACKAGE_NAME}_${component}_FOUND FALSE)
            set(${CMAKE_FIND_PACKAGE_NAME}_NOT_FOUND_MESSAGE "${_libmoq_wt_api_reason}")
        endif()
    endif()
endmacro()
