cmake_minimum_required(VERSION 3.20)
project(wtquic_api_fixture LANGUAGES C)
if(NOT DEFINED PROBE_CONFIG)
    set(PROBE_CONFIG Debug)
endif()
if(NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE "${PROBE_CONFIG}" CACHE STRING "Fixture configuration" FORCE)
elseif(NOT PROBE_CONFIG IN_LIST CMAKE_CONFIGURATION_TYPES)
    message(FATAL_ERROR "Fixture configuration '${PROBE_CONFIG}' is unavailable")
endif()
# Deliberately tiny ABI fixtures, not provider implementations or qualification.
set(_api_kinds old query admission flush-mode flush-query flush new)
set(_msquic_kinds msquic-old msquic-new msquic-missing msquic-keepalive-old
    msquic-keepalive-missing)
if(API_KIND IN_LIST _msquic_kinds)
    # An MsQuic component over a separately built core fixture: only the
    # MsQuic header and library. msquic-old's header predates
    # max_local_streams and keep_alive_interval_ms while its library defines
    # every probed entry point; msquic-missing declares every entry point but
    # defines no listener_start. msquic-keepalive-old differs from msquic-new
    # only in a header without keep_alive_interval_ms; msquic-keepalive-missing
    # only in a library without wtq_msquic_env_cfg_init_ex.
    set(local_field "")
    set(local_init "")
    if(NOT API_KIND STREQUAL "msquic-old")
        set(local_field " uint32_t max_local_streams;")
        set(local_init ", 0")
    endif()
    set(keep_alive_field "")
    set(keep_alive_init "")
    if(NOT API_KIND MATCHES "^msquic-(old|keepalive-old)$")
        set(keep_alive_field " uint32_t _reserved_keep_alive; uint32_t keep_alive_interval_ms;")
        set(keep_alive_init ", 0, 0")
    endif()
    file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/include/wtquic")
    file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/include/wtquic/wtquic_msquic.h"
        "#include <stddef.h>\n#include <stdint.h>\n#include <wtquic/session.h>\n#ifdef __cplusplus\nextern \"C\" {\n#endif\n"
        "typedef struct wtq_msquic_env wtq_msquic_env_t;\ntypedef struct wtq_msquic_listener wtq_msquic_listener_t;\n"
        "typedef struct { uint32_t struct_size;${local_field} } wtq_msquic_client_cfg_t;\n"
        "typedef struct { uint32_t struct_size;${local_field} } wtq_msquic_listener_cfg_t;\n"
        "#define WTQ_MSQUIC_CLIENT_CFG_INIT { (uint32_t)sizeof(wtq_msquic_client_cfg_t)${local_init} }\n"
        "#define WTQ_MSQUIC_LISTENER_CFG_INIT { (uint32_t)sizeof(wtq_msquic_listener_cfg_t)${local_init} }\n"
        "typedef struct { uint32_t struct_size;${keep_alive_field} } wtq_msquic_env_cfg_t;\n"
        "#define WTQ_MSQUIC_ENV_CFG_INIT { (uint32_t)sizeof(wtq_msquic_env_cfg_t)${keep_alive_init} }\n"
        "void wtq_msquic_client_cfg_init_ex(wtq_msquic_client_cfg_t *, size_t);\n"
        "void wtq_msquic_listener_cfg_init_ex(wtq_msquic_listener_cfg_t *, size_t);\n"
        "void wtq_msquic_env_cfg_init_ex(wtq_msquic_env_cfg_t *, size_t);\n"
        "wtq_result_t wtq_msquic_client_connect(wtq_msquic_env_t *, const wtq_msquic_client_cfg_t *, wtq_session_t **);\n"
        "wtq_result_t wtq_msquic_listener_start(wtq_msquic_env_t *, const wtq_msquic_listener_cfg_t *, wtq_msquic_listener_t **);\n"
        "#ifdef __cplusplus\n}\n#endif\n")
    set(source "#include <wtquic/wtquic_msquic.h>\n"
        "void wtq_msquic_client_cfg_init_ex(wtq_msquic_client_cfg_t *c, size_t n) { (void)c; (void)n; }\n"
        "void wtq_msquic_listener_cfg_init_ex(wtq_msquic_listener_cfg_t *c, size_t n) { (void)c; (void)n; }\n"
        "wtq_result_t wtq_msquic_client_connect(wtq_msquic_env_t *e, const wtq_msquic_client_cfg_t *c, wtq_session_t **s) { (void)e; (void)c; (void)s; return -1; }\n")
    if(NOT API_KIND STREQUAL "msquic-missing")
        string(APPEND source "wtq_result_t wtq_msquic_listener_start(wtq_msquic_env_t *e, const wtq_msquic_listener_cfg_t *c, wtq_msquic_listener_t **l) { (void)e; (void)c; (void)l; return -1; }\n")
    endif()
    if(NOT API_KIND STREQUAL "msquic-keepalive-missing")
        string(APPEND source "void wtq_msquic_env_cfg_init_ex(wtq_msquic_env_cfg_t *c, size_t n) { (void)c; (void)n; }\n")
    endif()
    file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/fixture.c" "${source}")
    add_library(fixture STATIC "${CMAKE_CURRENT_BINARY_DIR}/fixture.c")
    target_include_directories(fixture PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/include"
        "${CORE_INCLUDE}")
    file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/library-$<CONFIG>.txt"
        CONTENT "$<TARGET_FILE:fixture>")
    return()
endif()
if(NOT API_KIND IN_LIST _api_kinds)
    message(FATAL_ERROR "Unknown WT API fixture kind '${API_KIND}'")
endif()
set(_has_admission FALSE)
set(_has_flush_mode FALSE)
set(_has_flush_query FALSE)
set(_has_budget FALSE)
if(API_KIND MATCHES "^(admission|flush-mode|flush-query|flush|new)$")
    set(_has_admission TRUE)
endif()
if(API_KIND MATCHES "^(flush-mode|flush|new)$")
    set(_has_flush_mode TRUE)
endif()
if(API_KIND MATCHES "^(flush-query|flush|new)$")
    set(_has_flush_query TRUE)
endif()
if(API_KIND MATCHES "^(flush-mode|flush-query|new)$")
    set(_has_budget TRUE)
endif()
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/include/wtquic")
# Guarded: the MsQuic component fixture's header includes this one too.
set(header "#ifndef WTQ_API_FIXTURE_SESSION_H\n#define WTQ_API_FIXTURE_SESSION_H\n#include <stddef.h>\n#ifdef __cplusplus\nextern \"C\" {\n#endif\ntypedef struct wtq_session wtq_session_t;\ntypedef int wtq_result_t;\ntypedef enum { WTQ_PAUSE_NONE } wtq_receive_pause_mode_t;\n")
if(NOT API_KIND STREQUAL "old")
    string(APPEND header "wtq_result_t wtq_session_receive_contract(const wtq_session_t *, size_t *, wtq_receive_pause_mode_t *);\n")
endif()
if(_has_admission)
    string(APPEND header "wtq_result_t wtq_session_service_stream_admission(wtq_session_t *);\n")
endif()
if(_has_budget)
    string(APPEND header "typedef struct { unsigned int struct_size; unsigned int api_capacity; } wtq_stream_budget_t;\n#define WTQ_STREAM_BUDGET_V1_SIZE (sizeof(wtq_stream_budget_t))\n#define WTQ_STREAM_BUDGET_INIT { sizeof(wtq_stream_budget_t), 0 }\nwtq_result_t wtq_session_stream_budget(const wtq_session_t *, wtq_stream_budget_t *);\n")
endif()
if(_has_flush_mode OR _has_flush_query)
    string(APPEND header "typedef enum { WTQ_LOCAL_FLUSH_UNSUPPORTED = 0, WTQ_LOCAL_FLUSH_TRANSPORT = 1, WTQ_LOCAL_FLUSH_ACKNOWLEDGED = 2 } wtq_local_flush_mode_t;\n")
endif()
if(_has_flush_mode)
    string(APPEND header "wtq_local_flush_mode_t wtq_session_local_flush_mode(const wtq_session_t *);\n")
endif()
if(_has_flush_query)
    string(APPEND header "wtq_result_t wtq_session_local_flush(const wtq_session_t *);\n")
endif()
string(APPEND header "#ifdef __cplusplus\n}\n#endif\n#endif\n")
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/include/wtquic/session.h" "${header}")
set(source "#include <wtquic/session.h>\nint fixture_anchor(void) { return 0; }\n")
if(NOT API_KIND STREQUAL "old")
    string(APPEND source "wtq_result_t wtq_session_receive_contract(const wtq_session_t *s, size_t *n, wtq_receive_pause_mode_t *m) { (void)s; (void)n; (void)m; return -1; }\n")
endif()
if(_has_admission)
    string(APPEND source "wtq_result_t wtq_session_service_stream_admission(wtq_session_t *s) { (void)s; return -1; }\n")
endif()
if(_has_budget)
    string(APPEND source "wtq_result_t wtq_session_stream_budget(const wtq_session_t *s, wtq_stream_budget_t *b) { (void)s; (void)b; return -1; }\n")
endif()
if(_has_flush_mode)
    string(APPEND source "wtq_local_flush_mode_t wtq_session_local_flush_mode(const wtq_session_t *s) { (void)s; return WTQ_LOCAL_FLUSH_UNSUPPORTED; }\n")
endif()
if(_has_flush_query)
    string(APPEND source "wtq_result_t wtq_session_local_flush(const wtq_session_t *s) { (void)s; return -1; }\n")
endif()
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/fixture.c" "${source}")
add_library(fixture STATIC "${CMAKE_CURRENT_BINARY_DIR}/fixture.c")
target_include_directories(fixture PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/include")
file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/library-$<CONFIG>.txt"
    CONTENT "$<TARGET_FILE:fixture>")
