# check_wtquic_msquic_managed_symbols.cmake — pin the MsQuic managed facade's
# exports.
#
# The intended public surface is exactly the MOQ_API functions declared in the
# installed header moq/wtquic_msquic_managed.h. This test pins that EXACT set:
#   - every listed moq_wtquic_msquic_* symbol must be exported, and no other
#     moq_wtquic_msquic_* symbol may be (a leaked white-box seam or an
#     undeclared addition);
#   - for a shared library, NO symbol outside the moq_ namespace may be
#     exported. The facade links wtquic's MsQuic backend; when that backend is
#     a static archive its WTQ_API functions would otherwise be re-exported
#     from this library as an accidental second home for wtquic's API.
#
# Usage: cmake -DLIB=<path-to-libmoq-adapter-wtquic-msquic-managed.{a,dylib,so}>
#              -P check_wtquic_msquic_managed_symbols.cmake

if(NOT LIB)
    message(FATAL_ERROR "LIB not set")
endif()
if(NOT EXISTS "${LIB}")
    message(FATAL_ERROR "Library not found: ${LIB}")
endif()

cmake_host_system_information(RESULT _os QUERY OS_NAME)
if(_os MATCHES "Darwin" OR _os MATCHES "macOS")
    set(SYM_PREFIX "_")
    set(NM_FLAGS -gU)
elseif(_os MATCHES "Windows")
    message(STATUS "wtquic_msquic_managed_symbol_policy: skipped on Windows")
    return()
else()
    set(SYM_PREFIX "")
    set(NM_FLAGS --defined-only --extern-only)
endif()

# Installed public API (moq/wtquic_msquic_managed.h).
set(BASE_SYMBOLS
    moq_wtquic_msquic_lane_get_stats
    moq_wtquic_msquic_lane_index
    moq_wtquic_msquic_lane_next_conn
    moq_wtquic_msquic_lane_wake
    moq_wtquic_msquic_managed_adapter
    moq_wtquic_msquic_managed_cfg_init_sized
    moq_wtquic_msquic_managed_close_code
    moq_wtquic_msquic_managed_conn_ack_terminal
    moq_wtquic_msquic_managed_conn_adapter
    moq_wtquic_msquic_managed_conn_close
    moq_wtquic_msquic_managed_conn_count
    moq_wtquic_msquic_managed_conn_lane
    moq_wtquic_msquic_managed_conn_negotiated_version
    moq_wtquic_msquic_managed_conn_session
    moq_wtquic_msquic_managed_conn_set_user
    moq_wtquic_msquic_managed_conn_user
    moq_wtquic_msquic_managed_create
    moq_wtquic_msquic_managed_destroy
    moq_wtquic_msquic_managed_drain
    moq_wtquic_msquic_managed_fatal_code
    moq_wtquic_msquic_managed_is_closed
    moq_wtquic_msquic_managed_is_fatal
    moq_wtquic_msquic_managed_join
    moq_wtquic_msquic_managed_lane
    moq_wtquic_msquic_managed_lane_count
    moq_wtquic_msquic_managed_negotiated_version
    moq_wtquic_msquic_managed_poll_flush
    moq_wtquic_msquic_managed_port
    moq_wtquic_msquic_managed_session
    moq_wtquic_msquic_managed_stop
    moq_wtquic_msquic_managed_stop_begin
    moq_wtquic_msquic_managed_wait
    moq_wtquic_msquic_managed_wake
)

set(EXPECTED_SYMBOLS "")
foreach(base IN LISTS BASE_SYMBOLS)
    list(APPEND EXPECTED_SYMBOLS "${SYM_PREFIX}${base}")
endforeach()

find_program(NM_PROG NAMES ${CMAKE_NM} nm)
if(NOT NM_PROG)
    message(FATAL_ERROR "nm not found")
endif()

execute_process(
    COMMAND ${NM_PROG} ${NM_FLAGS} "${LIB}"
    OUTPUT_VARIABLE nm_output
    ERROR_VARIABLE nm_err
    RESULT_VARIABLE nm_rc
)
if(NOT nm_rc EQUAL 0)
    message(FATAL_ERROR "nm failed (rc=${nm_rc}): ${nm_err}")
endif()

string(REGEX MATCHALL "${SYM_PREFIX}moq_wtquic_msquic_[A-Za-z0-9_]+"
    FOUND_SYMBOLS "${nm_output}")
if(FOUND_SYMBOLS)
    list(REMOVE_DUPLICATES FOUND_SYMBOLS)
endif()

set(missing "")
foreach(sym IN LISTS EXPECTED_SYMBOLS)
    list(FIND FOUND_SYMBOLS "${sym}" idx)
    if(idx EQUAL -1)
        list(APPEND missing "${sym}")
    endif()
endforeach()

set(extra "")
foreach(sym IN LISTS FOUND_SYMBOLS)
    list(FIND EXPECTED_SYMBOLS "${sym}" idx)
    if(idx EQUAL -1)
        list(APPEND extra "${sym}")
    endif()
endforeach()

# Shared library: every exported name must be in the moq_ namespace. ELF
# images also carry linker-defined markers, which are not this library's API.
set(foreign "")
if(LIB MATCHES "\\.(dylib|so)(\\.[0-9.]+)?$")
    string(REPLACE "\n" ";" nm_lines "${nm_output}")
    foreach(line IN LISTS nm_lines)
        if(line MATCHES "^[0-9A-Fa-f]* *[A-Za-z] +([^ ]+)$")
            set(name "${CMAKE_MATCH_1}")
            if(name MATCHES "^${SYM_PREFIX}moq_")
                continue()
            endif()
            if(name MATCHES "^(_init|_fini|_edata|_end|__bss_start|_DYNAMIC|_GLOBAL_OFFSET_TABLE_)$")
                continue()
            endif()
            list(APPEND foreign "${name}")
        endif()
    endforeach()
endif()

if(missing OR extra OR foreign)
    set(msg "wtquic MsQuic managed symbol policy violation for ${LIB}:")
    if(missing)
        string(REPLACE ";" "\n    " missing_str "${missing}")
        string(APPEND msg "\n  MISSING:\n    ${missing_str}")
    endif()
    if(extra)
        string(REPLACE ";" "\n    " extra_str "${extra}")
        string(APPEND msg
            "\n  UNEXPECTED moq_wtquic_msquic_* (not declared in"
            " moq/wtquic_msquic_managed.h, or a leaked test seam):"
            "\n    ${extra_str}")
    endif()
    if(foreign)
        string(REPLACE ";" "\n    " foreign_str "${foreign}")
        string(APPEND msg
            "\n  FOREIGN (exported outside the moq_ namespace, e.g. a"
            " re-exported dependency implementation):\n    ${foreign_str}")
    endif()
    message(FATAL_ERROR "${msg}")
endif()

list(LENGTH EXPECTED_SYMBOLS count)
message(STATUS
    "wtquic_msquic_managed_symbol_policy: exact export allowlist verified (${count} symbols)")
