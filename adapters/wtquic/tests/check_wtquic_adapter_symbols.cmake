# check_wtquic_adapter_symbols.cmake — pin the wtquic attach adapter's exports.
#
# moq_wtquic_* symbols split into two groups, both deliberate:
#   - the installed public API (moq/wtquic.h), and
#   - ONE private, non-installed lockstep SPI (wtquic_adapter_internal.h) the
#     managed facades link across their separate DSO boundary.
# This test pins the EXACT set: every listed symbol must be exported and NO
# OTHER moq_wtquic_* symbol may be. It fails on a missing symbol (visibility
# breakage) or an extra one — including a leaked white-box test seam.
#
# Usage: cmake -DLIB=<path-to-libmoq-adapter-wtquic.a/.dylib/.so>
#              -P check_wtquic_adapter_symbols.cmake

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
    message(STATUS "wtquic_adapter_symbol_policy: skipped on Windows")
    return()
else()
    set(SYM_PREFIX "")
    set(NM_FLAGS --defined-only --extern-only)
endif()

# Installed public API (moq/wtquic.h).
set(BASE_SYMBOLS
    moq_wtquic_conn_cfg_init_sized
    moq_wtquic_conn_create
    moq_wtquic_conn_destroy
    moq_wtquic_conn_events
    moq_wtquic_conn_session
    moq_wtquic_conn_wtq_session
    moq_wtquic_conn_service
    moq_wtquic_conn_is_fatal
    moq_wtquic_conn_is_closed
)
# The single private lockstep SPI (wtquic_adapter_internal.h, NOT installed):
# the managed facades read the session's event-progress token + pending-event
# bit through it. Any addition here is a deliberate adapter-SPI change.
list(APPEND BASE_SYMBOLS moq_wtquic_conn_event_progress)
list(APPEND BASE_SYMBOLS moq_wtquic_conn_terminal_facts)

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

string(REGEX MATCHALL "${SYM_PREFIX}moq_wtquic_[A-Za-z0-9_]+"
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

if(missing OR extra)
    set(msg "wtquic adapter symbol policy violation for ${LIB}:")
    if(missing)
        string(REPLACE ";" "\n    " missing_str "${missing}")
        string(APPEND msg "\n  MISSING:\n    ${missing_str}")
    endif()
    if(extra)
        string(REPLACE ";" "\n    " extra_str "${extra}")
        string(APPEND msg
            "\n  UNEXPECTED (exported but not pinned — a new SPI export or a"
            " leaked test seam; update BASE_SYMBOLS only if intentional):"
            "\n    ${extra_str}")
    endif()
    message(FATAL_ERROR "${msg}")
endif()

list(LENGTH EXPECTED_SYMBOLS count)
message(STATUS
    "wtquic_adapter_symbol_policy: exact export allowlist verified (${count} symbols)")
