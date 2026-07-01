# check_bridge_symbols.cmake — pin the transport_bridge exported adapter ABI.
#
# moq_transport_bridge_* symbols are private exported ABI for adapter
# libraries: the shared libmoq-core exports them so separately-built adapter
# DSOs can link them, but the header (moq/transport_bridge.h) is intentionally
# NOT installed and this is NOT a stable third-party/application ABI.
#
# This test pins the EXACT set: the shared library must export ALL of the
# listed symbols and NO OTHER moq_transport_bridge_* symbol. It fails on a
# missing symbol (accidental visibility breakage) or an extra one (an
# unreviewed addition to the private adapter ABI). Either way, adding or
# removing an adapter-ABI symbol is a deliberate change that must update the
# BASE_SYMBOLS list below.
#
# Usage: cmake -DDYLIB=<path-to-libmoq-core.so/.dylib>
#              -P check_bridge_symbols.cmake

if(NOT DYLIB)
    message(FATAL_ERROR "DYLIB not set")
endif()

if(NOT EXISTS "${DYLIB}")
    message(FATAL_ERROR "Library not found: ${DYLIB}")
endif()

# Detect platform and set symbol names + nm flags.
cmake_host_system_information(RESULT _os QUERY OS_NAME)
if(_os MATCHES "Darwin" OR _os MATCHES "macOS")
    set(SYM_PREFIX "_")
    set(NM_FLAGS -gU)
elseif(_os MATCHES "Windows")
    message(STATUS "bridge_symbol_policy: skipped on Windows")
    return()
else()
    # Linux / ELF
    set(SYM_PREFIX "")
    set(NM_FLAGS -D --defined-only)
endif()

# The complete private adapter ABI surface (moq/transport_bridge.h MOQ_API
# symbols). Keep in sync with the header; any change here is a deliberate
# adapter-ABI change.
set(BASE_SYMBOLS
    moq_transport_bridge_cfg_init
    moq_transport_bridge_create
    moq_transport_bridge_destroy
    moq_transport_bridge_service
    moq_transport_bridge_on_peer_control_bytes
    moq_transport_bridge_on_peer_uni_bytes
    moq_transport_bridge_on_peer_uni_rcbuf
    moq_transport_bridge_on_peer_bidi_bytes
    moq_transport_bridge_on_peer_stream_reset
    moq_transport_bridge_on_peer_stop_sending
    moq_transport_bridge_on_peer_datagram
    moq_transport_bridge_on_transport_close
    moq_transport_bridge_on_transport_error
    moq_transport_bridge_is_fatal
    moq_transport_bridge_fatal_code
    moq_transport_bridge_is_closed
    moq_transport_bridge_close_code
    moq_transport_bridge_is_terminal
    moq_transport_bridge_has_pending
    moq_transport_bridge_has_outbound_pending
    moq_transport_bridge_uses_uni_control
    moq_transport_bridge_stream_has_pending
    moq_transport_bridge_stream_count
    moq_transport_bridge_tombstone_count
    moq_transport_bridge_find_ref
)

# Build the platform-specific expected symbol list (prefixed as nm reports).
set(EXPECTED_SYMBOLS "")
foreach(base IN LISTS BASE_SYMBOLS)
    list(APPEND EXPECTED_SYMBOLS "${SYM_PREFIX}${base}")
endforeach()

find_program(NM_PROG NAMES ${CMAKE_NM} nm)
if(NOT NM_PROG)
    message(FATAL_ERROR "nm not found")
endif()

execute_process(
    COMMAND ${NM_PROG} ${NM_FLAGS} "${DYLIB}"
    OUTPUT_VARIABLE nm_output
    ERROR_VARIABLE nm_err
    RESULT_VARIABLE nm_rc
)

if(NOT nm_rc EQUAL 0)
    message(FATAL_ERROR "nm failed (rc=${nm_rc}): ${nm_err}")
endif()

# Extract every exported moq_transport_bridge_* symbol actually present.
string(REGEX MATCHALL "${SYM_PREFIX}moq_transport_bridge_[A-Za-z0-9_]+"
    FOUND_SYMBOLS "${nm_output}")
if(FOUND_SYMBOLS)
    list(REMOVE_DUPLICATES FOUND_SYMBOLS)
endif()

# Missing: expected but not exported (accidental visibility breakage).
set(missing "")
foreach(sym IN LISTS EXPECTED_SYMBOLS)
    list(FIND FOUND_SYMBOLS "${sym}" idx)
    if(idx EQUAL -1)
        list(APPEND missing "${sym}")
    endif()
endforeach()

# Extra: exported but not in the pinned allowlist (unreviewed ABI addition).
set(extra "")
foreach(sym IN LISTS FOUND_SYMBOLS)
    list(FIND EXPECTED_SYMBOLS "${sym}" idx)
    if(idx EQUAL -1)
        list(APPEND extra "${sym}")
    endif()
endforeach()

if(missing OR extra)
    set(msg "Bridge symbol policy violation for ${DYLIB}:")
    if(missing)
        string(REPLACE ";" "\n    " missing_str "${missing}")
        string(APPEND msg
            "\n  MISSING (must be MOQ_API for adapter DSOs):\n    ${missing_str}")
    endif()
    if(extra)
        string(REPLACE ";" "\n    " extra_str "${extra}")
        string(APPEND msg
            "\n  UNEXPECTED (exported but not in the pinned adapter-ABI list;"
            " update BASE_SYMBOLS if this is intentional):\n    ${extra_str}")
    endif()
    message(FATAL_ERROR "${msg}")
endif()

list(LENGTH EXPECTED_SYMBOLS count)
message(STATUS
    "bridge_symbol_policy: exact adapter-ABI allowlist verified (${count} symbols)")
