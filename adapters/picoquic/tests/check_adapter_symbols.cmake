# check_adapter_symbols.cmake -- pin the exact dynamic export set of the shared
# libmoq-adapter-picoquic.
#
# The shared adapter exports exactly:
#   - its installed public API: <moq/picoquic.h> and <moq/picoquic_verify.h>
#     (PUBLIC_SYMBOLS), and
#   - the private adapter SPI in picoquic_spi.h (SPI_SYMBOLS), which the
#     separately built libmoq-adapter-picoquic-threaded links. That header is
#     not installed and is not application API.
# Every other symbol is internal and must not be exported. The check compares
# the complete set of defined dynamic symbols, not a prefix: a missing symbol
# (lost visibility annotation), an extra one (an internal helper leaking), or a
# wrong member at an equal count all fail by name. Adding or removing an export
# is a deliberate ABI change that must update these lists.
#
# Usage: cmake -DDYLIB=<path-to-libmoq-adapter-picoquic.{so,dylib}>
#              -P check_adapter_symbols.cmake

if(NOT DYLIB)
    message(FATAL_ERROR "DYLIB not set")
endif()
if(NOT EXISTS "${DYLIB}")
    message(FATAL_ERROR "Library not found: ${DYLIB}")
endif()

cmake_host_system_information(RESULT _os QUERY OS_NAME)
if(_os MATCHES "Darwin" OR _os MATCHES "macOS")
    set(SYM_PREFIX "_")
    set(NM_FLAGS -gU)
elseif(_os MATCHES "Windows")
    message(STATUS "adapter_picoquic_symbol_policy: skipped on Windows")
    return()
else()
    set(SYM_PREFIX "")
    set(NM_FLAGS -D --defined-only)
endif()

set(PUBLIC_SYMBOLS
    # <moq/picoquic.h>
    moq_pq_conn_cfg_init
    moq_pq_conn_cfg_init_sized
    moq_pq_conn_create
    moq_pq_conn_destroy
    moq_pq_conn_session
    moq_pq_conn_user_ctx
    moq_pq_conn_set_user_ctx
    moq_pq_conn_is_fatal
    moq_pq_conn_fatal_code
    moq_pq_conn_is_closed
    moq_pq_conn_close_code
    moq_pq_service
    moq_pq_callback
    # <moq/picoquic_verify.h>
    moq_picoquic_set_cert_verifier
    moq_picoquic_cert_verify_callback_supported
    moq_picoquic_set_cert_verify_callback
    moq_picoquic_ca_file_loadable
)
set(SPI_SYMBOLS
    # picoquic_spi.h (private adapter SPI, not installed)
    moq_pq_conn_cnx_released
    moq_pq_conn_get_send_stats
)

set(EXPECTED_SYMBOLS "")
foreach(base IN LISTS PUBLIC_SYMBOLS SPI_SYMBOLS)
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
    RESULT_VARIABLE nm_rc)
if(NOT nm_rc EQUAL 0)
    message(FATAL_ERROR "nm failed (rc=${nm_rc}): ${nm_err}")
endif()

# Every defined dynamic symbol: the last field of each nm line.
string(REPLACE "\n" ";" nm_lines "${nm_output}")
set(FOUND_SYMBOLS "")
foreach(line IN LISTS nm_lines)
    string(STRIP "${line}" line)
    if(line STREQUAL "")
        continue()
    endif()
    string(REGEX REPLACE "^.* " "" sym "${line}")
    list(APPEND FOUND_SYMBOLS "${sym}")
endforeach()
if(NOT FOUND_SYMBOLS)
    message(FATAL_ERROR "no exported symbols read from ${DYLIB}")
endif()
list(REMOVE_DUPLICATES FOUND_SYMBOLS)

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
    set(msg "Adapter symbol policy violation for ${DYLIB}:")
    if(missing)
        string(REPLACE ";" "\n    " missing_str "${missing}")
        string(APPEND msg "\n  MISSING (must be exported):\n    ${missing_str}")
    endif()
    if(extra)
        string(REPLACE ";" "\n    " extra_str "${extra}")
        string(APPEND msg
            "\n  UNEXPECTED (exported but neither public API nor the pinned"
            " SPI):\n    ${extra_str}")
    endif()
    message(FATAL_ERROR "${msg}")
endif()

list(LENGTH PUBLIC_SYMBOLS n_public)
list(LENGTH SPI_SYMBOLS n_spi)
message(STATUS "adapter_picoquic_symbol_policy: exact export set verified "
    "(${n_public} public, ${n_spi} private SPI)")
