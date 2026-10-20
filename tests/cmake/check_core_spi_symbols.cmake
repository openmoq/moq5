# check_core_spi_symbols.cmake — pin the core's private same-build SPI exports.
#
# moq_core_internal_* symbols (core/src/spi/core_spi.h) are exported from the
# shared libmoq-core only so a relay library built with the same core can call
# them. They are a private same-build ABI: the header is NOT installed and this
# is NOT a stable third-party/application ABI.
#
# This check pins the EXACT set: the library must export every listed symbol
# and NO OTHER moq_core_internal_* symbol. A missing symbol is a visibility
# breakage (the shared relay would fail to link); an extra one is an unreviewed
# addition to the private SPI. Adding or removing one is a deliberate change
# that must update BASE_SYMBOLS below.
#
# Usage: cmake -DDYLIB=<library> -DNM=<nm tool> -DFORMAT=<macho|elf>
#              -P check_core_spi_symbols.cmake
#
# Supported formats are the two native ones, named by the caller from the
# target's executable format (not guessed from the host):
#   macho  `nm -gU`: global defined symbols, "<hex> <type> _<name>"; the
#          leading underscore of a C name is removed.
#   elf    `nm -D --defined-only`: dynamic defined symbols, "<hex> <type>
#          <name>[@[@]<version>]"; the version suffix is removed.
# Any other format (PE among them) is refused as UNSUPPORTED: it is not a
# measured result, and the CTest registration reports it as skipped, never as
# a pass. A universal (multi-architecture) Mach-O listing is refused as
# malformed output rather than merged across slices.
#
# The scan fails closed: the tool must launch, exit 0, write nothing to
# stderr, print at least one symbol, and print only well-formed records.
# Symbols are compared as complete name fields, never as substrings.

if(NOT DYLIB)
    message(FATAL_ERROR "core_spi_symbol_policy: DYLIB not set")
endif()
if(NOT EXISTS "${DYLIB}")
    message(FATAL_ERROR "core_spi_symbol_policy: library not found: ${DYLIB}")
endif()
if(NOT NM)
    message(FATAL_ERROR "core_spi_symbol_policy: NM (the configured nm tool) not set")
endif()

if(FORMAT STREQUAL "macho")
    set(NM_FLAGS -gU)
elseif(FORMAT STREQUAL "elf")
    set(NM_FLAGS -D --defined-only)
else()
    message(FATAL_ERROR
        "core_spi_symbol_policy: UNSUPPORTED object format '${FORMAT}': "
        "no exported-symbol scan was performed")
endif()

# The complete private SPI surface (core/src/spi/core_spi.h MOQ_API symbols).
set(BASE_SYMBOLS
    moq_core_internal_track_max_cache_duration
    moq_core_internal_track_properties_validate
)
set(FAMILY_PREFIX "moq_core_internal_")

execute_process(
    COMMAND "${NM}" ${NM_FLAGS} "${DYLIB}"
    OUTPUT_VARIABLE nm_output
    ERROR_VARIABLE nm_err
    RESULT_VARIABLE nm_rc
)
if(NOT "${nm_rc}" STREQUAL "0")
    message(FATAL_ERROR
        "core_spi_symbol_policy: nm did not complete (result '${nm_rc}'): ${nm_err}")
endif()
if(NOT "${nm_err}" STREQUAL "")
    message(FATAL_ERROR
        "core_spi_symbol_policy: nm wrote diagnostics; the scan is not trusted:\n${nm_err}")
endif()

# Parse every record into its complete, normalized name field.
string(REPLACE ";" "\\;" nm_output "${nm_output}")
string(REPLACE "\n" ";" nm_lines "${nm_output}")
set(names "")
foreach(line IN LISTS nm_lines)
    string(REGEX REPLACE "\r$" "" line "${line}")
    if(line STREQUAL "")
        continue()
    endif()
    if(NOT line MATCHES "^[0-9A-Fa-f]+ [A-Za-z] ([^ \t]+)$")
        message(FATAL_ERROR
            "core_spi_symbol_policy: malformed nm record for format ${FORMAT}: '${line}'")
    endif()
    set(name "${CMAKE_MATCH_1}")
    if(FORMAT STREQUAL "macho")
        if(name MATCHES "^_(.+)$")
            set(name "${CMAKE_MATCH_1}")
        endif()
    else()
        string(REGEX REPLACE "@@?[^@]*$" "" name "${name}")
    endif()
    list(APPEND names "${name}")
endforeach()
list(LENGTH names name_count)
if(name_count EQUAL 0)
    message(FATAL_ERROR "core_spi_symbol_policy: nm listed no defined symbols")
endif()

# The exported family: complete names that begin with the family prefix.
set(found "")
foreach(name IN LISTS names)
    string(FIND "${name}" "${FAMILY_PREFIX}" at)
    if(at EQUAL 0)
        list(APPEND found "${name}")
    endif()
endforeach()
if(found)
    list(REMOVE_DUPLICATES found)
endif()

set(missing "")
foreach(sym IN LISTS BASE_SYMBOLS)
    list(FIND found "${sym}" idx)
    if(idx EQUAL -1)
        list(APPEND missing "${sym}")
    endif()
endforeach()
set(extra "")
foreach(sym IN LISTS found)
    list(FIND BASE_SYMBOLS "${sym}" idx)
    if(idx EQUAL -1)
        list(APPEND extra "${sym}")
    endif()
endforeach()

if(missing OR extra)
    set(msg "core_spi_symbol_policy: violation for ${DYLIB}:")
    if(missing)
        string(REPLACE ";" "\n    " missing_str "${missing}")
        string(APPEND msg
            "\n  MISSING (must be MOQ_API for the same-build relay):\n    ${missing_str}")
    endif()
    if(extra)
        string(REPLACE ";" "\n    " extra_str "${extra}")
        string(APPEND msg
            "\n  UNEXPECTED (exported but not in the pinned SPI list;"
            " update BASE_SYMBOLS if this is intentional):\n    ${extra_str}")
    endif()
    message(FATAL_ERROR "${msg}")
endif()

list(LENGTH BASE_SYMBOLS count)
message(STATUS
    "core_spi_symbol_policy: exact private SPI set verified (${count} symbols, ${FORMAT})")
