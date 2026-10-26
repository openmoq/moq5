# check_core_spi_symbols_controls.cmake — negative and positive controls for
# check_core_spi_symbols.cmake, driven with scripted stand-ins for nm.
#
# Each row writes a small shell script that prints a fixed listing (and,
# where the row needs it, a diagnostic or an exit status), runs the checker
# against it, and requires the expected verdict. The checker must accept the
# exact native inventories and refuse every other row.
#
# Usage: cmake -DCHECKER=<check_core_spi_symbols.cmake> -DWORK=<scratch dir>
#              -P check_core_spi_symbols_controls.cmake

if(NOT CHECKER OR NOT EXISTS "${CHECKER}")
    message(FATAL_ERROR "spi_symbol_controls: CHECKER not found: '${CHECKER}'")
endif()
if(NOT WORK)
    message(FATAL_ERROR "spi_symbol_controls: WORK not set")
endif()
find_program(SH_PROG NAMES sh)
if(NOT SH_PROG)
    message(FATAL_ERROR "spi_symbol_controls: sh not found (the controls need a POSIX shell)")
endif()

file(REMOVE_RECURSE "${WORK}")
file(MAKE_DIRECTORY "${WORK}")
set(lib "${WORK}/libfake.so")
file(WRITE "${lib}" "")

set(failures 0)

# run_row(<name> <format> <expect: pass|fail> <reason> <stdout> <stderr> <exit>)
# <reason> is text the checker's verdict must contain, so a row cannot pass
# by failing for an unrelated reason.
function(run_row name format expect reason out err code)
    set(script "${WORK}/${name}.sh")
    file(WRITE "${WORK}/${name}.out" "${out}")
    file(WRITE "${script}"
        "#!${SH_PROG}\n"
        "cat '${WORK}/${name}.out'\n"
        "printf '%s' '${err}' >&2\n"
        "exit ${code}\n")
    file(CHMOD "${script}" PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE)
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -DDYLIB=${lib} -DNM=${script} -DFORMAT=${format}
                -P "${CHECKER}"
        OUTPUT_VARIABLE o ERROR_VARIABLE e RESULT_VARIABLE rc)
    if("${rc}" STREQUAL "0")
        set(got pass)
    else()
        set(got fail)
    endif()
    string(FIND "${o}${e}" "${reason}" at)
    if(NOT got STREQUAL expect)
        message(SEND_ERROR "spi_symbol_controls: row '${name}' gave ${got}, expected ${expect}\n${o}${e}")
    elseif(at EQUAL -1)
        message(SEND_ERROR "spi_symbol_controls: row '${name}' lacks '${reason}'\n${o}${e}")
    else()
        message(STATUS "spi_symbol_controls: ${name}: ${got} (expected)")
    endif()
endfunction()

set(want "moq_core_internal_track_max_cache_duration")
set(want2 "moq_core_internal_track_properties_validate")
set(want3 "moq_core_internal_pub_reset_group")
set(other "moq_session_create")

# Exact native inventories.
run_row(macho_exact macho pass "exact private SPI set verified"
    "0000000000001000 T _${want}\n0000000000001800 T _${want2}\n0000000000001c00 T _${want3}\n0000000000002000 T _${other}\n" "" 0)
run_row(elf_exact elf pass "exact private SPI set verified"
    "0000000000001000 T ${want}\n0000000000001800 T ${want2}\n0000000000001c00 T ${want3}\n0000000000002000 T ${other}\n" "" 0)
run_row(elf_versioned elf pass "exact private SPI set verified"
    "0000000000001000 T ${want}@@MOQ_1\n0000000000001800 T ${want2}@@MOQ_1\n0000000000001c00 T ${want3}@@MOQ_1\n0000000000002000 T ${other}\n" "" 0)

# The expected symbol is absent.
run_row(missing macho fail "MISSING"
    "0000000000002000 T _${other}\n" "" 0)
run_row(missing_reset elf fail "MISSING"
    "0000000000001000 T ${want}\n0000000000001800 T ${want2}\n0000000000002000 T ${other}\n" "" 0)
# An unreviewed member of the family is exported.
run_row(extra macho fail "UNEXPECTED"
    "0000000000001000 T _${want}\n0000000000001800 T _${want2}\n0000000000001c00 T _${want3}\n0000000000003000 T _moq_core_internal_other\n" "" 0)
# Names that merely contain the expected one do not satisfy it.
run_row(embedded_prefix elf fail "MISSING"
    "0000000000001000 T other_${want}\n0000000000002000 T ${other}\n" "" 0)
run_row(embedded_suffix macho fail "MISSING"
    "0000000000001000 T _${want}.cold\n0000000000002000 T _${other}\n" "" 0)
# A diagnostic on stderr makes the scan untrusted even with exit 0.
run_row(stderr_exit_zero macho fail "wrote diagnostics"
    "0000000000001000 T _${want}\n" "nm: warning: truncated symbol table" 0)
# An empty listing, a nonzero exit, and a malformed record.
run_row(empty macho fail "listed no defined symbols" "" "" 0)
run_row(nonzero_exit macho fail "did not complete"
    "0000000000001000 T _${want}\n" "" 1)
run_row(malformed macho fail "malformed nm record"
    "0000000000001000 T _${want}\nnot a symbol record\n" "" 0)
# A universal listing names its architectures; it is not merged.
run_row(universal macho fail "malformed nm record"
    "\n${lib} (for architecture arm64):\n0000000000001000 T _${want}\n" "" 0)
# An unsupported format is refused, not passed.
run_row(unsupported_format pe fail "UNSUPPORTED object format"
    "0000000000001000 T ${want}\n" "" 0)

# The tool cannot be launched at all.
execute_process(
    COMMAND "${CMAKE_COMMAND}" -DDYLIB=${lib} -DNM=${WORK}/no-such-nm -DFORMAT=macho
            -P "${CHECKER}"
    OUTPUT_VARIABLE o ERROR_VARIABLE e RESULT_VARIABLE rc)
if("${rc}" STREQUAL "0")
    message(SEND_ERROR "spi_symbol_controls: row 'launch_error' passed, expected fail\n${o}${e}")
elseif(NOT "${o}${e}" MATCHES "did not complete")
    message(SEND_ERROR "spi_symbol_controls: row 'launch_error' lacks 'did not complete'\n${o}${e}")
else()
    message(STATUS "spi_symbol_controls: launch_error: fail (expected)")
endif()
