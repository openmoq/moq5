# Run the snapshot/broker test and require that it actually did its work.
#
# Written as a CMake script, not a shell script, so the checked path is the
# ONLY path: there is no "if a shell exists" branch that could silently fall
# back to invoking the bare executable. A bare invocation cannot tell a run
# that did the work from one that returned early, and CMake's
# PASS_REGULAR_EXPRESSION would ignore the exit status entirely.
#
# All three must hold together: status zero, exactly one completion receipt,
# and exactly one final success record. No clock, no sleeps, no network.
#
#   cmake -DMOQR_BIN=<path> -P run_snapshot_cap.cmake

if(NOT DEFINED MOQR_BIN)
    message(FATAL_ERROR "FAIL: MOQR_BIN was not provided")
endif()
if(NOT EXISTS "${MOQR_BIN}")
    message(FATAL_ERROR "FAIL: ${MOQR_BIN} does not exist")
endif()

execute_process(
    COMMAND "${MOQR_BIN}"
    OUTPUT_VARIABLE out
    ERROR_VARIABLE err
    RESULT_VARIABLE status)

set(combined "${out}${err}")
set(problems "")

if(NOT status EQUAL 0)
    string(APPEND problems "  process exited with status ${status}, expected 0\n")
endif()

# Records are matched as COMPLETE LINES, not as substrings.
#
# An unanchored search accepts arbitrary prefix and suffix bytes, and accepts
# both expected strings embedded in one line of prose -- which is not the exact
# record contract this oracle claims. Splitting into lines and comparing whole
# strings is the only form that means what it says.
#
# CRLF policy: a trailing carriage return is stripped, so a run on a platform
# that emits CRLF is accepted with the same records. A lone CR is also treated
# as a line break. Semicolons are escaped first, because they are CMake's list
# separator and would otherwise split a line in two.
set(RECEIPT_LINE "RECEIPT concurrent_pumps completed gens=16 threads=4")
set(SUCCESS_LINE "test_relay_snapshot_cap: OK")

string(REPLACE ";" "\\;" safe "${combined}")
string(REPLACE "\r\n" "\n" safe "${safe}")
string(REPLACE "\r" "\n" safe "${safe}")
string(REPLACE "\n" ";" split_lines "${safe}")

# Two counts per record: lines that EQUAL it, and lines that merely CONTAIN
# it. Both must be exactly one. Counting only equal lines would accept a run
# that emits the true record plus a decorated impostor alongside it, which is
# the shape a fabricated payload takes; requiring the containing count to match
# means the text appears nowhere except as its own line.
set(receipt_exact 0)
set(receipt_seen 0)
set(ok_exact 0)
set(ok_seen 0)
foreach(line IN LISTS split_lines)
    if(line STREQUAL RECEIPT_LINE)
        math(EXPR receipt_exact "${receipt_exact} + 1")
    endif()
    if(line STREQUAL SUCCESS_LINE)
        math(EXPR ok_exact "${ok_exact} + 1")
    endif()
    string(FIND "${line}" "${RECEIPT_LINE}" rpos)
    if(NOT rpos EQUAL -1)
        math(EXPR receipt_seen "${receipt_seen} + 1")
    endif()
    string(FIND "${line}" "${SUCCESS_LINE}" opos)
    if(NOT opos EQUAL -1)
        math(EXPR ok_seen "${ok_seen} + 1")
    endif()
endforeach()

set(receipt_count ${receipt_exact})
set(ok_count ${ok_exact})
if(NOT receipt_seen EQUAL receipt_exact)
    string(APPEND problems
           "  the receipt text appears on ${receipt_seen} line(s) but is an exact line ${receipt_exact} time(s)\n")
endif()
if(NOT ok_seen EQUAL ok_exact)
    string(APPEND problems
           "  the success text appears on ${ok_seen} line(s) but is an exact line ${ok_exact} time(s)\n")
endif()

if(NOT receipt_count EQUAL 1)
    string(APPEND problems
           "  expected exactly 1 line equal to the completion receipt, found ${receipt_count}\n")
endif()
if(NOT ok_count EQUAL 1)
    string(APPEND problems
           "  expected exactly 1 line equal to the final success record, found ${ok_count}\n")
endif()

if(NOT problems STREQUAL "")
    message("--- output ---\n${combined}")
    message(FATAL_ERROR "FAIL: snapshot_cap authority\n${problems}")
endif()

message("PASS: exit 0, one completion receipt, one success record")
