function(moq_consumer_warning_patterns out)
    set(${out} "[Ww][Aa][Rr][Nn][Ii][Nn][Gg]:;CMake (Deprecation )?Warning;\\[-Werror" PARENT_SCOPE)
endfunction()

# Quote one literal CMake argument, including empty values and list separators.
function(moq_consumer_quote_argument value out)
    string(REPLACE "\\" "\\\\" value "${value}")
    string(REPLACE "\"" "\\\"" value "${value}")
    string(REPLACE "$" "\\$" value "${value}")
    set(${out} "\"${value}\"" PARENT_SCOPE)
endfunction()

# Test-driver wrapper: retain and inspect output even when the child succeeds.
# Forward the original command/options unchanged, including expected failures.
function(moq_consumer_execute)
    cmake_parse_arguments(PARSE_ARGV 0 _moq_exec ""
        "RESULT_VARIABLE;OUTPUT_VARIABLE;ERROR_VARIABLE" "")
    set(_moq_extra)
    foreach(_kind RESULT OUTPUT ERROR)
        if(NOT _moq_exec_${_kind}_VARIABLE)
            set(_moq_exec_${_kind}_VARIABLE "_moq_captured_${_kind}")
            list(APPEND _moq_extra ${_kind}_VARIABLE "_moq_captured_${_kind}")
        endif()
    endforeach()
    # List expansion loses empty arguments. Evaluate a literally quoted call
    # built from ARGV<n>, without interpreting command data as CMake source.
    set(_moq_call "execute_process(")
    math(EXPR _moq_last "${ARGC} - 1")
    foreach(_moq_i RANGE ${_moq_last})
        moq_consumer_quote_argument("${ARGV${_moq_i}}" _moq_arg)
        string(APPEND _moq_call " ${_moq_arg}")
    endforeach()
    foreach(_moq_arg IN LISTS _moq_extra)
        moq_consumer_quote_argument("${_moq_arg}" _moq_quoted)
        string(APPEND _moq_call " ${_moq_quoted}")
    endforeach()
    string(APPEND _moq_call ")")
    cmake_language(EVAL CODE "${_moq_call}")
    set(_moq_output_vars "${_moq_exec_OUTPUT_VARIABLE};${_moq_exec_ERROR_VARIABLE}")
    list(REMOVE_DUPLICATES _moq_output_vars)
    set(_moq_transcript "")
    foreach(_var IN LISTS _moq_output_vars)
        string(APPEND _moq_transcript "${${_var}}")
    endforeach()
    get_property(_moq_index GLOBAL PROPERTY MOQ_CONSUMER_COMMAND_INDEX)
    if(NOT _moq_index)
        set(_moq_index 0)
    endif()
    math(EXPR _moq_index "${_moq_index} + 1")
    set_property(GLOBAL PROPERTY MOQ_CONSUMER_COMMAND_INDEX "${_moq_index}")
    file(MAKE_DIRECTORY "${WORK}")
    file(WRITE "${WORK}/consumer-command-${_moq_index}.log" "${_moq_transcript}")
    moq_reject_consumer_output("consumer command ${_moq_index}" "${_moq_transcript}")
    foreach(_kind RESULT OUTPUT ERROR)
        set(_var "${_moq_exec_${_kind}_VARIABLE}")
        set(${_var} "${${_var}}" PARENT_SCOPE)
    endforeach()
endfunction()

# Inspect the complete child-process output, not try_compile's build-only log
# or CTest's --output-on-failure summary.
function(moq_reject_consumer_output label output)
    moq_consumer_warning_patterns(patterns)
    foreach(pattern IN LISTS patterns)
        if("${output}" MATCHES "${pattern}")
            message(FATAL_ERROR "${label}: unexpected diagnostic:\n${output}")
        endif()
    endforeach()
endfunction()

function(moq_test_reject_consumer_warnings name)
    moq_consumer_warning_patterns(patterns)
    set_tests_properties(${name} PROPERTIES FAIL_REGULAR_EXPRESSION "${patterns}")
    get_property(registered GLOBAL PROPERTY MOQ_CONSUMER_WARNING_ORACLE_REGISTERED)
    if(NOT registered)
        add_test(NAME consumer_warning_diagnostics
            COMMAND "${CMAKE_COMMAND}"
                "-DORACLE_ROOT=${PROJECT_BINARY_DIR}/consumer-warning-diagnostics"
                "-DCTEST_COMMAND=${CMAKE_CTEST_COMMAND}"
                -P "${PROJECT_SOURCE_DIR}/tests/cmake/consumer_warning_diagnostics.cmake")
        set_property(GLOBAL PROPERTY MOQ_CONSUMER_WARNING_ORACLE_REGISTERED TRUE)
    endif()
endfunction()
