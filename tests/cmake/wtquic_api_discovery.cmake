cmake_minimum_required(VERSION 3.20)
include("${MOQ_SOURCE}/tests/cmake/RejectConsumerWarnings.cmake")
moq_consumer_warning_patterns(warnings)
if(NOT DEFINED CONFIG OR CONFIG STREQUAL "")
    set(CONFIG Debug)
endif()
function(probe_context language output)
set(context)
foreach(v CMAKE_TOOLCHAIN_FILE CMAKE_${language}_COMPILER
          CMAKE_${language}_FLAGS CMAKE_EXE_LINKER_FLAGS
          CMAKE_OSX_SYSROOT CMAKE_OSX_ARCHITECTURES CMAKE_OSX_DEPLOYMENT_TARGET
          CMAKE_SYSROOT CMAKE_SYSTEM_NAME CMAKE_SYSTEM_PROCESSOR)
    if(DEFINED ${v} AND NOT "${${v}}" STREQUAL "")
        string(REPLACE ";" "\\;" value "${${v}}")
        list(APPEND context "-D${v}=${value}")
    endif()
endforeach()
set(${output} "${context}" PARENT_SCOPE)
endfunction()
function(run name expected)
    set(command "execute_process(COMMAND")
    math(EXPR last "${ARGC} - 1")
    foreach(i RANGE 2 ${last})
        moq_consumer_quote_argument("${ARGV${i}}" arg)
        string(APPEND command " ${arg}")
    endforeach()
    string(APPEND command " RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)")
    cmake_language(EVAL CODE "${command}")
    file(WRITE "${WORK}/${name}.log" "${out}${err}")
    if(expected STREQUAL "pass")
        if(NOT rc EQUAL 0)
            message(FATAL_ERROR "${name}: unexpected failure: ${out}${err}")
        endif()
        foreach(pattern IN LISTS warnings)
            if("${out}${err}" MATCHES "${pattern}")
                message(FATAL_ERROR "${name}: unexpected warning: ${out}${err}")
            endif()
        endforeach()
    elseif(rc EQUAL 0 OR NOT "${out}${err}" MATCHES "wtquic public API compile/link contract requires")
        message(FATAL_ERROR "${name}: missing exact API rejection: ${out}${err}")
    endif()
endfunction()
function(write_probe path)
    set(command "moq_consumer_execute(COMMAND")
    math(EXPR last "${ARGC} - 1")
    foreach(i RANGE 1 ${last})
        moq_consumer_quote_argument("${ARGV${i}}" arg)
        string(APPEND command " ${arg}")
    endforeach()
    string(APPEND command " RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)\n")
    file(WRITE "${path}" "${command}")
endfunction()
file(MAKE_DIRECTORY "${WORK}")
# Exercise the same context builder/function boundary on every host without
# requiring that host's compiler to support multiple target architectures.
function(check_list_context)
    set(CMAKE_OSX_ARCHITECTURES "architecture one;architecture two")
    probe_context(C context)
    run(context-forward pass "${CMAKE_COMMAND}" ${context}
        -P "${MOQ_SOURCE}/tests/cmake/wtquic_api_context.cmake")
    write_probe("${WORK}/context-command.cmake" "${CMAKE_COMMAND}" ${context}
        -P "${MOQ_SOURCE}/tests/cmake/wtquic_api_context.cmake")
    run(context-nested pass "${CMAKE_COMMAND}" "-DMOQ_SOURCE=${MOQ_SOURCE}"
        "-DPROBE_FILE=${WORK}/context-command.cmake"
        "-DRECEIPT=${WORK}/context-nested-command.log"
        -P "${MOQ_SOURCE}/tests/cmake/wtquic_api_diagnostic.cmake")
endfunction()
check_list_context()
write_probe("${WORK}/arguments-command.cmake" "${CMAKE_COMMAND}" -DARGUMENT_CONTROL=ON
    -P "${MOQ_SOURCE}/tests/cmake/consumer_warning_diagnostics.cmake" --
    "" left "" "right ; with space" [=[brackets "quotes" \path ${UNDEFINED}]=]
    "\nline\n" "")
run(arguments-nested pass "${CMAKE_COMMAND}" "-DMOQ_SOURCE=${MOQ_SOURCE}"
    "-DPROBE_FILE=${WORK}/arguments-command.cmake"
    "-DRECEIPT=${WORK}/arguments-nested-command.log"
    -P "${MOQ_SOURCE}/tests/cmake/wtquic_api_diagnostic.cmake")
probe_context(C context)
foreach(kind old query admission flush-mode flush-query flush new)
    run("${kind}-configure" pass "${CMAKE_COMMAND}" -G "${GENERATOR}"
        -S "${MOQ_SOURCE}/tests/cmake/wtquic_api_fixture" -B "${WORK}/${kind}"
        "-DAPI_KIND=${kind}" "-DPROBE_CONFIG=${CONFIG}" ${context})
    run("${kind}-build" pass "${CMAKE_COMMAND}" --build "${WORK}/${kind}" --config "${CONFIG}")
    file(READ "${WORK}/${kind}/library-${CONFIG}.txt" lib_${kind})
endforeach()
foreach(lang C CXX)
    probe_context("${lang}" context)
    foreach(kind old query admission flush-mode flush-query flush new mixed
                 missing-flush-mode-library missing-flush-query-library
                 missing-budget-library)
        set(headers "${kind}")
        set(library "${kind}")
        set(expected fail)
        if(kind STREQUAL "mixed")
            set(headers new)
            set(library old)
        elseif(kind STREQUAL "missing-flush-mode-library")
            set(headers new)
            set(library flush-query)
        elseif(kind STREQUAL "missing-flush-query-library")
            set(headers new)
            set(library flush-mode)
        elseif(kind STREQUAL "missing-budget-library")
            set(headers new)
            set(library flush)
        elseif(kind STREQUAL "new")
            set(expected pass)
        endif()
        run("${lang}-${kind}" "${expected}" "${CMAKE_COMMAND}" -G "${GENERATOR}"
            -S "${MOQ_SOURCE}/tests/cmake/wtquic_api_probe" -B "${WORK}/${lang}-${kind}"
            "-DMOQ_SOURCE=${MOQ_SOURCE}" "-DPROBE_LANGUAGE=${lang}"
            "-DCMAKE_TRY_COMPILE_CONFIGURATION=${CONFIG}"
            "-DFIXTURE_INCLUDE=${WORK}/${headers}/include"
            "-DFIXTURE_LIBRARY=${lib_${library}}" ${context})
    endforeach()
    # Repeat in the successful build dir, changing only its library input.
    run("${lang}-stale" fail "${CMAKE_COMMAND}" -S "${MOQ_SOURCE}/tests/cmake/wtquic_api_probe"
        -B "${WORK}/${lang}-new" "-DFIXTURE_LIBRARY=${lib_old}")
    run("${lang}-restored" pass "${CMAKE_COMMAND}" -S "${MOQ_SOURCE}/tests/cmake/wtquic_api_probe"
        -B "${WORK}/${lang}-new" "-DFIXTURE_LIBRARY=${lib_new}")
    run("${lang}-stale-flush" fail "${CMAKE_COMMAND}" -S "${MOQ_SOURCE}/tests/cmake/wtquic_api_probe"
        -B "${WORK}/${lang}-new" "-DFIXTURE_LIBRARY=${lib_admission}")
    run("${lang}-restored-budget" pass "${CMAKE_COMMAND}" -S "${MOQ_SOURCE}/tests/cmake/wtquic_api_probe"
        -B "${WORK}/${lang}-new" "-DFIXTURE_LIBRARY=${lib_new}")
    run("${lang}-stale-budget" fail "${CMAKE_COMMAND}" -S "${MOQ_SOURCE}/tests/cmake/wtquic_api_probe"
        -B "${WORK}/${lang}-new" "-DFIXTURE_LIBRARY=${lib_flush}")
endforeach()
# MsQuic managed component: header field/initializer and link refusals over
# the complete current core fixture. Base and Network-only discovery above
# never run this probe. A current header over an older backend library that
# still defines every probed entry point links: that is availability, not
# qualification (such a session's budget query is refused at attach).
probe_context(C context)
foreach(kind msquic-old msquic-new msquic-missing)
    run("${kind}-configure" pass "${CMAKE_COMMAND}" -G "${GENERATOR}"
        -S "${MOQ_SOURCE}/tests/cmake/wtquic_api_fixture" -B "${WORK}/${kind}"
        "-DAPI_KIND=${kind}" "-DPROBE_CONFIG=${CONFIG}"
        "-DCORE_INCLUDE=${WORK}/new/include" ${context})
    run("${kind}-build" pass "${CMAKE_COMMAND}" --build "${WORK}/${kind}" --config "${CONFIG}")
    file(READ "${WORK}/${kind}/library-${CONFIG}.txt" lib_${kind})
endforeach()
foreach(lang C CXX)
    probe_context("${lang}" context)
    # row: msquic header kind, msquic library kind, core library kind, expected
    foreach(row "new;new;new;pass" "old;new;new;fail" "new;old;new;pass"
                "new;missing;new;fail" "new;new;flush;fail")
        list(GET row 0 header)
        list(GET row 1 library)
        list(GET row 2 core)
        list(GET row 3 expected)
        set(name "${lang}-msquic-${header}-${library}-${core}")
        run("${name}" "${expected}" "${CMAKE_COMMAND}" -G "${GENERATOR}"
            -S "${MOQ_SOURCE}/tests/cmake/wtquic_api_probe" -B "${WORK}/${name}"
            "-DMOQ_SOURCE=${MOQ_SOURCE}" "-DPROBE_LANGUAGE=${lang}"
            -DPROBE_COMPONENT=msquic
            "-DCMAKE_TRY_COMPILE_CONFIGURATION=${CONFIG}"
            "-DFIXTURE_INCLUDE=${WORK}/new/include"
            "-DFIXTURE_LIBRARY=${lib_${core}}"
            "-DFIXTURE_MSQUIC_INCLUDE=${WORK}/msquic-${header}/include"
            "-DFIXTURE_MSQUIC_LIBRARY=${lib_msquic-${library}}" ${context})
    endforeach()
    # Repeat in the successful build dir, changing only one library input: a
    # cached success must not survive replacement of either library.
    set(good "${WORK}/${lang}-msquic-new-new-new")
    run("${lang}-msquic-stale-entry" fail "${CMAKE_COMMAND}"
        -S "${MOQ_SOURCE}/tests/cmake/wtquic_api_probe" -B "${good}"
        "-DFIXTURE_MSQUIC_LIBRARY=${lib_msquic-missing}")
    run("${lang}-msquic-stale-core" fail "${CMAKE_COMMAND}"
        -S "${MOQ_SOURCE}/tests/cmake/wtquic_api_probe" -B "${good}"
        "-DFIXTURE_MSQUIC_LIBRARY=${lib_msquic-new}" "-DFIXTURE_LIBRARY=${lib_flush}")
    run("${lang}-msquic-restored" pass "${CMAKE_COMMAND}"
        -S "${MOQ_SOURCE}/tests/cmake/wtquic_api_probe" -B "${good}"
        "-DFIXTURE_MSQUIC_LIBRARY=${lib_msquic-new}" "-DFIXTURE_LIBRARY=${lib_new}")
endforeach()
file(MAKE_DIRECTORY "${WORK}/warning/include/wtquic")
file(READ "${WORK}/new/include/wtquic/session.h" warning_header)
file(WRITE "${WORK}/warning/include/wtquic/session.h"
    "${warning_header}\n#warning deliberate successful-probe diagnostic control\n")
probe_context(C context)
run(warning fail "${CMAKE_COMMAND}" -G "${GENERATOR}"
    -S "${MOQ_SOURCE}/tests/cmake/wtquic_api_probe" -B "${WORK}/warning-probe"
    "-DMOQ_SOURCE=${MOQ_SOURCE}" -DPROBE_LANGUAGE=C
    "-DCMAKE_TRY_COMPILE_CONFIGURATION=${CONFIG}"
    "-DFIXTURE_INCLUDE=${WORK}/warning/include" "-DFIXTURE_LIBRARY=${lib_new}" ${context})
run(no-language fail "${CMAKE_COMMAND}" -G "${GENERATOR}"
    -S "${MOQ_SOURCE}/tests/cmake/wtquic_api_probe" -B "${WORK}/no-language"
    "-DMOQ_SOURCE=${MOQ_SOURCE}" -DPROBE_LANGUAGE=NONE
    "-DFIXTURE_INCLUDE=${WORK}/new/include" "-DFIXTURE_LIBRARY=${lib_new}")
# try_compile cannot capture its own nested configure warnings. The actual
# consumer-process guard must refuse them even when the API link check passes.
set(marker "${WORK}/diagnostic-enabled")
set(toolchain "${WORK}/diagnostic-toolchain.cmake")
file(WRITE "${toolchain}" "set(marker [==[${marker}]==])\n")
if(CMAKE_TOOLCHAIN_FILE)
    file(APPEND "${toolchain}" "include([==[${CMAKE_TOOLCHAIN_FILE}]==])\n")
endif()
file(APPEND "${toolchain}" [=[
get_property(in_probe GLOBAL PROPERTY IN_TRY_COMPILE)
if(in_probe AND EXISTS "${marker}")
    file(READ "${marker}" diagnostic)
    if(diagnostic STREQUAL "warning")
        message(WARNING "deliberate nested API configure control")
    elseif(diagnostic STREQUAL "deprecation")
        message(DEPRECATION "deliberate nested API configure control")
    endif()
endif()
]=])
foreach(diagnostic clean warning deprecation)
    file(REMOVE "${marker}")
    set(probe_args "${context}")
    list(APPEND probe_args -G "${GENERATOR}"
        -S "${MOQ_SOURCE}/tests/cmake/wtquic_api_probe"
        -B "${WORK}/nested-${diagnostic}" "-DMOQ_SOURCE=${MOQ_SOURCE}"
        -DPROBE_LANGUAGE=C "-DCMAKE_TRY_COMPILE_CONFIGURATION=${CONFIG}"
        "-DFIXTURE_INCLUDE=${WORK}/new/include" "-DFIXTURE_LIBRARY=${lib_new}"
        "-DCMAKE_TOOLCHAIN_FILE=${toolchain}" "-DNESTED_DIAGNOSTIC_MARKER=${marker}"
        "-DNESTED_DIAGNOSTIC=${diagnostic}")
    write_probe("${WORK}/nested-${diagnostic}-command.cmake" "${CMAKE_COMMAND}" ${probe_args})
    execute_process(COMMAND "${CMAKE_COMMAND}" "-DMOQ_SOURCE=${MOQ_SOURCE}"
        "-DPROBE_FILE=${WORK}/nested-${diagnostic}-command.cmake" "-DRECEIPT=${WORK}/nested-${diagnostic}-configure.log"
        -P "${MOQ_SOURCE}/tests/cmake/wtquic_api_diagnostic.cmake"
        RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
    file(WRITE "${WORK}/nested-${diagnostic}-verdict.log" "${out}${err}")
    if(diagnostic STREQUAL "clean")
        if(NOT rc EQUAL 0)
            message(FATAL_ERROR "clean nested configure refused: ${out}${err}")
        endif()
    elseif(rc EQUAL 0 OR NOT "${out}${err}" MATCHES "consumer command 1: unexpected diagnostic")
        message(FATAL_ERROR "nested ${diagnostic} escaped complete-output guard: ${out}${err}")
    endif()
endforeach()
message(STATUS "WT API discovery: C/C++ receive/admission/budget/flush declaration and symbol refusals, MsQuic component field/initializer/link refusals, stale-library refusals, API-only pass, warning/no-language diagnostics")
