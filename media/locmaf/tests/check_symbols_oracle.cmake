cmake_minimum_required(VERSION 3.20)

file(MAKE_DIRECTORY "${CHECK_DIR}")
file(STRINGS "${SOURCE_DIR}/locmaf.symbols" expected)
set(fixture "#define PUBLIC __attribute__((visibility(\"default\")))\n")
foreach(name IN LISTS expected)
    if(name STREQUAL moq_locmaf_reason_name)
        string(APPEND fixture "#ifdef HIDE_SYMBOL\nvoid ${name}(void);\nvoid ${name}(void) { }\n#else\n")
    endif()
    string(APPEND fixture "PUBLIC void ${name}(void);\nPUBLIC void ${name}(void) { }\n")
    if(name STREQUAL moq_locmaf_reason_name)
        string(APPEND fixture "#endif\n")
    endif()
endforeach()
string(APPEND fixture
    "#ifdef EXTRA_EXPORT\nPUBLIC void accidental_export(void);\nPUBLIC void accidental_export(void) { }\n#endif\n"
    "#ifdef HIDDEN_SUPPORT\n__attribute__((visibility(\"hidden\"), common)) unsigned long ___asan_globals_registered;\n#endif\n"
    "#ifdef PUBLIC_SUPPORT\nPUBLIC __attribute__((common)) unsigned long ___asan_globals_registered;\n#endif\n")
file(WRITE "${CHECK_DIR}/symbol_fixture.c" "${fixture}")
set(failures 0)
foreach(case good hidden extra static_good static_hidden static_extra
        static_hidden_support static_public_support)
    set(flags)
    if(case MATCHES "^(static_)?hidden$")
        list(APPEND flags -DHIDE_SYMBOL)
    elseif(case MATCHES "^(static_)?extra$")
        list(APPEND flags -DEXTRA_EXPORT)
    elseif(case STREQUAL static_hidden_support)
        list(APPEND flags -DHIDDEN_SUPPORT)
    elseif(case STREQUAL static_public_support)
        list(APPEND flags -DPUBLIC_SUPPORT)
    endif()
    set(shared ON)
    set(link_flags -shared)
    set(library "${CHECK_DIR}/${case}.so")
    if(case MATCHES "^static_")
        set(shared OFF)
        set(link_flags -c)
        set(library "${CHECK_DIR}/${case}.o")
    endif()
    execute_process(COMMAND "${CC}" ${link_flags} -fPIC -fvisibility=hidden
        -Wall -Wextra -Wpedantic -Werror ${flags}
        "${CHECK_DIR}/symbol_fixture.c" -o "${library}"
        RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
    if(NOT rc EQUAL 0 OR NOT "${out}${err}" STREQUAL "")
        message(FATAL_ERROR "fixture compile failed: ${out}${err}")
    endif()
    if(NOT shared)
        execute_process(COMMAND "${AR}" rcs "${CHECK_DIR}/${case}.a" "${library}"
            RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
        if(NOT rc EQUAL 0 OR NOT "${out}${err}" STREQUAL "")
            message(FATAL_ERROR "fixture archive failed: ${out}${err}")
        endif()
        set(library "${CHECK_DIR}/${case}.a")
    endif()
    if(case STREQUAL hidden)
        # Retain a GLOBAL ordinary-symtab symbol which is absent from dynsym.
        # A dynamic export oracle must not adopt it from the ordinary table.
        execute_process(COMMAND "${OBJCOPY}" --globalize-symbol=moq_locmaf_reason_name
            "${library}" RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
        if(NOT rc EQUAL 0 OR NOT "${out}${err}" STREQUAL "")
            message(FATAL_ERROR "objcopy fixture failed: ${err}")
        endif()
    endif()
    execute_process(COMMAND "${CMAKE_COMMAND}" "-DNM=${NM}"
        "-DLIBRARY=${library}" "-DSHARED=${shared}" "-DREADELF=${READELF}" -DELF=ON
        -P "${SOURCE_DIR}/check_symbols.cmake"
        RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
    if(case STREQUAL good OR case STREQUAL static_good
            OR case STREQUAL static_hidden_support)
        if(NOT rc EQUAL 0 OR NOT "${out}${err}" STREQUAL "")
            message(FATAL_ERROR "valid export set refused: ${out}${err}")
        endif()
    elseif(rc EQUAL 0)
        message(STATUS "FAIL: ${case} export mutant escaped the oracle")
        math(EXPR failures "${failures} + 1")
    elseif(NOT "${out}${err}" MATCHES "LOCMAF export set mismatch")
        message(FATAL_ERROR "${case} failed for the wrong reason: ${out}${err}")
    endif()
    message(STATUS "export oracle ${case}: expected result")
endforeach()
if(failures)
    message(FATAL_ERROR "export oracle failures=${failures}")
endif()
